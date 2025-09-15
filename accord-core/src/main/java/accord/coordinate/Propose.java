/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.ProtocolModifiers.Faults;
import accord.api.RoutingKey;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.SimpleTracker;
import accord.local.Commands.AcceptOutcome;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Accept;
import accord.messages.Accept.AcceptReply;
import accord.messages.Callback;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.SortedListMap;
import accord.utils.WrappableException;

import static accord.api.ProtocolModifiers.Toggles.filterDuplicateDependenciesFromAcceptReply;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.messages.Commit.Invalidate.commitInvalidate;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.AcceptedInvalidate;
import static accord.primitives.TxnId.MediumPath.TrackStable;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

abstract class Propose<R> extends AbstractCoordination<FullRoute<?>, R, AcceptReply, AcceptReply>
{
    final Accept.Kind kind;
    final Ballot ballot;
    final Txn txn;
    final Route<?> require;
    final Deps deps;

    final Timestamp executeAt;
    final QuorumTracker tracker;

    Propose(Node node, SequentialAsyncExecutor executor, Topologies topologies, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Route<?> require, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
    {
        super(node, executor, txnId, route, topologies.nodes(), callback);
        this.kind = kind;
        this.ballot = ballot;
        this.txn = txn;
        this.require = require;
        this.deps = deps;
        this.executeAt = executeAt;
        this.tracker = new QuorumTracker(topologies);
        Invariants.require(txnId.isSyncPoint() || deps.maxTxnId(txnId).compareTo(executeAt) <= 0,
                           "Attempted to propose %s with an earlier executeAt than a conflicting transaction it witnessed: %s vs executeAt: %s", txnId, deps, executeAt);
        Invariants.require(topologies.currentEpoch() == executeAt.epoch());
    }

    @Override
    void start()
    {
        SortedArrays.SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        if (contact == null)
        {
            finishOnExaustion();
        }
        else
        {
            super.start();
            contact(to -> new Accept(to, tracker.topologies(), kind, ballot, txnId, scope, executeAt, deps, require != scope));
        }
    }

    @Override
    public void onSuccessInternal(Id from, int fromIndex, AcceptReply reply)
    {
        switch (reply.outcome)
        {
            default: throw new AssertionError("Unhandled AcceptOutcome: " + reply.outcome());
            case RejectedBallot:
                finishWithFailureOverride(Preempted.preempted(node.agent(), txnId, scope.homeKey()));
                break;

            case Truncated:
            case Redundant:
                if (require == scope || !isSufficientPartialReply(reply, from))
                {
                    Throwable failNow = null;
                    if (reply.outcome == AcceptOutcome.Truncated)
                        failNow = new Truncated(txnId, scope.homeKey());
                    else if (reply.supersededBy != null || ballot.equals(Ballot.ZERO))
                        failNow = Preempted.preempted(node.agent(), txnId, scope.homeKey());

                    if (failNow != null)
                        finishWithFailureOverride(failNow);
                    else
                        onFailureInternal(from, fromIndex, reply.committedExecuteAt == null ? null : new Redundant(txnId, scope.homeKey(), reply.committedExecuteAt));
                    break;
                }

            case Retired:
            case Success:
                recordOk(fromIndex, reply);
                if (tracker.recordSuccess(from) == Success)
                    onAccepted();
        }
    }

    private boolean isSufficientPartialReply(AcceptReply reply, Id from)
    {
        return reply.successful != null && reply.successful.containsAll(require.slice(tracker.topologies().computeRangesForNode(from), Minimal));
    }

    @Override
    public void onFailureInternal(Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
            finishOnFailure();
    }

    void onAccepted()
    {
        // TODO (desired): can we avoid merging on original accept? Would be nice to be able to reduce range txn dep calculation cost (by not including PreLoadContext).
        //  I think probably not possible, as we could have a recovery coordinator for some id' < id contact us before our original coordinator does.
        //  In this case either id' needs to wait (which requires potentially more states like the alternative medium path)
        //  Or we must pick it up as an Unstable dependency here.
        Deps newDeps = mergeNewDeps();
        Deps deps = mergeDeps(newDeps);
        node.agent().coordinatorEvents().onAccepted(txnId, ballot);
        if (kind == Accept.Kind.MEDIUM) adapter().execute(node, executor, tracker.topologies(), scope, ballot, ExecutePath.MEDIUM, CoordinationFlags.none(), txnId, txn, executeAt, deps, newDeps, finishAndTakeCallback());
        else adapter().stabilise(node, executor, tracker.topologies(), scope, ballot, txnId, txn, executeAt, deps, finishAndTakeCallback());
    }

    Deps mergeDeps()
    {
        return mergeNewDeps().with(deps);
    }

    Deps mergeDeps(Deps newDeps)
    {
        return Faults.discardPreAcceptDeps(txnId) ? newDeps : newDeps.with(deps);
    }

    Deps mergeNewDeps()
    {
        SortedListMap<Node.Id, AcceptReply> oks = finishOks();
        Deps deps = Deps.merge(oks, oks.domainSize(), SortedListMap::getValue, ok -> ok.deps);
        if (Faults.discardPreAcceptDeps(txnId))
            return deps;

        if (txnId.is(TrackStable))
        {
            // we must not propose as stable any dep < txnId that we did not propose as part of this phase
            if (filterDuplicateDependenciesFromAcceptReply())
                deps = deps.without(this.deps);

            deps = deps.markUnstableBefore(txnId);
        }

        return deps;
    }

    abstract CoordinationAdapter<R> adapter();


    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.Propose;
    }

    @Override
    public Ballot ballot()
    {
        return ballot;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    @Override
    public String describe()
    {
        return "ballot=" + ballot + ", executeAt=" + executeAt;
    }

    // A special version for proposing the invalidation of a transaction; only needs to succeed on one shard
    static class NotAccept extends AbstractCoordination<Participants<?>, Void, AcceptReply, Void> implements Callback<AcceptReply>
    {
        final Status status;
        final Ballot ballot;
        final TxnId txnId;

        private final SimpleTracker<?> tracker;

        NotAccept(Node node, SequentialAsyncExecutor executor, Status status, Topologies topologies, Ballot ballot, TxnId txnId, Participants<?> someParticipants, BiConsumer<Void, Throwable> callback)
        {
            super(node, executor, txnId, someParticipants, topologies.nodes(), callback);
            this.status = status;
            this.tracker = new QuorumTracker(topologies);
            this.ballot = ballot;
            this.txnId = txnId;
        }

        @Override
        void start()
        {
            super.start();
            contact(to -> new Accept.NotAccept(status, ballot, txnId, scope));
        }

        public static void proposeInvalidate(Node node, SequentialAsyncExecutor executor, Ballot ballot, TxnId txnId, RoutingKey invalidateWithParticipant, BiConsumer<Void, Throwable> callback)
        {
            proposeNotAccept(node, executor, AcceptedInvalidate, ballot, txnId, invalidateWithParticipant, callback);
        }

        public static void proposeNotAccept(Node node, SequentialAsyncExecutor executor, Status status, Ballot ballot, TxnId txnId, RoutingKey participatingKey, BiConsumer<Void, Throwable> callback)
        {
            try
            {
                Participants<?> participants = Participants.singleton(txnId.domain(), participatingKey);
                Topologies topologies = node.topology().forEpoch(participants, txnId.epoch(), SHARE);
                NotAccept notAccept = new NotAccept(node, executor, status, topologies, ballot, txnId, participants, callback);
                notAccept.start();
            }
            catch (Throwable t)
            {
                callback.accept(null, t);
            }
        }

        public static void proposeAndCommitInvalidate(Node node, SequentialAsyncExecutor executor, Ballot ballot, TxnId txnId, RoutingKey invalidateWithParticipant, Route<?> commitInvalidationTo, Timestamp invalidateUntil, BiConsumer<?, Throwable> callback)
        {
            proposeInvalidate(node, executor, ballot, txnId, invalidateWithParticipant, (success, fail) -> {
                if (fail != null)
                {
                    callback.accept(null, fail);
                }
                else
                {
                    node.agent().coordinatorEvents().onInvalidated(txnId);
                    node.withEpochExact(invalidateUntil.epoch(), executor, callback, t -> WrappableException.wrap(t), () -> {
                        commitInvalidate(node, txnId, commitInvalidationTo, invalidateUntil);
                        callback.accept(null, Invalidated.invalidated(node.agent(), txnId, invalidateWithParticipant));
                    });
                }
            });
        }

        @Override
        public void onSuccessInternal(Id from, int fromIndex, AcceptReply reply)
        {
            if (!reply.isOk())
            {
                finishWithFailureOverride(Preempted.preempted(node.agent(), txnId, null));
                return;
            }

            if (tracker.recordSuccess(from) == Success)
                finishWithSuccess(null);
        }

        @Override
        public void onFailureInternal(Id from, int fromIndex, Throwable failure)
        {
            recordFailure(failure);
            if (tracker.recordFailure(from) == Failed)
                finishOnFailure();
        }

        @Override
        public CoordinationKind kind()
        {
            return CoordinationKind.ProposeInvalidate;
        }

        @Override
        public Ballot ballot()
        {
            return ballot;
        }

        @Override
        public AbstractTracker<?> tracker()
        {
            return tracker;
        }

        @Override
        public String describe()
        {
            return "ballot=" + ballot;
        }
    }
}
