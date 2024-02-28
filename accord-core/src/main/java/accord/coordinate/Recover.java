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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import accord.api.Result;
import accord.coordinate.CoordinationAdapter.Invoke;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RecoveryTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.WaitOnCommit;
import accord.messages.WaitOnCommit.WaitOnCommitOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.coordinate.CoordinationAdapter.Factory.Step.InitiateRecovery;
import static accord.coordinate.CoordinationAdapter.Invoke.execute;
import static accord.coordinate.CoordinationAdapter.Invoke.persist;
import static accord.coordinate.CoordinationAdapter.Invoke.stabilise;
import static accord.coordinate.ExecutePath.RECOVER;
import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedOrLater;
import static accord.utils.Invariants.debug;
import static accord.utils.Invariants.illegalState;

// TODO (low priority, cleanup): rename to Recover (verb); rename Recover message to not clash
// TODO (expected): do not recover transactions that are known to be Stable and waiting to execute.
// TODO (expected): separate out recovery of sync points from standard transactions
public class Recover implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    class AwaitCommit extends AsyncResults.SettableResult<Timestamp> implements Callback<WaitOnCommitOk>
    {
        // TODO (desired, efficiency): this should collect the executeAt of any commit, and terminate as soon as one is found
        //                             that is earlier than TxnId for the Txn we are recovering; if all commits we wait for
        //                             are given earlier timestamps we can retry without restarting.
        final QuorumTracker tracker;

        AwaitCommit(Node node, TxnId txnId, Participants<?> participants)
        {
            Topology topology = node.topology().globalForEpoch(txnId.epoch()).forSelection(participants);
            this.tracker = new QuorumTracker(new Topologies.Single(node.topology().sorter(), topology));
            node.send(topology.nodes(), to -> new WaitOnCommit(to, topology, txnId, participants), this);
        }

        @Override
        public synchronized void onSuccess(Id from, WaitOnCommitOk reply)
        {
            if (isDone()) return;

            if (tracker.recordSuccess(from) == Success)
                trySuccess(null);
        }

        @Override
        public synchronized void onFailure(Id from, Throwable failure)
        {
            if (isDone()) return;

            if (tracker.recordFailure(from) == Failed)
                tryFailure(new Timeout(txnId, route.homeKey()));
        }

        @Override
        public void onCallbackFailure(Id from, Throwable failure)
        {
            tryFailure(failure);
        }
    }

    AsyncResult<Object> awaitCommits(Node node, Deps waitOn)
    {
        AtomicInteger remaining = new AtomicInteger(waitOn.txnIdCount());
        AsyncResult.Settable<Object> result = AsyncResults.settable();
        for (int i = 0 ; i < waitOn.txnIdCount() ; ++i)
        {
            TxnId txnId = waitOn.txnId(i);
            new AwaitCommit(node, txnId, waitOn.participants(txnId)).addCallback((success, failure) -> {
                if (result.isDone())
                    return;
                if (success != null && remaining.decrementAndGet() == 0)
                    result.setSuccess(success);
                else
                    result.tryFailure(failure);
            });
        }
        return result;
    }

    private final CoordinationAdapter<Result> adapter;
    private final Node node;
    private final Ballot ballot;
    private final TxnId txnId;
    private final Txn txn;
    private final FullRoute<?> route;
    private final BiConsumer<Outcome, Throwable> callback;
    private boolean isDone;

    private final List<RecoverOk> recoverOks = new ArrayList<>();
    private final RecoveryTracker tracker;
    private boolean isBallotPromised;
    private final Map<Id, RecoverReply> debug = debug() ? new HashMap<>() : null;

    private Recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback, Topologies topologies)
    {
        Invariants.checkState(txnId.kind().isGloballyVisible());
        // TODO (required, correctness): we may have to contact all epochs to ensure we spot any future transaction that might not have taken us as dependency?
        //    or we need an exclusive sync point covering us and closing out the old epoch before recovering;
        //    or we need to manage dependencies for ranges we don't own in future epochs; this might be simplest
        this.adapter = node.coordinationAdapter(txnId, InitiateRecovery);
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.callback = callback;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch();
        this.tracker = new RecoveryTracker(topologies);
    }

    @Override
    public void accept(Result result, Throwable failure)
    {
        isDone = true;
        if (failure == null)
        {
            callback.accept(ProgressToken.APPLIED, null);
            node.agent().metricsEventsListener().onRecover(txnId, ballot);
        }
        else
        {
            callback.accept(null, failure);
            if (failure instanceof Preempted)
                node.agent().metricsEventsListener().onPreempted(txnId);
            else if (failure instanceof Timeout)
                node.agent().metricsEventsListener().onTimeout(txnId);
            else if (failure instanceof Invalidated)
                node.agent().metricsEventsListener().onInvalidated(txnId);
        }

        node.agent().onRecover(node, result, failure);
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, txnId, txn, route, callback, node.topology().forEpoch(route, txnId.epoch()));
    }

    private static Recover recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback, Topologies topologies)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        return recover(node, ballot, txnId, txn, route, callback, topologies);
    }

    private static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, ballot, txnId, txn, route, callback, node.topology().forEpoch(route, txnId.epoch()));
    }

    private static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback, Topologies topologies)
    {
        Recover recover = new Recover(node, ballot, txnId, txn, route, callback, topologies);
        recover.start(topologies.nodes());
        return recover;
    }

    void start(Set<Id> nodes)
    {
        node.send(nodes, to -> new BeginRecovery(to, tracker.topologies(), txnId, txn, route, ballot), this);
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply reply)
    {
        if (isDone || isBallotPromised)
            return;

        if (debug != null) debug.put(from, reply);

        if (!reply.isOk())
        {
            accept(null, new Preempted(txnId, route.homeKey()));
            return;
        }

        RecoverOk ok = (RecoverOk) reply;
        recoverOks.add(ok);
        boolean fastPath = ok.executeAt.compareTo(txnId) == 0;
        if (tracker.recordSuccess(from, fastPath) == Success)
            recover();
    }

    private void recover()
    {
        Invariants.checkState(!isBallotPromised);
        isBallotPromised = true;

        // first look for the most recent Accept (or later); if present, go straight to proposing it again
        RecoverOk acceptOrCommit = maxAcceptedOrLater(recoverOks);
        if (acceptOrCommit != null)
        {
            Timestamp executeAt = acceptOrCommit.executeAt;
            switch (acceptOrCommit.status)
            {
                default: throw illegalState("Unknown status: " + acceptOrCommit.status);
                case Truncated:
                    callback.accept(ProgressToken.TRUNCATED, null);
                    return;

                case Invalidated:
                {
                    commitInvalidate();
                    return;
                }

                case Applied:
                case PreApplied:
                {
                    withCommittedDeps(executeAt, stableDeps -> {
                        // TODO (future development correctness): when writes/result are partially replicated, need to confirm we have quorum of these
                        persist(adapter, node, tracker.topologies(), route, txnId, txn, executeAt, stableDeps, acceptOrCommit.writes, acceptOrCommit.result, null);
                    });
                    accept(acceptOrCommit.result, null);
                    return;
                }

                case ReadyToExecute:
                case Stable:
                {
                    withCommittedDeps(executeAt, stableDeps -> {
                        execute(adapter, node, tracker.topologies(), route, RECOVER, txnId, txn, executeAt, stableDeps, this);
                    });
                    return;
                }

                case PreCommitted:
                case Committed:
                {
                    withCommittedDeps(executeAt, committedDeps -> {
                        stabilise(adapter, node, tracker.topologies(), route, ballot, txnId, txn, executeAt, committedDeps, this);
                    });
                    return;
                }

                case Accepted:
                {
                    // TODO (desired, behaviour): if we didn't find Accepted in *every* shard, consider invalidating for consistency of behaviour
                    //     however, note that we may have taken the fast path and recovered, so we can only do this if acceptedOrCommitted=Ballot.ZERO
                    //     (otherwise recovery was attempted and did not invalidate, so it must have determined it needed to complete)
                    Deps proposeDeps = LatestDeps.mergeProposal(recoverOks, ok -> ok.deps);
                    propose(acceptOrCommit.executeAt, proposeDeps);
                    return;
                }

                case AcceptedInvalidate:
                {
                    invalidate();
                    return;
                }

                case NotDefined:
                case PreAccepted:
                    throw illegalState("Should only be possible to have Accepted or later commands");
            }
        }

        if (tracker.rejectsFastPath() || recoverOks.stream().anyMatch(ok -> ok.rejectsFastPath))
        {
            invalidate();
            return;
        }

        // should all be PreAccept
        Deps proposeDeps = LatestDeps.mergeProposal(recoverOks, ok -> ok.deps);
        Deps earlierAcceptedNoWitness = Deps.merge(recoverOks, ok -> ok.earlierAcceptedNoWitness);
        Deps earlierCommittedWitness = Deps.merge(recoverOks, ok -> ok.earlierCommittedWitness);
        earlierAcceptedNoWitness = earlierAcceptedNoWitness.without(earlierCommittedWitness::contains);
        if (!earlierAcceptedNoWitness.isEmpty())
        {
            // If there exist commands that were proposed an earlier execution time than us that have not witnessed us,
            // we have to be certain these commands have not successfully committed without witnessing us (thereby
            // ruling out a fast path decision for us and changing our recovery decision).
            // So, we wait for these commands to finish committing before retrying recovery.
            // See whitepaper for more details
            awaitCommits(node, earlierAcceptedNoWitness).addCallback((success, failure) -> {
                if (failure != null) accept(null, failure);
                else retry();
            });
            return;
        }
        // TODO (required): if there are epochs before txnId.epoch that haven't propagated their dependencies we potentially have a problem
        //                  as we may propose deps that don't witness a transaction they should witness.
        //                  in this case we should run GetDeps before proposing
        propose(txnId, proposeDeps);
    }

    private void withCommittedDeps(Timestamp executeAt, Consumer<Deps> withDeps)
    {
        LatestDeps.MergedCommitResult merged = LatestDeps.mergeCommit(txnId, executeAt, recoverOks, ok -> ok.deps);
        node.withEpoch(executeAt.epoch(), () -> {
            Seekables<?, ?> missing = txn.keys().subtract(merged.sufficientFor);
            if (missing.isEmpty())
            {
                withDeps.accept(merged.deps);
            }
            else
            {
                CollectDeps.withDeps(node, txnId, missing.toRoute(route.homeKey()), missing, executeAt, (extraDeps, fail) -> {
                    if (fail != null) node.agent().onHandledException(fail);
                    else withDeps.accept(merged.deps.with(extraDeps));
                });
            }
        });
    }

    private void invalidate()
    {
        proposeInvalidate(node, ballot, txnId, route.someParticipatingKey(), (success, fail) -> {
            if (fail != null) accept(null, fail);
            else commitInvalidate();
        });
    }

    private void commitInvalidate()
    {
        // If not accepted then the executeAt is not consistent cross the peers and likely different on every node.  There is also an edge case
        // when ranges are removed from the topology, during this case the executeAt won't know the ranges and the invalidate commit will fail.
        Timestamp invalidateUntil = recoverOks.stream().map(ok -> ok.status.hasBeen(Status.Accepted) ? ok.executeAt : ok.txnId).reduce(txnId, Timestamp::max);
        node.withEpoch(invalidateUntil.epoch(), () -> Commit.Invalidate.commitInvalidate(node, txnId, route, invalidateUntil));
        isDone = true;
        locallyInvalidateAndCallback(node, txnId, route, ProgressToken.INVALIDATED, callback);
    }

    private void propose(Timestamp executeAt, Deps deps)
    {
        node.withEpoch(executeAt.epoch(), () -> Invoke.propose(adapter, node, route, ballot, txnId, txn, executeAt, deps, this));
    }

    private void retry()
    {
        Recover.recover(node, ballot, txnId, txn, route, callback, tracker.topologies());
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (tracker.recordFailure(from) == Failed)
            accept(null, new Timeout(txnId, route.homeKey()));
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        accept(null, failure);
        node.agent().onUncaughtException(failure);
    }
}
