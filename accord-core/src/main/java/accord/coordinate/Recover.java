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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RecoveryTracker;
import accord.local.Node;
import accord.local.Node.Id;
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
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.coordinate.ProposeAndExecute.proposeAndExecute;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.local.Status.Committed;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedOrLater;
import static accord.utils.Invariants.debug;

// TODO (low priority, cleanup): rename to Recover (verb); rename Recover message to not clash
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
        if (failure == null) callback.accept(ProgressToken.APPLIED, null);
        else callback.accept(null, failure);
        node.agent().onRecover(node, result, failure);
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, txnId, txn, route, callback, node.topology().forEpoch(route, txnId.epoch()));
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback, Topologies topologies)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        return recover(node, ballot, txnId, txn, route, callback, topologies);
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, ballot, txnId, txn, route, callback, node.topology().forEpoch(route, txnId.epoch()));
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, BiConsumer<Outcome, Throwable> callback, Topologies topologies)
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
                default: throw new IllegalStateException("Unknown status: " + acceptOrCommit.status);
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
                    Deps committedDeps = tryMergeCommittedDeps();
                    node.withEpoch(executeAt.epoch(), () -> {
                        // TODO (required, consider): when writes/result are partially replicated, need to confirm we have quorum of these
                        if (committedDeps != null) Persist.persistMaximal(node, txnId, route, txn, executeAt, committedDeps, acceptOrCommit.writes, acceptOrCommit.result);
                        else CollectDeps.withDeps(node, txnId, route, txn.keys(), executeAt, (deps, fail) -> {
                            if (fail != null) accept(null, fail);
                            else Persist.persistMaximal(node, txnId, route, txn, executeAt, deps, acceptOrCommit.writes, acceptOrCommit.result);
                        });
                    });
                    accept(acceptOrCommit.result, null);
                    return;
                }

                case ReadyToExecute:
                case PreCommitted:
                case Committed:
                {
                    Deps committedDeps = tryMergeCommittedDeps();
                    node.withEpoch(executeAt.epoch(), () -> {
                        if (committedDeps != null)
                        {
                            Execute.execute(node, txnId, txn, route, executeAt, committedDeps, this);
                        }
                        else
                        {
                            CollectDeps.withDeps(node, txnId, route, txn.keys(), executeAt, (deps, fail) -> {
                                if (fail != null) accept(null, fail);
                                else Execute.execute(node, txnId, txn, route, acceptOrCommit.executeAt, deps, this);
                            });
                        }
                    });
                    return;
                }

                case Accepted:
                {
                    // TODO (required): this is broken given feedback from Gotsman, Sutra and Ryabinin. Every transaction proposes deps.
                    // Today we send Deps for all Accept rounds, however their contract is different for SyncPoint
                    // and regular transactions. The former require that their deps be durably agreed before they
                    // proceed, but this is impossible for a regular transaction that may agree a later executeAt
                    // than proposed. Deps for standard transactions are only used for recovery.
                    // While we could safely behave identically here for them, we expect to stop sending deps in the
                    // Accept round for these transactions as it is not necessary for correctness with some modifications
                    // to recovery.
                    Deps deps;
                    if (txnId.rw().proposesDeps()) deps = tryMergeAcceptedDeps();
                    else deps = mergeDeps();

                    if (deps != null)
                    {
                        // TODO (desired, behaviour): if we didn't find Accepted in *every* shard, consider invalidating for consistency of behaviour
                        propose(acceptOrCommit.executeAt, deps);
                        return;
                    }

                    // if we propose deps, and cannot assemble a complete set, then we never reached consensus and can be invalidated
                }

                case AcceptedInvalidate:
                {
                    invalidate();
                    return;
                }

                case NotDefined:
                case PreAccepted:
                    throw new IllegalStateException("Should only be possible to have Accepted or later commands");
            }
        }

        if (txnId.rw().proposesDeps() || tracker.rejectsFastPath() || recoverOks.stream().anyMatch(ok -> ok.rejectsFastPath))
        {
            invalidate();
            return;
        }

        // should all be PreAccept
        Deps deps = mergeDeps();
        Deps earlierAcceptedNoWitness = Deps.merge(recoverOks, ok -> ok.earlierAcceptedNoWitness);
        Deps earlierCommittedWitness = Deps.merge(recoverOks, ok -> ok.earlierCommittedWitness);
        earlierAcceptedNoWitness = earlierAcceptedNoWitness.without(earlierCommittedWitness::contains);
        if (!earlierAcceptedNoWitness.isEmpty())
        {
            // If there exist commands that were proposed an earlier execution time than us that have not witnessed us,
            // we have to be certain these commands have not successfully committed without witnessing us (thereby
            // ruling out a fast path decision for us and changing our recovery decision).
            // So, we wait for these commands to finish committing before retrying recovery.
            // TODO (required): check paper: do we assume that witnessing in PreAccept implies witnessing in Accept? Not guaranteed.
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
        propose(txnId, deps);
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
        Timestamp invalidateUntil = recoverOks.stream().map(ok -> ok.executeAt).reduce(txnId, Timestamp::max);
        node.withEpoch(invalidateUntil.epoch(), () -> Commit.Invalidate.commitInvalidate(node, txnId, route, invalidateUntil));
        isDone = true;
        locallyInvalidateAndCallback(node, txnId, route, ProgressToken.INVALIDATED, callback);
    }

    private void propose(Timestamp executeAt, Deps deps)
    {
        node.withEpoch(executeAt.epoch(), () -> proposeAndExecute(node, ballot, txnId, txn, route, executeAt, deps, this));
    }

    private Deps mergeDeps()
    {
        Ranges ranges = recoverOks.stream().map(r -> r.deps.covering).reduce(Ranges::with).orElseThrow(NoSuchElementException::new);
        Invariants.checkState(ranges.containsAll(txn.keys()));
        return Deps.merge(recoverOks, r -> r.deps);
    }

    private Deps tryMergeAcceptedDeps()
    {
        Ranges ranges = recoverOks.stream().map(r -> r.acceptedDeps.covering).reduce(Ranges::with).orElseThrow(NoSuchElementException::new);
        if (!ranges.containsAll(txn.keys()))
            return null;
        return Deps.merge(recoverOks, r -> r.acceptedDeps);
    }

    private Deps tryMergeCommittedDeps()
    {
        Ranges ranges = recoverOks.stream()
                                  .filter(r -> r.status.hasBeen(Committed))
                                  .map(r -> r.acceptedDeps.covering)
                                  .reduce(Ranges::with).orElse(null);
        if (ranges == null || !ranges.containsAll(txn.keys()))
            return null;
        return Deps.merge(recoverOks, r -> r.status.hasBeen(Committed) ? r.acceptedDeps : null);
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
