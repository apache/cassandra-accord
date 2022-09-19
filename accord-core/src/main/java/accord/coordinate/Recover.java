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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import accord.primitives.*;
import accord.messages.Commit;
import com.google.common.base.Preconditions;

import accord.api.Result;
import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.WaitOnCommit;
import accord.messages.WaitOnCommit.WaitOnCommitOk;
import accord.topology.Topology;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedOrLater;
import static accord.messages.Commit.Invalidate.commitInvalidate;

// TODO: rename to Recover (verb); rename Recover message to not clash
public class Recover implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    class AwaitCommit extends AsyncFuture<Timestamp> implements Callback<WaitOnCommitOk>
    {
        // TODO: this should collect the executeAt of any commit, and terminate as soon as one is found
        //       that is earlier than TxnId for the Txn we are recovering; if all commits we wait for
        //       are given earlier timestamps we can retry without restarting.
        final QuorumTracker tracker;

        AwaitCommit(Node node, TxnId txnId, RoutingKeys someKeys)
        {
            Topology topology = node.topology().globalForEpoch(txnId.epoch).forKeys(someKeys);
            this.tracker = new QuorumTracker(topology);
            node.send(topology.nodes(), to -> new WaitOnCommit(to, topology, txnId, someKeys), this);
        }

        @Override
        public synchronized void onSuccess(Id from, WaitOnCommitOk reply)
        {
            if (isDone()) return;

            if (tracker.success(from))
                trySuccess(null);
        }

        @Override
        public synchronized void onFailure(Id from, Throwable failure)
        {
            if (isDone()) return;

            if (tracker.failure(from))
                tryFailure(new Timeout(txnId, route.homeKey));
        }

        @Override
        public void onCallbackFailure(Id from, Throwable failure)
        {
            tryFailure(failure);
        }
    }

    Future<Object> awaitCommits(Node node, Deps waitOn)
    {
        AtomicInteger remaining = new AtomicInteger(waitOn.txnIdCount());
        Promise<Object> future = new AsyncPromise<>();
        for (int i = 0 ; i < waitOn.txnIdCount() ; ++i)
        {
            TxnId txnId = waitOn.txnId(i);
            new AwaitCommit(node, txnId, waitOn.someRoutingKeys(txnId)).addCallback((success, failure) -> {
                if (future.isDone())
                    return;
                if (success != null && remaining.decrementAndGet() == 0)
                    future.setSuccess(success);
                else
                    future.tryFailure(failure);
            });
        }
        return future;
    }

    // TODO: not sure it makes sense to extend FastPathTracker, as intent here is a bit different
    static class ShardTracker extends FastPathTracker.FastPathShardTracker
    {
        int responsesFromElectorate;
        public ShardTracker(Shard shard)
        {
            super(shard);
        }

        @Override
        public boolean includeInFastPath(Node.Id node, boolean withFastPathTimestamp)
        {
            if (!shard.fastPathElectorate.contains(node))
                return false;

            ++responsesFromElectorate;
            return withFastPathTimestamp;
        }

        @Override
        public boolean hasMetFastPathCriteria()
        {
            int fastPathRejections = responsesFromElectorate - fastPathAccepts;
            return fastPathRejections <= shard.fastPathElectorate.size() - shard.fastPathQuorumSize;
        }
    }

    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final Route route;
    final BiConsumer<Outcome, Throwable> callback;
    private boolean isDone;

    final List<RecoverOk> recoverOks = new ArrayList<>();
    final FastPathTracker<ShardTracker> tracker;

    private Recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Route route, BiConsumer<Outcome, Throwable> callback, Topologies topologies)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.callback = callback;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch;
        this.tracker = new FastPathTracker<>(topologies, ShardTracker[]::new, ShardTracker::new);
    }

    @Override
    public void accept(Result result, Throwable failure)
    {
        isDone = true;
        if (failure == null) callback.accept(ProgressToken.APPLIED, null);
        else callback.accept(null, failure);
        node.agent().onRecover(node, result, failure);
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, Route route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, txnId, txn, route, callback, node.topology().forEpoch(route, txnId.epoch));
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, Route route, BiConsumer<Outcome, Throwable> callback, Topologies topologies)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        return recover(node, ballot, txnId, txn, route, callback, topologies);
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Route route, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, ballot, txnId, txn, route, callback, node.topology().forEpoch(route, txnId.epoch));
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Route route, BiConsumer<Outcome, Throwable> callback, Topologies topologies)
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
        if (isDone || tracker.hasReachedQuorum())
            return;

        if (!reply.isOk())
        {
            accept(null, new Preempted(txnId, route.homeKey));
            return;
        }

        RecoverOk ok = (RecoverOk) reply;
        recoverOks.add(ok);
        boolean fastPath = ok.executeAt.compareTo(txnId) == 0;
        tracker.recordSuccess(from, fastPath);

        if (tracker.hasReachedQuorum())
            recover();
    }

    private void recover()
    {
        // first look for the most recent Accept; if present, go straight to proposing it again
        RecoverOk acceptOrCommit = maxAcceptedOrLater(recoverOks);
        if (acceptOrCommit != null)
        {
            Timestamp executeAt = acceptOrCommit.executeAt;
            switch (acceptOrCommit.status)
            {
                default: throw new IllegalStateException();
                case Invalidated:
                    commitInvalidate();
                    return;

                case Applied:
                case PreApplied:
                    // TODO: in some cases we can use the deps we already have (e.g. if we have a quorum of Committed responses)
                    node.withEpoch(executeAt.epoch, () -> {
                        CollectDeps.withDeps(node, txnId, route, txn, acceptOrCommit.executeAt, (deps, fail) -> {
                            if (fail != null)
                            {
                                accept(null, fail);
                            }
                            else
                            {
                                // TODO: when writes/result are partially replicated, need to confirm we have quorum of these
                                Persist.persistAndCommit(node, txnId, route, txn, executeAt, deps, acceptOrCommit.writes, acceptOrCommit.result);
                                accept(acceptOrCommit.result, null);
                            }
                        });
                    });
                    return;

                case ReadyToExecute:
                case Committed:
                    // TODO: in some cases we can use the deps we already have (e.g. if we have a quorum of Committed responses)
                    node.withEpoch(executeAt.epoch, () -> {
                        CollectDeps.withDeps(node, txnId, route, txn, executeAt, (deps, fail) -> {
                            if (fail != null) accept(null, fail);
                            else Execute.execute(node, txnId, txn, route, acceptOrCommit.executeAt, deps, this);
                        });
                    });
                    return;

                case Accepted:
                    // no need to preaccept the later round, as future operations always include every old epoch (until it is fully migrated)
                    propose(acceptOrCommit.executeAt, mergeDeps());
                    return;

                case AcceptedInvalidate:
                    invalidate();
                    return;

                case NotWitnessed:
                case PreAccepted:
                    throw new IllegalStateException("Should only be possible to have Accepted or later commands");
            }
        }

        if (!tracker.hasMetFastPathCriteria())
        {
            invalidate();
            return;
        }

        for (RecoverOk ok : recoverOks)
        {
            if (ok.rejectsFastPath)
            {
                invalidate();
                return;
            }
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
            // TODO: check paper: do we assume that witnessing in PreAccept implies witnessing in Accept? Not guaranteed.
            // See whitepaper for more details
            awaitCommits(node, earlierAcceptedNoWitness).addCallback((success, failure) -> {
                if (failure != null) accept(null, failure);
                else retry();
            });
            return;
        }
        propose(txnId, deps);
    }

    private void invalidate()
    {
        proposeInvalidate(node, ballot, txnId, route.homeKey, (success, fail) -> {
            if (fail != null) accept(null, fail);
            else commitInvalidate();
        });
    }

    private void commitInvalidate()
    {
        Timestamp invalidateUntil = recoverOks.stream().map(ok -> ok.executeAt).reduce(txnId, Timestamp::max);
        node.withEpoch(invalidateUntil.epoch, () -> Commit.Invalidate.commitInvalidate(node, txnId, route, invalidateUntil));
        isDone = true;
        callback.accept(ProgressToken.INVALIDATED, null);
    }

    private void propose(Timestamp executeAt, Deps deps)
    {
        node.withEpoch(executeAt.epoch, () -> Propose.propose(node, ballot, txnId, txn, route, executeAt, deps, this));
    }

    private Deps mergeDeps()
    {
        KeyRanges ranges = recoverOks.stream().map(r -> r.deps.covering).reduce(KeyRanges::union).orElseThrow(NoSuchElementException::new);
        Preconditions.checkState(ranges.containsAll(txn.keys()));
        return Deps.merge(recoverOks, r -> r.deps);
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

        if (tracker.failure(from))
            accept(null, new Timeout(txnId, route.homeKey));
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        accept(null, failure);
        node.agent().onUncaughtException(failure);
    }
}
