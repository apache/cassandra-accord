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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.messages.Commit;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.primitives.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.Deps;
import accord.txn.Txn;
import accord.primitives.TxnId;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.WaitOnCommit;
import accord.messages.WaitOnCommit.WaitOnCommitOk;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import static accord.coordinate.Propose.Invalidate.proposeInvalidate;
import static accord.messages.BeginRecovery.RecoverOk.maxAcceptedOrLater;

// TODO: rename to Recover (verb); rename Recover message to not clash
public class Recover extends AsyncFuture<Result> implements Callback<RecoverReply>, BiConsumer<Result, Throwable>
{
    class AwaitCommit extends AsyncFuture<Timestamp> implements Callback<WaitOnCommitOk>
    {
        // TODO: this should collect the executeAt of any commit, and terminate as soon as one is found
        //       that is earlier than TxnId for the Txn we are recovering; if all commits we wait for
        //       are given earlier timestamps we can retry without restarting.
        final QuorumTracker tracker;

        AwaitCommit(Node node, TxnId txnId, Keys someKeys)
        {
            Topologies topologies = node.topology().preciseEpochs(someKeys, txnId.epoch);
            this.tracker = new QuorumTracker(topologies);
            node.send(topologies.nodes(), to -> new WaitOnCommit(to, topologies, txnId, someKeys), this);
        }

        @Override
        public synchronized void onSuccess(Id from, WaitOnCommitOk response)
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
                tryFailure(new Timeout(txnId, homeKey));
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
            new AwaitCommit(node, txnId, waitOn.someKeys(txnId)).addCallback((success, failure) -> {
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
    final Key homeKey;

    final List<RecoverOk> recoverOks = new ArrayList<>();
    final FastPathTracker<ShardTracker> tracker;

    public Recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey)
    {
        this(node, ballot, txnId, txn, homeKey, node.topology().forEpoch(txn, txnId.epoch));
    }

    private Recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey, Topologies topologies)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch;
        this.tracker = new FastPathTracker<>(topologies, ShardTracker[]::new, ShardTracker::new);
    }

    @Override
    public void accept(Result result, Throwable failure)
    {
        if (result != null) trySuccess(result);
        else tryFailure(failure);
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, Key homeKey)
    {
        return recover(node, txnId, txn, homeKey, node.topology().forEpoch(txn, txnId.epoch));
    }

    public static Recover recover(Node node, TxnId txnId, Txn txn, Key homeKey, Topologies topologies)
    {
        Ballot ballot = new Ballot(node.uniqueNow());
        return recover(node, ballot, txnId, txn, homeKey, topologies);
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey)
    {
        return recover(node, ballot, txnId, txn, homeKey, node.topology().forEpoch(txn, txnId.epoch));
    }

    public static Recover recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey, Topologies topologies)
    {
        Recover recover = new Recover(node, ballot, txnId, txn, homeKey, topologies);
        recover.start(topologies.nodes());
        return recover;
    }

    void start(Set<Id> nodes)
    {
        node.send(nodes, to -> new BeginRecovery(to, tracker.topologies(), txnId, txn, homeKey, ballot), this);
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply response)
    {
        if (isDone() || tracker.hasReachedQuorum())
            return;

        if (!response.isOK())
        {
            tryFailure(new Preempted(txnId, homeKey));
            return;
        }

        try
        {
            RecoverOk ok = (RecoverOk) response;
            recoverOks.add(ok);
            boolean fastPath = ok.executeAt.compareTo(txnId) == 0;
            tracker.recordSuccess(from, fastPath);

            if (tracker.hasReachedQuorum())
                recover();
        }
        catch (Throwable t)
        {
            tryFailure(t);
            node.agent().onUncaughtException(t);
        }
    }

    private void recover()
    {
        // first look for the most recent Accept; if present, go straight to proposing it again
        RecoverOk acceptOrCommit = maxAcceptedOrLater(recoverOks);
        if (acceptOrCommit != null)
        {
            switch (acceptOrCommit.status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                    throw new IllegalStateException("Should only be possible to have Accepted or later commands");
                case Accepted:
                    // no need to preaccept the later round, as future operations always include every old epoch (until it is fully migrated)
                    node.withEpoch(acceptOrCommit.executeAt.epoch, () -> {
                        Propose.propose(node, ballot, txnId, txn, homeKey, acceptOrCommit.executeAt, acceptOrCommit.deps, this);
                    });
                    return;
                case AcceptedInvalidate:
                    proposeInvalidate(node, ballot, txnId, homeKey).addCallback((success, fail) -> {
                        if (fail != null) tryFailure(fail);
                        else
                        {
                            tryFailure(new Invalidated());
                            Commit.commitInvalidate(node, txnId, txn.keys(), recoverOks.stream().map(ok -> ok.executeAt).reduce(txnId, Timestamp::max));
                        }
                    });
                    return;
                case Committed:
                case ReadyToExecute:
                    Execute.execute(node, txnId, txn, homeKey, acceptOrCommit.executeAt, acceptOrCommit.deps, this);
                    return;
                case Executed:
                case Applied:
                    Persist.persistAndCommit(node, txnId, homeKey, txn, acceptOrCommit.executeAt, acceptOrCommit.deps, acceptOrCommit.writes, acceptOrCommit.result);
                    trySuccess(acceptOrCommit.result);
                    return;
                case Invalidated:
                    Timestamp invalidateUntil = recoverOks.stream().map(ok -> ok.executeAt).reduce(txnId, Timestamp::max);
                    node.withEpoch(invalidateUntil.epoch, () -> {
                        Commit.commitInvalidate(node, txnId, txn.keys(), invalidateUntil);
                    });
                    tryFailure(new Invalidated());
                    return;
            }
        }

        // should all be PreAccept
        Deps deps = Deps.merge(recoverOks, ok -> ok.deps);
        Deps earlierAcceptedNoWitness = Deps.merge(recoverOks, ok -> ok.earlierAcceptedNoWitness);
        Deps earlierCommittedWitness = Deps.merge(recoverOks, ok -> ok.earlierCommittedWitness);
        Timestamp maxExecuteAt = txnId;
        boolean rejectsFastPath = false;
        for (RecoverOk ok : recoverOks)
        {
            maxExecuteAt = Timestamp.max(maxExecuteAt, ok.executeAt);
            rejectsFastPath |= ok.rejectsFastPath;
        }

        Timestamp executeAt;
        if (rejectsFastPath || !tracker.hasMetFastPathCriteria())
        {
            executeAt = maxExecuteAt;
        }
        else
        {
            earlierAcceptedNoWitness.without(earlierCommittedWitness::contains);
            if (!earlierAcceptedNoWitness.isEmpty())
            {
                awaitCommits(node, earlierAcceptedNoWitness).addCallback((success, failure) -> {
                    if (failure != null) tryFailure(failure);
                    else retry();
                });
                return;
            }
            executeAt = txnId;
        }

        node.withEpoch(executeAt.epoch, () -> {
            Propose.propose(node, ballot, txnId, txn, homeKey, executeAt, deps, this);
        });
    }

    private void retry()
    {
        Recover.recover(node, ballot, txnId, txn, homeKey, tracker.topologies()).addCallback((success, failure) -> {
            if (success != null) trySuccess(success);
            else tryFailure(failure);
        });
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (isDone())
            return;

        if (tracker.failure(from))
            tryFailure(new Timeout(txnId, homeKey));
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        tryFailure(failure);
    }
}
