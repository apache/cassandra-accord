package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import accord.api.Key;
import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.txn.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.messages.WaitOnCommit;
import accord.messages.WaitOnCommit.WaitOnCommitOk;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import static accord.local.Status.Accepted;

// TODO: rename to Recover (verb); rename Recover message to not clash
class Recover extends Propose implements Callback<RecoverReply>
{
    class AwaitCommit extends AsyncPromise<Object> implements Callback<WaitOnCommitOk>
    {
        final QuorumTracker tracker;

        AwaitCommit(Node node, TxnId txnId, Txn txn)
        {
            Topologies topologies = node.topology().unsyncForTxn(txn, txnId.epoch);
            this.tracker = new QuorumTracker(topologies);
            node.send(topologies.nodes(), to -> new WaitOnCommit(to, topologies, txnId, txn.keys()), this);
        }

        @Override
        public synchronized void onSuccess(Id from, WaitOnCommitOk response)
        {
            if (isDone()) return;

            if (tracker.success(from))
            {
                new Recover(node, ballot, txnId, txn, homeKey).addCallback((success, failure) -> {
                    if (success != null) trySuccess(success);
                    else tryFailure(failure);
                });
            }
        }

        @Override
        public synchronized void onFailure(Id from, Throwable throwable)
        {
            if (isDone()) return;

            if (tracker.failure(from))
                tryFailure(new Timeout());
        }
    }

    Future<Object> awaitCommits(Node node, Dependencies waitOn)
    {
        AtomicInteger remaining = new AtomicInteger(waitOn.size());
        Promise<Object> future = new AsyncPromise<>();
        for (Map.Entry<TxnId, Txn> e : waitOn)
        {
            new AwaitCommit(node, e.getKey(), e.getValue()).addCallback((success, failure) -> {
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

    final List<RecoverOk> recoverOks = new ArrayList<>();
    final FastPathTracker<ShardTracker> tracker;

    public Recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey)
    {
        this(node, ballot, txnId, txn, homeKey, node.topology().forEpoch(txn, txnId.epoch));
    }

    private Recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey, Topologies topologies)
    {
        super(node, ballot, txnId, txn, homeKey);
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch;
        this.tracker = new FastPathTracker<>(topologies, ShardTracker[]::new, ShardTracker::new);
        node.send(tracker.nodes(), to -> new BeginRecovery(to, topologies, txnId, txn, homeKey, ballot), this);
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply response)
    {
        if (isDone() || tracker.hasReachedQuorum())
            return;

        if (!response.isOK())
        {
            tryFailure(new Preempted());
            return;
        }

        RecoverOk ok = (RecoverOk) response;
        recoverOks.add(ok);
        boolean fastPath = ok.executeAt.compareTo(txnId) == 0;
        tracker.recordSuccess(from, fastPath);

        if (tracker.hasReachedQuorum())
            recover();
    }

    private void recover()
    {
        // first look for the most recent Accept; if present, go straight to proposing it again
        RecoverOk acceptOrCommit; {
            RecoverOk tmp = null;
            for (RecoverOk ok : recoverOks)
            {
                if (ok.status.compareTo(Accepted) >= 0)
                {
                    if (tmp == null) tmp = ok;
                    else if (tmp.status.compareTo(ok.status) < 0) tmp = ok;
                    else if (tmp.status == ok.status && tmp.accepted.compareTo(ok.accepted) < 0) tmp = ok;
                }
            }
            acceptOrCommit = tmp;
        }

        if (acceptOrCommit != null)
        {
            switch (acceptOrCommit.status)
            {
                case Accepted:
                    // TODO: this pattern needs to be given some efficient syntactic sugar
                    if (node.topology().hasEpoch(acceptOrCommit.executeAt.epoch))
                    {
                        Topologies topologies = node.topology().forTxn(txn, txnId.epoch, acceptOrCommit.executeAt.epoch);
                        startAccept(acceptOrCommit.executeAt, acceptOrCommit.deps, topologies);
                    }
                    else
                    {
                        node.configService().fetchTopologyForEpoch(acceptOrCommit.executeAt.epoch);
                        node.topology().awaitEpoch(acceptOrCommit.executeAt.epoch).addListener(() -> {
                            Topologies topologies = node.topology().forTxn(txn, txnId.epoch, acceptOrCommit.executeAt.epoch);
                            startAccept(acceptOrCommit.executeAt, acceptOrCommit.deps, topologies);
                        });
                    }
                    return;
                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                    setSuccess(new Agreed(txnId, txn, homeKey, acceptOrCommit.executeAt, acceptOrCommit.deps, acceptOrCommit.writes, acceptOrCommit.result));
                    return;
            }
        }

        // should all be PreAccept
        Timestamp maxExecuteAt = txnId;
        Dependencies deps = new Dependencies();
        Dependencies earlierAcceptedNoWitness = new Dependencies();
        Dependencies earlierCommittedWitness = new Dependencies();
        boolean rejectsFastPath = false;
        for (RecoverOk ok : recoverOks)
        {
            deps.addAll(ok.deps);
            earlierAcceptedNoWitness.addAll(ok.earlierAcceptedNoWitness);
            earlierCommittedWitness.addAll(ok.earlierCommittedWitness);
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
            earlierAcceptedNoWitness.removeAll(earlierCommittedWitness);
            if (!earlierAcceptedNoWitness.isEmpty())
            {
                awaitCommits(node, earlierAcceptedNoWitness).addCallback((success, failure) -> {
                    if (success != null) retry();
                    else tryFailure(new Timeout());
                });
                return;
            }
            executeAt = txnId;
        }

        if (!node.topology().hasEpoch(executeAt.epoch))
        {
            node.configService().fetchTopologyForEpoch(executeAt.epoch);
            node.topology().awaitEpoch(executeAt.epoch).addListener(() -> {
                startAccept(executeAt, deps, node.topology().forTxn(txn, txnId, executeAt));
            });
        }
        else
        {
            startAccept(executeAt, deps, node.topology().forTxn(txn, txnId, executeAt));
        }
    }

    private void retry()
    {
        new Recover(node, ballot, txnId, txn, homeKey, tracker.topologies()).addCallback((success, failure) -> {
            if (success != null) setSuccess(success);
            else tryFailure(failure);
        });
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (isDone())
            return;

        if (tracker.failure(from))
            tryFailure(new Timeout());
    }
}
