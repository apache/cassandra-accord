package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

import static accord.local.Status.Accepted;

// TODO: rename to Recover (verb); rename Recover message to not clash
class Recover extends AcceptPhase implements Callback<RecoverReply>
{
    class RetryAfterCommits implements Callback<WaitOnCommitOk>
    {
        final QuorumTracker retryTracker;
        final Topologies topologies;

        RetryAfterCommits(Dependencies waitOn)
        {
            topologies = node.topology().forTxn(txn);
            retryTracker = new QuorumTracker(topologies);
            for (Map.Entry<TxnId, Txn> e : waitOn)
                node.send(topologies.nodes(), to -> new WaitOnCommit(to, topologies, e.getKey(), e.getValue().keys()), this);
        }

        @Override
        public void onSuccess(Id from, WaitOnCommitOk response)
        {
            synchronized (Recover.this)
            {
                if (isDone() || retryTracker.hasReachedQuorum())
                    return;

                retryTracker.recordSuccess(from);

                if (retryTracker.hasReachedQuorum())
                {
                    new Recover(node, ballot, txnId, txn, topologies).handle((success, failure) -> {
                        if (success != null) complete(success);
                        else completeExceptionally(failure);
                        return null;
                    });
                }
            }
        }

        @Override
        public void onFailure(Id from, Throwable throwable)
        {
            synchronized (Recover.this)
            {
                if (isDone())
                    return;

                retryTracker.recordFailure(from);
                if (retryTracker.hasFailed())
                    completeExceptionally(new Timeout());
            }
        }
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

    public Recover(Node node, Ballot ballot, TxnId txnId, Txn txn)
    {
        // TODO (review): I'm a little concerned that this differs from PreAccept, as I believe its correctness
        //                depends on hitting earlier epochs to ensure all dependencies are caught. Since we
        //                are always performing Accept (or later) operations here to collect dependencies that issue
        //                is probably avoided.
        //                However, how does this process handle collecting the superseding/wait sets in the case where the
        //                voters are no longer replicas?
        this(node, ballot, txnId, txn, node.topology().forEpoch(txn, txnId.epoch));
    }

    private Recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Topologies topologies)
    {
        super(node, ballot, txnId, txn);
        this.tracker = new FastPathTracker<>(topologies, ShardTracker[]::new, ShardTracker::new);
        node.send(tracker.nodes(), to -> new BeginRecovery(to, topologies, txnId, txn, ballot), this);
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply response)
    {
        if (isDone() || tracker.hasReachedQuorum())
            return;

        if (!response.isOK())
        {
            completeExceptionally(new Preempted());
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
        RecoverOk acceptOrCommit = null;
        for (RecoverOk ok : recoverOks)
        {
            if (ok.status.compareTo(Accepted) >= 0)
            {
                if (acceptOrCommit == null) acceptOrCommit = ok;
                else if (acceptOrCommit.status.compareTo(ok.status) < 0) acceptOrCommit = ok;
                else if (acceptOrCommit.status == ok.status && acceptOrCommit.accepted.compareTo(ok.accepted) < 0) acceptOrCommit = ok;
            }
        }

        if (acceptOrCommit != null)
        {
            switch (acceptOrCommit.status)
            {
                case Accepted:
                    // TODO (review): for correct recovery (if recovery fails) this _must_ include txnId.epoch, which this seems not ... so question is why it works now...
                    //                (should be the Topology for txnId.epoch and executeAt.epoch, plus any trailing ones)
                    startAccept(acceptOrCommit.executeAt, acceptOrCommit.deps, node.topology().forTxn(txn));
                    return;
                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                    // TODO (review): should be the Topology for txnId.epoch and executeAt.epoch
                    complete(new Agreed(txnId, txn, acceptOrCommit.executeAt, acceptOrCommit.deps, node.topology().forTxn(txn), acceptOrCommit.writes, acceptOrCommit.result));
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
                new RetryAfterCommits(earlierCommittedWitness);
                return;
            }
            executeAt = txnId;
        }

        // TODO (review): should be the Topology for txnId.epoch and executeAt.epoch
        startAccept(executeAt, deps, node.topology().forTxn(txn));
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (isDone())
            return;

        tracker.recordFailure(from);
        if (tracker.hasFailed())
            completeExceptionally(new Timeout());
    }
}
