package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import accord.messages.Preempted;
import accord.txn.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Shards;
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
        final int[] failures;
        final int[] commits;
        int commitQuorums;

        RetryAfterCommits(Dependencies waitOn)
        {
            commits = new int[waitOn.size()];
            failures = new int[waitOn.size()];
            for (Map.Entry<TxnId, Txn> e : waitOn)
                node.send(shards, new WaitOnCommit(e.getKey(), e.getValue().keys()), this);
        }

        @Override
        public void onSuccess(Id from, WaitOnCommitOk response)
        {
            synchronized (Recover.this)
            {
                if (isDone() || commitQuorums == commits.length)
                    return;

                shards.forEachOn(from, (i, shard) -> {
                    if (++commits[i] == shard.slowPathQuorumSize)
                        ++commitQuorums;
                });

                if (commitQuorums == commits.length)
                {
                    new Recover(node, ballot, txnId, txn, shards).handle((success, failure) -> {
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

                shards.forEachOn(from, (i, shard) -> {
                    if (++failures[i] >= shard.slowPathQuorumSize)
                        completeExceptionally(new accord.messages.Timeout());
                });
            }
        }
    }

    final List<RecoverOk> recoverOks = new ArrayList<>();
    int[] failure;
    int[] recovery;
    int[] recoveryWithFastPath;
    int recoveryWithFastPathQuorums = 0;
    int recoveryQuorums = 0;

    public Recover(Node node, Ballot ballot, TxnId txnId, Txn txn)
    {
        this(node, ballot, txnId, txn, node.cluster().forKeys(txn.keys()));
    }

    private Recover(Node node, Ballot ballot, TxnId txnId, Txn txn, Shards shards)
    {
        super(node, ballot, txnId, txn, shards);
        this.failure = new int[this.shards.size()];
        this.recovery = new int[this.shards.size()];
        this.recoveryWithFastPath = new int[this.shards.size()];
        node.send(this.shards, new BeginRecovery(txnId, txn, ballot), this);
    }

    @Override
    public synchronized void onSuccess(Id from, RecoverReply response)
    {
        if (isDone() || recoveryQuorums == shards.size())
            return;

        if (!response.isOK())
        {
            completeExceptionally(new Preempted());
            return;
        }

        RecoverOk ok = (RecoverOk) response;
        recoverOks.add(ok);
        boolean fastPath = ok.executeAt.compareTo(txnId) == 0;
        shards.forEachOn(from, (i, shard) -> {
            if (fastPath && ++recoveryWithFastPath[i] == shard.recoveryFastPathSize)
                ++recoveryWithFastPathQuorums;

            if (++recovery[i] == shard.slowPathQuorumSize)
                ++recoveryQuorums;
        });

        if (recoveryQuorums == shards.size())
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
                    startAccept(acceptOrCommit.executeAt, acceptOrCommit.deps);
                    return;
                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                    complete(new Agreed(txnId, txn, acceptOrCommit.executeAt, acceptOrCommit.deps, shards, acceptOrCommit.writes, acceptOrCommit.result));
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
        if (rejectsFastPath || recoveryWithFastPathQuorums < shards.size())
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

        startAccept(executeAt, deps);
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (isDone())
            return;

        shards.forEachOn(from, (i, shard) -> {
            if (++failure[i] >= shard.slowPathQuorumSize)
                completeExceptionally(new accord.messages.Timeout());
        });
    }
}
