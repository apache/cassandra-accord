package accord.coordinate;

import accord.api.Key;
import accord.local.Command;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Shard;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.local.Status.Executed;
import static accord.local.Status.NotWitnessed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnCommitted extends CheckShardStatus
{
    CheckOnCommitted(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, long homeEpoch)
    {
        super(node, txnId, txn, homeKey, homeShard, homeEpoch, IncludeInfo.all());
    }

    public static CheckOnCommitted checkOnCommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch)
    {
        CheckOnCommitted checkOnCommitted = new CheckOnCommitted(node, txnId, txn, someKey, someShard, shardEpoch);
        checkOnCommitted.start();
        return checkOnCommitted;
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum() || hasApplied();
    }

    public boolean hasApplied()
    {
        return max != null && max.status.compareTo(Executed) >= 0;
    }

    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull max)
    {
        switch (max.status)
        {
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
                return;
        }

        Key progressKey = node.trySelectProgressKey(txnId, txn.keys, max.homeKey);
        switch (max.status)
        {
            default: throw new IllegalStateException();
            case Executed:
            case Applied:
                node.forEachLocalSince(txn.keys, max.executeAt.epoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.apply(txn, max.homeKey, progressKey, max.executeAt, max.deps, max.writes, max.result);
                });
                node.forEachLocal(txn.keys, txnId.epoch, max.executeAt.epoch - 1, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commit(txn, max.homeKey, progressKey, max.executeAt, max.deps);
                });
                break;
            case Committed:
            case ReadyToExecute:
                node.forEachLocalSince(txn.keys, txnId.epoch, commandStore -> {
                    Command command = commandStore.command(txnId);
                    command.commit(txn, max.homeKey, progressKey, max.executeAt, max.deps);
                });
        }
    }

    @Override
    void onSuccessCriteriaOrExhaustion()
    {
        try
        {
            onSuccessCriteriaOrExhaustion((CheckStatusOkFull) max);
        }
        catch (Throwable t)
        {
            trySuccess(max);
            throw t;
        }
        trySuccess(max);
    }
}
