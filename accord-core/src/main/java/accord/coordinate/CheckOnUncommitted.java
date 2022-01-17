package accord.coordinate;

import accord.api.Key;
import accord.local.Command;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.topology.Shard;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.local.Status.Committed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnUncommitted extends CheckOnCommitted
{
    CheckOnUncommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch)
    {
        super(node, txnId, txn, someKey, someShard, shardEpoch);
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, txn, someKey, someShard, shardEpoch);
        checkOnUncommitted.start();
        return checkOnUncommitted;
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum() || hasCommitted();
    }

    public boolean hasCommitted()
    {
        return max != null && max.status.compareTo(Committed) >= 0;
    }

    @Override
    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.status)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
                break;
            case PreAccepted:
            case Accepted:
                node.forEachLocalSince(txn.keys, txnId.epoch, commandStore -> {
                    Command command = commandStore.ifPresent(txnId);
                    if (command != null)
                        command.homeKey(full.homeKey);
                });
                break;
            case Executed:
            case Applied:
            case Committed:
            case ReadyToExecute:
                super.onSuccessCriteriaOrExhaustion(full);
        }
    }
}
