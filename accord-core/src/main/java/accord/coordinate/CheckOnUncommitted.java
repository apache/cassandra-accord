package accord.coordinate;

import accord.api.Key;
import accord.local.Command;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.topology.Shard;
import accord.primitives.Keys;
import accord.primitives.TxnId;

import static accord.local.Status.Committed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnUncommitted extends CheckOnCommitted
{
    final Keys someKeys;
    CheckOnUncommitted(Node node, TxnId txnId, Keys someKeys, Key someKey, Shard someShard, long shardEpoch)
    {
        super(node, txnId, someKey, someShard, shardEpoch);
        this.someKeys = someKeys;
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, Keys someKeys, Key someKey, Shard someShard, long shardEpoch)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, someKeys, someKey, someShard, shardEpoch);
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
        return max != null && max.status.hasBeen(Committed);
    }

    @Override
    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.status)
        {
            default: throw new IllegalStateException();
            case Invalidated:
                node.forEachLocalSince(someKeys, txnId.epoch, commandStore -> {
                    Command command = commandStore.ifPresent(txnId);
                    if (command != null)
                        command.commitInvalidate();
                });
            case NotWitnessed:
            case AcceptedInvalidate:
                break;
            case PreAccepted:
            case Accepted:
                node.forEachLocalSince(full.txn.keys, txnId.epoch, commandStore -> {
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
