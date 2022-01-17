package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.Key;
import accord.local.Node;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Shard;
import accord.txn.Ballot;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.local.Status.Accepted;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class MaybeRecover extends CheckShardStatus implements BiConsumer<Object, Throwable>
{
    final Status knownStatus;
    final Ballot knownPromised;
    final boolean knownPromisedHasBeenAccepted;

    MaybeRecover(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, long homeEpoch, Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted, byte includeInfo)
    {
        super(node, txnId, txn, homeKey, homeShard, homeEpoch, includeInfo);
        this.knownStatus = knownStatus;
        this.knownPromised = knownPromised;
        this.knownPromisedHasBeenAccepted = knownPromiseHasBeenAccepted;
    }

    @Override
    public void accept(Object unused, Throwable fail)
    {
        if (fail != null) tryFailure(fail);
        else trySuccess(null);
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum() || hasMadeProgress();
    }

    public boolean hasMadeProgress()
    {
        return max != null && (max.isCoordinating
                               || max.status.compareTo(knownStatus) > 0
                               || max.promised.compareTo(knownPromised) > 0
                               || (!knownPromisedHasBeenAccepted && knownStatus == Accepted && max.accepted.equals(knownPromised)));
    }

    // TODO (now): invoke from {node} so we may have mutual exclusion with other attempts to recover or coordinate
    public static Future<CheckStatusOk> maybeRecover(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, long homeEpoch,
                                                     Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted)
    {
        return maybeRecover(node, txnId, txn, homeKey, homeShard, homeEpoch, knownStatus, knownPromised, knownPromiseHasBeenAccepted, (byte)0);
    }

    private static Future<CheckStatusOk> maybeRecover(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, long homeEpoch,
                                                               Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted, byte includeInfo)
    {
        MaybeRecover maybeRecover = new MaybeRecover(node, txnId, txn, homeKey, homeShard, homeEpoch, knownStatus, knownPromised, knownPromiseHasBeenAccepted, includeInfo);
        maybeRecover.start();
        return maybeRecover;
    }

    void onSuccessCriteriaOrExhaustion()
    {
        switch (max.status)
        {
            default: throw new AssertionError();
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case Committed:
            case ReadyToExecute:
                if (hasMadeProgress())
                {
                    trySuccess(max);
                }
                else
                {
                    node.recover(txnId, txn, key)
                        .addCallback(this);
                }
                break;

            case Executed:
            case Applied:
                if (max.hasExecutedOnAllShards)
                {
                    // TODO: persist this knowledge locally?
                    trySuccess(null);
                }
                else if (max instanceof CheckStatusOkFull)
                {
                    CheckStatusOkFull full = (CheckStatusOkFull) max;
                    // TODO (now): consider topology choice here
                    Persist.persist(node, node.topology().syncForKeys(txn.keys, full.executeAt.epoch), txnId, key, txn, full.executeAt, full.deps, full.writes, full.result)
                           .addCallback(this);
                }
                else
                {
                    maybeRecover(node, txnId, txn, key, tracker.shard, epoch, max.status, max.promised, max.accepted.equals(max.promised), IncludeInfo.all())
                    .addCallback((success, fail) -> {
                        if (fail != null) tryFailure(fail);
                        else
                        {
                            assert success == null;
                            trySuccess(null);
                        }
                    });
                }
        }
    }
}
