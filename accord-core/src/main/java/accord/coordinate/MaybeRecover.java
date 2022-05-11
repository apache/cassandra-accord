package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.Key;
import accord.local.Node;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.Commit;
import accord.topology.Shard;
import accord.primitives.Ballot;
import accord.txn.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.local.Status.Accepted;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class MaybeRecover extends CheckShardStatus<CheckStatusOk> implements BiConsumer<Object, Throwable>
{
    final Txn txn;
    final Status knownStatus;
    final Ballot knownPromised;
    final boolean knownPromisedHasBeenAccepted;

    MaybeRecover(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, long homeEpoch, Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted)
    {
        super(node, txnId, homeKey, homeShard, homeEpoch, IncludeInfo.OnlyIfExecuted);
        this.txn = txn;
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

    public static Future<CheckStatusOk> maybeRecover(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, long homeEpoch,
                                                               Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted)
    {
        MaybeRecover maybeRecover = new MaybeRecover(node, txnId, txn, homeKey, homeShard, homeEpoch, knownStatus, knownPromised, knownPromiseHasBeenAccepted);
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
            case AcceptedInvalidate:
            case Committed:
            case ReadyToExecute:
                if (hasMadeProgress())
                {
                    trySuccess(max);
                }
                else
                {
                    node.recover(txnId, txn, someKey)
                        .addCallback(this);
                }
                break;

            case Executed:
            case Applied:
                CheckStatusOkFull full = (CheckStatusOkFull) max;
                node.withEpoch(full.executeAt.epoch, () -> {
                    if (!max.hasExecutedOnAllShards)
                        Persist.persistAndCommit(node, txnId, someKey, txn, full.executeAt, full.deps, full.writes, full.result);
                    else // TODO (now): we shouldn't need to do this?
                        Commit.commit(node, txnId, txn, full.homeKey, full.executeAt, full.deps);
                });
                trySuccess(full);
                break;

            case Invalidated:
                trySuccess(max);
        }
    }
}
