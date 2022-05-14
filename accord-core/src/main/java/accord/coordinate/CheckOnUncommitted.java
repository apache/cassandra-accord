package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
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
    CheckOnUncommitted(Node node, TxnId txnId, AbstractRoute route, long srcEpoch, long trgEpoch, BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        // invalidated transactions are not guaranteed to be disseminated, so we include the homeKey when
        // checking on uncommitted transactions to ensure we detect an invalidation
        super(node, txnId, route, route.with(route.homeKey), srcEpoch, trgEpoch, callback);
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, AbstractRoute route, long srcEpoch, long trgEpoch,
                                                        BiConsumer<CheckStatusOkFull, Throwable> callback)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, route, srcEpoch, trgEpoch, callback);
        checkOnUncommitted.start();
        return checkOnUncommitted;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ((CheckStatusOkFull)ok).fullStatus.hasBeen(Committed);
    }

    @Override
    void persistLocally(CheckStatusOkFull full)
    {
        switch (full.fullStatus)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
                break;
            case AcceptedInvalidate:
            case Accepted:
            case PreAccepted:
                KeyRanges localRanges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
                if (!route().covers(localRanges))
                    break;

                KeyRanges localCommitRanges = node.topology().localRangesForEpoch(txnId.epoch);
                RoutingKey progressKey = node.trySelectProgressKey(txnId, route().slice(localCommitRanges));
                AbstractRoute fullRoute = AbstractRoute.merge(route(), full.route);
                PartialRoute route = fullRoute.sliceStrict(localRanges);
                PartialTxn partialTxn = full.partialTxn.reconstitutePartial(route);
                node.forEachLocal(someKeys, txnId.epoch, untilLocalEpoch, commandStore -> {
                    commandStore.command(txnId).preaccept(partialTxn, fullRoute, progressKey);
                });
                break;
            case Executed:
            case Applied:
            case Committed:
            case ReadyToExecute:
            case Invalidated:
                super.persistLocally(full);
        }
    }
}
