package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Status.Phase;
import accord.local.Status.Known;
import accord.primitives.*;

import javax.annotation.Nullable;

import static accord.local.Status.Known.*;

/**
 * Find data and persist locally
 *
 * TODO accept lower bound epoch to avoid fetching data we should already have
 */
public class FetchData
{
    public static Object fetch(Known phase, Node node, TxnId txnId, RoutingKeys someKeys, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        return fetch(phase, node, txnId, someKeys, null, untilLocalEpoch, callback);
    }

    public static Object fetch(Known phase, Node node, TxnId txnId, RoutingKeys someKeys, @Nullable Timestamp executeAt, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        if (someKeys instanceof AbstractRoute) return fetch(phase, node, txnId, (AbstractRoute) someKeys, executeAt, untilLocalEpoch, callback);
        else return fetchWithSomeKeys(phase, node, txnId, someKeys, untilLocalEpoch, callback);
    }

    public static Object fetch(Known phase, Node node, TxnId txnId, AbstractRoute route, @Nullable Timestamp executeAt, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
        if (!route.covers(ranges))
        {
            return fetchWithHomeKey(phase, node, txnId, route.homeKey, untilLocalEpoch, callback);
        }
        else
        {
            return fetchInternal(ranges, phase, node, txnId, route.sliceStrict(ranges), executeAt, untilLocalEpoch, callback);
        }
    }

    private static Object fetchWithSomeKeys(Known phase, Node node, TxnId txnId, RoutingKeys someKeys, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        return FindHomeKey.findHomeKey(node, txnId, someKeys, (foundHomeKey, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundHomeKey == null) callback.accept(Nothing, null);
            else fetchWithHomeKey(phase, node, txnId, foundHomeKey, untilLocalEpoch, callback);
        });
    }

    private static Object fetchWithHomeKey(Known phase, Node node, TxnId txnId, RoutingKey homeKey, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        return FindRoute.findRoute(node, txnId, homeKey, (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute == null) callback.accept(Nothing, null);
            else fetch(phase, node, txnId, foundRoute.route, foundRoute.executeAt, untilLocalEpoch, callback);
        });
    }

    public static Object fetch(Known phase, Node node, TxnId txnId, Route route, @Nullable Timestamp executeAt, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
        return fetchInternal(ranges, phase, node, txnId, route.sliceStrict(ranges), executeAt, untilLocalEpoch, callback);
    }

    private static Object fetchInternal(KeyRanges ranges, Known target, Node node, TxnId txnId, PartialRoute route, @Nullable Timestamp executeAt, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        PartialRoute fetch = route.sliceStrict(ranges);
        long srcEpoch = target == ExecutionOrder || executeAt == null ? txnId.epoch : executeAt.epoch;
        return CheckOn.checkOn(target, node, txnId, fetch, srcEpoch, untilLocalEpoch, (ok, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (ok == null) callback.accept(Nothing, null);
            else
            {
                // even if we have enough information to Apply for the requested epochs, if we didn't request enough
                // information to fulfil that phase locally we should downgrade the response we give to the callback
                Known sufficientFor = ok.sufficientFor(fetch);
                // if we discover the executeAt as part of this action, use that to decide if we requested enough info
                Timestamp exec = executeAt != null ? executeAt : ok.saveStatus.isAtLeast(Phase.Commit) ? ok.executeAt : null;
                if (sufficientFor == Outcome && (exec == null || untilLocalEpoch < exec.epoch))
                    sufficientFor = ExecutionOrder;
                callback.accept(sufficientFor, null);
            }
        });
    }
}
