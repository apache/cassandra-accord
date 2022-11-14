package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Status.Phase;
import accord.local.Status.Known;
import accord.primitives.*;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

import static accord.local.Status.LogicalEpoch.Coordination;
import static accord.local.Status.Known.*;
import static accord.local.Status.Outcome.OutcomeKnown;
import static accord.local.Status.Outcome.OutcomeUnknown;

/**
 * Find data and persist locally
 *
 * TODO accept lower bound epoch to avoid fetching data we should already have
 */
public class FetchData
{
    public static Object fetch(Known fetch, Node node, TxnId txnId, RoutingKeys someKeys, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        return fetch(fetch, node, txnId, someKeys, null, untilLocalEpoch, callback);
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, RoutingKeys someKeys, @Nullable Timestamp executeAt, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        if (someKeys instanceof AbstractRoute) return fetch(fetch, node, txnId, (AbstractRoute) someKeys, executeAt, untilLocalEpoch, callback);
        else return fetchWithSomeKeys(fetch, node, txnId, someKeys, untilLocalEpoch, callback);
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, AbstractRoute route, @Nullable Timestamp executeAt, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        Preconditions.checkArgument(node.topology().hasEpoch(untilLocalEpoch));
        KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
        if (!route.covers(ranges))
        {
            return fetchWithHomeKey(fetch, node, txnId, route.homeKey, untilLocalEpoch, callback);
        }
        else
        {
            return fetchInternal(ranges, fetch, node, txnId, route.sliceStrict(ranges), executeAt, untilLocalEpoch, callback);
        }
    }

    private static Object fetchWithSomeKeys(Known fetch, Node node, TxnId txnId, RoutingKeys someKeys, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        Preconditions.checkArgument(node.topology().hasEpoch(untilLocalEpoch));
        return FindHomeKey.findHomeKey(node, txnId, someKeys, (foundHomeKey, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundHomeKey == null) callback.accept(Nothing, null);
            else fetchWithHomeKey(fetch, node, txnId, foundHomeKey, untilLocalEpoch, callback);
        });
    }

    private static Object fetchWithHomeKey(Known fetch, Node node, TxnId txnId, RoutingKey homeKey, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        Preconditions.checkArgument(node.topology().hasEpoch(untilLocalEpoch));
        return FindRoute.findRoute(node, txnId, homeKey, (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute == null) callback.accept(Nothing, null);
            else fetch(fetch, node, txnId, foundRoute.route, foundRoute.executeAt, untilLocalEpoch, callback);
        });
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, Route route, @Nullable Timestamp executeAt, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
        return fetchInternal(ranges, fetch, node, txnId, route.sliceStrict(ranges), executeAt, untilLocalEpoch, callback);
    }

    private static Object fetchInternal(KeyRanges ranges, Known target, Node node, TxnId txnId, PartialRoute route, @Nullable Timestamp executeAt, long untilLocalEpoch, BiConsumer<Known, Throwable> callback)
    {
        long srcEpoch = executeAt == null || target.epoch() == Coordination ? txnId.epoch : executeAt.epoch;
        if (!node.topology().hasEpoch(srcEpoch))
            return node.topology().awaitEpoch(srcEpoch).map(ignore -> fetchInternal(ranges, target, node, txnId, route, executeAt, untilLocalEpoch, callback));

        PartialRoute fetch = route.sliceStrict(ranges);
        return CheckOn.checkOn(target, node, txnId, fetch, srcEpoch, untilLocalEpoch, (ok, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (ok == null) callback.accept(Nothing, null);
            else
            {
                // even if we have enough information to Apply for the requested epochs, if we didn't request enough
                // information to fulfil that phase locally we should downgrade the response we give to the callback
                Known sufficientFor = ok.sufficientFor(fetch);
                // if we discover the executeAt as part of this action, use that to decide if we requested enough info
                Timestamp exec = executeAt != null ? executeAt : ok.saveStatus.known.executeAt.isDecisionKnown() ? ok.executeAt : null;
                if (sufficientFor.outcome == OutcomeKnown && (exec == null || untilLocalEpoch < exec.epoch))
                    sufficientFor = sufficientFor.with(OutcomeUnknown);
                callback.accept(sufficientFor, null);
            }
        });
    }
}
