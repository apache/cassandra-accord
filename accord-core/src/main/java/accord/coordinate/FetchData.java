package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Status;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.PartialRoute;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.coordinate.FetchData.Outcome.NotFullyReplicated;
import static accord.coordinate.FetchData.Outcome.Success;

/**
 * Find data and persist locally
 *
 * TODO accept lower bound epoch to avoid fetching data we should already have
 */
public class FetchData
{
    public enum Outcome { Success, NotFullyReplicated }

    public static void fetchWithSomeKeys(Node node, TxnId txnId, RoutingKeys someKeys, long untilLocalEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        FindHomeKey.findHomeKey(node, txnId, someKeys, (homeKey, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (homeKey == null) callback.accept(NotFullyReplicated, null);
            else fetchWithHomeKey(node, txnId, homeKey, untilLocalEpoch, callback);
        });
    }

    public static void fetchWithHomeKey(Node node, TxnId txnId, RoutingKey homeKey, long untilLocalEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        FindRoute.findRoute(node, txnId, homeKey, (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute == null) callback.accept(NotFullyReplicated, null);
            else if (foundRoute.executeAt == null) fetchUncommitted(node, txnId, foundRoute.route, untilLocalEpoch, callback);
            else fetchCommitted(node, txnId, foundRoute.route, foundRoute.executeAt, untilLocalEpoch, callback);
        });
    }

    public static void fetchUncommitted(Node node, TxnId txnId, AbstractRoute route, long untilLocalEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
        if (!route.covers(ranges))
        {
            fetchWithHomeKey(node, txnId, route.homeKey, untilLocalEpoch, callback);
        }
        else
        {
            fetchUncommittedInternal(node, txnId, route.sliceStrict(ranges), untilLocalEpoch, callback);
        }
    }

    public static void fetchUncommitted(Node node, TxnId txnId, Route route, long untilLocalEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
        fetchUncommittedInternal(node, txnId, route.sliceStrict(ranges), untilLocalEpoch, callback);
    }

    private static void fetchUncommittedInternal(Node node, TxnId txnId, PartialRoute route, long untilLocalEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        KeyRanges ranges = node.topology().localRangesForEpochs(txnId.epoch, untilLocalEpoch);
        CheckOnUncommitted.checkOnUncommitted(node, txnId, route.sliceStrict(ranges), txnId.epoch, untilLocalEpoch, (ok, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (ok == null) callback.accept(NotFullyReplicated, null);
            else if (ok.fullStatus == Status.NotWitnessed) callback.accept(NotFullyReplicated, null);
            else callback.accept(Success, null);
        });
    }

    public static void fetchCommitted(Node node, TxnId txnId, AbstractRoute route, Timestamp executeAt, long untilLocalEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        KeyRanges ranges = node.topology().localRangesForEpochs(executeAt.epoch, untilLocalEpoch);
        if (!route.covers(ranges))
        {
            fetchWithHomeKey(node, txnId, route.homeKey, untilLocalEpoch, callback);
        }
        else
        {
            fetchCommittedInternal(node, txnId, route.sliceStrict(ranges), executeAt, untilLocalEpoch, callback);
        }
    }

    public static void fetchCommitted(Node node, TxnId txnId, Route route, Timestamp executeAt, long untilLocalEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        KeyRanges ranges = node.topology().localRangesForEpochs(executeAt.epoch, untilLocalEpoch);
        fetchCommittedInternal(node, txnId, route.sliceStrict(ranges), executeAt, untilLocalEpoch, callback);
    }

    public static void fetchCommittedInternal(Node node, TxnId txnId, PartialRoute route, Timestamp executeAt, long untilLocalEpoch, BiConsumer<Outcome, Throwable> callback)
    {
        CheckOnUncommitted.checkOnCommitted(node, txnId, route, executeAt.epoch, untilLocalEpoch, (ok, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (ok == null) callback.accept(NotFullyReplicated, null);
            else if (ok.fullStatus == Status.NotWitnessed) callback.accept(NotFullyReplicated, null);
            else callback.accept(Success, null);
        });
    }
}
