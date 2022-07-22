package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

/**
 * Find the Route of a known (txnId, homeKey) pair
 */
public class FindRoute extends CheckShards
{
    public static class Result
    {
        public final Route route;
        public final Timestamp executeAt;

        public Result(Route route, Timestamp executeAt)
        {
            this.route = route;
            this.executeAt = executeAt;
        }

        public Result(CheckStatusOk status)
        {
            this.route = (Route)status.route;
            this.executeAt = status.status.compareTo(Status.Committed) >= 0 ? status.executeAt : null;
        }
    }

    final BiConsumer<Result, Throwable> callback;
    FindRoute(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Result, Throwable> callback)
    {
        super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch, IncludeInfo.Route);
        this.callback = callback;
    }

    public static FindRoute findRoute(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Result, Throwable> callback)
    {
        FindRoute findRoute = new FindRoute(node, txnId, homeKey, callback);
        findRoute.start();
        return findRoute;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ok.route instanceof Route;
    }

    @Override
    protected void onDone(Done done, Throwable failure)
    {
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            super.onDone(done, failure);
            callback.accept(merged == null || merged.route == null ? null : new Result(merged), null);
        }
    }
}
