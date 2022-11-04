package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class RecoverWithHomeKey extends CheckShards implements BiConsumer<Object, Throwable>
{
    final RoutingKey homeKey;
    final BiConsumer<Outcome, Throwable> callback;

    RecoverWithHomeKey(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Outcome, Throwable> callback)
    {
        super(node, txnId, RoutingKeys.of(homeKey), txnId.epoch, IncludeInfo.Route);
        this.homeKey = homeKey;
        this.callback = callback;
    }

    public static RecoverWithHomeKey recover(Node node, TxnId txnId, RoutingKey homeKey, BiConsumer<Outcome, Throwable> callback)
    {
        RecoverWithHomeKey maybeRecover = new RecoverWithHomeKey(node, txnId, homeKey, callback);
        maybeRecover.start();
        return maybeRecover;
    }

    @Override
    public void accept(Object unused, Throwable fail)
    {
        callback.accept(null, fail);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return ok.route != null;
    }

    @Override
    protected void onDone(Success success, Throwable fail)
    {
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else if (merged == null || !(merged.route instanceof Route))
        {
            switch (success)
            {
                default: throw new IllegalStateException();
                case Success:
                    // home shard must know full Route. Our success criteria is that the response contained a route,
                    // so reaching here without a response or one without a full Route is a bug.
                    callback.accept(null, new IllegalStateException());
                    return;
                case Quorum:
                    Invalidate.invalidateIfNotAccepted(node, txnId, contactKeys, homeKey, callback);
            }
        }
        else
        {
            // start recovery
            node.recover(txnId, (Route) merged.route).addCallback(callback);
        }
    }
}
