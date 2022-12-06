package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.Unseekables;
import accord.primitives.TxnId;

/**
 * Find the homeKey of a txnId with some known keys
 */
public class FindHomeKey extends CheckShards
{
    final BiConsumer<RoutingKey, Throwable> callback;
    FindHomeKey(Node node, TxnId txnId, Unseekables<?, ?> unseekables, BiConsumer<RoutingKey, Throwable> callback)
    {
        super(node, txnId, unseekables, txnId.epoch(), IncludeInfo.No);
        this.callback = callback;
    }

    public static FindHomeKey findHomeKey(Node node, TxnId txnId, Unseekables<?, ?> unseekables, BiConsumer<RoutingKey, Throwable> callback)
    {
        FindHomeKey findHomeKey = new FindHomeKey(node, txnId, unseekables, callback);
        findHomeKey.start();
        return findHomeKey;
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return ok.homeKey != null;
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure != null) callback.accept(null, failure);
        else callback.accept(merged == null ? null : merged.homeKey, null);
    }
}
