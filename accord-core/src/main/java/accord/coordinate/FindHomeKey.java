package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

/**
 * Find the homeKey of a txnId with some known keys
 */
public class FindHomeKey extends CheckShards
{
    final BiConsumer<RoutingKey, Throwable> callback;
    FindHomeKey(Node node, TxnId txnId, RoutingKeys someKeys, BiConsumer<RoutingKey, Throwable> callback)
    {
        super(node, txnId, someKeys, txnId.epoch, IncludeInfo.No);
        this.callback = callback;
    }

    public static FindHomeKey findHomeKey(Node node, TxnId txnId, RoutingKeys someKeys, BiConsumer<RoutingKey, Throwable> callback)
    {
        FindHomeKey findHomeKey = new FindHomeKey(node, txnId, someKeys, callback);
        findHomeKey.start();
        return findHomeKey;
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        return ok.homeKey != null;
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
            callback.accept(merged == null ? null : merged.homeKey, null);
        }
    }
}
