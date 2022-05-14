package accord.coordinate;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.TxnId;

/**
 * Thrown when a transaction exceeds its specified timeout for obtaining a result for a client
 */
public class CoordinateFailed extends Throwable
{
    public final TxnId txnId;
    public final @Nullable RoutingKey homeKey;
    public CoordinateFailed(TxnId txnId, @Nullable RoutingKey homeKey)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
    }
}
