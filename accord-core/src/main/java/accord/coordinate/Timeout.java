package accord.coordinate;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.TxnId;

/**
 * Thrown when a transaction exceeds its specified timeout for obtaining a result for a client
 */
public class Timeout extends CoordinateFailed
{
    public Timeout(TxnId txnId, @Nullable RoutingKey homeKey)
    {
        super(txnId, homeKey);
    }

    Timeout with(TxnId txnId, RoutingKey homeKey)
    {
        if (this.txnId == null || (this.homeKey == null && homeKey != null))
            return new Timeout(txnId, homeKey);
        return this;
    }
}
