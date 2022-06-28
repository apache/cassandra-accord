package accord.coordinate;

import javax.annotation.Nullable;

import accord.api.Key;
import accord.primitives.TxnId;

/**
 * Thrown when a transaction exceeds its specified timeout for obtaining a result for a client
 */
public class CoordinateFailed extends Throwable
{
    public final TxnId txnId;
    public final @Nullable Key homeKey;
    public CoordinateFailed(TxnId txnId, @Nullable Key homeKey)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
    }
}
