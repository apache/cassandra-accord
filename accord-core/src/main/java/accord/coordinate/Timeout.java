package accord.coordinate;

import javax.annotation.Nullable;

import accord.api.Key;
import accord.txn.TxnId;

/**
 * Thrown when a transaction exceeds its specified timeout for obtaining a result for a client
 */
public class Timeout extends CoordinateFailed
{
    public Timeout(TxnId txnId, @Nullable Key homeKey)
    {
        super(txnId, homeKey);
    }

    Timeout with(TxnId txnId, Key homeKey)
    {
        if (this.txnId == null || (this.homeKey == null && homeKey != null))
            return new Timeout(txnId, homeKey);
        return this;
    }
}
