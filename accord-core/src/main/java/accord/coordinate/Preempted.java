package accord.coordinate;

import javax.annotation.Nullable;

import accord.api.Key;
import accord.txn.TxnId;

/**
 * Thrown when a coordinator is preempted by another recovery
 * coordinator intending to complete the transaction
 */
public class Preempted extends CoordinateFailed
{
    public Preempted(TxnId txnId, @Nullable Key homeKey)
    {
        super(txnId, homeKey);
    }
}
