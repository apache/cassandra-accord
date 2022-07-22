package accord.coordinate;

import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.TxnId;

/**
 * Thrown when a coordinator is preempted by another recovery
 * coordinator intending to complete the transaction
 */
public class Preempted extends CoordinateFailed
{
    public Preempted(TxnId txnId, @Nullable RoutingKey homeKey)
    {
        super(txnId, homeKey);
    }
}
