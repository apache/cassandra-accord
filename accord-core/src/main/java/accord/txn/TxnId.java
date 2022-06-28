package accord.txn;

import accord.local.Node.Id;

// TODO: consider moving homeKey concept here? might make later optimisations of its storage trickier
public class TxnId extends Timestamp
{
    public TxnId(Timestamp timestamp)
    {
        super(timestamp);
    }

    public TxnId(long epoch, long real, int logical, Id node)
    {
        super(epoch, real, logical, node);
    }
}
