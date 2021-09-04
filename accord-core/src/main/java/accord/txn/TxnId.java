package accord.txn;

import accord.local.Node.Id;

public class TxnId extends Timestamp
{
    public TxnId(Timestamp timestamp)
    {
        super(timestamp);
    }

    public TxnId(long real, int logical, Id node)
    {
        super(real, logical, node);
    }
}
