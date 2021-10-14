package accord.txn;

import accord.local.Node.Id;

public class Ballot extends Timestamp
{
    public static final Ballot ZERO = new Ballot(Timestamp.NONE);

    public Ballot(Timestamp from)
    {
        super(from);
    }

    public Ballot(long epoch, long real, int logical, Id node)
    {
        super(epoch, real, logical, node);
    }
}
