package accord.txn;

import accord.local.Node.Id;

public class Timestamp implements Comparable<Timestamp>
{
    public static final Timestamp NONE = new Timestamp(0, 0, 0, Id.NONE);
    public static final Timestamp MAX = new Timestamp(Long.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE, Id.MAX);

    public final long epoch;
    public final long real;
    public final int logical;
    public final Id node;

    public Timestamp(long epoch, long real, int logical, Id node)
    {
        this.epoch = epoch;
        this.real = real;
        this.logical = logical;
        this.node = node;
    }

    public Timestamp(Timestamp copy)
    {
        this.epoch = copy.epoch;
        this.real = copy.real;
        this.logical = copy.logical;
        this.node = copy.node;
    }

    public Timestamp withMinEpoch(long minEpoch)
    {
        return minEpoch <= epoch ? this : new Timestamp(minEpoch, real, logical, node);
    }

    @Override
    public int compareTo(Timestamp that)
    {
        int c = Long.compare(this.epoch, that.epoch);
        if (c == 0) c = Long.compare(this.real, that.real);
        if (c == 0) c = Integer.compare(this.logical, that.logical);
        if (c == 0) c = this.node.compareTo(that.node);
        return c;
    }

    @Override
    public int hashCode()
    {
        return (int) (((((epoch * 31) + real) * 31) + node.hashCode()) * 31 + logical);
    }

    public boolean equals(Timestamp that)
    {
        return this.epoch == that.epoch && this.real == that.real && this.logical == that.logical && this.node.equals(that.node);
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof Timestamp && equals((Timestamp) that);
    }

    public static <T extends Timestamp> T max(T a, T b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    @Override
    public String toString()
    {
        return "[" + epoch + ',' + real + ',' + logical + ',' + node + ']';
    }

}
