package accord.messages;

import accord.local.Node.Id;
import accord.topology.KeyRanges;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Txn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Indicates the ranges the coordinator expects the recipient to service for a request, and
 * the minimum epochs the recipient will need to be aware of for each range
 */
public class TxnRequestScope
{
    public static class EpochRanges
    {
        public final long epoch;
        public final KeyRanges ranges;

        public EpochRanges(long epoch, KeyRanges ranges)
        {
            this.epoch = epoch;
            this.ranges = ranges;
        }

        static EpochRanges forTopology(Topology topology, Id node, Keys keys)
        {
            KeyRanges topologyRanges = topology.rangesForNode(node);
            if (topologyRanges == null)
                return null;
            topologyRanges = topologyRanges.intersection(keys);
            return !topologyRanges.isEmpty() ? new EpochRanges(topology.epoch(), topologyRanges) : null;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EpochRanges that = (EpochRanges) o;
            return epoch == that.epoch && ranges.equals(that.ranges);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(epoch, ranges);
        }

        @Override
        public String toString()
        {
            return "EpochRanges{" +
                    "epoch=" + epoch +
                    ", ranges=" + ranges +
                    '}';
        }
    }

    private final long maxEpoch;
    private final EpochRanges[] ranges;

    public TxnRequestScope(long maxEpoch, EpochRanges[] ranges)
    {
        this.maxEpoch = maxEpoch;
        this.ranges = ranges;
    }

    public int size()
    {
        return ranges.length;
    }

    public EpochRanges get(int i)
    {
        return ranges[i];
    }

    public static TxnRequestScope forTopologies(Id node, Topologies topologies, Keys keys)
    {
        List<EpochRanges> ranges = new ArrayList<>(topologies.size());
        for (int i=topologies.size() - 1; i>=0; i--)
        {
            Topology topology = topologies.get(i);
            EpochRanges epochRanges = EpochRanges.forTopology(topology, node, keys);
            if (epochRanges != null)
            {
                ranges.add(epochRanges);
            }
        }

        return new TxnRequestScope(topologies.currentEpoch(), ranges.toArray(EpochRanges[]::new));
    }

    public static TxnRequestScope forTopologies(Id node, Topologies topologies, Txn txn)
    {
        return forTopologies(node, topologies, txn.keys());
    }

    public long maxEpoch()
    {
        return maxEpoch;
    }

    public boolean intersects(KeyRanges ranges)
    {
        for (EpochRanges epochRanges : this.ranges)
        {
            if (epochRanges.ranges.intersects(ranges))
                return true;
        }

        return false;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnRequestScope that = (TxnRequestScope) o;
        return maxEpoch == that.maxEpoch && Arrays.equals(ranges, that.ranges);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(maxEpoch);
        result = 31 * result + Arrays.hashCode(ranges);
        return result;
    }

    @Override
    public String toString()
    {
        return "TxnRequestScope{" +
                "ranges=" + Arrays.toString(ranges) +
                '}';
    }
}
