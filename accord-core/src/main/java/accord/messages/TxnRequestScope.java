package accord.messages;

import accord.local.Node.Id;
import accord.topology.KeyRanges;
import accord.topology.Topologies;
import accord.topology.Topology;

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

        static EpochRanges forTopology(Topology topology, Id node, KeyRanges previous)
        {
            KeyRanges topologyRanges = topology.rangesForNode(node);
            if (topologyRanges == null)
                return null;
            KeyRanges ranges = topologyRanges.difference(previous);
            if (ranges.isEmpty())
                return null;
            return new EpochRanges(topology.epoch(), ranges);
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

    public static TxnRequestScope forTopologies(Id node, Topologies topologies)
    {
        KeyRanges currentRanges = KeyRanges.EMPTY;
        List<EpochRanges> ranges = new ArrayList<>(topologies.size());
        for (int i=topologies.size() - 1; i>=0; i--)
        {
            Topology topology = topologies.get(i);
            EpochRanges epochRanges = EpochRanges.forTopology(topology, node, currentRanges);
            if (epochRanges != null)
            {
                ranges.add(epochRanges);
                // TODO: should this range math be done on the replica side? Is probably also useful when determining
                //  whether to respond to a range we no longer replicate. IE: if we lose a range after a reconfig, but
                //  need to respond to satisfy multi-quorum requirements, this will make whether to respond unabiguous
                currentRanges = currentRanges.union(epochRanges.ranges).mergeTouching();
            }
        }

        return new TxnRequestScope(topologies.currentEpoch(), ranges.toArray(EpochRanges[]::new));
    }

    public long maxEpoch()
    {
        return maxEpoch;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnRequestScope that = (TxnRequestScope) o;
        return Arrays.equals(ranges, that.ranges);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(ranges);
    }

    @Override
    public String toString()
    {
        return "TxnRequestScope{" +
                "ranges=" + Arrays.toString(ranges) +
                '}';
    }
}
