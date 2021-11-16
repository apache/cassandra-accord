package accord.messages;

import accord.local.Node.Id;
import accord.topology.KeyRanges;
import accord.topology.Topologies;
import accord.topology.Topology;

import java.util.ArrayList;
import java.util.List;

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

        static EpochRanges forTopology(Topology topology, Id node)
        {
            KeyRanges ranges = topology.rangesForNode(node);
            return new EpochRanges(topology.epoch(), ranges);
        }
    }

    private final EpochRanges[] ranges;

    public TxnRequestScope(EpochRanges[] ranges)
    {
        this.ranges = ranges;
    }

    public static TxnRequestScope forTopologies(Id node, Topologies topologies)
    {
        List<EpochRanges> ranges = new ArrayList<>(topologies.size());
        for (int i=topologies.size() - 1; i>=0; i--)
        {
            EpochRanges epochRanges = EpochRanges.forTopology(topologies.get(i), node);
            if (epochRanges != null)
                ranges.add(epochRanges);
        }

        return new TxnRequestScope(ranges.toArray(EpochRanges[]::new));
    }
}
