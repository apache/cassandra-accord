package accord.impl;


import accord.local.Node;
import accord.api.Key;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.topology.Topology;

import java.util.*;

public class TopologyFactory
{
    public final int rf;
    // TODO: convert to KeyRanges
    final KeyRange[] ranges;

    public TopologyFactory(int rf, KeyRange... ranges)
    {
        this.rf = rf;
        this.ranges = ranges;
    }

    public Topology toTopology(Node.Id[] cluster)
    {
        return TopologyUtils.initialTopology(cluster, new KeyRanges(ranges), rf);
    }

    public Topology toTopology(List<Node.Id> cluster)
    {
        return toTopology(cluster.toArray(Node.Id[]::new));
    }

    public static Topology toTopology(List<Node.Id> cluster, int rf, KeyRange... ranges)
    {
        return new TopologyFactory(rf, ranges).toTopology(cluster);
    }
}
