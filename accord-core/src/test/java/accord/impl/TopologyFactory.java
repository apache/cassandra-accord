package accord.impl;


import accord.local.Node;
import accord.api.Key;
import accord.api.KeyRange;
import accord.topology.KeyRanges;
import accord.topology.Topology;

import java.util.*;

public class TopologyFactory<K extends Key<K>>
{
    public final int rf;
    // TODO: convert to KeyRanges
    final KeyRange<K>[] ranges;

    public TopologyFactory(int rf, KeyRange<K>... ranges)
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

    public static <K extends Key<K>> Topology toTopology(List<Node.Id> cluster, int rf, KeyRange<K>... ranges)
    {
        return new TopologyFactory<>(rf, ranges).toTopology(cluster);
    }
}
