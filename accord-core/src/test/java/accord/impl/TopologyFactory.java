package accord.impl;


import accord.local.Node;
import accord.api.Key;
import accord.api.KeyRange;
import accord.topology.KeyRanges;
import accord.topology.Shards;

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

    public Shards toShards(Node.Id[] cluster)
    {
        return TopologyUtils.initialTopology(cluster, new KeyRanges(ranges), rf);
    }

    public Shards toShards(List<Node.Id> cluster)
    {
        return toShards(cluster.toArray(Node.Id[]::new));
    }

    public static <K extends Key<K>> Shards toShards(List<Node.Id> cluster, int rf, KeyRange<K>... ranges)
    {
        return new TopologyFactory<>(rf, ranges).toShards(cluster);
    }
}
