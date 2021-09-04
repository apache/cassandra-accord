package accord.impl;


import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Key;
import accord.utils.KeyRange;
import accord.topology.Shard;
import accord.topology.Shards;
import accord.utils.WrapAroundList;
import accord.utils.WrapAroundSet;

import java.util.*;

public class TopologyFactory<K extends Key<K>>
{
    public final int rf;
    final KeyRange<K>[] ranges;

    public TopologyFactory(int rf, KeyRange<K>... ranges)
    {
        this.rf = rf;
        this.ranges = ranges;
    }

    public Shards toShards(Node.Id[] cluster)
    {
        final Map<Node.Id, Integer> lookup = new HashMap<>();
        for (int i = 0 ; i < cluster.length ; ++i)
            lookup.put(cluster[i], i);

        List<WrapAroundList<Id>> electorates = new ArrayList<>();
        List<Set<Node.Id>> fastPathElectorates = new ArrayList<>();

        for (int i = 0 ; i < cluster.length + rf - 1 ; ++i)
        {
            WrapAroundList<Node.Id> electorate = new WrapAroundList<>(cluster, i % cluster.length, (i + rf) % cluster.length);
            Set<Node.Id> fastPathElectorate = new WrapAroundSet<>(lookup, electorate);
            electorates.add(electorate);
            fastPathElectorates.add(fastPathElectorate);
        }

        final List<Shard> shards = new ArrayList<>();
        for (int i = 0 ; i < ranges.length ; ++i)
            shards.add(new Shard(ranges[i].start, ranges[i].end, electorates.get(i % electorates.size()), fastPathElectorates.get(i % fastPathElectorates.size())));
        return new Shards(shards.toArray(Shard[]::new));
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
