package accord.impl;

import accord.api.TopologySorter;
import accord.local.Node;
import accord.topology.ShardSelection;
import accord.topology.Topologies;
import accord.topology.Topology;

public class SizeOfIntersectionSorter implements TopologySorter
{
    public static final TopologySorter.Supplier SUPPLIER = new Supplier() {
        @Override
        public TopologySorter get(Topology topology)
        {
            return new SizeOfIntersectionSorter(new Topologies.Single(this, topology));
        }

        @Override
        public TopologySorter get(Topologies topologies)
        {
            return new SizeOfIntersectionSorter(topologies);
        }
    };

    final Topologies topologies;
    SizeOfIntersectionSorter(Topologies topologies)
    {
        this.topologies = topologies;
    }

    @Override
    public int compare(Node.Id node1, Node.Id node2, ShardSelection shards)
    {
        int maxShardsPerEpoch = topologies.maxShardsPerEpoch();
        int count1 = 0, count2 = 0;
        for (int i = 0, mi = topologies.size() ; i < mi ; ++i)
        {
            Topology topology = topologies.get(i);
            count1 += count(node1, shards, i * maxShardsPerEpoch, topology);
            count2 += count(node2, shards, i * maxShardsPerEpoch, topology);
        }

        // sort more intersections later
        return count1 - count2;
    }

    private static int count(Node.Id node, ShardSelection shards, int offset, Topology topology)
    {
        return topology.foldlIntOn(node, (shard, v, index) -> shard.get(index) ? v + 1 : v, shards, offset, 0, 0);
    }
}
