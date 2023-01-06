package accord.api;

import accord.local.Node;
import accord.topology.ShardSelection;
import accord.topology.Topologies;
import accord.topology.Topology;

public interface TopologySorter
{
    interface Supplier
    {
        TopologySorter get(Topology topologies);
        TopologySorter get(Topologies topologies);
    }

    interface StaticSorter extends Supplier, TopologySorter
    {
        @Override
        default TopologySorter get(Topology topologies) { return this; }
        @Override
        default TopologySorter get(Topologies topologies) { return this; }
    }

    /**
     * Compare two nodes that occur in some topologies, so that the one that is *least* preferable sorts first
     */
    int compare(Node.Id node1, Node.Id node2, ShardSelection shards);
}
