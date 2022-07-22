package accord.coordinate.tracking;

import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.topology.Topologies;
import accord.topology.Topologies.Single;
import accord.topology.Topology;

public class QuorumTracker extends AbstractQuorumTracker<QuorumShardTracker>
{
    public QuorumTracker(Topologies topologies)
    {
        super(topologies, QuorumShardTracker[]::new, QuorumShardTracker::new);
    }

    public QuorumTracker(Topology topology)
    {
        super(new Single(topology, false), QuorumShardTracker[]::new, QuorumShardTracker::new);
    }

    // return true iff hasReachedQuorum()
    public boolean success(Node.Id node)
    {
        return allForNode(node, QuorumShardTracker::success) && hasReachedQuorum();
    }
}
