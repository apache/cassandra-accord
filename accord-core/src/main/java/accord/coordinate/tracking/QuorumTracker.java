package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Topology;

public class QuorumTracker extends AbstractQuorumTracker<QuorumTracker.QuorumShardTracker>
{
    public QuorumTracker(Topology shards)
    {
        super(shards, QuorumShardTracker[]::new, QuorumShardTracker::new);
    }

    public void recordSuccess(Node.Id node)
    {
        forEachTrackerForNode(node, QuorumShardTracker::onSuccess);
    }
}
