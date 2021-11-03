package accord.coordinate.tracking;

import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.topology.Topology;

public class QuorumTracker extends AbstractQuorumTracker<QuorumShardTracker>
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
