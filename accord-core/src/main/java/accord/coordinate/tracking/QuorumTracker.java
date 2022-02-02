package accord.coordinate.tracking;

import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.topology.Topologies;

public class QuorumTracker extends AbstractQuorumTracker<QuorumShardTracker>
{
    public QuorumTracker(Topologies topologies)
    {
        super(topologies, QuorumShardTracker[]::new, (shard, epoch) -> new QuorumShardTracker(shard));
    }

    public void recordSuccess(Node.Id node)
    {
        forEachTrackerForNode(node, QuorumShardTracker::onSuccess);
    }
}
