package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shards;

public class QuorumTracker extends AbstractQuorumTracker<QuorumTracker.QuorumShardTracker>
{
    public QuorumTracker(Shards shards)
    {
        super(shards, QuorumShardTracker[]::new, QuorumShardTracker::new);
    }

    public void recordSuccess(Node.Id node)
    {
        forEachTrackerForNode(node, QuorumShardTracker::onSuccess);
    }
}
