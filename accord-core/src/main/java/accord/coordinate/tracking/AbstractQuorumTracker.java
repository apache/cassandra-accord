package accord.coordinate.tracking;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;

public class AbstractQuorumTracker<T extends AbstractQuorumTracker.QuorumShardTracker> extends AbstractResponseTracker<T>
{
    static class QuorumShardTracker extends ShardTracker
    {
        private final Set<Node.Id> inflight;
        private int success = 0;
        private int failures = 0;
        public QuorumShardTracker(Shard shard)
        {
            super(shard);
            this.inflight = new HashSet<>(shard.nodes);
        }

        public boolean onSuccess(Node.Id id)
        {
            if (!inflight.remove(id))
                return false;
            success++;
            return true;
        }

        boolean onFailure(Node.Id id)
        {
            if (!inflight.remove(id))
                return false;
            failures++;
            return true;
        }

        boolean hasFailed()
        {
            return failures >= shard.slowPathQuorumSize;
        }

        boolean hasReachedQuorum()
        {
            return success >= shard.slowPathQuorumSize;
        }

        boolean hasInFlight()
        {
            return !inflight.isEmpty();
        }
    }

    public AbstractQuorumTracker(Topologies topologies, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        super(topologies, arrayFactory, trackerFactory);
    }

    // TODO: refactor to return true if this call caused the state change to failed
    public void recordFailure(Node.Id node)
    {
        forEachTrackerForNode(node, QuorumShardTracker::onFailure);
    }

    public boolean hasReachedQuorum()
    {
        return all(QuorumShardTracker::hasReachedQuorum);
    }

    public boolean hasFailed()
    {
        return any(QuorumShardTracker::hasFailed);
    }

    public boolean hasInFlight()
    {
        return any(QuorumShardTracker::hasInFlight);
    }

}
