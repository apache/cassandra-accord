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
    public static class QuorumShardTracker extends ShardTracker
    {
        private final Set<Node.Id> inflight;
        private int success = 0;
        private int failures = 0;
        public QuorumShardTracker(Shard shard)
        {
            super(shard);
            this.inflight = new HashSet<>(shard.nodes);
        }

        protected boolean oneSuccess(Node.Id id)
        {
            if (!inflight.remove(id))
                return false;
            success++;
            return true;
        }

        // return true iff hasReachedQuorum()
        public boolean success(Node.Id id)
        {
            oneSuccess(id);
            return hasReachedQuorum();
        }

        // return true iff hasFailed()
        public boolean failure(Node.Id id)
        {
            if (!inflight.remove(id))
                return false;
            failures++;
            return hasFailed();
        }

        public boolean hasFailed()
        {
            return failures > shard.maxFailures;
        }

        public boolean hasReachedQuorum()
        {
            return success >= shard.slowPathQuorumSize;
        }

        public boolean hasInFlight()
        {
            return !inflight.isEmpty();
        }
    }

    public AbstractQuorumTracker(Topologies topologies, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        super(topologies, arrayFactory, trackerFactory);
    }

    // return true iff hasFailed()
    public boolean failure(Node.Id node)
    {
        return anyForNode(node, QuorumShardTracker::failure);
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
