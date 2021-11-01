package accord.coordinate.tracking;

import java.util.function.Function;
import java.util.function.IntFunction;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;

public class FastPathTracker<T extends FastPathTracker.FastPathShardTracker> extends AbstractQuorumTracker<T>
{
    public abstract static class FastPathShardTracker extends QuorumTracker.QuorumShardTracker
    {
        protected int fastPathAccepts = 0;

        public FastPathShardTracker(Shard shard)
        {
            super(shard);
        }

        public abstract boolean includeInFastPath(Node.Id node, boolean withFastPathTimestamp);

        public void onSuccess(Node.Id node, boolean withFastPathTimestamp)
        {
            if (onSuccess(node) && includeInFastPath(node, withFastPathTimestamp))
                fastPathAccepts++;
        }

        public abstract boolean hasMetFastPathCriteria();
    }

    public FastPathTracker(Topology topology, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        super(topology, arrayFactory, trackerFactory);
    }

    public void recordSuccess(Node.Id node, boolean withFastPathTimestamp)
    {
        forEachTrackerForNode(node, (tracker, n) -> tracker.onSuccess(n, withFastPathTimestamp));
    }

    public boolean hasMetFastPathCriteria()
    {
        return all(FastPathShardTracker::hasMetFastPathCriteria);
    }
}
