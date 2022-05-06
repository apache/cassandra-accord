package accord.coordinate.tracking;

import accord.local.Node.Id;
import accord.topology.Shard;

import accord.topology.Topologies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;

public class ReadTracker extends AbstractResponseTracker<ReadTracker.ReadShardTracker>
{
    public static class ReadShardTracker extends AbstractResponseTracker.ShardTracker
    {
        protected final Set<Id> inflight = new HashSet<>();
        private boolean hasData = false;
        private int contacted;

        public ReadShardTracker(Shard shard)
        {
            super(shard);
        }

        public void recordInflightRead(Id node)
        {
            if (!inflight.add(node))
                throw new IllegalStateException();
            ++contacted;
        }

        public boolean recordReadSuccess(Id node)
        {
            Preconditions.checkArgument(shard.nodes.contains(node));
            hasData = true;
            return inflight.remove(node);
        }

        public boolean shouldRead()
        {
            return !hasData && inflight.isEmpty();
        }

        public boolean recordReadFailure(Id node)
        {
            return inflight.remove(node);
        }

        public boolean hasCompletedRead()
        {
            return hasData;
        }

        public boolean hasFailed()
        {
            return !hasData && inflight.isEmpty() && contacted == shard.nodes.size();
        }
    }

    // TODO: abstract the candidate selection process so the implementation may prioritise based on distance/health etc
    private final List<Id> candidates;

    public ReadTracker(Topologies topologies)
    {
        super(topologies, ReadShardTracker[]::new, ReadShardTracker::new);
        candidates = new ArrayList<>(topologies.nodes());
    }

    @VisibleForTesting
    void recordInflightRead(Id node)
    {
        forEachTrackerForNode(node, ReadShardTracker::recordInflightRead);
    }

    public void recordReadSuccess(Id node)
    {
        forEachTrackerForNode(node, ReadShardTracker::recordReadSuccess);
    }

    public void recordReadFailure(Id node)
    {
        forEachTrackerForNode(node, ReadShardTracker::recordReadFailure);
    }

    public boolean hasCompletedRead()
    {
        return all(ReadShardTracker::hasCompletedRead);
    }

    public boolean hasFailed()
    {
        return any(ReadShardTracker::hasFailed);
    }

    private int intersectionSize(Id node, Set<ReadShardTracker> target)
    {
        return matchingTrackersForNode(node, target::contains);
    }

    private int compareIntersections(Id left, Id right, Set<ReadShardTracker> target)
    {
        return Integer.compare(intersectionSize(left, target), intersectionSize(right, target));
    }

    /**
     * Return the smallest set of nodes needed to satisfy required reads.
     *
     * Returns null if the read cannot be completed.
     *
     * TODO: prioritisation of nodes should be implementation-defined
     */
    public Set<Id> computeMinimalReadSetAndMarkInflight()
    {
        Set<ReadShardTracker> toRead = foldl((tracker, accumulate) -> {
            if (!tracker.shouldRead())
                return accumulate;

            if (accumulate == null)
                accumulate = new HashSet<>();

            accumulate.add(tracker);
            return accumulate;
        }, null);

        if (toRead == null)
            return Collections.emptySet();

        assert !toRead.isEmpty();
        Set<Id> nodes = new HashSet<>();
        while (!toRead.isEmpty())
        {
            if (candidates.isEmpty())
                return null;

            // TODO: Topology needs concept of locality/distance
            candidates.sort((a, b) -> compareIntersections(a, b, toRead));

            int i = candidates.size() - 1;
            Id node = candidates.get(i);
            nodes.add(node);
            recordInflightRead(node);
            candidates.remove(i);
            forEachTrackerForNode(node, (tracker, ignore) -> toRead.remove(tracker));
        }

        return nodes;
    }

}
