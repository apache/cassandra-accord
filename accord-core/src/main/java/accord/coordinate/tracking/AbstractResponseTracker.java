package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

public abstract class AbstractResponseTracker<T extends AbstractResponseTracker.ShardTracker>
{
    private final Topologies topologies;
    private final T[][] trackerSets;

    public static class ShardTracker
    {
        public final Shard shard;

        public ShardTracker(Shard shard)
        {
            this.shard = shard;
        }
    }

    public AbstractResponseTracker(Topologies topologies, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        this.topologies = topologies;
        this.trackerSets = (T[][]) new ShardTracker[topologies.size()][];
        this.topologies.forEach((i, topology) -> {
            T[] trackers = arrayFactory.apply(topology.size());
            topology.forEach((j, shard) -> trackers[j] = trackerFactory.apply(shard));
            this.trackerSets[i] = trackers;
        });
    }

    protected void forEachTrackerForNode(Node.Id node, BiConsumer<T, Node.Id> consumer)
    {
        this.topologies.forEach((i, topology) -> {
            T[] trackers = trackerSets[i];
            topology.forEachOn(node, (j, shard) -> consumer.accept(trackers[j], node));
        });
    }

    protected int matchingTrackersForNode(Node.Id node, Predicate<T> consumer)
    {
        int matches = 0;
        for (int i=0, mi=topologies.size(); i<mi; i++)
        {
            Topology topology = topologies.get(i);
            T[] trackers = trackerSets[i];
            matches += topology.matchesOn(node, (j, shard) -> consumer.test(trackers[j]));
        }
        return matches;
    }

    protected boolean all(Predicate<T> predicate)
    {
        for (T[] trackers : trackerSets)
            for (T tracker : trackers)
                if (!predicate.test(tracker))
                    return false;
        return true;
    }

    protected boolean any(Predicate<T> predicate)
    {
        for (T[] trackers : trackerSets)
            for (T tracker : trackers)
                if (predicate.test(tracker))
                    return true;
        return false;
    }

    protected <V> V accumulate(BiFunction<T, V, V> function, V start)
    {
        for (T[] trackers : trackerSets)
            for (T tracker : trackers)
                start = function.apply(tracker, start);
        return start;
    }

    public Set<Node.Id> nodes()
    {
        return topologies.nodes();
    }

    @VisibleForTesting
    public T unsafeGet(int topologyIdx, int shardIdx)
    {
        return trackerSets[topologyIdx][shardIdx];
    }

    public T unsafeGet(int i)
    {
        Preconditions.checkArgument(trackerSets.length == 1);
        return unsafeGet(0, i);
    }
}
