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
        this.topologies.forEach((j, topology) -> {
            T[] trackers = arrayFactory.apply(topology.size());
            topology.forEach((i, shard) -> trackers[i] = trackerFactory.apply(shard));
            this.trackerSets[j] = trackers;
        });
    }

    protected void forEachTrackerForNode(Node.Id node, BiConsumer<T, Node.Id> consumer)
    {
        this.topologies.forEach((j, topology) -> {
            T[] trackers = trackerSets[j];
            topology.forEachOn(node, (i, shard) -> consumer.accept(trackers[i], node));
        });
    }

    protected int matchingTrackersForNode(Node.Id node, Predicate<T> consumer)
    {
        int matches = 0;
        for (int j=0, mj=topologies.size(); j<mj; j++)
        {
            Topology topology = topologies.get(j);
            T[] trackers = trackerSets[j];
            matches += topology.matchesOn(node, (i, shard) -> consumer.test(trackers[i]));
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
    public T unsafeGet(int topologyIdx, int i)
    {
        return trackerSets[topologyIdx][i];
    }

    public T unsafeGet(int i)
    {
        Preconditions.checkArgument(trackerSets.length == 1);
        return unsafeGet(0, i);
    }
}
