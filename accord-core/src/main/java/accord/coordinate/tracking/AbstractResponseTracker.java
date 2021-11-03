package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topology;
import com.google.common.annotations.VisibleForTesting;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

public abstract class AbstractResponseTracker<T extends AbstractResponseTracker.ShardTracker>
{
    private final Topology topology;
    private final T[] trackers;

    public static class ShardTracker
    {
        public final Shard shard;

        public ShardTracker(Shard shard)
        {
            this.shard = shard;
        }
    }

    public AbstractResponseTracker(Topology topology, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        this.topology = topology;
        this.trackers = arrayFactory.apply(topology.size());
        topology.forEach((i, shard) -> trackers[i] = trackerFactory.apply(shard));
    }

    protected void forEachTrackerForNode(Node.Id node, BiConsumer<T, Node.Id> consumer)
    {
        topology.forEachOn(node, (i, shard) -> consumer.accept(trackers[i], node));
    }

    protected int matchingTrackersForNode(Node.Id node, Predicate<T> consumer)
    {
        return topology.matchesOn(node, (i, shard) -> consumer.test(trackers[i]));
    }

    protected boolean all(Predicate<T> predicate)
    {
        for (T tracker : trackers)
            if (!predicate.test(tracker))
                return false;
        return true;
    }

    protected boolean any(Predicate<T> predicate)
    {
        for (T tracker : trackers)
            if (predicate.test(tracker))
                return true;
        return false;
    }

    protected <V> V accumulate(BiFunction<T, V, V> function, V start)
    {
        for (T tracker : trackers)
            start = function.apply(tracker, start);
        return start;
    }

    public Set<Node.Id> nodes()
    {
        return topology.nodes();
    }

    @VisibleForTesting
    public T unsafeGet(int i)
    {
        return trackers[i];
    }
}
