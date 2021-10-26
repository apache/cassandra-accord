package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Shards;
import com.google.common.annotations.VisibleForTesting;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

abstract class AbstractResponseTracker<T extends AbstractResponseTracker.ShardTracker>
{
    private final Shards shards;
    private final T[] trackers;

    static class ShardTracker
    {
        public final Shard shard;

        public ShardTracker(Shard shard)
        {
            this.shard = shard;
        }
    }

    public AbstractResponseTracker(Shards shards, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        this.shards = shards;
        this.trackers = arrayFactory.apply(shards.size());
        shards.forEach((i, shard) -> trackers[i] = trackerFactory.apply(shard));
    }

    void forEachTrackerForNode(Node.Id node, BiConsumer<T, Node.Id> consumer)
    {
        shards.forEachOn(node, (i, shard) -> consumer.accept(trackers[i], node));
    }

    int matchingTrackersForNode(Node.Id node, Predicate<T> consumer)
    {
        return shards.matchesOn(node, (i, shard) -> consumer.test(trackers[i]));
    }

    boolean all(Predicate<T> predicate)
    {
        for (T tracker : trackers)
            if (!predicate.test(tracker))
                return false;
        return true;
    }

    boolean any(Predicate<T> predicate)
    {
        for (T tracker : trackers)
            if (predicate.test(tracker))
                return true;
        return false;
    }

    <V> V accumulate(BiFunction<T, V, V> function, V start)
    {
        for (T tracker : trackers)
            start = function.apply(tracker, start);
        return start;
    }

    public Set<Node.Id> nodes()
    {
        return shards.nodes();
    }

    @VisibleForTesting
    public T unsafeGet(int i)
    {
        return trackers[i];
    }
}
