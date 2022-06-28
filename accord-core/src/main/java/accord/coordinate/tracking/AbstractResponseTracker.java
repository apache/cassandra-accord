package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.IndexedIntFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

public abstract class AbstractResponseTracker<T extends AbstractResponseTracker.ShardTracker>
{
    private static final int[] SINGLETON_OFFSETS = new int[0];
    private final Topologies topologies;
    private final T[] trackers;
    private final int[] offsets;

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
        this.trackers = arrayFactory.apply(topologies.totalShards());

        if (topologies.size() > 1)
        {
            this.offsets = new int[topologies.size() - 1];
            int offset = topologies.get(0).size();
            for (int i=1, mi=topologies.size(); i<mi; i++)
            {
                this.offsets[i - 1] = offset;
                offset += topologies.get(i).size();
            }
        }
        else
        {
            this.offsets = SINGLETON_OFFSETS;
        }

        this.topologies.forEach((i, topology) -> {
            int offset = topologyOffset(i);
            topology.forEach((j, shard) -> trackers[offset + j] = trackerFactory.apply(shard));
        });
    }

    protected int topologyOffset(int topologyIdx)
    {
        return topologyIdx > 0 ? offsets[topologyIdx - 1] : 0;
    }

    protected int topologyLength(int topologyIdx)
    {
        if (topologyIdx > offsets.length)
            throw new IndexOutOfBoundsException();

        int endIdx = topologyIdx == offsets.length ? trackers.length : topologyOffset(topologyIdx + 1);
        return endIdx - topologyOffset(topologyIdx);
    }

    public Topologies topologies()
    {
        return topologies;
    }

    protected void forEachTrackerForNode(Node.Id node, BiConsumer<T, Node.Id> consumer)
    {
        this.topologies.forEach((i, topology) -> {
            int offset = topologyOffset(i);
            topology.forEachOn(node, (j, shard) -> consumer.accept(trackers[offset + j], node));
        });
    }

    protected boolean anyForNode(Node.Id node, BiPredicate<T, Node.Id> consumer)
    {
        return matchingTrackersForNode(node, consumer, 1) == 1;
    }

    protected boolean allForNode(Node.Id node, BiPredicate<T, Node.Id> consumer)
    {
        return nonMatchingTrackersForNode(node, consumer, Integer.MAX_VALUE) == 0;
    }

    protected int nonMatchingTrackersForNode(Node.Id node, BiPredicate<T, Node.Id> consumer, int limit)
    {
        return foldlForNode(node, (shardIndex, shard, v) -> consumer.test(trackers[shardIndex], node) ? v : v + 1, 0, limit);
    }

    protected int matchingTrackersForNode(Node.Id node, BiPredicate<T, Node.Id> consumer, int limit)
    {
        return foldlForNode(node, (shardIndex, shard, v) -> consumer.test(trackers[shardIndex], node) ? v + 1 : v, 0, limit);
    }

    protected int matchingTrackersForNode(Node.Id node, Predicate<T> consumer)
    {
        return foldlForNode(node, (shardIndex, shard, v) -> consumer.test(trackers[shardIndex]) ? v + 1 : v, 0, Integer.MAX_VALUE);
    }

    protected int foldlForNode(Node.Id node, IndexedIntFunction<Shard> function, int initialValue, int terminalValue)
    {
        for (int i = 0 ; i < topologies.size() && initialValue != terminalValue ; ++i)
        {
            initialValue = topologies.get(i).foldlIntOn(node, function, topologyOffset(i), initialValue, terminalValue);
        }
        return initialValue;
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

    protected <V> V foldl(BiFunction<T, V, V> function, V accumulator)
    {
        for (T tracker : trackers)
            accumulator = function.apply(tracker, accumulator);
        return accumulator;
    }

    public Set<Node.Id> nodes()
    {
        return topologies.nodes();
    }

    @VisibleForTesting
    public T unsafeGet(int topologyIdx, int shardIdx)
    {
        if (shardIdx >= topologyLength(topologyIdx))
            throw new IndexOutOfBoundsException();
        return trackers[topologyOffset(topologyIdx) + shardIdx];
    }

    public T unsafeGet(int i)
    {
        Preconditions.checkArgument(offsets.length == 0);
        return unsafeGet(0, i);
    }
}
