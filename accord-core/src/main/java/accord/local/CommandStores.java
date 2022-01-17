package accord.local;

import accord.api.Agent;
import accord.api.Key;
import accord.api.DataStore;
import accord.local.CommandStore.RangesForEpoch;
import accord.messages.TxnRequest;
import accord.api.ProgressLog;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.*;

import static java.lang.Boolean.FALSE;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores
{
    public interface Factory
    {
        CommandStores create(int num,
                             Node node,
                             Agent agent,
                             DataStore store,
                             ProgressLog.Factory progressLogFactory);
    }

    interface Fold<I1, I2, O>
    {
        O fold(CommandStore store, I1 i1, I2 i2, O accumulator);
    }

    interface Select<Scope>
    {
        long select(ShardedRanges ranges, Scope scope, long minEpoch, long maxEpoch);
    }

    static class KeysAndEpoch
    {
        final Keys keys;
        final long epoch;

        KeysAndEpoch(Keys keys, long epoch)
        {
            this.keys = keys;
            this.epoch = epoch;
        }
    }

    static class KeysAndEpochRange
    {
        final Keys keys;
        final long minEpoch;
        final long maxEpoch;

        KeysAndEpochRange(Keys keys, long minEpoch, long maxEpoch)
        {
            this.keys = keys;
            this.minEpoch = minEpoch;
            this.maxEpoch = maxEpoch;
        }
    }

    static class KeyAndEpoch
    {
        final Key key;
        final long epoch;

        KeyAndEpoch(Key key, long epoch)
        {
            this.key = key;
            this.epoch = epoch;
        }
    }

    private static class Supplier
    {
        private final Node node;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final CommandStore.Factory shardFactory;
        private final int numShards;

        Supplier(Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory, int numShards)
        {
            this.node = node;
            this.agent = agent;
            this.store = store;
            this.progressLogFactory = progressLogFactory;
            this.shardFactory = shardFactory;
            this.numShards = numShards;
        }

        CommandStore create(int generation, int shardIndex, RangesForEpoch rangesForEpoch)
        {
            return shardFactory.create(generation, shardIndex, numShards, node, agent, store, progressLogFactory, rangesForEpoch);
        }

        ShardedRanges createShardedRanges(int generation, long epoch, KeyRanges ranges, RangesForEpoch rangesForEpoch)
        {
            CommandStore[] newStores = new CommandStore[numShards];
            for (int i=0; i<numShards; i++)
                newStores[i] = create(generation, i, rangesForEpoch);

            return new ShardedRanges(newStores, epoch, ranges);
        }
    }

    static class ShardedRanges
    {
        final CommandStore[] shards;
        final long[] epochs;
        final KeyRanges[] ranges;

        public ShardedRanges(CommandStore[] shards, long epoch, KeyRanges ranges)
        {
            Preconditions.checkArgument(shards.length <= 64);
            this.shards = shards;
            this.epochs = new long[] { epoch };
            this.ranges = new KeyRanges[] { ranges };
        }

        private ShardedRanges(CommandStore[] shards, long[] epochs, KeyRanges[] ranges)
        {
            Preconditions.checkArgument(shards.length <= 64);
            this.shards = shards;
            this.epochs = epochs;
            this.ranges = ranges;
        }

        ShardedRanges withRanges(long epoch, KeyRanges ranges)
        {
            long[] newEpochs = Arrays.copyOf(this.epochs, this.epochs.length + 1);
            KeyRanges[] newRanges = Arrays.copyOf(this.ranges, this.ranges.length + 1);
            newEpochs[this.epochs.length] = epoch;
            newRanges[this.ranges.length] = ranges;
            return new ShardedRanges(shards, newEpochs, newRanges);
        }

        KeyRanges rangesForEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 -i;
            if (i < 0) return null;
            return ranges[i];
        }

        KeyRanges rangesSinceEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = Math.max(0, -2 -i);
            KeyRanges result = ranges[i++];
            while (i < ranges.length)
                result = ranges[i++].union(result);
            return result;
        }

        int indexForEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 -i;
            return i;
        }

        long all()
        {
            return -1L >>> (64 - shards.length);
        }

        long shards(Keys keys, long minEpoch, long maxEpoch)
        {
            long accumulate = 0L;
            // TODO (now): it should be safe to only evaluate in the two precise epochs that are relevant, but confirm
            for (int i = Math.max(0, indexForEpoch(minEpoch)), maxi = indexForEpoch(maxEpoch); i <= maxi ; ++i)
            {
                accumulate = keys.foldl(ranges[i], ShardedRanges::addKeyIndex, shards.length, accumulate, -1L);
            }
            return accumulate;
        }

        long shard(Key scope, long minEpoch, long maxEpoch)
        {
            long result = 0L;
            for (int i = Math.max(0, indexForEpoch(minEpoch)), maxi = indexForEpoch(maxEpoch); i <= maxi ; ++i)
            {
                int index = ranges[i].rangeIndexForKey(scope);
                if (index < 0)
                    continue;
                result = addKeyIndex(scope, shards.length, result);
            }
            return result;
        }

        KeyRanges currentRanges()
        {
            return ranges[ranges.length - 1];
        }

        static long keyIndex(Key key, long numShards)
        {
            return Integer.toUnsignedLong(key.keyHash()) % numShards;
        }

        private static long addKeyIndex(Key key, long numShards, long accumulate)
        {
            return accumulate | (1L << keyIndex(key, numShards));
        }
    }

    static class Snapshot
    {
        final ShardedRanges[] ranges;
        final Topology global;
        final Topology local;
        final int size;

        Snapshot(ShardedRanges[] ranges, Topology global, Topology local)
        {
            this.ranges = ranges;
            this.global = global;
            this.local = local;
            int size = 0;
            for (ShardedRanges group : ranges)
                size += group.shards.length;
            this.size = size;
        }
    }

    final Supplier supplier;
    volatile Snapshot current;

    private CommandStores(Supplier supplier)
    {
        this.supplier = supplier;
        this.current = new Snapshot(new ShardedRanges[0], Topology.EMPTY, Topology.EMPTY);
    }

    public CommandStores(int num, Node node, Agent agent, DataStore store,
                         ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        this(new Supplier(node, agent, store, progressLogFactory, shardFactory, num));
    }

    public Topology local()
    {
        return current.local;
    }

    public Topology global()
    {
        return current.global;
    }

    private Snapshot updateTopology(Snapshot prev, Topology newTopology)
    {
        Preconditions.checkArgument(!newTopology.isSubset(), "Use full topology for CommandStores.updateTopology");

        long epoch = newTopology.epoch();
        if (epoch <= prev.global.epoch())
            return prev;

        Topology newLocalTopology = newTopology.forNode(supplier.node.id());
        KeyRanges added = newLocalTopology.ranges().difference(prev.local.ranges());
        KeyRanges subtracted = prev.local.ranges().difference(newLocalTopology.ranges());
//            for (ShardedRanges range : stores.ranges)
//            {
//                // FIXME: remove this (and the corresponding check in TopologyRandomizer) once lower bounds are implemented.
//                //  In the meantime, the logic needed to support acquiring ranges that we previously replicated is pretty
//                //  convoluted without the ability to jettison epochs.
//                Preconditions.checkState(!range.ranges.intersects(added));
//            }

        if (added.isEmpty() && subtracted.isEmpty())
            return new Snapshot(prev.ranges, newTopology, newLocalTopology);

        ShardedRanges[] result = new ShardedRanges[prev.ranges.length + (added.isEmpty() ? 0 : 1)];
        if (subtracted.isEmpty())
        {
            int newGeneration = prev.ranges.length;
            System.arraycopy(prev.ranges, 0, result, 0, newGeneration);
            result[newGeneration] = supplier.createShardedRanges(newGeneration, epoch, added, rangesForEpochFunction(newGeneration));
        }
        else
        {
            int i = 0;
            while (i < prev.ranges.length)
            {
                ShardedRanges ranges = prev.ranges[i];
                if (ranges.currentRanges().intersects(subtracted))
                    ranges = ranges.withRanges(newTopology.epoch(), ranges.currentRanges().difference(subtracted));
                result[i++] = ranges;
            }
            if (i < result.length)
                result[i] = supplier.createShardedRanges(i, epoch, added, rangesForEpochFunction(i));
        }

        return new Snapshot(result, newTopology, newLocalTopology);
    }

    private RangesForEpoch rangesForEpochFunction(int generation)
    {
        return new RangesForEpoch()
        {

            @Override
            public KeyRanges at(long epoch)
            {
                return current.ranges[generation].rangesForEpoch(epoch);
            }

            @Override
            public KeyRanges since(long epoch)
            {
                return current.ranges[generation].rangesSinceEpoch(epoch);
            }
        };
    }

    public synchronized void shutdown()
    {
        for (ShardedRanges group : current.ranges)
            for (CommandStore commandStore : group.shards)
                commandStore.shutdown();
    }

    protected abstract <S> void forEach(Select<S> select, S scope, long minEpoch, long maxEpoch, Consumer<? super CommandStore> forEach);
    protected abstract <S, T> T mapReduce(Select<S> select, S scope, long minEpoch, long maxEpoch, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce);

    public void forEach(Consumer<CommandStore> forEach)
    {
        forEach((s, i, min, max) -> s.all(), null, 0, 0, forEach);
    }

    public void forEach(Keys keys, long epoch, Consumer<CommandStore> forEach)
    {
        forEach(keys, epoch, epoch, forEach);
    }

    public void forEach(Keys keys, long minEpoch, long maxEpoch, Consumer<CommandStore> forEach)
    {
        forEach(ShardedRanges::shards, keys, minEpoch, maxEpoch, forEach);
    }

    public <T> T mapReduce(Keys keys, long epoch, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(keys, epoch, epoch, map, reduce);
    }

    public <T> T mapReduce(Keys keys, long minEpoch, long maxEpoch, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(ShardedRanges::shards, keys, minEpoch, maxEpoch, map, reduce);
    }

    public <T> T mapReduce(Key key, long epoch, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(ShardedRanges::shard, key, epoch, epoch, map, reduce);
    }

    public <T extends Collection<CommandStore>> T collect(Keys keys, long epoch, IntFunction<T> factory)
    {
        return foldl(ShardedRanges::shards, keys, epoch, epoch, CommandStores::append, null, null, factory);
    }

    public <T extends Collection<CommandStore>> T collect(Keys keys, long minEpoch, long maxEpoch, IntFunction<T> factory)
    {
        return foldl(ShardedRanges::shards, keys, minEpoch, maxEpoch, CommandStores::append, null, null, factory);
    }

    public synchronized void updateTopology(Topology newTopology)
    {
        current = updateTopology(current, newTopology);
    }

    private static <T extends Collection<CommandStore>> T append(CommandStore store, Object ignore1, Object ignore2, T to)
    {
        to.add(store);
        return to;
    }

    private <I1, I2, O> O foldl(int startGroup, long bitset, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, O accumulator)
    {
        ShardedRanges[] ranges = current.ranges;
        int groupIndex = startGroup;
        ShardedRanges group = ranges[groupIndex];
        int offset = 0;
        while (true)
        {
            int i = Long.numberOfTrailingZeros(bitset) - offset;
            while (i < group.shards.length)
            {
                accumulator = fold.fold(group.shards[i], param1, param2, accumulator);
                bitset ^= Long.lowestOneBit(bitset);
                i = Long.numberOfTrailingZeros(bitset) - offset;
            }

            if (++groupIndex == ranges.length)
                break;

            if (bitset == 0)
                break;

            offset += group.shards.length;
            group = ranges[groupIndex];
            if (offset + group.shards.length > 64)
                break;
        }
        return accumulator;
    }

    <S, I1, I2, O> O foldl(Select<S> select, S scope, long minEpoch, long maxEpoch, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, IntFunction<? extends O> factory)
    {
        ShardedRanges[] ranges = current.ranges;
        O accumulator = null;
        int startGroup = 0;
        while (startGroup < ranges.length)
        {
            long bits = select.select(ranges[startGroup], scope, minEpoch, maxEpoch);
            if (bits == 0)
            {
                ++startGroup;
                continue;
            }

            int offset = ranges[startGroup].shards.length;
            int endGroup = startGroup + 1;
            while (endGroup < ranges.length)
            {
                ShardedRanges group = ranges[endGroup];
                if (offset + group.shards.length > 64)
                    break;

                bits += select.select(group, scope, minEpoch, maxEpoch) << offset;
                offset += group.shards.length;
                ++endGroup;
            }

            if (accumulator == null)
                accumulator = factory.apply(Long.bitCount(bits));

            accumulator = foldl(startGroup, bits, fold, param1, param2, accumulator);
            startGroup = endGroup;
        }

        return accumulator;
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(Key key)
    {
        ShardedRanges[] ranges = current.ranges;
        for (ShardedRanges group : ranges)
        {
            if (group.currentRanges().contains(key))
            {
                for (CommandStore store : group.shards)
                {
                    if (store.hashIntersects(key))
                        return store;
                }
            }
        }
        throw new IllegalArgumentException();
    }

    public static class Synchronized extends CommandStores
    {
        public Synchronized(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory, CommandStore.Synchronized::new);
        }

        public Synchronized(Supplier supplier)
        {
            super(supplier);
        }

        @Override
        protected <S, T> T mapReduce(Select<S> select, S scope, long minEpoch, long maxEpoch, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return foldl(select, scope, minEpoch, maxEpoch, (store, f, r, t) -> t == null ? f.apply(store) : r.apply(t, f.apply(store)), map, reduce, ignore -> null);
        }

        @Override
        protected <S> void forEach(Select<S> select, S scope, long minEpoch, long maxEpoch, Consumer<? super CommandStore> forEach)
        {
            foldl(select, scope, minEpoch, maxEpoch, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
        }
    }

    public static class SingleThread extends CommandStores
    {
        public SingleThread(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            this(num, node, agent, store, progressLogFactory, CommandStore.SingleThread::new);
        }

        public SingleThread(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
        {
            super(num, node, agent, store, progressLogFactory, shardFactory);
        }

        private <S, F, T> T mapReduce(Select<S> select, S scope, long minEpoch, long maxEpoch, F f, Fold<F, ?, List<Future<T>>> fold, BiFunction<T, T, T> reduce)
        {
            List<Future<T>> futures = foldl(select, scope, minEpoch, maxEpoch, fold, f, null, ArrayList::new);
            T result = null;
            for (Future<T> future : futures)
            {
                try
                {
                    T next = future.get();
                    if (result == null) result = next;
                    else result = reduce.apply(result, next);
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e.getCause());
                }
            }
            return result;
        }

        @Override
        protected <S, T> T mapReduce(Select<S> select, S scope, long minEpoch, long maxEpoch, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return mapReduce(select, scope, minEpoch, maxEpoch, map, (store, f, i, t) -> { t.add(store.process(f)); return t; }, reduce);
        }

        protected <S> void forEach(Select<S> select, S scope, long minEpoch, long maxEpoch, Consumer<? super CommandStore> forEach)
        {
            mapReduce(select, scope, minEpoch, maxEpoch, forEach, (store, f, i, t) -> { t.add(store.process(f)); return t; }, (Void i1, Void i2) -> null);
        }
    }

    public static class Debug extends SingleThread
    {
        public Debug(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory, CommandStore.Debug::new);
        }
    }

}
