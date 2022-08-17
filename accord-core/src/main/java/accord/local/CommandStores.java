/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.local;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.local.CommandStore.RangesForEpoch;
import accord.primitives.KeyRanges;
import accord.topology.Topology;
import accord.primitives.Keys;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import accord.messages.TxnRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.*;
import java.util.concurrent.ExecutionException;

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

    protected interface Fold<I1, I2, O>
    {
        O fold(CommandStore store, I1 i1, I2 i2, O accumulator);
    }

    protected interface Select<Scope>
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

    protected static class Supplier
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

    protected static class ShardedRanges
    {
        final CommandStore[] shards;
        final long[] epochs;
        final KeyRanges[] ranges;

        protected ShardedRanges(CommandStore[] shards, long epoch, KeyRanges ranges)
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

        public long all()
        {
            return -1L >>> (64 - shards.length);
        }

        public long shards(Keys keys, long minEpoch, long maxEpoch)
        {
            long accumulate = 0L;
            for (int i = Math.max(0, indexForEpoch(minEpoch)), maxi = indexForEpoch(maxEpoch); i <= maxi ; ++i)
            {
                accumulate = keys.foldl(ranges[i], ShardedRanges::addKeyIndex, shards.length, accumulate, -1L);
            }
            return accumulate;
        }

        long shards(TxnRequest request, long minEpoch, long maxEpoch)
        {
            return shards(request.scope(), minEpoch, maxEpoch);
        }

        long shard(Key scope, long minEpoch, long maxEpoch)
        {
            long result = 0L;
            for (int i = Math.max(0, indexForEpoch(minEpoch)), maxi = indexForEpoch(maxEpoch); i <= maxi ; ++i)
            {
                int index = ranges[i].rangeIndexForKey(scope);
                if (index < 0)
                    continue;
                result = addKeyIndex(0, scope, shards.length, result);
            }
            return result;
        }

        KeyRanges currentRanges()
        {
            return ranges[ranges.length - 1];
        }

        static long keyIndex(Key key, long numShards)
        {
            return Integer.toUnsignedLong(key.routingHash()) % numShards;
        }

        private static long addKeyIndex(int i, Key key, long numShards, long accumulate)
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

    protected CommandStores(Supplier supplier)
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

            @Override
            public boolean intersects(long epoch, Keys keys)
            {
                KeyRanges ranges = at(epoch);
                return ranges != null && ranges.intersects(keys);
            }
        };
    }

    public synchronized void shutdown()
    {
        for (ShardedRanges group : current.ranges)
            for (CommandStore commandStore : group.shards)
                commandStore.shutdown();
    }

    private static <T> Fold<TxnOperation, Void, List<Future<T>>> mapReduceFold(Function<CommandStore, T> map)
    {
        return (store, op, i, t) -> { t.add(store.process(op, map)); return t; };
    }

    private static <T> T reduce(List<Future<T>> futures, BiFunction<T, T, T> reduce)
    {
        T result = null;
        for (Future<T> future : futures)
        {
            try
            {
                T next = future.get();
                result = result == null ? next : reduce.apply(result, next);
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

    private <F, T> T setup(F f, Fold<F, ?, List<Future<T>>> fold, BiFunction<T, T, T> reduce)
    {
        List<Future<T>> futures = foldl((s, i, mn, mx) -> s.all(), null, Long.MIN_VALUE, Long.MAX_VALUE, fold, f, null, ArrayList::new);
        return reduce(futures, reduce);
    }

    private  <S, T> T mapReduce(TxnOperation operation, Select<S> select, S scope, long minEpoch, long maxEpoch, Fold<TxnOperation, Void, List<Future<T>>> fold, BiFunction<T, T, T> reduce)
    {
        List<Future<T>> futures = foldl(select, scope, minEpoch, maxEpoch, fold, operation, null, ArrayList::new);
        if (futures == null)
            return null;
        return reduce(futures, reduce);
    }

    public <T> T mapReduce(TxnOperation operation, Key key, long minEpoch, long maxEpoch, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(operation, ShardedRanges::shard, key, minEpoch, maxEpoch, mapReduceFold(map), reduce);
    }

    public <T> T mapReduce(TxnOperation operation, Key key, long epoch, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(operation, key, epoch, epoch, map, reduce);
    }

    public <T> T mapReduceSince(TxnOperation operation, Key key, long epoch, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(operation, key, epoch, Long.MAX_VALUE, map, reduce);
    }

    public <T> T mapReduce(TxnOperation operation, Keys keys, long minEpoch, long maxEpoch, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        // probably need to split txnOperation and scope stuff here
        return mapReduce(operation, ShardedRanges::shards, keys, minEpoch, maxEpoch, mapReduceFold(map), reduce);
    }

    public void setup(Consumer<CommandStore> forEach)
    {
        setup(forEach, (store, f, i, t) -> { t.add(store.processSetup(f)); return t; }, (Void i1, Void i2) -> null);
    }

    public <T> T setup(Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return setup(map, (store, f, i, t) -> { t.add(store.processSetup(f)); return t; }, reduce);
    }

    private static Fold<TxnOperation, Void, List<Future<Void>>> forEachFold(Consumer<CommandStore> forEach)
    {
        return (store, op, i, t) -> { t.add(store.process(op, forEach)); return t; };
    }

    public void forEach(TxnOperation operation, Keys keys, long minEpoch, long maxEpoch, Consumer<CommandStore> forEach)
    {
        mapReduce(operation, ShardedRanges::shards, keys, minEpoch, maxEpoch, forEachFold(forEach), (o1, o2) -> null);
    }

    public void forEach(TxnOperation operation, Consumer<CommandStore> forEach)
    {
        mapReduce(operation, (s, i, min, max) -> s.all(), null, 0, 0, forEachFold(forEach), (o1, o2) -> null);
    }

    public void forEach(TxnRequest request, long epoch, Consumer<CommandStore> forEach)
    {
        forEach(request, request.scope(), epoch, epoch, forEach);
    }

    public void forEach(TxnOperation operation, Keys keys, long epoch, Consumer<CommandStore> forEach)
    {
        forEach(operation, keys, epoch, epoch, forEach);
    }
    public void forEachSince(TxnOperation operation, Keys keys, long epoch, Consumer<CommandStore> forEach)
    {
        forEach(operation, keys, epoch, Long.MAX_VALUE, forEach);
    }

    public void forEach(TxnRequest request, long minEpoch, long maxEpoch, Consumer<CommandStore> forEach)
    {
        forEach(request, request.scope(), minEpoch, maxEpoch, forEach);
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

    protected <S, I1, I2, O> O foldl(Select<S> select, S scope, long minEpoch, long maxEpoch, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, IntFunction<? extends O> factory)
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

    public static Future<?> forEachNonBlocking(Collection<CommandStore> commandStores, TxnOperation scope, Consumer<CommandStore> forEach)
    {
        List<Future<Void>> futures = new ArrayList<>(commandStores.size());
        for (CommandStore commandStore : commandStores)
            futures.add(commandStore.process(scope, forEach));
        return FutureCombiner.allOf(futures);
    }

    public static void forEachBlocking(Collection<CommandStore> commandStores, TxnOperation scope, Consumer<CommandStore> forEach)
    {
        try
        {
            forEachNonBlocking(commandStores, scope, forEach).get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static <T> T mapReduce(Collection<CommandStore> commandStores, TxnOperation scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        List<Future<T>> futures = new ArrayList<>(commandStores.size());
        for (CommandStore commandStore : commandStores)
            futures.add(commandStore.process(scope, map));
        return reduce(futures, reduce);
    }
}
