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

import accord.api.*;
import accord.primitives.*;
import accord.api.RoutingKey;
import accord.topology.Topology;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;

import com.google.common.annotations.VisibleForTesting;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2ObjectHashMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static accord.local.PreLoadContext.empty;
import static accord.utils.Invariants.checkArgument;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores<S extends CommandStore>
{
    public interface Factory
    {
        CommandStores<?> create(NodeTimeService time,
                                Agent agent,
                                DataStore store,
                                ShardDistributor shardDistributor,
                                ProgressLog.Factory progressLogFactory);
    }

    private static class Supplier
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final CommandStore.Factory shardFactory;

        Supplier(NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.progressLogFactory = progressLogFactory;
            this.shardFactory = shardFactory;
        }

        CommandStore create(int id, RangesForEpochHolder rangesForEpoch)
        {
            return shardFactory.create(id, time, agent, store, progressLogFactory, rangesForEpoch);
        }
    }

    public static class RangesForEpochHolder
    {
        // no need for safe publication; RangesForEpoch members are final, and will be guarded by other synchronization actions
        protected RangesForEpoch current;

        /**
         * This is updated asynchronously, so should only be fetched between executing tasks;
         * otherwise the contents may differ between invocations for the same task
         * @return the current RangesForEpoch
         */
        public RangesForEpoch get() { return current; }
    }

    static class ShardHolder
    {
        final CommandStore store;
        final RangesForEpochHolder ranges;

        ShardHolder(CommandStore store, RangesForEpochHolder ranges)
        {
            this.store = store;
            this.ranges = ranges;
        }

        RangesForEpoch ranges()
        {
            return ranges.current;
        }
    }

    public static class RangesForEpoch
    {
        final long[] epochs;
        final Ranges[] ranges;

        public RangesForEpoch(long epoch, Ranges ranges)
        {
            this.epochs = new long[] { epoch };
            this.ranges = new Ranges[] { ranges };
        }

        public RangesForEpoch(long[] epochs, Ranges[] ranges)
        {
            this.epochs = epochs;
            this.ranges = ranges;
        }

        public RangesForEpoch withRanges(long epoch, Ranges ranges)
        {
            long[] newEpochs = Arrays.copyOf(this.epochs, this.epochs.length + 1);
            Ranges[] newRanges = Arrays.copyOf(this.ranges, this.ranges.length + 1);
            newEpochs[this.epochs.length] = epoch;
            newRanges[this.ranges.length] = ranges;
            return new RangesForEpoch(newEpochs, newRanges);
        }

        public Ranges at(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 -i;
            if (i < 0) return Ranges.EMPTY;
            return ranges[i];
        }

        public Ranges between(long fromInclusive, long toInclusive)
        {
            if (fromInclusive > toInclusive)
                throw new IndexOutOfBoundsException();

            if (fromInclusive == toInclusive)
                return at(fromInclusive);

            int i = Arrays.binarySearch(epochs, fromInclusive);
            if (i < 0) i = -2 - i;
            if (i < 0) i = 0;

            int j = Arrays.binarySearch(epochs, toInclusive);
            if (j < 0) j = -2 - j;
            if (i > j) return Ranges.EMPTY;

            Ranges result = ranges[i++];
            while (i <= j)
                result = result.with(ranges[i++]);
            return result;
        }

        public Ranges since(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = Math.max(0, -2 -i);
            Ranges result = ranges[i++];
            while (i < ranges.length)
                result = ranges[i++].with(result);
            return result;
        }

        int indexForEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 -i;
            return i;
        }

        public boolean intersects(long epoch, AbstractKeys<?, ?> keys)
        {
            return at(epoch).intersects(keys);
        }

        public Ranges currentRanges()
        {
            return ranges[ranges.length - 1];
        }

        public Ranges maximalRanges()
        {
            return ranges[0];
        }
    }

    static class Snapshot
    {
        final ShardHolder[] shards;
        final Int2ObjectHashMap<CommandStore> byId;
        final Topology local;
        final Topology global;

        Snapshot(ShardHolder[] shards, Topology local, Topology global)
        {
            this.shards = shards;
            this.byId = new Int2ObjectHashMap<>(shards.length, Hashing.DEFAULT_LOAD_FACTOR, true);
            for (ShardHolder shard : shards)
                byId.put(shard.store.id(), shard.store);
            this.local = local;
            this.global = global;
        }
    }

    final Supplier supplier;
    final ShardDistributor shardDistributor;
    volatile Snapshot current;
    int nextId;

    private CommandStores(Supplier supplier, ShardDistributor shardDistributor)
    {
        this.supplier = supplier;
        this.shardDistributor = shardDistributor;
        this.current = new Snapshot(new ShardHolder[0], Topology.EMPTY, Topology.EMPTY);
    }

    public CommandStores(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor,
                         ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        this(new Supplier(time, agent, store, progressLogFactory, shardFactory), shardDistributor);
    }

    public Topology local()
    {
        return current.local;
    }

    public Topology global()
    {
        return current.global;
    }

    private synchronized Snapshot updateTopology(Snapshot prev, Topology newTopology)
    {
        checkArgument(!newTopology.isSubset(), "Use full topology for CommandStores.updateTopology");

        long epoch = newTopology.epoch();
        if (epoch <= prev.global.epoch())
            return prev;

        Topology newLocalTopology = newTopology.forNode(supplier.time.id()).trim();
        Ranges added = newLocalTopology.ranges().difference(prev.local.ranges());
        Ranges subtracted = prev.local.ranges().difference(newLocalTopology.ranges());

        if (added.isEmpty() && subtracted.isEmpty())
            return new Snapshot(prev.shards, newLocalTopology, newTopology);

        List<ShardHolder> result = new ArrayList<>(prev.shards.length + added.size());
        if (subtracted.isEmpty())
        {
            Collections.addAll(result, prev.shards);
        }
        else
        {
            for (ShardHolder shard : prev.shards)
            {
                if (subtracted.intersects(shard.ranges().currentRanges()))
                    shard.ranges.current = shard.ranges().withRanges(newTopology.epoch(), shard.ranges().currentRanges().difference(subtracted));
                result.add(shard);
            }
        }

        if (!added.isEmpty())
        {
            // TODO (required): shards must rebalance
            for (Ranges add : shardDistributor.split(added))
            {
                RangesForEpochHolder rangesHolder = new RangesForEpochHolder();
                rangesHolder.current = new RangesForEpoch(epoch, add);
                result.add(new ShardHolder(supplier.create(nextId++, rangesHolder), rangesHolder));
            }
        }

        return new Snapshot(result.toArray(new ShardHolder[0]), newLocalTopology, newTopology);
    }

    interface MapReduceAdapter<S extends CommandStore, Intermediate, Accumulator, O>
    {
        Accumulator allocate();
        Intermediate apply(MapReduce<? super SafeCommandStore, O> map, S commandStore, PreLoadContext context);
        Accumulator reduce(MapReduce<? super SafeCommandStore, O> reduce, Accumulator accumulator, Intermediate next);
        void consume(MapReduceConsume<?, O> consume, Intermediate reduced);
        Intermediate reduce(MapReduce<?, O> reduce, Accumulator accumulator);
    }

    public AsyncChain<Void> forEach(Consumer<SafeCommandStore> forEach)
    {
        List<AsyncChain<Void>> list = new ArrayList<>();
        Snapshot snapshot = current;
        for (ShardHolder shard : snapshot.shards)
        {
            list.add(shard.store.execute(empty(), forEach));
        }
        return AsyncChains.reduce(list, (a, b) -> null);
    }

    public AsyncChain<Void> ifLocal(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, RoutingKeys.of(key), minEpoch, maxEpoch, forEach, false);
    }

    public AsyncChain<Void> forEach(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, RoutingKeys.of(key), minEpoch, maxEpoch, forEach, true);
    }

    public AsyncChain<Void> forEach(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, keys, minEpoch, maxEpoch, forEach, true);
    }

    private AsyncChain<Void> forEach(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach, boolean matchesMultiple)
    {
        return this.mapReduce(context, keys, minEpoch, maxEpoch, new MapReduce<SafeCommandStore, Void>()
        {
            @Override
            public Void apply(SafeCommandStore in)
            {
                forEach.accept(in);
                return null;
            }

            @Override
            public Void reduce(Void o1, Void o2)
            {
                if (!matchesMultiple && minEpoch == maxEpoch)
                    throw new IllegalStateException();

                return null;
            }
        }, AsyncCommandStores.AsyncMapReduceAdapter.instance());
    }

    /**
     * See {@link #mapReduceConsume(PreLoadContext, Routables, long, long, MapReduceConsume)}
     */
    public <O> void mapReduceConsume(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        mapReduceConsume(context, RoutingKeys.of(key), minEpoch, maxEpoch, mapReduceConsume);
    }

    /**
     * Maybe asynchronously, {@code apply} the function to each applicable {@code CommandStore}, invoke {@code reduce}
     * on pairs of responses until only one remains, then {@code accept} the result.
     *
     * Note that {@code reduce} and {@code accept} are invoked by only one thread, and never concurrently with {@code apply},
     * so they do not require mutual exclusion.
     *
     * Implementations are expected to invoke {@link #mapReduceConsume(PreLoadContext, Routables, long, long, MapReduceConsume, MapReduceAdapter)}
     */
    public abstract <O> void mapReduceConsume(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume);
    public abstract <O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume);

    protected <T1, T2, O> void mapReduceConsume(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume,
                                                MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        T1 reduced = mapReduce(context, keys, minEpoch, maxEpoch, mapReduceConsume, adapter);
        adapter.consume(mapReduceConsume, reduced);
    }

    protected <T1, T2, O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume,
                                                   MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        T1 reduced = mapReduce(context, commandStoreIds, mapReduceConsume, adapter);
        adapter.consume(mapReduceConsume, reduced);
    }

    protected <T1, T2, O> T1 mapReduce(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduce<? super SafeCommandStore, O> mapReduce,
                                       MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        T2 accumulator = adapter.allocate();
        Snapshot snapshot = current;
        ShardHolder[] shards = snapshot.shards;
        for (ShardHolder shard : shards)
        {
            // TODO (urgent, efficiency): range map for intersecting ranges (e.g. that to be introduced for range dependencies)
            Ranges shardRanges = shard.ranges().between(minEpoch, maxEpoch);
            if (!shardRanges.intersects(keys))
                continue;

            T1 next = adapter.apply(mapReduce, (S)shard.store, context);
            accumulator = adapter.reduce(mapReduce, accumulator, next);
        }
        return adapter.reduce(mapReduce, accumulator);
    }

    protected <T1, T2, O> T1 mapReduce(PreLoadContext context, IntStream commandStoreIds, MapReduce<? super SafeCommandStore, O> mapReduce,
                                       MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        // TODO (low priority, efficiency): avoid using an array, or use a scratch buffer
        int[] ids = commandStoreIds.toArray();
        T2 accumulator = adapter.allocate();
        for (int id : ids)
        {
            T1 next = adapter.apply(mapReduce, (S)forId(id), context);
            accumulator = adapter.reduce(mapReduce, accumulator, next);
        }
        return adapter.reduce(mapReduce, accumulator);
    }

    public synchronized void updateTopology(Topology newTopology)
    {
        current = updateTopology(current, newTopology);
    }

    public synchronized void shutdown()
    {
        for (ShardHolder shard : current.shards)
            shard.store.shutdown();
    }

    public CommandStore forId(int id)
    {
        Snapshot snapshot = current;
        return snapshot.byId.get(id);
    }

    public int count()
    {
        return current.shards.length;
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(Key key)
    {
        ShardHolder[] shards = current.shards;
        for (ShardHolder shard : shards)
        {
            if (shard.ranges().currentRanges().contains(key))
                return shard.store;
        }
        throw new IllegalArgumentException();
    }
}
