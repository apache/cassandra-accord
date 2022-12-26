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
import accord.local.CommandStore.RangesForEpoch;
import accord.primitives.*;
import accord.api.RoutingKey;
import accord.topology.Topology;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;

import accord.utils.ReducingFuture;
import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.ArrayList;
import java.util.Arrays;
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
        CommandStores<?> create(int num,
                                Node node,
                                Agent agent,
                                DataStore store,
                                ProgressLog.Factory progressLogFactory);
    }

    private static class Supplier
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final CommandStore.Factory shardFactory;
        private final int numShards;

        Supplier(NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory, int numShards)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.progressLogFactory = progressLogFactory;
            this.shardFactory = shardFactory;
            this.numShards = numShards;
        }

        CommandStore create(int id, int generation, int shardIndex, RangesForEpoch rangesForEpoch)
        {
            return shardFactory.create(id, generation, shardIndex, numShards, time, agent, store, progressLogFactory, rangesForEpoch);
        }

        ShardedRanges createShardedRanges(int generation, long epoch, Ranges ranges, RangesForEpoch rangesForEpoch)
        {
            CommandStore[] newStores = new CommandStore[numShards];
            for (int i=0; i<numShards; i++)
                newStores[i] = create(generation * numShards + i, generation, i, rangesForEpoch);

            return new ShardedRanges(newStores, epoch, ranges);
        }
    }

    protected static class ShardedRanges
    {
        final CommandStore[] shards;
        final long[] epochs;
        final Ranges[] ranges;

        protected ShardedRanges(CommandStore[] shards, long epoch, Ranges ranges)
        {
            this.shards = checkArgument(shards, shards.length <= 64);
            this.epochs = new long[] { epoch };
            this.ranges = new Ranges[] { ranges };
        }

        private ShardedRanges(CommandStore[] shards, long[] epochs, Ranges[] ranges)
        {
            this.shards = checkArgument(shards, shards.length <= 64);
            this.epochs = epochs;
            this.ranges = ranges;
        }

        ShardedRanges withRanges(long epoch, Ranges ranges)
        {
            long[] newEpochs = Arrays.copyOf(this.epochs, this.epochs.length + 1);
            Ranges[] newRanges = Arrays.copyOf(this.ranges, this.ranges.length + 1);
            newEpochs[this.epochs.length] = epoch;
            newRanges[this.ranges.length] = ranges;
            return new ShardedRanges(shards, newEpochs, newRanges);
        }

        Ranges rangesForEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 -i;
            if (i < 0) return Ranges.EMPTY;
            return ranges[i];
        }

        Ranges rangesBetweenEpochs(long fromInclusive, long toInclusive)
        {
            if (fromInclusive > toInclusive)
                throw new IndexOutOfBoundsException();

            if (fromInclusive == toInclusive)
                return rangesForEpoch(fromInclusive);

            int i = Arrays.binarySearch(epochs, fromInclusive);
            if (i < 0) i = -2 - i;
            if (i < 0) i = 0;

            int j = Arrays.binarySearch(epochs, toInclusive);
            if (j < 0) j = -2 - j;
            if (i > j) return Ranges.EMPTY;

            Ranges result = ranges[i++];
            while (i <= j)
                result = result.union(ranges[i++]);
            return result;
        }

        Ranges rangesSinceEpoch(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = Math.max(0, -2 -i);
            Ranges result = ranges[i++];
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

        public <T extends Routable> long shards(Routables<T, ?> keysOrRanges, long minEpoch, long maxEpoch)
        {
            long terminalValue = -1L >>> (32 - shards.length);
            switch (keysOrRanges.kindOfContents())
            {
                default: throw new AssertionError();
                case Key:
                {
                    long accumulate = 0L;
                    for (int i = Math.max(0, indexForEpoch(minEpoch)), maxi = indexForEpoch(maxEpoch); i <= maxi ; ++i)
                    {
                        accumulate = Routables.foldl((AbstractKeys<?, ?>)keysOrRanges, ranges[i], ShardedRanges::addKeyIndex, shards.length, accumulate, terminalValue);
                    }
                    return accumulate;
                }

                case Range:
                {
                    long accumulate = 0L;
                    for (int i = Math.max(0, indexForEpoch(minEpoch)), maxi = indexForEpoch(maxEpoch); i <= maxi ; ++i)
                    {
                        // include every shard if we match a range
                        accumulate = Routables.foldl((Ranges)keysOrRanges, ranges[i], (k, p, a, idx) -> p, terminalValue, accumulate, terminalValue);
                    }
                    return accumulate;
                }
            }
        }

        Ranges currentRanges()
        {
            return ranges[ranges.length - 1];
        }

        static long keyIndex(RoutableKey key, long numShards)
        {
            return Integer.toUnsignedLong(key.routingHash()) % numShards;
        }

        private static long addKeyIndex(RoutableKey key, long numShards, long accumulate, int i)
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

    public CommandStores(int num, NodeTimeService time, Agent agent, DataStore store,
                         ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        this(new Supplier(time, agent, store, progressLogFactory, shardFactory, num));
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
        checkArgument(!newTopology.isSubset(), "Use full topology for CommandStores.updateTopology");

        long epoch = newTopology.epoch();
        if (epoch <= prev.global.epoch())
            return prev;

        Topology newLocalTopology = newTopology.forNode(supplier.time.id()).trim();
        Ranges added = newLocalTopology.ranges().difference(prev.local.ranges());
        Ranges subtracted = prev.local.ranges().difference(newLocalTopology.ranges());
//            for (ShardedRanges range : stores.ranges)
//            {
//                // FIXME: remove this (and the corresponding check in TopologyRandomizer) once lower bounds are implemented.
//                //  In the meantime, the logic needed to support acquiring ranges that we previously replicated is pretty
//                //  convoluted without the ability to jettison epochs.
//                Invariants.checkState(!range.ranges.intersects(added));
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
            public Ranges at(long epoch)
            {
                return current.ranges[generation].rangesForEpoch(epoch);
            }

            @Override
            public Ranges between(long fromInclusive, long toInclusive)
            {
                return current.ranges[generation].rangesBetweenEpochs(fromInclusive, toInclusive);
            }

            @Override
            public Ranges since(long epoch)
            {
                return current.ranges[generation].rangesSinceEpoch(epoch);
            }

            @Override
            public boolean owns(long epoch, RoutingKey key)
            {
                return current.ranges[generation].rangesForEpoch(epoch).contains(key);
            }

        };
    }

    interface MapReduceAdapter<S extends CommandStore, Intermediate, Accumulator, O>
    {
        Accumulator allocate();
        Intermediate apply(MapReduce<? super SafeCommandStore, O> map, S commandStore, PreLoadContext context);
        Accumulator reduce(MapReduce<? super SafeCommandStore, O> reduce, Accumulator accumulator, Intermediate next);
        void consume(MapReduceConsume<?, O> consume, Intermediate reduced);
        Intermediate reduce(MapReduce<?, O> reduce, Accumulator accumulator);
    }

    public Future<Void> forEach(Consumer<SafeCommandStore> forEach)
    {
        List<Future<Void>> list = new ArrayList<>();
        Snapshot snapshot = current;
        for (ShardedRanges ranges : snapshot.ranges)
        {
            for (CommandStore store : ranges.shards)
            {
                list.add(store.execute(empty(), forEach));
            }
        }
        return ReducingFuture.reduce(list, (a, b) -> null);
    }

    public Future<Void> ifLocal(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, RoutingKeys.of(key), minEpoch, maxEpoch, forEach, false);
    }

    public Future<Void> forEach(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, RoutingKeys.of(key), minEpoch, maxEpoch, forEach, true);
    }

    public Future<Void> forEach(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, keys, minEpoch, maxEpoch, forEach, true);
    }

    private Future<Void> forEach(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach, boolean matchesMultiple)
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
        for (ShardedRanges ranges : current.ranges)
        {
            long bits = ranges.shards(keys, minEpoch, maxEpoch);
            while (bits != 0)
            {
                int i = Long.numberOfTrailingZeros(bits);
                T1 next = adapter.apply(mapReduce, (S)ranges.shards[i], context);
                accumulator = adapter.reduce(mapReduce, accumulator, next);
                bits ^= Long.lowestOneBit(bits);
            }
        }
        return adapter.reduce(mapReduce, accumulator);
    }

    protected <T1, T2, O> T1 mapReduce(PreLoadContext context, IntStream commandStoreIds, MapReduce<? super SafeCommandStore, O> mapReduce,
                                       MapReduceAdapter<? super S, T1, T2, O> adapter)
    {
        // TODO: efficiency
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
        for (ShardedRanges group : current.ranges)
            for (CommandStore commandStore : group.shards)
                commandStore.shutdown();
    }

    CommandStore forId(int id)
    {
        ShardedRanges[] ranges = current.ranges;
        return ranges[id / supplier.numShards].shards[id % supplier.numShards];
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

}
