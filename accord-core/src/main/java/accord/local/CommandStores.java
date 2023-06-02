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
import accord.api.ConfigurationService.EpochReady;
import accord.primitives.*;
import accord.topology.Topology;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;

import com.google.common.annotations.VisibleForTesting;

import accord.utils.RandomSource;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2ObjectHashMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nonnull;

import static accord.api.ConfigurationService.EpochReady.done;
import static accord.local.PreLoadContext.empty;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.checkArgument;
import static java.util.stream.Collectors.toList;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores
{
    public interface Factory
    {
        CommandStores create(NodeTimeService time,
                                Agent agent,
                                DataStore store,
                                RandomSource random,
                                ShardDistributor shardDistributor,
                                ProgressLog.Factory progressLogFactory);
    }

    private static class StoreSupplier
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final CommandStore.Factory shardFactory;
        private final RandomSource random;

        StoreSupplier(NodeTimeService time, Agent agent, DataStore store, RandomSource random, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.random = random;
            this.progressLogFactory = progressLogFactory;
            this.shardFactory = shardFactory;
        }

        CommandStore create(int id, RangesForEpochHolder rangesForEpoch)
        {
            return shardFactory.create(id, time, agent, this.store, progressLogFactory, rangesForEpoch);
        }
    }

    public static class RangesForEpochHolder
    {
        // no need for safe publication; RangesForEpoch members are final, and will be guarded by other synchronization actions
        protected RangesForEpoch current;

        /**
         * This is updated asynchronously, so should only be fetched between executing tasks;
         * otherwise the contents may differ between invocations for the same task.
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

        public String toString()
        {
            return store.id() + " " + ranges.current;
        }
    }

    public static class RangesForEpoch
    {
        final long[] epochs;
        final Ranges[] ranges;
        final CommandStore store;

        public RangesForEpoch(long epoch, Ranges ranges, CommandStore store)
        {
            this.epochs = new long[] { epoch };
            this.ranges = new Ranges[] { ranges };
            this.store = store;
        }

        public RangesForEpoch(long[] epochs, Ranges[] ranges, CommandStore store)
        {
            this.epochs = epochs;
            this.ranges = ranges;
            this.store = store;
        }

        public RangesForEpoch withRanges(long epoch, Ranges ranges)
        {
            long[] newEpochs = Arrays.copyOf(this.epochs, this.epochs.length + 1);
            Ranges[] newRanges = Arrays.copyOf(this.ranges, this.ranges.length + 1);
            newEpochs[this.epochs.length] = epoch;
            newRanges[this.ranges.length] = ranges;
            return new RangesForEpoch(newEpochs, newRanges, store);
        }

        public @Nonnull Ranges coordinates(TxnId txnId)
        {
            return allAt(txnId);
        }

        public @Nonnull Ranges safeToReadAt(Timestamp at)
        {
            return allAt(at).slice(store.safeToReadAt(at), Minimal);
        }

        public @Nonnull Ranges unsafeToReadAt(Timestamp at)
        {
            return allAt(at).subtract(store.safeToReadAt(at));
        }

        public @Nonnull Ranges allAt(Timestamp at)
        {
            return allAt(at.epoch());
        }

        public @Nonnull Ranges allAt(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 -i;
            if (i < 0) return Ranges.EMPTY;
            return ranges[i];
        }

        public @Nonnull Ranges allBetween(Timestamp fromInclusive, Timestamp toInclusive)
        {
            return allBetween(fromInclusive.epoch(), toInclusive.epoch());
        }

        public @Nonnull Ranges allBetween(long fromInclusive, Timestamp toInclusive)
        {
            return allBetween(fromInclusive, toInclusive.epoch());
        }

        public @Nonnull Ranges allBetween(long fromInclusive, long toInclusive)
        {
            if (fromInclusive > toInclusive)
                throw new IndexOutOfBoundsException();

            if (fromInclusive == toInclusive)
                return allAt(fromInclusive);

            return allInternal(floorIndex(fromInclusive), 1 + floorIndex(toInclusive));
        }

        public @Nonnull Ranges allSince(long fromInclusive)
        {
            return allInternal(floorIndex(fromInclusive), ranges.length);
        }

        public @Nonnull Ranges all()
        {
            return allInternal(0, ranges.length);
        }

        public @Nonnull Ranges allBefore(long toExclusive)
        {
            return allInternal(0, ceilIndex(toExclusive));
        }

        public @Nonnull Ranges allUntil(long toInclusive)
        {
            return allInternal(0, 1 + floorIndex(toInclusive));
        }

        private int floorIndex(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -2 - i;
            return i;
        }

        private int ceilIndex(long epoch)
        {
            int i = Arrays.binarySearch(epochs, epoch);
            if (i < 0) i = -1 - i;
            return i;
        }

        private @Nonnull Ranges allInternal(int startIndex, int endIndex)
        {
            if (startIndex >= endIndex) return Ranges.EMPTY;
            Ranges result = ranges[startIndex];
            for (int i = 1 ; i < endIndex; ++i)
                result = ranges[i].with(result);

            return result;
        }

        public @Nonnull Ranges applyRanges(Timestamp executeAt)
        {
            // Note: we COULD slice to only those ranges we haven't bootstrapped, but no harm redundantly writing
            return allAt(executeAt.epoch());
        }

        public @Nonnull Ranges currentRanges()
        {
            return ranges[ranges.length - 1];
        }

        public String toString()
        {
            return IntStream.range(0, ranges.length).mapToObj(i -> epochs[i] + ": " + ranges[i])
                            .collect(Collectors.joining(", "));
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

    final StoreSupplier supplier;
    final ShardDistributor shardDistributor;
    volatile Snapshot current;
    int nextId;

    private CommandStores(StoreSupplier supplier, ShardDistributor shardDistributor)
    {
        this.supplier = supplier;
        this.shardDistributor = shardDistributor;
        this.current = new Snapshot(new ShardHolder[0], Topology.EMPTY, Topology.EMPTY);
    }

    public CommandStores(NodeTimeService time, Agent agent, DataStore store, RandomSource random, ShardDistributor shardDistributor,
                         ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        this(new StoreSupplier(time, agent, store, random, progressLogFactory, shardFactory), shardDistributor);
    }

    public Topology local()
    {
        return current.local;
    }

    static class TopologyUpdate
    {
        final Snapshot snapshot;
        final Supplier<EpochReady> bootstrap;

        TopologyUpdate(Snapshot snapshot, Supplier<EpochReady> bootstrap)
        {
            this.snapshot = snapshot;
            this.bootstrap = bootstrap;
        }
    }

    protected boolean shouldBootstrap(Node node, Topology local, Topology newLocalTopology, Range add)
    {
        return newLocalTopology.epoch() != 1;
    }

    private synchronized TopologyUpdate updateTopology(Node node, Snapshot prev, Topology newTopology, boolean startSync)
    {
        checkArgument(!newTopology.isSubset(), "Use full topology for CommandStores.updateTopology");

        long epoch = newTopology.epoch();
        if (epoch <= prev.global.epoch())
            return new TopologyUpdate(prev, () -> done(epoch));

        Topology newLocalTopology = newTopology.forNode(supplier.time.id()).trim();
        Ranges added = newLocalTopology.ranges().subtract(prev.local.ranges());
        Ranges subtracted = prev.local.ranges().subtract(newLocalTopology.ranges());

        if (added.isEmpty() && subtracted.isEmpty())
        {
            Supplier<EpochReady> epochReady = () -> done(epoch);
            // even though we haven't changed our replication, we need to check if the membership of our shard has changed
            if (newLocalTopology.shards().equals(prev.local.shards()))
                return new TopologyUpdate(new Snapshot(prev.shards, newLocalTopology, newTopology), epochReady);
            // if it has, we still need to make sure we have witnessed the transactions of the majority of prior epoch
            // which we do by fetching deps and replicating them to CommandsForKey/historicalRangeCommands
        }

        List<Supplier<EpochReady>> bootstrapUpdates = new ArrayList<>();
        List<ShardHolder> result = new ArrayList<>(prev.shards.length + added.size());
        for (ShardHolder shard : prev.shards)
        {
            Ranges slice = subtracted.slice(shard.ranges().currentRanges());
            if (!slice.isEmpty())
            {
                shard.ranges.current = shard.ranges().withRanges(newTopology.epoch(), shard.ranges().currentRanges().subtract(subtracted));
                bootstrapUpdates.add(shard.store.unbootstrap(epoch, slice));
            }
            // TODO (desired): only sync affected shards
            Ranges ranges = shard.ranges().currentRanges();
            // ranges can be empty when ranges are lost or consolidated across epochs.
            if (epoch > 1 && startSync && !ranges.isEmpty())
                bootstrapUpdates.add(shard.store.sync(node, ranges, epoch));
            result.add(shard);
        }

        if (!added.isEmpty())
        {
            // TODO (required): shards must rebalance
            for (Ranges add : shardDistributor.split(added))
            {
                RangesForEpochHolder rangesHolder = new RangesForEpochHolder();
                ShardHolder shardHolder = new ShardHolder(supplier.create(nextId++, rangesHolder), rangesHolder);
                rangesHolder.current = new RangesForEpoch(epoch, add, shardHolder.store);

                Map<Boolean, Ranges> partitioned = add.partitioningBy(range -> shouldBootstrap(node, prev.global, newLocalTopology, range));
                if (partitioned.containsKey(true))
                    bootstrapUpdates.add(shardHolder.store.bootstrapper(node, partitioned.get(true), newLocalTopology.epoch()));
                if (partitioned.containsKey(false))
                    bootstrapUpdates.add(() -> shardHolder.store.initialise(epoch, partitioned.get(false)));
                result.add(shardHolder);
            }
        }

        Supplier<EpochReady> bootstrap = bootstrapUpdates.isEmpty() ? () -> done(epoch) : () -> {
            List<EpochReady> list = bootstrapUpdates.stream().map(Supplier::get).collect(toList());
            return new EpochReady(epoch,
                AsyncChains.reduce(list.stream().map(b -> b.metadata).collect(toList()), (a, b) -> null).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.coordination).collect(toList()), (a, b) -> null).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.data).collect(toList()), (a, b) -> null).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.reads).collect(toList()), (a, b) -> null).beginAsResult()
            );
        };
        return new TopologyUpdate(new Snapshot(result.toArray(new ShardHolder[0]), newLocalTopology, newTopology), bootstrap);
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
        });
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
     * Implementations are expected to invoke {@link #mapReduceConsume(PreLoadContext, Routables, long, long, MapReduceConsume)}
     */
    protected <O> void mapReduceConsume(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, keys, minEpoch, maxEpoch, mapReduceConsume);
        reduced.begin(mapReduceConsume);
    }

    public  <O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, commandStoreIds, mapReduceConsume);
        reduced.begin(mapReduceConsume);
    }

    public <O> AsyncChain<O> mapReduce(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduce<? super SafeCommandStore, O> mapReduce)
    {
        AsyncChain<O> chain = null;
        BiFunction<O, O, O> reducer = mapReduce::reduce;
        Snapshot snapshot = current;
        ShardHolder[] shards = snapshot.shards;
        for (ShardHolder shard : shards)
        {
            // TODO (urgent, efficiency): range map for intersecting ranges (e.g. that to be introduced for range dependencies)
            Ranges shardRanges = shard.ranges().allBetween(minEpoch, maxEpoch);
            if (!shardRanges.intersects(keys))
                continue;

            AsyncChain<O> next = shard.store.submit(context, mapReduce);
            chain = chain != null ? AsyncChains.reduce(chain, next, reducer) : next;
        }
        if (chain == null)
            return AsyncChains.success(null);
        return chain;
    }

    protected <O> AsyncChain<O> mapReduce(PreLoadContext context, IntStream commandStoreIds, MapReduce<? super SafeCommandStore, O> mapReduce)
    {
        // TODO (low priority, efficiency): avoid using an array, or use a scratch buffer
        int[] ids = commandStoreIds.toArray();
        AsyncChain<O> chain = null;
        BiFunction<O, O, O> reducer = mapReduce::reduce;
        for (int id : ids)
        {
            CommandStore commandStore = forId(id);
            AsyncChain<O> next = commandStore.submit(context, mapReduce);
            chain = chain != null ? AsyncChains.reduce(chain, next, reducer) : next;
        }
        if (chain == null)
            return AsyncChains.success(null);
        return chain;
    }

    public <O> void mapReduceConsume(PreLoadContext context, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, mapReduceConsume);
        reduced.begin(mapReduceConsume);
    }

    protected <O> AsyncChain<O> mapReduce(PreLoadContext context, MapReduce<? super SafeCommandStore, O> mapReduce)
    {
        // TODO (low priority, efficiency): avoid using an array, or use a scratch buffer
        AsyncChain<O> chain = null;
        BiFunction<O, O, O> reducer = mapReduce::reduce;
        for (ShardHolder shardHolder : current.shards)
        {
            CommandStore commandStore = shardHolder.store;
            AsyncChain<O> next = commandStore.submit(context, mapReduce);
            chain = chain != null ? AsyncChains.reduce(chain, next, reducer) : next;
        }
        if (chain == null)
            return AsyncChains.success(null);
        return chain;
    }

    public synchronized Supplier<EpochReady> updateTopology(Node node, Topology newTopology, boolean startSync)
    {
        TopologyUpdate update = updateTopology(node, current, newTopology, startSync);
        current = update.snapshot;
        return update.bootstrap;
    }

    public synchronized void shutdown()
    {
        for (ShardHolder shard : current.shards)
            shard.store.shutdown();
    }

    public CommandStore select(RoutingKey key)
    {
        return  select(ranges -> ranges.contains(key));
    }

    public CommandStore select(Route<?> route)
    {
        return  select(ranges -> ranges.intersects(route));
    }

    private CommandStore select(Predicate<Ranges> fn)
    {
        ShardHolder[] shards = current.shards;
        for (ShardHolder holder : shards)
        {
            if (fn.test(holder.ranges().currentRanges()))
                return holder.store;
        }
        return any();
    }

    @VisibleForTesting
    public CommandStore any()
    {
        ShardHolder[] shards = current.shards;
        if (shards.length == 0) throw new IllegalStateException("Unable to get CommandStore; non defined");
        return shards[supplier.random.nextInt(shards.length)].store;
    }

    public CommandStore forId(int id)
    {
        Snapshot snapshot = current;
        return snapshot.byId.get(id);
    }

    public int[] ids()
    {
        Snapshot snapshot = current;
        Int2ObjectHashMap<CommandStore>.KeySet set = snapshot.byId.keySet();
        int[] ids = new int[set.size()];
        int idx = 0;
        for (int a : set)
            ids[idx++] = a;
        Arrays.sort(ids);
        return ids;
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
