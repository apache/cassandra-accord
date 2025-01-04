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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.ConfigurationService.EpochReady;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.CommandStore.EpochUpdateHolder;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.api.ConfigurationService.EpochReady.done;
import static accord.local.PreLoadContext.empty;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.illegalState;
import static java.util.stream.Collectors.toList;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CommandStores.class);

    public interface Factory
    {
        CommandStores create(NodeCommandStoreService time,
                             Agent agent,
                             DataStore store,
                             RandomSource random,
                             Journal journal,
                             ShardDistributor shardDistributor,
                             ProgressLog.Factory progressLogFactory,
                             LocalListeners.Factory listenersFactory);
    }

    private static class StoreSupplier
    {
        private final NodeCommandStoreService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final LocalListeners.Factory listenersFactory;
        private final CommandStore.Factory shardFactory;
        private final RandomSource random;

        StoreSupplier(NodeCommandStoreService time, Agent agent, DataStore store, RandomSource random, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, CommandStore.Factory shardFactory)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.random = random;
            this.progressLogFactory = progressLogFactory;
            this.listenersFactory = listenersFactory;
            this.shardFactory = shardFactory;
        }

        CommandStore create(int id, EpochUpdateHolder rangesForEpoch)
        {
            return shardFactory.create(id, time, agent, this.store, progressLogFactory, listenersFactory, rangesForEpoch);
        }
    }

    protected static class ShardHolder
    {
        public final CommandStore store;
        RangesForEpoch ranges;

        ShardHolder(CommandStore store)
        {
            this.store = store;
        }

        private ShardHolder(CommandStore store, RangesForEpoch ranges)
        {
            this.store = store;
            this.ranges = ranges;
        }

        public ShardHolder withStoreUnsafe(CommandStore store)
        {
            return new ShardHolder(store, ranges);
        }

        RangesForEpoch ranges()
        {
            return ranges;
        }

        public String toString()
        {
            return store.id() + " " + ranges;
        }
    }

    public interface RangesForEpochSupplier
    {
        RangesForEpoch ranges();
    }

    // TODO (expected): merge with RedundantBefore, and standardise executeRanges() to treat removing stale ranges the same as adding new epoch ranges
    // We ONLY remove ranges to keep logic manageable; likely to only merge CommandStores into a new CommandStore via some kind of Bootstrap
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
            Invariants.checkState(epochs.length == ranges.length);
            this.epochs = epochs;
            this.ranges = ranges;
        }

        public int size()
        {
            return epochs.length;
        }

        public void forEach(BiConsumer<Long, Ranges> forEach)
        {
            for (int i = 0; i < epochs.length; i++)
                forEach.accept(epochs[i], ranges[i]);
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            RangesForEpoch that = (RangesForEpoch) object;
            return Objects.deepEquals(epochs, that.epochs) && Objects.deepEquals(ranges, that.ranges);
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        public RangesForEpoch withRanges(long epoch, Ranges latestRanges)
        {
            Invariants.checkArgument(epochs.length == 0 || epochs[epochs.length - 1] <= epoch);
            int newLength = epochs.length == 0 || epochs[epochs.length - 1] < epoch ? epochs.length + 1 : epochs.length;
            long[] newEpochs = Arrays.copyOf(epochs, newLength);
            Ranges[] newRanges = Arrays.copyOf(ranges, newLength);
            newEpochs[newLength - 1] = epoch;
            newRanges[newLength - 1] = latestRanges;
            Invariants.checkState(newEpochs[newLength - 1] == 0 || newEpochs[newLength - 1] == epoch, "Attempted to override historic epoch %d with %d", newEpochs[newLength - 1], epoch);
            return new RangesForEpoch(newEpochs, newRanges);
        }

        public @Nonnull Ranges coordinates(TxnId txnId)
        {
            return allAt(txnId);
        }

        public @Nonnull Ranges allAt(Timestamp at)
        {
            return allAt(at.epoch());
        }

        public @Nonnull Ranges allAt(long epoch)
        {
            int i = floorIndex(epoch);
            if (i < 0) return Ranges.EMPTY;
            return ranges[i];
        }

        /**
         * Extend a previously computed set of Ranges that included {@code fromInclusive}
         * to include ranges up to {@code toInclusive}
         */
        public @Nonnull Ranges extend(Ranges extend, long curFrom, long curTo, long extendFrom, long extendTo)
        {
            if (extend.isEmpty()) // this captures the case where curTo < epochs[0]
                return allBetween(extendFrom, extendTo);

            if (extendFrom >= curFrom)
                return extend;

            int startCurIndex = floorIndex(curFrom);
            int startExtendIndex = Math.max(0, floorIndex(extendFrom));
            if (startCurIndex <= startExtendIndex)
                return extend;

            return ranges[startExtendIndex];
        }

        public @Nonnull Ranges allBetween(long fromInclusive, EpochSupplier toInclusive)
        {
            return allBetween(fromInclusive, toInclusive.epoch());
        }

        public @Nonnull Ranges allBetween(long fromInclusive, long toInclusive)
        {
            if (fromInclusive > toInclusive)
                throw new IndexOutOfBoundsException();

            int since = floorIndex(fromInclusive);
            if (since >= 0) return ranges[since];

            int to = floorIndex(toInclusive);
            if (to >= 0) return ranges[0];
            return Ranges.EMPTY;
        }

        public @Nonnull Ranges all()
        {
            return ranges[0];
        }

        public @Nonnull Ranges notRetired(SafeCommandStore safeStore)
        {
            return safeStore.redundantBefore().removeRetired(ranges[0]);
        }

        public @Nonnull Ranges allBefore(long toExclusive)
        {
            int to = ceilIndex(toExclusive);
            return to <= 0 ? Ranges.EMPTY : ranges[0];
        }

        public @Nonnull Ranges allUntil(long toInclusive)
        {
            int to = floorIndex(toInclusive);
            return to < 0 ? Ranges.EMPTY : ranges[0];
        }

        public @Nonnull Ranges allSince(long fromInclusive)
        {
            int since = floorIndex(fromInclusive);
            return ranges[Math.max(since, 0)];
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

        public @Nonnull Ranges currentRanges()
        {
            return ranges[ranges.length - 1];
        }

        public String toString()
        {
            return IntStream.range(0, ranges.length).mapToObj(i -> epochs[i] + ": " + ranges[i])
                            .collect(Collectors.joining(", "));
        }

        public long earliestLaterEpochThatFullyCovers(long sinceEpoch, Routables<?> keysOrRanges)
        {
            return Math.max(sinceEpoch, epochs[0]);
        }

        public long latestEarlierEpochThatFullyCovers(long beforeEpoch, Routables<?> keysOrRanges)
        {
            int i = ceilIndex(beforeEpoch);
            long latest = beforeEpoch;
            Ranges existing = i >= ranges.length ? Ranges.EMPTY : ranges[i];
            while (--i >= 0)
            {
                if (ranges[i].without(existing).intersects(keysOrRanges))
                    latest = epochs[i];
                existing = existing.with(ranges[i]);
            }
            return latest;
        }

        public Ranges removed(long presentIn, long removedByInclusive)
        {
            int i = Math.max(1, floorIndex(presentIn));
            int maxi = 1 + floorIndex(removedByInclusive);
            Ranges removed = Ranges.EMPTY;
            while (i < maxi)
            {
                removed = removed.with(ranges[i - 1].without(ranges[i]));
                ++i;
            }
            return removed;
        }
    }

    // This method should only be used on node startup.
    // "Unsafe" because it relies on user to synchronise and sequence the call properly.
    public void restoreShardStateUnsafe(Consumer<Topology> reportTopology)
    {
        Iterator<Journal.TopologyUpdate> iter = journal.replayTopologies();
        // First boot
        if (!iter.hasNext())
            return;

        Journal.TopologyUpdate lastUpdate = null;
        while (iter.hasNext())
        {
            Journal.TopologyUpdate update = iter.next();
            reportTopology.accept(update.global);
            if (lastUpdate == null || update.global.epoch() > lastUpdate.global.epoch())
                lastUpdate = update;
        }

        ShardHolder[] shards = new ShardHolder[lastUpdate.commandStores.size()];
        int i = 0;
        for (Map.Entry<Integer, RangesForEpoch> e : lastUpdate.commandStores.entrySet())
        {
            RangesForEpoch ranges = e.getValue();
            CommandStore commandStore = null;
            for (ShardHolder shard : current.shards)
            {
                if (shard.ranges.equals(ranges))
                    commandStore = shard.store;
            }
            Invariants.nonNull(commandStore, "Command store should have been reloaded").restore();
            ShardHolder shard = new ShardHolder(commandStore, e.getValue());
            shards[i++] = shard;
        }

        loadSnapshot(new Snapshot(shards, lastUpdate.local, lastUpdate.global));
    }

    protected void loadSnapshot(Snapshot toLoad)
    {
        current = toLoad;
    }

    protected static class Snapshot extends Journal.TopologyUpdate
    {
        public final ShardHolder[] shards;
        public final Int2ObjectHashMap<CommandStore> byId;

        Snapshot(ShardHolder[] shards, Topology local, Topology global)
        {
            super(asMap(shards), local, global);
            this.shards = shards;
            this.byId = new Int2ObjectHashMap<>(shards.length, Hashing.DEFAULT_LOAD_FACTOR, true);
            for (ShardHolder shard : shards)
                byId.put(shard.store.id(), shard.store);
        }

        // This method exists to ensure we do not hold references to command stores
        public Journal.TopologyUpdate asTopologyUpdate()
        {
            return new Journal.TopologyUpdate(commandStores, local, global);
        }

        private static Int2ObjectHashMap<CommandStores.RangesForEpoch> asMap(ShardHolder[] shards)
        {
            Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores = new Int2ObjectHashMap<>();
            for (ShardHolder shard : shards)
                commandStores.put(shard.store.id, shard.ranges);
            return commandStores;
        }
    }

    final StoreSupplier supplier;
    final ShardDistributor shardDistributor;
    final Journal journal;
    volatile Snapshot current;
    int nextId;

    private CommandStores(StoreSupplier supplier, ShardDistributor shardDistributor, Journal journal)
    {
        this.supplier = supplier;
        this.shardDistributor = shardDistributor;

        this.current = new Snapshot(new ShardHolder[0], Topology.EMPTY, Topology.EMPTY);
        this.journal = journal;
    }

    public CommandStores(NodeCommandStoreService time, Agent agent, DataStore store, RandomSource random, Journal journal, ShardDistributor shardDistributor,
                         ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, CommandStore.Factory shardFactory)
    {
        this(new StoreSupplier(time, agent, store, random, progressLogFactory, listenersFactory, shardFactory), shardDistributor, journal);
    }

    public Topology local()
    {
        return current.local;
    }

    public DataStore dataStore()
    {
        return supplier.store;
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
        Ranges addedGlobal = newTopology.ranges().without(prev.global.ranges());
        node.addNewRangesToDurableBefore(addedGlobal, epoch);

        Ranges added = newLocalTopology.ranges().without(prev.local.ranges());
        Ranges subtracted = prev.local.ranges().without(newLocalTopology.ranges());
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
            Ranges current = shard.ranges().currentRanges();
            Ranges removeRanges = subtracted.slice(current, Minimal);
            if (!removeRanges.isEmpty())
            {
                // TODO (required): This is updating the a non-volatile field in the previous Snapshot, why modify it at all, even with volatile the guaranteed visibility is weak even with mutual exclusion
                shard.ranges = shard.ranges().withRanges(newTopology.epoch(), current.without(subtracted));
                shard.store.epochUpdateHolder.remove(epoch, shard.ranges, removeRanges);
                bootstrapUpdates.add(shard.store.unbootstrap(epoch, removeRanges));
            }
            // TODO (desired): only sync affected shards
            Ranges ranges = shard.ranges().currentRanges();
            // ranges can be empty when ranges are lost or consolidated across epochs.
            if (epoch > 1 && startSync && requiresSync(ranges, prev.global, newTopology))
            {
                bootstrapUpdates.add(shard.store.sync(node, ranges, epoch));
            }
            result.add(shard);
        }

        if (!added.isEmpty())
        {
            for (Ranges addRanges : shardDistributor.split(added))
            {
                EpochUpdateHolder updateHolder = new EpochUpdateHolder();
                ShardHolder shard = new ShardHolder(supplier.create(nextId++, updateHolder));
                shard.ranges = new RangesForEpoch(epoch, addRanges);
                shard.store.epochUpdateHolder.add(epoch, shard.ranges, addRanges);

                Map<Boolean, Ranges> partitioned = addRanges.partitioningBy(range -> shouldBootstrap(node, prev.global, newLocalTopology, range));
                if (partitioned.containsKey(false))
                    bootstrapUpdates.add(shard.store.initialise(epoch, partitioned.get(false)));
                if (partitioned.containsKey(true))
                    bootstrapUpdates.add(shard.store.bootstrapper(node, partitioned.get(true), newLocalTopology.epoch()));
                result.add(shard);
            }
        }

        Supplier<EpochReady> bootstrap = bootstrapUpdates.isEmpty() ? () -> done(epoch) : () -> {
            List<EpochReady> list = bootstrapUpdates.stream().map(Supplier::get).collect(toList());
            return new EpochReady(epoch,
                AsyncChains.reduce(list.stream().map(b -> b.metadata).collect(toList()), (a, b) -> null).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.coordinate).collect(toList()), (a, b) -> null).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.data).collect(toList()), (a, b) -> null).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.reads).collect(toList()), (a, b) -> null).beginAsResult()
            );
        };
        return new TopologyUpdate(new Snapshot(result.toArray(new ShardHolder[0]), newLocalTopology, newTopology), bootstrap);
    }

    private static boolean requiresSync(Ranges ranges, Topology oldTopology, Topology newTopology)
    {
        List<Shard> oldShards = oldTopology.foldl(ranges, (oldShard, shards, i) -> {
            shards.add(oldShard);
            return shards;
        }, new ArrayList<>());

        List<Shard> newShards = newTopology.foldl(ranges, (newShard, shards, i) -> {
            shards.add(newShard);
            return shards;
        }, new ArrayList<>());

        if (oldShards.size() != newShards.size())
            return true;

        for (int i = 0 ; i < oldShards.size() ; ++i)
        {
            Shard oldShard = oldShards.get(i);
            Shard newShard = newShards.get(i);
            if (!oldShard.fastPathElectorate.containsAll(newShard.fastPathElectorate))
                return true;

            if (!newShard.fastPathElectorate.containsAll(oldShard.fastPathElectorate))
                return true;

            if (!newShard.nodes.equals(oldShard.nodes))
                return true;
        }
        return false;
    }

    public <R> R unsafeFoldLeft(R initial, BiFunction<R, CommandStore, R> f)
    {
        Snapshot snapshot = current;
        for (ShardHolder shard : snapshot.shards)
            initial = f.apply(initial, shard.store);
        return initial;
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

    public void forEachCommandStore(Consumer<CommandStore> forEach)
    {
        Snapshot snapshot = current;
        for (ShardHolder shard : snapshot.shards)
            forEach.accept(shard.store);
    }

    public AsyncChain<Void> ifLocal(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, RoutingKeys.of(key), minEpoch, maxEpoch, forEach, false);
    }

    public AsyncChain<Void> forEach(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, RoutingKeys.of(key), minEpoch, maxEpoch, forEach, true);
    }

    public AsyncChain<Void> forEach(PreLoadContext context, Routables<?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, keys, minEpoch, maxEpoch, forEach, true);
    }

    private AsyncChain<Void> forEach(PreLoadContext context, Routables<?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach, boolean matchesMultiple)
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

            @Override
            public String toString()
            {
                return forEach.getClass().getName();
            }
        });
    }

    /**
     * See {@link #mapReduceConsume(PreLoadContext, Routables, long, long, MapReduceConsume)}
     */
    public <O> Cancellable mapReduceConsume(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        return mapReduceConsume(context, RoutingKeys.of(key), minEpoch, maxEpoch, mapReduceConsume);
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
    protected <O> Cancellable mapReduceConsume(PreLoadContext context, Routables<?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, keys, minEpoch, maxEpoch, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    public  <O> Cancellable mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, commandStoreIds, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    public <O> AsyncChain<O> mapReduce(PreLoadContext context, Routables<?> keys, long minEpoch, long maxEpoch, MapReduce<? super SafeCommandStore, O> mapReduce)
    {
        AsyncChain<O> chain = null;
        BiFunction<O, O, O> reducer = mapReduce::reduce;
        Snapshot snapshot = current;
        ShardHolder[] shards = snapshot.shards;
        for (ShardHolder shard : shards)
        {
            // TODO (required, efficiency): range map for intersecting ranges (e.g. that to be introduced for range dependencies)
            Ranges shardRanges = shard.ranges().allBetween(minEpoch, maxEpoch);
            if (!shardRanges.intersects(keys))
                continue;

            AsyncChain<O> next = shard.store.submit(context, mapReduce);
            chain = chain != null ? AsyncChains.reduce(chain, next, reducer) : next;
        }
        return chain == null ? AsyncChains.success(null) : chain;
    }

    public <O> AsyncChain<List<O>> map(Function<? super SafeCommandStore, O> mapper)
    {
        return map(PreLoadContext.empty(), mapper);
    }

    public <O> AsyncChain<List<O>> map(PreLoadContext context, Function<? super SafeCommandStore, O> mapper)
    {
        ShardHolder[] shards = current.shards;
        List<AsyncChain<O>> results = new ArrayList<>(shards.length);

        for (ShardHolder shard : shards)
            results.add(shard.store.submit(context, mapper));

        return AsyncChains.all(results);
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
        return chain == null ? AsyncChains.success(null) : chain;
    }

    public <O> Cancellable mapReduceConsume(PreLoadContext context, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
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
        return chain == null ? AsyncChains.success(null) : chain;
    }

    public synchronized Supplier<EpochReady> updateTopology(Node node, Topology newTopology, boolean startSync)
    {
        TopologyUpdate update = updateTopology(node, current, newTopology, startSync);
        if (update.snapshot != current)
        {
            AsyncResults.SettableResult<Void> flush = new AsyncResults.SettableResult<>();
            journal.saveTopology(update.snapshot.asTopologyUpdate(), () -> flush.setSuccess(null));
            current = update.snapshot;
            return () -> {
                EpochReady ready = update.bootstrap.get();
                return new EpochReady(ready.epoch,
                                      flush.flatMap(ignore -> ready.metadata).beginAsResult(),
                                      flush.flatMap(ignore -> ready.coordinate).beginAsResult(),
                                      flush.flatMap(ignore -> ready.data).beginAsResult(),
                                      flush.flatMap(ignore -> ready.reads).beginAsResult());
            };
        }
        return update.bootstrap;
    }

    public synchronized void shutdown()
    {
        for (ShardHolder shard : current.shards)
            shard.store.shutdown();
    }

    public CommandStore select(RoutingKey key)
    {
        return select(ranges -> ranges.contains(key));
    }

    public CommandStore select(Route<?> route)
    {
        return select(ranges -> ranges.intersects(route));
    }

    public CommandStore select(Participants<?> participants)
    {
        return select(ranges -> ranges.intersects(participants));
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
        if (shards.length == 0) throw illegalState("Unable to get CommandStore; non defined");
        return shards[supplier.random.nextInt(shards.length)].store;
    }

    public CommandStore[] all()
    {
        ShardHolder[] shards = current.shards;
        CommandStore[] all = new CommandStore[shards.length];
        for (int i = 0; i < shards.length; i++)
            all[i] = shards[i].store;
        return all;
    }

    public CommandStore forId(int id)
    {
        Snapshot snapshot = current;
        return snapshot.byId.get(id);
    }

    public int[] ids()
    {
        ShardHolder[] shards = current.shards;
        int[] ids = new int[shards.length];
        for (int i = 0; i < ids.length; i++)
            ids[i] = shards[i].store.id;
        Arrays.sort(ids);
        return ids;
    }

    public int count()
    {
        return current.shards.length;
    }

    public ShardDistributor shardDistributor()
    {
        return shardDistributor;
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(RoutingKey key)
    {
        ShardHolder[] shards = current.shards;
        for (ShardHolder shard : shards)
        {
            if (shard.ranges().currentRanges().contains(key))
                return shard.store;
        }
        throw new IllegalArgumentException();
    }

    protected Snapshot current()
    {
        return current;
    }
}
