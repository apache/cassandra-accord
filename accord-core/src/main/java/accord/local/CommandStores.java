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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.AsyncExecutorFactory;
import accord.api.AsyncExecutor;
import accord.api.ConfigurationService.EpochReady;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.CommandStore.EpochUpdateHolder;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.IndexedQuadConsumer;
import accord.utils.IndexedRangeQuadConsumer;
import accord.utils.Invariants;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;
import accord.utils.RandomSource;
import accord.utils.Reduce;
import accord.utils.SearchableRangeList;
import accord.utils.SimpleBitSet;
import accord.utils.TriFunction;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;
import org.agrona.collections.Hashing;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.api.ConfigurationService.EpochReady.done;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;
import static java.util.stream.Collectors.toList;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores implements AsyncExecutorFactory
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CommandStores.class);

    public interface LatentStoreSelector
    {
        StoreSelector refine(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants);

        class StandardLatentStoreSelector implements LatentStoreSelector
        {
            private static final StandardLatentStoreSelector INSTANCE = new StandardLatentStoreSelector();

            @Override
            public StoreSelector refine(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
            {
                return snapshot -> StoreFinder.find(snapshot, participants)
                                              .filter(snapshot, participants, txnId.epoch(), (executeAt != null ? executeAt : txnId).epoch())
                                              .iterator(snapshot);
            }
        }

        static LatentStoreSelector standard()
        {
            return StandardLatentStoreSelector.INSTANCE;
        }
    }

    public interface StoreSelector extends LatentStoreSelector
    {
        default StoreSelector refine(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants) { return this; }
        Iterator<CommandStore> select(Snapshot snapshot);
    }

    public static class IncludingSpecificStoreSelector implements StoreSelector
    {
        final int storeId;

        public IncludingSpecificStoreSelector(int storeId)
        {
            this.storeId = storeId;
        }

        @Override
        public StoreSelector refine(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
        {
            return snapshot -> {
                StoreFinder finder = StoreFinder.find(snapshot, participants)
                                                .filter(snapshot, participants, txnId.epoch(), (executeAt != null ? executeAt : txnId).epoch());
                finder.set(snapshot.byId.get(storeId));
                return finder.iterator(snapshot);
            };
        }

        @Override
        public Iterator<CommandStore> select(Snapshot snapshot)
        {
            return Collections.singletonList(snapshot.byId(storeId)).iterator();
        }
    }

    // TODO (required): as we get more tables this will become expensive to allocate; we need to index first by prefix
    public static class StoreFinder extends SimpleBitSet implements IndexedQuadConsumer<Object, Object, Object, Object>, IndexedRangeQuadConsumer<Object, Object, Object, Object>
    {
        final int[] indexMap;

        private StoreFinder(int size, int[] indexMap)
        {
            super(size);
            this.indexMap = indexMap;
        }

        public StoreFinder(Snapshot snapshot)
        {
            this(snapshot.shards.length, snapshot.indexForRange);
        }

        public static StoreSelector selector(Unseekables<?> unseekables, long minEpoch, long maxEpoch)
        {
            return snapshot -> {
                StoreFinder finder = StoreFinder.find(snapshot, unseekables);
                finder.filter(snapshot, unseekables, minEpoch, maxEpoch);
                return finder.iterator(snapshot);
            };
        }

        public static StoreFinder find(Snapshot snapshot, Unseekables<?> unseekables)
        {
            StoreFinder finder = new StoreFinder(snapshot);
            switch (unseekables.domain())
            {
                default: throw new UnhandledEnum(unseekables.domain());
                case Range:
                {
                    int minIndex = 0;
                    for (Range range : (AbstractRanges)unseekables)
                        minIndex = snapshot.lookupByRange.forEachRange(range, finder, finder, null, null, null, null, minIndex);
                    break;
                }
                case Key:
                {
                    int minIndex = 0;
                    for (RoutingKey key : (AbstractUnseekableKeys)unseekables)
                        minIndex = snapshot.lookupByRange.forEachKey(key, finder, finder, null, null, null, null, minIndex);
                    break;
                }
            }
            return finder;
        }

        public StoreFinder filter(Snapshot snapshot, Unseekables<?> unseekables, long minEpoch, long maxEpoch)
        {
            for (int i = firstSetBit(); i >= 0 ; i = nextSetBit(i + 1, -1))
            {
                ShardHolder shard = snapshot.shards[i];
                Ranges shardRanges = shard.ranges().allBetween(minEpoch, maxEpoch);
                if (shardRanges != shard.ranges.all() && !shardRanges.intersects(unseekables))
                    unset(i);
            }
            return this;
        }

        public Iterator<CommandStore> iterator(Snapshot snapshot)
        {
            return new Iterator<>()
            {
                int i = firstSetBit();
                @Override
                public boolean hasNext()
                {
                    return i >= 0;
                }

                @Override
                public CommandStore next()
                {
                    CommandStore next = snapshot.shards[i].store;
                    i = nextSetBit(i + 1, -1);
                    return next;
                }
            };
        }

        @Override
        public void accept(Object p1, Object p2, Object p3, Object p4, int index)
        {
            set(indexMap[index]);
        }

        @Override
        public void accept(Object p1, Object p2, Object p3, Object p4, int fromIndex, int toIndex)
        {
            for (int i = fromIndex ; i < toIndex ; ++i)
                set(indexMap[i]);
        }
    }

    public interface Factory
    {
        CommandStores create(NodeCommandStoreService node,
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
        private final NodeCommandStoreService node;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final LocalListeners.Factory listenersFactory;
        private final CommandStore.Factory shardFactory;
        private final RandomSource random;
        private final Journal journal;

        StoreSupplier(NodeCommandStoreService node, Agent agent, DataStore store, RandomSource random, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, CommandStore.Factory shardFactory, Journal journal)
        {
            this.node = node;
            this.agent = agent;
            this.store = store;
            this.random = random;
            this.progressLogFactory = progressLogFactory;
            this.listenersFactory = listenersFactory;
            this.shardFactory = shardFactory;
            this.journal = journal;
        }

        CommandStore create(int id, EpochUpdateHolder rangesForEpoch)
        {
            return shardFactory.create(id, node, agent, this.store, progressLogFactory, listenersFactory, rangesForEpoch, journal);
        }
    }

    public static class ShardHolder
    {
        public final CommandStore store;
        RangesForEpoch ranges;

        ShardHolder(CommandStore store)
        {
            this.store = store;
        }

        public ShardHolder(CommandStore store, RangesForEpoch ranges)
        {
            this.store = store;
            this.ranges = ranges;
        }

        public ShardHolder withStoreUnsafe(CommandStore store)
        {
            return new ShardHolder(store, ranges);
        }

        public RangesForEpoch ranges()
        {
            return ranges;
        }

        boolean filter(long minEpoch, long maxEpoch, Unseekables<?> unseekables)
        {
            Ranges shardRanges = ranges.allBetween(minEpoch, maxEpoch);
            return shardRanges != ranges.all() && !shardRanges.intersects(unseekables);
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

    // We ONLY remove ranges to keep logic manageable; likely to only merge CommandStores into a new CommandStore via some kind of Bootstrap
    public static class RangesForEpoch
    {
        final long[] epochs;
        final Ranges[] ranges;
        public static final RangesForEpoch EMPTY = new RangesForEpoch(new long[0], new Ranges[0]);

        public RangesForEpoch(long epoch, Ranges ranges)
        {
            this.epochs = new long[] { epoch };
            this.ranges = new Ranges[] { ranges };
        }

        public RangesForEpoch(long[] epochs, Ranges[] ranges)
        {
            Invariants.require(epochs.length == ranges.length);
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
            Invariants.requireArgument(epochs.length == 0 || epochs[epochs.length - 1] <= epoch);
            int newLength = epochs.length == 0 || epochs[epochs.length - 1] < epoch ? epochs.length + 1 : epochs.length;
            long[] newEpochs = Arrays.copyOf(epochs, newLength);
            Ranges[] newRanges = Arrays.copyOf(ranges, newLength);
            newEpochs[newLength - 1] = epoch;
            newRanges[newLength - 1] = latestRanges;
            Invariants.require(newEpochs[newLength - 1] == 0 || newEpochs[newLength - 1] == epoch, "Attempted to override historic epoch %d with %d", newEpochs[newLength - 1], epoch);
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

        public Ranges rangesAtIndex(int index)
        {
            return ranges[index];
        }

        public long epochAtIndex(int index)
        {
            return epochs[index];
        }

        public int floorIndex(long epoch)
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

        public int indexOffset(long lowEpoch, long highEpoch)
        {
            if (lowEpoch == highEpoch)
                return 0;

            int lowIndex = Math.max(0, floorIndex(lowEpoch));
            int highIndex = lowIndex;
            while (highIndex + 1 < epochs.length && epochs[highIndex + 1] <= highEpoch)
                ++highIndex;
            return highIndex - lowIndex;
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

        public long earliestLaterEpochThatFullyCovers(long sinceEpoch, Unseekables<?> keysOrRanges)
        {
            return Math.max(sinceEpoch, epochs[0]);
        }

        public long latestEarlierEpochThatFullyCovers(long beforeEpoch, Unseekables<?> keysOrRanges)
        {
            int i = ceilIndex(beforeEpoch);
            if (i == 0)
                return beforeEpoch;

            long latest = beforeEpoch;
            Ranges existing = Ranges.EMPTY;
            long next = beforeEpoch;
            if (i < epochs.length)
            {
                existing = ranges[i];
                next = Math.min(next, epochs[i]);
            }
            while (--i >= 0)
            {
                if (ranges[i].without(existing).intersects(keysOrRanges))
                    latest = next - 1;
                existing = existing.with(ranges[i]);
                next = epochs[i];
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

    protected void loadSnapshot(Snapshot toLoad)
    {
        current = toLoad;
    }

    public static class Snapshot extends Journal.TopologyUpdate implements Iterable<ShardHolder>
    {
        public final Topology local;
        final ShardHolder[] shards;
        final Int2IntHashMap byId;
        private final int[] indexForRange;
        final SearchableRangeList lookupByRange;

        public Snapshot(ShardHolder[] shards, Topology local, Topology global)
        {
            super(asMap(shards), global);
            this.local = local;
            this.shards = shards;
            this.byId = new Int2IntHashMap(shards.length, Hashing.DEFAULT_LOAD_FACTOR, -1);
            int count = 0;
            for (int i = 0 ; i < shards.length ; ++i)
            {
                ShardHolder shard = shards[i];
                byId.put(shard.store.id(), i);
                count += shard.ranges.all().size();
            }
            class RangeAndIndex
            {
                final Range range;
                final int index;

                RangeAndIndex(Range range, int index)
                {
                    this.range = range;
                    this.index = index;
                }
            }
            RangeAndIndex[] rangesAndIndexes = new RangeAndIndex[count];
            count = 0;
            for (int i = 0; i < shards.length ; ++i)
            {
                Ranges add = shards[i].ranges.all();
                for (Range range : add)
                    rangesAndIndexes[count++] = new RangeAndIndex(range, i);
            }

            Arrays.sort(rangesAndIndexes, (a, b) -> a.range.compareTo(b.range));

            Range[] ranges = new Range[count];
            indexForRange = new int[count];
            for (int i = 0 ; i < rangesAndIndexes.length ; ++i)
            {
                ranges[i] = rangesAndIndexes[i].range;
                indexForRange[i] = rangesAndIndexes[i].index;
            }
            lookupByRange = SearchableRangeList.build(ranges);
        }

        // This method exists to ensure we do not hold references to command stores
        public Journal.TopologyUpdate asTopologyUpdate()
        {
            return new Journal.TopologyUpdate(commandStores, global);
        }

        private static Int2ObjectHashMap<CommandStores.RangesForEpoch> asMap(ShardHolder[] shards)
        {
            Int2ObjectHashMap<CommandStores.RangesForEpoch> commandStores = new Int2ObjectHashMap<>();
            for (ShardHolder shard : shards)
                commandStores.put(shard.store.id, shard.ranges);
            return commandStores;
        }

        public CommandStore byId(int id)
        {
            return shards[byId.get(id)].store;
        }

        @Override
        public Iterator<ShardHolder> iterator()
        {
            return Arrays.asList(shards).iterator();
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
        this(new StoreSupplier(time, agent, store, random, progressLogFactory, listenersFactory, shardFactory, journal), shardDistributor, journal);
    }

    public Node.Id nodeId()
    {
        return supplier.node.id();
    }

    public Topology local()
    {
        return current.local;
    }

    public void forEach(BiConsumer<CommandStore, RangesForEpoch> forEach)
    {
        for (ShardHolder shard : current.shards)
        {
            forEach.accept(shard.store, shard.ranges);
        }
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
        Invariants.requireArgument(!newTopology.isSubset(), "Use full topology for CommandStores.updateTopology");

        long epoch = newTopology.epoch();
        if (epoch <= prev.global.epoch())
            return new TopologyUpdate(prev, () -> done(epoch));

        Topology newLocalTopology = newTopology.forNode(supplier.node.id()).trim();
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
                RangesForEpoch rangesForEpoch = new RangesForEpoch(epoch, addRanges);
                updateHolder.add(epoch, rangesForEpoch, addRanges);
                ShardHolder shard = new ShardHolder(supplier.create(nextId++, updateHolder));
                shard.ranges = rangesForEpoch;

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
                AsyncChains.reduce(list.stream().map(b -> b.metadata).collect(toList()), Reduce.toNull()).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.coordinate).collect(toList()), Reduce.toNull()).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.data).collect(toList()), Reduce.toNull()).beginAsResult(),
                AsyncChains.reduce(list.stream().map(b -> b.reads).collect(toList()), Reduce.toNull()).beginAsResult()
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
            if (!oldShard.notInFastPath.equals(newShard.notInFastPath))
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

    public AsyncChain<Void> forEach(PreLoadContext context, Consumer<SafeCommandStore> forEach)
    {
        List<AsyncChain<Void>> list = new ArrayList<>();
        Snapshot snapshot = current;
        for (ShardHolder shard : snapshot.shards)
            list.add(shard.store.build(context, forEach));
        return AsyncChains.reduce(list, Reduce.toNull(), null);
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

    public AsyncChain<Void> forEach(PreLoadContext context, Unseekables<?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return forEach(context, keys, minEpoch, maxEpoch, forEach, true);
    }

    private AsyncChain<Void> forEach(PreLoadContext context, Unseekables<?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach, boolean matchesMultiple)
    {
        return this.mapReduce(context, keys, minEpoch, maxEpoch, new MapReduce<>()
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

    public AsyncChain<Void> forEach(PreLoadContext context, StoreSelector selector, Consumer<SafeCommandStore> forEach)
    {
        return this.mapReduce(context, selector, new MapReduce<>()
        {
            @Override
            public Void apply(SafeCommandStore in)
            {
                forEach.accept(in);
                return null;
            }
            @Override
            public Void reduce(Void o1, Void o2) { return null; }
            @Override public String toString() { return forEach.getClass().getName(); }
        });
    }

    /**
     * See {@link #mapReduceConsume(PreLoadContext, Unseekables, long, long, MapReduceConsume)}
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
     * Implementations are expected to invoke {@link #mapReduceConsume(PreLoadContext, Unseekables, long, long, MapReduceConsume)}
     */
    protected <O> Cancellable mapReduceConsume(PreLoadContext context, Unseekables<?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, keys, minEpoch, maxEpoch, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    protected <O> Cancellable mapReduceConsume(PreLoadContext context, StoreSelector selector, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, selector, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    public  <O> Cancellable mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, commandStoreIds, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    public <O> AsyncChain<O> mapReduce(PreLoadContext context, Unseekables<?> unseekables, long minEpoch, long maxEpoch, MapReduce<? super SafeCommandStore, O> mapReduce)
    {
        // TODO (desired): we shouldn't need to allocate a new lambda here
        return mapReduce(CommandStore::build, context, mapReduce, mapReduce, unseekables, minEpoch, maxEpoch);
    }

    public <O> Cancellable mapReduceConsume(Unseekables<?> unseekables, long minEpoch, long maxEpoch, Function<? super CommandStore, AsyncChain<O>> map, Reduce<O, O> reduce, BiConsumer<? super O, Throwable> consume)
    {
        AsyncChain<O> reduced = mapReduce(unseekables, minEpoch, maxEpoch, map, reduce);
        return reduced.begin(consume);
    }

    public <O> AsyncChain<O> mapReduce(Unseekables<?> unseekables, long minEpoch, long maxEpoch, Function<? super CommandStore, AsyncChain<O>> map, Reduce<O, O> reducer)
    {
        return mapReduce((commandStore, i, f) -> f.apply(commandStore), null, map, reducer, unseekables, minEpoch, maxEpoch);
    }

    public <O, P1, P2> AsyncChain<O> mapReduce(TriFunction<CommandStore, P1, P2, AsyncChain<O>> applyMap, P1 p1, P2 p2, Reduce<O, O> reducer, Unseekables<?> unseekables, long minEpoch, long maxEpoch)
    {
        return mapReduce(StoreFinder.selector(unseekables, minEpoch, maxEpoch), applyMap, p1, p2, reducer);
    }

    public <O> AsyncChain<O> mapReduce(PreLoadContext context, StoreSelector selector, MapReduce<? super SafeCommandStore, O> mapReduce)
    {
        return mapReduce(selector, CommandStore::build, context, mapReduce, mapReduce);
    }

    public <O, P1, P2> AsyncChain<O> mapReduce(StoreSelector selector, TriFunction<CommandStore, P1, P2, AsyncChain<O>> applyMap, P1 p1, P2 p2, Reduce<O, O> reducer)
    {
        Snapshot snapshot = current;
        Iterator<CommandStore> stores = selector.select(snapshot);
        AsyncChain<O> chain = null;
        while (stores.hasNext())
        {
            AsyncChain<O> next = applyMap.apply(stores.next(), p1, p2);
            if (next != null)
                chain = chain != null ? AsyncChains.reduce(chain, next, reducer) : next;
        }
        return chain == null ? AsyncChains.success(null) : chain;
    }

    protected <O> AsyncChain<O> mapReduce(PreLoadContext context, IntStream commandStoreIds, MapReduce<? super SafeCommandStore, O> mapReduce)
    {
        return mapReduce(context, snapshot -> commandStoreIds.mapToObj(snapshot::byId).iterator(), mapReduce);
    }

    public <O> Cancellable mapReduceConsume(PreLoadContext context, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        AsyncChain<O> reduced = mapReduce(context, mapReduceConsume);
        return reduced.begin(mapReduceConsume);
    }

    protected <O> AsyncChain<O> mapReduce(PreLoadContext context, MapReduce<? super SafeCommandStore, O> mapReduce)
    {
        AsyncChain<O> chain = null;
        for (ShardHolder shardHolder : current.shards)
        {
            CommandStore commandStore = shardHolder.store;
            AsyncChain<O> next = commandStore.build(context, mapReduce);
            chain = chain != null ? AsyncChains.reduce(chain, next, mapReduce) : next;
        }
        return chain == null ? AsyncChains.success(null) : chain;
    }


    public <O> AsyncChain<List<O>> map(PreLoadContext context, Function<? super SafeCommandStore, O> mapper)
    {
        ShardHolder[] shards = current.shards;
        List<AsyncChain<O>> results = new ArrayList<>(shards.length);

        for (ShardHolder shard : shards)
            results.add(shard.store.build(context, mapper));

        return AsyncChains.allOf(results);
    }

    protected <O> AsyncChain<List<O>> map(PreLoadContext context, IntStream commandStoreIds, Function<? super SafeCommandStore, O> map)
    {
        // TODO (low priority, efficiency): avoid using an array, or use a scratch buffer
        int[] ids = commandStoreIds.toArray();
        if (ids.length == 1)
            return forId(ids[0]).build(context, map).map(Collections::singletonList);

        List<AsyncChain<O>> list = new ArrayList<>(ids.length);
        for (int id : ids)
        {
            CommandStore commandStore = forId(id);
            AsyncChain<O> next = commandStore.build(context, map);
            list.add(next);
        }

        return AsyncChains.allOf(list);
    }

    /**
     * Initialize topology from snapshot on boot.
     */
    public synchronized void initializeTopologyUnsafe(Journal.TopologyUpdate update)
    {
        Invariants.require(current.global.epoch() == 0);
        ShardHolder[] shards = new ShardHolder[update.commandStores.size()];
        int i = 0;
        int maxId = -1;
        for (Map.Entry<Integer, RangesForEpoch> e : update.commandStores.entrySet())
        {
            Invariants.require(e.getValue() != null);
            EpochUpdateHolder holder = new EpochUpdateHolder();
            holder.add(1, e.getValue(), e.getValue().all());
            shards[i++] = new ShardHolder(supplier.create(e.getKey(), holder), e.getValue());
            maxId = Math.max(maxId, e.getKey());
        }

        nextId = maxId + 1;
        loadSnapshot(new Snapshot(shards, update.global.forNode(supplier.node.id()).trim(), update.global));
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


    @Override
    public AsyncExecutor someExecutor()
    {
        return someSequentialExecutor();
    }

    @Override
    public SequentialAsyncExecutor someSequentialExecutor()
    {
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
        return snapshot.shards[snapshot.byId.get(id)].store;
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
