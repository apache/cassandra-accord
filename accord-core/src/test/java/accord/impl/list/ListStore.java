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

package accord.impl.list;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import accord.api.TopologyListener;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.Scheduler;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.ExecutionContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.SortedArrays;
import accord.utils.Timestamped;
import accord.utils.async.AsyncResult;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.LongArrayList;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;

public class ListStore extends Snapshotter<ListStore.Snapshot> implements DataStore, TopologyListener
{
    private static class ChangeAt
    {
        private final long epoch;
        private final Ranges ranges;
        private Ranges pending;

        private ChangeAt(long epoch, Ranges ranges)
        {
            this.epoch = epoch;
            this.ranges = ranges;
            this.pending = ranges;
        }

        @Override
        public String toString()
        {
            return "ChangeAt{" +
                   "epoch=" + epoch +
                   ", ranges=" + ranges +
                   '}';
        }
    }

    private static class FetchComplete
    {
        private final int storeId;
        private final TxnId bound;
        private final Ranges ranges;

        private FetchComplete(int storeId, TxnId bound, Ranges ranges)
        {
            this.storeId = storeId;
            this.bound = bound;
            this.ranges = ranges;
        }

        @Override
        public String toString()
        {
            return "FetchComplete{" +
                   "storeId=" + storeId +
                   ", bound=" + bound +
                   '}';
        }
    }

    private static class PurgeAt
    {
        private final long epoch;
        private final Ranges ranges;

        private PurgeAt(long epoch, Ranges ranges)
        {
            this.epoch = epoch;
            this.ranges = ranges;
        }

        @Override
        public String toString()
        {
            return "PurgeAt{" +
                   ", epoch=" + epoch +
                   ", ranges=" + ranges +
                   '}';
        }
    }

    static final boolean VERIFY_ACCESS = Boolean.getBoolean(System.getProperty("accord.test.verify_liststore_access", "true"));

    static final Timestamped<int[]> EMPTY = new Timestamped<>(Timestamp.NONE, new int[0], Arrays::toString);
    final NavigableMap<RoutableKey, Timestamped<int[]>> data = new TreeMap<>();

    private final List<ChangeAt> addedAts = new ArrayList<>();
    private final List<ChangeAt> removedAts = new ArrayList<>();
    private final List<PurgeAt> purgedAts = new ArrayList<>();
    private final List<FetchComplete> fetchCompletes = new ArrayList<>();
    private Ranges allowedReads = null, allowedWrites = null;
    // used only to detect changes when a new Topology is notified
    private Topology previousTopology = null;
    // used to make sure removes are applied in epoch order and not in the order sync points complete in
    private final LongArrayList pendingRemoves = new LongArrayList();

    public static final class Snapshot
    {
        private final NavigableMap<RoutableKey, Timestamped<int[]>> data;
        private final List<ChangeAt> addedAts;
        private final List<ChangeAt> removedAts;
        private final List<PurgeAt> purgedAts;
        private final List<FetchComplete> fetchCompletes;
        private final LongArrayList pendingRemoves;

        private Snapshot(NavigableMap<RoutableKey, Timestamped<int[]>> data, List<ChangeAt> addedAts, List<ChangeAt> removedAts, List<PurgeAt> purgedAts, List<FetchComplete> fetchCompletes, LongArrayList pendingRemoves)
        {
            this.data = new TreeMap<>(data);
            this.addedAts = new ArrayList<>(addedAts);
            this.removedAts = new ArrayList<>(removedAts);
            this.purgedAts = new ArrayList<>(purgedAts);
            this.fetchCompletes = new ArrayList<>(fetchCompletes);
            this.pendingRemoves = new LongArrayList();
            this.pendingRemoves.addAll(pendingRemoves);
        }
    }

    public void ensureDurable(CommandStore commandStore, RedundantBefore onSuccess, int flags)
    {
        if (commandStore.node().isReplaying())
            return;
        snapshot(false).invoke((success, fail) -> {
            if (fail == null) commandStore.execute((ExecutionContext.Empty)()->"Report DataStore Durable", safeStore -> safeStore.reportDurable(onSuccess, flags));
        });
    }

    public AsyncResult<Void> snapshot(boolean runBeforeRestart)
    {
        Snapshot snapshot = new Snapshot(data, addedAts, removedAts, purgedAts, fetchCompletes, pendingRemoves);
        return super.snapshot(runBeforeRestart, snapshot);
    }

    public void restore()
    {
        super.restore(snapshot -> {
            data.putAll(snapshot.data);
            addedAts.addAll(snapshot.addedAts);
            removedAts.addAll(snapshot.removedAts);
            purgedAts.addAll(snapshot.purgedAts);
            fetchCompletes.addAll(snapshot.fetchCompletes);
            pendingRemoves.addAll(snapshot.pendingRemoves);
        });
    }

    public void clear()
    {
        data.clear();
        addedAts.clear();
        removedAts.clear();
        purgedAts.clear();
        fetchCompletes.clear();
        pendingRemoves.clear();
    }

    // adding here to help trace burn test queries
    public final Node.Id node;

    public ListStore(Scheduler scheduler, RandomSource random, Node.Id node)
    {
        super(scheduler, random);
        this.node = node;
    }

    public synchronized Timestamped<int[]> get(Ranges unavailable, Timestamp executeAt, Key key, boolean isDirectRead)
    {
        // we perform the read, and report alongside its result what we were unable to provide to the coordinator
        // since we might have part of the read request, and it might be necessary for availability for us to serve that part
        if (!unavailable.contains(key))
            checkReadAccess(executeAt, key, isDirectRead);
        Timestamped<int[]> v = data.get(key);
        return v == null ? EMPTY : v;
    }

    public synchronized List<Map.Entry<Key, Timestamped<int[]>>> get(Ranges unavailable, Timestamp executeAt, Range range)
    {
        // we perform the read, and report alongside its result what we were unable to provide to the coordinator
        // since we might have part of the read request, and it might be necessary for availability for us to serve that part
        if (!unavailable.intersects(range))
            checkReadAccess(executeAt, range);
        return data.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                .entrySet().stream().map(e -> (Map.Entry<Key, Timestamped<int[]>>)(Map.Entry)e)
                .collect(Collectors.toList());
    }

    public void write(Key key, Timestamp executeAt, int[] value)
    {
        write(key, new Timestamped<>(executeAt, value, Arrays::toString));
    }

    public void write(Key key, Timestamped<int[]> value)
    {
        checkWriteAccess(value.timestamp, key);
        writeUnsafe(key, value);
    }

    public void writeUnsafe(Key key, Timestamp executeAt, int[] value)
    {
        writeUnsafe(key, new Timestamped<>(executeAt, value, Arrays::toString));
    }

    public void writeUnsafe(Key key, Timestamped<int[]> value)
    {
        data.merge(key, value, ListStore::merge);
    }

    private void checkReadAccess(Timestamp executeAt, Key key, boolean isDirectRead)
    {
        if (!allowedReads.contains(key))
        {
            // TODO (testing): improve this validation logic
            // but in the meantime, whitelist valid things, e.g. ephemeral reads can access data that has been retired
            if (((executeAt instanceof TxnId && ((TxnId) executeAt).awaitsOnlyDeps()) || isDirectRead)
                && removedAts.stream().anyMatch(r -> r.ranges.contains(key) && r.epoch > executeAt.epoch()))
                return;

            throw illegalState("Attempted to access key %s on node %s, which is not in the range %s;\nexecuteAt = %s\n%s",
                               key, node, allowedReads, executeAt, history(key));
        }
    }

    private void checkWriteAccess(Timestamp executeAt, Key key)
    {
        if (!allowedWrites.contains(key))
            throw illegalState("Attempted to access key %s on node %s, which is not in the range %s;\nexecuteAt = %s\n%s",
                               key, node, allowedReads, executeAt, history(key));
    }

    private void checkReadAccess(Timestamp executeAt, Range range)
    {
        if (executeAt instanceof TxnId)
        {
            switch (((TxnId) executeAt).kind())
            {
                case EphemeralRead:
                case ExclusiveSyncPoint:
                    return; // safe to access later
            }
        }
        Ranges singleRanges = Ranges.of(range);
        if (!allowedReads.containsAll(singleRanges))
        {
            // TODO (testing): it is actually safe for a node on an old epoch to still be executing a transaction that has been executed in a later epoch,
            //   making this check over-enthusiastic.
            illegalState(String.format("Attempted to access range %s on node %s, which is not in the range %s;\nexecuteAt = %s\n%s",
                                       range, node, allowedReads, executeAt, history(singleRanges)));
        }
    }

    private String history(String type, Object key, Predicate<Ranges> test)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < addedAts.size(); i++)
        {
            ChangeAt adds = addedAts.get(i);
            if (test.test(adds.ranges))
                sb.append(String.format("Added in %d: %s", (Long)adds.epoch, adds.ranges)).append('\n');
        }
        for (Map.Entry<Integer, Ranges> e : pendingFetches.entrySet())
        {
            if (test.test(e.getValue()))
                sb.append(String.format("Fetch pending for %d -> %s", e.getKey(), e.getValue())).append('\n');
        }
        for (int i = 0; i < fetchCompletes.size(); i++)
        {
            FetchComplete fetch = fetchCompletes.get(i);
            if (test.test(fetch.ranges))
                sb.append(String.format("Fetch seen in store=%d for %s", (Integer)fetch.storeId, fetch.bound)).append('\n');
        }
        for (int i = 0; i < removedAts.size(); i++)
        {
            ChangeAt removes = removedAts.get(i);
            if (test.test(removes.ranges))
                sb.append(String.format("Removed in %d: %s", (Long)removes.epoch, removes.ranges)).append('\n');
        }
        for (int i = 0; i < purgedAts.size(); i++)
        {
            PurgeAt purge = purgedAts.get(i);
            if (test.test(purge.ranges))
                sb.append(String.format("Purged %s in epoch %d",
                                        key, (Long)purge.epoch)).append('\n');
        }
        if (sb.length() == 0)
            sb.append(String.format("Attempted to access %s %s, this node never owned that", type, key));
        return sb.toString();
    }

    private String history(Ranges ranges)
    {
        return history("range", ranges, other -> other.intersects(ranges));
    }

    private String history(RoutableKey key)
    {
        return history("key", key, other -> other.contains(key));
    }

    private final Int2ObjectHashMap<Ranges> pendingFetches = new Int2ObjectHashMap<>();

    @Override
    public FetchResult image(Node node, SafeCommandStore safeStore, Ranges ranges, TxnId atLeast, SortedArrays.SortedArrayList<Node.Id> readable, FetchRanges delegate)
    {
        int storeId = safeStore.commandStore().id();
        synchronized (this)
        {
            pendingFetches.merge(storeId, ranges, Ranges::with);
        }
        FetchRanges hook = new FetchRanges()
        {
            private Ranges success = Ranges.EMPTY;

            @Override
            public StartingRangeFetch starting(Ranges ranges)
            {
                return delegate.starting(ranges);
            }

            @Override
            public void fetched(Ranges fetched)
            {
                synchronized (ListStore.this)
                {
                    allowedReads = allowedReads.with(fetched);
                    success = success.with(fetched);
                    if (pendingFetches.containsKey(storeId))
                    {
                        Ranges pending = pendingFetches.get(storeId).without(fetched);
                        if (pending.isEmpty()) pendingFetches.remove(storeId);
                        else                   pendingFetches.put(storeId, pending);
                    }
                    if (success.equals(ranges))
                        fetchCompletes.add(new FetchComplete(storeId, atLeast, ranges));
                }
                delegate.fetched(fetched);
            }

            @Override
            public void fail(Ranges ranges, Throwable failure)
            {
                synchronized (ListStore.this)
                {
                    if (pendingFetches.containsKey(storeId))
                    {
                        Ranges pending = pendingFetches.get(storeId).without(ranges);
                        if (pending.isEmpty()) pendingFetches.remove(storeId);
                        else                   pendingFetches.put(storeId, pending);
                    }

                    if (!success.isEmpty())
                        fetchCompletes.add(new FetchComplete(storeId, atLeast, success));
                }
                delegate.fail(ranges, new Throwable("Failed Fetch", failure));
            }
        };

        // TODO (desired): start separate coordinators for each TxnId, or otherwise handle the bounds better
        ListFetchCoordinator coordinator;
        try
        {
            coordinator = new ListFetchCoordinator(node, atLeast, ranges, readable, hook, safeStore.commandStore(), this);
        }
        catch (Throwable t)
        {
            return new FetchResult.Failure(t);
        }
        coordinator.start();
        return coordinator.result();
    }

    static Timestamped<int[]> merge(Timestamped<int[]> a, Timestamped<int[]> b)
    {
        return Timestamped.merge(a, b, ListStore::isStrictPrefix, Arrays::equals);
    }

    static Timestamped<int[]> mergeEqual(Timestamped<int[]> a, Timestamped<int[]> b)
    {
        return Timestamped.mergeEqual(a, b, Arrays::equals);
    }

    public NavigableMap<RoutableKey, Timestamped<int[]>> copyOfCurrentData()
    {
        return new TreeMap<>(data);
    }

    public void checkAtLeast(CommandStores commandStores, NavigableMap<RoutableKey, Timestamped<int[]>> a)
    {
        checkAtLeast(commandStores, a, data);
    }

    public static void checkAtLeast(CommandStores commandStores, NavigableMap<RoutableKey, Timestamped<int[]>> a, NavigableMap<RoutableKey, Timestamped<int[]>> b)
    {
        if (a.isEmpty())
            return;
        for (Map.Entry<RoutableKey, Timestamped<int[]>> ae : a.entrySet())
        {
            RoutableKey k = ae.getKey();
            Timestamped<int[]> av = ae.getValue();
            Timestamped<int[]> bv = b.get(k);
            if (bv == null || bv.timestamp.compareTo(av.timestamp) < 0)
            {
                for (CommandStore commandStore : commandStores.all())
                {
                    if (!commandStore.unsafeGetRangesForEpoch().allSince(av.timestamp.epoch()).contains(k))
                        continue;
                    Invariants.require(!commandStore.unsafeGetSafeToRead().lastEntry().getValue().contains(k));
                }
                return;
            }
            Invariants.require(bv.timestamp.equals(av.timestamp) ? Arrays.equals(av.data, bv.data) : ListStore.isStrictPrefix(av.data, bv.data));
        }
    }

    private static boolean isStrictPrefix(int[] a, int[] b)
    {
        if (a.length >= b.length)
            return false;
        for (int i = 0; i < a.length ; ++i)
        {
            if (a[i] != b[i])
                return false;
        }
        return true;
    }

    public synchronized void onTopologyUpdate(Node node, Topology topology)
    {
        long epoch = topology.epoch();
        Ranges updatedRanges = topology.rangesForNode(node.id());
        if (previousTopology == null)
        {
            previousTopology = topology;
            allowedReads = updatedRanges;
            allowedWrites = updatedRanges;
            addedAts.add(new ChangeAt(epoch, updatedRanges));
            return;
        }

        Ranges previousRanges = previousTopology.rangesForNode(node.id());

        Ranges added = updatedRanges.without(previousRanges);
        Ranges removed = previousRanges.without(updatedRanges);
        if (!added.isEmpty())
        {
            addedAts.add(new ChangeAt(epoch, added));
            for (Range range : added)
            {
                allowedWrites = allowedWrites.with(Ranges.of(range));
                if (!previousTopology.ranges().intersects(range))
                {
                    // A range was added that globally didn't exist before; there is nothing to bootstrap here!
                    // TODO (testing): document this history change
                    allowedReads = allowedReads.with(Ranges.of(range));
                }
            }
        }
        if (!removed.isEmpty())
        {
            pendingRemoves.add((Long)epoch);
            removedAts.add(new ChangeAt(epoch, removed));
        }
        previousTopology = topology;
    }

    @Override
    public void onEpochRetired(Ranges ranges, long epoch, @Nullable Topology topology)
    {
        if (pendingRemoves.containsLong(epoch))
        {
            for (ChangeAt change : removedAts)
            {
                if (change.epoch == epoch && ranges.intersects(change.pending))
                {
                    purgedAts.add(new PurgeAt(epoch, ranges.slice(change.pending, Minimal)));
                    change.pending = change.pending.without(ranges);
                    if (change.pending.isEmpty())
                        pendingRemoves.removeLong(epoch);
                    break;
                }
            }
        }
    }
}
