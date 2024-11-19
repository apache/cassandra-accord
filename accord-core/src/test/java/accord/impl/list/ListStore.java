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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import accord.api.DataStore;
import accord.api.Key;
import accord.api.Scheduler;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.ExecuteSyncPoint;
import accord.coordinate.ExecuteSyncPoint.SyncPointErased;
import accord.coordinate.Exhausted;
import accord.coordinate.Invalidated;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.coordinate.TopologyMismatch;
import accord.coordinate.Truncated;
import accord.coordinate.tracking.AllTracker;
import accord.impl.basic.SimulatedFault;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.Timestamped;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.LongArrayList;

import static accord.utils.Invariants.illegalState;

public class ListStore implements DataStore
{
    private static class ChangeAt
    {
        private final long epoch;
        private final Ranges ranges;

        private ChangeAt(long epoch, Ranges ranges)
        {
            this.epoch = epoch;
            this.ranges = ranges;
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
        private final SyncPoint syncPoint;
        private final Ranges ranges;

        private FetchComplete(int storeId, SyncPoint syncPoint, Ranges ranges)
        {
            this.storeId = storeId;
            this.syncPoint = syncPoint;
            this.ranges = ranges;
        }

        @Override
        public String toString()
        {
            return "FetchComplete{" +
                   "storeId=" + storeId +
                   ", syncPoint=(" + syncPoint.syncId + ", " + syncPoint.route + ")" +
                   ", ranges=" + ranges +
                   '}';
        }
    }

    private static class PurgeAt
    {
        private final SyncPoint syncPoint;
        private final long epoch;
        private final Ranges ranges;

        private PurgeAt(SyncPoint syncPoint, long epoch, Ranges ranges)
        {
            this.syncPoint = syncPoint;
            this.epoch = epoch;
            this.ranges = ranges;
        }

        @Override
        public String toString()
        {
            return "PurgeAt{" +
                   "syncPoint=(" + syncPoint.syncId + ", " + syncPoint.route + ")" +
                   ", epoch=" + epoch +
                   ", ranges=" + ranges +
                   '}';
        }
    }

    static final boolean VERIFY_ACCESS = Boolean.getBoolean(System.getProperty("accord.test.verify_liststore_access", "true"));
    static final Timestamped<int[]> EMPTY = new Timestamped<>(Timestamp.NONE, new int[0], Arrays::toString);
    final NavigableMap<RoutableKey, Timestamped<int[]>> data = new TreeMap<>();
    final Scheduler scheduler;
    final RandomSource random;

    private final List<ChangeAt> addedAts = new ArrayList<>();
    private final List<ChangeAt> removedAts = new ArrayList<>();
    private final List<PurgeAt> purgedAts = new ArrayList<>();
    private final List<FetchComplete> fetchCompletes = new ArrayList<>();
    private Ranges allowed = null;
    // used only to detect changes when a new Topology is notified
    private Topology previousTopology = null;
    // used to make sure removes are applied in epoch order and not in the order sync points complete in
    private final LongArrayList pendingRemoves = new LongArrayList();
    // when out of order epochs are detected, this holds the callbacks to try again
    private final List<Runnable> onRemovalDone = new ArrayList<>();

    private static final class Snapshot
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

    private static final class PendingSnapshot
    {
        // we don't restart bootstraps after clearing journal state and replaying, so must finish snapshots
        final boolean runBeforeRestart;
        final long delay;
        final Consumer<Boolean> onCompletion;

        private PendingSnapshot(boolean runBeforeRestart, long delay, Consumer<Boolean> onCompletion)
        {
            this.runBeforeRestart = runBeforeRestart;
            this.delay = delay;
            this.onCompletion = onCompletion;
        }
    }

    private Snapshot snapshot;
    private Scheduler.Scheduled scheduled;
    private final Deque<PendingSnapshot> pendingSnapshots = new ArrayDeque<>();
    private long pendingDelay = 0;

    @Override
    public AsyncResult<Void> snapshot(Ranges ranges, TxnId before)
    {
        return snapshot(false, ranges, before);
    }

    public AsyncResult<Void> snapshot(boolean runBeforeRestart, Ranges ranges, TxnId before)
    {
        Snapshot snapshot = new Snapshot(data, addedAts, removedAts, purgedAts, fetchCompletes, pendingRemoves);
        AsyncResult.Settable<Void> result = new AsyncResults.SettableResult<>();
        long delay = Math.max(1, random.nextBiasedLong(100, 1000, 5000) - pendingDelay);
        pendingDelay += delay;

        if (scheduled != null && runBeforeRestart && !pendingSnapshots.stream().anyMatch(p -> p.runBeforeRestart))
        {
            scheduled.cancel();
            scheduled = null;
        }

        pendingSnapshots.add(new PendingSnapshot(runBeforeRestart, delay, success -> {
            if (success)
            {
                this.snapshot = snapshot;
                result.setSuccess(null);
            }
            else
            {
                result.setFailure(new RuntimeException("Snapshot aborted due to earlier snapshot being restored"));
            }
        }));

        if (scheduled == null)
            scheduleRunSnapshot();
        return result;
    }

    private void scheduleRunSnapshot()
    {
        Invariants.checkState(!pendingSnapshots.isEmpty());
        // schedule as recurring so that we don't run them
        Runnable run = () -> {
            scheduled = null;
            if (pendingSnapshots.isEmpty())
                return;

            PendingSnapshot pendingSnapshot = pendingSnapshots.pollFirst();
            pendingSnapshot.onCompletion.accept(true);
            pendingDelay -= pendingSnapshot.delay;
            if (!pendingSnapshots.isEmpty())
                scheduleRunSnapshot();
        };

        if (pendingSnapshots.stream().anyMatch(p -> p.runBeforeRestart)) scheduled = scheduler.once(run, pendingSnapshots.peekFirst().delay, TimeUnit.MILLISECONDS);
        else scheduled = scheduler.selfRecurring(run, pendingSnapshots.peekFirst().delay, TimeUnit.MILLISECONDS);
    }

    public void restoreFromSnapshot()
    {
        if (snapshot != null)
        {
            data.putAll(snapshot.data);
            addedAts.addAll(snapshot.addedAts);
            removedAts.addAll(snapshot.removedAts);
            purgedAts.addAll(snapshot.purgedAts);
            fetchCompletes.addAll(snapshot.fetchCompletes);
            pendingRemoves.addAll(snapshot.pendingRemoves);
        }

        while (!pendingSnapshots.isEmpty())
            pendingSnapshots.pollFirst().onCompletion.accept(false);
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
        this.scheduler = scheduler;
        this.random = random;
        this.node = node;
    }

    public synchronized Timestamped<int[]> get(Ranges unavailable, Timestamp executeAt, Key key)
    {
        // we perform the read, and report alongside its result what we were unable to provide to the coordinator
        // since we might have part of the read request, and it might be necessary for availability for us to serve that part
        if (!unavailable.contains(key))
            checkAccess(executeAt, key);
        Timestamped<int[]> v = data.get(key);
        return v == null ? EMPTY : v;
    }

    public synchronized List<Map.Entry<Key, Timestamped<int[]>>> get(Ranges unavailable, Timestamp executeAt, Range range)
    {
        // we perform the read, and report alongside its result what we were unable to provide to the coordinator
        // since we might have part of the read request, and it might be necessary for availability for us to serve that part
        if (!unavailable.intersects(range))
            checkAccess(executeAt, range);
        return data.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                .entrySet().stream().map(e -> (Map.Entry<Key, Timestamped<int[]>>)(Map.Entry)e)
                .collect(Collectors.toList());
    }

    public synchronized void write(Key key, Timestamp executeAt, int[] value)
    {
        checkAccess(executeAt, key);
        data.merge(key, new Timestamped<>(executeAt, value, Arrays::toString), ListStore::merge);
    }

    private void checkAccess(Timestamp executeAt, Key key)
    {
        if (!allowed.contains(key))
        {
            // TODO (expected): improve this validation logic
            // but in the meantime, whitelist valid things, e.g. ephemeral reads can access data that has been retired
            if (executeAt instanceof TxnId && ((TxnId) executeAt).awaitsOnlyDeps()
                && removedAts.stream().anyMatch(r -> r.ranges.contains(key) && r.epoch > executeAt.epoch()))
                return;

            throw illegalState("Attempted to access key %s on node %s, which is not in the range %s;\nexecuteAt = %s\n%s",
                               key, node, allowed, executeAt, history(key));
        }
    }

    private void checkAccess(Timestamp executeAt, Range range)
    {
        if (executeAt instanceof TxnId)
        {
            switch (((TxnId) executeAt).kind())
            {
                case EphemeralRead:
                case ExclusiveSyncPoint:
                    // TODO (required): introduce some mechanism for EphemeralReads/ExclusiveSyncPoints to abort if local store has been expunged (and check it here)
                    return; // safe to access later
            }
        }
        Ranges singleRanges = Ranges.of(range);
        if (!allowed.containsAll(singleRanges))
        {
            // TODO (required): it is actually safe for a node on an old epoch to still be executing a transaction that has been executed in a later epoch,
            //   making this check over-enthusiastic.
            illegalState(String.format("Attempted to access range %s on node %s, which is not in the range %s;\nexecuteAt = %s\n%s",
                                       range, node, allowed, executeAt, history(singleRanges)));
        }
    }

    private String history(String type, Object key, Predicate<Ranges> test)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < addedAts.size(); i++)
        {
            ChangeAt adds = addedAts.get(i);
            if (test.test(adds.ranges))
                sb.append(String.format("Added in %d: %s", adds.epoch, adds.ranges)).append('\n');
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
                sb.append(String.format("Fetch seen in store=%d for %s: %s", fetch.storeId, format(fetch.syncPoint), fetch.ranges)).append('\n');
        }
        for (int i = 0; i < removedAts.size(); i++)
        {
            ChangeAt removes = removedAts.get(i);
            if (test.test(removes.ranges))
                sb.append(String.format("Removed in %d: %s", removes.epoch, removes.ranges)).append('\n');
        }
        for (int i = 0; i < purgedAts.size(); i++)
        {
            PurgeAt purge = purgedAts.get(i);
            if (test.test(purge.ranges))
                sb.append(String.format("Purged in %s for epoch %d; waiting-for %s",
                                        format(purge.syncPoint),
                                        purge.epoch,
                                        purge.syncPoint.waitFor.txnIds())).append('\n');
        }
        if (sb.length() == 0)
            sb.append(String.format("Attempted to access %s %s, this node never owned that", type, key));
        return sb.toString();
    }

    private static String format(SyncPoint sp)
    {
        return String.format("(%s -> %s)", sp.syncId, sp.route);
    }

    public String historySeekable(Seekable o)
    {
        switch (o.domain())
        {
            case Key: return history(o.asKey());
            case Range: return history(Ranges.single(o.asRange()));
            default: throw new IllegalArgumentException("Unknown domain: " + o.domain() + ", input=" + o);
        }
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
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges delegate)
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
                    allowed = allowed.with(fetched);
                    success = success.with(fetched);
                    if (pendingFetches.containsKey(storeId))
                    {
                        Ranges pending = pendingFetches.get(storeId).without(fetched);
                        if (pending.isEmpty()) pendingFetches.remove(storeId);
                        else                   pendingFetches.put(storeId, pending);
                    }
                    if (success.equals(ranges))
                        fetchCompletes.add(new FetchComplete(storeId, syncPoint, ranges));
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
                        fetchCompletes.add(new FetchComplete(storeId, syncPoint, success));
                }
                delegate.fail(ranges, new Throwable("Failed Fetch", failure));
            }
        };
        ListFetchCoordinator coordinator = new ListFetchCoordinator(node, ranges, syncPoint, hook, safeStore.commandStore(), this);
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

    public boolean equals(NavigableMap<RoutableKey, Timestamped<int[]>> a)
    {
        return equal(a, data);
    }

    public static boolean equal(NavigableMap<RoutableKey, Timestamped<int[]>> a, NavigableMap<RoutableKey, Timestamped<int[]>> b)
    {
        Iterator<Map.Entry<RoutableKey, Timestamped<int[]>>> aiter = a.entrySet().iterator();
        Iterator<Map.Entry<RoutableKey, Timestamped<int[]>>> biter = b.entrySet().iterator();
        while (aiter.hasNext() && biter.hasNext())
        {
            Map.Entry<RoutableKey, Timestamped<int[]>> av = aiter.next();
            Map.Entry<RoutableKey, Timestamped<int[]>> bv = biter.next();
            if (!av.getKey().equals(bv.getKey()))
                return false;
            if (!av.getValue().equals(bv.getValue(), Arrays::equals))
                return false;
        }
        if (aiter.hasNext() || biter.hasNext())
            return false;
        return true;
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
            allowed = updatedRanges;
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
                if (!previousTopology.ranges().intersects(range))
                {
                    // A range was added that globally didn't exist before; there is nothing to bootstrap here!
                    // TODO (now): document this history change
                    allowed = allowed.with(Ranges.of(range));
                }
            }
        }
        if (!removed.isEmpty())
        {
            pendingRemoves.add(epoch);
            removedAts.add(new ChangeAt(epoch, removed));
            // There are 2 different types of remove
            // 1) node no longer covers, but the range exists in the cluster
            // 2) the range no longer exists in the cluster
            // Given this, we need 2 different solutions for the purge logic as sync points are not safe when running in older epoch.
            Ranges localRemove = Ranges.EMPTY;
            Ranges globalRemove = Ranges.EMPTY;
            for (Range range : removed)
            {
                if (topology.ranges().intersects(range)) localRemove = localRemove.with(Ranges.of(range));
                else                                     globalRemove = globalRemove.with(Ranges.of(range));
            }
            if (!localRemove.isEmpty())
            {
                Ranges finalLocalRemove = localRemove;
                runWhenReady(node, epoch, () -> removeLocalWhenReady(node, epoch, finalLocalRemove));
            }
            // TODO (correctness, coverage): add cleanup logic for global ranges removed. This must be solved in CASSANDRA-18675
        }
        previousTopology = topology;
    }

    private void runWhenReady(Node node, long epoch, Runnable whenKnown)
    {
        if (node.topology().epoch() >= epoch) whenKnown.run();
        else                                  node.scheduler().selfRecurring(() -> runWhenReady(node, epoch, whenKnown), 10, TimeUnit.SECONDS);
    }

    private void removeLocalWhenReady(Node node, long epoch, Ranges removed)
    {
        // TODO (now): can this migrate to a listener as bootstrap will do a SyncPoint which will propgate this knowlege?

        // TODO (duplication): there is overlap with durability scheduling, but that logic has a few issues which make it hard to use here:
        // * always works off latest topology, so the removed ranges won't actually get marked durable!
        // * api is range -> TxnId, so we still need a TxnId to be referenced off... so we need to create a TxnId to know when we are in-sync!
        // * if we have a sync point, we really only care if the sync point is applied globally...
        // * even though CoordinateDurabilityScheduling is part of BurnTest, it was seen that shard/global syncs were only happening in around 1/5 of the tests (mostly due to timeouts/invalidates), which would mean purge was called infrequently.
        currentSyncPoint(node, removed).flatMap(sp -> awaitSyncPoint(node, sp)).begin((s, f) -> {
            if (f != null)
            {
                node.agent().onUncaughtException(f);
                return;
            }
            performRemoval(epoch, removed, s);
        });
    }

    private synchronized void performRemoval(long epoch, Ranges removed, SyncPoint s)
    {
        // TODO (effeciency, correctness): remove the delayed removal logic.
        // This logic was added to make sure the sequence of events made sense but doesn't handle everything perfectly; I (David C) believe that this code
        // will suffer from the ABA problem; if a range is removed, then added back it is not likley to be handled correctly (the add will no-op as it wasn't removed, then the remove will remove it!)
        if (pendingRemoves.isEmpty()) return;
        if (pendingRemoves.get(0) != epoch)
        {
            onRemovalDone.add(() -> performRemoval(epoch, removed, s));
            return;
        }
        pendingRemoves.remove(epoch);
        this.allowed = this.allowed.without(removed);
        purgedAts.add(new PurgeAt(s, epoch, removed));
        // C* encodes keyspace/table within a Range, so Ranges being added/removed to the cluster are expected behaviors and not just local range movements.
        // however, there is no reason to erase old data as it being used should be detected as a violation due to being stale,
        // and clearing data unnecessarily complicates journal replay

        List<Runnable> callbacks = new ArrayList<>(onRemovalDone);
        onRemovalDone.clear();
        callbacks.forEach(Runnable::run);
    }

    private static AsyncChain<SyncPoint<Range>> currentSyncPoint(Node node, Ranges removed)
    {
        return CoordinateSyncPoint.exclusiveSyncPoint(node, removed)
                                  .recover(t -> {
                                      // TODO (api, effeciency): retry with backoff.
                                      // TODO (effeciency): if Preempted, can we keep block waiting?
                                      if (t instanceof Invalidated || t instanceof Timeout || t instanceof Preempted || t instanceof Truncated)
                                          return currentSyncPoint(node, removed);
                                      if (t instanceof TopologyMismatch)
                                      {
                                          // If the home key was randomly selected and no longer valid, can just retry to get a new home key,
                                          // but if the actual range is no longer valid then ExclusiveSyncPoints are broken and can not be used.
                                          if (!((TopologyMismatch) t).hasReason(TopologyMismatch.Reason.KEYS_OR_RANGES))
                                              return currentSyncPoint(node, removed);
                                          // TODO (correctness): handle this case
                                          node.agent().onUncaughtException(t);
                                      }
                                      return null;
                                  });
    }

    private static AsyncChain<SyncPoint<Range>> awaitSyncPoint(Node node, SyncPoint<Range> exclusiveSyncPoint)
    {
        Await e = new Await(node, exclusiveSyncPoint);
        e.addCallback(() -> node.configService().reportEpochRedundant(exclusiveSyncPoint.route.toRanges(), exclusiveSyncPoint.syncId.epoch() - 1));
        e.start();
        return e.recover(t -> {
            if (t.getClass() == SyncPointErased.class)
                return AsyncChains.success(null);
            if (t instanceof Timeout ||
                // TODO (expected): why are we not simply handling Insufficient properly?
                t instanceof RuntimeException && "Insufficient".equals(t.getMessage()) ||
                t instanceof SimulatedFault ||
                t instanceof Exhausted)
                return awaitSyncPoint(node, exclusiveSyncPoint);
            // cannot loop indefinitely
            if (t instanceof RuntimeException && "Redundant".equals(t.getMessage()))
                return AsyncChains.success(null);
            return null;
        });
    }

    private static class Await extends ExecuteSyncPoint.ExecuteExclusive
    {
        public Await(Node node, SyncPoint<Range> syncPoint)
        {
            super(node, syncPoint, AllTracker::new);
        }

        @Override
        public void start()
        {
            super.start();
        }
    }
}
