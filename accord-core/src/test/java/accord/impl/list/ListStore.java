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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import accord.api.DataStore;
import accord.api.Key;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.Exhausted;
import accord.coordinate.Invalidated;
import accord.coordinate.tracking.AppliedTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.messages.Callback;
import accord.messages.ReadData;
import accord.messages.WaitUntilApplied;
import accord.primitives.FullRangeRoute;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.topology.Topologies;
import accord.utils.Timestamped;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.collections.Int2ObjectHashMap;

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
                   ", syncPoint=" + syncPoint +
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
                   "syncPoint=" + syncPoint +
                   ", epoch=" + epoch +
                   ", ranges=" + ranges +
                   '}';
        }
    }

    static final Timestamped<int[]> EMPTY = new Timestamped<>(Timestamp.NONE, new int[0]);
    final NavigableMap<RoutableKey, Timestamped<int[]>> data = new TreeMap<>();
    private final List<ChangeAt> addedAts = new ArrayList<>();
    private final List<ChangeAt> removedAts = new ArrayList<>();
    private final List<PurgeAt> purgedAts = new ArrayList<>();
    private final List<FetchComplete> fetchCompletes = new ArrayList<>();
    private Ranges ranges = null;

    // adding here to help trace burn test queries
    public final Node.Id node;

    public ListStore(Node.Id node)
    {
        this.node = node;
    }

    public synchronized Timestamped<int[]> get(Timestamp executeAt, Key key)
    {
        checkAccess(executeAt, key);
        Timestamped<int[]> v = data.get(key);
        return v == null ? EMPTY : v;
    }

    public synchronized List<Map.Entry<Key, Timestamped<int[]>>> get(Timestamp executeAt, Range range)
    {
        checkAccess(executeAt, range);
        return data.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                .entrySet().stream().map(e -> (Map.Entry<Key, Timestamped<int[]>>)(Map.Entry)e)
                .collect(Collectors.toList());
    }

    public synchronized void write(Key key, Timestamp executeAt, int[] value)
    {
        checkAccess(executeAt, key);
        data.merge(key, new Timestamped<>(executeAt, value), ListStore::merge);
    }

    private void checkAccess(Timestamp executeAt, Key key)
    {
        if (!ranges.contains(key))
            throw new IllegalStateException(String.format("Attempted to access key %s on node %s, which is not in the range %s;\nexecuteAt = %s\n%s",
                                                          key, node, ranges,
                                                          executeAt,
                                                          history(executeAt, key)));
    }

    private void checkAccess(Timestamp executeAt, Range range)
    {
        Ranges singleRanges = Ranges.of(range);
        if (!ranges.containsAll(singleRanges))
            throw new IllegalStateException(String.format("Attempted to access range %s on node %s, which is not in the range %s;\nexecuteAt = %s\n%s",
                                                          range, node, ranges,
                                                          executeAt,
                                                          history(executeAt, singleRanges)));
    }

    private String history(Timestamp executeAt, String type, Object key, Predicate<Ranges> test)
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
                sb.append(String.format("Fetch seen in %d for %s: %s", fetch.storeId, fetch.syncPoint, fetch.ranges)).append('\n');
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
                sb.append(String.format("Purged in (%s -> %s) for epoch %d: %s",
                                        purge.syncPoint.syncId, purge.syncPoint.ranges,
                                        purge.epoch,
                                        purge.ranges)).append('\n');
        }
        if (sb.length() == 0)
            sb.append(String.format("Attempted to access %s %s, this node never owned that", type, key));
        return sb.toString();
    }

    private String history(Timestamp executeAt, Ranges ranges)
    {
        return history(executeAt, "range", ranges, other -> other.intersects(ranges));
    }

    private String history(Timestamp executeAt, RoutableKey key)
    {
        return history(executeAt, "key", key, other -> other.contains(key));
    }

    private final Int2ObjectHashMap<Ranges> pendingFetches = new Int2ObjectHashMap<>();

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        if (!ranges.containsAll(ranges))
            throw new IllegalStateException(String.format("Attempted to access ranges %s, which is not in the range %s", ranges, this.ranges));

        ListFetchCoordinator coordinator = new ListFetchCoordinator(node, ranges, syncPoint, callback, safeStore.commandStore(), this);
        coordinator.start();
        FetchResult result = coordinator.result();
        int storeId = safeStore.commandStore().id();
        synchronized (this)
        {
            Ranges pending = pendingFetches.getOrDefault(storeId, Ranges.EMPTY);
            pending = pending.with(ranges);
            pendingFetches.put(storeId, pending);
        }
        // AsyncResult listeners are applied in FILO, which means that the consumer of this will be run before this local callback; so need to use .map to sequence events properly
        AsyncChain<Ranges> callbackApplied = result.map(empty -> {
            int c1 = storeId;
            Node c2 = node;
            synchronized (this)
            {
                fetchCompletes.add(new FetchComplete(storeId, syncPoint, ranges));
                this.ranges = this.ranges.with(ranges);
            }
            return empty;
        }).addCallback((s, f) -> {
            int c1 = storeId;
            Node c2 = node;
            Ranges pending = pendingFetches.get(storeId);
            pending = pending.subtract(ranges);
            if (pending.isEmpty()) pendingFetches.remove(storeId);
            else                   pendingFetches.put(storeId, pending);
        });
        class Forwarding extends AsyncResults.SettableResult<Ranges> implements FetchResult
        {
            @Override
            public void abort(Ranges ranges)
            {
                result.abort(ranges);
            }
        }
        Forwarding forwarding = new Forwarding();
        callbackApplied.begin(forwarding.settingCallback());
        return forwarding;
    }

    static Timestamped<int[]> merge(Timestamped<int[]> a, Timestamped<int[]> b)
    {
        return Timestamped.merge(a, b, ListStore::isStrictPrefix, Arrays::equals);
    }

    static Timestamped<int[]> mergeEqual(Timestamped<int[]> a, Timestamped<int[]> b)
    {
        return Timestamped.mergeEqual(a, b, Arrays::equals);
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

    private Ranges previousRanges = null;
    private long lastRemovedEpoch = -1;

    public synchronized void onRangeUpdate(Node node, long epoch, Ranges updatedRanges)
    {
        if (previousRanges == null)
        {
            previousRanges = ranges = updatedRanges;
            this.lastRemovedEpoch = epoch;
            return;
        }

        Ranges added = updatedRanges.subtract(previousRanges);
        Ranges removed = previousRanges.subtract(updatedRanges);
        if (!added.isEmpty())
            addedAts.add(new ChangeAt(epoch, added));
        if (!removed.isEmpty())
            removedAts.add(new ChangeAt(epoch, removed));

        if (!removed.isEmpty())
            runWhenReady(node, epoch, () -> removeWhenReady(node, epoch, removed));
        previousRanges = updatedRanges;
    }

    private void runWhenReady(Node node, long epoch, Runnable whenKnown)
    {
        if (node.topology().epoch() >= epoch) whenKnown.run();
        else                                  node.scheduler().once(() -> runWhenReady(node, epoch, whenKnown), 10, TimeUnit.SECONDS);
    }

    private void removeWhenReady(Node node, long epoch, Ranges removed)
    {
        AsyncChain<SyncPoint> await = node.topology().awaitEpoch(epoch).flatMap(i1 -> syncPoint(node, removed));
        await.begin((s, f) -> {
            if (f != null)
            {
                node.agent().onUncaughtException(f);
            }
            else
            {
                synchronized (this)
                {
                    if (this.lastRemovedEpoch >= epoch)
                    {
                        node.agent().onUncaughtException(new IllegalStateException("Seen a epoch ready before the other one?"));
                        return;
                    }
                    this.lastRemovedEpoch = epoch;
                    this.ranges = this.ranges.subtract(removed);
                    purgedAts.add(new PurgeAt(s, epoch, removed));
                    for (Range range : removed)
                    {
                        NavigableMap<RoutableKey, Timestamped<int[]>> historicData = data.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive());
                        historicData.clear();
                    }
                }
            }
        });
    }

    private static AsyncChain<SyncPoint> syncPoint(Node node, Ranges removed)
    {
        return CoordinateSyncPoint.exclusive(node, removed)
                                  .recover(t -> {
                                      if (t instanceof Invalidated)
                                          return syncPoint(node, removed);
                                      return null;
                                  })
               .flatMap(sp -> new Await(node, sp));
    }

    private static class Await extends AsyncResults.SettableResult<SyncPoint> implements Callback<ReadData.ReadReply>
    {
        private final Node node;
        private final AppliedTracker tracker;
        private final SyncPoint exclusiveSyncPoint;

        private Await(Node node, SyncPoint exclusiveSyncPoint)
        {
            FullRangeRoute route = (FullRangeRoute) node.computeRoute(exclusiveSyncPoint.syncId, exclusiveSyncPoint.ranges);
            Topologies topologies = node.topology().withOpenEpochs(route, exclusiveSyncPoint.syncId, exclusiveSyncPoint.syncId);

            this.node = node;
            this.tracker = new AppliedTracker(topologies);
            this.exclusiveSyncPoint = exclusiveSyncPoint;
        }

        public static AsyncResult<SyncPoint> coordinate(Node node, SyncPoint sp)
        {
            Await coordinate = new Await(node, sp);
            coordinate.start();
            return coordinate;
        }

        private void start()
        {
            node.send(tracker.nodes(), to -> new WaitUntilApplied(to, tracker.topologies(), exclusiveSyncPoint.syncId, exclusiveSyncPoint.ranges, exclusiveSyncPoint.syncId), this);
        }

        @Override
        public void onSuccess(Node.Id from, ReadData.ReadReply reply)
        {
            if (!reply.isOk())
            {
                ReadData.ReadNack nack = (ReadData.ReadNack) reply;
                switch (nack)
                {
                    default: throw new AssertionError("Unhandled: " + reply);

                    case NotCommitted:
                    case Redundant:
                        tryFailure(new RuntimeException(nack.name()));
                        return;

                    case Invalid:
                        tryFailure(new Invalidated(exclusiveSyncPoint.syncId, exclusiveSyncPoint.homeKey));
                        return;
                }
            }
            else
            {
                if (tracker.recordSuccess(from) == RequestStatus.Success)
                {
                    node.configService().reportEpochRedundant(exclusiveSyncPoint.ranges, exclusiveSyncPoint.syncId.epoch());
                    trySuccess(exclusiveSyncPoint);
                }
            }
        }

        @Override
        public void onFailure(Node.Id from, Throwable failure)
        {
            if (tracker.recordFailure(from) == RequestStatus.Failed)
                tryFailure(new Exhausted(exclusiveSyncPoint.syncId, exclusiveSyncPoint.homeKey));
        }

        @Override
        public void onCallbackFailure(Node.Id from, Throwable failure)
        {
            tryFailure(failure);
        }
    }
}
