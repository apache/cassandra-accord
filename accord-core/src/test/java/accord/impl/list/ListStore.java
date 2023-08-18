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
import java.util.stream.Collectors;

import accord.api.ConfigurationService;
import accord.api.DataStore;
import accord.api.Key;
import accord.coordinate.CoordinateSyncPoint;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.utils.Timestamped;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.agrona.collections.Long2ObjectHashMap;

public class ListStore implements DataStore
{
    private static class RemovedAt
    {
        private final long epoch;
        private final Ranges removed;

        private RemovedAt(long epoch, Ranges removed)
        {
            this.epoch = epoch;
            this.removed = removed;
        }

        @Override
        public String toString()
        {
            return "RemovedAt{" +
                   "epoch=" + epoch +
                   ", removed=" + removed +
                   '}';
        }
    }

    static final Timestamped<int[]> EMPTY = new Timestamped<>(Timestamp.NONE, new int[0]);
    final NavigableMap<RoutableKey, Timestamped<int[]>> data = new TreeMap<>();
    private final List<RemovedAt> removedAts = new ArrayList<>();
    // TODO (now): remove?  I have it here for when I am running in a debugger...
    private long epoch = -1; // this is here for
    private Ranges ranges = null;

    // adding here to help trace burn test queries
    public final Node.Id node;

    public ListStore(Node.Id node)
    {
        this.node = node;
    }

    public synchronized Timestamped<int[]> get(Key key)
    {
        checkAccess(key);
        Timestamped<int[]> v = data.get(key);
        return v == null ? EMPTY : v;
    }

    public synchronized List<Map.Entry<Key, Timestamped<int[]>>> get(Range range)
    {
        checkAccess(range);
        return data.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                .entrySet().stream().map(e -> (Map.Entry<Key, Timestamped<int[]>>)(Map.Entry)e)
                .collect(Collectors.toList());
    }

    public synchronized void write(Key key, Timestamp executeAt, int[] value)
    {
        checkAccess(key);
        data.merge(key, new Timestamped<>(executeAt, value), ListStore::merge);
    }

    private void checkAccess(Key key)
    {
        if (!ranges.contains(key))
            throw new IllegalStateException(String.format("Attempted to access key %s, which is not in the range %s;\n", key, ranges, findRemovedAt(key)));
    }

    private void checkAccess(Range range)
    {
        Ranges singleRanges = Ranges.of(range);
        if (!ranges.containsAll(singleRanges))
            throw new IllegalStateException(String.format("Attempted to access range %s, which is not in the range %s;\n", range, ranges, String.join("\n", findRemovedAt(singleRanges))));
    }

    private List<String> findRemovedAt(Ranges ranges)
    {
        List<String> matches = new ArrayList<>();
        for (int i = removedAts.size() - 1; i >= 0 && !ranges.isEmpty(); i--)
        {
            RemovedAt at = removedAts.get(i);
            if (at.removed.intersects(ranges))
            {
                ranges = ranges.subtract(at.removed);
                matches.add(String.format("Attempted to access range %s, removed at epoch %d", at.removed, at.epoch));
            }
        }
        if (!ranges.isEmpty())
            matches.add(String.format("Attempted to access range %s, this node never owned that range", ranges));
        return matches;
    }

    private String findRemovedAt(RoutableKey key)
    {
        for (int i = removedAts.size() - 1; i >= 0 && !ranges.isEmpty(); i--)
        {
            RemovedAt at = removedAts.get(i);
            if (at.removed.contains(key))
            {
                ranges = ranges.subtract(at.removed);
                return String.format("Attempted to access range %s, removed at epoch %d", at.removed, at.epoch);
            }
        }
        return String.format("Attempted to access range %s, this node never owned that range", ranges);
    }

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        if (!ranges.containsAll(ranges))
            throw new IllegalStateException(String.format("Attempted to access ranges %s, which is not in the range %s", ranges, this.ranges));

        ListFetchCoordinator coordinator = new ListFetchCoordinator(node, ranges, syncPoint, callback, safeStore.commandStore(), this);
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

    private final Long2ObjectHashMap<Ranges> pendingRanges = new Long2ObjectHashMap<>();

    public synchronized void onRangeUpdate(Node node, long epoch, Ranges updatedRanges)
    {
        if (ranges == null)
        {
            ranges = updatedRanges;
            this.epoch = epoch;
        }
        else
        {
            pendingRanges.put(epoch, updatedRanges);

            Ranges added = updatedRanges.subtract(ranges);
            Ranges removed = ranges.subtract(updatedRanges);
            if (!removed.isEmpty())
                removedAts.add(new RemovedAt(epoch, removed));

            if (!(added.isEmpty() || removed.isEmpty()))
                runWhenReady(node, epoch, () -> updateDataWhenEpochReady(node, epoch, updatedRanges, removed));
        }
    }

    private void runWhenReady(Node node, long epoch, Runnable whenKnown)
    {
        if (node.topology().epoch() >= epoch) whenKnown.run();
        else                                  node.scheduler().once(() -> runWhenReady(node, epoch, whenKnown), 10, TimeUnit.SECONDS);
    }

    private void updateDataWhenEpochReady(Node node, long epoch, Ranges updatedRanges, Ranges removed)
    {
        // TODO (now): a store can make progress once that bootstrap completes, so waiting on all causes issues
        // need to ADD ranges on bootstrap complete
        // need to REMOVE ranges on hand-rolled sync point
        ConfigurationService.EpochReady ready = node.topology().epochReady(epoch);
        // data/reads should be the same, but just in case wait for both!
        AsyncChain<?> await = AsyncChains.reduce(ready.data, ready.reads, (a, b) -> null);
        // when you add you bootstrap, when you remove you only invalidate pending bootstraps (or no-op), so create a sync point to make sure we don't see old things
        await = await.flatMap(ignore -> CoordinateSyncPoint.exclusive(node, removed));
        await.begin((s, f) -> {
            if (f != null)
            {
                node.agent().onUncaughtException(f);
            }
            else
            {
                synchronized (this)
                {
                    if (this.epoch >= epoch)
                        throw new IllegalStateException("Seen a epoch ready before the other one?");
                    for (Range range : removed)
                    {
                        NavigableMap<RoutableKey, Timestamped<int[]>> historicData = data.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive());
                        historicData.clear();
                    }
                    this.epoch = epoch;
                    this.ranges = updatedRanges;
                }
            }
        });
    }
}
