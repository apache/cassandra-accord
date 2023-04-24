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

import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.Key;
import accord.coordinate.FetchCoordinator;
import accord.impl.InMemoryCommandStore;
import accord.local.CommandStore;
import accord.local.Node;
import accord.api.DataStore;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.messages.ReadData.ReadNack;
import accord.messages.Callback;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.WaitAndReadData;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.Timestamped;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.primitives.Routables.Slice.Minimal;

public class ListStore implements DataStore
{
    static class SyncResult extends SettableResult<Ranges> implements FetchResult
    {
        final SyncCoordinator coordinator;

        SyncResult(SyncCoordinator coordinator)
        {
            this.coordinator = coordinator;
        }

        @Override
        public void abort(Ranges abort)
        {
            coordinator.abort(abort);
        }
    }
    static class SyncCoordinator extends FetchCoordinator
    {
        static class Key
        {
            final Node.Id id;
            final Ranges ranges;

            Key(Node.Id id, Ranges ranges)
            {
                this.id = id;
                this.ranges = ranges;
            }

            @Override
            public int hashCode()
            {
                return id.hashCode() + ranges.hashCode();
            }

            @Override
            public boolean equals(Object obj)
            {
                if (this == obj) return true;
                if (!(obj instanceof Key)) return false;
                Key that = (Key) obj;
                return id.equals(that.id) && ranges.equals(that.ranges);
            }
        }

        final FetchRanges fetchRanges;
        final CommandStore commandStore;
        final ListStore dataStore;
        final Map<Key, StartingRangeFetch> inflight = new HashMap<>();
        final SyncResult done = new SyncResult(this);
        final List<AsyncResult<Void>> persisting = new ArrayList<>();

        private SyncCoordinator(Node node, Ranges ranges, SyncPoint syncPoint, FetchRanges fetchRanges, CommandStore commandStore, ListStore dataStore)
        {
            super(node, ranges, syncPoint, fetchRanges);
            this.fetchRanges = fetchRanges;
            this.commandStore = commandStore;
            this.dataStore = dataStore;
        }

        @Override
        public void contact(Node.Id to, Ranges ranges)
        {
            Key key = new Key(to, ranges);
            inflight.put(key, starting(to, ranges));
            Ranges ownedRanges = ownedRangesForNode(to);
            Invariants.checkArgument(ownedRanges.containsAll(ranges));
            PartialDeps partialDeps = syncPoint.waitFor.slice(ownedRanges, ranges);
            PartialTxn partialTxn = new PartialTxn.InMemory(ranges, Txn.Kind.Read, ranges, new ListRead(unsafeStore -> unsafeStore, ranges, ranges), new ListQuery(Node.Id.NONE, Long.MIN_VALUE), null);
            node.send(to, new StoreSync(syncPoint.sourceEpoch(), syncPoint.syncId, ranges, partialDeps, partialTxn), new Callback<ReadReply>()
            {
                @Override
                public void onSuccess(Node.Id from, ReadReply reply)
                {
                    if (!reply.isOk())
                    {
                        fail(to, new RuntimeException(reply.toString()));
                        inflight.remove(key).cancel();
                        switch ((ReadNack) reply)
                        {
                            default: throw new AssertionError("Unhandled enum");
                            case Invalid:
                            case Redundant:
                            case NotCommitted:
                                throw new AssertionError();
                            case Error:
                                // TODO (required): ensure errors are propagated to coordinators and can be logged
                        }
                        return;
                    }

                    SyncReply ok = (SyncReply) reply;
                    Ranges received;
                    if (ok.unavailable != null)
                    {
                        unavailable(to, ok.unavailable);
                        if (ok.data == null)
                        {
                            inflight.remove(key).cancel();
                            return;
                        }
                        received = ranges.difference(ok.unavailable);
                    }
                    else
                    {
                        received = ranges;
                    }

                    // TODO (now): make sure it works if invoked in either order
                    inflight.remove(key).started(ok.maxApplied);
                    ListData data = (ListData) ok.data;
                    if (data != null)
                    {
                        persisting.add(commandStore.execute(PreLoadContext.empty(), safeStore -> {
                            data.forEach((key, value) -> dataStore.data.merge(key, value, Timestamped::merge));
                        }).addCallback((ignore, fail) -> {
                            synchronized (this)
                            {
                                if (fail == null) success(to, received);
                                else fail(to, received, fail);
                            }
                        }).beginAsResult());
                    }
                    // received must be invoked after submitting the persistence future, as it triggers onDone
                    // which creates a ReducingFuture over {@code persisting}
                }

                @Override
                public void onFailure(Node.Id from, Throwable failure)
                {
                    inflight.remove(key).cancel();
                    fail(from, failure);
                }

                @Override
                public void onCallbackFailure(Node.Id from, Throwable failure)
                {
                    // TODO (soon)
                    failure.printStackTrace();
                }
            });
        }

        @Override
        protected void onDone(Ranges success, Throwable failure)
        {
            if (success.isEmpty()) done.setFailure(failure);
            else if (persisting.isEmpty()) done.setSuccess(null);
            else AsyncChains.reduce(persisting, (a, b)-> null)
                               .begin((s, f) -> {
                                   if (f == null) done.setSuccess(ranges);
                                   else done.setFailure(f);
                               });
        }

        @Override
        protected void start()
        {
            super.start();
        }

        void abort(Ranges abort)
        {
            // TODO (required, later): implement abort
        }
    }

    static class StoreSync extends WaitAndReadData
    {
        final PartialDeps partialDeps;
        Timestamp maxApplied;

        StoreSync(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn)
        {

            super(ranges, sourceEpoch, Status.Applied, partialDeps, Timestamp.MAX, syncId, partialTxn);
            this.partialDeps = partialDeps;
        }

        @Override
        protected void readComplete(CommandStore commandStore, Data result, Ranges unavailable)
        {
            commandStore.execute(PreLoadContext.empty(), safeStore -> {
                Ranges slice = safeStore.ranges().allAt(executeReadAt).difference(unavailable);
                Timestamp newMaxApplied = ((InMemoryCommandStore.InMemorySafeStore)safeStore).maxApplied(readScope, slice);
                synchronized (this)
                {
                    if (maxApplied == null) maxApplied = newMaxApplied;
                    else maxApplied = Timestamp.max(maxApplied, newMaxApplied);
                    Ranges reportUnavailable = unavailable.slice((Ranges)this.readScope, Minimal);
                    super.readComplete(commandStore, result, reportUnavailable);
                }
            }).begin(node.agent());
        }

        @Override
        protected void reply(@Nullable Ranges unavailable, @Nullable Data data)
        {
            node.reply(replyTo, replyContext, new SyncReply(unavailable, data, maxApplied));
        }
    }

    static class SyncReply extends ReadOk
    {
        final Timestamp maxApplied;
        public SyncReply(@Nullable Ranges unavailable, @Nullable Data data, Timestamp maxApplied)
        {
            super(unavailable, data);
            this.maxApplied = maxApplied;
        }
    }

    static final Timestamped<int[]> EMPTY = new Timestamped<>(Timestamp.NONE, new int[0]);
    final NavigableMap<RoutableKey, Timestamped<int[]>> data = new TreeMap<>();

    // adding here to help trace burn test queries
    public final Node.Id node;

    public ListStore(Node.Id node)
    {
        this.node = node;
    }

    public Timestamped<int[]> get(Key key)
    {
        Timestamped<int[]> v = data.get(key);
        return v == null ? EMPTY : v;
    }

    public List<Map.Entry<Key, Timestamped<int[]>>> get(Range range)
    {
        return data.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                .entrySet().stream().map(e -> (Map.Entry<Key, Timestamped<int[]>>)(Map.Entry)e)
                .collect(Collectors.toList());
    }

    public synchronized void write(Key key, Timestamp executeAt, int[] value)
    {
        data.merge(key, new Timestamped<>(executeAt, value), Timestamped::merge);
    }

    @Override
    public FetchResult fetch(Node node, SafeCommandStore safeStore, Ranges ranges, SyncPoint syncPoint, FetchRanges callback)
    {
        SyncCoordinator coordinator = new SyncCoordinator(node, ranges, syncPoint, callback, safeStore.commandStore(), this);
        coordinator.start();
        return coordinator.done;
    }
}
