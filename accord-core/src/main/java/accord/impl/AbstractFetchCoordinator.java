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

package accord.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import accord.api.Data;
import accord.api.DataStore;
import accord.coordinate.FetchCoordinator;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.Status;
import accord.messages.Callback;
import accord.messages.MessageType;
import accord.messages.ReadData;
import accord.messages.WaitAndReadData;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.primitives.Routables.Slice.Minimal;

public abstract class AbstractFetchCoordinator extends FetchCoordinator
{
    static class FetchResult extends AsyncResults.SettableResult<Ranges> implements DataStore.FetchResult
    {
        final AbstractFetchCoordinator coordinator;

        FetchResult(AbstractFetchCoordinator coordinator)
        {
            this.coordinator = coordinator;
        }

        @Override
        public void abort(Ranges abort)
        {
            coordinator.abort(abort);
        }
    }

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

    final DataStore.FetchRanges fetchRanges;
    final CommandStore commandStore;
    final Map<Key, DataStore.StartingRangeFetch> inflight = new HashMap<>();
    final FetchResult result = new FetchResult(this);
    final List<AsyncResult<Void>> persisting = new ArrayList<>();

    protected AbstractFetchCoordinator(Node node, Ranges ranges, SyncPoint syncPoint, DataStore.FetchRanges fetchRanges, CommandStore commandStore)
    {
        super(node, ranges, syncPoint, fetchRanges);
        this.fetchRanges = fetchRanges;
        this.commandStore = commandStore;
    }

    protected abstract PartialTxn rangeReadTxn(Ranges ranges);

    protected abstract void onReadOk(Node.Id from, CommandStore commandStore, Data data, Ranges ranges);

    @Override
    public void contact(Node.Id to, Ranges ranges)
    {
        Key key = new Key(to, ranges);
        inflight.put(key, starting(to, ranges));
        Ranges ownedRanges = ownedRangesForNode(to);
        Invariants.checkArgument(ownedRanges.containsAll(ranges));
        PartialDeps partialDeps = syncPoint.waitFor.slice(ownedRanges, ranges);
        node.send(to, new FetchRequest(syncPoint.sourceEpoch(), syncPoint.syncId, ranges, partialDeps, rangeReadTxn(ranges)), new Callback<ReadData.ReadReply>()
        {
            @Override
            public void onSuccess(Node.Id from, ReadData.ReadReply reply)
            {
                if (!reply.isOk())
                {
                    fail(to, new RuntimeException(reply.toString()));
                    inflight.remove(key).cancel();
                    switch ((ReadData.ReadNack) reply)
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

                FetchResponse ok = (FetchResponse) reply;
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
                onReadOk(to, commandStore, ok.data, received);
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
    protected synchronized void success(Node.Id to, Ranges ranges)
    {
        super.success(to, ranges);
    }

    @Override
    protected synchronized void fail(Node.Id to, Ranges ranges, Throwable failure)
    {
        super.fail(to, ranges, failure);
    }

    public FetchResult result()
    {
        return result;
    }

    @Override
    protected void onDone(Ranges success, Throwable failure)
    {
        if (success.isEmpty()) result.setFailure(failure);
        else if (persisting.isEmpty()) result.setSuccess(null);
        else AsyncChains.reduce(persisting, (a, b)-> null)
                        .begin((s, f) -> {
                            if (f == null) result.setSuccess(ranges);
                            else result.setFailure(f);
                        });
    }

    @Override
    public void start()
                      {
                         super.start();
                                       }

    void abort(Ranges abort)
    {
        // TODO (required, later): implement abort
    }

    public static class FetchRequest extends WaitAndReadData
    {
        public final PartialDeps partialDeps;
        private transient Timestamp maxApplied;

        public FetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn)
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
            node.reply(replyTo, replyContext, new FetchResponse(unavailable, data, maxApplied));
        }

        @Override
        public MessageType type()
        {
            return MessageType.FETCH_DATA_REQ;
        }
    }

    public static class FetchResponse extends ReadData.ReadOk
    {
        public final Timestamp maxApplied;
        public FetchResponse(@Nullable Ranges unavailable, @Nullable Data data, Timestamp maxApplied)
        {
            super(unavailable, data);
            this.maxApplied = maxApplied;
        }

        @Override
        public MessageType type()
        {
            return MessageType.FETCH_DATA_RSP;
        }
    }
}
