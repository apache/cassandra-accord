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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.DataStore;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.FetchCoordinator;
import accord.local.CommandStore;
import accord.local.Node;
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

import static accord.messages.ReadData.ReadNack.NotCommitted;
import static accord.primitives.Routables.Slice.Minimal;

public abstract class AbstractFetchCoordinator extends FetchCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractFetchCoordinator.class);

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
            return (31  + id.hashCode()) * 31 + ranges.hashCode();
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

    public CommandStore commandStore()
    {
        return commandStore;
    }

    protected abstract PartialTxn rangeReadTxn(Ranges ranges);

    protected abstract void onReadOk(Node.Id from, CommandStore commandStore, Data data, Ranges ranges);

    protected boolean collectMaxApplied()
    {
        return false;
    }

    @Override
    public void contact(Node.Id to, Ranges ranges)
    {
        Key key = new Key(to, ranges);
        inflight.put(key, starting(to, ranges));
        Ranges ownedRanges = ownedRangesForNode(to);
        Invariants.checkArgument(ownedRanges.containsAll(ranges), "Got a reply from %s for ranges %s, but owned ranges %s does not contain all the ranges", to, ranges, ownedRanges);
        PartialDeps partialDeps = syncPoint.waitFor.slice(ownedRanges, ranges);
        node.send(to, new FetchRequest(syncPoint.sourceEpoch(), syncPoint.syncId, ranges, partialDeps, rangeReadTxn(ranges), collectMaxApplied()), new Callback<ReadData.ReadReply>()
        {
            @Override
            public void onSuccess(Node.Id from, ReadData.ReadReply reply)
            {
                if (!reply.isOk())
                {
                    if (reply == NotCommitted)
                    {
                        CoordinateSyncPoint.sendApply(node, from, syncPoint);
                    }
                    else
                    {
                        fail(to, new RuntimeException(reply.toString()));
                        inflight.remove(key).cancel();
                        switch ((ReadData.ReadNack) reply)
                        {
                            default: throw new AssertionError("Unhandled enum: " + reply);
                            case Invalid:
                            case Redundant:
                                throw new AssertionError(String.format("Unexpected reply: %s", reply));
                        }
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
                    received = ranges.subtract(ok.unavailable);
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
                logger.error("Fetch coordination failure from " + from, failure);
            }
        });
    }

    public FetchResult result()
    {
        return result;
    }

    @Override
    protected void onDone(Ranges success, Throwable failure)
    {
        if (failure != null || success.isEmpty()) result.setFailure(failure);
        else if (persisting.isEmpty()) result.setSuccess(Ranges.EMPTY);
        else AsyncChains.reduce(persisting, (a, b) -> null)
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
        public final boolean collectMaxApplied;
        private transient Timestamp maxApplied;

        public FetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn, boolean collectMaxApplied)
        {
            super(syncId, ranges, syncId, sourceEpoch, partialTxn);
            this.partialDeps = partialDeps;
            this.collectMaxApplied = collectMaxApplied;
        }

        @Override
        protected void readComplete(CommandStore commandStore, Data result, Ranges unavailable)
        {
            Ranges slice = commandStore.unsafeRangesForEpoch().allAt(txnId).subtract(unavailable);
            if (collectMaxApplied)
            {
                commandStore.maxAppliedFor((Ranges)readScope, slice).begin((newMaxApplied, failure) -> {
                    if (failure != null)
                    {
                        commandStore.agent().onUncaughtException(failure);
                    }
                    else
                    {
                        synchronized (this)
                        {
                            if (maxApplied == null) maxApplied = newMaxApplied;
                            else maxApplied = Timestamp.max(maxApplied, newMaxApplied);
                            Ranges reportUnavailable = unavailable.slice((Ranges)this.readScope, Minimal);
                            super.readComplete(commandStore, result, reportUnavailable);
                        }
                    }
                });
            }
            else
            {
                Ranges reportUnavailable = unavailable.slice((Ranges)this.readScope, Minimal);
                super.readComplete(commandStore, result, reportUnavailable);
            }
        }

        @Override
        protected void reply(@Nullable Ranges unavailable, @Nullable Data data, @Nullable Throwable fail)
        {
            // TODO (review): If the fetch response actually does some streaming, but we send back the error
            // it is a lot of work and data that might move and be unaccounted for at the coordinator
            node.reply(replyTo, replyContext, data != null ? new FetchResponse(unavailable, data, maxApplied) : null, fail);
        }

        @Override
        public MessageType type()
        {
            return MessageType.FETCH_DATA_REQ;
        }
    }

    public static class FetchResponse extends ReadData.ReadOk
    {
        public final @Nullable Timestamp maxApplied;
        public FetchResponse(@Nullable Ranges unavailable, @Nullable Data data, @Nullable Timestamp maxApplied)
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
