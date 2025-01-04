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

import accord.local.Command;
import accord.local.SafeCommandStore;
import accord.messages.ReadData;
import accord.utils.async.AsyncChain;
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
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static accord.messages.ReadEphemeralTxnData.retryInLaterEpoch;
import static accord.primitives.SaveStatus.Applied;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.messages.ReadData.CommitOrReadNack.Insufficient;
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
    protected abstract FetchRequest newFetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn);

    @Override
    public void contact(Node.Id to, Ranges ranges)
    {
        Key key = new Key(to, ranges);
        inflight.put(key, starting(to, ranges));
        Ranges ownedRanges = ownedRangesForNode(to);
        Invariants.checkArgument(ownedRanges.containsAll(ranges), "Got a reply from %s for ranges %s, but owned ranges %s does not contain all the ranges", to, ranges, ownedRanges);
        PartialDeps partialDeps = syncPoint.waitFor.intersecting(ranges);
        node.send(to, newFetchRequest(syncPoint.syncId.epoch(), syncPoint.syncId, ranges, partialDeps, rangeReadTxn(ranges)), new Callback<ReadReply>()
        {
            @Override
            public void onSuccess(Node.Id from, ReadReply reply)
            {
                if (!reply.isOk())
                {
                    if (reply == Insufficient)
                    {
                        CoordinateSyncPoint.sendApply(node, from, syncPoint);
                    }
                    else
                    {
                        fail(to, new RuntimeException(reply.toString()));
                        inflight.remove(key).cancel();
                        switch ((CommitOrReadNack) reply)
                        {
                            default: throw new AssertionError("Unhandled enum: " + reply);
                            case Redundant:
                                // TODO (expected): stop fetch sync points from garbage collecting too quickly
                                // too late, sync point has been erased
                                break;
                            case Invalid:
                            case Rejected:
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
                    received = ranges.without(ok.unavailable);
                }
                else
                {
                    received = ranges;
                }

                // TODO (now): make sure it works if invoked in either order
                inflight.remove(key).started(ok.safeToReadAfter);
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
        // TODO (expected): implement abort
    }

    public static abstract class FetchRequest extends ReadData
    {
        // Note for future: we cannot safely execute on an Erased sync point without more work.
        // Specifically, if the range has partially lost ownership on the recipient, the SyncPoint
        // will not represent a safe point to snapshot from, and we won't have enough information to
        // report the range as unavailable.
        private static final ExecuteOn EXECUTE_ON = new ExecuteOn(Applied, TruncatedApply);
        public final PartialTxn read;
        public final PartialDeps partialDeps;
        private transient Timestamp safeToReadAfter;

        public FetchRequest(long sourceEpoch, TxnId syncId, Ranges ranges, PartialDeps partialDeps, PartialTxn partialTxn)
        {
            super(syncId, ranges, sourceEpoch);
            this.read = partialTxn;
            this.partialDeps = partialDeps;
        }

        @Override
        protected ExecuteOn executeOn()
        {
            return EXECUTE_ON;
        }

        @Override
        public ReadType kind()
        {
            return ReadType.waitUntilApplied;
        }

        @Override
        protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Ranges unavailable)
        {
            return read.read(safeStore, executeAt, unavailable);
        }

        // must be invoked by implementations some time after the read has started OR must override safeToReadAt()
        protected void readStarted(SafeCommandStore safeStore, Ranges unavailable)
        {
            safeToReadAfter = Timestamp.nonNullOrMax(Timestamp.NONE, Timestamp.nonNullOrMax(safeToReadAfter, safeStore.commandStore().unsafeGetMaxConflicts().foldl(Timestamp::nonNullOrMax)));
        }

        protected Timestamp safeToReadAfter()
        {
            return safeToReadAfter;
        }

        @Override
        protected void readComplete(CommandStore commandStore, Data result, Ranges unavailable)
        {
            Ranges reportUnavailable = unavailable == null ? null : unavailable.slice((Ranges)this.readScope, Minimal);
            super.readComplete(commandStore, result, reportUnavailable);
        }

        @Override
        protected ReadOk constructReadOk(Ranges unavailable, Data data)
        {
            Timestamp safeToReadAfter = safeToReadAfter();
            Invariants.checkState(data == null || safeToReadAfter != null);
            return new FetchResponse(unavailable, data, safeToReadAfter);
        }

        @Override
        protected void read(SafeCommandStore safeStore, Command command)
        {
            long retryInLaterEpoch = retryInLaterEpoch(executeAtEpoch, safeStore, command);
            if (retryInLaterEpoch > 0)
            {
                Ranges unavailable = ((Ranges)readScope).slice(safeStore.ranges().allAt(executeAtEpoch), Minimal);
                readComplete(safeStore.commandStore(), null, unavailable);
            }
            else
            {
                super.read(safeStore, command);
            }
        }

        @Override
        public MessageType type()
        {
            return MessageType.FETCH_DATA_REQ;
        }
    }

    public static class FetchResponse extends ReadOk
    {
        // only null if retryInFutureEpoch is set
        public final @Nullable Timestamp safeToReadAfter;

        public FetchResponse(@Nullable Ranges unavailable, @Nullable Data data, @Nonnull Timestamp safeToReadAfter)
        {
            super(unavailable, data);
            this.safeToReadAfter = safeToReadAfter;
        }

        @Override
        public MessageType type()
        {
            return MessageType.FETCH_DATA_RSP;
        }
    }
}
