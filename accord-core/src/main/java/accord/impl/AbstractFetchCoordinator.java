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
import accord.local.MaxConflicts;
import accord.local.SafeCommandStore;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Callback.ConcreteCallbackExclusive;
import accord.messages.ReadData;
import accord.primitives.SyncPoint;
import accord.primitives.Participants;
import accord.topology.TopologyException;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;

import accord.api.Data;
import accord.api.DataStore;
import accord.coordinate.CoordinateSyncPoint;
import accord.coordinate.FetchCoordinator;
import accord.local.CommandStore;
import accord.local.Node;
import accord.messages.MessageType;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import javax.annotation.Nullable;

import static accord.messages.MessageType.StandardMessage.FETCH_DATA_REQ;
import static accord.messages.MessageType.StandardMessage.FETCH_DATA_RSP;
import static accord.messages.ReadData.CommitOrReadNack.Redundant;
import static accord.messages.ReadData.CommitOrReadNack.Waiting;
import static accord.messages.ReadEphemeralTxnData.retryInLaterEpoch;
import static accord.primitives.SaveStatus.Applied;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.messages.ReadData.CommitOrReadNack.InsufficientAndWaiting;
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
    protected final List<AsyncResult<Void>> persisting = new ArrayList<>();

    protected AbstractFetchCoordinator(Node node, SequentialAsyncExecutor executor, Ranges ranges, SyncPoint syncPoint, DataStore.FetchRanges fetchRanges, CommandStore commandStore) throws TopologyException
    {
        super(node, executor, ranges, syncPoint, fetchRanges);
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
        Invariants.requireArgument(ownedRanges.containsAll(ranges), "Got a reply from %s for ranges %s, but owned ranges %s does not contain all the ranges", to, ranges, ownedRanges);
        PartialDeps partialDeps = syncPoint.waitFor.intersecting(ranges);
        node.send(to, newFetchRequest(syncPoint.syncId.epoch(), syncPoint.syncId, ranges, partialDeps, rangeReadTxn(ranges)), executor, new ConcreteCallbackExclusive<ReadReply>(executor)
        {
            @Override
            public void onSuccessExclusive(Node.Id from, ReadReply reply)
            {
                if (!reply.isOk())
                {
                    if (reply == InsufficientAndWaiting)
                    {
                        CoordinateSyncPoint.sendApply(node, from, syncPoint, tracing);
                    }
                    else if (reply == Redundant)
                    {
                        fail(to, new RuntimeException(reply.toString()));
                        inflight.remove(key).cancel();
                        // too late, sync point has been erased
                        // TODO (desired): stop fetch sync points from garbage collecting too quickly
                    }
                    else if (reply != Waiting)
                    {
                        throw new UnhandledEnum(((CommitOrReadNack)reply).kind);
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

                // TODO (expected): make sure it works if invoked in either order
                inflight.remove(key).started(ok.safeToReadAfter);
                onReadOk(to, commandStore, ok.data, received);
                // received must be invoked after submitting the persistence future, as it triggers onDone
                // which creates a ReducingFuture over {@code persisting}
            }

            @Override
            public void onFailureExclusive(Node.Id from, Throwable failure)
            {
                inflight.remove(key).cancel();
                fail(from, failure);
            }

            @Override
            public void onCallbackFailureExclusive(Node.Id from, @Nullable Throwable failure)
            {
                node.agent().onException(failure);
            }
        }, tracing);
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
        else AsyncResults.reduce(persisting, (a, b) -> null)
                        .invoke((s, f) -> {
                            if (f == null) result.setSuccess(ranges);
                            else result.setFailure(f);
                        });
    }

    @Override
    public void start()
    {
        super.start();
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
            super(syncId, ranges, null, syncId, sourceEpoch);
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
        protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Participants<?> executes)
        {
            return read.read(safeStore, executeAt, executes);
        }

        // must be invoked by implementations some time after the read has started OR must override safeToReadAt()
        protected void readStarted(SafeCommandStore safeStore)
        {
            safeToReadAfter = Timestamp.nonNullOrMax(Timestamp.NONE, Timestamp.nonNullOrMax(safeToReadAfter, safeStore.commandStore().unsafeGetMaxConflicts().foldl(TxnId.NONE, (id, e, min) -> e.get(min, id), Timestamp.NONE)));
        }

        protected Timestamp safeToReadAfter()
        {
            return safeToReadAfter;
        }

        @Override
        protected void readComplete(CommandStore commandStore, Data result, Ranges unavailable)
        {
            Ranges reportUnavailable = unavailable == null ? null : unavailable.slice((Ranges)this.scope, Minimal);
            super.readComplete(commandStore, result, reportUnavailable);
        }

        @Override
        protected void reply(Ranges unavailable, Data data, long uniqueHlc)
        {
            Timestamp safeToReadAfter = safeToReadAfter();
            Invariants.require(data == null || safeToReadAfter != null);
            reply(new FetchResponse(unavailable, data, safeToReadAfter), null);
        }

        @Override
        protected void read(SafeCommandStore safeStore, Command command)
        {
            long retryInLaterEpoch = retryInLaterEpoch(executeAtEpoch, safeStore, command);
            if (retryInLaterEpoch > 0)
            {
                Ranges unavailable = ((Ranges) scope).slice(safeStore.ranges().allAt(executeAtEpoch), Minimal);
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
            return FETCH_DATA_REQ;
        }
    }

    public static class FetchResponse extends ReadOk
    {
        // only null if retryInFutureEpoch is set
        public final @Nullable Timestamp safeToReadAfter;

        public FetchResponse(@Nullable Ranges unavailable, @Nullable Data data, @Nullable Timestamp safeToReadAfter)
        {
            super(unavailable, data);
            this.safeToReadAfter = safeToReadAfter;
            Invariants.require(safeToReadAfter != null || (unavailable != null && data == null));
        }

        @Override
        public MessageType type()
        {
            return FETCH_DATA_RSP;
        }
    }
}
