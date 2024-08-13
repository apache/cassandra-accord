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

package accord.impl.basic;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import accord.api.Result;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.messages.Message;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;

import static accord.utils.Invariants.illegalState;


public class Journal implements Runnable
{
    private final Queue<RequestContext> unframedRequests = new ArrayDeque<>();
    private final LongArrayList waitForEpochs = new LongArrayList();
    private final Long2ObjectHashMap<ArrayList<RequestContext>> delayedRequests = new Long2ObjectHashMap<>();
    private final Map<Key, List<Diff>> diffs = new HashMap<>();
    private Node node;
    boolean isScheduled;

    public Journal()
    {
    }

    public void start(Node node)
    {
        this.node = node;
    }

    private void ensureScheduled()
    {
        if (isScheduled) return;
        node.scheduler().once(this, 1, TimeUnit.MILLISECONDS);
        isScheduled = true;
    }

    public void shutdown()
    {
        this.node = null;
    }

    public void handle(Request request, Node.Id from, ReplyContext replyContext)
    {
        ensureScheduled();
        if (request.type() != null && request.type().hasSideEffects())
        {
            // enqueue
            unframedRequests.add(new RequestContext(request, request.waitForEpoch(), () -> node.receive(request, from, replyContext)));
            return;
        }
        node.receive(request, from, replyContext);
    }

    @Override
    public void run()
    {
        isScheduled = false;
        if (this.node == null)
            return;
        try
        {
            doRun();
        }
        catch (Throwable t)
        {
            node.agent().onUncaughtException(t);
        }
    }

    private void doRun()
    {
        ArrayList<RequestContext> requests = null;
        // check to see if any pending epochs are in
        waitForEpochs.sort(null);
        for (int i = 0; i < waitForEpochs.size(); i++)
        {
            long waitForEpoch = waitForEpochs.getLong(i);
            if (!node.topology().hasEpoch(waitForEpoch))
                break;
            List<RequestContext> delayed = delayedRequests.remove(waitForEpoch);
            if (null == requests) requests = new ArrayList<>(delayed.size());
            requests.addAll(delayed);
        }
        waitForEpochs.removeIfLong(epoch -> !delayedRequests.containsKey(epoch));

        // for anything queued, put into the pending epochs or schedule
        RequestContext request;
        while (null != (request = unframedRequests.poll()))
        {
            long waitForEpoch = request.waitForEpoch;
            if (waitForEpoch != 0 && !node.topology().hasEpoch(waitForEpoch))
            {
                delayedRequests.computeIfAbsent(waitForEpoch, ignore -> new ArrayList<>()).add(request);
                if (!waitForEpochs.containsLong(waitForEpoch))
                    waitForEpochs.addLong(waitForEpoch);
            }
            else
            {
                if (null == requests) requests = new ArrayList<>();
                requests.add(request);
            }
        }

        // schedule
        if (requests != null)
        {
            requests.forEach(Runnable::run);
        }
    }

    public Command reconstruct(int commandStoreId, TxnId txnId, Result result)
    {
        Key key = new Key(txnId, commandStoreId);
        List<Diff> diffs = this.diffs.get(key);
        if (diffs == null || diffs.isEmpty())
            return null;

        Timestamp executeAt = null;
        Timestamp executesAtLeast = null;
        SaveStatus saveStatus = null;
        Status.Durability durability = null;

        Ballot acceptedOrCommitted = Ballot.ZERO;
        Ballot promised = Ballot.ZERO;

        Route<?> route = null;
        PartialTxn partialTxn = null;
        PartialDeps partialDeps = null;
        Seekables<?, ?> additionalKeysOrRanges = null;

        Command.WaitingOn waitingOn = null;
        Writes writes = null;

        for (Diff diff : diffs)
        {
            if (diff.txnId != null)
                txnId = diff.txnId;
            if (diff.executeAt != null)
                executeAt = diff.executeAt;
            if (diff.executesAtLeast != null)
                executesAtLeast = diff.executesAtLeast;
            if (diff.saveStatus != null)
                saveStatus = diff.saveStatus;
            if (diff.durability != null)
                durability = diff.durability;

            if (diff.acceptedOrCommitted != null)
                acceptedOrCommitted = diff.acceptedOrCommitted;
            if (diff.promised != null)
                promised = diff.promised;

            if (diff.route != null)
                route = diff.route;
            if (diff.partialTxn != null)
                partialTxn = diff.partialTxn;
            if (diff.partialDeps != null)
                partialDeps = diff.partialDeps;
            if (diff.additionalKeysOrRanges != null)
                additionalKeysOrRanges = diff.additionalKeysOrRanges;

            if (diff.waitingOn != null)
                waitingOn = diff.waitingOn;
            if (diff.writes != null)
                writes = diff.writes;
        }

        if (!txnId.kind().awaitsOnlyDeps())
            executesAtLeast = null;

        switch (saveStatus.known.outcome)
        {
            case Erased:
            case WasApply:
                writes = null;
                result = null;
                break;
        }

        CommonAttributes.Mutable attrs = new CommonAttributes.Mutable(txnId);
        if (partialTxn != null)
            attrs.partialTxn(partialTxn);
        if (durability != null)
            attrs.durability(durability);
        if (route != null)
            attrs.route(route);

        // TODO (desired): we can simplify this logic if, instead of diffing, we will infer the diff from the status
        if (partialDeps != null &&
            (saveStatus.known.deps != Status.KnownDeps.NoDeps &&
             saveStatus.known.deps != Status.KnownDeps.DepsErased &&
             saveStatus.known.deps != Status.KnownDeps.DepsUnknown))
            attrs.partialDeps(partialDeps);
        if (additionalKeysOrRanges != null)
            attrs.additionalKeysOrRanges(additionalKeysOrRanges);
        Invariants.checkState(saveStatus != null,
                              "Save status is null after applying %s", diffs);

        switch (saveStatus.status)
        {
            case NotDefined:
                return saveStatus == SaveStatus.Uninitialised ? Command.NotDefined.uninitialised(attrs.txnId())
                                                              : Command.NotDefined.notDefined(attrs, promised);
            case PreAccepted:
                return Command.PreAccepted.preAccepted(attrs, executeAt, promised);
            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                return Command.Accepted.accepted(attrs, saveStatus, executeAt, promised, acceptedOrCommitted);
            case Committed:
            case Stable:
                return Command.Committed.committed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn);
            case PreApplied:
            case Applied:
                return Command.Executed.executed(attrs, saveStatus, executeAt, promised, acceptedOrCommitted, waitingOn, writes, result);
            case Invalidated:
            case Truncated:
                return truncated(attrs, saveStatus, executeAt, executesAtLeast, writes, result);
            default:
                throw new IllegalStateException("Do not know " + saveStatus.status + " " + saveStatus);
        }
    }

    private static Command.Truncated truncated(CommonAttributes.Mutable attrs, SaveStatus status, Timestamp executeAt, Timestamp executesAtLeast, Writes writes, Result result)
    {
        switch (status)
        {
            default:
                throw illegalState("Unhandled SaveStatus: " + status);
            case TruncatedApplyWithOutcome:
            case TruncatedApplyWithDeps:
            case TruncatedApply:
                return Command.Truncated.truncatedApply(attrs, status, executeAt, writes, result, executesAtLeast);
            case ErasedOrInvalidOrVestigial:
                return Command.Truncated.erasedOrInvalidOrVestigial(attrs.txnId(), attrs.durability(), attrs.route());
            case Erased:
                return Command.Truncated.erased(attrs.txnId(), attrs.durability(), attrs.route());
            case Invalidated:
                return Command.Truncated.invalidated(attrs.txnId());
        }
    }

    public void onExecute(int commandStoreId, Command before, Command after, boolean isPrimary)
    {
        if (before == null && after == null)
            return;
        Diff diff = diff(before, after);
        if (!isPrimary)
            diff = diff.asNonprimary();

        if (diff != null)
        {
            Key key = new Key(after.txnId(), commandStoreId);
            diffs.computeIfAbsent(key, (k_) -> new ArrayList<>()).add(diff);
        }
    }

    private static class RequestContext implements Runnable
    {
        final long waitForEpoch;
        final Message message;
        final Runnable fn;

        protected RequestContext(Message request, long waitForEpoch, Runnable fn)
        {
            this.waitForEpoch = waitForEpoch;
            this.message = request;
            this.fn = fn;
        }

        @Override
        public void run()
        {
            fn.run();
        }
    }

    private static class Diff
    {
        public final TxnId txnId;

        public final Timestamp executeAt;
        public final Timestamp executesAtLeast;
        public final SaveStatus saveStatus;
        public final Status.Durability durability;

        public final Ballot acceptedOrCommitted;
        public final Ballot promised;

        public final Route<?> route;
        public final PartialTxn partialTxn;
        public final PartialDeps partialDeps;

        public final Writes writes;
        public final Command.WaitingOn waitingOn;
        public final Seekables<?, ?> additionalKeysOrRanges;

        public Diff(TxnId txnId,
                    Timestamp executeAt,
                    Timestamp executesAtLeast,
                    SaveStatus saveStatus,
                    Status.Durability durability,

                    Ballot acceptedOrCommitted,
                    Ballot promised,

                    Route<?> route,
                    PartialTxn partialTxn,
                    PartialDeps partialDeps,
                    Command.WaitingOn waitingOn,

                    Writes writes,
                    Seekables<?, ?> additionalKeysOrRanges)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.executesAtLeast = executesAtLeast;
            this.saveStatus = saveStatus;
            this.durability = durability;

            this.acceptedOrCommitted = acceptedOrCommitted;
            this.promised = promised;

            this.route = route;
            this.partialTxn = partialTxn;
            this.partialDeps = partialDeps;

            this.writes = writes;
            this.waitingOn = waitingOn;
            this.additionalKeysOrRanges = additionalKeysOrRanges;
        }

        // We allow only save status, waitingOn, and listeners to be updated by non-primary transactions
        public Diff asNonprimary()
        {
            return new Diff(null, null, null, saveStatus, null, null, null, null, null, null, waitingOn, null, null);
        }

        public boolean allNulls()
        {
            if (txnId != null) return false;
            if (executeAt != null) return false;
            if (executesAtLeast != null) return false;
            if (saveStatus != null) return false;
            if (durability != null) return false;
            if (acceptedOrCommitted != null) return false;
            if (promised != null) return false;
            if (route != null) return false;
            if (partialTxn != null) return false;
            if (partialDeps != null) return false;
            if (writes != null) return false;
            if (waitingOn != null) return false;
            if (additionalKeysOrRanges != null) return false;
            return true;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder("SavedDiff{");
            if (txnId != null)
                builder.append("txnId = ").append(txnId).append(" ");
            if (executeAt != null)
                builder.append("executeAt = ").append(executeAt).append(" ");
            if (executesAtLeast != null)
                builder.append("executesAtLeast = ").append(executesAtLeast).append(" ");
            if (saveStatus != null)
                builder.append("saveStatus = ").append(saveStatus).append(" ");
            if (durability != null)
                builder.append("durability = ").append(durability).append(" ");
            if (acceptedOrCommitted != null)
                builder.append("acceptedOrCommitted = ").append(acceptedOrCommitted).append(" ");
            if (promised != null)
                builder.append("promised = ").append(promised).append(" ");
            if (route != null)
                builder.append("route = ").append(route).append(" ");
            if (partialTxn != null)
                builder.append("partialTxn = ").append(partialTxn).append(" ");
            if (partialDeps != null)
                builder.append("partialDeps = ").append(partialDeps).append(" ");
            if (writes != null)
                builder.append("writes = ").append(writes).append(" ");
            if (waitingOn != null)
                builder.append("waitingOn = ").append(waitingOn).append(" ");
            if (additionalKeysOrRanges != null)
                builder.append("additionalKeysOrRanges = ").append(additionalKeysOrRanges).append(" ");
            builder.append("}");
            return builder.toString();
        }
    }

    static Diff diff(Command before, Command after)
    {
        if (Objects.equals(before, after))
            return null;

        Diff diff = new Diff(ifNotEqual(before, after, Command::txnId, false),
                             ifNotEqual(before, after, Command::executeAt, true),
                             ifNotEqual(before, after, Command::executesAtLeast, true),
                             ifNotEqual(before, after, Command::saveStatus, false),

                             ifNotEqual(before, after, Command::durability, false),
                             ifNotEqual(before, after, Command::acceptedOrCommitted, false),
                             ifNotEqual(before, after, Command::promised, false),

                             ifNotEqual(before, after, Command::route, true),
                             ifNotEqual(before, after, Command::partialTxn, false),
                             ifNotEqual(before, after, Command::partialDeps, false),
                             ifNotEqual(before, after, Journal::getWaitingOn, true),
                             ifNotEqual(before, after, Command::writes, false),
                             ifNotEqual(before, after, Command::additionalKeysOrRanges, false));
        if (diff.allNulls())
            return null;

        return diff;
    }

    static Command.WaitingOn getWaitingOn(Command command)
    {
        if (command instanceof Command.Committed)
            return command.asCommitted().waitingOn();

        return null;
    }

    private static <OBJ, VAL> VAL ifNotEqual(OBJ lo, OBJ ro, Function<OBJ, VAL> convert, boolean allowClassMismatch)
    {
        VAL l = null;
        VAL r = null;
        if (lo != null) l = convert.apply(lo);
        if (ro != null) r = convert.apply(ro);

        if (l == r)
            return null;
        if (l == null || r == null)
            return r;
        assert allowClassMismatch || l.getClass() == r.getClass() : String.format("%s != %s", l.getClass(), r.getClass());

        if (l.equals(r))
            return null;

        return r;
    }

    public static class Key
    {
        final TxnId timestamp;
        final int commandStoreId;

        public Key(TxnId timestamp, int commandStoreId)
        {
            this.timestamp = timestamp;
            this.commandStoreId = commandStoreId;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return commandStoreId == key.commandStoreId && Objects.equals(timestamp, key.timestamp);
        }

        public int hashCode()
        {
            return Objects.hash(timestamp, commandStoreId);
        }
    }
}