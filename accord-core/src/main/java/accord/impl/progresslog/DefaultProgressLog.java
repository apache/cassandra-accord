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

package accord.impl.progresslog;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.LogGroupTimers;
import accord.utils.btree.BTree;
import accord.utils.btree.BTreeRemoval;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.impl.progresslog.CoordinatePhase.AwaitReadyToExecute;
import static accord.impl.progresslog.CoordinatePhase.ReadyToExecute;
import static accord.impl.progresslog.CoordinatePhase.Undecided;
import static accord.impl.progresslog.Progress.Awaiting;
import static accord.impl.progresslog.Progress.NoneExpected;
import static accord.impl.progresslog.Progress.Querying;
import static accord.impl.progresslog.Progress.Queued;
import static accord.impl.progresslog.TxnStateKind.Home;
import static accord.impl.progresslog.TxnStateKind.Waiting;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.PreApplied;
import static accord.utils.ArrayBuffers.cachedAny;
import static accord.utils.btree.UpdateFunction.noOpReplace;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DefaultProgressLog implements ProgressLog, Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultProgressLog.class);

    final Node node;
    final CommandStore commandStore;

    private Object[] stateMap = BTree.empty();
    private Object[] progressTokenMap = BTree.empty();

    private final LogGroupTimers<TxnState> timers = new LogGroupTimers<>(MICROSECONDS);
    private final LogGroupTimers<TxnState>.Scheduling timerScheduling;

    /**
     * A collection of active callbacks (waiting remote replies) or submitted run invocations
     * (perhaps waiting load from disk, or for the CommandStore thread to be available).
     *
     * These callbacks are required to have hashCode() == txnId.hashCode() and equals(txnId) == true,
     * so that we can manage overriding callbacks on the relevant TxnState.
     */
    private final ObjectHashSet<Object> activeWaiting = new ObjectHashSet<>();
    private final ObjectHashSet<Object> activeHome = new ObjectHashSet<>();

    private final Map<TxnId, StackTraceElement[]> deleted = Invariants.debug() ? new Object2ObjectHashMap<>() : null;

    private static final Object[] EMPTY_RUN_BUFFER = new Object[0];

    // The tasks whose timers have elapsed and are going to be run
    // The queue is drained here first before processing tasks so that tasks can modify the queue
    private Object[] runBuffer;
    private int runBufferCount;

    private long nextInvokerId;

    DefaultProgressLog(Node node, CommandStore commandStore)
    {
        this.node = node;
        this.commandStore = commandStore;
        Function<TxnState, Runnable> taskFactory = next -> {
            PreLoadContext context = next == null ? PreLoadContext.empty() : PreLoadContext.contextFor(next.txnId);
            return () -> commandStore.execute(context, safeStore -> run())
                                     .begin(commandStore.agent());
        };
        this.timerScheduling = timers.new Scheduling(node.scheduler(), taskFactory,
                                                     MILLISECONDS.toMicros(10), 100, SECONDS.toMicros(1L));
    }

    Node node()
    {
        return node;
    }

    void update(long deadline, TxnState timer)
    {
        update(node.elapsed(MICROSECONDS), deadline, timer);
    }

    void update(long nowMicros, long deadline, TxnState timer)
    {
        timers.update(deadline, timer);
        maybeReschedule(nowMicros, deadline);
    }

    void add(long nowMicros, long deadline, TxnState timer)
    {
        timers.add(deadline, timer);
        maybeReschedule(nowMicros, deadline);
    }

    @VisibleForTesting
    public @Nullable TxnState get(TxnId txnId)
    {
        Invariants.checkState(txnId.kind().isGloballyVisible());
        return BTree.<TxnId, TxnState>find(stateMap, (id, state) -> id.compareTo(state.txnId), txnId);
    }

    TxnState ensure(TxnId txnId)
    {
        Invariants.checkState(txnId.kind().isGloballyVisible());
        TxnState result = BTree.<TxnId, TxnState>find(stateMap, (id, state) -> id.compareTo(state.txnId), txnId);
        if (result == null)
        {
            Invariants.checkState(deleted == null || !deleted.containsKey(txnId));
            node.agent().metricsEventsListener().onProgressLogSizeChange(txnId, 1);
            stateMap = BTree.update(stateMap, BTree.singleton(result = new TxnState(txnId)), TxnState::compareTo);
        }
        return result;
    }

    private TxnState insert(TxnId txnId)
    {
        Invariants.checkState(deleted == null || !deleted.containsKey(txnId));
        node.agent().metricsEventsListener().onProgressLogSizeChange(txnId, 1);
        TxnState result = new TxnState(txnId);
        stateMap = BTree.update(stateMap, BTree.singleton(result), TxnState::compareTo);
        return result;
    }

    ProgressToken savedProgressToken(TxnId txnId)
    {
        ProgressToken saved = BTree.<TxnId, SavedProgressToken>find(progressTokenMap, (id, e) -> id.compareTo(e.txnId), txnId);
        if (saved == null)
            return ProgressToken.NONE;

        progressTokenMap = BTreeRemoval.<TxnId, SavedProgressToken>remove(progressTokenMap, (id, e) -> id.compareTo(e.txnId), txnId);
        return saved;
    }

    void saveProgressToken(TxnId txnId, ProgressToken token)
    {
        SavedProgressToken save = new SavedProgressToken(txnId, token);
        // we could save memory by setting ballot to ZERO when we have the same ballot in command (and can restore isAccepted)
        // but this isn't likely to offer dramatic savings very often
        progressTokenMap = BTree.update(progressTokenMap, BTree.singleton(save), SavedProgressToken::compare, noOpReplace());
    }

    void clearProgressToken(TxnId txnId)
    {
        progressTokenMap = BTreeRemoval.<TxnId, SavedProgressToken>remove(progressTokenMap, (id, e) -> id.compareTo(e.txnId), txnId);
    }

    @Override
    public void update(SafeCommandStore safeStore, TxnId txnId, Command before, Command after)
    {
        if (!txnId.kind().isGloballyVisible())
            return;

        TxnState state = null;
        if (before.route() == null && after.route() != null)
        {
            RoutingKey homeKey = after.homeKey();
            Ranges coordinateRanges = safeStore.ranges().allAt(txnId.epoch());
            boolean isHome = coordinateRanges.contains(homeKey);
            state = get(txnId);
            if (isHome)
            {
                if (state == null)
                    state = insert(txnId);

                if (after.durability().isDurableOrInvalidated())
                {
                    state.setHomeDoneAnyMaybeRemove(this);
                    if (!after.hasBeen(PreApplied))
                        state.waiting().setBlockedUntil(safeStore, this, CanApply);
                }
                else
                    state.set(safeStore, this, Undecided, Queued);
            }
            else if (state != null)
            {
                // not home shard
                state.setHomeDone(this);
            }
        }
        else if (after.durability().isDurableOrInvalidated() && !before.durability().isDurableOrInvalidated())
        {
            state = get(txnId);
            if (state != null)
                state.setHomeDoneAnyMaybeRemove(this);

            if (!after.hasBeen(PreApplied))
            {
                // this command should be ready to apply locally, so fetch it
                if (state == null)
                    state = insert(txnId);
                state.waiting().setBlockedUntil(safeStore, this, CanApply);
            }
        }

        SaveStatus beforeSaveStatus = before.saveStatus();
        SaveStatus afterSaveStatus = after.saveStatus();
        if (beforeSaveStatus == afterSaveStatus)
            return;

        if (state == null)
            state = get(txnId);

        if (state == null)
            return;

        state.waiting().record(this, afterSaveStatus);
        if (state.isHomeInitialised())
        {
            switch (afterSaveStatus)
            {
                case Stable:
                    state.home().atLeast(safeStore, this, Undecided, NoneExpected);
                    break;
                case ReadyToExecute:
                    state.home().atLeast(safeStore, this, AwaitReadyToExecute, Queued);
                    break;
                case PreApplied:
                    state.home().atLeast(safeStore, this, ReadyToExecute, Queued);
                    break;
            }
        }
    }

    @Override
    public void clear(TxnId txnId)
    {
        if (!txnId.kind().isGloballyVisible())
            return;

        TxnState state = get(txnId);
        if (state != null)
            clear(state);
    }

    private void clear(TxnState state)
    {
        state.setHomeDone(this);
        state.setWaitingDone(this);
        Invariants.checkState(!state.isScheduled());
        remove(state.txnId);
    }

    void remove(TxnId txnId)
    {
        Object[] newStateMap = BTreeRemoval.<TxnId, TxnState>remove(stateMap, (id, s) -> id.compareTo(s.txnId), txnId);
        if (stateMap != newStateMap)
            node.agent().metricsEventsListener().onProgressLogSizeChange(txnId, -1);
        stateMap = newStateMap;
        if (deleted != null)
            deleted.put(txnId, Thread.currentThread().getStackTrace());
    }

    @Override
    public void remoteCallback(SafeCommandStore safeStore, SafeCommand safeCommand, SaveStatus remoteStatus, int callbackId, Node.Id from)
    {
        TxnState state = get(safeCommand.txnId());
        if (state != null)
            state.asynchronousAwaitCallback(this, safeStore, remoteStatus, from, callbackId);
    }

    @Override
    public void waiting(BlockedUntil blockedUntil, SafeCommandStore safeStore, SafeCommand blockedBy, Route<?> blockedOnRoute, Participants<?> blockedOnParticipants)
    {
        if (!blockedBy.txnId().kind().isGloballyVisible())
            return;

        // ensure we have a record to work with later; otherwise may think has been truncated
        // TODO (expected): we shouldn't rely on this anymore
        blockedBy.initialise();
        Invariants.checkState(blockedBy.current().saveStatus().compareTo(blockedUntil.minSaveStatus) < 0);

        // first save the route/participant info into the Command if it isn't already there
        CommonAttributes update = blockedBy.current();
        Route<?> currentRoute = update.route();
        if (currentRoute != null)
        {
            Route<?> updatedRoute = currentRoute;
            if (blockedOnRoute != null) updatedRoute = currentRoute.with((Route) blockedOnRoute);
            else if (blockedOnParticipants != null)
                updatedRoute = currentRoute.with((Participants) blockedOnParticipants);
            if (updatedRoute != currentRoute)
                update = update.mutable().route(updatedRoute);
        }
        else if (blockedOnRoute != null) update = update.mutable().route(blockedOnRoute);
        else if (blockedOnParticipants != null) update = update.mutable().participants(blockedOnParticipants);
        if (update != blockedBy.current())
            blockedBy.updateAttributes(safeStore, update);

        // TODO (consider): consider triggering a preemption of existing coordinator (if any) in some circumstances;
        //                  today, an LWT can pre-empt more efficiently (i.e. instantly) a failed operation whereas Accord will
        //                  wait for some progress interval before taking over; there is probably some middle ground where we trigger
        //                  faster preemption once we're blocked on a transaction, while still offering some amount of time to complete.
        // TODO (desirable, efficiency): forward to local progress shard for processing (if known)
        // TODO (desirable, efficiency): if we are co-located with the home shard, don't need to do anything unless we're in a
        //                               later topology that wasn't covered by its coordination
        TxnState state = ensure(blockedBy.txnId());
        state.waiting().setBlockedUntil(safeStore, this, blockedUntil);
    }

    void ensureScheduled(long nowMicros)
    {
        timerScheduling.ensureScheduled(nowMicros);
    }

    void maybeReschedule(long nowMicros, long newDeadlineMicros)
    {
        timerScheduling.maybeReschedule(nowMicros, newDeadlineMicros);
    }

    @Override
    public void run()
    {
        long nowMicros = node.elapsed(TimeUnit.MICROSECONDS);
        try
        {
            if (DefaultProgressLogs.pauseForTest(this))
            {
                logger.info("Skipping progress log because it is paused for test");
                return;
            }

            // drain to a buffer to avoid reentrancy
            runBufferCount = 0;
            runBuffer = EMPTY_RUN_BUFFER;
            timers.advance(nowMicros, this, DefaultProgressLog::addToRunBuffer);
            processRunBuffer(nowMicros);
            cachedAny().forceDiscard(runBuffer, runBufferCount);
        }
        catch (Throwable t)
        {
            node.agent().onUncaughtException(t);
        }
        finally
        {
            if (!timers.isEmpty())
                ensureScheduled(nowMicros);
        }
    }

    private void addToRunBuffer(TxnState add)
    {
        if (runBufferCount == runBuffer.length)
            runBuffer = cachedAny().resize(runBuffer, runBufferCount, Math.max(8, runBuffer.length * 2));
        runBuffer[runBufferCount++] = add;
    }

    // TODO (expected): invoke immediately if the command is already loaded
    private void processRunBuffer(long nowMicros)
    {
        for (int i = 0; i < runBufferCount; ++i)
        {
            TxnState run = (TxnState) runBuffer[i];
            Invariants.checkState(!run.isScheduled());
            TxnStateKind runKind = run.wasScheduledTimer();
            validatePreRunState(run, runKind);

            long pendingTimerDeadline = run.pendingTimerDeadline(DefaultProgressLog.this);
            if (pendingTimerDeadline > 0)
            {
                run.clearPendingTimerDelay();
                if (pendingTimerDeadline <= nowMicros)
                {
                    invoke(run, runKind.other());
                }
                else
                {
                    run.setScheduledTimer(runKind.other());
                    timers.add(pendingTimerDeadline, run);
                }
            }

            invoke(run, runKind);
        }

        Arrays.fill(runBuffer, 0, runBufferCount, null);
        runBufferCount = 0;
    }

    private void validatePreRunState(TxnState run, TxnStateKind kind)
    {
        Progress progress = kind == Waiting ? run.waiting().waitingProgress() : run.home().homeProgress();
        Invariants.checkState(progress != NoneExpected && progress != Querying);
    }

    void invoke(TxnState run, TxnStateKind runKind)
    {
        RunInvoker invoker = new RunInvoker(nextInvokerId(), run, runKind);
        registerActive(runKind, run.txnId, invoker);
        node.withEpoch(run.txnId.epoch(), commandStore.agent(), () -> {
            commandStore.execute(contextFor(run.txnId), invoker)
                        .begin(commandStore.agent());
        });
    }

    class RunInvoker implements Consumer<SafeCommandStore>
    {
        final long id;
        final TxnState run;
        final TxnStateKind runKind;

        RunInvoker(long id, TxnState run, TxnStateKind runKind)
        {
            this.id = id;
            this.run = run;
            this.runKind = runKind;
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            // we have to read safeCommand first as it may become truncated on load, which may clear the progress log and invalidate us
            SafeCommand safeCommand = safeStore.ifInitialised(run.txnId);
            Invariants.checkState(safeCommand != null);

            if (!deregisterActive(runKind, this))
                return; // we've been cancelled

            Invariants.checkState(get(run.txnId) == run);
            Invariants.checkState(run.scheduledTimer() != runKind, "We are actively executing %s, but we are also scheduled to run this same TxnState later. This should not happen.", runKind);
            Invariants.checkState(run.pendingTimer() != runKind, "We are actively executing %s, but we also have a pending scheduled task to run this same TxnState later. This should not happen.", runKind);

            validatePreRunState(run, runKind);
            if (runKind == Home)
            {
                boolean isRetry = run.homeProgress() == Awaiting;
                if (isRetry) run.incrementHomeRetryCounter();
                run.home().runHome(DefaultProgressLog.this, safeStore, safeCommand);
            }
            else
            {
                boolean isRetry = run.waitingProgress() == Awaiting;
                if (isRetry) run.incrementWaitingRetryCounter();
                run.runWaiting(safeStore, safeCommand, DefaultProgressLog.this);
            }
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null) return false;
            if (obj.getClass() == TxnId.class) return run.txnId.equals(obj);
            if (obj.getClass() != getClass()) return false;
            RunInvoker that = (RunInvoker) obj;
            return id == that.id && run.txnId.equals(that.run.txnId) && runKind.equals(that.runKind);
        }

        @Override
        public int hashCode()
        {
            return run.txnId.hashCode();
        }
    }

    long nextInvokerId()
    {
        return nextInvokerId++;
    }

    ObjectHashSet<Object> active(TxnStateKind kind)
    {
        return kind == Waiting ? activeWaiting : activeHome;
    }

    void registerActive(TxnStateKind kind, TxnId txnId, Object object)
    {
        ObjectHashSet<Object> active = active(kind);
        Invariants.checkState(!active.contains(txnId));
        active.add(object);
    }

    boolean hasActive(TxnStateKind kind, TxnId txnId)
    {
        return active(kind).contains(txnId);
    }

    boolean deregisterActive(TxnStateKind kind, Object object)
    {
        return active(kind).remove(object);
    }

    void clearActive(TxnStateKind kind, TxnId txnId)
    {
        active(kind).remove(txnId);
    }

    void unschedule(TxnState state)
    {
        timers.remove(state);
    }
}
