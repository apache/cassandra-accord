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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Tracing;
import accord.coordinate.MaybeRecover;
import accord.coordinate.Outcome;
import accord.local.Command;
import accord.local.CommandStores;
import accord.local.CommandStores.IncludingSpecificStoreSelector;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.ProgressToken;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.coordinate.Coordination.CoordinationKind.HomeProgress;
import static accord.impl.progresslog.CallbackInvoker.invokeHomeCallback;
import static accord.impl.progresslog.CoordinatePhase.Done;
import static accord.impl.progresslog.Progress.NoneExpected;
import static accord.impl.progresslog.Progress.Querying;
import static accord.impl.progresslog.Progress.Queued;
import static accord.impl.progresslog.TxnStateKind.Home;

/**
 * TODO (documentation): describe state machine
 *
 * TODO (expected): do not attempt recovery every run; simply check the coordinator is still active
 * TODO (expected): do not attempt execution until all shards are ready; use the WaitingState to achieve this
 */
abstract class HomeState extends WaitingState
{
    private static final int PROGRESS_SHIFT = WaitingState.WAITING_STATE_END_SHIFT;
    private static final long PROGRESS_MASK = 0x3;
    private static final int STATUS_SHIFT = PROGRESS_SHIFT + 2;
    private static final long STATUS_MASK = 0x7;
    private static final int RUN_COUNTER_SHIFT = STATUS_SHIFT + 3;
    private static final long RUN_COUNTER_MASK = 0x7;
    private static final long SET_MASK = ~((PROGRESS_MASK << PROGRESS_SHIFT)
                                           | (STATUS_MASK << STATUS_SHIFT));
    static final int HOME_STATE_END_SHIFT = RUN_COUNTER_SHIFT + 3;

    static
    {
        Invariants.require(HOME_STATE_END_SHIFT <= BaseTxnState.BASE_STATE_START_SHIFT);
    }

    HomeState(TxnId txnId)
    {
        super(txnId);
    }

    void set(SafeCommandStore safeStore, DefaultProgressLog instance, CoordinatePhase newCoordinatePhase, Progress newProgress)
    {
        encodedState &= SET_MASK;
        encodedState |= ((long)newCoordinatePhase.ordinal() << STATUS_SHIFT)
                        | ((long)newProgress.ordinal() << PROGRESS_SHIFT);

        if (newProgress == NoneExpected)
            instance.clearProgressToken(txnId);
        updateScheduling(safeStore, instance, Home, null, newProgress);
    }

    @Nonnull CoordinatePhase phase()
    {
        return phase(encodedState);
    }

    final @Nonnull Progress homeProgress()
    {
        return homeProgress(encodedState);
    }

    private static @Nonnull CoordinatePhase phase(long encodedState)
    {
        return CoordinatePhase.forOrdinal((int) ((encodedState >>> STATUS_SHIFT) & STATUS_MASK));
    }

    private static @Nonnull Progress homeProgress(long encodedState)
    {
        return Progress.forOrdinal((int) ((encodedState >>> PROGRESS_SHIFT) & PROGRESS_MASK));
    }

    final int homeRunCounter()
    {
        return (int) ((encodedState >>> RUN_COUNTER_SHIFT) & RUN_COUNTER_MASK);
    }

    final void incrementHomeRunCounter()
    {
        long shiftedMask = RUN_COUNTER_MASK << RUN_COUNTER_SHIFT;
        long current = encodedState & shiftedMask;
        long updated = Math.min(shiftedMask, current + (1L << RUN_COUNTER_SHIFT));
        encodedState &= ~shiftedMask;
        encodedState |= updated;
    }

    final void clearHomeRunCounter()
    {
        long shiftedMask = RUN_COUNTER_MASK << RUN_COUNTER_SHIFT;
        encodedState &= ~shiftedMask;
    }

    void atLeast(SafeCommandStore safeStore, DefaultProgressLog instance, CoordinatePhase newPhase, Progress newProgress)
    {
        if (phase() == Done)
            return;

        if (newPhase.compareTo(phase()) > 0)
        {
            instance.clearPendingAndActive(Home, txnId);
            clearHomeRunCounter();
            set(safeStore, instance, newPhase, newProgress);
        }
    }

    final void runHome(DefaultProgressLog instance, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        incrementHomeRunCounter();
        Invariants.require(!isHomeDoneOrUninitialised());
        Command command = safeCommand.current();
        Tracing tracing = instance.node.agent().trace(txnId, command.participants().max(), HomeProgress);
        // note: we may truncate locally based on shard-specific criteria, but this doesn't mean we're globally persisted

        if (command.saveStatus() == SaveStatus.Erased // TODO (expected): improve progressLog.clear() so we can expect these to be cleared from the progress log
            || !Invariants.expect(!command.durability().isDurableOrInvalidated(), "Command is durable or invalidated, but we have not cleared the ProgressLog"))
        {
            setHomeDone(instance);
            return;
        }

        // TODO (expected): when invalidated, safer to maintain HomeState until known to be globally invalidated
        // TODO (expected): validate that we clear HomeState when we receive a Durable reply, to replace the token check logic
        if (Route.isFullRoute(command.route()))
        {
            HasOutcome min = safeStore.durableBefore().min(txnId, command.route());
            if (min.isDurable())
            {
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "DurableBefore records %s; terminating home state", min);
                safeCommand.incidentalUpdate(command.updateDurability(command.durability().mergeMax(Durability.UniversalOrInvalidated)));
                setHomeDone(instance);
                return;
            }
        }

        ProgressToken maxProgressToken = instance.savedProgressToken(txnId).merge(command);
        CallbackInvoker<ProgressToken, Outcome> invoker = invokeHomeCallback(instance, txnId, maxProgressToken, HomeState::recoverCallback);
        CommandStores.StoreSelector reportTo = new IncludingSpecificStoreSelector(safeStore.commandStore().id());

        if (tracing != null)
            tracing.trace(safeStore.commandStore(), "Invoking MaybeRecover with progress token %s", maxProgressToken);

        instance.start(invoker, MaybeRecover.maybeRecover(instance.node(), txnId, invalidIf(), command.route(), maxProgressToken, reportTo, invoker));
        set(safeStore, instance, phase(), Querying);
    }

    static void recoverCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog instance, TxnId txnId, @Nullable ProgressToken prevProgressToken, Outcome success, Throwable fail)
    {
        HomeState state = instance.get(txnId);
        if (state == null)
            return;

        Command command = safeCommand.current();
        Tracing tracing = instance.node.agent().trace(safeCommand.txnId(), command.participants().max(), HomeProgress);
        CoordinatePhase status = state.phase();
        if (status.isAtMostReadyToExecute() && state.homeProgress() == Querying)
        {
            if (fail != null)
            {
                if (tracing != null)
                {
                    tracing.trace(safeStore.commandStore(), "Failed to recover: %s", fail);
                    tracing.trace(safeStore.commandStore(), "Waiting to retry (%d) with progress token %s", state.homeRunCounter(), prevProgressToken);
                }

                safeStore.agent().onCaughtException(fail, "Failed recovering " + state);

                // re-save prior progress token
                if (prevProgressToken != null && prevProgressToken.compareTo(command) > 0)
                    instance.saveProgressToken(command.txnId(), prevProgressToken);
                state.set(safeStore, instance, status, Queued);
            }
            else
            {
                ProgressToken token = success.asProgressToken().merge(command);
                if (prevProgressToken != null)
                    token = token.merge(prevProgressToken);

                if (token.outcome.isDurableOrInvalidated())
                {
                    if (tracing != null)
                        tracing.trace(safeStore.commandStore(), "Callback: progress token %s reports durable; marking home state done.", token);
                    state.setHomeDoneAndMaybeRemove(instance);
                    if (token.outcome.compareTo(command.durability().allShardsOrInvalidated()) >= 0)
                    {
                        Durability newDurability = null;
                        switch (token.outcome)
                        {
                            case Quorum: newDurability = Durability.AllQuorums; break;
                            case Universal: newDurability = Durability.Universal; break;
                            case UniversalOrInvalidated: newDurability = Durability.UniversalOrInvalidated; break;
                        }
                        if (newDurability != null)
                            safeCommand.incidentalUpdate(command.updateDurability(command.durability().mergeMax(newDurability)));
                    }
                }
                else
                {
                    if (tracing != null)
                        tracing.trace(safeStore.commandStore(), "Callback: progress token %s reports not durable; saving token and scheduling retry (%d).", token, state.homeRunCounter());
                    if (prevProgressToken != null && token.compareTo(command) > 0)
                        instance.saveProgressToken(command.txnId(), token);
                    state.set(safeStore, instance, status, Queued);
                }
            }
        }
        else if (tracing != null)
        {
            if (status == Done)
                tracing.trace(safeStore.commandStore(), "Callback: received, but already done");
            else
                tracing.trace(safeStore.commandStore(), "Callback: received, but not querying");
        }
    }

    void setHomeDone(DefaultProgressLog instance)
    {
        set(null, instance, Done, NoneExpected);
        clearHomeRunCounter();
        instance.clearPendingAndActive(Home, txnId);
    }

    void setHomeDoneAndMaybeRemove(DefaultProgressLog instance)
    {
        setHomeDone(instance);
        maybeRemove(instance);
    }

    @Override
    public String toStateString()
    {
        return (isHomeUninitialised() ? "" : isHomeDone() ? "Done; " : "{" + phase() + ',' + homeProgress() + "}; ") + super.toStateString();
    }

    boolean isHomeDone()
    {
        return phase() == Done;
    }

    boolean isHomeDoneOrUninitialised()
    {
        CoordinatePhase phase = phase();
        return phase == Done || phase == CoordinatePhase.NotInitialised;
    }

    boolean isHomeInitialised()
    {
        return phase() != CoordinatePhase.NotInitialised;
    }

    private boolean isHomeUninitialised()
    {
        return phase() == CoordinatePhase.NotInitialised;
    }
}
