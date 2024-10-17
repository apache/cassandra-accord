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

import accord.coordinate.MaybeRecover;
import accord.coordinate.Outcome;
import accord.local.Command;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Status;
import accord.primitives.ProgressToken;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.api.ProgressLog.BlockedUntil.CanCoordinateExecution;
import static accord.impl.progresslog.CallbackInvoker.invokeHomeCallback;
import static accord.impl.progresslog.CoordinatePhase.Done;
import static accord.impl.progresslog.CoordinatePhase.ReadyToExecute;
import static accord.impl.progresslog.Progress.NoneExpected;
import static accord.impl.progresslog.Progress.Querying;
import static accord.impl.progresslog.Progress.Queued;
import static accord.impl.progresslog.TxnStateKind.Home;

/**
 * TODO (required): describe state machine
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
    private static final int RETRY_COUNTER_SHIFT = PROGRESS_SHIFT + 3;
    private static final long RETRY_COUNTER_MASK = 0x7;
    private static final long SET_MASK = ~((PROGRESS_MASK << PROGRESS_SHIFT)
                                           | (STATUS_MASK << STATUS_SHIFT));
    final int HOME_STATE_END_SHIFT = RETRY_COUNTER_SHIFT + 3;

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
        updateScheduling(safeStore, instance, Home, CanCoordinateExecution, newProgress);
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

    final int homeRetryCounter()
    {
        return (int) ((encodedState >>> RETRY_COUNTER_SHIFT) & RETRY_COUNTER_MASK);
    }

    final void incrementHomeRetryCounter()
    {
        long shiftedMask = RETRY_COUNTER_MASK << RETRY_COUNTER_SHIFT;
        long current = encodedState & shiftedMask;
        long updated = Math.min(shiftedMask, current + (1L << RETRY_COUNTER_SHIFT));
        encodedState &= ~shiftedMask;
        encodedState |= updated;
    }

    final void clearHomeRetryCounter()
    {
        long shiftedMask = RETRY_COUNTER_MASK << RETRY_COUNTER_SHIFT;
        encodedState &= ~shiftedMask;
    }

    void atLeast(SafeCommandStore safeStore, DefaultProgressLog instance, CoordinatePhase newPhase, Progress newProgress)
    {
        if (phase() == Done)
            return;

        if (newPhase.compareTo(phase()) > 0)
        {
            instance.clearActive(Home, txnId);
            clearHomeRetryCounter();
            set(safeStore, instance, newPhase, newProgress);
        }
    }

    final void runHome(DefaultProgressLog instance, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Invariants.checkState(!isHomeDoneOrUninitialised());
        Command command = safeCommand.current();
        if (command.hasBeen(Status.Truncated))
        {
            // TODO (required): validate this better
            setHomeDone(instance);
            return;
        }
        Invariants.checkState(!command.hasBeen(Status.Truncated), "Command %s is truncated", command);

        Invariants.checkState(command.durability() != null);
        // TODO (expected): when invalidated, safer to maintain HomeState until known to be globally invalidated
        // TODO (now): validate that we clear HomeState when we receive a Durable reply, to replace the token check logic
        Invariants.checkState(!command.durability().isDurableOrInvalidated(), "Command is durable or invalidated, but we have not cleared the ProgressLog");

        ProgressToken maxProgressToken = instance.savedProgressToken(txnId).merge(command);

        CallbackInvoker<ProgressToken, Outcome> invoker = invokeHomeCallback(instance, txnId, maxProgressToken, HomeState::recoverCallback);
        instance.debugActive(MaybeRecover.maybeRecover(instance.node(), txnId, command.route(), maxProgressToken, invoker), invoker);
        set(safeStore, instance, ReadyToExecute, Querying);
    }

    static void recoverCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog instance, TxnId txnId, @Nullable ProgressToken prevProgressToken, Outcome success, Throwable fail)
    {
        HomeState state = instance.get(txnId);
        if (state == null)
            return;

        Command command = safeCommand.current();
        if (command.is(Status.Truncated))
        {
            state.setHomeDone(instance);
            return;
        }

        CoordinatePhase status = state.phase();
        if (status.isAtMostReadyToExecute() && state.homeProgress() == Querying)
        {
            if (fail != null)
            {
                state.incrementHomeRetryCounter();
                state.set(safeStore, instance, status, Queued);
            }
            else
            {
                ProgressToken token = success.asProgressToken().merge(command);
                if (prevProgressToken != null)
                    token = token.merge(prevProgressToken);

                if (token.durability.isDurableOrInvalidated())
                {
                    state.setHomeDoneAndMaybeRemove(instance);
                }
                else
                {
                    if (prevProgressToken != null && token.compareTo(command) > 0)
                        instance.saveProgressToken(command.txnId(), token);
                    state.set(safeStore, instance, status, Queued);
                }
            }
        }
    }

    void setHomeDone(DefaultProgressLog instance)
    {
        set(null, instance, Done, NoneExpected);
        clearHomeRetryCounter();
        instance.clearActive(Home, txnId);
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
