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
import accord.local.CommandStores.IncludingSpecificStoreSelector;
import accord.local.CommandStores.LatentStoreSelector;
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
import static accord.impl.progresslog.HomePhase.Cleared;
import static accord.impl.progresslog.HomePhase.Decided;
import static accord.impl.progresslog.HomePhase.Done;
import static accord.impl.progresslog.HomePhase.NotInitialised;
import static accord.impl.progresslog.HomePhase.ReadyToExecute;
import static accord.impl.progresslog.Progress.NoneExpected;
import static accord.impl.progresslog.Progress.Querying;
import static accord.impl.progresslog.Progress.Queued;
import static accord.impl.progresslog.TxnStateKind.Home;
import static accord.primitives.Status.Durability.HasDecision.None;

/**
 * TODO (documentation): describe state machine
 *
 * TODO (expected): do not attempt recovery every run; simply check the coordinator is still active
 * TODO (expected): do not attempt execution until all shards are ready; use the WaitingState to achieve this
 */
abstract class HomeState extends BaseTxnState
{
    private static final int PROGRESS_SHIFT = 0;
    private static final long PROGRESS_MASK = 0x3;
    private static final int STATUS_SHIFT = PROGRESS_SHIFT + 2;
    private static final long STATUS_MASK = 0x7;
    private static final int RUN_COUNTER_SHIFT = STATUS_SHIFT + 3;
    private static final long RUN_COUNTER_MASK = 0x7;
    private static final long SET_MASK = ~((PROGRESS_MASK << PROGRESS_SHIFT)
                                           | (STATUS_MASK << STATUS_SHIFT));
    static final int HOME_STATE_END_SHIFT = RUN_COUNTER_SHIFT + 3;
    static final long SNAPSHOT_HOME_MASK = ~SET_MASK;

    static
    {
        Invariants.require(HOME_STATE_END_SHIFT <= BaseTxnState.BASE_STATE_START_SHIFT);
    }

    HomeState(TxnId txnId)
    {
        super(txnId);
    }

    void set(SafeCommandStore safeStore, DefaultProgressLog owner, HomePhase newHomePhase, Progress newProgress)
    {
        setWithoutScheduling(newHomePhase, newProgress);
        if (newProgress == NoneExpected)
            owner.clearProgressToken(txnId);
        updateScheduling(safeStore, owner, Home, null, newProgress);
    }

    void setWithoutScheduling(HomePhase newHomePhase, Progress newProgress)
    {
        encodedState &= SET_MASK;
        encodedState |= ((long) newHomePhase.ordinal() << STATUS_SHIFT)
                        | ((long)newProgress.ordinal() << PROGRESS_SHIFT);
    }

    @Nonnull
    HomePhase homePhase()
    {
        return homePhase(encodedState);
    }

    final @Nonnull Progress homeProgress()
    {
        return homeProgress(encodedState);
    }

    private static @Nonnull HomePhase homePhase(long encodedState)
    {
        return HomePhase.forOrdinal((int) ((encodedState >>> STATUS_SHIFT) & STATUS_MASK));
    }

    private static @Nonnull Progress homeProgress(long encodedState)
    {
        return Progress.forId((int) ((encodedState >>> PROGRESS_SHIFT) & PROGRESS_MASK));
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

    void atLeast(SafeCommandStore safeStore, DefaultProgressLog owner, HomePhase newPhase, Progress newProgress)
    {
        if (homePhase().compareTo(Done) >= 0)
            return;

        if (newPhase.compareTo(homePhase()) > 0)
        {
            owner.clearPendingAndActive(Home, txnId);
            clearHomeRunCounter();
            set(safeStore, owner, newPhase, newProgress);
        }
    }

    void maybeUpdatePhase(SafeCommandStore safeStore, DefaultProgressLog owner, Command command)
    {
        HomePhase newPhase = shouldUpdatePhase(owner, command);
        if (newPhase != null)
            atLeast(safeStore, owner, newPhase, newPhase.expectsProgress ? Queued : NoneExpected);
    }

    HomePhase shouldUpdatePhase(DefaultProgressLog owner, Command command)
    {
        if (command.durability().isDurableOrInvalidated() || command.saveStatus() == SaveStatus.Erased)
            return Done;

        HomePhase phase = homePhase();
        if (phase.compareTo(ReadyToExecute) >= 0)
            return null;

        if (command.saveStatus().compareTo(SaveStatus.ReadyToExecute) >= 0)
            return ReadyToExecute;

        if (phase.compareTo(Decided) < 0)
        {
            // while we can infer that a command is durably decided by looking at Stable and accepted/commit ballots
            // this doesn't tell us whether the Stable record is itself durable at other replicas.
            // Since we currently gate further execution progress on the home shard's ability to execute, we want
            // all shards to independently be able to reach Stable before we stop performing home shard progress work
            // So, we rely exclusively on Durability information which records a coordinator's knowledge about a phase's durability
            if (command.durability().isDurablyStable() && (!owner.homeExpectsLocallyApplied() || command.saveStatus().compareTo(SaveStatus.Stable) >= 0))
                return Decided;
        }

        return null;
    }

    final void runHome(DefaultProgressLog owner, SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        incrementHomeRunCounter();
        Invariants.require(!isHomeDoneOrUninitialised());
        Command command = safeCommand.current();
        Tracing tracing = owner.node.agent().trace(txnId, command.participants().max(), HomeProgress);
        // note: we may truncate locally based on shard-specific criteria, but this doesn't mean we're globally persisted

        {
            HomePhase updatePhase = shouldUpdatePhase(owner, command);
            if (updatePhase != null)
            {
                if (!updatePhase.expectsProgress)
                {
                    set(safeStore, owner, updatePhase, NoneExpected);
                    return;
                }
                setWithoutScheduling(updatePhase, Queued);
            }
        }

        // TODO (expected): when invalidated, safer to maintain HomeState until known to be globally invalidated
        // TODO (expected): validate that we clear HomeState when we receive a Durable reply, to replace the token check logic
        if (Route.isFullRoute(command.route()))
        {
            HasOutcome min = safeStore.durableBefore().min(txnId, command.route());
            if (min.isDurable() && owner.isHomeDoneIfDurable(command))
            {
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "DurableBefore records %s; terminating home state", min);
                safeCommand.incidentalUpdate(command.updateDurability(command.durability().mergeMax(Durability.get(None, min, min, true))));
                setHomeDone(owner);
                return;
            }
        }

        ProgressToken maxProgressToken = owner.savedProgressToken(txnId).merge(command);
        CallbackInvoker<ProgressToken, Outcome> invoker = invokeHomeCallback(owner, txnId, maxProgressToken, HomeState::recoverCallback);
        LatentStoreSelector reportTo = new IncludingSpecificStoreSelector(safeStore.commandStore().id());

        if (tracing != null)
            tracing.trace(safeStore.commandStore(), "Invoking MaybeRecover with progress token %s", maxProgressToken);

        owner.start(invoker, MaybeRecover.maybeRecover(owner.node(), txnId, invalidIf(), command.route(), maxProgressToken, owner.homeExpectsLocallyApplied(), reportTo, invoker));
        set(safeStore, owner, homePhase(), Querying);
    }

    static void recoverCallback(SafeCommandStore safeStore, SafeCommand safeCommand, DefaultProgressLog owner, TxnId txnId, @Nullable ProgressToken prevProgressToken, Outcome success, Throwable fail)
    {
        HomeState state = owner.get(txnId);
        if (state == null)
            return;

        Command command = safeCommand.current();
        Tracing tracing = owner.node.agent().trace(safeCommand.txnId(), command.participants().max(), HomeProgress);
        HomePhase status = state.homePhase();
        if (status.isAtMostReadyToExecute() && state.homeProgress() == Querying)
        {
            if (fail != null)
            {
                if (tracing != null)
                {
                    tracing.trace(safeStore.commandStore(), "Failed to recover: %s", fail);
                    tracing.trace(safeStore.commandStore(), "Waiting to retry (%d) with progress token %s", state.homeRunCounter(), prevProgressToken);
                }

                safeStore.agent().onException(fail, "Failed recovering " + state);

                // re-save prior progress token
                if (prevProgressToken != null && prevProgressToken.compareTo(command) > 0)
                    owner.saveProgressToken(command.txnId(), prevProgressToken);
                state.set(safeStore, owner, status, Queued);
            }
            else
            {
                ProgressToken token = success.asProgressToken().merge(command);
                if (prevProgressToken != null)
                    token = token.merge(prevProgressToken);

                if (token.outcome.isDurableOrInvalidated() && owner.isHomeDoneIfDurable(command))
                {
                    if (tracing != null)
                        tracing.trace(safeStore.commandStore(), "Callback: progress token %s reports durable; marking home state done.", token);
                    state.setHomeDoneAndMaybeRemove(owner);
                    if (token.outcome.compareTo(command.durability().allShardsOrInvalidated()) >= 0)
                    {
                        Durability newDurability = null;
                        switch (token.outcome)
                        {
                            case Quorum: newDurability = Durability.AllQuorums; break;
                            case QuorumOrInvalidated: newDurability = Durability.QuorumOrInvalidated; break;
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
                        owner.saveProgressToken(command.txnId(), token);
                    state.set(safeStore, owner, status, Queued);
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

    void clearHome(DefaultProgressLog instance)
    {
        clearHome(instance, Cleared);
    }

    void setHomeDone(DefaultProgressLog instance)
    {
        clearHome(instance, Done);
    }

    void clearHome(DefaultProgressLog instance, HomePhase phase)
    {
        set(null, instance, phase, NoneExpected);
        clearHomeRunCounter();
        instance.clearPendingAndActive(Home, txnId);
    }

    void setHomeDoneAndMaybeRemove(DefaultProgressLog instance)
    {
        setHomeDone(instance);
        maybeRemove(instance);
    }

    boolean isHomeDone()
    {
        return homePhase().compareTo(Done) >= 0;
    }

    boolean isHomeDoneOrUninitialised()
    {
        HomePhase phase = homePhase();
        return phase.compareTo(Done) >= 0 || phase == NotInitialised;
    }

    boolean isHomeInitialised()
    {
        return !isHomeUninitialised();
    }

    boolean isHomeUninitialised()
    {
        HomePhase phase = homePhase();
        return phase == NotInitialised;
    }

    boolean isHomeUninitialisedOrCleared()
    {
        HomePhase phase = homePhase();
        return phase == NotInitialised || phase == Cleared;
    }
}
