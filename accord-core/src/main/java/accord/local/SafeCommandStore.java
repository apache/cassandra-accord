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

package accord.local;

import java.util.NavigableMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Deps;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import static accord.local.KeyHistory.COMMANDS;
import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.SaveStatus.Applied;

/**
 * A CommandStore with exclusive access; a reference to this should not be retained outside of the scope of the method
 * that it is passed to. For the duration of the method invocation only, the methods on this interface are safe to invoke.
 *
 * Method implementations may therefore be single threaded, without volatile access or other concurrency control
 */
public abstract class SafeCommandStore
{
    public interface CommandFunction<P1, I, O>
    {
        O apply(P1 p1, Unseekable keyOrRange, TxnId txnId, Timestamp executeAt, I in);
    }

    public enum TestStartedAt
    {
        STARTED_BEFORE,
        STARTED_AFTER,
        ANY
    }
    public enum TestDep { WITH, WITHOUT, ANY_DEPS }
    public enum TestStatus { ANY_STATUS, IS_PROPOSED, IS_STABLE }

    /**
     * If the transaction exists (with some associated data) in the CommandStore, return it. Otherwise return null.
     *
     * This is useful for operations that do not retain a route, but do expect to operate on existing local state;
     * this guards against recreating a previously truncated command when we do not otherwise have enough information
     * to prevent it.
     */
    public @Nullable SafeCommand ifInitialised(TxnId txnId)
    {
        SafeCommand safeCommand = get(txnId);
        Command command = safeCommand.current();
        if (command.saveStatus().isUninitialised())
            return null;
        return maybeCleanup(safeCommand);
    }

    // decidedExecuteAt == null if not yet PreCommitted

    /**
     * Retrieve a SafeCommand. If it is initialised, optionally use its present contents to determine if it should be
     * truncated, and apply the truncation before returning the command.
     * This behaviour may be overridden by implementations if they know any truncation would already have been applied.
     *
     * If it is not initialised, use the provided parameters to determine if the record may have been expunged;
     * if not, create it.
     *
     * We do not distinguish between participants, home keys, and non-participating home keys for now, even though
     * these fundamentally have different implications. Logically, we may erase a home shard's record as soon as
     * the transaction has been made durable at a majority of replicas of every shard, and state for any participating
     * keys may be erased as soon as their non-faulty peers have recorded the outcome.
     *
     * However if in some cases we don't know which commands are home keys or participants we need to wait to erase
     * a transaction until both of these criteria are met for every key.
     *
     * TODO (desired): Introduce static types that permit us to propagate this information safely.
     */
    public SafeCommand get(TxnId txnId, StoreParticipants participants)
    {
        SafeCommand safeCommand = get(txnId);
        Commands.maybeCleanup(this, safeCommand, safeCommand.current(), participants);
        return safeCommand;
    }

    protected SafeCommand maybeCleanup(SafeCommand safeCommand)
    {
        return maybeCleanup(safeCommand, safeCommand.current(), StoreParticipants.empty(safeCommand.txnId()));
    }

    protected SafeCommand maybeCleanup(SafeCommand safeCommand, Command command, @Nonnull StoreParticipants participants)
    {
        Commands.maybeCleanup(this, safeCommand, command, participants);
        return safeCommand;
    }

    /**
     * If the transaction is in memory, return it (and make it visible to future invocations of {@code command}, {@code ifPresent} etc).
     * Otherwise return null.
     *
     * This permits efficient operation when a transaction involved in processing another transaction happens to be in memory.
     */
    public SafeCommand ifLoadedAndInitialised(TxnId txnId)
    {
        SafeCommand safeCommand = getInternalIfLoadedAndInitialised(txnId);
        if (safeCommand == null)
            return null;
        return maybeCleanup(safeCommand, safeCommand.current(), StoreParticipants.empty(txnId));
    }

    protected SafeCommand get(TxnId txnId)
    {
        SafeCommand safeCommand = getInternal(txnId);
        return maybeCleanup(safeCommand, safeCommand.current(), StoreParticipants.empty(txnId));
    }

    public SafeCommand unsafeGet(TxnId txnId)
    {
        return get(txnId);
    }

    protected SafeCommandsForKey maybeCleanup(SafeCommandsForKey safeCfk)
    {
        RedundantBefore.Entry entry = redundantBefore().get(safeCfk.key().toUnseekable());
        if (entry != null)
            safeCfk.updateRedundantBefore(this, entry);
        return safeCfk;
    }

    /**
     * If the transaction is in memory, return it (and make it visible to future invocations of {@code command}, {@code ifPresent} etc).
     * Otherwise return null.
     *
     * This permits efficient operation when a transaction involved in processing another transaction happens to be in memory.
     */
    public final SafeCommandsForKey ifLoadedAndInitialised(RoutingKey key)
    {
        SafeCommandsForKey safeCfk = getInternalIfLoadedAndInitialised(key);
        if (safeCfk == null)
            return null;
        return maybeCleanup(safeCfk);
    }

    public SafeCommandsForKey get(RoutingKey key)
    {
        SafeCommandsForKey safeCfk = getInternal(key);
        return maybeCleanup(safeCfk);
    }

    public long preAcceptTimeout()
    {
        return agent().preAcceptTimeout();
    }

    protected abstract SafeCommand getInternal(TxnId txnId);
    protected abstract SafeCommand getInternalIfLoadedAndInitialised(TxnId txnId);
    protected abstract SafeCommandsForKey getInternal(RoutingKey key);
    protected abstract SafeCommandsForKey getInternalIfLoadedAndInitialised(RoutingKey key);
    public abstract boolean canExecuteWith(PreLoadContext context);

    protected void update(Command prev, Command updated)
    {
        updateMaxConflicts(prev, updated);
        updateCommandsForKey(prev, updated);
        updateExclusiveSyncPoint(prev, updated);
    }

    public void updateExclusiveSyncPoint(Command prev, Command updated)
    {
        if (updated.txnId().kind() != Kind.ExclusiveSyncPoint || updated.txnId().domain() != Domain.Range) return;
        if (updated.route() == null) return;

        SaveStatus oldSaveStatus = prev == null ? SaveStatus.Uninitialised : prev.saveStatus();
        SaveStatus newSaveStatus = updated.saveStatus();

        TxnId txnId = updated.txnId();
        if (newSaveStatus.known.isDefinitionKnown() && !oldSaveStatus.known.isDefinitionKnown())
        {
            Ranges ranges = updated.route().slice(ranges().all(), Minimal).toRanges();
            commandStore().markExclusiveSyncPoint(this, txnId, ranges);
        }

        if (newSaveStatus == Applied && oldSaveStatus != Applied)
        {
            Ranges ranges = updated.route().slice(ranges().all(), Minimal).toRanges();
            commandStore().markExclusiveSyncPointLocallyApplied(this, txnId, ranges);
        }
    }

    public void updateMaxConflicts(Command prev, Command updated)
    {
        SaveStatus oldSaveStatus = prev == null ? SaveStatus.Uninitialised : prev.saveStatus();
        SaveStatus newSaveStatus = updated.saveStatus();
        if (newSaveStatus.status.equals(oldSaveStatus.status) && oldSaveStatus.known.definition.isKnown())
            return;

        TxnId txnId = updated.txnId();
        if (!txnId.isVisible())
            return;

        commandStore().updateMaxConflicts(prev, updated);
    }

    /**
     * Methods that implementors can use to capture changes to auxiliary collections:
     */

    public abstract void upsertRedundantBefore(RedundantBefore addRedundantBefore);

    protected void unsafeSetRedundantBefore(RedundantBefore newRedundantBefore)
    {
        commandStore().unsafeSetRedundantBefore(newRedundantBefore);
    }

    protected void unsafeUpsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        commandStore().unsafeUpsertRedundantBefore(addRedundantBefore);
    }

    public void setBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        commandStore().unsafeSetBootstrapBeganAt(newBootstrapBeganAt);
    }

    public void setSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        commandStore().unsafeSetSafeToRead(newSafeToRead);
    }

    public void setRangesForEpoch(CommandStores.RangesForEpoch rangesForEpoch)
    {
        commandStore().unsafeSetRangesForEpoch(rangesForEpoch);
    }

    protected void registerHistoricalTransactions(Deps deps)
    {
        commandStore().registerHistoricalTransactions(deps, this);
    }

    public void updateCommandsForKey(Command prev, Command next)
    {
        if (!CommandsForKey.needsUpdate(prev, next))
            return;

        TxnId txnId = next.txnId();
        if (CommandsForKey.manages(txnId)) updateManagedCommandsForKey(this, prev, next);
        if (!CommandsForKey.managesExecution(txnId) && next.hasBeen(Status.Stable) && !next.hasBeen(Status.Truncated) && !prev.hasBeen(Status.Stable))
            updateUnmanagedExecutionCommandsForKey(this, next);
    }

    private static void updateManagedCommandsForKey(SafeCommandStore safeStore, Command prev, Command next)
    {
        TxnId txnId = next.txnId();
        StoreParticipants participants = next.participants().supplement(prev.participants());
        Participants<?> update = next.hasBeen(Status.Committed) ? participants.hasTouched : participants.touches;

        // TODO (expected): we don't want to insert any dependencies for those we only touch; we just need to record them as decided/applied for execution
        PreLoadContext context = PreLoadContext.contextFor(txnId, update, COMMANDS);
        // TODO (expected): execute immediately for any keys we already have loaded, and save only those we haven't for later
        if (safeStore.canExecuteWith(context))
        {
            for (RoutingKey key : (AbstractUnseekableKeys)update)
            {
                safeStore.get(key).update(safeStore, next, update != participants.touches && !participants.touches(key));
            }
        }
        else
        {
            safeStore = safeStore; // prevent accidental usage inside lambda
            safeStore.commandStore().execute(context, safeStore0 -> updateManagedCommandsForKey(safeStore0, prev, next))
                          .begin(safeStore.commandStore().agent);
        }
    }

    private static void updateUnmanagedExecutionCommandsForKey(SafeCommandStore safeStore, Command next)
    {
        TxnId txnId = next.txnId();
        // TODO (required): use StoreParticipants.executes()
        Command.WaitingOn waitingOn = next.asCommitted().waitingOn();
        RoutingKeys keys = waitingOn.keys;
        // TODO (required): consider how execution works for transactions that await future deps and where the command store inherits additional keys in execution epoch
        PreLoadContext context = PreLoadContext.contextFor(txnId, keys, COMMANDS);
        // TODO (expected): execute immediately for any keys we already have loaded, and save only those we haven't for later
        if (safeStore.canExecuteWith(context))
        {
            int index = 0;
            for (RoutingKey key : keys)
            {
                if (!waitingOn.isWaitingOnKey(index++)) continue;
                safeStore.get(key).registerUnmanaged(safeStore, safeStore.get(txnId));
            }
        }
        else
        {
            safeStore = safeStore;
            safeStore.commandStore().execute(context, safeStore0 -> updateUnmanagedExecutionCommandsForKey(safeStore0, next))
                          .begin(safeStore.commandStore().agent);
        }
    }



    /**
     * Visits keys first and then ranges, both in ascending order.
     * Within each key or range visits all visible txnids needed for the given scope in ascending order of queried timestamp.
     * TODO (expected): no need for slice in most (all?) cases
     */
    public abstract <P1, T> T mapReduceActive(Unseekables<?> keys, @Nullable Timestamp withLowerTxnId, Kinds kinds, CommandFunction<P1, T, T> map, P1 p1, T initialValue);

    /**
     * Visits keys first and then ranges, both in ascending order.
     * Within each key or range visits all unevicted txnids needed for the given scope in ascending order of queried timestamp.
     */
    public abstract <P1, T> T mapReduceFull(Unseekables<?> keys,
                                            TxnId testTxnId,
                                            Kinds testKind,
                                            TestStartedAt testStartedAt,
                                            TestDep testDep,
                                            TestStatus testStatus,
                                            CommandFunction<P1, T, T> map, P1 p1, T initialValue);

    public abstract CommandStore commandStore();
    public abstract DataStore dataStore();
    public abstract Agent agent();
    public abstract ProgressLog progressLog();
    public abstract NodeCommandStoreService node();
    public abstract CommandStores.RangesForEpoch ranges();

    protected NavigableMap<TxnId, Ranges> bootstrapBeganAt()
    {
        return commandStore().unsafeGetBootstrapBeganAt();
    }

    protected NavigableMap<Timestamp, Ranges> safeToReadAt()
    {
        return commandStore().unsafeGetSafeToRead();
    }

    public RedundantBefore redundantBefore()
    {
        return commandStore().unsafeGetRedundantBefore();
    }

    public DurableBefore durableBefore()
    {
        return commandStore().node.durableBefore();
    }

    public Ranges futureRanges(TxnId txnId)
    {
        return ranges().allBefore(txnId.epoch());
    }

    public Ranges coordinateRanges(TxnId txnId)
    {
        return ranges().allAt(txnId.epoch());
    }

    public Ranges ranges(TxnId txnId, Timestamp executeAt)
    {
        return ranges(txnId, executeAt.epoch());
    }

    public Ranges ranges(TxnId txnId, long untilLocalEpoch)
    {
        return ranges().allBetween(txnId.epoch(), untilLocalEpoch);
    }

    public final Ranges safeToReadAt(Timestamp at)
    {
        return safeToReadAt().lowerEntry(at).getValue();
    }

    public @Nonnull Ranges unsafeToReadAt(Timestamp at)
    {
        return ranges().allAt(at).without(safeToReadAt(at));
    }

    // if we have to re-bootstrap (due to failed bootstrap or catching up on a range) then we may
    // have dangling redundant commands; these can safely be executed locally because we are a timestamp store
    final boolean isFullyPreBootstrapOrStale(Command command, Participants<?> forKeys)
    {
        return redundantBefore().preBootstrapOrStale(command.txnId(), forKeys) == FULLY;
    }

    public void registerListener(SafeCommand listeningTo, SaveStatus await, TxnId waiting)
    {
        Invariants.checkState(listeningTo.current().saveStatus().compareTo(await) < 0);
        Invariants.checkState(!CommandsForKey.managesExecution(listeningTo.txnId()));
        commandStore().listeners.register(listeningTo.txnId(), await, waiting);
    }

    public LocalListeners.Registered registerAndInvoke(TxnId txnId, RoutingKey someKey, LocalListeners.ComplexListener listener)
    {
        StoreParticipants participants = StoreParticipants.read(this, Participants.singleton(txnId.domain(), someKey), txnId);
        LocalListeners.Registered registered = register(txnId, listener);
        if (!listener.notify(this, get(txnId, participants)))
            registered.cancel();
        return registered;
    }

    public LocalListeners.Registered register(TxnId txnId, LocalListeners.ComplexListener listener)
    {
        return commandStore().listeners.register(txnId, listener);
    }

    public void notifyListeners(SafeCommand safeCommand, Command prev)
    {
        commandStore().listeners.notify(this, safeCommand, prev);
    }
}
