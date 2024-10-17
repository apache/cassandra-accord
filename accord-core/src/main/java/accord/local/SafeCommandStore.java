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
import accord.impl.SafeTimestampsForKey;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.cfk.UpdateUnmanagedMode;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
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
import accord.utils.async.AsyncChain;

import static accord.local.KeyHistory.INCR;
import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;
import static accord.local.cfk.UpdateUnmanagedMode.REGISTER;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.SaveStatus.Applied;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Invariants.illegalState;

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
        return safeCommand;
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
        SafeCommand safeCommand = getInternal(txnId);
        if (safeCommand == null)
            throw notFound(txnId);

        return maybeCleanup(safeCommand, safeCommand.current(), participants);
    }

    protected SafeCommand get(TxnId txnId)
    {
        SafeCommand safeCommand = getInternal(txnId);
        if (safeCommand == null)
            throw notFound(txnId);

        return maybeCleanup(safeCommand, safeCommand.current(), StoreParticipants.empty(txnId));
    }

    public SafeCommand unsafeGet(TxnId txnId)
    {
        return get(txnId);
    }

    public SafeCommand unsafeGetNoCleanup(TxnId txnId)
    {
        return getInternal(txnId);
    }

    private RuntimeException notFound(TxnId txnId)
    {
        if (context().txnIds().contains(txnId)) throw illegalState("%s was specified in %s but was not returned by getInternal(key)", txnId, context().txnIds());
        else throw illegalArgument("%s was not specified in %s", txnId, context().txnIds());
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
    public SafeCommand ifLoadedAndInitialisedAndNotErased(TxnId txnId)
    {
        SafeCommand safeCommand = getInternal(txnId);
        if (safeCommand != null)
            return safeCommand.current().saveStatus().isUninitialised() ? null : safeCommand;

        safeCommand = ifLoadedAndInitialisedAndNotErasedInternal(txnId);
        if (safeCommand == null || safeCommand.current().saveStatus().isUninitialised())
            return null;

        return maybeCleanup(safeCommand, safeCommand.current(), StoreParticipants.empty(txnId));
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
        SafeCommandsForKey safeCfk = getInternal(key);
        if (safeCfk != null)
            return safeCfk;

        safeCfk = ifLoadedInternal(key);
        if (safeCfk == null)
            return null;
        return maybeCleanup(safeCfk);
    }

    public SafeCommandsForKey get(RoutingKey key)
    {
        SafeCommandsForKey safeCfk = getInternal(key);
        if (safeCfk != null)
            return maybeCleanup(safeCfk);

        if (context().keys().contains(key)) throw illegalState("%s was specified in %s but was not returned by getInternal(key)", key, context().keys());
        else throw illegalArgument("%s was not specified in %s", key, context());
    }

    public long preAcceptTimeout()
    {
        return agent().preAcceptTimeout();
    }

    /** Get anything already referenced (should include anything in PreLoadContext). If returned, should be initialised. */
    protected abstract SafeCommand getInternal(TxnId txnId);
    /** Get if available and initialised */
    protected abstract SafeCommand ifLoadedAndInitialisedAndNotErasedInternal(TxnId txnId);
    /** Get anything already referenced (should include anything in PreLoadContext) */
    protected abstract SafeCommandsForKey getInternal(RoutingKey key);
    /** Get if available */
    protected abstract SafeCommandsForKey ifLoadedInternal(RoutingKey key);

    // TODO (required): to be deprecated
    public abstract SafeTimestampsForKey timestampsForKey(RoutingKey key);

    public final boolean canExecuteWith(PreLoadContext context) { return canExecute(context) == context; }

    /**
     * Attempt to ready the provided PreLoadContext; if this can only be achieved partially, a new PreLoadContext
     * will be returned containing the readily available data. If nothing is available, null will be returned.
     */
    public abstract @Nullable PreLoadContext canExecute(PreLoadContext context);

    /**
     * The current PreLoadContext, excluding any upgrade.
     */
    public abstract PreLoadContext context();

    protected void update(Command prev, Command updated)
    {
        updateMaxConflicts(prev, updated);
        updateCommandsForKey(prev, updated);
        updateExclusiveSyncPoint(prev, updated);
    }

    public void updateExclusiveSyncPoint(Command prev, Command updated)
    {
        if (updated.txnId().kind() != Kind.ExclusiveSyncPoint || updated.txnId().domain() != Range) return;
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

    public void updateCommandsForKey(Command prev, Command next)
    {
        if (!CommandsForKey.needsUpdate(prev, next))
            return;

        TxnId txnId = next.txnId();
        if (CommandsForKey.manages(txnId)) updateManagedCommandsForKey(this, prev, next);
        if (!CommandsForKey.managesExecution(txnId) && next.hasBeen(Status.Stable) && !next.hasBeen(Status.Truncated) && !prev.hasBeen(Status.Stable))
            updateUnmanagedCommandsForKey(this, next, REGISTER);
        // TODO (expected): register deps during Accept phase to more quickly sync epochs
//        else if (txnId.is(Range) && next.known().deps.hasProposedOrDecidedDeps())
//            updateUnmanagedCommandsForKey(this, next, REGISTER_DEPS_ONLY);
    }

    private static void updateManagedCommandsForKey(SafeCommandStore safeStore, Command prev, Command next)
    {
        StoreParticipants participants = next.participants().supplement(prev.participants());
        Participants<?> update = next.hasBeen(Status.Committed) ? participants.hasTouched : participants.touches;
        if (update.isEmpty())
            return;

        // TODO (expected): we don't want to insert any dependencies for those we only touch; we just need to record them as decided/applied for execution
        PreLoadContext context = PreLoadContext.contextFor(update, INCR);
        PreLoadContext execute = safeStore.canExecute(context);
        if (execute != null)
        {
            updateManagedCommandsForKey(safeStore, execute.keys(), participants, next);
        }
        if (execute != context)
        {
            if (execute != null)
                context = PreLoadContext.contextFor(update.without(execute.keys()), INCR);

            Invariants.checkState(!context.keys().isEmpty());
            safeStore = safeStore; // prevent accidental usage inside lambda
            safeStore.commandStore().execute(context, safeStore0 -> updateManagedCommandsForKey(safeStore0, safeStore0.context().keys(), participants, next))
                     .begin(safeStore.commandStore().agent);
        }
    }

    private static void updateManagedCommandsForKey(SafeCommandStore safeStore, Unseekables<?> update, StoreParticipants participants, Command next)
    {
        for (RoutingKey key : (AbstractUnseekableKeys)update)
        {
            safeStore.get(key).update(safeStore, next, update != participants.touches && !participants.touches(key));
        }
    }

    private static void updateUnmanagedCommandsForKey(SafeCommandStore safeStore, Command next, UpdateUnmanagedMode mode)
    {
        TxnId txnId = next.txnId();
        // TODO (required): use StoreParticipants.executes()
        Command.WaitingOn waitingOn = next.asCommitted().waitingOn();
        RoutingKeys keys = waitingOn.waitingOnKeys();
        // TODO (required): consider how execution works for transactions that await future deps and where the command store inherits additional keys in execution epoch
        PreLoadContext context = PreLoadContext.contextFor(txnId, keys, INCR);
        PreLoadContext execute = safeStore.canExecute(context);
        // TODO (expected): execute immediately for any keys we already have loaded, and save only those we haven't for later
        if (execute != null)
        {
            updateUnmanagedCommandsForKey(safeStore, execute.keys(), txnId, mode);
        }
        if (execute == context)
        {
            if (next.txnId().is(Range))
                registerTransitive(safeStore, txnId, next);
        }
        else
        {
            if (execute != null)
                context = PreLoadContext.contextFor(txnId, keys.without(execute.keys()), INCR);

            safeStore = safeStore;
            CommandStore unsafeStore = safeStore.commandStore();
            AsyncChain<Void> submit = unsafeStore.execute(context, safeStore0 -> updateUnmanagedCommandsForKey(safeStore0, safeStore0.context().keys(), txnId, mode));
            if (next.txnId().is(Range))
                submit = submit.flatMap(success -> unsafeStore.execute(PreLoadContext.empty(), safeStore0 -> registerTransitive(safeStore0, txnId, next)));
            submit.begin(safeStore.commandStore().agent);
        }
    }

    private static void updateUnmanagedCommandsForKey(SafeCommandStore safeStore, Unseekables<?> update, TxnId txnId, UpdateUnmanagedMode mode)
    {
        SafeCommand safeCommand = safeStore.get(txnId);
        for (RoutingKey key : (AbstractUnseekableKeys)update)
        {
            safeStore.get(key).registerUnmanaged(safeStore, safeCommand, mode);
        }
    }

    private static void registerTransitive(SafeCommandStore safeStore, TxnId txnId, Command next)
    {
        CommandStore commandStore = safeStore.commandStore();
        Ranges ranges = next.participants().touches.toRanges();
        commandStore.registerTransitive(safeStore, next.partialDeps().rangeDeps);
        commandStore.markSynced(safeStore, txnId, ranges);
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
