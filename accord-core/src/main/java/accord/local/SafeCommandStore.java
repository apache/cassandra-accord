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
import accord.local.CommandStores.RangesForEpochSupplier;
import accord.local.RedundantBefore.RedundantBeforeSupplier;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.cfk.UpdateUnmanagedMode;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.KeyDeps;
import accord.primitives.Participants;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import accord.utils.SortedList;
import accord.utils.async.AsyncChain;

import static accord.local.KeyHistory.INCR;
import static accord.local.KeyHistory.NONE;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_WITNESSED_ONLY;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.cfk.UpdateUnmanagedMode.REGISTER;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.SaveStatus.Applied;
import static accord.primitives.SaveStatus.Committed;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Timestamp.Flag.SHARD_BOUND;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Invariants.illegalState;

/**
 * A CommandStore with exclusive access; a reference to this should not be retained outside of the scope of the method
 * that it is passed to. For the duration of the method invocation only, the methods on this interface are safe to invoke.
 *
 * Method implementations may therefore be single threaded, without volatile access or other concurrency control
 */
public abstract class SafeCommandStore implements RangesForEpochSupplier, RedundantBeforeSupplier, CommandSummaries
{
    private static final int MAX_REENTRANCY = 100;
    private int reentrancyCounter;
    public boolean tryRecurse()
    {
        if (reentrancyCounter == MAX_REENTRANCY)
            return false;
        ++reentrancyCounter;
        return true;
    }
    public void unrecurse()
    {
        --reentrancyCounter;
        Invariants.require(reentrancyCounter >= 0);
    }

    /**
     * If the transaction exists (with some associated data) in the CommandStore, return it. Otherwise return null.
     *
     * This is useful for operations that do not retain a route, but do expect to operate on existing local state;
     * this guards against recreating a previously truncated command when we do not otherwise have enough information
     * to prevent it.
     */
    public @Nullable SafeCommand ifInitialised(TxnId txnId)
    {
        SafeCommand safeCommand = getInternal(txnId);
        if (safeCommand == null)
            return null;
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
        SafeCommand safeCommand = getInternal(txnId);
        if (safeCommand == null)
            throw notFound(txnId);

        return maybeCleanup(safeCommand, participants);
    }

    protected SafeCommand get(TxnId txnId)
    {
        SafeCommand safeCommand = getInternal(txnId);
        if (safeCommand == null)
            throw notFound(txnId);

        return maybeCleanup(safeCommand);
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
        Command command = safeCommand.current();
        Commands.maybeCleanup(this, safeCommand, command, command.participants());
        return safeCommand;
    }

    protected SafeCommand maybeCleanup(SafeCommand safeCommand, @Nonnull StoreParticipants supplemental)
    {
        Command command = safeCommand.current();
        StoreParticipants participants = command.participants().supplementOrMerge(command.saveStatus(), supplemental);
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
        SafeCommand safeCommand = getInternal(txnId);
        if (safeCommand == null)
        {
            safeCommand = ifLoadedInternal(txnId);
            if (safeCommand == null)
                return null;
        }

        if (safeCommand.isUnset() || safeCommand.current().saveStatus() == Uninitialised)
            return null;

        return maybeCleanup(safeCommand);
    }

    protected SafeCommandsForKey maybeCleanup(SafeCommandsForKey safeCfk)
    {
        RedundantBefore.Bounds bounds = redundantBefore().get(safeCfk.key().toUnseekable());
        if (bounds != null)
            safeCfk.updateRedundantBefore(this, bounds);
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

        if (context().keyHistory() != NONE && context().keys().contains(key)) throw illegalState("%s was specified in %s but was not returned by getInternal(key)", key, context().keys());
        else throw illegalArgument("%s was not specified in %s", key, context());
    }

    /** Get anything already referenced (should include anything in PreLoadContext). If returned, should be initialised. */
    protected abstract SafeCommand getInternal(TxnId txnId);
    /** Get if available */
    protected abstract SafeCommand ifLoadedInternal(TxnId txnId);
    /** Get anything already referenced (should include anything in PreLoadContext) */
    protected abstract SafeCommandsForKey getInternal(RoutingKey key);
    /** Get if available */
    protected abstract SafeCommandsForKey ifLoadedInternal(RoutingKey key);

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

    protected void update(Command prev, Command updated, boolean force)
    {
        updateMaxConflicts(prev, updated, force);
        updateCommandsForKey(prev, updated, force);
        updateExclusiveSyncPoint(prev, updated, force);
    }

    public void updateExclusiveSyncPoint(Command prev, Command updated, boolean force)
    {
        if (updated.txnId().kind() != Kind.ExclusiveSyncPoint || updated.txnId().domain() != Range) return;
        if (updated.route() == null) return;

        SaveStatus oldSaveStatus = prev == null ? SaveStatus.Uninitialised : prev.saveStatus();
        SaveStatus newSaveStatus = updated.saveStatus();

        if (newSaveStatus.known.isDefinitionKnown() && (force || !oldSaveStatus.known.isDefinitionKnown()))
        {
            Ranges ranges = updated.participants().touches().toRanges();
            commandStore().markExclusiveSyncPoint(this, updated.txnId(), ranges);
        }

        if (newSaveStatus.compareTo(Committed) >= 0 && oldSaveStatus.compareTo(Committed) < 0 && !newSaveStatus.hasBeen(Status.Truncated))
        {
            Ranges ranges = updated.participants().owns().toRanges();
            commandStore().markExclusiveSyncPointDecided(this, updated.txnId(), ranges);
        }

        if (newSaveStatus == Applied && (force || oldSaveStatus != Applied))
        {
            Ranges ranges = updated.participants().touches().toRanges();
            TxnId txnIdWithFlags = (TxnId)updated.executeAt();
            commandStore().markExclusiveSyncPointLocallyApplied(this, txnIdWithFlags, ranges);
        }

        if (updated.partialDeps() != null)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.EMPTY;
            RangeDeps deps = updated.partialDeps().rangeDeps;
            for (int i = 0 ; i < deps.txnIdCount() ; ++i)
            {
                TxnId txnId = deps.txnIdWithFlags(i);
                if (txnId.is(SHARD_BOUND))
                {
                    Ranges ranges = deps.ranges(txnId).slice(ranges().all(), Minimal);
                    addRedundantBefore = RedundantBefore.merge(addRedundantBefore, RedundantBefore.create(ranges, txnId, LOCALLY_WITNESSED_ONLY));
                }
            }
            // TODO (expected): we should be able to use unsafeUpsertRedundantBefore here as the implementation will replay deps
            //  BUT the implementation may not replay all versions of the partialDeps we report here.
            //  This is likely to still be fine, as we don't imply anything for GC, but we won't do it for the moment.
            //  We might instead prefer to report these deps only once we are certain they won't change (i.e. when Stable),
            //  BUT in this case we cannot update waitingOnSync until then either (also probably fine)
            if (addRedundantBefore != RedundantBefore.EMPTY)
                upsertRedundantBefore(addRedundantBefore);
        }
    }

    public void updateMaxConflicts(Command prev, Command updated, boolean force)
    {
        SaveStatus oldSaveStatus = prev == null ? SaveStatus.Uninitialised : prev.saveStatus();
        SaveStatus newSaveStatus = updated.saveStatus();
        if (newSaveStatus.status.equals(oldSaveStatus.status) && oldSaveStatus.known.definition().isKnown() && !force)
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

    protected void unsafeSetRebootstrapping(boolean val)
    {
        commandStore().unsafeSetRebootstrapping(val);
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

    public void updateCommandsForKey(Command prev, Command next, boolean force)
    {
        if (!CommandsForKey.needsUpdate(this, prev, next) && !force)
            return;

        TxnId txnId = next.txnId();
        if (CommandsForKey.manages(txnId)) updateManagedCommandsForKey(this, prev, next);
        if (!CommandsForKey.managesExecution(txnId) && next.hasBeen(Status.Stable) && !next.hasBeen(Status.Truncated) && !prev.hasBeen(Status.Stable))
            updateUnmanagedCommandsForKey(this, next, REGISTER);
        // TODO (expected): register deps during Accept phase to more quickly sync epochs
//        else if (txnId.is(Range) && next.known().deps.hasProposedOrDecidedDeps())
//            updateUnmanagedCommandsForKey(this, next, REGISTER_DEPS_ONLY);
    }

    abstract protected void persistFieldUpdates();

    private static void updateManagedCommandsForKey(SafeCommandStore safeStore, Command prev, Command next)
    {
        StoreParticipants participants = next.participants().supplement(prev.participants());
        Participants<?> update = next.hasBeen(Status.Committed) ? participants.hasTouched() : participants.stillTouches();
        if (update.isEmpty())
            return;

        // TODO (expected): we don't want to insert any dependencies for those we only touch; we just need to record them as decided/applied for execution
        PreLoadContext context = PreLoadContext.contextFor(next.txnId(), update, INCR);
        PreLoadContext execute = safeStore.canExecute(context);
        if (execute != null)
        {
            updateManagedCommandsForKey(safeStore, execute.keys(), next.txnId());
        }
        if (execute != context)
        {
            if (execute != null)
                context = PreLoadContext.contextFor(next.txnId(), update.without(execute.keys()), INCR);

            Invariants.require(!context.keys().isEmpty());
            safeStore = safeStore; // prevent accidental usage inside lambda
            safeStore.commandStore().execute(context, safeStore0 -> {
                PreLoadContext ctx = safeStore0.context();
                TxnId txnId = ctx.primaryTxnId();
                Unseekables<?> keys = ctx.keys();
                updateManagedCommandsForKey(safeStore0, keys, txnId);
            }, safeStore.commandStore().agent);
        }
    }

    private static void updateManagedCommandsForKey(SafeCommandStore safeStore, Unseekables<?> update, TxnId txnId)
    {
        // TODO (expected): avoid reentrancy / recursion
        SafeCommand safeCommand = safeStore.get(txnId);
        for (RoutingKey key : (AbstractUnseekableKeys)update)
        {
            // we use callback and re-fetch current to guard against reentrancy causing
            // us to interact with "future" or stale information (respectively)
            safeStore.get(key).callback(safeStore, safeCommand.current());
        }
    }

    private static void updateUnmanagedCommandsForKey(SafeCommandStore safeStore, Command next, UpdateUnmanagedMode mode)
    {
        TxnId txnId = next.txnId();
        RoutingKeys keys;

        if (!txnId.is(Kind.ExclusiveSyncPoint)) keys = next.asCommitted().waitingOn().keys;
        else
        {
            Command.WaitingOn waitingOn = next.asCommitted().waitingOn;
            RedundantBefore redundantBefore = safeStore.redundantBefore();
            KeyDeps deps = next.partialDeps().keyDeps;
            keys = deps.keys();
            SimpleBitSet select = new SimpleBitSet(keys.size());
            for (int i = 0 ; i < keys.size() ; ++i)
            {
                if (waitingOn.isWaitingOnKey(i))
                {
                    select.set(i);
                    continue;
                }

                SortedList<TxnId> txnIdsForKey = deps.txnIdsForKeyIndex(i);
                RoutingKey key = keys.get(i);
                TxnId maxTxnId = txnIdsForKey.get(txnIdsForKey.size() - 1);
                // TODO (desired): convert to O(n) merge
                RedundantStatus status = redundantBefore.status(maxTxnId, null, key);
                if (!status.all(SHARD_APPLIED) || !status.all(LOCALLY_REDUNDANT))
                    select.set(i);
            }
            if (select.getSetBitCount() != keys.size())
            {
                RoutingKey[] array = new RoutingKey[select.getSetBitCount()];
                int count = 0;
                for (int i = 0 ; i < keys.size() ; ++i)
                {
                    if (select.get(i))
                        array[count++] = keys.get(i);
                }
                keys = RoutingKeys.ofSortedUnique(array);
            }
        }
        // TODO (required): use StoreParticipants.executes()
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
            AsyncChain<Void> submit = unsafeStore.build(context, safeStore0 -> { updateUnmanagedCommandsForKey(safeStore0, safeStore0.context().keys() , txnId, mode); });
            if (next.txnId().is(Range))
                submit = submit.flatMap(success -> unsafeStore.build(PreLoadContext.empty(), safeStore0 -> { registerTransitive(safeStore0, txnId, next); }));
            submit.begin(safeStore.commandStore().agent);
        }
    }

    private static TxnId maxTxnId(KeyDeps keyDeps, RoutingKey key)
    {
        int i = keyDeps.keys().indexOf(key);
        if (i < 0)
            return TxnId.NONE;
        SortedList<TxnId> txnIdsForKey = keyDeps.txnIdsForKeyIndex(i);
        return txnIdsForKey.get(txnIdsForKey.size() - 1);
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
        Ranges ranges = next.participants().touches().toRanges();
        commandStore.registerTransitive(safeStore, next.partialDeps().rangeDeps);
        if (txnId.is(Kind.ExclusiveSyncPoint))
            commandStore.markSynced(safeStore, txnId, ranges);
    }

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

    public boolean safeToCoordinate(TxnId txnId, Unseekables<?> participants)
    {
        return !commandStore().unsafeGetRebootstrapping() || safeToReadAt(txnId).intersects(participants);
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

    public void registerListener(SafeCommand listeningTo, SaveStatus await, TxnId waiting)
    {
        Invariants.require(listeningTo.current().saveStatus().compareTo(await) < 0);
        Invariants.require(!CommandsForKey.managesExecution(listeningTo.txnId()));
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
