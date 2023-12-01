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

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.ErasedSafeCommand;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

import javax.annotation.Nullable;

import com.google.common.base.Predicates;

import static accord.local.Commands.Cleanup.NO;
import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;

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
        O apply(P1 p1, Seekable keyOrRange, TxnId txnId, Timestamp executeAt, Status status, Supplier<List<TxnId>> depsSupplier, I in);
    }

    public enum TestTimestamp
    {
        STARTED_BEFORE,
        STARTED_AFTER,
        MAY_EXECUTE_BEFORE, // started before and uncommitted, or committed and executes before
        EXECUTES_AFTER
    }
    public enum TestDep { WITH, WITHOUT, ANY_DEPS }

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
        return maybeTruncate(safeCommand, command, txnId, null);
    }

    public SafeCommand get(TxnId txnId, RoutingKey unseekable)
    {
        SafeCommand safeCommand = get(txnId);
        Command command = safeCommand.current();
        if (command.saveStatus().isUninitialised())
        {
            if (commandStore().durableBefore().isUniversal(txnId, unseekable))
                return new ErasedSafeCommand(txnId);
        }
        return maybeTruncate(safeCommand, command, txnId, null);
    }

    public abstract void removeCommandFromSeekableDeps(Seekable seekable, TxnId txnId, Timestamp executeAt, Status status);

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
    public SafeCommand get(TxnId txnId, EpochSupplier toEpoch, Unseekables<?> unseekables)
    {
        SafeCommand safeCommand = get(txnId);
        Command command = safeCommand.current();
        if (command.saveStatus().isUninitialised())
        {
            if (commandStore().durableBefore().isUniversal(txnId, unseekables))
                return new ErasedSafeCommand(txnId);
        }
        return maybeTruncate(safeCommand, command, toEpoch, unseekables);
    }

    protected SafeCommand maybeTruncate(SafeCommand safeCommand, Command command, @Nullable EpochSupplier toEpoch, @Nullable Unseekables<?> maybeFullRoute)
    {
        Commands.cleanup(this, safeCommand, command, toEpoch, maybeFullRoute);
        return safeCommand;
    }

    /**
     * If the transaction is in memory, return it (and make it visible to future invocations of {@code command}, {@code ifPresent} etc).
     * Otherwise return null.
     *
     * This permits efficient operation when a transaction involved in processing another transaction happens to be in memory.
     */
    public final SafeCommand ifLoadedAndInitialised(TxnId txnId)
    {
        SafeCommand safeCommand = getInternalIfLoadedAndInitialised(txnId);
        if (safeCommand == null)
            return null;
        return maybeTruncate(safeCommand, safeCommand.current(), txnId, null);
    }

    protected SafeCommand get(TxnId txnId)
    {
        SafeCommand safeCommand = getInternal(txnId);
        return maybeTruncate(safeCommand, safeCommand.current(), null, null);
    }

    protected abstract SafeCommand getInternal(TxnId txnId);
    protected abstract SafeCommand getInternalIfLoadedAndInitialised(TxnId txnId);

    public abstract boolean canExecuteWith(PreLoadContext context);

    /**
     * Visits keys first and then ranges, both in ascending order.
     * Within each key or range visits all unevicted txnids needed for the given scope in ascending order of queried timestamp.
     */
    public <P1, T> T mapReduce(Seekables<?, ?> keys, Ranges slice, KeyHistory keyHistory,
                                        Kinds testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                                        TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus,
                                        CommandFunction<P1, T, T> map, P1 p1, T initialValue)
    {
        return mapReduce(keys, slice, keyHistory, testKind, testTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, p1, initialValue, Predicates.alwaysFalse());
    }

    public abstract <P1, T> T mapReduce(Seekables<?, ?> keys, Ranges slice, KeyHistory keyHistory,
                                        Kinds testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                                        TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus,
                                        CommandFunction<P1, T, T> map, P1 p1, T initialValue, Predicate<? super T> terminate);
    protected abstract void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command);
    protected abstract void register(Seekable keyOrRange, Ranges slice, Command command);

    public abstract CommandStore commandStore();
    public abstract DataStore dataStore();
    public abstract Agent agent();
    public abstract ProgressLog progressLog();
    public abstract NodeTimeService time();
    public abstract CommandStores.RangesForEpoch ranges();
    public abstract Timestamp maxConflict(Seekables<?, ?> keys, Ranges slice);
    public abstract void registerHistoricalTransactions(Deps deps);
    public abstract void erase(SafeCommand safeCommand);

    public long latestEpoch()
    {
        return time().epoch();
    }

    public boolean isTruncated(Command command)
    {
        return Commands.shouldCleanup(this, command, null, null) != NO;
    }

    // if we have to re-bootstrap (due to failed bootstrap or catching up on a range) then we may
    // have dangling redundant commands; these can safely be executed locally because we are a timestamp store
    final boolean isFullyPreBootstrapOrStale(Command command, Participants<?> forKeys)
    {
        return commandStore().redundantBefore().preBootstrapOrStale(command.txnId(), command.executeAtOrTxnId(), forKeys) == FULLY;
    }

    public void notifyListeners(SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        notifyListeners(safeCommand, command, command.durableListeners(), safeCommand.transientListeners());
    }

    public void notifyListeners(SafeCommand safeCommand, Command command, Listeners<Command.DurableAndIdempotentListener> durableListeners, Listeners<Command.TransientListener> transientListeners)
    {
        Iterator<Command.DurableAndIdempotentListener> durableIterator = durableListeners.reverseIterator();
        while (durableIterator.hasNext())
        {
            Command.DurableAndIdempotentListener listener = durableIterator.next();
            notifyListener(this, safeCommand, command, listener);
        }

        Iterator<Command.TransientListener> transientIterator = transientListeners.reverseIterator();
        while (transientIterator.hasNext())
        {
            Command.TransientListener listener = transientIterator.next();
            notifyListener(this, safeCommand, command, listener);
        }
    }

    public static void notifyListener(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, Command.TransientListener listener)
    {
        if (!safeCommand.transientListeners().contains(listener))
            return;

        PreLoadContext context = listener.listenerPreLoadContext(command.txnId());
        if (safeStore.canExecuteWith(context))
        {
            listener.onChange(safeStore, safeCommand);
        }
        else
        {
            TxnId txnId = command.txnId();
            safeStore.commandStore()
                     .execute(context, safeStore2 -> {
                         SafeCommand safeCommand2 = safeStore2.get(txnId);
                         // listeners invocations may be triggered more than once asynchronously for different changes
                         // so one pending invocation may unregister the listener prior to the second invocation running
                         // so we check if the listener is still valid before running
                         if (safeCommand2.transientListeners().contains(listener))
                            listener.onChange(safeStore2, safeCommand2);
                     })
                     .begin(safeStore.agent());
        }
    }

    public static void notifyListener(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, Command.DurableAndIdempotentListener listener)
    {
        PreLoadContext context = listener.listenerPreLoadContext(command.txnId());
        if (safeStore.canExecuteWith(context))
        {
            listener.onChange(safeStore, safeCommand);
        }
        else
        {
            TxnId txnId = command.txnId();
            safeStore.commandStore()
                     .execute(context, safeStore2 -> listener.onChange(safeStore2, safeStore2.get(txnId)))
                     .begin(safeStore.agent());
        }
    }
}
