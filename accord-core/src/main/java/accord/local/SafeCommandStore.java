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

import java.util.function.Predicate;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.primitives.Deps;
import accord.primitives.Ranges;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import javax.annotation.Nullable;

import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;

/**
 * A CommandStore with exclusive access; a reference to this should not be retained outside of the scope of the method
 * that it is passed to. For the duration of the method invocation only, the methods on this interface are safe to invoke.
 *
 * Method implementations may therefore be single threaded, without volatile access or other concurrency control
 */
public interface SafeCommandStore
{
    SafeCommand ifPresent(TxnId txnId);

    /**
     * If the transaction is in memory, return it (and make it visible to future invocations of {@code command}, {@code ifPresent} etc).
     * Otherwise return null.
     *
     * This permits efficient operation when a transaction involved in processing another transaction happens to be in memory.
     */
    SafeCommand ifLoaded(TxnId txnId);
    SafeCommand command(TxnId txnId);

    boolean canExecuteWith(PreLoadContext context);

    interface CommandFunction<I, O>
    {
        O apply(Seekable keyOrRange, TxnId txnId, Timestamp executeAt, I in);
    }

    enum TestTimestamp
    {
        STARTED_BEFORE,
        STARTED_AFTER,
        MAY_EXECUTE_BEFORE, // started before and uncommitted, or committed and executes before
        EXECUTES_AFTER
    }
    enum TestDep { WITH, WITHOUT, ANY_DEPS }
    enum TestKind implements Predicate<Kind>
    {
        Ws, RorWs, WsOrSyncPoint, SyncPoints, Any;

        @Override
        public boolean test(Kind kind)
        {
            switch (this)
            {
                default: throw new AssertionError();
                case Any: return true;
                case WsOrSyncPoint: return kind == Write || kind == Kind.SyncPoint || kind == ExclusiveSyncPoint;
                case SyncPoints: return kind == Kind.SyncPoint || kind == ExclusiveSyncPoint;
                case Ws: return kind == Write;
                case RorWs: return kind == Read || kind == Write;
            }
        }

        public static TestKind conflicts(Kind kind)
        {
            switch (kind)
            {
                default: throw new AssertionError();
                case Read:
                case NoOp:
                    return Ws;
                case Write:
                    return RorWs;
                case SyncPoint:
                case ExclusiveSyncPoint:
                    return Any;
            }
        }

        public static TestKind shouldHaveWitnessed(Kind kind)
        {
            switch (kind)
            {
                default: throw new AssertionError();
                case Read:
                    return WsOrSyncPoint;
                case Write:
                    return Any;
                case SyncPoint:
                case ExclusiveSyncPoint:
                case NoOp:
                    return SyncPoints;
            }
        }

    }

    /**
     * Visits keys first and then ranges, both in ascending order.
     * Within each key or range visits TxnId in ascending order of queried timestamp.
     */
    <T> T mapReduce(Seekables<?, ?> keys, Ranges slice,
                    TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                    TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus,
                    CommandFunction<T, T> map, T initialValue, T terminalValue);

    void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command);
    void register(Seekable keyOrRange, Ranges slice, Command command);

    CommandStore commandStore();
    DataStore dataStore();
    Agent agent();
    ProgressLog progressLog();
    NodeTimeService time();
    CommandStores.RangesForEpoch ranges();
    Timestamp maxConflict(Seekables<?, ?> keys, Ranges slice);
    void registerHistoricalTransactions(Deps deps);

    default long latestEpoch()
    {
        return time().epoch();
    }

    default void notifyListeners(SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        for (Command.DurableAndIdempotentListener listener : command.durableListeners())
            notifyListener(this, safeCommand, command, listener);

        for (Command.TransientListener listener : safeCommand.transientListeners())
            notifyListener(this, safeCommand, command, listener);
    }

    static void notifyListener(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, Command.TransientListener listener)
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
                         SafeCommand safeCommand2 = safeStore2.command(txnId);
                         // listeners invocations may be triggered more than once asynchronously for different changes
                         // so one pending invocation may unregister the listener prior to the second invocation running
                         // so we check if the listener is still valid before running
                         if (safeCommand2.transientListeners().contains(listener))
                            listener.onChange(safeStore2, safeCommand2);
                     })
                     .begin(safeStore.agent());
        }
    }

    static void notifyListener(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, Command.DurableAndIdempotentListener listener)
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
                     .execute(context, safeStore2 -> listener.onChange(safeStore2, safeStore2.command(txnId)))
                     .begin(safeStore.agent());
        }
    }
}
