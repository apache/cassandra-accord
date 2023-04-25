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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.primitives.*;

import javax.annotation.Nullable;

import static accord.utils.Utils.listOf;

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

    /**
     * Register a listener against the given TxnId, then load the associated transaction and invoke the listener
     * with its current state.
     */
    default void addAndInvokeListener(TxnId txnId, TxnId listenerId)
    {
        PreLoadContext context = PreLoadContext.contextFor(listOf(txnId, listenerId), Keys.EMPTY);
        commandStore().execute(context, safeStore -> {
            SafeCommand safeCommand = safeStore.command(txnId);
            Command.ProxyListener listener = new Command.ProxyListener(listenerId);
            safeCommand.addListener(listener);
            listener.onChange(safeStore, safeCommand);
        }).begin(agent());
    }

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
    enum TestKind { Ws, RorWs }

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
    long latestEpoch();
    Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys);

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
