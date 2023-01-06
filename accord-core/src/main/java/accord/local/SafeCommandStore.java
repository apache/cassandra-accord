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
import accord.api.Key;
import accord.api.ProgressLog;
import accord.primitives.*;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A CommandStore with exclusive access; a reference to this should not be retained outside of the scope of the method
 * that it is passed to. For the duration of the method invocation only, the methods on this interface are safe to invoke.
 *
 * Method implementations may therefore be single threaded, without volatile access or other concurrency control
 */
public interface SafeCommandStore
{
    Command ifPresent(TxnId txnId);

    /**
     * If the transaction is in memory, return it (and make it visible to future invocations of {@code command}, {@code ifPresent} etc).
     * Otherwise return null.
     *
     * This permits efficient operation when a transaction involved in processing another transaction happens to be in memory.
     */
    Command ifLoaded(TxnId txnId);
    Command command(TxnId txnId);

    CommandsForKey commandsForKey(Key key);
    CommandsForKey maybeCommandsForKey(Key key);

    /**
     * Register a listener against the given TxnId, then load the associated transaction and invoke the listener
     * with its current state.
     */
    void addAndInvokeListener(TxnId txnId, CommandListener listener);

    <T> T mapReduce(Routables<?, ?> keys, Ranges slice, Function<CommandsForKey, T> map, BinaryOperator<T> reduce, T initialValue);
    void forEach(Routables<?, ?> keys, Ranges slice, Consumer<CommandsForKey> forEach);
    void forEach(Routable keyOrRange, Ranges slice, Consumer<CommandsForKey> forEach);

    CommandStore commandStore();
    DataStore dataStore();
    Agent agent();
    ProgressLog progressLog();
    NodeTimeService time();
    CommandStores.RangesForEpoch ranges();
    long latestEpoch();
    Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys);

    Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function);
}
