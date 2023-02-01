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
import org.apache.cassandra.utils.concurrent.Future;

import javax.annotation.Nullable;
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

    /**
     * Register a listener against the given TxnId, then load the associated transaction and invoke the listener
     * with its current state.
     */
    void addAndInvokeListener(TxnId txnId, CommandListener listener);

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
     * Is able to search based on dependency contents, which may require reading from storage
     */
    interface SlowSearcher
    {
        <T> T fold(TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                   TestDep testDep, @Nullable TxnId depId,
                   @Nullable Status minStatus, @Nullable Status maxStatus,
                   SearchFunction<T, T> mapReduce, T initialValue, T terminalValue);
    }

    interface SlowSearchFunction<I, O>
    {
        O apply(SlowSearcher slowSearcher, Seekable keyOrRange, I in);
    }

    interface SearchFunction<I, O>
    {
        O apply(Seekable keyOrRange, TxnId txnId, Timestamp executeAt, I in);
    }

    /**
     * A slow fold that permits applying multiple searches to the data for an intersecting
     * key or range. TODO (expected): we might want some pre-filter?
     *
     * Visits keys in unspecified order, hence neither foldl/foldr, though we aim for foldl
     * (ascending order in keys/ranges then txnId).
     * This may be applied asynchronously, though it is expected to normally respond immediately.
     */
    <T> Future<T> slowFold(Seekables<?, ?> keys, Ranges slice,
                           SlowSearchFunction<T, T> fold, T initialValue, T terminalValue);

    /**
     * Visits keys in unspecified order, hence neither foldl/foldr, though we aim for foldl
     * (ascending order in keys/ranges then txnId).
     * This may be applied asynchronously, though it is expected to normally respond immediately.
     */
    <T> Future<T> fold(Seekables<?, ?> keys, Ranges slice,
                       TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp,
                       @Nullable Status minStatus, @Nullable Status maxStatus,
                       SearchFunction<T, T> fold, T initialValue, T terminalValue);

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

    Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function);
}
