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

package accord.api;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import accord.utils.async.AsyncChain;
import accord.utils.async.Cancellable;

// TODO (required): consistent RejectedExecutionException handling
public interface AsyncExecutor extends Executor
{
    Cancellable execute(Runnable run, BiConsumer<? super Void, Throwable> callback);
    <V> Cancellable execute(Callable<V> call, BiConsumer<? super V, Throwable> callback);
    <V> Cancellable flatExecute(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback);

    default Cancellable executeContinuation(Runnable run, BiConsumer<? super Void, Throwable> callback)
    {
        return execute(run, callback);
    }

    default <V> Cancellable executeContinuation(Callable<V> call, BiConsumer<? super V, Throwable> callback)
    {
        return execute(call, callback);
    }

    default <V> Cancellable flatExecuteContinuation(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback)
    {
        return flatExecute(call, callback);
    }

    default boolean tryExecuteImmediately(Runnable run) { return false; }

    // Depending on this implementation this method may queue-jump, i.e. task submission order is not guaranteed.
    // Make sure this is semantically safe at all call-sites.
    // TODO (required): RejectedExecutionException?
    default boolean executeMaybeImmediately(Runnable run)
    {
        if (tryExecuteImmediately(run))
            return true;

        execute(run);
        return false;
    }

    AsyncChain<Void> chain(Runnable run);

    /**
     * A task that logically continues the running task. Note that the enclosing task is the one that invokes
     * begin(), not the one that invokes continuationChain()
     */
    AsyncChain<Void> continuationChain(Runnable run);
    <V> AsyncChain<V> chain(Callable<V> call);
    <V> AsyncChain<V> flatChain(Callable<? extends AsyncChain<V>> call);
}
