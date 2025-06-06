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

package accord.utils.async;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

public interface AsyncChain<V>
{
    <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper);

    default <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper, Executor executor)
    {
        return AsyncChains.map(this, mapper, executor);
    }

    <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper);

    default <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper, Executor executor)
    {
        return AsyncChains.flatMap(this, mapper, executor);
    }

    default <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper, BooleanSupplier inExecutor, Executor executor)
    {
        return flatMap(input -> {
            if (inExecutor.getAsBoolean())
                return AsyncChains.success(mapper.apply(input));
            else
                return AsyncChains.ofCallable(executor, () -> mapper.apply(input));
        });
    }

    /**
     * When the chain has failed, this allows the chain to attempt to recover if possible.  The provided function may return a {@code null} to represent
     * that recovery was not possible and that the original exception should propgate.
     * <p/>
     * This is similiar to {@link java.util.concurrent.CompletableFuture#exceptionally(Function)} but with async handling; would have the same semantics as the following
     * <p/>
     * {@code
     * CompletableFuture<V> failedFuture ...
     * failedFuture.exceptionally(cause -> {
     *     if (canHandle(cause)
     *       return handle(cause); // returns CompletableFuture<V>
     *     return CompletableFuture.completeExceptionally(cause) // return original exception
     * }).flatMap(f -> f); // "flatten" from CompletableFuture<CompletableFuture<V>> to CompletableFuture<V>
     * }
     */
    AsyncChain<V> recover(Function<? super Throwable, ? extends AsyncChain<V>> mapper);

    default AsyncChain<V> invokeIfSuccess(Consumer<? super V> action)
    {
        return map(r -> {
            action.accept(r);
            return r;
        });
    }

    default AsyncChain<V> invokeIfSuccess(Runnable runnable, Executor executor)
    {
        return map(r -> {
            runnable.run();
            return r;
        }, executor);
    }

    default AsyncChain<V> invokeIfSuccess(Consumer<? super V> action, Executor executor)
    {
        return map(r -> {
            action.accept(r);
            return r;
        }, executor);
    }

    default AsyncChain<V> withExecutor(Executor e)
    {
        // since a chain runs as a sequence of callbacks, by adding a callback that moves to this executor any new actions
        // will be run on that desired executor.
        return map(a -> a, e);
    }

    AsyncChain<V> invoke(BiConsumer<? super V, Throwable> callback);

    /**
     * Adds a callback that fires on success only
     */
    default AsyncChain<V> invokeIfSuccess(Runnable runnable)
    {
        return invoke((success, fail) -> {
            if (fail == null) runnable.run();
        });
    }

    /**
     * Adds a callback that fires on either success or failure
     */
    default AsyncChain<V> invoke(Runnable runnable)
    {
        return invoke((success, fail) -> runnable.run());
    }

    default AsyncChain<V> invoke(BiConsumer<? super V, Throwable> callback, ExecutorService es)
    {
        return invoke(AsyncCallbacks.inExecutorService(callback, es));
    }

    /**
     * Causes the chain to begin, starting all work required.  This method must be called exactly once, not calling will
     * not cause any work to start, and calling multiple times will be rejected.
     */
    @Nullable Cancellable begin(BiConsumer<? super V, Throwable> callback);

    default AsyncResult<V> beginAsResult()
    {
        return AsyncResults.forChain(this);
    }
}