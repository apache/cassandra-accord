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

import com.google.common.util.concurrent.ListenableFuture;

public interface AsyncChain<V>
{
    /**
     * Support {@link com.google.common.util.concurrent.Futures#transform(ListenableFuture, com.google.common.base.Function, Executor)} natively
     */
    <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper);

    default <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper, Executor executor)
    {
        return AsyncChains.map(this, mapper, executor);
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transform(ListenableFuture, com.google.common.base.Function, Executor)} natively
     */
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

    default AsyncChain<Void> accept(Consumer<? super V> action)
    {
        return map(r -> {
            action.accept(r);
            return null;
        });
    }

    default AsyncChain<Void> accept(Consumer<? super V> action, Executor executor)
    {
        return map(r -> {
            action.accept(r);
            return null;
        }, executor);
    }

    default AsyncChain<V> withExecutor(Executor e)
    {
        // since a chain runs as a sequence of callbacks, by adding a callback that moves to this executor any new actions
        // will be run on that desired executor.
        return map(a -> a, e);
    }

    /**
     * Support {@link com.google.common.util.concurrent.Futures#addCallback} natively
     */
    AsyncChain<V> addCallback(BiConsumer<? super V, Throwable> callback);

    /**
     * Adds a callback that only listens to the successful case, a failed chain will not trigger the callback
     */
    default AsyncChain<V> addCallback(Runnable runnable)
    {
        return addCallback(AsyncCallbacks.toCallback(runnable));
    }

    default AsyncChain<V> addCallback(BiConsumer<? super V, Throwable> callback, ExecutorService es)
    {
        return addCallback(AsyncCallbacks.inExecutorService(callback, es));
    }

    default AsyncChain<V> addCallback(Runnable runnable, ExecutorService es)
    {
        return addCallback(AsyncCallbacks.inExecutorService(runnable, es));
    }

    /**
     * Causes the chain to begin, starting all work required.  This method must be called exactly once, not calling will
     * not cause any work to start, and calling multiple times will be rejected.
     *
     * TODO (expected): Executor with cheap cancel() that cleansup task queue
     */
    @Nullable Cancellable begin(BiConsumer<? super V, Throwable> callback);

    default AsyncResult<V> beginAsResult()
    {
        return AsyncResults.forChain(this);
    }
}