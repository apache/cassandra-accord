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
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.util.concurrent.ListenableFuture;

public interface AsyncChain<V>
{
    /**
     * Support {@link com.google.common.util.concurrent.Futures#transform(ListenableFuture, com.google.common.base.Function, Executor)} natively
     */
    <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper);

    /**
     * Support {@link com.google.common.util.concurrent.Futures#transform(ListenableFuture, com.google.common.base.Function, Executor)} natively
     */
    <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper);

    /**
     * Support {@link com.google.common.util.concurrent.Futures#addCallback} natively
     */
    AsyncChain<V> addCallback(BiConsumer<? super V, Throwable> callback);

    void begin(BiConsumer<? super V, Throwable> callback);

    default void begin(Runnable runnable)
    {
        begin((unused, failure) -> {
            if (failure == null) runnable.run();
            else throw new RuntimeException(failure);
        });
    }

    default AsyncResult<V> beginAsResult()
    {
        return AsyncResults.forChain(this);
    }
}