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

/**
 * Handle for async computations that supports multiple listeners and registering
 * listeners after the computation has started
 */
public interface AsyncResult<V> extends AsyncChain<V>
{
    AsyncResult<V> addCallback(BiConsumer<? super V, Throwable> callback);

    default AsyncResult<V> addCallback(Runnable runnable)
    {
        return addCallback((unused, failure) -> {
            if (failure == null) runnable.run();
            else throw new RuntimeException(failure);
        });
    }

    default AsyncResult<V> addCallback(Runnable runnable, Executor executor)
    {
        addCallback(AsyncCallbacks.inExecutor(runnable, executor));
        return this;
    }

    boolean isDone();
    boolean isSuccess();

    default AsyncResult<V> addCallback(BiConsumer<? super V, Throwable> callback, Executor executor)
    {
        addCallback(AsyncCallbacks.inExecutor(callback, executor));
        return this;
    }

    default AsyncResult<V> addListener(Runnable runnable)
    {
        addCallback(runnable);
        return this;
    }

    default AsyncResult<V> addListener(Runnable runnable, Executor executor)
    {
        addCallback(runnable, executor);
        return this;
    }

    @Override
    default void begin(BiConsumer<? super V, Throwable> callback)
    {
        addCallback(callback);
    }

    default AsyncResult<V> beginAsResult()
    {
        return this;
    }

    interface Settable<V> extends AsyncResult<V>
    {
        boolean trySuccess(V value);

        default void setSuccess(V value)
        {
            if (!trySuccess(value))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        boolean tryFailure(Throwable throwable);

        default void setFailure(Throwable throwable)
        {
            if (!tryFailure(throwable))
                throw new IllegalStateException("Result has already been set on " + this);
        }

        default BiConsumer<V, Throwable> settingCallback()
        {
            return (result, throwable) -> {

                if (throwable == null)
                    trySuccess(result);
                else
                    tryFailure(throwable);
            };
        }
    }
}
