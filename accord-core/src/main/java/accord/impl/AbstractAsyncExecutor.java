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

package accord.impl;

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import accord.api.AsyncExecutor;
import accord.utils.async.AsyncCallbacks;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

// we keep this as a separate interface for simulator compatibility,
// else we use the wrong class loader's AsyncChain.Head.
public interface AbstractAsyncExecutor extends AsyncExecutor
{
    @Override
    default AsyncChain<Void> chain(Runnable run)
    {
        return AsyncChains.chain(this, run);
    }

    @Override
    default AsyncChain<Void> continuationChain(Runnable run)
    {
        return AsyncChains.continuationChain(this, run);
    }

    @Override
    default <V> AsyncChain<V> chain(Callable<V> call)
    {
        return AsyncChains.chain(this, call);
    }

    @Override
    default <V> AsyncChain<V> flatChain(Callable<? extends AsyncChain<V>> call)
    {
        return AsyncChains.flatChain(this, call);
    }

    default Cancellable execute(Runnable run, BiConsumer<? super Void, Throwable> callback)
    {
        return AsyncCallbacks.execute(this, new AsyncCallbacks.RunAndCallback(run, callback));
    }

    default <V> Cancellable execute(Callable<V> call, BiConsumer<? super V, Throwable> callback)
    {
        return AsyncCallbacks.execute(this, new AsyncCallbacks.CallAndCallback<>(call, callback));
    }

    default <V> Cancellable flatExecute(Callable<? extends AsyncChain<V>> call, BiConsumer<? super V, Throwable> callback)
    {
        return AsyncCallbacks.execute(this, new AsyncCallbacks.FlatCallAndCallback<>(call, callback));
    }
}
