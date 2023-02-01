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

package accord.utils;

import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiFunction;

public class ReducingFuture<V> extends AsyncPromise<V>
{
    private static final AtomicIntegerFieldUpdater<ReducingFuture> PENDING_UPDATER = AtomicIntegerFieldUpdater.newUpdater(ReducingFuture.class, "pending");
    private final List<? extends Future<V>> futures;
    private final Reduce<V> reducer;
    private volatile int pending;

    private ReducingFuture(List<? extends Future<V>> futures, Reduce<V> reducer)
    {
        this.futures = futures;
        this.reducer = reducer;
        this.pending = futures.size();
        futures.forEach(f -> f.addListener(this::operationComplete));
    }

    private <F extends io.netty.util.concurrent.Future<?>> void operationComplete(F future) throws Exception
    {
        if (isDone())
            return;

        if (!future.isSuccess())
        {
            tryFailure(future.cause());
        }
        else if (PENDING_UPDATER.decrementAndGet(this) == 0)
        {
            V result = futures.get(0).getNow();
            for (int i=1, mi=futures.size(); i<mi; i++)
                result = reducer.reduce(result, futures.get(i).getNow());

            trySuccess(result);
        }
    }

    public static <T> Future<T> reduce(List<? extends Future<T>> futures, Reduce<T> reducer)
    {
        Preconditions.checkArgument(!futures.isEmpty(), "future list is empty");

        if (futures.size() == 1)
            return futures.get(0);

        return new ReducingFuture<>(futures, reducer);
    }
}
