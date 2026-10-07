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

package accord.local.durability;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import accord.utils.ThrowableSafety;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResults;

class ConcurrencyControl implements BiConsumer<Object, Throwable>
{
    final Queue<Pending<?>> pending = new ArrayDeque<>();
    int maxConcurrency;
    int inflight;

    public ConcurrencyControl(int maxConcurrency)
    {
        this.maxConcurrency = maxConcurrency;
    }

    static class Pending<V>
    {
        final AsyncResults.SettableByCallback<V> result = new AsyncResults.SettableByCallback<>();
        final Supplier<? extends AsyncChain<V>> supplier;

        Pending(Supplier<? extends AsyncChain<V>> supplier)
        {
            this.supplier = supplier;
        }

        boolean start(ConcurrencyControl concurrencyControl)
        {
            AsyncChain<V> chain;
            try
            {
                chain = supplier.get();
            }
            catch (Throwable t)
            {
                ThrowableSafety.safeFail(result, t);
                return false;
            }
            chain.invoke(result).begin(concurrencyControl);
            return true;
        }
    }

    @Override
    public void accept(Object o, Throwable throwable)
    {
        Pending<?> next;
        do
        {
            synchronized (this)
            {
                if (inflight > 0) --inflight;
                if (inflight >= maxConcurrency)
                    return;

                next = pending.poll();
                if (next == null)
                    return;
                ++inflight;
            }
        } while (!next.start(this));
    }

    public <V> AsyncChain<V> submit(Supplier<? extends AsyncChain<V>> supplier)
    {
        synchronized (this)
        {
            if (inflight >= maxConcurrency)
            {
                Pending<V> newPending = new Pending<>(supplier);
                pending.add(newPending);
                return newPending.result.chain();
            }
            ++inflight;
        }

        AsyncChain<V> chain;
        try
        {
            chain = supplier.get();
        }
        catch (Throwable t)
        {
            accept(null, t); // release our slot
            throw t;
        }
        return chain.invoke(this);
    }

    synchronized void setMaxConcurrency(int newMaxConcurrency)
    {
        this.maxConcurrency = newMaxConcurrency;
    }
}
