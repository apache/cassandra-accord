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

import accord.utils.Invariants;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

public class AsyncChainCombiner<I> extends AsyncChains.Head<I[]>
{
    private static final AtomicIntegerFieldUpdater<AsyncChainCombiner> REMAINING = AtomicIntegerFieldUpdater.newUpdater(AsyncChainCombiner.class, "remaining");
    private volatile Object state;
    private volatile BiConsumer<? super I[], Throwable> callback;
    private volatile int remaining;

    public AsyncChainCombiner(List<? extends AsyncChain<? extends I>> inputs)
    {
        Invariants.checkArgument(!inputs.isEmpty(), "No inputs defined");
        this.state = inputs;
    }

    List<AsyncChain<? extends I>> inputs()
    {
        Object current = state;
        Invariants.checkState(current instanceof List, "Expected state to be List but was %s", (current == null ? null : current.getClass()));
        return (List<AsyncChain<? extends I>>) current;
    }

    private I[] results()
    {
        Object current = state;
        Invariants.checkState(current instanceof Object[], "Expected state to be Object[] but was %s", (current == null ? null : current.getClass()));
        return (I[]) current;
    }

    private void callback(int idx, I result, Throwable throwable)
    {
        int current = remaining;
        if (current == 0)
            return;

        if (throwable != null && REMAINING.getAndSet(this, 0) > 0)
        {
            callback.accept(null, throwable);
            return;
        }

        I[] results = results();
        results[idx] = result;
        if (REMAINING.decrementAndGet(this) == 0)
            callback.accept(results, null);
    }

    private BiConsumer<I, Throwable> callbackFor(int idx)
    {
        return (result, failure) -> callback(idx, result, failure);
    }

    @Override
    protected void start(BiConsumer<? super I[], Throwable> callback)
    {
        List<? extends AsyncChain<? extends I>> chains = inputs();
        state = new Object[chains.size()];

        int size = chains.size();
        this.callback = callback;
        this.remaining = size;
        for (int i=0; i<size; i++)
            chains.get(i).begin(callbackFor(i));
    }
}
