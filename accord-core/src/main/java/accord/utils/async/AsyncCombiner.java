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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;

import accord.utils.Invariants;

public abstract class AsyncCombiner<A, I, O> extends AsyncChains.Head<O>
{
    public static abstract class ChainCombiner<I, O> extends AsyncCombiner<AsyncChain<? extends I>, I, O>
    {
        public ChainCombiner(List<? extends AsyncChain<? extends I>> inputs)
        {
            super(inputs);
        }

        @Override
        protected Cancellable begin(AsyncChain<? extends I> input, BiConsumer<I, Throwable> callback)
        {
            return input.begin(callback);
        }
    }

    public static abstract class ResultCombiner<I, O> extends AsyncCombiner<AsyncResult<? extends I>, I, O>
    {
        public ResultCombiner(List<? extends AsyncResult<? extends I>> inputs)
        {
            super(inputs);
        }

        @Override
        protected Cancellable begin(AsyncResult<? extends I> input, BiConsumer<I, Throwable> callback)
        {
            input.invoke(callback);
            return null;
        }
    }

    public static abstract class DebuggableResultCombiner<I, O> extends ResultCombiner<I, O>
    {
        AsyncResult<? extends I>[] waitingOn;
        public DebuggableResultCombiner(List<? extends AsyncResult<? extends I>> inputs)
        {
            super(inputs);
            waitingOn = inputs.toArray(new AsyncResult[0]);
        }

        @Override
        void callback(int idx, I success, Throwable failure)
        {
            if (failure != null) Arrays.fill(waitingOn, null);
            else waitingOn[idx] = null;
            super.callback(idx, success, failure);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder("[");
            for (AsyncResult<?> waiting : waitingOn)
            {
                if (waiting == null)
                    continue;

                if (sb.length() > 1)
                    sb.append(", ");
                sb.append(waiting);
            }
            if (sb.length() == 1)
                return "Done";
            sb.append("]");
            return sb.toString();
        }
    }

    private static final AtomicIntegerFieldUpdater<AsyncCombiner> REMAINING = AtomicIntegerFieldUpdater.newUpdater(AsyncCombiner.class, "remaining");
    private volatile Object state;
    private volatile BiConsumer<? super O, Throwable> callback;
    private volatile int remaining;

    public AsyncCombiner(List<? extends A> inputs)
    {
        this.state = inputs;
    }

    List<A> inputs()
    {
        Object current = state;
        Invariants.require(current instanceof List, "Expected state to be List but was %s", (current == null ? null : current.getClass()));
        return (List<A>) current;
    }

    private I[] results()
    {
        Object current = state;
        Invariants.require(current instanceof Object[], "Expected state to be Object[] but was %s", (current == null ? null : current.getClass()));
        return (I[]) current;
    }

    void callback(int idx, I success, Throwable failure)
    {
        int current = remaining;
        if (current <= 0)
            return;

        if (failure != null && REMAINING.getAndSet(this, 0) > 0)
        {
            callback.accept(null, failure);
            return;
        }

        I[] results = results();
        results[idx] = success;
        if (REMAINING.decrementAndGet(this) == 0)
            complete(callback, results);
    }

    private void complete(BiConsumer<? super O, Throwable> callback, I[] results)
    {
        O result;
        try { result = process(results); }
        catch (Throwable t) { callback.accept(null, t); return; }
        callback.accept(result, null);
    }

    BiConsumer<I, Throwable> callbackFor(int idx)
    {
        return (success, failure) -> callback(idx, success, failure);
    }

    abstract O process(I[] inputs);

    @Override
    protected Cancellable start(BiConsumer<? super O, Throwable> callback)
    {
        List<? extends A> chains = inputs();
        int size = chains.size();

        this.state = new Object[size];
        this.callback = callback;
        this.remaining = size;

        if (size == 0)
        {
            complete(callback, results());
            return null;
        }
        Cancellable cancellable = null;
        for (int i=0; i<size; i++)
        {
            Cancellable next = begin(chains.get(i), callbackFor(i));
            if (next != null)
            {
                Cancellable prev = cancellable;
                if (prev == null) cancellable = next;
                else cancellable = () -> { prev.cancel(); next.cancel(); };
            }
        }
        return cancellable;
    }

    protected abstract Cancellable begin(A input, BiConsumer<I, Throwable> callback);
}
