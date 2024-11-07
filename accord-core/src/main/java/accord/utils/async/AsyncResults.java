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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;

import accord.api.VisibleForImplementation;
import accord.utils.Invariants;

import static accord.utils.Invariants.createIllegalState;
import static accord.utils.Invariants.illegalState;

public class AsyncResults
{
    public static final AsyncResult SUCCESS_NULL = new Immediate<>(null);

    private AsyncResults() {}

    private static class Result<V>
    {
        final V value;
        final Throwable failure;

        public Result(V value, Throwable failure)
        {
            this.value = value;
            this.failure = failure;
        }
    }

    public static class AbstractResult<V> implements AsyncResult<V>
    {
        private static final AtomicReferenceFieldUpdater<AbstractResult, Object> STATE = AtomicReferenceFieldUpdater.newUpdater(AbstractResult.class, Object.class, "state");

        private volatile Object state;

        private static class Listener<V>
        {
            final BiConsumer<? super V, Throwable> callback;
            Listener<V> next;

            public Listener(BiConsumer<? super V, Throwable> callback)
            {
                this.callback = callback;
            }
        }

        private void notify(Listener<V> listener, Result<V> result)
        {
            Listener<V> reversed = null;
            Listener<V> tmp;
            while (listener != null)
            {
                tmp = listener;
                listener = listener.next;
                tmp.next = reversed;
                reversed = tmp;
            }
            listener = reversed;

            List<Throwable> failures = null;
            while (listener != null)
            {
                try
                {
                    listener.callback.accept(result.value, result.failure);
                }
                catch (Throwable t)
                {
                    if (failures == null)
                        failures = new ArrayList<>();
                    failures.add(t);
                }
                listener = listener.next;
            }
            if (failures != null)
            {
                IllegalStateException f = createIllegalState("Callbacks threw");
                failures.forEach(f::addSuppressed);
                throw f;
            }
        }

        boolean trySetResult(Result<V> result)
        {
            while (true)
            {
                Object current = state;
                if (current instanceof Result)
                    return false;
                Listener<V> listener = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, result))
                {
                    notify(listener, result);
                    return true;
                }
            }
        }

        protected boolean trySetResult(V result, Throwable failure)
        {
            return trySetResult(new Result<>(result, failure));
        }

        protected boolean trySuccess(V value)
        {
            return trySetResult(value, null);
        }
        protected boolean tryFailure(Throwable throwable)
        {
            return trySetResult(null, throwable);
        }

        private AsyncChain<V> newChain()
        {
            return new AsyncChains.Head<V>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super V, Throwable> callback)
                {
                    AbstractResult.this.addCallback(callback);
                    return null;
                }
            };
        }

        void setResult(V result, Throwable failure)
        {
            if (!trySetResult(result, failure))
            {
                IllegalStateException f = createIllegalState("Result has already been set on " + this);
                if (failure != null)
                    f.addSuppressed(failure);
                throw f;
            }
        }

        @Override
        public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
        {
            return newChain().map(mapper);
        }

        @Override
        public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
        {
            return newChain().flatMap(mapper);
        }

        @Override
        public AsyncChain<V> recover(Function<? super Throwable, ? extends AsyncChain<V>> mapper)
        {
            return newChain().recover(mapper);
        }

        @Override
        public AsyncResult<V> addCallback(BiConsumer<? super V, Throwable> callback)
        {
            Listener<V> listener = null;
            while (true)
            {
                Object current = state;
                if (current instanceof Result)
                {
                    Result<V> result = (Result<V>) current;
                    callback.accept(result.value, result.failure);
                    return this;
                }
                if (listener == null)
                    listener = new Listener<>(callback);

                listener.next = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, listener))
                    return this;
            }
        }

        @Override
        public boolean isDone()
        {
            return state instanceof Result;
        }

        @Override
        public boolean isSuccess()
        {
            Object current = state;
            return current instanceof Result && ((Result) current).failure == null;
        }

        private Result<V> getResult()
        {
            Object current = state;
            Invariants.checkState(current instanceof Result);
            return (Result<V>) current;
        }

        public V result()
        {
            Result<V> result = getResult();
            if (result.failure != null)
                throw new IllegalStateException("Result failed", result.failure);
            return result.value;
        }

        public Throwable failure()
        {
            Result<V> result = getResult();
            if (result.failure == null)
                illegalState("Result succeeded");
            return result.failure;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "{status=" + (isDone() ? isSuccess() ? "success" : "failure" : "pending") + "}";
        }
    }

    static class Chain<V> extends AbstractResult<V>
    {
        public Chain(AsyncChain<V> chain)
        {
            chain.begin(this::setResult);
        }
    }

    public static class SettableResult<V> extends AbstractResult<V> implements AsyncResult.Settable<V>
    {
        @Override
        public boolean trySuccess(V value)
        {
            return super.trySuccess(value);
        }

        @Override
        public boolean tryFailure(Throwable throwable)
        {
            return super.tryFailure(throwable);
        }
    }

    static class Immediate<V> implements AsyncResult<V>
    {
        private final V value;
        private final Throwable failure;

        Immediate(V value)
        {
            this.value = value;
            this.failure = null;
        }

        Immediate(Throwable failure)
        {
            this.value = null;
            this.failure = failure;
        }

        private AsyncChain<V> newChain()
        {
            return new AsyncChains.Head<V>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super V, Throwable> callback)
                {
                    AsyncResults.Immediate.this.addCallback(callback);
                    return null;
                }
            };
        }

        @Override
        public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
        {
            return newChain().map(mapper);
        }

        @Override
        public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
        {
            return newChain().flatMap(mapper);
        }

        @Override
        public AsyncChain<V> recover(Function<? super Throwable, ? extends AsyncChain<V>> mapper)
        {
            return newChain().recover(mapper);
        }

        @Override
        public AsyncResult<V> addCallback(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(value, failure);
            return this;
        }

        @Override
        public boolean isDone()
        {
            return true;
        }

        @Override
        public boolean isSuccess()
        {
            return failure == null;
        }

        @Override
        public String toString()
        {
            return "Immediate{" + (isSuccess() ? "success" : "failure") + "}";
        }
    }

    /**
     * Creates an AsyncResult for the given chain. This calls begin on the supplied chain
     */
    public static <V> AsyncResult<V> forChain(AsyncChain<V> chain)
    {
        return new Chain<>(chain);
    }

    public static <V> AsyncResult<V> success(V value)
    {
        if (value == null)
            return SUCCESS_NULL;

        return new Immediate<>(value);
    }

    public static <V> AsyncResult<V> failure(Throwable failure)
    {
        return new Immediate<>(failure);
    }

    public static <V> AsyncResult.Settable<V> settable()
    {
        return new SettableResult<>();
    }

    /**
     * An AsyncResult that also implements Runnable
     * @param <V>
     */
    @VisibleForImplementation
    public static class RunnableResult<V> extends AbstractResult<V> implements Runnable
    {
        protected final Callable<V> callable;

        public RunnableResult(Callable<V> callable)
        {
            this.callable = callable;
        }

        @Override
        public void run()
        {
            // There are two different type of exceptions: user function throws, listener throws.  To make sure this is clear,
            // make sure to catch the exception from the user function and set as failed, and let the listener exceptions bubble up.
            V call;
            try
            {
                call = callable.call();
            }
            catch (Throwable t)
            {
                trySetResult(null, t);
                return;
            }
            trySetResult(call, null);
        }
    }

    public static <V> RunnableResult<V> runnableResult(Callable<V> callable)
    {
        return new RunnableResult<>(callable);
    }

    public static RunnableResult<Void> runnableResult(Runnable runnable)
    {
        return new RunnableResult<>(() -> {
            runnable.run();
            return null;
        });
    }

}
