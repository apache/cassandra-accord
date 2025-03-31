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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.VisibleForImplementation;
import accord.utils.Invariants;

import static accord.utils.Invariants.createIllegalState;
import static accord.utils.async.AsyncChains.DEBUG;

public class AsyncResults
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncResults.class);
    public static final AsyncResult SUCCESS_NULL = new Immediate<>((Object) null);

    private AsyncResults() {}

    public static class AbstractResult<V> implements AsyncResult<V>
    {
        private static final AtomicReferenceFieldUpdater<AbstractResult, Object> STATE = AtomicReferenceFieldUpdater.newUpdater(AbstractResult.class, Object.class, "state");

        private final Exception asyncChainRoot = DEBUG ? new Exception("Async stack trace injection") : null;

        static final class FailureHolder
        {
            final Throwable cause;
            FailureHolder(Throwable cause)
            {
                this.cause = cause;
            }
        }

        @Override
        public Throwable asyncChainRoot()
        {
            return asyncChainRoot;
        }

        private static final class Listener<V>
        {
            final BiConsumer<? super V, Throwable> callback;
            Listener<V> next;

            public Listener(BiConsumer<? super V, Throwable> callback)
            {
                this.callback = callback;
            }
        }

        private static final Object INIT = new Listener<>(null);
        private volatile Object state = INIT;

        private void notify(Listener<V> listener, V success, Throwable failure)
        {
            Listener<V> reversed = null;
            Listener<V> tmp;
            while (listener != INIT)
            {
                tmp = listener;
                listener = listener.next;
                tmp.next = reversed;
                reversed = tmp;
            }
            listener = reversed;

            while (listener != null)
            {
                try
                {
                    listener.callback.accept(success, failure);
                }
                catch (Throwable t)
                {
                    try
                    {
                        Thread thread = Thread.currentThread();
                        thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                    }
                    catch (Throwable t2)
                    {
                        t2.addSuppressed(t);
                        logger.error("Unexpected exception thrown by UncaughtExceptionHandler", t2);
                    }
                }
                listener = listener.next;
            }
        }

        protected final boolean trySetResult(V success, Throwable failure)
        {
            Invariants.require(failure == null || success == null);
            Object result = failure == null ? success : new FailureHolder(failure);
            while (true)
            {
                Object current = state;
                if (!(current instanceof Listener))
                    return false;
                Listener<V> listener = (Listener<V>) current;
                if (STATE.compareAndSet(this, current, result))
                {
                    notify(listener, success, failure);
                    return true;
                }
            }
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
            return new AsyncChains.Head<>()
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
                if (!(current instanceof Listener<?>))
                {
                    V success = current instanceof FailureHolder ? null : (V)current;
                    Throwable failure = current instanceof FailureHolder ? ((FailureHolder)current).cause : null;
                    callback.accept(success, failure);
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
            return !(state instanceof Listener);
        }

        @Override
        public boolean isSuccess()
        {
            Object current = state;
            return !(current instanceof Listener)  && !(current instanceof FailureHolder);
        }

        public V result()
        {
            Object current = state;
            if (current instanceof FailureHolder)
            {
                FailureHolder failure = (FailureHolder) current;
                throw new IllegalStateException("Result was failure, or not yet finished", failure.cause);
            }
            return (V)current;
        }

        public Throwable failure()
        {
            Object current = state;
            Invariants.require(current instanceof FailureHolder, "Result was not failure");
            return ((FailureHolder)current).cause;
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
        final Exception trace;

        public SettableResult()
        {
            super();
            if (DEBUG)
            {
                this.trace = new Exception();
            }
            else
                this.trace = null;
        }

        @Override
        public Throwable asyncChainRoot()
        {
            return trace;
        }

        @Override
        public boolean trySuccess(V value)
        {
            return super.trySuccess(value);
        }

        @Override
        public boolean tryFailure(Throwable throwable)
        {
            if (DEBUG)
                throwable.addSuppressed(trace);
            return super.tryFailure(throwable);
        }
    }

    public static class SettableByCallback<V> extends SettableResult<V> implements BiConsumer<V, Throwable>
    {
        @Override
        public void accept(V v, Throwable throwable)
        {
            if (throwable == null) trySuccess(v);
            else tryFailure(throwable);
        }
    }

    static class Immediate<V> implements AsyncResult<V>
    {
        private final V value;
        private final Throwable failure;
        private final Exception trace = DEBUG ? new Exception("Async stack trace injection") : null;

        Immediate(V value)
        {
            this.value = value;
            this.failure = null;
        }

        Immediate(Throwable failure)
        {
            this.value = null;
            this.failure = failure;
            if (asyncChainRoot() != null)
                failure.initCause(asyncChainRoot());
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
        public Throwable asyncChainRoot()
        {
            return trace;
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
