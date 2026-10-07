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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Invariants;
import accord.utils.Reduce;
import accord.utils.ThrowableSafety;

import static accord.utils.Invariants.createIllegalState;

public class AsyncResults
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncResults.class);
    public static final AsyncResult SUCCESS_NULL = new ImmediateSuccess<>(null);

    private AsyncResults() {}

    public static class AbstractResult<V> implements AsyncResult<V>
    {
        private static final AtomicReferenceFieldUpdater<AbstractResult, Object> STATE = AtomicReferenceFieldUpdater.newUpdater(AbstractResult.class, Object.class, "state");

        static final class FailureHolder
        {
            final Throwable cause;
            FailureHolder(Throwable cause)
            {
                this.cause = cause;
            }
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
                ThrowableSafety.safeInvoke(listener.callback, success, failure);
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
        public AsyncResult<V> invoke(BiConsumer<? super V, Throwable> callback)
        {
            Listener<V> listener = null;
            while (true)
            {
                Object current = state;
                if (!(current instanceof Listener<?>))
                {
                    V success = current instanceof FailureHolder ? null : (V)current;
                    Throwable failure = current instanceof FailureHolder ? ((FailureHolder)current).cause : null;
                    ThrowableSafety.safeInvoke(callback, success, failure);
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
            return getClass().getSimpleName() + statusString();
        }

        protected String statusString()
        {
            return "{status=" + (isDone() ? isSuccess() ? "success" : "failure" : "pending") + "}";
        }
    }

    public static class Chain<V> extends AbstractResult<V> implements BiConsumer<V, Throwable>
    {
        private AsyncChain<V> chain;
        public Chain(AsyncChain<V> chain)
        {
            this.chain = chain;
            chain.begin(this::setResult);
        }

        @Override
        void setResult(V result, Throwable failure)
        {
            chain = null;
            super.setResult(result, failure);
        }

        @Override
        public void accept(V success, Throwable fail)
        {
            setResult(success, fail);
        }

        @Override
        public String toString()
        {
            AsyncChain<V> chain = this.chain;
            if (chain != null)
                return "Waiting On: " + chain;
            return super.toString();
        }
    }

    public static class CancellableChain<V> extends AbstractResult<V> implements Cancellable, BiConsumer<V, Throwable>, CancellableAsyncResult<V>
    {
        private AsyncChain<V> chain;
        private Cancellable cancel;

        public CancellableChain(AsyncChain<V> chain)
        {
            this.chain = chain;
            this.cancel = chain.begin(this);
        }

        @Override
        void setResult(V result, Throwable failure)
        {
            chain = null;
            cancel = null;
            super.setResult(result, failure);
        }

        @Override
        public void cancel()
        {
            Cancellable cancel = this.cancel;
            if (cancel == null)
                return;
            this.chain = null;
            this.cancel = null;
            cancel.cancel();
        }

        @Override
        public void accept(V success, Throwable fail)
        {
            setResult(success, fail);
        }

        @Override
        public String toString()
        {
            AsyncChain<V> chain = this.chain;
            if (chain != null)
                return "Waiting On: " + chain;
            return super.toString();
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

    public static class SettableByCallback<V> extends SettableResult<V> implements BiConsumer<V, Throwable>
    {
        @Override
        public void accept(V v, Throwable throwable)
        {
            if (throwable == null) trySuccess(v);
            else tryFailure(throwable);
        }
    }

    public static class SettableWithDescription<V> extends SettableByCallback<V>
    {
        final String description;

        public SettableWithDescription(String description)
        {
            this.description = description;
        }

        @Override
        public String toString()
        {
            return description + statusString();
        }
    }

    public static class CountingResult extends AbstractResult<Void>
    {
        private volatile int count;
        private static final AtomicIntegerFieldUpdater<CountingResult> countUpdater = AtomicIntegerFieldUpdater.newUpdater(CountingResult.class, "count");

        public CountingResult(int initialCount)
        {
            count = initialCount;
            if (initialCount == 0)
                trySuccess(null);
        }

        public void decrement()
        {
            if (0 == countUpdater.decrementAndGet(this))
                trySuccess(null);
        }

        public void increment()
        {
            if (0 == countUpdater.getAndIncrement(this))
                throw new IllegalStateException("Count was already zero");
        }

        @Override
        public boolean tryFailure(Throwable throwable)
        {
            return super.tryFailure(throwable);
        }
    }

    public static abstract class AbstractImmediate<V> implements AsyncResult<V>
    {
        @Override
        public AsyncChain<V> chain()
        {
            return new AsyncChains.Head<>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super V, Throwable> callback)
                {
                    AbstractImmediate.this.invoke(callback);
                    return null;
                }
            };
        }

        @Override
        public boolean isDone()
        {
            return true;
        }
    }

    public static class ImmediateSuccess<V> extends AbstractImmediate<V>
    {
        private final V success;

        ImmediateSuccess(V success)
        {
            this.success = success;
        }

        @Override
        public AsyncResult<V> invoke(BiConsumer<? super V, Throwable> callback)
        {
            ThrowableSafety.safeSuccess(callback, success);
            return this;
        }

        @Override
        public boolean isSuccess()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "Success" + (success != null ? "{" + success + '}' : "");
        }
    }

    public static class ImmediateFailure<V> extends AbstractImmediate<V>
    {
        private final Throwable failure;

        protected ImmediateFailure(Throwable failure)
        {
            this.failure = failure;
        }

        @Override
        public AsyncResult<V> invoke(BiConsumer<? super V, Throwable> callback)
        {
            ThrowableSafety.safeFail(callback, failure);
            return this;
        }

        @Override
        public boolean isSuccess()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "Failure{" + failure + "}";
        }
    }

    /**
     * Creates an AsyncResult for the given chain. This calls begin on the supplied chain
     */
    public static <V> AsyncResult<V> begin(AsyncChain<V> chain)
    {
        return new Chain<>(chain);
    }

    /**
     * Creates an AsyncResult for the given chain. This calls begin on the supplied chain
     */
    public static <V> CancellableAsyncResult<V> beginCancellable(AsyncChain<V> chain)
    {
        return new CancellableChain<>(chain);
    }

    public static <V> AsyncResult<V> success(V value)
    {
        if (value == null)
            return SUCCESS_NULL;

        return new ImmediateSuccess<>(value);
    }

    public static <V> AsyncResult<V> failure(Throwable failure)
    {
        return new ImmediateFailure<>(failure);
    }

    public static <V> AsyncResult.Settable<V> settable()
    {
        return new SettableResult<>();
    }

    /**
     * An AsyncResult that also implements Runnable
     * @param <V>
     */
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

    public static <V> AsyncResult<List<V>> allOf(List<? extends AsyncResult<? extends V>> results)
    {
        return new AsyncCombiner.ResultCombiner<V, List<V>>(results) {

            @Override
            List<V> process(V[] inputs)
            {
                return Arrays.asList(inputs);
            }
        }.beginAsResult();
    }

    public static <V> AsyncResult<V> reduce(List<? extends AsyncResult<? extends V>> results, Reduce<V, V> reducer)
    {
        if (results.size() == 1)
            return (AsyncResult<V>) results.get(0);

        return new AsyncCombiner.ResultCombiner<V, V>(results)
        {
            @Override
            V process(V[] inputs)
            {
                return reduce(inputs, reducer);
            }
        }.beginAsResult();
    }

    public static <V> AsyncResult<V> debuggableReduce(List<? extends AsyncResult<? extends V>> results, Reduce<V, V> reducer)
    {
        if (results.size() == 1)
            return (AsyncResult<V>) results.get(0);

        return new AsyncCombiner.DebuggableResultCombiner<V, V>(results)
        {
            @Override
            V process(V[] inputs)
            {
                return reduce(inputs, reducer);
            }
        }.beginAsResult();
    }

    private static <V> V reduce(V[] inputs, Reduce<V, V> reducer)
    {
        V result = inputs[0];
        for (int i = 1; i < inputs.length ; ++i)
            result = reducer.reduce(result, inputs[i]);
        return result;
    }


}
