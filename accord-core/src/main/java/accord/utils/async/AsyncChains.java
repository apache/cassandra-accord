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
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.VisibleForImplementation;
import accord.utils.Invariants;

import static accord.utils.Invariants.illegalState;

public abstract class AsyncChains<V> implements AsyncChain<V>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncChains.class);

    static class Immediate<V> implements AsyncChain<V>
    {
        static class FailureHolder
        {
            final Throwable cause;
            FailureHolder(Throwable cause)
            {
                this.cause = cause;
            }
        }

        final private Object value;
        private Immediate(V success) { this.value = success; }
        private Immediate(Throwable failure) { this.value = new FailureHolder(failure); }

        @Override
        public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
        {
            if (isFailed())
                return (AsyncChain<T>) this;
            try
            {
                return new Immediate<>(mapper.apply((V) value));
            }
            catch (Throwable t)
            {
                return new Immediate<>(t);
            }
        }

        @Override
        public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
        {
            if (isFailed())
                return (AsyncChain<T>) this;
            try
            {
                return mapper.apply((V) value);
            }
            catch (Throwable t)
            {
                return new Immediate<>(t);
            }
        }

        @Override
        public AsyncChain<V> recover(Function<? super Throwable, ? extends AsyncChain<V>> mapper)
        {
            if (!isFailed())
                return this;

            Throwable cause = ((FailureHolder) value).cause;
            try
            {
                AsyncChain<V> recover = mapper.apply(cause);
                return recover == null ? this : recover;
            }
            catch (Throwable t)
            {
                try
                {
                    cause.addSuppressed(t);
                }
                catch (Throwable ignore)
                {
                    // can't add as suppressed...
                }
                return new Immediate<>(cause);
            }
        }

        private boolean isFailed()
        {
            return value != null && value.getClass() == FailureHolder.class;
        }

        @Override
        public AsyncChain<V> addCallback(BiConsumer<? super V, Throwable> callback)
        {
            if (value == null || value.getClass() != FailureHolder.class)
                callback.accept((V) value, null);
            else
                callback.accept(null, ((FailureHolder)value).cause);
            return this;
        }

        @Override
        public void begin(BiConsumer<? super V, Throwable> callback)
        {
            addCallback(callback);
        }
    }

    public abstract static class Head<V> extends AsyncChains<V> implements BiConsumer<V, Throwable>
    {
        protected Head()
        {
            super(null);
            next = this;
        }

        protected abstract void start(BiConsumer<? super V, Throwable> callback);

        @Override
        public final void begin(BiConsumer<? super V, Throwable> callback)
        {
            Invariants.checkArgument(next != null);
            next = null;
            start(callback);
        }

        void begin()
        {
            Invariants.checkArgument(next != null);
            BiConsumer<? super V, Throwable> next = this.next;
            this.next = null;
            start(next);
        }

        @Override
        public void accept(V v, Throwable throwable)
        {
            // we implement here just to simplify logic a little
            throw new UnsupportedOperationException();
        }
    }

    static abstract class Link<I, O> extends AsyncChains<O> implements BiConsumer<I, Throwable>
    {
        protected Link(Head<?> head)
        {
            super(head);
        }

        @Override
        public void begin(BiConsumer<? super O, Throwable> callback)
        {
            Invariants.checkArgument(!(callback instanceof AsyncChains.Head));
            checkNextIsHead();
            Head<?> head = (Head<?>) next;
            next = callback;
            head.begin();
        }
    }

    public static abstract class Map<I, O> extends Link<I, O> implements Function<I, O>
    {
        Map(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable != null) next.accept(null, throwable);
            else
            {
                O update;
                try
                {
                    update = apply(i);
                }
                catch (Throwable t)
                {
                    next.accept(null, t);
                    return;
                }
                next.accept(update, null);
            }
        }
    }

    static class EncapsulatedMap<I, O> extends Map<I, O>
    {
        final Function<? super I, ? extends O> map;

        EncapsulatedMap(Head<?> head, Function<? super I, ? extends O> map)
        {
            super(head);
            this.map = map;
        }

        @Override
        public O apply(I i)
        {
            return map.apply(i);
        }
    }

    public static abstract class FlatMap<I, O> extends Link<I, O> implements Function<I, AsyncChain<O>>
    {
        FlatMap(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable != null) next.accept(null, throwable);
            else apply(i).begin(next);
        }
    }

    static class EncapsulatedFlatMap<I, O> extends FlatMap<I, O>
    {
        final Function<? super I, ? extends AsyncChain<O>> map;

        EncapsulatedFlatMap(Head<?> head, Function<? super I, ? extends AsyncChain<O>> map)
        {
            super(head);
            this.map = map;
        }

        @Override
        public AsyncChain<O> apply(I i)
        {
            try
            {
                return map.apply(i);
            }
            catch (Throwable t)
            {
                return AsyncChains.failure(t);
            }
        }
    }

    public static abstract class Recover<I> extends Link<I, I> implements Function<Throwable, AsyncChain<I>>
    {
        Recover(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable == null)
            {
                next.accept(i, null);
                return;
            }
            AsyncChain<I> recover = apply(throwable);
            if (recover == null) next.accept(null, throwable);
            else                 recover.begin(next);
        }
    }

    static class EncapsulatedRecover<I> extends Recover<I>
    {
        private final Function<? super Throwable, ? extends AsyncChain<I>> map;

        public EncapsulatedRecover(Head<?> head, Function<? super Throwable, ? extends AsyncChain<I>> function)
        {
            super(head);
            this.map = function;
        }

        @Override
        public AsyncChain<I> apply(Throwable throwable)
        {
            try
            {
                return map.apply(throwable);
            }
            catch (Throwable t)
            {
                return AsyncChains.failure(t);
            }
        }
    }

    // if extending Callback, be sure to invoke super.accept()
    static class Callback<I> extends Link<I, I>
    {
        Callback(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            next.accept(i, throwable);
        }
    }

    static class EncapsulatedCallback<I> extends Callback<I>
    {
        final BiConsumer<? super I, Throwable> callback;

        EncapsulatedCallback(Head<?> head, BiConsumer<? super I, Throwable> callback)
        {
            super(head);
            this.callback = callback;
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            super.accept(i, throwable);
            callback.accept(i, throwable);
        }
    }

    private static class DetectLeak extends AsyncChains.Head<Void>
    {
        private final AtomicBoolean called = new AtomicBoolean(false);
        private final Throwable caller = new IllegalStateException("AsyncChain.begin not called");
        private final Consumer<Throwable> onLeak;
        private final Runnable onCall;

        private DetectLeak(Consumer<Throwable> onLeak, Runnable onCall)
        {
            this.onLeak = Objects.requireNonNull(onLeak);
            this.onCall = Objects.requireNonNull(onCall);
        }

        @Override
        protected void start(BiConsumer<? super Void, Throwable> callback)
        {
            called.set(true);
            onCall.run();
            callback.accept(null, null);
        }

        @Override
        protected void finalize()
        {
            if (!called.get())
                onLeak.accept(caller);
        }
    }

    @VisibleForTesting
    static class AccumulatingReducerAsyncChain<V> extends AsyncChains.Head<V>
    {
        private final BiFunction<? super V, ? super V, ? extends V> reducer;
        private final AsyncChainCombiner<V> chain;

        private AccumulatingReducerAsyncChain(AsyncChain<V> accum, AsyncChain<V> add, BiFunction<? super V, ? super V, ? extends V> reducer)
        {
            List<AsyncChain<V>> list = new ArrayList<>(2);
            list.add(accum);
            list.add(add);
            this.chain = new AsyncChainCombiner<>(list);
            this.reducer = reducer;
        }

        private static <V> boolean match(AsyncChain<V> accum, BiFunction<? super V, ? super V, ? extends V> reducer)
        {
            return accum instanceof AccumulatingReducerAsyncChain && ((AccumulatingReducerAsyncChain<?>) accum).reducer == reducer;
        }

        private void add(AsyncChain<V> a)
        {
            chain.inputs().add(a);
        }

        @VisibleForTesting
        int size()
        {
            return chain.inputs().size();
        }

        @Override
        protected void start(BiConsumer<? super V, Throwable> callback)
        {
            chain.map(r -> reduceArray(r, reducer)).begin(callback);
        }
    }

    // either the thing we start, or the thing we do in follow-up
    BiConsumer<? super V, Throwable> next;
    AsyncChains(Head<?> head)
    {
        this.next = (BiConsumer) head;
    }

    @Override
    public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
    {
        return add(EncapsulatedMap::new, mapper);
    }

    @Override
    public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
    {
        return add(EncapsulatedFlatMap::new, mapper);
    }

    @Override
    public AsyncChain<V> recover(Function<? super Throwable, ? extends AsyncChain<V>> mapper)
    {
        return add(EncapsulatedRecover::new, mapper);
    }

    @Override
    public AsyncChain<V> addCallback(BiConsumer<? super V, Throwable> callback)
    {
        return add(EncapsulatedCallback::new, callback);
    }

    // can be used by transformations that want efficiency, and can directly extend Link, FlatMap or Callback
    // (or perhaps some additional helper implementations that permit us to simply implement apply for Map and FlatMap)
    <O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> add(Function<Head<?>, T> factory)
    {
        checkNextIsHead();
        Head<?> head = (Head<?>) next;
        T result = factory.apply(head);
        next = result;
        return result;
    }

    <P, O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> add(BiFunction<Head<?>, P, T> factory, P param)
    {
        checkNextIsHead();
        Head<?> head = (Head<?>) next;
        T result = factory.apply(head, param);
        next = result;
        return result;
    }

    protected void checkNextIsHead()
    {
        Invariants.checkState(next != null, "Begin was called multiple times");
        Invariants.checkState(next instanceof Head<?>, "Next is not an instance of AsyncChains.Head (it is %s); was map/flatMap called on the same object multiple times?", next.getClass());
    }

    public static AsyncChain<?> detectLeak(Consumer<Throwable> onLeak, Runnable onCall)
    {
        return new DetectLeak(onLeak, onCall);
    }

    private static <V> Runnable encapsulate(Callable<V> callable, BiConsumer<? super V, Throwable> receiver)
    {
        return () -> {
            try
            {
                V result = callable.call();
                receiver.accept(result, null);
            }
            catch (Throwable t)
            {
                logger.debug("AsyncChain Callable threw an Exception", t);
                receiver.accept(null, t);
            }
        };
    }

    private static Runnable encapsulate(Runnable runnable, BiConsumer<? super Void, Throwable> receiver)
    {
        return () -> {
            try
            {
                runnable.run();
                receiver.accept(null, null);
            }
            catch (Throwable t)
            {
                logger.debug("AsyncChain Runnable threw an Exception", t);
                receiver.accept(null, t);
            }
        };
    }

    public static <V> AsyncChain<V> success(V success)
    {
        return new Immediate<>(success);
    }

    public static <V> AsyncChain<V> failure(Throwable failure)
    {
        return new Immediate<>(failure);
    }

    public static <V, T> AsyncChain<T> map(AsyncChain<V> chain, Function<? super V, ? extends T> mapper, Executor executor)
    {
        return chain.flatMap(v -> new Head<T>()
        {
            @Override
            protected void start(BiConsumer<? super T, Throwable> callback)
            {
                try
                {
                    executor.execute(() -> {
                        T value;
                        try
                        {
                            value = mapper.apply(v);
                        }
                        catch (Throwable t)
                        {
                            callback.accept(null, t);
                            return;
                        }
                        callback.accept(value, null);
                    });
                }
                catch (Throwable t)
                {
                    // TODO (low priority, correctness): If the executor is shutdown then the callback may run in an unexpected thread, which may not be thread safe
                    callback.accept(null, t);
                }
            }
        });
    }

    public static <V, T> AsyncChain<T> flatMap(AsyncChain<V> chain, Function<? super V, ? extends AsyncChain<T>> mapper, Executor executor)
    {
        return chain.flatMap(v -> new Head<T>()
        {
            @Override
            protected void start(BiConsumer<? super T, Throwable> callback)
            {
                try
                {
                    executor.execute(() -> {
                        try
                        {
                            mapper.apply(v).begin(callback);
                        }
                        catch (Throwable t)
                        {
                            callback.accept(null, t);
                        }
                    });
                }
                catch (Throwable t)
                {
                    // TODO (low priority, correctness): If the executor is shutdown then the callback may run in an unexpected thread, which may not be thread safe
                    callback.accept(null, t);
                }
            }
        });
    }

    public static <V> AsyncChain<V> ofCallable(Executor executor, Callable<V> callable)
    {
        return new Head<V>()
        {
            @Override
            protected void start(BiConsumer<? super V, Throwable> callback)
            {
                try
                {
                    executor.execute(encapsulate(callable, callback));
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                }
            }
        };
    }

    public static AsyncChain<Void> ofRunnable(Executor executor, Runnable runnable)
    {
        return new Head<Void>()
        {
            @Override
            protected void start(BiConsumer<? super Void, Throwable> callback)
            {
                try
                {
                    executor.execute(AsyncChains.encapsulate(runnable, callback));
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                }
            }
        };
    }

    @VisibleForImplementation
    public static AsyncChain<Void> ofRunnables(Executor executor, Iterable<? extends Runnable> runnables)
    {
        return ofRunnable(executor, () -> {
            Throwable failure = null;
            for (Runnable runnable : runnables)
            {
                try
                {
                    runnable.run();
                }
                catch (Throwable t)
                {
                    if (failure == null)
                        failure = t;
                    else
                        failure.addSuppressed(t);
                }
            }
            if (failure != null)
                throw new RuntimeException(failure);
        });
    }

    public static <V> AsyncChain<V[]> allOf(List<? extends AsyncChain<? extends V>> chains)
    {
        return new AsyncChainCombiner<>(chains);
    }

    public static <V> AsyncChain<List<V>> all(List<? extends AsyncChain<? extends V>> chains)
    {
        return new AsyncChainCombiner<>(chains).map(Lists::newArrayList);
    }

    public static <V> AsyncChain<V> reduce(List<? extends AsyncChain<? extends V>> chains, BiFunction<? super V, ? super V, ? extends V> reducer)
    {
        if (chains.size() == 1)
            return (AsyncChain<V>) chains.get(0);
        return allOf(chains).map(r -> reduceArray(r, reducer));
    }

    private static <V> V reduceArray(V[] results, BiFunction<? super V, ? super V, ? extends V> reducer)
    {
        V result = results[0];
        for (int i=1; i< results.length; i++)
            result = reducer.apply(result, results[i]);
        return result;
    }

    /**
     * Special variant of {@link #reduce(List, BiFunction)} that returns a mutable chain, where new chains can be appended as long as the returned chain has not started.  The target use case are for patterns such as the following
     * <p/>
     * <pre>{@code
     * BiFunction<? super V, ? super V, ? extends V> reducer = ...;
     * AsyncChain<V> chain = null;
     * for (...)
     * {
     *   AsyncChain<V> next = ...;
     *   chain = chain == null ? next : reduce(chain, next, reducer);
     * }
     * }</pre>
     */
    public static <V> AsyncChain<V> reduce(AsyncChain<V> accum, AsyncChain<V> add, BiFunction<? super V, ? super V, ? extends V> reducer)
    {
        if (AccumulatingReducerAsyncChain.match(accum, reducer))
        {
            ((AccumulatingReducerAsyncChain<V>) accum).add(add);
            return accum;
        }
        return new AccumulatingReducerAsyncChain<>(accum, add, reducer);
    }

    public static <A, B> AsyncChain<B> reduce(List<? extends AsyncChain<? extends A>> chains, B identity, BiFunction<B, ? super A, B> reducer)
    {
        switch (chains.size())
        {
            case 0: return AsyncChains.success(identity);
            case 1: return chains.get(0).map(a -> reducer.apply(identity, a));
        }
        return allOf(chains).map(results -> {
            B result = identity;
            for (A r : results)
                result = reducer.apply(result, r);
            return result;
        });
    }

    public static <V> V getBlocking(AsyncChain<V> chain) throws InterruptedException, ExecutionException
    {
        try
        {
            return getBlocking(chain, 0, TimeUnit.DAYS);
        }
        catch (TimeoutException e)
        {
            throw illegalState("Should not throw timeout exception e");
        }
    }

    public static <V> V getBlockingAndRethrow(AsyncChain<V> chain)
    {
        class Result
        {
            final V result;
            final Throwable failure;

            public Result(V result, Throwable failure)
            {
                this.result = result;
                this.failure = failure;
            }
        }

        AtomicReference<Result> callbackResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        chain.begin((result, failure) -> {
            callbackResult.set(new Result(result, failure));
            latch.countDown();
        });

        try
        {
            latch.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }

        Result result = callbackResult.get();
        if (result.failure == null) return result.result;
        else throw new RuntimeException(result.failure);
    }

    public static <V> V getBlocking(AsyncChain<V> chain, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException, ExecutionException
    {
        class Result
        {
            final V result;
            final Throwable failure;

            public Result(V result, Throwable failure)
            {
                this.result = result;
                this.failure = failure;
            }
        }

        AtomicReference<Result> callbackResult = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        chain.begin((result, failure) -> {
            callbackResult.set(new Result(result, failure));
            latch.countDown();
        });

        if (timeout > 0)
        {
            if (!latch.await(timeout, unit))
                throw new TimeoutException();
        }
        else
            latch.await();

        Result result = callbackResult.get();
        if (result.failure == null) return result.result;
        else throw new ExecutionException(result.failure);
    }

    public static <V> V getUninterruptibly(AsyncChain<V> chain) throws ExecutionException
    {
        try
        {
            return getUninterruptibly(chain, 0, TimeUnit.DAYS);
        }
        catch (TimeoutException e)
        {
            throw illegalState("Should not throw timeout exception e");
        }
    }

    public static <V> V getUninterruptibly(AsyncChain<V> chain, long time, TimeUnit unit) throws ExecutionException, TimeoutException
    {
        boolean interrupted = false;
        try
        {
            while (true)
            {
                try
                {
                    return getBlocking(chain, time, unit);
                }
                catch (InterruptedException e)
                {
                    interrupted = true;
                }
            }
        }
        finally
        {
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    public static <V> V getUnchecked(AsyncChain<V> chain)
    {
        try
        {
            return getUninterruptibly(chain);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public static void awaitUninterruptibly(AsyncChain<?> chain)
    {
        try
        {
            getUninterruptibly(chain);
        }
        catch (ExecutionException e)
        {
            // ignore
        }
    }

    public static void awaitUninterruptiblyAndRethrow(AsyncChain<?> chain)
    {
        try
        {
            getUninterruptibly(chain);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }
}
