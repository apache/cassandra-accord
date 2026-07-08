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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.AsyncExecutor;
import accord.utils.Invariants;
import accord.utils.Reduce;
import accord.utils.TriFunction;
import accord.utils.async.AsyncCombiner.ChainCombiner;

public class AsyncChains
{
    static class ImmediateSuccess<V> extends AsyncChains.Head<V>
    {
        final private V success;
        private ImmediateSuccess(V success) { this.success = success; }

        @Override
        protected Cancellable start(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(success, null);
            return null;
        }
    }

    static class ImmediateFailure<V> extends AsyncChains.Head<V>
    {
        final private Throwable failure;
        private ImmediateFailure(Throwable failure) { this.failure = failure; }

        @Override
        protected Cancellable start(BiConsumer<? super V, Throwable> callback)
        {
            callback.accept(null, failure);
            return null;
        }
    }

    public abstract static class Head<V> extends AbstractChain<V> implements AsyncChain.Head<V>
    {
        protected Head()
        {
            super(null);
            next = this;
        }

        protected abstract @Nullable Cancellable start(BiConsumer<? super V, Throwable> callback);

        @Override
        public final @Nullable Cancellable begin(BiConsumer<? super V, Throwable> callback)
        {
            Invariants.requireArgument(next != null);
            next = null;
            return start(callback);
        }

        public Cancellable begin()
        {
            Invariants.requireArgument(next != null);
            BiConsumer<? super V, Throwable> next = this.next;
            this.next = null;
            return start(next);
        }

        @Override
        public void accept(V v, Throwable throwable)
        {
            // we implement here just to simplify logic a little
            throw new UnsupportedOperationException();
        }
    }

    public static abstract class Link<I, O> extends AbstractChain<O> implements BiConsumer<I, Throwable>
    {
        protected Link(Head<?> head)
        {
            super(head);
        }

        @Override
        public Cancellable begin(BiConsumer<? super O, Throwable> callback)
        {
            Invariants.requireArgument(!(callback instanceof AsyncChains.Head));
            checkNextIsHead();
            Head<?> head = (Head<?>) next;
            next = callback;
            return head.begin();
        }
    }

    // TODO (desired): for efficiency common call-sites should directly use Map
    public static abstract class MapLink<I, O> extends Link<I, O> implements Function<I, O>
    {
        protected MapLink(Head<?> head)
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

    public static class MapToNull<I, O> extends MapLink<I, O>
    {
        public MapToNull(Head<?> head)
        {
            super(head);
        }

        @Override
        public O apply(I i)
        {
            return null;
        }
    }

    public static class Map<I, O> extends MapLink<I, O>
    {
        final Function<? super I, ? extends O> map;

        Map(Head<?> head, Function<? super I, ? extends O> map)
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

    public static class AsyncMap<I, O> extends FlatMapLink<I, O>
    {
        final Function<? super I, ? extends O> map;
        final AsyncExecutor executor;

        AsyncMap(Head<?> head, Function<? super I, ? extends O> map, AsyncExecutor executor)
        {
            super(head);
            this.map = map;
            this.executor = executor;
        }

        @Override
        public AsyncChain<O> apply(I i)
        {
            // TODO (desired): have executor specialisations for map.apply
            return executor.chain(() -> map.apply(i));
        }
    }

    public static abstract class FlatMapLink<I, O> extends Link<I, O> implements Function<I, AsyncChain<O>>
    {
        protected FlatMapLink(Head<?> head)
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

    public static class FlatMap<I, O> extends FlatMapLink<I, O>
    {
        final Function<? super I, ? extends AsyncChain<O>> map;

        FlatMap(Head<?> head, Function<? super I, ? extends AsyncChain<O>> map)
        {
            super(head);
            this.map = map;
        }

        @Override
        public AsyncChain<O> apply(I i)
        {
            try { return map.apply(i); }
            catch (Throwable t) { return AsyncChains.failure(t); }
        }
    }

    public static class AsyncFlatMap<I, O> extends FlatMapLink<I, O>
    {
        final Function<? super I, ? extends AsyncChain<O>> map;
        final AsyncExecutor executor;

        AsyncFlatMap(Head<?> head, Function<? super I, ? extends AsyncChain<O>> map, AsyncExecutor executor)
        {
            super(head);
            this.map = map;
            this.executor = executor;
        }

        @Override
        public AsyncChain<O> apply(I in)
        {
            return executor.flatChain(() -> map.apply(in));
        }
    }

    public static class FlatMapOverride<I, O> extends FlatMapLink<I, O>
    {
        final Supplier<? extends AsyncChain<O>> override;

        public FlatMapOverride(Head<?> head, Supplier<? extends AsyncChain<O>> override)
        {
            super(head);
            this.override = override;
        }

        @Override
        public AsyncChain<O> apply(I i)
        {
            try { return override.get(); }
            catch (Throwable t) { return AsyncChains.failure(t); }
        }
    }

    public static abstract class FlatMapResultLink<I, O> extends Link<I, O> implements Function<I, AsyncResult<O>>
    {
        protected FlatMapResultLink(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            if (throwable != null) next.accept(null, throwable);
            else apply(i).invoke(next);
        }
    }

    public static class FlatMapResult<I, O> extends FlatMapResultLink<I, O>
    {
        final Function<? super I, ? extends AsyncResult<O>> map;

        FlatMapResult(Head<?> head, Function<? super I, ? extends AsyncResult<O>> map)
        {
            super(head);
            this.map = map;
        }

        @Override
        public AsyncResult<O> apply(I i)
        {
            try { return map.apply(i); }
            catch (Throwable t) { return AsyncResults.failure(t); }
        }
    }

    public static abstract class MapRecoverLink<I> extends Link<I, I> implements Function<Throwable, AsyncChain<I>>
    {
        MapRecoverLink(Head<?> head)
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

    public static class MapRecover<I> extends MapRecoverLink<I>
    {
        private final Function<? super Throwable, ? extends AsyncChain<I>> map;

        public MapRecover(Head<?> head, Function<? super Throwable, ? extends AsyncChain<I>> function)
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
    public static class CallbackLink<I> extends Link<I, I>
    {
        CallbackLink(Head<?> head)
        {
            super(head);
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            next.accept(i, throwable);
        }
    }

    static class Callback<I> extends CallbackLink<I>
    {
        final BiConsumer<? super I, Throwable> callback;

        Callback(Head<?> head, BiConsumer<? super I, Throwable> callback)
        {
            super(head);
            this.callback = callback;
        }

        @Override
        public void accept(I i, Throwable throwable)
        {
            callback.accept(i, throwable);
            super.accept(i, throwable);
        }
    }

    public static class AsyncCallback<I> extends Link<I, I>
    {
        final BiConsumer<? super I, Throwable> callback;
        final AsyncExecutor executor;

        AsyncCallback(Head<?> head, BiConsumer<? super I, Throwable> callback, AsyncExecutor executor)
        {
            super(head);
            this.callback = callback;
            this.executor = executor;
        }

        @Override
        public void accept(I success, Throwable fail)
        {
            executor.execute(() -> {
                callback.accept(success, fail);
                next.accept(success, fail);
            });
        }
    }

    static class AccumulatingReducer<V> extends ChainCombiner<V, V>
    {
        private final Reduce<V, V> reducer;

        private AccumulatingReducer(Reduce<V, V> reducer)
        {
            super(new ArrayList<>());
            this.reducer = reducer;
        }

        public synchronized void add(AsyncChain<V> add)
        {
            inputs().add(add);
        }

        @VisibleForTesting
        public int size()
        {
            return inputs().size();
        }

        public static <V> boolean match(AsyncChain<V> accum, Reduce<? super V, ? extends V> reducer)
        {
            return accum instanceof AccumulatingReducer && ((AccumulatingReducer<?>) accum).reducer == reducer;
        }

        @Override
        V process(V[] inputs)
        {
            V result = inputs[0];
            for (int i = 1; i < inputs.length; i++)
                result = reducer.reduce(result, inputs[i]);
            return result;
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
        protected Cancellable start(BiConsumer<? super Void, Throwable> callback)
        {
            called.set(true);
            onCall.run();
            callback.accept(null, null);
            return null;
        }

        @Override
        protected void finalize()
        {
            if (!called.get())
                onLeak.accept(caller);
        }
    }

    public static abstract class AbstractChain<V> implements AsyncChain<V>
    {
        // either the thing we start, or the thing we do in follow-up
        BiConsumer<? super V, Throwable> next;
        AbstractChain(AsyncChain.Head<?> head)
        {
            this.next = (BiConsumer) head;
        }

        @Override
        public <T> AsyncChain<T> map(Function<? super V, ? extends T> mapper)
        {
            return then(Map::new, mapper);
        }

        @Override
        public <T> AsyncChain<T> flatMap(Function<? super V, ? extends AsyncChain<T>> mapper)
        {
            return then(FlatMap::new, mapper);
        }

        @Override
        public AsyncChain<V> recover(Function<? super Throwable, ? extends AsyncChain<V>> mapper)
        {
            return then(MapRecover::new, mapper);
        }

        @Override
        public AsyncChain<V> invoke(BiConsumer<? super V, Throwable> callback)
        {
            return then(Callback::new, callback);
        }

        // can be used by transformations that want efficiency, and can directly extend Link, FlatMap or Callback
        // (or perhaps some additional helper implementations that permit us to simply implement apply for Map and FlatMap)
        public <O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> then(Function<AsyncChain.Head<?>, T> factory)
        {
            checkNextIsHead();
            Head<?> head = (Head<?>) next;
            T result = factory.apply(head);
            next = result;
            return result;
        }

        @Override
        public <P, O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> then(BiFunction<AsyncChain.Head<?>, P, T> factory, P param)
        {
            checkNextIsHead();
            Head<?> head = (Head<?>) next;
            T result = factory.apply(head, param);
            next = result;
            return result;
        }

        @Override
        public <P1, P2, O, T extends AsyncChain<O> & BiConsumer<? super V, Throwable>> AsyncChain<O> then(TriFunction<Head<?>, P1, P2, T> factory, P1 p1, P2 p2)
        {
            checkNextIsHead();
            Head<?> head = (Head<?>) next;
            T result = factory.apply(head, p1, p2);
            next = result;
            return result;
        }

        protected void checkNextIsHead()
        {
            Invariants.require(next != null, "Begin was called multiple times");
            Invariants.require(next instanceof Head<?>, "Next is not an instance of AsyncChains.Head (it is %s); was map/flatMap called on the same object multiple times?", next.getClass());
        }
    }

    public static AsyncChain<?> detectLeak(Consumer<Throwable> onLeak, Runnable onCall)
    {
        return new DetectLeak(onLeak, onCall);
    }

    public static <V> AsyncChain<V> success(V success)
    {
        return new ImmediateSuccess<>(success);
    }

    public static <V> AsyncChain<V> failure(Throwable failure)
    {
        return new ImmediateFailure<>(failure);
    }

    public static AsyncChain<Void> chain(Runnable run)
    {
        return new Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super Void, Throwable> callback)
            {
                AsyncCallbacks.runAndCallback(run, callback);
                return null;
            }
        };
    }

    public static <V> AsyncChain<V> chain(Callable<V> call)
    {
        return new Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                AsyncCallbacks.callAndCallback(call, callback);
                return null;
            }
        };
    }

    public static AsyncChain<Void> chain(AsyncExecutor executor, Runnable run)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super Void, Throwable> callback)
            {
                return executor.execute(run, callback);
            }
        };
    }

    public static AsyncChain<Void> continuationChain(AsyncExecutor executor, Runnable run)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super Void, Throwable> callback)
            {
                return executor.executeContinuation(run, callback);
            }
        };
    }

    public static <V> AsyncChain<V> chain(AsyncExecutor executor, Callable<V> call)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                return executor.execute(call, callback);
            }

            @Override
            public String toString()
            {
                return call.toString();
            }
        };
    }

    public static <V> AsyncChain<V> flatChain(AsyncExecutor executor, Callable<? extends AsyncChain<V>> call)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                return executor.flatExecute(call, callback);
            }
        };
    }

    public static <V> AsyncChain<V> chain(Executor executor, Callable<V> call)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super V, Throwable> callback)
            {
                return AsyncCallbacks.execute(executor, new AsyncCallbacks.CallAndCallback<>(call, callback));
            }
        };
    }

    public static <V> AsyncChain<List<V>> allOf(List<? extends AsyncChain<? extends V>> chains)
    {
        return new ChainCombiner<>(chains) {

            @Override
            List<V> process(V[] inputs)
            {
                return Arrays.asList(inputs);
            }
        };
    }

    public static <V> AsyncChain<V> reduce(List<? extends AsyncChain<? extends V>> chains, Reduce<V, V> reducer)
    {
        if (chains.size() == 1)
            return (AsyncChain<V>) chains.get(0);

        return new ChainCombiner<>(chains)
        {
            @Override
            V process(V[] inputs)
            {
                V result = inputs[0];
                for (int i = 1; i < inputs.length ; ++i)
                    result = reducer.reduce(result, inputs[i]);
                return result;
            }
        };
    }

    public static <I, O> AsyncChain<O> reduce(List<? extends AsyncChain<? extends I>> chains, Reduce<I, O> reducer, O identity)
    {
        switch (chains.size())
        {
            case 0: return AsyncChains.success(identity);
            case 1: return chains.get(0).map(a -> reducer.reduce(identity, a));
        }
        return new ChainCombiner<>(chains)
        {
            @Override
            O process(I[] inputs)
            {
                O result = identity;
                for (I input : inputs)
                    result = reducer.reduce(result, input);
                return result;
            }
        };
    }

    public static <V> AsyncChain<V> reduce(AsyncChain<V> accum, AsyncChain<V> add, Reduce<V, V> reduce)
    {
        if (AccumulatingReducer.match(accum, reduce))
        {
            ((AccumulatingReducer<V>) accum).add(add);
            return accum;
        }
        AccumulatingReducer<V> reducer = new AccumulatingReducer<>(reduce);
        reducer.add(accum);
        reducer.add(add);
        return reducer;
    }
}
