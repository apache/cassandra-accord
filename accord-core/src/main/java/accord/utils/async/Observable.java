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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Stream like interface that is "pushed" results (to the {@link #onNext(Object)} method).  This interface is similar,
 * yet different from {@link AsyncChain} as that type works with a single element, whereas this type works with 0-n.
 */
public interface Observable<T>
{
    void onNext(T value) throws Exception;

    void onError(Throwable t);

    void onCompleted();

    default <R> Observable<R> map(Function<? super R, ? extends T> mapper)
    {
        Observable<T> self = this;
        // since this project still targets jdk8, can't create private classes, so to avoid adding types to the public api,
        // use ananomus classes.
        return new Observable<R>()
        {
            @Override
            public void onNext(R value) throws Exception
            {
                self.onNext(mapper.apply(value));
            }

            @Override
            public void onError(Throwable t)
            {
                self.onError(t);
            }

            @Override
            public void onCompleted()
            {
                self.onCompleted();
            }
        };
    }

    static <T> Observable<T> distinct(Observable<T> callback)
    {
        return new Observable<T>()
        {
            Set<T> keys = new HashSet<>();

            @Override
            public void onNext(T value) throws Exception
            {
                if (keys.add(value))
                    callback.onNext(value);
            }

            @Override
            public void onError(Throwable t)
            {
                keys.clear();
                keys = null;
                callback.onError(t);
            }

            @Override
            public void onCompleted()
            {
                keys.clear();
                keys = null;
                callback.onCompleted();
            }
        };
    }

    static <T> Observable<T> forCallback(BiConsumer<? super List<T>, Throwable> callback)
    {
        return forCallback(callback, Collectors.toList());
    }

    static <T, Result, Accumulator> Observable<T> forCallback(BiConsumer<? super Result, Throwable> callback,
                                                              Collector<? super T, Accumulator, Result> collector)
    {
        return new Observable<T>()
        {
            Accumulator values = collector.supplier().get();

            @Override
            public void onNext(T value)
            {
                collector.accumulator().accept(values, value);
            }

            @Override
            public void onError(Throwable t)
            {
                values = null;
                callback.accept(null, t);
            }

            @Override
            public void onCompleted()
            {
                Result result = collector.finisher().apply(this.values);
                this.values = null;
                callback.accept(result, null);
            }
        };
    }

    static <A> AsyncChain<List<A>> asChain(Consumer<Observable<A>> work)
    {
        return asChain(work, Function.identity(), Collectors.toList());
    }

    static <A, Result, Accumulator> AsyncChain<Result> asChain(Consumer<Observable<A>> work,
                                                               Collector<? super A, Accumulator, Result> collector)
    {
        return asChain(work, Function.identity(), collector);
    }

    static <A, B, Result, Accumulator> AsyncChain<Result> asChain(Consumer<Observable<A>> work,
                                                                  Function<? super A, ? extends B> mapper,
                                                                  Collector<? super B, Accumulator, Result> collector)
    {
        return new AsyncChains.Head<Result>()
        {
            @Override
            protected void start(BiConsumer<? super Result, Throwable> callback)
            {
                work.accept(forCallback(callback, collector).map(mapper));
            }
        };
    }
}
