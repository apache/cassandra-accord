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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

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

    static <T> Observable<T> forCallback(BiConsumer<? super List<T>, Throwable> callback)
    {
        return new Observable<T>()
        {
            private List<T> elements = new ArrayList<>();

            @Override
            public void onNext(T value)
            {
                elements.add(value);
            }

            @Override
            public void onError(Throwable t)
            {
                this.elements.clear();
                this.elements = null;
                callback.accept(null, t);
            }

            @Override
            public void onCompleted()
            {
                List<T> elements = this.elements;
                this.elements = null;
                callback.accept(elements, null);
            }
        };
    }

    static <A> AsyncChain<List<A>> asChain(Consumer<Observable<A>> work)
    {
        return asChain(work, Function.identity());
    }

    static <A, B> AsyncChain<List<B>> asChain(Consumer<Observable<A>> work, Function<? super A, ? extends B> mapper)
    {
        return new AsyncChains.Head<List<B>>()
        {
            @Override
            protected void start(BiConsumer<? super List<B>, Throwable> callback)
            {
                work.accept(new Observable<A>()
                {
                    List<B> values = new ArrayList<>();

                    @Override
                    public void onNext(A value)
                    {
                        values.add(mapper.apply(value));
                    }

                    @Override
                    public void onError(Throwable t)
                    {
                        values.clear();
                        values = null;
                        callback.accept(null, t);
                    }

                    @Override
                    public void onCompleted()
                    {
                        List<B> values = this.values;
                        this.values = null;
                        callback.accept(values, null);
                    }
                });
            }
        };
    }
}
