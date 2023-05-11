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

import java.util.function.Function;

/**
 * Stream like interface that is "pushed" results (to the {@link #onNext(Object)} method).  This interface is similar,
 * yet different from {@link AsyncChain} as that type works with a single element, whereas this type works with 0-n.
 */
public interface Observable<T>
{
    void onNext(T value);
    default void onError(Throwable t) {}
    default void onCompleted() {}

    default <R> Observable<R> map(Function<? super R, ? extends T> mapper)
    {
        return new Map<>(this, mapper);
    }

    class Map<A, B> implements Observable<A>
    {
        private final Observable<B> next;
        private final Function<? super A, ? extends B> mapper;

        public Map(Observable<B> next, Function<? super A, ? extends B> mapper)
        {
            this.next = next;
            this.mapper = mapper;
        }

        @Override
        public void onNext(A value)
        {
            next.onNext(mapper.apply(value));
        }

        @Override
        public void onError(Throwable t)
        {
            next.onError(t);
        }

        @Override
        public void onCompleted()
        {
            next.onCompleted();
        }
    }
}
