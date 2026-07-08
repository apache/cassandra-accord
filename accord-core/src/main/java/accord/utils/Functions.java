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

package accord.utils;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class Functions
{
    public static <I, O> O mapReduceNonNull(Function<I, O> map, BiFunction<O, O, O> reduce, List<I> input)
    {
        O result = null;
        for (I i : input)
        {
            if (i == null) continue;

            O o = map.apply(i);
            if (o == null) continue;

            if (result == null) result = o;
            else result = reduce.apply(result, o);
        }
        return result;
    }

    public static <I, O> O mapReduceNonNull(Function<I, O> map, BiFunction<O, O, O> reduce, I[] input)
    {
        O result = null;
        for (I i : input)
        {
            if (i == null) continue;

            O o = map.apply(i);
            if (o == null) continue;

            if (result == null) result = o;
            else result = reduce.apply(result, o);
        }
        return result;
    }

    public static <I, O> O foldl(List<I> list, BiFunction<I, O, O> foldl, O zero)
    {
        O result = zero;
        for (int i = 0, mi = list.size(); i < mi; ++i)
            result = foldl.apply(list.get(i), result);
        return result;
    }

    public static <I> long foldl(List<I> list, FoldToLong<I> foldl, long accumulate)
    {
        long result = accumulate;
        for (int i = 0, mi = list.size(); i < mi; ++i)
            result = foldl.apply(list.get(i), result);
        return result;
    }

    public static <I, O> O foldl(I[] array, BiFunction<I, O, O> foldl, O zero)
    {
        O result = zero;
        for (I in : array)
            result = foldl.apply(in, result);
        return result;
    }

    public static <I, O> O foldlNonNull(I[] array, BiFunction<I, O, O> foldl, O zero)
    {
        O result = zero;
        for (I in : array)
        {
            if (in != null)
                result = foldl.apply(in, result);
        }
        return result;
    }

    public static <T> Predicate<T> alwaysFalse()
    {
        return ignore -> false;
    }

    public static <T> Predicate<T> alwaysTrue()
    {
        return ignore -> true;
    }

    public static Predicate<Boolean> identityPredicate() { return v -> v; }

    public static <I, O extends I> long foldlNonNull(I[] array, FoldToLong<O> foldl, long zero)
    {
        long result = zero;
        for (I in : array)
        {
            if (in != null)
                result = foldl.apply((O)in, result);
        }
        return result;
    }

    public static <V> Function<V, Void> returningVoid(Consumer<V> wrap)
    {
        return new Function<>()
        {
            @Override
            public Void apply(V v)
            {
                wrap.accept(v);
                return null;
            }

            @Override
            public String toString()
            {
                return wrap.toString();
            }
        };
    }

    public static <V> Callable<Void> returningVoid(Runnable wrap)
    {
        return new Callable<>()
        {
            @Override
            public Void call()
            {
                wrap.run();
                return null;
            }

            @Override
            public String toString()
            {
                return wrap.toString();
            }
        };
    }
}
