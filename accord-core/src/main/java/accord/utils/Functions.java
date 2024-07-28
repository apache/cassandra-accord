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

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Functions
{

    public static <T> T reduceNonNull(BiFunction<T, T, T> merge, T a, T b)
    {
        return a == null ? b : b == null ? a : merge.apply(a, b);
    }

    public static <T1, T2> T1 reduceNonNull(BiFunction<T1, T2, T1> merge, @Nonnull T1 a, T2 ... bs)
    {
        for (T2 b : bs)
        {
            if (b != null)
                a = merge.apply(a, b);
        }
        return a;
    }

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

}
