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

import accord.api.RoutingKey;
import accord.primitives.*;

import java.util.function.BiFunction;
import java.util.function.Predicate;

import static accord.utils.SortedArrays.Search.FAST;
import static accord.utils.SortedArrays.exponentialSearch;

public class ReducingRangeMap<V> extends ReducingIntervalMap<RoutingKey, V>
{
    public static class SerializerSupport
    {
        public static <V> ReducingRangeMap<V> create(boolean inclusiveEnds, RoutingKey[] ends, V[] values)
        {
            return new ReducingRangeMap<>(inclusiveEnds, ends, values);
        }
    }

    public ReducingRangeMap()
    {
        super();
    }

    protected ReducingRangeMap(boolean inclusiveEnds, RoutingKey[] ends, V[] values)
    {
        super(inclusiveEnds, ends, values);
    }

    public V foldl(Routables<?, ?> routables, BiFunction<V, V, V> fold, V accumulator)
    {
        return foldl(routables, (a, b, f, ignore) -> f.apply(a, b), accumulator, fold, null, ignore -> false);
    }

    public <V2> V2 foldl(Routables<?, ?> routables, BiFunction<V, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
    {
        return foldl(routables, (a, b, f, ignore) -> f.apply(a, b), accumulator, fold, null, terminate);
    }

    public <V2, P1, P2> V2 foldl(Routables<?, ?> routables, QuadFunction<V, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        return foldl(routables, (v, v2, param1, param2, i, j) -> fold.apply(v, v2, param1, param2), accumulator, p1, p2, terminate);
    }

    public <V2, P1, P2> V2 foldl(Routables<?, ?> routables, IndexedRangeQuadFunction<V, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError();
            case Key: return foldl((AbstractKeys<?, ?>) routables, fold, accumulator, p1, p2, terminate);
            case Range: return foldl((AbstractRanges<?>) routables, fold, accumulator, p1, p2, terminate);
        }
    }

    // TODO (required): test
    public <V2, P1, P2> V2 foldl(AbstractKeys<?, ?> keys, IndexedRangeQuadFunction<V, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        if (values.length == 0)
            return accumulator;

        int i = 0, j = keys.findNext(0, starts[0], FAST);
        if (j < 0) j = -1 - j;
        else if (inclusiveEnds) ++j;

        while (j < keys.size())
        {
            i = exponentialSearch(starts, i, starts.length, keys.get(j));
            if (i < 0) i = -2 - i;
            else if (inclusiveEnds) --i;

            if (i >= values.length)
                return accumulator;

            int nextj = keys.findNext(j, starts[i + 1], FAST);
            if (nextj < 0) nextj = -1 -nextj;
            else if (inclusiveEnds) ++nextj;

            if (j != nextj && values[i] != null)
            {
                accumulator = fold.apply(values[i], accumulator, p1, p2, j, nextj);
                if (terminate.test(accumulator))
                    return accumulator;
            }
            ++i;
            j = nextj;
        }
        return accumulator;
    }

    // TODO (required): test
    public <V2, P1, P2> V2 foldl(AbstractRanges<?> ranges, IndexedRangeQuadFunction<V, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        if (values.length == 0)
            return accumulator;

        // TODO (desired): first searches should be binarySearch
        int j = ranges.findNext(0, starts[0], FAST);
        if (j < 0) j = -1 - j;
        else if (inclusiveEnds && ranges.get(j).end().equals(starts[0])) ++j;

        int i = 0;
        while (j < ranges.size())
        {
            Range range = ranges.get(j);
            RoutingKey start = range.start();
            int nexti = exponentialSearch(starts, i, starts.length, start);
            if (nexti < 0) i = Math.max(i, -2 - nexti);
            else if (nexti > i && !inclusiveStarts()) i = nexti - 1;
            else i = nexti;

            if (i >= values.length)
                return accumulator;

            int toj, nextj = ranges.findNext(j, starts[i + 1], FAST);
            if (nextj < 0) toj = nextj = -1 -nextj;
            else
            {
                toj = nextj + 1;
                if (inclusiveEnds && ranges.get(nextj).end().equals(starts[i + 1]))
                    ++nextj;
            }

            if (toj > j && values[i] != null)
            {
                accumulator = fold.apply(values[i], accumulator, p1, p2, j, toj);
                if (terminate.test(accumulator))
                    return accumulator;
            }
            ++i;
            j = nextj;
        }
        return accumulator;
    }

    public static <V> ReducingRangeMap<V> create(Ranges ranges, V value)
    {
        if (value == null)
            throw new IllegalArgumentException();

        if (ranges.isEmpty())
            return new ReducingRangeMap<>();

        ReducingRangeMap.Builder<V> builder = new ReducingRangeMap.Builder<>(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (Range range : ranges)
        {
            builder.append(range.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(range.end(), null, (a, b) -> a); // if we are equal to prev end, take the prev value not zero
        }
        return builder.build();
    }

    public static <V> ReducingRangeMap<V> add(ReducingRangeMap<V> existing, Ranges ranges, V value, BiFunction<V, V, V> reduce)
    {
        ReducingRangeMap<V> add = create(ranges, value);
        return merge(existing, add, reduce);
    }

    public static ReducingRangeMap<Timestamp> add(ReducingRangeMap<Timestamp> existing, Ranges ranges, Timestamp value)
    {
        return add(existing, ranges, value, Timestamp::max);
    }

    public static <V> ReducingRangeMap<V> merge(ReducingRangeMap<V> historyLeft, ReducingRangeMap<V> historyRight, BiFunction<V, V, V> reduce)
    {
        return ReducingIntervalMap.merge(historyLeft, historyRight, reduce, ReducingRangeMap.Builder::new);
    }

    static class Builder<V> extends ReducingIntervalMap.Builder<RoutingKey, V, ReducingRangeMap<V>>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected ReducingRangeMap<V> buildInternal()
        {
            return new ReducingRangeMap<>(inclusiveEnds, starts.toArray(new RoutingKey[0]), (V[])values.toArray(new Object[0]));
        }
    }
}
