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
import com.google.common.annotations.VisibleForTesting;

import java.util.function.BiFunction;
import java.util.function.Predicate;

import static accord.utils.SortedArrays.Search.FAST;

public class ReducingRangeMap<V> extends ReducingIntervalMap<RoutingKey, V>
{
    public static class SerializerSupport
    {
        public static <V> ReducingRangeMap<V> create(boolean inclusiveEnds, RoutingKey[] ends, V[] values)
        {
            return new ReducingRangeMap<>(inclusiveEnds, ends, values);
        }
    }

    final RoutingKeys endKeys;

    public ReducingRangeMap(V value)
    {
        super(value);
        this.endKeys = RoutingKeys.EMPTY;
    }

    ReducingRangeMap(boolean inclusiveEnds, RoutingKey[] ends, V[] values)
    {
        super(inclusiveEnds, ends, values);
        this.endKeys = RoutingKeys.ofSortedUnique(ends);
    }

    public V foldl(Routables<?, ?> routables, BiFunction<V, V, V> fold, V initialValue)
    {
        return foldl(routables, fold, initialValue, ignore -> false);
    }

    public <V2> V2 foldl(Routables<?, ?> routables, BiFunction<V, V2, V2> fold, V2 initialValue, Predicate<V2> terminate)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError();
            case Key: return foldl((AbstractKeys<?, ?>) routables, fold, initialValue, terminate);
            case Range: return foldl((AbstractRanges<?>) routables, fold, initialValue, terminate);
        }
    }

    // TODO (required): test
    public <V2> V2 foldl(AbstractKeys<?, ?> keys, BiFunction<V, V2, V2> reduce, V2 accumulator, Predicate<V2> terminate)
    {
        int i = 0, j = 0;
        while (j < keys.size())
        {
            i = endKeys.findNext(i, keys.get(j), FAST);
            if (i < 0) i = -1 - i;
            else if (!inclusiveEnds) ++i;

            accumulator = reduce.apply(values[i], accumulator);
            if (terminate.test(accumulator))
                return accumulator;

            if (i == endKeys.size())
                return j + 1 == keys.size() ? accumulator : reduce.apply(values[i], accumulator);

            j = keys.findNext(j + 1, endKeys.get(i), FAST);
            if (j < 0) j = -1 - j;
        }
        return accumulator;
    }

    // TODO (required): test
    public <V2> V2 foldl(AbstractRanges<?> ranges, BiFunction<V, V2, V2> reduce, V2 accumulator, Predicate<V2> terminate)
    {
        int i = 0, j = 0;
        while (j < ranges.size())
        {
            Range range = ranges.get(j);
            i = endKeys.findNext(i, range.start(), FAST);
            if (i < 0) i = -1 - i;
            else if (inclusiveEnds) ++i;

            int nexti = endKeys.findNext(i, range.end(), FAST);
            if (nexti < 0) nexti = -nexti;
            else if (inclusiveEnds) ++nexti;

            while (i < nexti)
            {
                accumulator = reduce.apply(values[i++], accumulator);
                if (terminate.test(accumulator))
                    return accumulator;
            }
            if (i > endKeys.size())
                return accumulator;

            j = ranges.findNext(j + 1, endKeys.get(i - 1), FAST);
            if (j < 0) j = -1 - j;
        }
        return accumulator;
    }

    /**
     * returns a copy of this ReducingRangeMap limited to the ranges supplied, with all other ranges reporting the "zero" value
     */
    @VisibleForTesting
    static <V> ReducingRangeMap<V> trim(ReducingRangeMap<V> existing, Ranges ranges, BiFunction<V, V, V> reduce)
    {
        boolean inclusiveEnds = inclusiveEnds(existing.inclusiveEnds, existing.size() > 0, ranges.size() > 0 && ranges.get(0).endInclusive(), ranges.size() > 0);
        ReducingRangeMap.Builder<V> builder = new ReducingRangeMap.Builder<>(inclusiveEnds, existing.size());

        V zero = existing.values[0];
        for (Range select : ranges)
        {
            ReducingIntervalMap<RoutingKey, V>.RangeIterator intersects = existing.intersecting(select.start(), select.end());
            while (intersects.hasNext())
            {
                if (zero.equals(intersects.value()))
                {
                    intersects.next();
                    continue;
                }

                RoutingKey start = intersects.hasStart() && intersects.start().compareTo(select.start()) >= 0 ? intersects.start() : select.start();
                RoutingKey end = intersects.hasEnd() && intersects.end().compareTo(select.end()) <= 0 ? intersects.end() : select.end();

                builder.append(start, zero, reduce);
                builder.append(end, intersects.value(), reduce);
                intersects.next();
            }
        }

        builder.appendLast(zero);
        return builder.build();
    }

    public static <V> ReducingRangeMap<V> create(Ranges ranges, V value, V zero)
    {
        if (ranges.isEmpty())
            return new ReducingRangeMap<>(zero);

        ReducingRangeMap.Builder<V> builder = new ReducingRangeMap.Builder<>(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (Range range : ranges)
        {
            builder.append(range.start(), zero, (a, b) -> a); // if we are equal to prev end, take the prev value not zero
            builder.append(range.end(), value, (a, b) -> { throw new IllegalStateException(); });
        }
        builder.appendLast(zero);
        return builder.build();
    }

    public static <V> ReducingRangeMap<V> add(ReducingRangeMap<V> existing, Ranges ranges, V value, BiFunction<V, V, V> reduce)
    {
        return add(existing, ranges, value, reduce, existing.values[0]);
    }

    public static <V> ReducingRangeMap<V> add(ReducingRangeMap<V> existing, Ranges ranges, V value, BiFunction<V, V, V> reduce, V zero)
    {
        ReducingRangeMap<V> add = create(ranges, value, zero);
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
        Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        ReducingRangeMap<V> buildInternal()
        {
            return new ReducingRangeMap<>(inclusiveEnds, ends.toArray(new RoutingKey[0]), (V[])values.toArray(new Object[0]));
        }
    }
}
