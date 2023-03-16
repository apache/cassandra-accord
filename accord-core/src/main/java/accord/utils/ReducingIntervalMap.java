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

import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Mostly copied/adapted from Cassandra's PaxosRepairHistory class
 *
 * Represents a map of ranges where precisely one value is bound to each point in the continuum of ranges,
 * and a simple function is sufficient to merge values inserted to overlapping ranges.
 *
 * A simple sorted array of bounds is sufficient to represent the state and perform efficient lookups.
 *
 * TODO (desired): use a mutable b-tree instead
 */
public class ReducingIntervalMap<K extends Comparable<? super K>, V>
{
    private static final Comparable[] NO_OBJECTS = new Comparable[0];

    // for simplicity at construction, we permit this to be overridden by the first insertion
    final boolean inclusiveEnds;
    final K[] ends;
    final V[] values;

    public ReducingIntervalMap(V value)
    {
        this(false, value);
    }

    public ReducingIntervalMap(boolean inclusiveEnds, V value)
    {
        this.inclusiveEnds = inclusiveEnds;
        this.ends = (K[]) NO_OBJECTS;
        this.values = (V[]) new Object[] { value };
    }

    @VisibleForTesting
    ReducingIntervalMap(boolean inclusiveEnds, K[] ends, V[] values)
    {
        this.inclusiveEnds = inclusiveEnds;
        this.ends = ends;
        this.values = values;
    }

    public V reduceValues(BiFunction<V, V, V> reduce)
    {
        V result = values[0];
        for (int i = 1; i < values.length; ++i)
            result = reduce.apply(result, values[i]);
        return result;
    }

    public String toString()
    {
        return toString(v -> true);
    }

    public String toString(Predicate<V> include)
    {
        return IntStream.range(0, ends.length)
                        .filter(i -> include.test(values[i]))
                        .mapToObj(i -> ends[i] + "=" + values[i])
                        .collect(Collectors.joining(", ", "{", "}"));
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReducingIntervalMap that = (ReducingIntervalMap) o;
        return Arrays.equals(ends, that.ends) && Arrays.equals(values, that.values);
    }

    public int hashCode()
    {
        return Arrays.hashCode(values);
    }

    public V get(K key)
    {
        int idx = Arrays.binarySearch(ends, key);
        if (idx < 0) idx = -1 - idx;
        else if (!inclusiveEnds) ++idx;
        return values[idx];
    }

    public V value(int idx)
    {
        if (idx < 0 || idx > size())
            throw new IndexOutOfBoundsException();

        return values[idx];
    }

    public int indexOf(K key)
    {
        int idx = Arrays.binarySearch(ends, key);
        if (idx < 0) idx = -1 - idx;
        else if (!inclusiveEnds) --idx;
        return idx;
    }

    private boolean contains(int idx, K key)
    {
        if (idx < 0 || idx > size())
            throw new IndexOutOfBoundsException();

        int cmp = inclusiveEnds ? 0 : 1;
        return  (   idx == 0      || ends[idx - 1].compareTo(key) <  cmp)
                && (idx == size() || ends[idx    ].compareTo(key) >= cmp);
    }

    public int size()
    {
        return ends.length;
    }

    RangeIterator rangeIterator()
    {
        return new RangeIterator();
    }

    public Searcher searcher()
    {
        return new Searcher();
    }

    // append the item to the given list, modifying the underlying list
    // if the item makes previoud entries redundant

    static <K extends Comparable<? super K>, V, M extends ReducingIntervalMap<K, V>> M merge(M historyLeft, M historyRight, BiFunction<V, V, V> reduce, BuilderFactory<K, V, M> factory)
    {
        if (historyLeft == null)
            return historyRight;

        if (historyRight == null)
            return historyLeft;

        boolean inclusiveEnds = inclusiveEnds(historyLeft.inclusiveEnds, historyLeft.size() > 0, historyRight.inclusiveEnds, historyRight.size() > 0);
        Builder<K, V, M> builder = factory.create(inclusiveEnds, historyLeft.size() + historyRight.size());

        ReducingIntervalMap<K, V>.RangeIterator left = historyLeft.rangeIterator();
        ReducingIntervalMap<K, V>.RangeIterator right = historyRight.rangeIterator();
        while (left.hasEnd() && right.hasEnd())
        {
            int cmp = left.end().compareTo(right.end());

            V value = reduce.apply(left.value(), right.value());
            if (cmp == 0)
            {
                builder.append(left.end(), value, reduce);
                left.next();
                right.next();
            }
            else
            {
                ReducingIntervalMap<K, V>.RangeIterator firstIter = cmp < 0 ? left : right;
                builder.append(firstIter.end(), value, reduce);
                firstIter.next();
            }
        }

        while (left.hasEnd())
        {
            builder.append(left.end(), reduce.apply(left.value(), right.value()), reduce);
            left.next();
        }

        while (right.hasEnd())
        {
            builder.append(right.end(), reduce.apply(left.value(), right.value()), reduce);
            right.next();
        }

        builder.appendLast(reduce.apply(left.value(), right.value()));
        return builder.build();
    }

    RangeIterator intersecting(K start, K end)
    {
        int from = Arrays.binarySearch(ends, start);
        if (from < 0) from = -1 - from;
        else if (inclusiveEnds) ++from;

        int to = Arrays.binarySearch(ends, end);
        if (to < 0) to = -1 - to;
        else if (!inclusiveEnds) ++to;

        return new RangeIterator(from, 1 + to);
    }

    public class Searcher
    {
        int idx = -1;

        public V find(K key)
        {
            // TODO (low priority, efficiency): assume ascending iteration and use expSearch
            if (idx < 0 || !contains(idx, key))
                idx = indexOf(key);
            return value(idx);
        }
    }

    class RangeIterator
    {
        final int end;
        int i;

        RangeIterator()
        {
            this.end = values.length;
        }

        RangeIterator(int from, int to)
        {
            this.i = from;
            this.end = to;
        }

        boolean hasNext()
        {
            return i < end;
        }

        void next()
        {
            ++i;
        }

        boolean hasStart()
        {
            return i > 0;
        }

        boolean hasEnd()
        {
            return i < ends.length;
        }

        K start()
        {
            return ends[i - 1];
        }

        K end()
        {
            return ends[i];
        }

        V value()
        {
            return values[i];
        }
    }

    interface BuilderFactory<K extends Comparable<? super K>, V, M extends ReducingIntervalMap<K, V>>
    {
        Builder<K, V, M> create(boolean inclusiveEnds, int capacity);
    }

    static abstract class Builder<K extends Comparable<? super K>, V, M extends ReducingIntervalMap<K, V>>
    {
        final boolean inclusiveEnds;
        final List<K> ends;
        final List<V> values;

        Builder(boolean inclusiveEnds, int capacity)
        {
            this.inclusiveEnds = inclusiveEnds;
            this.ends = new ArrayList<>(capacity);
            this.values = new ArrayList<>(capacity + 1);
        }

        void append(K end, V value, BiFunction<V, V, V> reduce)
        {
            int tailIdx = ends.size() - 1;

            assert ends.size() == values.size();
            assert tailIdx < 0 || end.compareTo(ends.get(tailIdx)) >= 0;

            boolean sameAsTailKey = tailIdx >= 0 && end.equals(ends.get(tailIdx));
            boolean sameAsTailValue = tailIdx >= 0 && value.equals(values.get(tailIdx));
            if (sameAsTailKey || sameAsTailValue)
            {
                if (sameAsTailValue) ends.set(tailIdx, end);
                else values.set(tailIdx, reduce.apply(value, values.get(tailIdx)));
            }
            else
            {
                ends.add(end);
                values.add(value);
            }
        }

        void appendLast(V value)
        {
            assert values.size() == ends.size();
            int tailIdx = ends.size() - 1;
            if (!values.isEmpty() && value.equals(values.get(tailIdx)))
                ends.remove(tailIdx);
            else
                values.add(value);
        }

        abstract M buildInternal();

        final M build()
        {
            Invariants.checkState(ends.size() + 1 == values.size());
            return buildInternal();
        }
    }

    static boolean inclusiveEnds(boolean leftIsInclusive, boolean leftIsDecisive, boolean rightIsInclusive, boolean rightIsDecisive)
    {
        if (leftIsInclusive == rightIsInclusive)
            return leftIsInclusive;
        else if (leftIsDecisive && rightIsDecisive)
            throw new IllegalStateException("Mismatching bound inclusivity/exclusivity");
        else if (leftIsDecisive)
            return leftIsInclusive;
        else
            return rightIsInclusive;
    }
}
