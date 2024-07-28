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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import static accord.utils.Invariants.illegalState;

/**
 * Represents a map of ranges where precisely one value is bound to each point in the continuum of ranges,
 * and a simple function is sufficient to merge values inserted to overlapping ranges.
 *
 * Copied/adapted from Cassandra's PaxosRepairHistory class, however has a major distinction: applies only to the
 * covered ranges, i.e. the first start bound is the lower bound, and the last start bound is the upper bound,
 * and everything else is considered unknown. This is in contrast to the C* version where every logical range has
 * some associated information, with the first and last entries applying to everything either side of the start/end bound.
 *
 * A simple sorted array of bounds is sufficient to represent the state and perform efficient lookups.
 *
 * TODO (desired): use a mutable b-tree instead
 */
public class ReducingIntervalMap<K extends Comparable<? super K>, V>
{
    @SuppressWarnings("rawtypes")
    private static final Comparable[] NO_OBJECTS = new Comparable[0];

    // for simplicity at construction, we permit this to be overridden by the first insertion
    final boolean inclusiveEnds;
    // starts is 1 longer than values, so that starts[0] == start of values[0]
    protected final K[] starts;
    protected final V[] values;

    public ReducingIntervalMap()
    {
        this(false);
    }

    @SuppressWarnings("unchecked")
    public ReducingIntervalMap(boolean inclusiveEnds)
    {
        this.inclusiveEnds = inclusiveEnds;
        this.starts = (K[]) NO_OBJECTS;
        this.values = (V[]) NO_OBJECTS;
    }

    @VisibleForTesting
    ReducingIntervalMap(boolean inclusiveEnds, K[] starts, V[] values)
    {
        Invariants.checkArgument(starts.length == values.length + 1);
        this.inclusiveEnds = inclusiveEnds;
        this.starts = starts;
        this.values = values;
    }

    public V foldl(BiFunction<V, V, V> reduce)
    {
        V result = values[0];
        for (int i = 1; i < values.length; ++i)
        {
            if (values[i] != null)
                result = reduce.apply(result, values[i]);
        }
        return result;
    }

    public <V2> V2 foldl(BiFunction<V, V2, V2> reduce, V2 accumulator, Predicate<V2> terminate)
    {
        for (V value : values)
        {
            if (value != null)
            {
                accumulator = reduce.apply(value, accumulator);
                if (terminate.test(accumulator))
                    break;
            }
        }
        return accumulator;
    }

    public <V2> V2 foldlWithBounds(QuadFunction<V, V2, K, K, V2> fold, V2 accumulator, Predicate<V2> terminate)
    {
        for (int i = 0 ; i < values.length ; ++i)
        {
            V value = values[i];
            if (value != null)
            {
                accumulator = fold.apply(value, accumulator, starts[i], starts[i+1]);
                if (terminate.test(accumulator))
                    break;
            }
        }
        return accumulator;
    }

    public String toString()
    {
        return toString(v -> true);
    }

    public String toString(Predicate<V> include)
    {
        return IntStream.range(0, values.length)
                        .filter(i -> include.test(values[i]))
                        .mapToObj(i -> (inclusiveStarts() ? "[" : "(") + starts[i] + "," + starts[i + 1] + (inclusiveEnds ? "]" : ")") + "=" + values[i])
                        .collect(Collectors.joining(", ", "{", "}"));
    }

    @SuppressWarnings("rawtypes")
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReducingIntervalMap that = (ReducingIntervalMap) o;
        return inclusiveEnds == that.inclusiveEnds && Arrays.equals(starts, that.starts) && Arrays.equals(values, that.values);
    }

    public int hashCode()
    {
        return Arrays.hashCode(values);
    }

    public boolean inclusiveEnds()
    {
        return inclusiveEnds;
    }

    public V get(K key)
    {
        int idx = find(key);
        if (idx < 0 || idx >= values.length)
            return null;
        return values[idx];
    }

    public K startAt(int idx)
    {
        if (idx < 0 || idx > size() + 1)
            throw new IndexOutOfBoundsException(String.format("%d < 0 or > %d", idx, size() + 1));
        return starts[idx];
    }

    public V valueAt(int idx)
    {
        if (idx < 0 || idx > size())
            throw new IndexOutOfBoundsException(String.format("%d < 0 or > %d", idx, size()));
        return values[idx];
    }

    private int find(K key)
    {
        int idx = Arrays.binarySearch(starts, key);
        if (idx < 0) idx = -2 - idx;
        else if (inclusiveEnds) --idx;
        return idx;
    }

    protected final boolean inclusiveStarts()
    {
        return !inclusiveEnds;
    }

    public int size()
    {
        return values.length;
    }

    RangeIterator rangeIterator()
    {
        return new RangeIterator();
    }

    // append the item to the given list, modifying the underlying list
    // if the item makes previoud entries redundant

    protected static <K extends Comparable<? super K>, V, M extends ReducingIntervalMap<K, V>> M mergeIntervals(M historyLeft, M historyRight, IntervalBuilderFactory<K, V, M> factory)
    {
        if (historyLeft == null || historyLeft.values.length == 0)
            return historyRight;
        if (historyRight == null || historyRight.values.length == 0)
            return historyLeft;

        boolean inclusiveEnds = inclusiveEnds(historyLeft.inclusiveEnds, historyLeft.size() > 0, historyRight.inclusiveEnds, historyRight.size() > 0);
        IntervalBuilder<K, V, M> builder = factory.create(inclusiveEnds, historyLeft.size() + historyRight.size());

        ReducingIntervalMap<K, V>.RangeIterator left = historyLeft.rangeIterator();
        ReducingIntervalMap<K, V>.RangeIterator right = historyRight.rangeIterator();

        K start;
        {   // first loop over any range only covered by one of the two
            ReducingIntervalMap<K, V>.RangeIterator first = left.start().compareTo(right.start()) <= 0 ? left : right;
            ReducingIntervalMap<K, V>.RangeIterator second = first == left ? right : left;

            while (first.hasCurrent() && first.end().compareTo(second.start()) <= 0)
            {
                if (first.value() != null)
                    builder.append(first.start(), first.end(), first.value());
                first.next();
            }

            start = second.start();
            if (first.hasCurrent() && first.start().compareTo(start) < 0 && first.value() != null)
                builder.append(first.start(), start, builder.slice(first.start(), start, first.value()));
            Invariants.checkState(start.compareTo(second.start()) <= 0);
        }

        // loop over any range covered by both
        while (left.hasCurrent() && right.hasCurrent())
        {
            int cmp = left.end().compareTo(right.end());
            K end = (cmp <= 0 ? left : right).end();
            V value = sliceAndReduce(start, end, left.value(), right.value(), builder);
            if (cmp <= 0) left.next();
            if (cmp >= 0) right.next();
            if (value != null)
                builder.append(start, end, value);
            start = end;
        }

        // finally loop over any remaining range covered by only one
        ReducingIntervalMap<K, V>.RangeIterator remaining = left.hasCurrent() ? left : right;
        if (remaining.hasCurrent())
        {
            {   // only slice the first one
                K end = remaining.end();
                V value = remaining.value();
                if (value != null)
                {
                    value = builder.slice(start, end, value);
                    builder.append(start, end, value);
                }
                start = end;
                remaining.next();
            }
            while (remaining.hasCurrent())
            {
                K end = remaining.end();
                V value = remaining.value();
                if (value != null)
                    builder.append(start, end, value);
                start = end;
                remaining.next();
            }
        }

        return builder.build();
    }

    protected static <K extends Comparable<? super K>, V, M extends ReducingIntervalMap<K, V>> M merge(M historyLeft, M historyRight, BiFunction<V, V, V> reduce, BuilderFactory<K, V, M> factory)
    {
        if (historyLeft == null || historyLeft.values.length == 0)
            return historyRight;
        if (historyRight == null || historyRight.values.length == 0)
            return historyLeft;

        boolean inclusiveEnds = inclusiveEnds(historyLeft.inclusiveEnds, historyLeft.size() > 0, historyRight.inclusiveEnds, historyRight.size() > 0);
        AbstractBoundariesBuilder<K, V, M> builder = factory.create(inclusiveEnds, historyLeft.size() + historyRight.size());

        ReducingIntervalMap<K, V>.RangeIterator left = historyLeft.rangeIterator();
        ReducingIntervalMap<K, V>.RangeIterator right = historyRight.rangeIterator();

        K start;
        {   // first loop over any range only covered by one of the two
            ReducingIntervalMap<K, V>.RangeIterator first = left.start().compareTo(right.start()) <= 0 ? left : right;
            ReducingIntervalMap<K, V>.RangeIterator second = first == left ? right : left;

            K end = null;
            while (first.hasCurrent() && first.end().compareTo(second.start()) <= 0)
            {
                builder.append(first.start(), first.value(), reduce);
                end = first.end();
                first.next();
            }

            start = second.start();
            if (first.hasCurrent()) builder.append(first.start(), first.value(), reduce);
            else builder.append(end, null, reduce);
            Invariants.checkState(start.compareTo(second.start()) <= 0);
        }

        // loop over any range covered by both
        // TODO (expected): optimise merging of very different sized maps (i.e. for inserts)
        while (left.hasCurrent() && right.hasCurrent())
        {
            int cmp = left.end().compareTo(right.end());
            V value = reduce(left.value(), right.value(), reduce);

            if (cmp == 0)
            {
                builder.append(start, value, reduce);
                start = left.end();
                left.next();
                right.next();
            }
            else if (cmp < 0)
            {
                builder.append(start, value, reduce);
                start = left.end();
                left.next();
            }
            else
            {
                builder.append(start, value, reduce);
                start = right.end();
                right.next();
            }
        }

        // finally loop over any remaining range covered by only one
        while (left.hasCurrent())
        {
            builder.append(start, left.value(), reduce);
            start = left.end();
            left.next();
        }

        while (right.hasCurrent())
        {
            builder.append(start, right.value(), reduce);
            start = right.end();
            right.next();
        }

        builder.append(start, null, reduce);
        return builder.build();
    }


    private static <V> V reduce(V left, V right, BiFunction<V, V, V> reduce)
    {
        return left == null ? right : right == null ? left : reduce.apply(left, right);
    }

    private static <K extends Comparable<? super K>, V> V sliceAndReduce(K start, K end, V left, V right, IntervalBuilder<K, V, ?> builder)
    {
        if (left != null) left = builder.slice(start, end, left);
        if (right != null) right = builder.slice(start, end, right);
        return left == null ? right : right == null ? left : builder.reduce(left, right);
    }

    RangeIterator intersecting(K start, K end)
    {
        int from = Arrays.binarySearch(starts, start);
        if (from < 0) from = Math.max(0, -2 - from);
        else if (!inclusiveStarts()) ++from;

        int to = Arrays.binarySearch(starts, end);
        if (to < 0) to = -1 - to;
        else if (inclusiveStarts()) ++to;
        return new RangeIterator(from, to);
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

        boolean hasCurrent()
        {
            return i < end;
        }

        void next()
        {
            ++i;
        }

        K start()
        {
            return starts[i];
        }

        K end()
        {
            return starts[i + 1];
        }

        V value()
        {
            return values[i];
        }
    }

    protected interface BuilderFactory<K extends Comparable<? super K>, V, M extends ReducingIntervalMap<K, V>>
    {
        AbstractBoundariesBuilder<K, V, M> create(boolean inclusiveEnds, int capacity);
    }

    protected static abstract class AbstractBoundariesBuilder<K extends Comparable<? super K>, V, M extends ReducingIntervalMap<K, V>>
    {
        protected final boolean inclusiveEnds;
        protected final List<K> starts;
        protected final List<V> values;
        protected AbstractBoundariesBuilder(boolean inclusiveEnds, int capacity)
        {
            this.inclusiveEnds = inclusiveEnds;
            this.starts = new ArrayList<>(capacity);
            this.values = new ArrayList<>(capacity + 1);
        }

        public boolean isEmpty()
        {
            return values.isEmpty();
        }

        /**
         * null is a valid value to represent no knowledge, and is the *expected* final value, representing
         * the bound of our knowledge (any higher key will find no associated information)
         */
        public void append(K start, @Nullable V value, BiFunction<V, V, V> reduce)
        {
            int tailIdx = starts.size() - 1;

            Invariants.checkState(starts.size() == values.size());
            Invariants.checkState( tailIdx < 0 || start.compareTo(starts.get(tailIdx)) >= 0);

            boolean sameAsTailKey, sameAsTailValue;
            V tailValue;
            if (tailIdx < 0)
            {
                sameAsTailKey = sameAsTailValue = false;
                tailValue = null;
            }
            else
            {
                sameAsTailKey = start.equals(starts.get(tailIdx));
                tailValue = values.get(tailIdx);
                sameAsTailValue = Objects.equals(value, tailValue);
            }
            if (sameAsTailKey || sameAsTailValue)
            {
                if (sameAsTailValue)
                {
                    values.set(tailIdx, value == null ? null : tailValue);
                }
                else if (tailValue != null)
                {
                    values.set(tailIdx, reduce.apply(tailValue, value));
                }
                else if (tailIdx >= 1 && value.equals(values.get(tailIdx - 1)))
                {
                    // just remove the null value and start
                    values.remove(tailIdx);
                    starts.remove(tailIdx);
                }
                else
                {
                    values.set(tailIdx, value);
                }
            }
            else
            {
                starts.add(start);
                values.add(value);
            }
        }

        protected abstract M buildInternal();

        public final M build()
        {
            if (!values.isEmpty())
            {
                Invariants.checkState(values.get(values.size() - 1) == null);
                values.remove(values.size() - 1);
                Invariants.checkState(values.get(0) != null);
                Invariants.checkState(starts.size() == values.size() + 1);
            }
            return buildInternal();
        }
    }

    protected interface IntervalBuilderFactory<K extends Comparable<? super K>, V, M extends ReducingIntervalMap<K, V>>
    {
        IntervalBuilder<K, V, M> create(boolean inclusiveEnds, int capacity);
    }

    protected static abstract class IntervalBuilder<K extends Comparable<? super K>, V, M>
    {
        protected abstract V slice(K start, K end, V value);
        protected abstract V reduce(V a, V b);
        protected V tryMergeEqual(V a, V b)
        {
            return a.equals(b) ? a : null;
        }
        public abstract void append(K start, K end, @Nonnull V value);
        protected abstract M build();
    }

    protected static abstract class AbstractIntervalBuilder<K extends Comparable<? super K>, V, M> extends IntervalBuilder<K, V, M>
    {
        protected final boolean inclusiveEnds;
        protected final List<K> starts;
        protected final List<V> values;

        private K prevEnd;
        protected AbstractIntervalBuilder(boolean inclusiveEnds, int capacity)
        {
            this.inclusiveEnds = inclusiveEnds;
            this.starts = new ArrayList<>(capacity);
            this.values = new ArrayList<>(capacity + 1);
        }

        public void append(K start, K end, @Nonnull V value)
        {
            if (prevEnd != null)
            {
                int c = prevEnd.compareTo(start);
                Invariants.checkState(c <= 0);
                if (c < 0)
                {
                    starts.add(prevEnd);
                    values.add(null);
                }
            }

            int tailIdx = starts.size() - 1;
            assert starts.size() == values.size();
            assert tailIdx < 0 || start.compareTo(starts.get(tailIdx)) >= 0;

            V tailValue;
            if (tailIdx >= 0 && null != (tailValue = values.get(tailIdx)) && null != (tailValue = tryMergeEqual(tailValue, value)))
            {
                values.set(tailIdx, tailValue);
            }
            else
            {
                starts.add(start);
                values.add(value);
            }
            prevEnd = end;
        }

        protected abstract M buildInternal();

        public final M build()
        {
            if (prevEnd != null)
            {
                starts.add(prevEnd);
                prevEnd = null;
            }
            return buildInternal();
        }
    }
    static boolean inclusiveEnds(boolean leftIsInclusive, boolean leftIsDecisive, boolean rightIsInclusive, boolean rightIsDecisive)
    {
        if (leftIsInclusive == rightIsInclusive)
            return leftIsInclusive;
        else if (leftIsDecisive && rightIsDecisive)
            throw illegalState("Mismatching bound inclusivity/exclusivity");
        else if (leftIsDecisive)
            return leftIsInclusive;
        else
            return rightIsInclusive;
    }
}
