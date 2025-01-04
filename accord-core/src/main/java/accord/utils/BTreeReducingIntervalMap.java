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

import accord.utils.btree.BTree;
import com.google.common.collect.AbstractIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static accord.utils.Invariants.*;
import static com.google.common.collect.Iterators.filter;
import static com.google.common.collect.Iterators.transform;

/**
 * Represents a map of ranges where precisely one value is bound to each point in the continuum of ranges,
 * and a simple function is sufficient to merge values inserted to overlapping ranges.
 * <p>
 * Copied/adapted from Cassandra's PaxosRepairHistory class, however has a major distinction: applies only to the
 * covered ranges, i.e. the first start bound is the lower bound, and the last start bound is the upper bound,
 * and everything else is considered unknown. This is in contrast to the C* version where every logical range has
 * some associated information, with the first and last entries applying to everything either side of the start/end bound.
 * <p>
 * A simple sorted array of bounds is sufficient to represent the state and perform efficient lookups.
 * <p>
 */
public class BTreeReducingIntervalMap<K extends Comparable<? super K>, V>
{
    protected final boolean inclusiveEnds;
    protected final Object[] tree;

    public BTreeReducingIntervalMap()
    {
        this(false);
    }

    public BTreeReducingIntervalMap(boolean inclusiveEnds)
    {
        this(inclusiveEnds, BTree.empty());
    }

    protected BTreeReducingIntervalMap(boolean inclusiveEnds, Object[] tree)
    {
        this.inclusiveEnds = inclusiveEnds;
        this.tree = tree;
    }

    public boolean inclusiveEnds()
    {
        return inclusiveEnds;
    }

    public final boolean inclusiveStarts()
    {
        return !inclusiveEnds;
    }

    public boolean isEmpty()
    {
        return BTree.isEmpty(tree);
    }

    public int size()
    {
        return Math.max(BTree.size(tree) - 1, 0);
    }

    public V get(K key)
    {
        checkArgument(null != key);
        int idx = BTree.findIndex(tree, EntryComparator.instance(), key);

        if (idx < 0) idx = -2 - idx;
        else if (inclusiveEnds) --idx;

        return idx < 0 || idx >= size()
             ? null
             : valueAt(idx);
    }

    public V foldl(BiFunction<V, V, V> reduce)
    {
        // TODO (expected): use BTree fold methods
        checkState(!isEmpty());
        Iterator<V> iter = valuesIterator(false);
        V result = iter.next();
        while (iter.hasNext())
        {
            V next = iter.next();
            if (next != null)
                result = reduce.apply(result, next);
        }
        return result;
    }

    public <V2> V2 foldl(BiFunction<V, V2, V2> reduce, V2 accumulator, Predicate<V2> terminate)
    {
        Iterator<V> iter = valuesIterator(true);
        while (iter.hasNext())
        {
            accumulator = reduce.apply(iter.next(), accumulator);
            if (terminate.test(accumulator))
                break;
        }
        return accumulator;
    }

    public <V2> V2 foldlWithBounds(QuadFunction<V, V2, K, K, V2> fold, V2 accumulator, Predicate<V2> terminate)
    {
        // TODO (expected): use BTree fold methods
        WithBoundsIterator<K, V> iter = withBoundsIterator(true);
        while (iter.advance())
        {
            accumulator = fold.apply(iter.value(), accumulator, iter.start(), iter.end());
            if (terminate.test(accumulator))
                break;
        }
        return accumulator;
    }

    @Override
    public String toString()
    {
        return toString(v -> true);
    }

    public String toString(Predicate<V> include)
    {
        if (isEmpty())
            return "{}";

        StringBuilder builder = new StringBuilder("{");
        boolean isFirst = true;

        WithBoundsIterator<K, V> iter = withBoundsIterator();
        while (iter.advance())
        {
            if (!include.test(iter.value()))
                continue;

            if (!isFirst)
                builder.append(", ");

            builder.append(inclusiveStarts() ? '[' : '(')
                   .append(iter.start())
                   .append(',')
                   .append(iter.end())
                   .append(inclusiveEnds() ? ']' : ')')
                   .append('=')
                   .append(iter.value());

            isFirst = false;
        }
        return builder.append('}').toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        @SuppressWarnings("unchecked")
        BTreeReducingIntervalMap<K, V> that = (BTreeReducingIntervalMap<K, V>) o;
        return this.inclusiveEnds == that.inclusiveEnds && BTree.equals(this.tree, that.tree);
    }

    @Override
    public int hashCode()
    {
        return Boolean.hashCode(inclusiveEnds) + 31 * BTree.hashCode(tree);
    }

    public static class EntryComparator<K extends Comparable<? super K>, V> implements AsymmetricComparator<K, Entry<K, V>>
    {
        private static final EntryComparator<?,?> INSTANCE = new EntryComparator<>();

        @SuppressWarnings("unchecked")
        public static <K extends Comparable<? super K>, V> EntryComparator<K, V> instance()
        {
            return (EntryComparator<K, V>) INSTANCE;
        }

        @Override
        public int compare(K start, Entry<K, V> entry)
        {
            return start.compareTo(entry.start);
        }
    }

    public static class Entry<K extends Comparable<? super K>, V> implements Comparable<Entry<K, V>>
    {
        private final K start;
        private final V value;
        private final boolean hasValue;

        private Entry(K start, V value, boolean hasValue)
        {
            this.start = start;
            this.value = value;
            this.hasValue = hasValue;
        }

        public static <K extends Comparable<? super K>, V> Entry<K, V> make(K start)
        {
            return new Entry<>(start, null, false);
        }

        public static <K extends Comparable<? super K>, V> Entry<K, V> make(K start, V value)
        {
            return new Entry<>(start, value, true);
        }

        public K start()
        {
            return start;
        }

        public V value()
        {
            if (hasValue) return value;
            throw illegalState();
        }

        /** null is considered a valid value, distinct from not having a set value at all */
        public boolean hasValue()
        {
            return hasValue;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            @SuppressWarnings("unchecked")
            Entry<K, V> that = (Entry<K, V>) o;
            return Objects.equals(this.start, that.start) && this.hasValue == that.hasValue && Objects.equals(this.value, that.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(start) + 31 * (Boolean.hashCode(hasValue) + 31 * Objects.hashCode(value));
        }

        @Override
        public int compareTo(@Nonnull Entry<K, V> that)
        {
            return this.start.compareTo(that.start);
        }
    }

    protected static <K extends Comparable<? super K>, V, M extends BTreeReducingIntervalMap<K, V>> M mergeIntervals(
            M historyLeft, M historyRight, IntervalBuilderFactory<K, V, M> factory)
    {
        if (historyLeft == null || historyLeft.isEmpty())
            return historyRight;
        if (historyRight == null || historyRight.isEmpty())
            return historyLeft;

        boolean inclusiveEnds = inclusiveEnds(historyLeft.inclusiveEnds, !historyLeft.isEmpty(), historyRight.inclusiveEnds, !historyRight.isEmpty());
        AbstractIntervalBuilder<K, V, M> builder = factory.create(inclusiveEnds, historyLeft.size() + historyRight.size());

        WithBoundsIterator<K, V> left = historyLeft.withBoundsIterator();
        WithBoundsIterator<K, V> right = historyRight.withBoundsIterator();

        left.advance();
        right.advance();

        K start;
        {
            // first loop over any range only covered by one of the two
            WithBoundsIterator<K, V> first = left.start().compareTo(right.start()) <= 0 ? left : right;
            WithBoundsIterator<K, V> second = first == left ? right : left;

            while (first.hasCurrent() && first.end().compareTo(second.start()) <= 0)
            {
                if (first.value() != null)
                    builder.append(first.start(), first.end(), first.value());
                first.advance();
            }

            start = second.start();
            if (first.hasCurrent() && first.start().compareTo(start) < 0 && first.value() != null)
                builder.append(first.start(), start, builder.slice(first.start(), start, first.value()));
            checkState(start.compareTo(second.start()) <= 0);
        }

        // loop over any range covered by both
        while (left.hasCurrent() && right.hasCurrent())
        {
            int cmp = left.end().compareTo(right.end());
            K end = (cmp <= 0 ? left : right).end();
            V value = sliceAndReduce(start, end, left.value(), right.value(), builder);
            if (cmp <= 0) left.advance();
            if (cmp >= 0) right.advance();
            if (value != null)
                builder.append(start, end, value);
            start = end;
        }

        // finally loop over any remaining range covered by only one
        WithBoundsIterator<K, V> remaining = left.hasCurrent() ? left : right;
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
                remaining.advance();
            }
            while (remaining.hasCurrent())
            {
                K end = remaining.end();
                V value = remaining.value();
                if (value != null)
                    builder.append(start, end, value);
                start = end;
                remaining.advance();
            }
        }

        return builder.build();
    }

    protected static <K extends Comparable<? super K>, V, M extends BTreeReducingIntervalMap<K, V>> M merge(
            M historyLeft, M historyRight, BiFunction<V, V, V> reduce, BoundariesBuilderFactory<K, V, M> factory)
    {
        if (historyLeft == null || historyLeft.isEmpty())
            return historyRight;
        if (historyRight == null || historyRight.isEmpty())
            return historyLeft;

        boolean inclusiveEnds = inclusiveEnds(historyLeft.inclusiveEnds, !historyLeft.isEmpty(), historyRight.inclusiveEnds, !historyRight.isEmpty());
        AbstractBoundariesBuilder<K, V, M> builder = factory.create(inclusiveEnds, historyLeft.size() + historyRight.size());

        WithBoundsIterator<K, V> left = historyLeft.withBoundsIterator();
        WithBoundsIterator<K, V> right = historyRight.withBoundsIterator();

        left.advance();
        right.advance();

        K start;
        {
            // first loop over any range only covered by one of the two
            WithBoundsIterator<K, V> first = left.start().compareTo(right.start()) <= 0 ? left : right;
            WithBoundsIterator<K, V> second = first == left ? right : left;

            K end = null;
            while (first.hasCurrent() && first.end().compareTo(second.start()) <= 0)
            {
                builder.append(first.start(), first.value(), reduce);
                end = first.end();
                first.advance();
            }

            start = second.start();
            if (first.hasCurrent())
                builder.append(first.start(), first.value(), reduce);
            else
                builder.append(end, null, reduce);
            checkState(start.compareTo(second.start()) <= 0);
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
                left.advance();
                right.advance();
            }
            else if (cmp < 0)
            {
                builder.append(start, value, reduce);
                start = left.end();
                left.advance();
            }
            else
            {
                builder.append(start, value, reduce);
                start = right.end();
                right.advance();
            }
        }

        // finally loop over any remaining range covered by only one
        while (left.hasCurrent())
        {
            builder.append(start, left.value(), reduce);
            start = left.end();
            left.advance();
        }

        while (right.hasCurrent())
        {
            builder.append(start, right.value(), reduce);
            start = right.end();
            right.advance();
        }

        builder.append(start, null, reduce);

        return builder.build();
    }

    static boolean inclusiveEnds(boolean leftIsInclusive, boolean leftIsDecisive, boolean rightIsInclusive, boolean rightIsDecisive)
    {
        if (leftIsInclusive == rightIsInclusive)
            return leftIsInclusive;

        if (leftIsDecisive && rightIsDecisive)
            throw illegalState("Mismatching bound inclusivity/exclusivity");

        return leftIsDecisive ? leftIsInclusive : rightIsInclusive;
    }

    private static <V> V reduce(V left, V right, BiFunction<V, V, V> reduce)
    {
        return left == null ? right : right == null ? left : reduce.apply(left, right);
    }

    private static <K extends Comparable<? super K>, V> V sliceAndReduce(K start, K end, V left, V right, AbstractIntervalBuilder<K, V, ?> builder)
    {
        if (left != null) left = builder.slice(start, end, left);
        if (right != null) right = builder.slice(start, end, right);
        return left == null ? right : right == null ? left : builder.reduce(left, right);
    }

    public interface BoundariesBuilderFactory<K extends Comparable<? super K>, V, M extends BTreeReducingIntervalMap<K, V>>
    {
        AbstractBoundariesBuilder<K, V, M> create(boolean inclusiveEnds, int capacity);
    }

    public static abstract class AbstractBoundariesBuilder<K extends Comparable<? super K>, V, M extends BTreeReducingIntervalMap<K, V>>
    {
        protected final boolean inclusiveEnds;

        private final BTree.Builder<Entry<K, V>> treeBuilder;
        private final TinyKVBuffer<K, V> buffer;

        protected AbstractBoundariesBuilder(boolean inclusiveEnds, int capacity)
        {
            this.inclusiveEnds = inclusiveEnds;
            this.treeBuilder = BTree.builder(Comparator.naturalOrder(), capacity);
            this.buffer = new TinyKVBuffer<>();
        }

        public boolean isEmpty()
        {
            return buffer.isEmpty();
        }

        /**
         * null is a valid value to represent no knowledge, and is the *expected* final value, representing
         * the bound of our knowledge (any higher key will find no associated information)
         */
        public void append(K start, @Nullable V value, BiFunction<V, V, V> reduce)
        {
            if (buffer.isEmpty())
            {
                buffer.append(start, value);
                return;
            }

            K prevStart = buffer.lastKey();
            V prevValue = buffer.lastValue();

            checkArgument(start.compareTo(prevStart) >= 0);

            boolean samePrevStart = start.equals(prevStart);
            boolean samePrevValue = Objects.equals(value, prevValue);

            if (!(samePrevStart || samePrevValue))
            {
                if (buffer.isFull())
                {
                    treeBuilder.add(Entry.make(buffer.firstKey(), buffer.firstValue()));
                    buffer.dropFirst();
                }
                buffer.append(start, value);
                return;
            }

            if (samePrevValue)
                return;

            if (prevValue != null)
                buffer.lastValue(reduce.apply(prevValue, value));
            else if (buffer.size() >= 2 && value.equals(buffer.penultimateValue()))
                buffer.dropLast(); // just remove the last start and (null) value
            else
                buffer.lastValue(value);
        }

        protected abstract M buildInternal(Object[] tree);

        public final M build()
        {
            while (buffer.size() > 1)
            {
                treeBuilder.add(Entry.make(buffer.firstKey(), buffer.firstValue()));
                buffer.dropFirst();
            }

            if (!buffer.isEmpty())
            {
                checkState(buffer.lastValue() == null);
                treeBuilder.add(Entry.make(buffer.firstKey()));
                buffer.dropFirst();
            }

            return buildInternal(treeBuilder.build());
        }
    }

    public interface IntervalBuilderFactory<K extends Comparable<? super K>, V, M extends BTreeReducingIntervalMap<K, V>>
    {
        AbstractIntervalBuilder<K, V, M> create(boolean inclusiveEnds, int capacity);
    }

    public static abstract class AbstractIntervalBuilder<K extends Comparable<? super K>, V, M>
    {
        protected final boolean inclusiveEnds;
        private final BTree.Builder<Entry<K, V>> treeBuilder;
        private final TinyKVBuffer<K, V> buffer;

        private K prevEnd;

        protected AbstractIntervalBuilder(boolean inclusiveEnds, int capacity)
        {
            this.inclusiveEnds = inclusiveEnds;
            this.treeBuilder = BTree.builder(Comparator.naturalOrder(), capacity);
            this.buffer = new TinyKVBuffer<>();
        }

        protected abstract V slice(K start, K end, V value);
        protected abstract V reduce(V a, V b);

        protected V tryMergeEqual(@Nonnull V a, V b)
        {
            return a.equals(b) ? a : null;
        }

        public void append(K start, K end, @Nonnull V value)
        {
            if (prevEnd != null)
            {
                int c = prevEnd.compareTo(start);
                checkArgument(c <= 0);
                if (c < 0)
                {
                    if (buffer.isFull())
                    {
                        treeBuilder.add(Entry.make(buffer.firstKey(), buffer.firstValue()));
                        buffer.dropFirst();
                    }
                    buffer.append(prevEnd, null);
                }
            }

            V prevValue;
            if (!buffer.isEmpty() && null != (prevValue = buffer.lastValue()) && null != (prevValue = tryMergeEqual(prevValue, value)))
            {
                buffer.lastValue(prevValue);
            }
            else
            {
                if (buffer.isFull())
                {
                    treeBuilder.add(Entry.make(buffer.firstKey(), buffer.firstValue()));
                    buffer.dropFirst();
                }
                buffer.append(start, value);
            }

            prevEnd = end;
        }

        protected abstract M buildInternal(Object[] tree);

        public final M build()
        {
            while (!buffer.isEmpty())
            {
                treeBuilder.add(Entry.make(buffer.firstKey(), buffer.firstValue()));
                buffer.dropFirst();
            }

            if (prevEnd != null)
            {
                treeBuilder.add(Entry.make(prevEnd));
                prevEnd = null;
            }

            return buildInternal(treeBuilder.build());
        }
    }

    protected Entry<K, V> entryAt(int idx)
    {
        return BTree.findByIndex(tree, idx);
    }

    protected K startAt(int idx)
    {
        return entryAt(idx).start();
    }

    protected V valueAt(int idx)
    {
        return entryAt(idx).value();
    }

    protected Iterator<Entry<K, V>> entriesIterator()
    {
        return BTree.iterator(tree);
    }

    protected Iterator<V> valuesIterator(boolean skipNullValues)
    {
        return transform(filter(entriesIterator(),
                                skipNullValues ? e -> e.hasValue() && e.value() != null : Entry::hasValue),
                         Entry::value);
    }

    protected void forEachWithBounds(boolean skipNullValues, TriConsumer<K, K, V> consumer)
    {
        if (isEmpty())
            return;
        WithBoundsIterator<K, V> iter = withBoundsIterator(skipNullValues);
        while (iter.advance())
            consumer.accept(iter.start(), iter.end(), iter.value());
    }

    public WithBoundsIterator<K, V> withBoundsIterator()
    {
        return new WithBoundsIterator<>(false, entriesIterator());
    }

    public WithBoundsIterator<K, V> withBoundsIterator(boolean skipNullValues)
    {
        return new WithBoundsIterator<>(skipNullValues, entriesIterator());
    }

    public <M> Iterator<M> iterator(boolean skipNullValues, TriFunction<K, K, V, M> mapper)
    {
        return new WithBoundsIterator<>(skipNullValues, entriesIterator()).map(mapper);
    }

    public <M> Stream<M> stream(boolean skipNullValues, TriFunction<K, K, V, M> mapper)
    {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(skipNullValues, mapper), 0), false);
    }

    private enum IteratorState { START, ADVANCED, END }

    public static class WithBoundsIterator<K extends Comparable<? super K>, V>
    {
        private final boolean skipNullValues;
        private final Iterator<Entry<K, V>> iterator;

        private Entry<K, V> curr, next;
        private IteratorState state = IteratorState.START;

        private WithBoundsIterator(boolean skipNullValues, Iterator<Entry<K, V>> entriesIterator)
        {
            this.skipNullValues = skipNullValues;
            this.iterator = entriesIterator;
        }

        public boolean hasCurrent()
        {
            return state == IteratorState.ADVANCED;
        }

        public K start()
        {
            return curr.start();
        }

        public K end()
        {
            return next.start();
        }

        public V value()
        {
            return curr.value();
        }

        public boolean advance()
        {
            if (!skipNullValues)
                return advanceInternal();

            //noinspection StatementWithEmptyBody
            while (advanceInternal() && value() == null);
            return hasCurrent();
        }

        private boolean advanceInternal()
        {
            switch (state)
            {
                case START:
                    if (iterator.hasNext())
                    {
                        curr = iterator.next();
                        checkState(iterator.hasNext()); // must contain at least two entries
                        next = iterator.next();
                        state = IteratorState.ADVANCED;
                    }
                    else
                    {
                        state = IteratorState.END;
                    }
                    break;
                case ADVANCED:
                    if (next.hasValue())
                    {
                        checkState(iterator.hasNext()); // must be at least one entry after next if it has a value
                        curr = next;
                        next = iterator.next();
                        state = IteratorState.ADVANCED;
                    }
                    else
                    {
                        checkState(!iterator.hasNext()); // should have no more entries if next has no value
                        curr = next = null;
                        state = IteratorState.END;
                    }
                    break;
            }

            return state == IteratorState.ADVANCED;
        }

        private <M> Iterator<M> map(TriFunction<K, K, V, M> mapper)
        {
            return new AbstractIterator<>()
            {
                @Override
                protected M computeNext()
                {
                    return advance() ? mapper.apply(start(), end(), value()) : endOfData();
                }
            };
        }
    }
}
