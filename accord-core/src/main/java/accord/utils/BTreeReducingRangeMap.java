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
import accord.utils.btree.BTree;
import accord.utils.btree.ReducingBTree;
import accord.utils.btree.ReducingBTree.Entry;

import java.io.Closeable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import static accord.utils.Invariants.require;
import static accord.utils.Invariants.requireArgument;

/**
 * Represents a map of ranges where precisely one value is bound to each point in the continuum of ranges,
 * and a simple function is sufficient to merge values inserted to overlapping ranges.
 */
public class BTreeReducingRangeMap<E extends Entry<E>> implements Iterable<E>
{
    protected final Object[] tree;

    public BTreeReducingRangeMap()
    {
        this(BTree.empty());
    }

    protected BTreeReducingRangeMap(Object[] tree)
    {
        this.tree = tree;
    }

    public boolean isEmpty()
    {
        return BTree.isEmpty(tree);
    }

    public int size()
    {
        return BTree.size(tree);
    }

    public E get(RoutingKey key)
    {
        requireArgument(null != key);
        return BTree.find(tree, (RoutingKey k, Entry<?> e) -> Entry.compare(k, e), key);
    }

    public <V2> V2 foldl(BiFunction<E, V2, V2> reduce, V2 accumulator)
    {
        return BTree.foldl(tree, reduce, accumulator);
    }

    public <V2, P1> V2 foldl(P1 p1, TriFunction<P1, E, V2, V2> reduce, V2 accumulator)
    {
        return BTree.foldl(tree, p1, reduce, accumulator);
    }

    @Override
    public String toString()
    {
        return toString(v -> true);
    }

    public String toString(Predicate<E> include)
    {
        if (isEmpty())
            return "{}";

        StringBuilder builder = new StringBuilder("{");

        for (E e : this)
        {
            if (!include.test(e))
                continue;

            if (builder.length() > 1)
                builder.append(", ");

            builder.append(e);
        }
        return builder.append('}').toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        @SuppressWarnings("unchecked")
        BTreeReducingRangeMap<E> that = (BTreeReducingRangeMap<E>) o;
        return BTree.equals(this.tree, that.tree);
    }

    @Override
    public int hashCode()
    {
        return 31 * BTree.hashCode(tree);
    }

    public E entryAt(int idx)
    {
        return BTree.findByIndex(tree, idx);
    }

    @Nonnull
    public Iterator<E> iterator()
    {
        return BTree.iterator(tree);
    }

    public Stream<E> stream()
    {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), 0), false);
    }

    public <V2> V2 foldl(Routables<?> routables, BiFunction<E, V2, V2> fold, V2 accumulator)
    {
        return foldl(routables, (a, b, f, ignore) -> f.apply(a, b), accumulator, fold, null);
    }

    public <V2, P1> V2 foldl(Routables<?> routables, TriFunction<E, V2, P1, V2> fold, V2 accumulator, P1 p1)
    {
        return foldl(routables, (v, p, a, f) -> f.apply(v, p, a), accumulator, p1, fold);
    }

    public <V2, P1, P2> V2 foldl(Routables<?> routables, QuadFunction<E, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError("Unknown domain: " + routables.domain());
            case Key: return foldl((AbstractKeys<?>) routables, fold, accumulator, p1, p2);
            case Range: return foldl((AbstractRanges) routables, fold, accumulator, p1, p2);
        }
    }

    public <V2, P1, P2> V2 foldl(AbstractKeys<?> keys, QuadFunction<E, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2)
    {
        if (isEmpty())
            return accumulator;

        return ReducingBTree.foldl(tree, keys, 0, keys.size(), fold, accumulator, p1, p2);
    }

    public <V2, P1, P2> V2 foldl(AbstractRanges ranges, QuadFunction<E, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2)
    {
        if (isEmpty())
            return accumulator;

        return ReducingBTree.foldl(tree, ranges, 0, ranges.size(), fold, accumulator, p1, p2);
    }

    public <V2> V2 foldlWithDefault(Routables<?> routables, BiFunction<E, V2, V2> fold, E ifNull, V2 accumulator)
    {
        return foldlWithDefault(routables, (a, b, f, ignore) -> f.apply(a, b), ifNull, accumulator, fold, null);
    }

    public <V2, P1> V2 foldlWithDefault(Routables<?> routables, TriFunction<E, V2, P1, V2> fold, E ifNull, V2 accumulator, P1 p1)
    {
        return foldlWithDefault(routables, (v, p, a, f) -> f.apply(v, p, a), ifNull, accumulator, p1, fold);
    }

    public <V2, P1, P2> V2 foldlWithDefault(Routables<?> routables, QuadFunction<E, V2, P1, P2, V2> fold, E ifNull, V2 accumulator, P1 p1, P2 p2)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError("Unknown domain: " + routables.domain());
            case Key: return foldlWithDefault((AbstractKeys<?>) routables, fold, ifNull, accumulator, p1, p2);
            case Range: return foldlWithDefault((AbstractRanges) routables, fold, ifNull, accumulator, p1, p2);
        }
    }

    // TODO (expected): should foldlWithDefault just have a default result?
    public <V2, P1, P2> V2 foldlWithDefault(AbstractKeys<?> keys, QuadFunction<E, V2, P1, P2, V2> fold, E ifNull, V2 accumulator, P1 p1, P2 p2)
    {
        return ReducingBTree.foldlWithDefault(tree, keys, 0, keys.size(), fold, ifNull, accumulator, p1, p2);
    }

    public <V2, P1, P2> V2 foldlWithDefault(AbstractRanges ranges, QuadFunction<E, V2, P1, P2, V2> fold, E ifNull, V2 accumulator, P1 p1, P2 p2)
    {
        return ReducingBTree.foldlWithDefault(tree, ranges, 0, ranges.size(), fold, ifNull, accumulator, p1, p2);
    }

    public int findIndex(RoutableKey key)
    {
        return BTree.<Entry<?>, RoutableKey>findIndex(tree, Entry::compare, key);
    }

    public static <E extends Entry<E>, M> M create(Routables<?> rs, E value, Function<Object[], M> mapConstructor)
    {
        return create(rs, value, (r, e) -> e.with(r.start(), r.end()), mapConstructor);
    }

    public static <V, E extends Entry<E>, M> M create(Routables<?> rs, V value, BiFunction<Range, V, E> entryConstructor, Function<Object[], M> mapConstructor)
    {
        Invariants.requireArgument(value != null, "value is null");

        try (AnyBuilder<E, M> builder = new AnyBuilder<>())
        {
            for (int i = 0, size = rs.size() ; i < size ; ++i)
                builder.append(entryConstructor.apply(rs.get(i).toUnseekable().asRange(), value));
            return builder.build(mapConstructor);
        }
    }

    public static <E extends Entry<E>, M> M add(BTreeReducingRangeMap<E> existing, Routables<?> keysOrRanges, E value,
                                                   QuadFunction<RoutingKey, RoutingKey, E, E, E> reduce,
                                                   Function<Object[], M> mapConstructor)
    {
        return merge(existing.tree, create(keysOrRanges, value, i -> i), reduce, mapConstructor);
    }

    public static <V, E extends Entry<E>, M> M add(BTreeReducingRangeMap<E> existing, Routables<?> keysOrRanges, V value,
                                                   BiFunction<Range, V, E> entryConstructor,
                                                   QuadFunction<RoutingKey, RoutingKey, E, E, E> reduce,
                                                   Function<Object[], M> mapConstructor)
    {
        return merge(existing.tree, create(keysOrRanges, value, entryConstructor, i -> i), reduce, mapConstructor);
    }

    public static <E extends Entry<E>, M> M merge(BTreeReducingRangeMap<E> historyLeft, BTreeReducingRangeMap<E> historyRight, QuadFunction<RoutingKey, RoutingKey, E, E, E> reduce, Function<Object[], M> constructor)
    {
        return merge(historyLeft.tree, historyRight.tree, reduce, constructor);
    }

    private static <E extends Entry<E>, M> M merge(Object[] left, Object[] right, QuadFunction<RoutingKey, RoutingKey, E, E, E> reduce, Function<Object[], M> constructor)
    {
        return constructor.apply(ReducingBTree.merge(left, right, reduce));
    }

    public Ranges ranges(Predicate<E> include)
    {
        Range[] ranges = new Range[size()];
        Iterator<E> iter = iterator();
        int count = 0;
        while (iter.hasNext())
        {
            E next = iter.next();
            if (include.test(next))
                ranges[count++] = next.toPlainRange();
        }
        if (count < ranges.length)
            ranges = Arrays.copyOf(ranges, count);
        return Ranges.ofSortedAndDeoverlapped(ranges);
    }


    public static class AnyBuilder<E extends Entry<E>, M> implements Closeable
    {
        private BTree.FastBuilder<E> treeBuilder;
        private E prev;

        protected AnyBuilder()
        {
        }

        public AnyBuilder<E, M> append(E entry)
        {
            Invariants.requireArgument(prev == null || entry.start().compareTo(prev.end()) >= 0);
            if (treeBuilder == null)
                treeBuilder = BTree.fastBuilder();
            treeBuilder.add(entry);
            prev = entry;
            return this;
        }

        protected M build(Function<Object[], M> constructor)
        {
            return constructor.apply(treeBuilder == null ? BTree.empty() : treeBuilder.build());
        }

        @Override
        public void close()
        {
            if (treeBuilder != null)
            {
                treeBuilder.close();
                treeBuilder = null;
                prev = null;
            }
        }
    }

    /**
     * A non-validating builder that expects all entries to be in correct order. For implementations' ser/de logic.
     */
    public abstract static class Builder<E extends Entry<E>, M> extends AnyBuilder<E, M>
    {
        public abstract M build();
    }

    public static boolean isWellFormed(BTreeReducingRangeMap<?> map)
    {
        return ReducingBTree.isWellFormed(map.tree);
    }
}
