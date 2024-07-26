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
import accord.utils.btree.BTreeRemoval;
import accord.utils.btree.UpdateFunction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static accord.utils.Invariants.checkArgument;
import static accord.utils.SortedArrays.Search.FAST;

public class BTreeReducingRangeMap<V> extends BTreeReducingIntervalMap<RoutingKey, V>
{
    public BTreeReducingRangeMap()
    {
        super();
    }

    protected BTreeReducingRangeMap(boolean inclusiveEnds, Object[] tree)
    {
        super(inclusiveEnds, tree);
    }

    public V foldl(Routables<?> routables, BiFunction<V, V, V> fold, V accumulator)
    {
        return foldl(routables, fold, accumulator, ignore -> false);
    }

    public <V2> V2 foldl(Routables<?> routables, BiFunction<V, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
    {
        return foldl(routables, (a, b, f, ignore) -> f.apply(a, b), accumulator, fold, null, terminate);
    }

    public <V2, P1, P2> V2 foldl(Routables<?> routables, QuadFunction<V, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        switch (routables.domain())
        {
            default: throw new AssertionError("Unknown domain: " + routables.domain());
            case Key: return foldl((AbstractKeys<?>) routables, fold, accumulator, p1, p2, terminate);
            case Range: return foldl((AbstractRanges) routables, fold, accumulator, p1, p2, terminate);
        }
    }

    public <V2, P1, P2> V2 foldl(AbstractKeys<?> keys, QuadFunction<V, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        if (isEmpty())
            return accumulator;

        int treeSize = BTree.size(tree),
            keysSize = keys.size();

        int i = keys.find(startAt(0), FAST);
        if (i < 0) i = -1 - i;
        else if (inclusiveEnds) ++i;

        while (i < keysSize)
        {
            int idx = findIndex(keys.get(i));
            if (idx < 0) idx = -2 - idx;
            else if (inclusiveEnds) --idx;

            if (idx >= treeSize - 1)
                return accumulator;

            int nexti = keys.findNext(i, startAt(idx + 1), FAST);
            if (nexti < 0) nexti = -1 -nexti;
            else if (inclusiveEnds) ++nexti;

            Entry<RoutingKey, V> entry = entryAt(idx);
            if (i != nexti && entry.hasValue() && entry.value() != null)
            {
                accumulator = fold.apply(entry.value(), accumulator, p1, p2);
                if (terminate.test(accumulator))
                    return accumulator;
            }
            i = nexti;
        }
        return accumulator;
    }

    public <V2, P1, P2> V2 foldl(AbstractRanges ranges, QuadFunction<V, V2, P1, P2, V2> fold, V2 accumulator, P1 p1, P2 p2, Predicate<V2> terminate)
    {
        if (isEmpty())
            return accumulator;

        int treeSize = BTree.size(tree),
            rangesSize = ranges.size();

        RoutingKey start = startAt(0);
        int i = ranges.find(start, FAST);
        if (i < 0) i = -1 - i;
        else if (inclusiveEnds && ranges.get(i).end().equals(start)) ++i;

        while (i < rangesSize)
        {
            Range range = ranges.get(i++);

            int startIdx = findIndex(range.start());
            int startPos = startIdx < 0 ? (-1 - startIdx) : startIdx;
            if (startIdx == treeSize - 1 || startPos == treeSize)
                return accumulator;  // is last or out of bounds -> we are done
            if (startIdx < 0) startPos = Math.max(0, startPos - 1); // inclusive

            int endIdx = findIndex(range.end());
            int endPos = endIdx < 0 ? (-1 - endIdx) : endIdx;
            if (endPos == 0)
                continue; // is first or out of bounds -> continue
            endPos = Math.min(endPos - 1, treeSize - 2); // inclusive

            Iterator<Entry<RoutingKey,V>> iterator =
                BTree.iterator(tree, startPos, endPos, BTree.Dir.ASC);

            while (iterator.hasNext())
            {
                Entry<RoutingKey, V> entry = iterator.next();
                if (entry.hasValue() && entry.value() != null)
                {
                    accumulator = fold.apply(entry.value(), accumulator, p1, p2);
                    if (terminate.test(accumulator))
                        return accumulator;
                }
            }

            if (endPos >= treeSize - 2)
                return accumulator;

            RoutingKey nextStart = startAt(endPos + 1);
            i = ranges.findNext(i, nextStart, FAST);
            if (i < 0) i = -1 - i;
            else if (inclusiveEnds && ranges.get(i).end().equals(nextStart)) ++i;
        }

        return accumulator;
    }

    public int findIndex(RoutableKey key)
    {
        return BTree.findIndex(tree, EntryComparator.instance(), key);
    }

    public static <V> BTreeReducingRangeMap<V> create(AbstractRanges ranges, V value)
    {
        checkArgument(value != null, "value is null");

        if (ranges.isEmpty())
            return new BTreeReducingRangeMap<>();

        return create(ranges, value, BTreeReducingRangeMap.Builder::new);
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(Unseekables<?> keysOrRanges, V value, BoundariesBuilderFactory<RoutingKey, V, M> builder)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + keysOrRanges.domain());
            case Range: return create((AbstractRanges) keysOrRanges, value, builder);
            case Key: return create((AbstractUnseekableKeys) keysOrRanges, value, builder);
        }
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(Seekables<?, ?> keysOrRanges, V value, BoundariesBuilderFactory<RoutingKey, V, M> builder)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + keysOrRanges.domain());
            case Range: return create((AbstractRanges) keysOrRanges, value, builder);
            case Key: return create((Keys) keysOrRanges, value, builder);
        }
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(AbstractRanges ranges, V value, BoundariesBuilderFactory<RoutingKey, V, M> factory)
    {
        checkArgument(value != null, "value is null");

        AbstractBoundariesBuilder<RoutingKey, V, M> builder = factory.create(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (Range cur : ranges)
        {
            builder.append(cur.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(cur.end(), null, (a, b) -> { throw new IllegalStateException(); });
        }

        return builder.build();
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(AbstractUnseekableKeys keys, V value, BoundariesBuilderFactory<RoutingKey, V, M> factory)
    {
        checkArgument(value != null, "value is null");

        AbstractBoundariesBuilder<RoutingKey, V, M> builder = factory.create(keys.get(0).asRange().endInclusive(), keys.size() * 2);
        for (int i = 0 ; i < keys.size() ; ++i)
        {
            Range range = keys.get(i).asRange();
            builder.append(range.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(range.end(), null, (a, b) -> { throw new IllegalStateException(); });
        }

        return builder.build();
    }

    public static <V, M extends BTreeReducingRangeMap<V>> M create(Keys keys, V value, BoundariesBuilderFactory<RoutingKey, V, M> factory)
    {
        checkArgument(value != null, "value is null");

        RoutingKey prev = keys.get(0).toUnseekable();
        AbstractBoundariesBuilder<RoutingKey, V, M> builder;
        {
            Range range = prev.asRange();
            builder = factory.create(prev.asRange().endInclusive(), keys.size() * 2);
            builder.append(range.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(range.end(), null, (a, b) -> { throw new IllegalStateException(); });
        }

        for (int i = 1 ; i < keys.size() ; ++i)
        {
            RoutingKey unseekable = keys.get(i).toUnseekable();
            if (unseekable.equals(prev))
                continue;

            Range range = unseekable.asRange();
            builder.append(range.start(), value, (a, b) -> { throw new IllegalStateException(); });
            builder.append(range.end(), null, (a, b) -> { throw new IllegalStateException(); });
            prev = unseekable;
        }

        return builder.build();
    }

    public static <V> BTreeReducingRangeMap<V> add(BTreeReducingRangeMap<V> existing, Ranges ranges, V value, BiFunction<V, V, V> reduce)
    {
        return update(existing, ranges, value, reduce, BTreeReducingRangeMap::new);
    }

    public static BTreeReducingRangeMap<Timestamp> add(BTreeReducingRangeMap<Timestamp> existing, Ranges ranges, Timestamp value)
    {
        return add(existing, ranges, value, Timestamp::max);
    }

    public static <V> BTreeReducingRangeMap<V> merge(BTreeReducingRangeMap<V> historyLeft, BTreeReducingRangeMap<V> historyRight, BiFunction<V, V, V> reduce)
    {
        return BTreeReducingIntervalMap.merge(historyLeft, historyRight, reduce, BTreeReducingRangeMap.Builder::new);
    }

    /**
     *  Update the range map retaining as much of the underlying BTree as possible
     */
    public static <M extends BTreeReducingRangeMap<V>, V> M update(
        M map, Unseekables<?> keysOrRanges, V value, BiFunction<V, V, V> valueResolver,
        BiFunction<Boolean, Object[], M> factory, BoundariesBuilderFactory<RoutingKey, V, M> builderFactory)
    {
        if (keysOrRanges.isEmpty())
            return map;

        if (map.isEmpty())
            return create(keysOrRanges, value, builderFactory);

        if (map.inclusiveEnds() != keysOrRanges.get(0).toUnseekable().asRange().endInclusive())
            throw new IllegalStateException("Mismatching bound inclusivity/exclusivity - can't be updated");

        return update(map, keysOrRanges, value, valueResolver, factory);
    }

    private static <M extends BTreeReducingRangeMap<V>, V> M update(
            M map, Unseekables<?> keysOrRanges, V value, BiFunction<V, V, V> valueResolver, BiFunction<Boolean, Object[], M> factory)
    {
        Accumulator<V> acc = accumulator();

        int treeSize = BTree.size(map.tree);
        boolean updatedEndEntry = false;

        Range thisRange, nextRange = null;
        for (int i = 0, rangesSize = keysOrRanges.size(); i < rangesSize; ++i)
        {
            thisRange = i == 0 ? keysOrRanges.get(i).toUnseekable().asRange() : nextRange;
            nextRange = i < rangesSize - 1 ? keysOrRanges.get(i + 1).toUnseekable().asRange() : null;

            assert thisRange != null;

            int startIdx = map.findIndex(thisRange.start());
            int   endIdx = map.findIndex(thisRange.end());

            int startIns = startIdx >= 0 ? startIdx : -1 - startIdx;
            int   endIns =   endIdx >= 0 ?   endIdx : -1 - endIdx;

            boolean isRangeOpen = false;

            if (startIdx < 0) // if we start on an existing bound, we don't have to do anything *here*
            {
                if (startIns == 0) // insert before first map entry
                {
                    acc.add(thisRange.start(), value);
                    isRangeOpen = true;
                }
                else if (startIns == treeSize) // inserting past last map entry
                {
                    // when adding past current end, need to update the very last entry from having no value to having value = null
                    if (!updatedEndEntry)
                    {
                        acc.add(map.startAt(treeSize - 1), null);
                        updatedEndEntry = true;
                    }
                    acc.add(thisRange.start(), value);
                    isRangeOpen = true;
                }
                else // start within the current map bounds
                {
                    // split the range we start in if our value is higher than the range's
                    if (supersedes(map.entryAt(startIns - 1), value, valueResolver))
                    {
                        acc.add(thisRange.start(), value);
                        isRangeOpen = true;
                    }
                }
            }

            if (startIns < endIns)
            {
                // start and end are inclusive with BTree iterator
                Iterator<Entry<RoutingKey, V>> iter = BTree.iterator(map.tree, startIns, endIns - 1, BTree.Dir.ASC);
                while (iter.hasNext())
                {
                    Entry<RoutingKey, V> entry = iter.next();
                    boolean supersedes = supersedes(entry, value, valueResolver);
                    if (supersedes)
                    {
                        if (isRangeOpen)
                            acc.remove(entry.start());
                        else
                            acc.add(entry.start(), value);
                    }
                    isRangeOpen = supersedes;
                }
            }

            if (endIdx < 0) // if we end on an existing bound, we don't have to do anything
            {
                if (endIns == 0) // range ends before first entry in the map
                {
                    acc.add(thisRange.end(), null);
                }
                else if (endIns == treeSize) // range ends after last entry in the map
                {
                    if (nextRange == null)
                        acc.add(thisRange.end());
                    else
                        acc.add(thisRange.end(), null);

                    updatedEndEntry = true;
                }
                else // ends between two existing map bounds
                {
                    // split the range we end in if our value is higher than the range's
                    if (isRangeOpen) acc.add(thisRange.end(), map.valueAt(endIns - 1));
                }
            }
        }

        Object[] updated = acc.apply(map.tree); acc.reuse();
        return map.tree == updated
             ? map
             : factory.apply(map.inclusiveEnds(), updated);
    }

    private static final ThreadLocal<Accumulator<?>> accumulator = ThreadLocal.withInitial(Accumulator::new);

    @SuppressWarnings("unchecked")
    private static <V> Accumulator<V> accumulator()
    {
        return (Accumulator<V>) accumulator.get().reuse();
    }

    private static class Accumulator<V> implements BTree.Builder.QuickResolver<Entry<RoutingKey, V>>
    {
        private boolean isClean = true;

        BTree.Builder<Entry<RoutingKey, V>> toAdd;
        List<RoutingKey> toRemove;

        void remove(RoutingKey key)
        {
            isClean = false;
            if (toRemove == null)
                toRemove = new ArrayList<>();
            toRemove.add(key);
        }

        void add(RoutingKey key)
        {
            isClean = false;
            toAdd().add(Entry.make(key));
        }

        void add(RoutingKey key, V value)
        {
            isClean = false;
            toAdd().add(Entry.make(key, value));
        }

        private BTree.Builder<Entry<RoutingKey, V>> toAdd()
        {
            if (toAdd == null)
            {
                toAdd = BTree.builder(Comparator.naturalOrder());
                toAdd.setQuickResolver(this);
            }
            return toAdd;
        }

        Object[] apply(Object[] tree)
        {
            if (toRemove != null)
                for (RoutingKey key : toRemove)
                    tree = BTreeRemoval.remove(tree, EntryComparator.instance(), key);

            if (toAdd != null && !toAdd.isEmpty())
                tree = BTree.update(tree, toAdd.build(), Comparator.<Entry<RoutingKey, V>>naturalOrder(), UpdateFunction.noOpReplace());

            return tree;
        }

        @Override
        public Entry<RoutingKey, V> resolve(Entry<RoutingKey, V> left, Entry<RoutingKey, V> right)
        {
            if (left.hasValue() != right.hasValue())
                return left.hasValue() ? left : right;

            if (!left.hasValue())
                return left;

            if ((left.value() == null) != (right.value() == null))
                return left.value() == null ? right : left;

            return right;
        }

        Accumulator<V> reuse()
        {
            if (!isClean)
            {
                if (toRemove != null) toRemove.clear();
                if (toAdd != null) toAdd.reuse();
                isClean = true;
            }
            return this;
        }
    }

    private static <V> boolean supersedes(Entry<RoutingKey, V> entry, V value, BiFunction<V, V, V> resolver)
    {
        return !entry.hasValue()
            || (entry.value() == null && value != null)
            || (entry.value() != null && value != null && !resolver.apply(entry.value(), value).equals(entry.value()));
    }

    static class Builder<V> extends AbstractBoundariesBuilder<RoutingKey, V, BTreeReducingRangeMap<V>>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected BTreeReducingRangeMap<V> buildInternal(Object[] tree)
        {
            return new BTreeReducingRangeMap<>(inclusiveEnds, tree);
        }
    }

    /**
     * A non-validating builder that expects all entries to be in correct order. For implementations' ser/de logic.
     */
    public static class RawBuilder<V, M>
    {
        protected final boolean inclusiveEnds;
        protected final int capacity;

        private BTree.Builder<Entry<RoutingKey, V>> treeBuilder;
        private boolean lastStartAppended;

        public RawBuilder(boolean inclusiveEnds, int capacity)
        {
            this.inclusiveEnds = inclusiveEnds;
            this.capacity = capacity;
        }

        public RawBuilder<V, M> append(RoutingKey start, V value)
        {
            return append(Entry.make(start, value));
        }

        public RawBuilder<V, M> append(RoutingKey start)
        {
            return append(Entry.make(start));
        }

        public RawBuilder<V, M> append(Entry<RoutingKey, V> entry)
        {
            Invariants.checkState(!lastStartAppended);
            if (treeBuilder == null)
                (treeBuilder = BTree.builder(Comparator.naturalOrder(), capacity + 1)).auto(false);
            treeBuilder.add(entry);
            lastStartAppended = !entry.hasValue();
            return this;
        }

        public final M build(BiFunction<Boolean, Object[], M> constructor)
        {
            Invariants.checkState(lastStartAppended || treeBuilder == null);
            return constructor.apply(inclusiveEnds, treeBuilder == null ? BTree.empty() : treeBuilder.build());
        }
    }
}
