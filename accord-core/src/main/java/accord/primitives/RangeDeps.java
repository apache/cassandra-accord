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

package accord.primitives;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.utils.ArrayBuffers;
import accord.utils.IndexedBiConsumer;
import accord.utils.IndexedConsumer;
import accord.utils.IndexedFunction;
import accord.utils.IndexedQuadConsumer;
import accord.utils.IndexedRangeQuadConsumer;
import accord.utils.Invariants;
import accord.utils.RelationMultiMap;
import accord.utils.RelationMultiMap.MergeAdapter;
import accord.utils.SearchableRangeList;
import accord.utils.SortedArrays;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedList;
import accord.utils.SymmetricComparator;
import accord.utils.TriFunction;
import net.nicoulaj.compilecommand.annotations.DontInline;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.ObjectBuffers;
import static accord.utils.ArrayBuffers.cachedInts;
import static accord.utils.ArrayBuffers.cachedRanges;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.RelationMultiMap.AbstractBuilder;
import static accord.utils.RelationMultiMap.Adapter;
import static accord.utils.RelationMultiMap.LinearMerger;
import static accord.utils.RelationMultiMap.NO_INTS;
import static accord.utils.RelationMultiMap.SortedRelationList;
import static accord.utils.RelationMultiMap.endOffset;
import static accord.utils.RelationMultiMap.invert;
import static accord.utils.RelationMultiMap.linearUnion;
import static accord.utils.RelationMultiMap.newIterator;
import static accord.utils.RelationMultiMap.startOffset;
import static accord.utils.RelationMultiMap.testEquality;
import static accord.utils.RelationMultiMap.trimUnusedValues;
import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * <p>Maintains a lazily-constructed, bidirectional map between Range and TxnId.
 * <p>Ranges are stored sorted by start then end, and indexed by a secondary {@link SearchableRangeList} structure.
 * <p>The relationship between Range and TxnId is maintained via {@code int[]} utilising {@link RelationMultiMap}
 * functionality.
 *
 * TODO (expected): de-overlap ranges per txnId if possible cheaply, or else reduce use of partial ranges where possible
 * TODO (expected): keep only the latest exclusive sync point for a given range
 * TODO (expected): permit building out-of-order
 * TODO (expected): currently permitting duplicates
 * TODO (testing): randomised testing of all iteration methods
 */
public class RangeDeps implements Iterable<Map.Entry<Range, TxnId>>
{
    public static class SerializerSupport
    {
        private SerializerSupport() {}

        public static int rangesToTxnIdsCount(RangeDeps deps)
        {
            return deps.rangesToTxnIds.length;
        }

        public static int rangesToTxnIds(RangeDeps deps, int idx)
        {
            return deps.rangesToTxnIds[idx];
        }

        public static RangeDeps create(Range[] ranges, TxnId[] txnIds, int[] rangesToTxnIds)
        {
            return new RangeDeps(ranges, txnIds, rangesToTxnIds);
        }
    }

    private static final Range[] NO_RANGES = new Range[0];
    public static final RangeDeps NONE = new RangeDeps(new Range[0], new TxnId[0], new int[0], new int[0]);

    final TxnId[] txnIds;
    // the list of ranges and their mappings to txnIds
    // unique, and sorted by start()
    final Range[] ranges;
    /**
     * See {@link RelationMultiMap}.
     * TODO consider alternative layout depending on real-world data distributions:
     *      if most ranges have at most TxnId (or vice-versa) might be better to use negative values
     *      to index into the dynamic portion of the array. We started with this, but decided it was
     *      hard to justify the extra work for two layouts for the moment.
     */
    final int[] rangesToTxnIds;
    int[] txnIdsToRanges;

    private SearchableRangeList searchable;
    private Ranges covering;

    public static LinearMerger<Range, TxnId, RangeDeps> newMerger()
    {
        return new LinearMerger<>(ADAPTER);
    }

    // TODO (expected): merge by TxnId key, not by range, so that we can merge overlapping ranges for same TxnId
    public static <C, T1, T2> RangeDeps merge(C merge, int mergeSize, IndexedFunction<C, T1> getter1, Function<T1, T2> getter2, Function<T2, RangeDeps> getter3)
    {
        try (LinearMerger<Range, TxnId, RangeDeps> linearMerger = newMerger())
        {
            int mergeIndex = 0;
            while (mergeIndex < mergeSize)
            {
                T1 t1 = getter1.apply(merge, mergeIndex++);
                if (t1 == null) continue;

                T2 t2 = getter2.apply(t1);
                if (t2 == null) continue;

                RangeDeps deps = getter3.apply(t2);
                if (deps == null || deps.isEmpty())
                    continue;

                linearMerger.update(deps, deps.ranges, deps.txnIds, deps.rangesToTxnIds);
            }

            return linearMerger.get(RangeDeps::new, NONE);
        }
    }

    public static RangeDeps merge(Stream<RangeDeps> merge)
    {
        try (LinearMerger<Range, TxnId, RangeDeps> linearMerger = newMerger())
        {
            merge.forEach(deps -> {
                if (!deps.isEmpty())
                    linearMerger.update(deps, deps.ranges, deps.txnIds, deps.rangesToTxnIds);
            });

            return linearMerger.get(RangeDeps::new, NONE);
        }
    }

    private RangeDeps(Range[] ranges, TxnId[] txnIds, int[] rangesToTxnIds)
    {
        this(ranges, txnIds, rangesToTxnIds, null);
    }

    private RangeDeps(Range[] ranges, TxnId[] txnIds, int[] rangesToTxnIds, int[] txnIdsToRanges)
    {
        Invariants.checkArgument(rangesToTxnIds.length >= ranges.length);
        Invariants.checkArgument(ranges.length > 0 || rangesToTxnIds.length == 0);
        Invariants.paranoid(SortedArrays.isSorted(ranges, Range::compare));
        this.ranges = ranges;
        this.txnIds = txnIds;
        this.rangesToTxnIds = rangesToTxnIds;
        this.txnIdsToRanges = txnIdsToRanges;
    }

    public void forEach(RoutableKey key, Consumer<TxnId> forEach)
    {
        forEach(key, Consumer::accept, forEach, 0, null);
    }

    @Inline
    public <P1, P2, P3, P4> int forEach(RoutableKey key, IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange, P1 p1, P2 p2, P3 p3, P4 p4, int minIndex)
    {
        return ensureSearchable().forEachKey(key, forEachScanOrCheckpoint, forEachRange, p1, p2, p3, p4, minIndex);
    }

    private <P1> int forEach(RoutableKey key, BiConsumer<P1, TxnId> forEach, P1 p1, int minIndex, @Nullable BitSet visited)
    {
        return forEach(key, RangeDeps::visitTxnIdsForRangeIndex, RangeDeps::visitTxnIdsForRangeIndex,
                this, forEach, p1, visited, minIndex);
    }

    private <P> int forEach(RoutableKey key, IndexedConsumer<P> forEach, P param, int minIndex)
    {
        return forEach(key, IndexedConsumer::accept, forEach, param, minIndex);
    }

    private <P1, P2> int forEach(RoutableKey key, IndexedBiConsumer<P1, P2> forEach, P1 p1, P2 p2, int minIndex)
    {
        return forEach(key, RangeDeps::visitTxnIdxsForRangeIndex, RangeDeps::visitTxnIdxsForRangeIndex,
                this, forEach, p1, p2, minIndex);
    }

    @Inline
    public <P1, P2, P3, P4> int forEach(Range range, IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange, P1 p1, P2 p2, P3 p3, P4 p4, int minIndex)
    {
        return ensureSearchable().forEachRange(range, forEachScanOrCheckpoint, forEachRange, p1, p2, p3, p4, minIndex);
    }

    @Inline
    public <P1, P2, P3, P4> int forEach(RoutingKey start, RoutingKey end, IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange, P1 p1, P2 p2, P3 p3, P4 p4, int minIndex)
    {
        return ensureSearchable().forEachRange(start, end, forEachScanOrCheckpoint, forEachRange, p1, p2, p3, p4, minIndex);
    }

    private <P1, P2, P3, P4> void forEach(AbstractRanges ranges, IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange, P1 p1, P2 p2, P3 p3, P4 p4)
    {
        int minIndex = 0;
        for (int i = 0; i < ranges.size() ; ++i)
            minIndex = forEach(ranges.get(i), forEachScanOrCheckpoint, forEachRange, p1, p2, p3, p4, minIndex);
    }

    private <P1, P2, P3, P4> void forEach(AbstractUnseekableKeys keys, IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange, P1 p1, P2 p2, P3 p3, P4 p4)
    {
        int minIndex = 0;
        for (int i = 0; i < keys.size() ; ++i)
            minIndex = forEach(keys.get(i), forEachScanOrCheckpoint, forEachRange, p1, p2, p3, p4, minIndex);
    }

    private <P1> int forEach(Range range, BiConsumer<P1, TxnId> forEach, P1 p1, int minIndex, @Nullable BitSet visited)
    {
        return forEach(range, RangeDeps::visitTxnIdsForRangeIndex, RangeDeps::visitTxnIdsForRangeIndex,
                this, forEach, p1, visited, minIndex);
    }

    public <P1, P2> int forEach(Range range, IndexedBiConsumer<P1, P2> forEach, P1 p1, P2 p2, int minIndex)
    {
        return forEach(range, RangeDeps::visitTxnIdxsForRangeIndex, RangeDeps::visitTxnIdxsForRangeIndex,
                this, forEach, p1, p2, minIndex);
    }

    public <P1, P2> int forEach(RoutingKey start, RoutingKey end, IndexedBiConsumer<P1, P2> forEach, P1 p1, P2 p2, int minIndex)
    {
        return forEach(start, end, RangeDeps::visitTxnIdxsForRangeIndex, RangeDeps::visitTxnIdxsForRangeIndex,
                this, forEach, p1, p2, minIndex);
    }

    private <P1> void visitTxnIdsForRangeIndex(BiConsumer<P1, TxnId> forEach, P1 p1, @Nullable BitSet visited, int rangeIndex)
    {
        for (int i = startOffset(ranges, rangesToTxnIds, rangeIndex), end = endOffset(rangesToTxnIds, rangeIndex) ; i < end ; ++i)
            visitTxnId(rangesToTxnIds[i], forEach, p1, visited);
    }

    private <P1> void visitTxnIdsForRangeIndex(BiConsumer<P1, TxnId> forEach, P1 p1, @Nullable BitSet visited, int start, int end)
    {
        if (end <= start)
            return;
        for (int i = startOffset(ranges, rangesToTxnIds, start) ; i < endOffset(rangesToTxnIds, end - 1) ; ++i)
            visitTxnId(rangesToTxnIds[i], forEach, p1, visited);
    }

    // TODO (low priority, efficiency): ideally we would accept something like a BitHashSet or IntegerTrie
    //   as O(N) space needed for BitSet here (but with a very low constant multiplier)
    private <P1> void visitTxnId(int txnIdx, BiConsumer<P1, TxnId> forEach, P1 p1, @Nullable BitSet visited)
    {
        if (visited == null || !visited.get(txnIdx))
        {
            if (visited != null)
                visited.set(txnIdx);
            forEach.accept(p1, txnIds[txnIdx]);
        }
    }

    private <P1, P2> void visitTxnIdxsForRangeIndex(IndexedBiConsumer<P1, P2> forEach, P1 p1, P2 p2, int rangeIndex)
    {
        for (int i = startOffset(ranges, rangesToTxnIds, rangeIndex), end = endOffset(rangesToTxnIds, rangeIndex) ; i < end ; ++i)
            forEach.accept(p1, p2, rangesToTxnIds[i]);
    }

    private <P1, P2> void visitTxnIdxsForRangeIndex(IndexedBiConsumer<P1, P2> forEach, P1 p1, P2 p2, int start, int end)
    {
        if (end == 0)
            return;
        for (int i = startOffset(ranges, rangesToTxnIds, start) ; i < endOffset(rangesToTxnIds, end - 1) ; ++i)
            forEach.accept(p1, p2, rangesToTxnIds[i]);
    }

    /**
     * Each matching TxnId will be provided precisely once
     */
    public void forEachUniqueTxnId(RoutableKey key, Consumer<TxnId> forEach)
    {
        forEachUniqueTxnId(key, forEach, Consumer::accept);
    }

    /**
     * Each matching TxnId will be provided precisely once
     */
    public <P1> void forEachUniqueTxnId(RoutableKey key, P1 p1, BiConsumer<P1, TxnId> forEach)
    {
        forEach(key, forEach, p1, 0, new BitSet());
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public void forEach(Range range, Consumer<TxnId> forEach)
    {
        forEach(range, Consumer::accept, forEach, 0, null);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P1, P2> void forEach(Range range, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        forEach(range, forEach, p1, p2, 0);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P1> void forEach(AbstractRanges ranges, P1 p1, BiConsumer<P1, TxnId> forEach)
    {
        int minIndex = 0;
        for (int i = 0; i < ranges.size() ; ++i)
            minIndex = forEach(ranges.get(i), forEach, p1, minIndex, null);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P1> void forEach(Unseekables<?> unseekables, P1 p1, IndexedConsumer<P1> forEach)
    {
        forEach(unseekables, forEach, p1, IndexedConsumer::accept);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P1, P2> void forEach(Unseekables<?> unseekables, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        forEach(unseekables, null, 0, unseekables.size(), p1, p2, forEach);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     * @param slice is only useful in case unseekables is a Ranges, in which case we only visit the intersection of the slice and the ranges we walk
     *              for keys it is expected that the caller has already sliced in this manner
     */
    public <P1, P2> void forEach(Unseekables<?> unseekables, @Nullable Range slice, int from, int to, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        switch (unseekables.domain())
        {
            default: throw new AssertionError("Unknown domain: " + unseekables.domain());
            case Key:
                forEach((AbstractKeys<?>) unseekables, from, to, p1, p2, forEach);
                break;
            case Range:
                forEach((AbstractRanges) unseekables, slice, from, to, p1, p2, forEach);
                break;
        }
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P1, P2> void forEach(AbstractKeys<?> keys, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        forEach(keys, 0, keys.size(), p1, p2, forEach);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P1, P2> void forEach(AbstractRanges ranges, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        forEach(ranges, null, 0, ranges.size(), p1, p2, forEach);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P1, P2> void forEach(AbstractKeys<?> keys, int from, int to, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        int minIndex = 0;
        for (int i = from ; i < to ; ++i)
            minIndex = forEach(keys.get(i), forEach, p1, p2, minIndex);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P1, P2> void forEach(AbstractRanges ranges, @Nullable Range slice, int from, int to, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        int minIndex = 0;
        for (int i = from; i < to ; ++i)
        {
            Range range = ranges.get(i);
            RoutingKey start = range.start();
            RoutingKey end = range.end();
            if (slice != null)
            {
                if (slice.start().compareTo(start) > 0) start = slice.start();
                if (slice.end().compareTo(end) < 0) end = slice.end();
                if (end.compareTo(start) <= 0)
                    continue;
            }
            minIndex = forEach(start, end, forEach, p1, p2, minIndex);
        }
    }

    /**
     * Each matching TxnId will be provided precisely once
     */
    public void forEachUniqueTxnId(Range range, Consumer<TxnId> forEach)
    {
        forEach(range, Consumer::accept, forEach, 0, new BitSet());
    }

    /**
     * Each matching TxnId will be provided precisely once
     *
     * @param ranges to match on
     * @param forEach function to call on each unique {@link TxnId}
     */
    public void forEachUniqueTxnId(AbstractRanges ranges, Consumer<TxnId> forEach)
    {
        forEachUniqueTxnId(ranges, forEach, Consumer::accept);
    }

    /**
     * Each matching TxnId will be provided precisely once
     *
     * @param ranges to match on
     * @param forEach function to call on each unique {@link TxnId}
     */
    public <P1> void forEachUniqueTxnId(AbstractRanges ranges, P1 p1, BiConsumer<P1, TxnId> forEach)
    {
        int minIndex = 0;
        BitSet visited = new BitSet();
        for (int i = 0; i < ranges.size() ; ++i)
            minIndex = forEach(ranges.get(i), forEach, p1, minIndex, visited);
    }

    // return true iff we map any ranges to any txnId
    // if the mapping is empty we return false, whether or not we have any ranges or txnId by themselves
    public boolean isEmpty()
    {
        return RelationMultiMap.isEmpty(ranges, rangesToTxnIds);
    }

    public Ranges participants(TxnId txnId)
    {
        return ranges(txnId);
    }

    public Ranges ranges(TxnId txnId)
    {
        int txnIdx = Arrays.binarySearch(txnIds, txnId);
        if (txnIdx < 0)
            return Ranges.EMPTY;

        return ranges(txnIdx);
    }

    public int indexOf(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId);
    }

    public Ranges ranges(int txnIdx)
    {
        ensureTxnIdToRange();

        int start = txnIdx == 0 ? txnIds.length : txnIdsToRanges[txnIdx - 1];
        int end = txnIdsToRanges[txnIdx];
        if (start == end)
            return Ranges.EMPTY;

        Range[] result = new Range[end - start];
        result[0] = ranges[txnIdsToRanges[start]];
        int resultCount = 1;
        for (int i = start + 1 ; i < end ; ++i)
        {
            Range next = ranges[txnIdsToRanges[i]];
            if (!next.equals(result[resultCount - 1]))
                result[resultCount++] = next;
        }

        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);

        // it's possible to have overlapping ranges in the RangeDeps after merging; to avoid this we would need to merge
        // by txnId, or else have some post-filter, which probably isn't worth the effort.
        // This occurs when a range transaction or sync point is sliced differently on different replicas
        return Ranges.ofSorted(result);
    }

    public boolean intersects(TxnId txnId, Ranges ranges)
    {
        int txnIdx = Arrays.binarySearch(txnIds, txnId);
        if (txnIdx < 0)
            return false;

        return intersects(txnIdx, ranges);
    }

    public boolean intersects(int txnIdx, Ranges intersects)
    {
        ensureTxnIdToRange();

        int start = txnIdx == 0 ? txnIds.length : txnIdsToRanges[txnIdx - 1];
        int end = txnIdsToRanges[txnIdx];
        if (start == end)
            return false;

        int li = start, ri = 0;
        while (li < end && ri < intersects.size())
        {
            ri = intersects.findNext(ri, ranges[txnIdsToRanges[li]], FAST);
            if (ri >= 0) return true;
            ri = -1 - ri;
            ++li;
        }
        return false;
    }

    public boolean intersects(TxnId txnId, RoutableKey key)
    {
        int txnIdx = Arrays.binarySearch(txnIds, txnId);
        if (txnIdx < 0)
            throw new IllegalArgumentException("Key not found");

        return intersects(txnIdx, key);
    }

    public boolean intersects(int txnIdx, RoutableKey key)
    {
        ensureTxnIdToRange();

        int start = txnIdx == 0 ? txnIds.length : txnIdsToRanges[txnIdx - 1];
        int end = txnIdsToRanges[txnIdx];
        for (int i = start ; i < end ; ++i)
        {
            int c = ranges[i].compareTo(key);
            if (c == 0) return true;
            if (c > 0) return false;
        }
        return false;
    }

    public int indexOfStart(RoutableKey key)
    {
        return SortedArrays.binarySearch(ranges, 0, ranges.length, key, (k, r) -> k.compareTo(r.start()), CEIL);
    }

    public <P1, V> V foldEachRange(int txnIdx, P1 p1, V accumulate, TriFunction<P1, Range, V, V> fold)
    {
        ensureTxnIdToRange();

        int start = txnIdx == 0 ? txnIds.length : txnIdsToRanges[txnIdx - 1];
        int end = txnIdsToRanges[txnIdx];
        for (int i = start; i < end ; ++i)
            accumulate = fold.apply(p1, ranges[txnIdsToRanges[i]], accumulate);
        return accumulate;
    }

    void ensureTxnIdToRange()
    {
        if (txnIdsToRanges != null)
            return;

        txnIdsToRanges = invert(rangesToTxnIds, rangesToTxnIds.length, ranges.length, txnIds.length);
    }

    public RangeDeps intersecting(Unseekables<?> select)
    {
        switch (select.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + select.domain());
            case Key: return intersecting((AbstractUnseekableKeys) select);
            case Range: return slice((AbstractRanges) select);
        }
    }

    public RangeDeps slice(Ranges select)
    {
        return slice((AbstractRanges) select);
    }

    private RangeDeps slice(AbstractRanges select)
    {
        if (isEmpty())
            return new RangeDeps(NO_RANGES, txnIds, NO_INTS);

        try (RangeAndMapCollector collector = new RangeAndMapCollector(ensureSearchable().maxScanAndCheckpointMatches))
        {
            forEach(select, collector, collector, ranges, rangesToTxnIds, null, null);
            return build(collector);
        }
    }

    private RangeDeps intersecting(AbstractUnseekableKeys select)
    {
        if (isEmpty())
            return new RangeDeps(NO_RANGES, txnIds, NO_INTS);

        try (RangeAndMapCollector collector = new RangeAndMapCollector(ensureSearchable().maxScanAndCheckpointMatches))
        {
            forEach(select, collector, collector, ranges, rangesToTxnIds, null, null);
            return build(collector);
        }
    }

    private RangeDeps build(RangeAndMapCollector collector)
    {
        if (collector.rangesCount == 0)
            return new RangeDeps(NO_RANGES, NO_TXNIDS, NO_INTS);

        if (collector.rangesCount == this.ranges.length)
            return this;

        Range[] ranges = collector.getRanges();
        int[] rangesToTxnIds = collector.getRangesToTxnIds();
        TxnId[] txnIds = trimUnusedValues(ranges, this.txnIds, rangesToTxnIds, TxnId[]::new);
        return new RangeDeps(ranges, txnIds, rangesToTxnIds);
    }

    public RangeDeps with(RangeDeps that)
    {
        if (isEmpty() || that.isEmpty())
            return isEmpty() ? that : this;

        return linearUnion(
                this.ranges, this.ranges.length, this.txnIds, this.txnIds.length, this.rangesToTxnIds, this.rangesToTxnIds.length,
                that.ranges, that.ranges.length, that.txnIds, that.txnIds.length, that.rangesToTxnIds, that.rangesToTxnIds.length,
                rangeComparator(), TxnId::compareTo, null, null,
                cachedRanges(), cachedTxnIds(), cachedInts(),
                (ranges, rangesLength, txnIds, txnIdsLength, out, outLength) ->
                        new RangeDeps(cachedRanges().complete(ranges, rangesLength),
                                cachedTxnIds().complete(txnIds, txnIdsLength),
                                cachedInts().complete(out, outLength))
        );
    }

    public RangeDeps without(RangeDeps remove)
    {
        if (isEmpty() || remove.isEmpty()) return this;
        try (BuilderByTxnId builder = new BuilderByTxnId())
        {
            ensureTxnIdToRange();
            remove.ensureTxnIdToRange();
            if (!RelationMultiMap.removeWithPartialMatches(txnIds, ranges, txnIdsToRanges,
                                                           remove.txnIds, remove.ranges, remove.txnIdsToRanges,
                                                           TxnId::compareTo, Range::compareIntersecting, builder, (b, id, kr, rr) -> {
                Range remainder = null;
                if (rr != null)
                {
                    int compareStarts = rr.start().compareTo(kr.start());
                    if (rr.end().compareTo(kr.end()) < 0)
                        remainder = rr.newRange(compareStarts >= 0 ? rr.start() : kr.start(), rr.end());
                    if (compareStarts <= 0)
                        return remainder;
                    kr = kr.newRange(kr.start(), rr.start());
                }
                b.add(id, kr);
                return remainder;
            }))
            {
                return this;
            }
            return builder.build();
        }
    }

    public boolean contains(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId) >= 0;
    }

    public SortedArrayList<TxnId> txnIds()
    {
        return new SortedArrayList<>(txnIds);
    }

    public SortedRelationList<TxnId> txnIdsForRangeIndex(int rangeIndex)
    {
        Invariants.checkState(rangeIndex < ranges.length);
        int start = startOffset(ranges, rangesToTxnIds, rangeIndex);
        int end = endOffset(rangesToTxnIds, rangeIndex);
        Invariants.checkState(end >= start);
        return txnIds(rangesToTxnIds, start, end);
    }

    static class ListBuilder
    {
        TxnId[] buffer = NO_TXNIDS;
        int count;

        void add(TxnId txnId)
        {
            if (count == buffer.length)
                buffer = cachedTxnIds().resize(buffer, count, Math.max(8, count + (count >> 1)));
            buffer[count++] = txnId;
        }

        SortedList<TxnId> build()
        {
            TxnId[] txnIds = cachedTxnIds().completeAndDiscard(buffer, count);
            Arrays.sort(txnIds);
            return new SortedArrayList<>(txnIds);
        }
    }

    public SortedList<TxnId> computeTxnIds(RoutingKey key)
    {
        ListBuilder builder = new ListBuilder();
        forEachUniqueTxnId(key, builder::add);
        return builder.build();
    }

    public List<TxnId> computeTxnIds(Range key)
    {
        ListBuilder builder = new ListBuilder();
        forEachUniqueTxnId(key, builder::add);
        return builder.build();
    }

    private SortedRelationList<TxnId> txnIds(int[] ids, int start, int end)
    {
        if (start == end)
            return SortedRelationList.EMPTY;

        return new SortedRelationList<>(txnIds, ids, start, end);
    }

    public TxnId txnId(int i)
    {
        return txnIds[i];
    }

    public int txnIdCount()
    {
        return txnIds.length;
    }

    public Range range(int i)
    {
        return ranges[i];
    }

    public int rangeCount()
    {
        return ranges.length;
    }

    public TxnId maxTxnId(TxnId orElse)
    {
        return txnIdCount() == 0 ? orElse : txnId(txnIdCount() - 1);
    }

    public Ranges covering()
    {
        if (covering == null)
            covering = Ranges.ofSorted(ranges);
        return covering;
    }

    @Override
    public boolean equals(Object that)
    {
        return this == that || (that instanceof RangeDeps && equals((RangeDeps)that));
    }

    public boolean equals(RangeDeps that)
    {
        return Objects.equals(covering, that.covering) && testEquality(this.ranges, this.txnIds, this.rangesToTxnIds, that.ranges, that.txnIds, that.rangesToTxnIds);
    }

    @Override
    public String toString()
    {
        return RelationMultiMap.toSimpleString(ranges, txnIds, rangesToTxnIds);
    }

    public String toBriefString()
    {
        return RelationMultiMap.toBriefString(ranges, txnIds);
    }

    @Nonnull
    @Override
    public Iterator<Map.Entry<Range, TxnId>> iterator()
    {
        return newIterator(ranges, txnIds, rangesToTxnIds);
    }

    private SearchableRangeList ensureSearchable()
    {
        if (searchable == null)
            buildSearchable();
        return searchable;
    }

    @DontInline
    private void buildSearchable()
    {
        searchable = SearchableRangeList.build(ranges);
    }

    public boolean isSearchable()
    {
        return searchable != null;
    }

    static class RangeCollector implements
            IndexedRangeQuadConsumer<Range[], int[], Object, Object>,
            IndexedQuadConsumer<Range[], int[], Object, Object>,
            AutoCloseable
    {
        int[] oooBuffer;
        Range[] rangesOut;
        int oooCount, rangesCount;

        RangeCollector(int maxScanAndCheckpointCount)
        {
            oooBuffer = cachedInts().getInts(maxScanAndCheckpointCount);
            rangesOut = cachedRanges().get(32);
        }

        @Override
        public void accept(Range[] o, int[] o2, Object o3, Object o4, int index)
        {
            oooBuffer[oooCount++] = index;
        }

        @Override
        public void accept(Range[] ranges, int[] rangesToTxnIds, Object o3, Object o4, int fromIndex, int toIndex)
        {
            if (oooCount > 0)
            {
                Arrays.sort(oooBuffer, 0, oooCount);
                int oooCount = Arrays.binarySearch(oooBuffer, 0, this.oooCount, fromIndex);
                if (oooCount < 0) oooCount = -1 - oooCount;
                copy(ranges, rangesToTxnIds, oooCount, fromIndex, toIndex);
                this.oooCount = 0;
            }
            else if (fromIndex < toIndex)
            {
                copy(ranges, rangesToTxnIds, 0, fromIndex, toIndex);
            }
        }

        protected void copy(Range[] ranges, int[] rangesToTxnIds, int oooCount, int start, int end)
        {
            int count = oooCount + (end - start);
            if (rangesCount + count >= rangesOut.length)
                rangesOut = cachedRanges().resize(rangesOut, rangesCount, rangesCount + count + (rangesCount /2));
            for (int i = 0 ; i < oooCount ; ++i)
                rangesOut[rangesCount++] = ranges[oooBuffer[i]];
            for (int i = start ; i < end ; ++i)
                rangesOut[rangesCount++] = ranges[i];
        }

        Range[] getRanges()
        {
            Invariants.checkState(oooCount == 0);
            Range[] result = cachedRanges().completeAndDiscard(rangesOut, rangesCount);
            rangesOut = null;
            return result;
        }

        @Override
        public void close()
        {
            if (oooBuffer != null)
            {
                cachedInts().forceDiscard(oooBuffer);
                oooBuffer = null;
            }
            if (rangesOut != null)
            {
                cachedRanges().forceDiscard(rangesOut, rangesCount);
                rangesOut = null;
            }
        }
    }

    static class RangeAndMapCollector extends RangeCollector
    {
        int[] headers;
        int[] lists;
        int headerCount, listOffset;

        RangeAndMapCollector(int maxScanAndCheckpointCount)
        {
            super(maxScanAndCheckpointCount);
            headers = cachedInts().getInts(32);
            lists = cachedInts().getInts(32);
        }

        @Override
        protected void copy(Range[] ranges, int[] rangesToTxnIds, int oooCount, int start, int end)
        {
            super.copy(ranges, rangesToTxnIds, oooCount, start, end);
            int count = oooCount + (end - start);
            if (headerCount + count >= headers.length)
                headers = cachedInts().resize(headers, headerCount, headerCount + count + (headerCount /2));
            for (int i = 0 ; i < oooCount ; ++i)
            {
                int ri = oooBuffer[i];
                copyToDynamic(rangesToTxnIds, startOffset(ranges, rangesToTxnIds, ri), endOffset(rangesToTxnIds, ri));
                headers[headerCount++] = listOffset;
            }
            int startOffset = startOffset(ranges, rangesToTxnIds, start);
            for (int i = start ; i < end ; ++i)
                headers[this.headerCount++] = listOffset + rangesToTxnIds[i] - startOffset;
            copyToDynamic(rangesToTxnIds, startOffset, startOffset(ranges, rangesToTxnIds, end));
        }

        protected void copyToDynamic(int[] rangesToTxnIds, int start, int end)
        {
            int count = end - start;
            if (count + listOffset >= lists.length)
                lists = cachedInts().resize(lists, listOffset, listOffset + (listOffset /2) + count);
            System.arraycopy(rangesToTxnIds, start, lists, listOffset, count);
            listOffset += count;
        }

        public int[] getRangesToTxnIds()
        {
            int[] out = new int[headerCount + listOffset];
            for (int i = 0; i < headerCount; ++i)
                out[i] = headers[i] + headerCount;
            System.arraycopy(lists, 0, out, headerCount, listOffset);
            return out;
        }
    }

    public static RangeDeps of(Map<TxnId, Ranges> txnIdRanges)
    {
        if (txnIdRanges.isEmpty())
            return NONE;

        try (BuilderByTxnId builder = new BuilderByTxnId())
        {
            for (Map.Entry<TxnId, Ranges> e : txnIdRanges.entrySet())
            {
                builder.nextKey(e.getKey());
                Ranges ranges = e.getValue();
                for (int i = 0 ; i < ranges.size() ; ++i)
                    builder.add(ranges.get(i));
            }
            return builder.build();
        }
    }

    public static BuilderByRange builderByRange()
    {
        return new BuilderByRange();
    }

    public static abstract class AbstractRangeBuilder<K, V> extends AbstractBuilder<K, V, RangeDeps>
    {
        AbstractRangeBuilder(Adapter<K, V> adapter)
        {
            super(adapter);
        }
        public abstract void add(Range range, TxnId txnId);
    }

    public static final class BuilderByRange extends AbstractRangeBuilder<Range, TxnId>
    {
        public BuilderByRange()
        {
            super(ADAPTER);
        }

        @Override
        public void add(Range range, TxnId txnId)
        {
            super.add(range, txnId);
        }

        @Override
        protected RangeDeps none()
        {
            return RangeDeps.NONE;
        }

        @Override
        protected RangeDeps build(Range[] ranges, TxnId[] txnIds, int[] keyToValue)
        {
            return new RangeDeps(ranges, txnIds, keyToValue);
        }
    }

    public static BuilderByTxnId byTxnIdBuilder()
    {
        return new BuilderByTxnId();
    }

    public static final class BuilderByTxnId extends AbstractRangeBuilder<TxnId, Range>
    {
        public BuilderByTxnId()
        {
            super(REVERSE_ADAPTER);
        }

        @Override
        protected RangeDeps none()
        {
            return RangeDeps.NONE;
        }

        @Override
        protected RangeDeps build(TxnId[] txnIds, Range[] ranges, int[] txnIdsToRanges)
        {
            return new RangeDeps(ranges, txnIds, invert(txnIdsToRanges, txnIdsToRanges.length, txnIds.length, ranges.length), txnIdsToRanges);
        }

        @Override
        public void add(Range range, TxnId txnId)
        {
            if (txnId.equals(lastKey()))
            {
                Range last = lastValue();
                if (range.compareIntersecting(last) == 0)
                {
                    RoutingKey rstart = range.start(), lstart = last.start();
                    Invariants.checkState(rstart.compareTo(lstart) >= 0);
                    RoutingKey rend = range.end(), lend = last.end();
                    if (rend.compareTo(lend) > 0)
                        updateLast(last.newRange(lstart, rend));
                    return;
                }
            }
            else
            {
                nextKey(txnId);
            }
            add(range);
        }
    }

    public static SymmetricComparator<? super Range> rangeComparator()
    {
        return Range::compare;
    }

    private static final RangeDepsAdapter ADAPTER = new RangeDepsAdapter();
    private static final class RangeDepsAdapter implements MergeAdapter<Range, TxnId>
    {
        @Override public SymmetricComparator<? super Range> keyComparator() { return rangeComparator(); }
        @Override public SymmetricComparator<? super TxnId> valueComparator() { return TxnId::compareTo; }
        @Override public int compareKeys(Range a, Range b) { return a.compareTo(b); }
        @Override public int compareValues(TxnId a, TxnId b) { return a.compareTo(b); }
        @Override public ObjectBuffers<Range> cachedKeys() { return ArrayBuffers.cachedRanges(); }
        @Override public ObjectBuffers<TxnId> cachedValues() { return ArrayBuffers.cachedTxnIds(); }
    }

    private static final ReverseRangeDepsAdapter REVERSE_ADAPTER = new ReverseRangeDepsAdapter();
    private static final class ReverseRangeDepsAdapter implements MergeAdapter<TxnId, Range>
    {
        @Override public SymmetricComparator<? super TxnId> keyComparator() { return TxnId::compareTo; }
        @Override public SymmetricComparator<? super Range> valueComparator() { return rangeComparator(); }
        @Override public int compareKeys(TxnId a, TxnId b) { return a.compareTo(b); }
        @Override public int compareValues(Range a, Range b) { return a.compare(b); }
        @Override public ObjectBuffers<TxnId> cachedKeys() { return ArrayBuffers.cachedTxnIds(); }
        @Override public ObjectBuffers<Range> cachedValues() { return ArrayBuffers.cachedRanges(); }
    }

}
