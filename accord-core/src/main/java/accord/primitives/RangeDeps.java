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

import accord.api.Key;
import accord.utils.*;
import accord.utils.SortedArrays.SortedArrayList;
import net.nicoulaj.compilecommand.annotations.DontInline;
import net.nicoulaj.compilecommand.annotations.Inline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static accord.utils.ArrayBuffers.*;
import static accord.utils.RelationMultiMap.*;
import static accord.utils.SortedArrays.Search.CEIL;

/**
 * <p>Maintains a lazily-constructed, bidirectional map between Range and TxnId.
 * <p>Ranges are stored sorted by start then end, and indexed by a secondary {@link SearchableRangeList} structure.
 * <p>The relationship between Range and TxnId is maintained via {@code int[]} utilising {@link RelationMultiMap}
 * functionality.
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

    public static <T1, T2> RangeDeps merge(List<T1> merge, Function<T1, T2> getter1, Function<T2, RangeDeps> getter2)
    {
        try (LinearMerger<Range, TxnId, RangeDeps> linearMerger = new LinearMerger<>(ADAPTER))
        {
            int mergeIndex = 0, mergeSize = merge.size();
            while (mergeIndex < mergeSize)
            {
                T2 intermediate = getter1.apply(merge.get(mergeIndex++));
                if (intermediate == null)
                    continue;

                RangeDeps deps = getter2.apply(intermediate);
                if (deps == null || deps.isEmpty())
                    continue;

                linearMerger.update(deps, deps.ranges, deps.txnIds, deps.rangesToTxnIds);
            }

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

    @Inline
    public <P1, P2, P3> int forEach(Key key, IndexedTriConsumer<P1, P2, P3> forEachScanOrCheckpoint, IndexedRangeTriConsumer<P1, P2, P3> forEachRange, P1 p1, P2 p2, P3 p3, int minIndex)
    {
        return ensureSearchable().forEach(key, forEachScanOrCheckpoint, forEachRange, p1, p2, p3, minIndex);
    }

    private int forEach(Key key, Consumer<TxnId> forEach, int minIndex, @Nullable BitSet visited)
    {
        return forEach(key, RangeDeps::visitTxnIdsForRangeIndex, RangeDeps::visitTxnIdsForRangeIndex,
                this, forEach, visited, minIndex);
    }

    @Inline
    public <P1, P2, P3> int forEach(Range range, IndexedTriConsumer<P1, P2, P3> forEachScanOrCheckpoint, IndexedRangeTriConsumer<P1, P2, P3> forEachRange, P1 p1, P2 p2, P3 p3, int minIndex)
    {
        return ensureSearchable().forEach(range, forEachScanOrCheckpoint, forEachRange, p1, p2, p3, minIndex);
    }

    private <P1, P2, P3> void forEach(Ranges ranges, IndexedTriConsumer<P1, P2, P3> forEachScanOrCheckpoint, IndexedRangeTriConsumer<P1, P2, P3> forEachRange, P1 p1, P2 p2, P3 p3)
    {
        int minIndex = 0;
        for (int i = 0; i < ranges.size() ; ++i)
            minIndex = forEach(ranges.get(i), forEachScanOrCheckpoint, forEachRange, p1, p2, p3, minIndex);
    }

    private int forEach(Range range, Consumer<TxnId> forEach, int minIndex, @Nullable BitSet visited)
    {
        return forEach(range, RangeDeps::visitTxnIdsForRangeIndex, RangeDeps::visitTxnIdsForRangeIndex,
                this, forEach, visited, minIndex);
    }

    private <P> int forEach(Range range, IndexedConsumer<P> forEach, P param, int minIndex)
    {
        return forEach(range, RangeDeps::visitTxnIdxsForRangeIndex, RangeDeps::visitTxnIdxsForRangeIndex,
                this, forEach, param, minIndex);
    }

    private void visitTxnIdsForRangeIndex(Consumer<TxnId> forEach, @Nullable BitSet visited, int rangeIndex)
    {
        for (int i = startOffset(ranges, rangesToTxnIds, rangeIndex), end = endOffset(rangesToTxnIds, rangeIndex) ; i < end ; ++i)
            visitTxnId(rangesToTxnIds[i], forEach, visited);
    }

    private void visitTxnIdsForRangeIndex(Consumer<TxnId> forEach, @Nullable BitSet visited, int start, int end)
    {
        if (end == 0)
            return;
        for (int i = startOffset(ranges, rangesToTxnIds, start) ; i < endOffset(rangesToTxnIds, end - 1) ; ++i)
            visitTxnId(rangesToTxnIds[i], forEach, visited);
    }

    // TODO (low priority, efficiency): ideally we would accept something like a BitHashSet or IntegerTrie
    //   as O(N) space needed for BitSet here (but with a very low constant multiplier)
    private void visitTxnId(int txnIdx, Consumer<TxnId> forEach, @Nullable BitSet visited)
    {
        if (visited == null || !visited.get(txnIdx))
        {
            if (visited != null)
                visited.set(txnIdx);
            forEach.accept(txnIds[txnIdx]);
        }
    }

    private <P> void visitTxnIdxsForRangeIndex(IndexedConsumer<P> forEach, P param, int rangeIndex)
    {
        for (int i = startOffset(ranges, rangesToTxnIds, rangeIndex), end = endOffset(rangesToTxnIds, rangeIndex) ; i < end ; ++i)
            forEach.accept(param, rangesToTxnIds[i]);
    }

    private <P> void visitTxnIdxsForRangeIndex(IndexedConsumer<P> forEach, P param, int start, int end)
    {
        if (end == 0)
            return;
        for (int i = startOffset(ranges, rangesToTxnIds, start) ; i < endOffset(rangesToTxnIds, end - 1) ; ++i)
            forEach.accept(param, rangesToTxnIds[i]);
    }

    /**
     * Each matching TxnId will be provided precisely once
     */
    public void forEachUniqueTxnId(Key key, Consumer<TxnId> forEach)
    {
        forEach(key, forEach, 0, new BitSet());
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public void forEach(Range range, Consumer<TxnId> forEach)
    {
        forEach(range, forEach, 0, null);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public void forEach(Ranges ranges, Consumer<TxnId> forEach)
    {
        int minIndex = 0;
        for (int i = 0; i < ranges.size() ; ++i)
            minIndex = forEach(ranges.get(i), forEach, minIndex, null);
    }

    /**
     * The same TxnId may be provided as a parameter multiple times
     */
    public <P> void forEach(Ranges ranges, P param, IndexedConsumer<P> forEach)
    {
        int minIndex = 0;
        for (int i = 0; i < ranges.size() ; ++i)
            minIndex = forEach(ranges.get(i), forEach, param, minIndex);
    }

    /**
     * Each matching TxnId will be provided precisely once
     */
    public void forEachUniqueTxnId(Range range, Consumer<TxnId> forEach)
    {
        forEach(range, forEach, 0, new BitSet());
    }

    /**
     * Each matching TxnId will be provided precisely once
     *
     * @param ranges to match on
     * @param forEach function to call on each unique {@link TxnId}
     */
    public void forEachUniqueTxnId(Ranges ranges, Consumer<TxnId> forEach)
    {
        int minIndex = 0;
        for (int i = 0; i < ranges.size() ; ++i)
            minIndex = forEach(ranges.get(i), forEach, minIndex, new BitSet());
    }

    // return true iff we map any ranges to any txnId
    // if the mapping is empty we return false, whether or not we have any ranges or txnId by themselves
    public boolean isEmpty()
    {
        return RelationMultiMap.isEmpty(ranges, rangesToTxnIds);
    }

    public Ranges someUnseekables(TxnId txnId)
    {
        return ranges(txnId);
    }

    public Ranges ranges(TxnId txnId)
    {
        int txnIdIndex = Arrays.binarySearch(txnIds, txnId);
        if (txnIdIndex < 0)
            throw new IllegalStateException("Cannot create a RouteFragment without any keys");

        ensureTxnIdToRange();

        int start = txnIdIndex == 0 ? txnIds.length : txnIdsToRanges[txnIdIndex - 1];
        int end = txnIdsToRanges[txnIdIndex];
        if (start == end)
            throw new IllegalStateException("Cannot create a RouteFragment without any keys");

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
        return new Ranges(result);
    }

    void ensureTxnIdToRange()
    {
        if (txnIdsToRanges != null)
            return;

        txnIdsToRanges = invert(rangesToTxnIds, rangesToTxnIds.length, ranges.length, txnIds.length);
    }

    public RangeDeps slice(Ranges select)
    {
        if (isEmpty())
            return new RangeDeps(NO_RANGES, txnIds, NO_INTS);

        try (RangeAndMapCollector collector = new RangeAndMapCollector(ensureSearchable().maxScanAndCheckpointMatches))
        {
            forEach(select, collector, collector, ranges, rangesToTxnIds, null);

            if (collector.rangesCount == 0)
                return new RangeDeps(NO_RANGES, NO_TXNIDS, NO_INTS);

            if (collector.rangesCount == this.ranges.length)
                return this;

            Range[] ranges = collector.getRanges();
            int[] rangesToTxnIds = collector.getRangesToTxnIds();
            TxnId[] txnIds = trimUnusedValues(ranges, this.txnIds, rangesToTxnIds, TxnId[]::new);
            return new RangeDeps(ranges, txnIds, rangesToTxnIds);
        }
    }

    public RangeDeps with(RangeDeps that)
    {
        if (isEmpty() || that.isEmpty())
            return isEmpty() ? that : this;

        return linearUnion(
                this.ranges, this.ranges.length, this.txnIds, this.txnIds.length, this.rangesToTxnIds, this.rangesToTxnIds.length,
                that.ranges, that.ranges.length, that.txnIds, that.txnIds.length, that.rangesToTxnIds, that.rangesToTxnIds.length,
                rangeComparator(), TxnId::compareTo,
                cachedRanges(), cachedTxnIds(), cachedInts(),
                (ranges, rangesLength, txnIds, txnIdsLength, out, outLength) ->
                        new RangeDeps(cachedRanges().complete(ranges, rangesLength),
                                cachedTxnIds().complete(txnIds, txnIdsLength),
                                cachedInts().complete(out, outLength))
        );
    }

    public RangeDeps without(Predicate<TxnId> remove)
    {
        return remove(this, ranges, txnIds, rangesToTxnIds, remove,
                NONE, TxnId[]::new, ranges, RangeDeps::new);
    }

    public boolean contains(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId) >= 0;
    }

    public boolean isCoveredBy(Ranges covering)
    {
        // check that every entry intersects with some entry in covering
        int prev = 0;
        for (Range range : covering)
        {
            int start = SortedArrays.binarySearch(ranges, 0, ranges.length, range.start(), (a, b) -> a.compareTo(b.start()), CEIL);
            if (start < 0) start = -1 - start;
            int end = SortedArrays.binarySearch(ranges, 0, ranges.length, range.end(), (a, b) -> a.compareTo(b.start()), CEIL);
            if (end < 0) end = -1 - end;
            for (int i = prev; i < start ; ++i)
            {
                if (range.compareIntersecting(ranges[i]) != 0)
                    return false;
            }
            prev = end;
        }
        return prev == ranges.length;
    }

    public SortedArrayList<TxnId> txnIds()
    {
        return new SortedArrayList<>(txnIds);
    }

    public List<TxnId> txnIds(Key key)
    {
        List<TxnId> result = new ArrayList<>();
        forEachUniqueTxnId(key, result::add);
        result.sort(TxnId::compareTo);
        return result;
    }

    public List<TxnId> txnIds(Range key)
    {
        List<TxnId> result = new ArrayList<>();
        forEachUniqueTxnId(key, result::add);
        result.sort(TxnId::compareTo);
        return result;
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

    @Override
    public boolean equals(Object that)
    {
        return this == that || (that instanceof RangeDeps && equals((RangeDeps)that));
    }

    public boolean equals(RangeDeps that)
    {
        return testEquality(this.ranges, this.txnIds, this.rangesToTxnIds, that.ranges, that.txnIds, that.rangesToTxnIds);
    }

    @Override
    public String toString()
    {
        return RelationMultiMap.toSimpleString(ranges, txnIds, rangesToTxnIds);
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
            IndexedRangeTriConsumer<Range[], int[], Object>,
            IndexedTriConsumer<Range[], int[], Object>,
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
        public void accept(Range[] o, int[] o2, Object o3, int index)
        {
            oooBuffer[oooCount++] = index;
        }

        @Override
        public void accept(Range[] ranges, int[] rangesToTxnIds, Object o3, int fromIndex, int toIndex)
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<Range, TxnId, RangeDeps>
    {
        public Builder()
        {
            super(ADAPTER);
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

    public static class BuilderByTxnId extends AbstractBuilder<TxnId, Range, RangeDeps>
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
    }

    public static SymmetricComparator<? super Range> rangeComparator()
    {
        return Range::compare;
    }

    private static final RangeDepsAdapter ADAPTER = new RangeDepsAdapter();
    private static final class RangeDepsAdapter implements Adapter<Range, TxnId>
    {
        @Override
        public SymmetricComparator<? super Range> keyComparator()
        {
            return rangeComparator();
        }

        @Override
        public SymmetricComparator<? super TxnId> valueComparator()
        {
            return TxnId::compareTo;
        }

        @Override
        public ObjectBuffers<Range> cachedKeys()
        {
            return ArrayBuffers.cachedRanges();
        }

        @Override
        public ObjectBuffers<TxnId> cachedValues()
        {
            return ArrayBuffers.cachedTxnIds();
        }
    }

    private static final ReverseRangeDepsAdapter REVERSE_ADAPTER = new ReverseRangeDepsAdapter();
    private static final class ReverseRangeDepsAdapter implements Adapter<TxnId, Range>
    {
        @Override
        public SymmetricComparator<? super TxnId> keyComparator()
        {
            return TxnId::compareTo;
        }

        @Override
        public SymmetricComparator<? super Range> valueComparator()
        {
            return rangeComparator();
        }

        @Override
        public ObjectBuffers<TxnId> cachedKeys()
        {
            return ArrayBuffers.cachedTxnIds();
        }

        @Override
        public ObjectBuffers<Range> cachedValues()
        {
            return ArrayBuffers.cachedRanges();
        }
    }

}
