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

import accord.api.RoutingKey;
import accord.utils.ArrayBuffers;
import accord.utils.IndexedBiConsumer;
import accord.utils.IndexedConsumer;
import accord.utils.IndexedFunction;
import accord.utils.IndexedTriConsumer;
import accord.utils.RelationMultiMap;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SymmetricComparator;
import accord.utils.TriFunction;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import static accord.primitives.RoutingKeys.toRoutingKeys;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.*;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.RelationMultiMap.*;
import static accord.utils.SortedArrays.Search.FAST;

// TODO (desired, consider): switch to RoutingKey? Would mean adopting execution dependencies less precisely, but saving ser/deser of large keys
// TODO (expected): consider which projection we should default to on de/serialise
/**
 * A collection of dependencies for a transaction, organised by the key the dependency is adopted via.
 * An inverse map from TxnId to Key may also be constructed and stored in this collection.
 */
public class KeyDeps implements Iterable<Map.Entry<RoutingKey, TxnId>>
{
    public static final KeyDeps NONE = new KeyDeps(RoutingKeys.EMPTY, NO_TXNIDS, NO_INTS);

    public static class SerializerSupport
    {
        private SerializerSupport() {}

        public static int keysToTxnIdsCount(KeyDeps deps)
        {
            return deps.keysToTxnIds.length;
        }

        public static int keysToTxnIds(KeyDeps deps, int idx)
        {
            return deps.keysToTxnIds[idx];
        }

        public static KeyDeps create(RoutingKeys keys, TxnId[] txnIds, int[] keyToTxnId)
        {
            return new KeyDeps(keys, txnIds, keyToTxnId);
        }
    }

    public static KeyDeps none(RoutingKeys keys)
    {
        int[] keysToTxnId = new int[keys.size()];
        Arrays.fill(keysToTxnId, keys.size());
        return new KeyDeps(keys, NO_TXNIDS, keysToTxnId, null);
    }

    /**
     * Expects Command to be provided in TxnId order
     */
    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<RoutingKey, TxnId, KeyDeps>
    {
        public Builder()
        {
            super(ADAPTER);
        }

        @Override
        protected KeyDeps none()
        {
            return KeyDeps.NONE;
        }

        @Override
        protected KeyDeps build(RoutingKey[] keys, TxnId[] txnIds, int[] keysToTxnIds)
        {
            return new KeyDeps(RoutingKeys.ofSortedUnique(keys), txnIds, keysToTxnIds);
        }
    }

    public static LinearMerger<RoutingKey, TxnId, KeyDeps> newMerger()
    {
        return new LinearMerger<>(ADAPTER);
    }

    public static <C, T1, T2> KeyDeps merge(C merge, int mergeSize, IndexedFunction<C, T1> getter1, Function<T1, T2> getter2, Function<T2, KeyDeps> getter3)
    {
        try (LinearMerger<RoutingKey, TxnId, KeyDeps> linearMerger = newMerger())
        {
            int mergeIndex = 0;
            while (mergeIndex < mergeSize)
            {
                T1 t1 = getter1.apply(merge, mergeIndex++);
                if (t1 == null) continue;

                T2 t2 = getter2.apply(t1);
                if (t2 == null) continue;

                KeyDeps deps = getter3.apply(t2);
                if (deps == null || deps.isEmpty())
                    continue;

                linearMerger.update(deps, deps.keys.keys, deps.txnIds, deps.keysToTxnIds);
            }

            return linearMerger.get(KeyDeps::new, NONE);
        }
    }

    public static KeyDeps merge(Stream<KeyDeps> merge)
    {
        try (LinearMerger<RoutingKey, TxnId, KeyDeps> linearMerger = newMerger())
        {
            merge.forEach(deps -> {
                if (!deps.isEmpty())
                    linearMerger.update(deps, deps.keys.keys, deps.txnIds, deps.keysToTxnIds);
            });

            return linearMerger.get(KeyDeps::new, NONE);
        }
    }

    final RoutingKeys keys; // unique Keys
    final TxnId[] txnIds; // unique TxnId

    /**
     * This represents a map of {@code Key -> [TxnId] } where each TxnId is actually a pointer into the txnIds array.
     * The beginning of the array (the first keys.size() entries) are offsets into this array.
     * <p/>
     * Example:
     * <p/>
     * {@code
     *   int keyIdx = keys.indexOf(key);
     *   int startOfTxnOffset = keyIdx == 0 ? keys.size() : keyToTxnId[keyIdx - 1];
     *   int endOfTxnOffset = keyToTxnId[keyIdx];
     *   for (int i = startOfTxnOffset; i < endOfTxnOffset; i++)
     *   {
     *       TxnId id = txnIds[keyToTxnId[i]]
     *       ...
     *   }
     * }
     */
    // TODO (expected): support deserializing to one or the other
    int[] keysToTxnIds; // Key -> [TxnId]
    int[] txnIdsToKeys; // TxnId -> [Key]

    KeyDeps(RoutingKey[] keys, TxnId[] txnIds, int[] keysToTxnIds)
    {
        this(RoutingKeys.ofSortedUnique(keys), txnIds, keysToTxnIds);
    }

    KeyDeps(RoutingKeys keys, TxnId[] txnIds, int[] keysToTxnIds)
    {
        this(keys, txnIds, keysToTxnIds, null);
    }

    KeyDeps(RoutingKeys keys, TxnId[] txnIds, int[] keysToTxnIds, @Nullable int[] txnIdsToKeys)
    {
        this.keys = keys;
        this.txnIds = txnIds;
        this.keysToTxnIds = keysToTxnIds;
        this.txnIdsToKeys = txnIdsToKeys;
        if (!(keys.isEmpty() || keysToTxnIds[keys.size() - 1] == keysToTxnIds.length))
            throw illegalArgument(String.format("Last key (%s) in keyToTxnId does not point (%d) to the end of the array (%d);\nkeyToTxnId=%s", keys.get(keys.size() - 1), keysToTxnIds[keys.size() - 1], keysToTxnIds.length, Arrays.toString(keysToTxnIds)));
        checkValid(keys.keys, txnIds, keysToTxnIds);
    }

    public KeyDeps slice(Ranges ranges)
    {
        return select(keys.slice(ranges));
    }

    public KeyDeps intersecting(Unseekables<?> participants)
    {
        AbstractUnseekableKeys select = keys.intersecting(participants);
        return select(toRoutingKeys(select));
    }

    public @Nullable TxnId minTxnId(Unseekables<?> participants)
    {
        return Routables.foldl(keys, participants, (rk, min, index) -> {
            int start = index == 0 ? keys.size() : keysToTxnIds[index - 1];
            int end = keysToTxnIds[index];
            if (start == end)
                return min;
            return TxnId.nonNullOrMin(min, txnIds[start]);
        }, (TxnId)null);
    }

    private KeyDeps select(RoutingKeys select)
    {
        // TODO (low priority, efficiency): can slice in parallel with selecting keyToTxnId contents to avoid duplicate merging
        if (select.isEmpty())
            return KeyDeps.NONE;

        if (select.size() == keys.size())
            return this;

        int i = 0;
        int offset = select.size();
        for (int j = 0 ; j < select.size() ; ++j)
        {
            int findi = keys.findNext(i, select.get(j), FAST);
            if (findi < 0)
                continue;

            i = findi;
            offset += keysToTxnIds[i] - (i == 0 ? keys.size() : keysToTxnIds[i - 1]);
        }

        int[] src = keysToTxnIds;
        int[] trg = new int[offset];

        i = 0;
        offset = select.size();
        for (int j = 0 ; j < select.size() ; ++j)
        {
            int findi = keys.findNext(i, select.get(j), FAST);
            if (findi >= 0)
            {
                i = findi;
                int start = i == 0 ? keys.size() : src[i - 1];
                int count = src[i] - start;
                System.arraycopy(src, start, trg, offset, count);
                offset += count;
            }
            trg[j] = offset;
        }

        TxnId[] txnIds = trimUnusedValues(select.keys, this.txnIds, trg, TxnId[]::new);
        return new KeyDeps(select, txnIds, trg);
    }

    public KeyDeps with(KeyDeps that)
    {
        if (isEmpty() || that.isEmpty())
            return isEmpty() ? that : this;

        return linearUnion(
                this.keys.keys, this.keys.keys.length, this.txnIds, this.txnIds.length, this.keysToTxnIds, this.keysToTxnIds.length,
                that.keys.keys, that.keys.keys.length, that.txnIds, that.txnIds.length, that.keysToTxnIds, that.keysToTxnIds.length,
                RoutingKey::compareTo, TxnId::compareTo, null, null,
                cachedRoutingKeys(), cachedTxnIds(), cachedInts(),
                (keys, keysLength, txnIds, txnIdsLength, out, outLength) ->
                        new KeyDeps(RoutingKeys.ofSortedUnique(cachedRoutingKeys().complete(keys, keysLength)),
                                cachedTxnIds().complete(txnIds, txnIdsLength),
                                cachedInts().complete(out, outLength))
                );
    }

    public KeyDeps without(KeyDeps remove)
    {
        if (isEmpty() || remove.isEmpty()) return this;
        try (Builder builder = new Builder())
        {
            if (!RelationMultiMap.remove(keys.keys, txnIds, keysToTxnIds,
                                         remove.keys.keys, remove.txnIds, remove.keysToTxnIds,
                                         RoutingKey::compareTo, TxnId::compareTo, builder::add))
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

    public boolean intersects(TxnId txnId, Ranges ranges)
    {
        int txnIdx = Arrays.binarySearch(txnIds, txnId);
        if (txnIdx < 0)
            return false;

        int[] txnIdsToKeys = txnIdsToKeys();

        int start = txnIdx == 0 ? txnIds.length : txnIdsToKeys[txnIdx - 1];
        int end = txnIdsToKeys[txnIdx];
        if (start == end)
            return false;

        int li = start, ri = 0;
        while (li < end && ri < ranges.size())
        {
            ri = ranges.findNext(ri, keys.get(txnIdsToKeys[li]), FAST);
            if (ri >= 0) return true;
            ri = -1 - ri;
            ++li;
        }
        return false;
    }

    // return true iff we map any keys to any txnId
    // if the mapping is empty we return false, whether or not we have any keys or txnId by themselves
    public boolean isEmpty()
    {
        return keysToTxnIds.length == keys.size();
    }

    public RoutingKeys participatingKeys(TxnId txnId)
    {
        int txnIdx = Arrays.binarySearch(txnIds, txnId);
        if (txnIdx < 0)
            return RoutingKeys.EMPTY;

        return participatingKeys(txnIdx);
    }

    public RoutingKeys participatingKeys(int txnIdx)
    {
        int[] txnIdsToKeys = txnIdsToKeys();

        int start = txnIdx == 0 ? txnIds.length : txnIdsToKeys[txnIdx - 1];
        int end = txnIdsToKeys[txnIdx];
        if (start == end)
            return RoutingKeys.EMPTY;

        RoutingKey[] result = new RoutingKey[end - start];
        for (int i = start ; i < end ; ++i)
            result[i - start] = keys.get(txnIdsToKeys[i]);
        return RoutingKeys.of(result);
    }

    // TODO (desired): consider optionally not inverting before answering, as a single txnId may be answered more efficiently without inversion
    public RoutingKeys participants(TxnId txnId)
    {
        int txnIdIndex = Arrays.binarySearch(txnIds, txnId);
        if (txnIdIndex < 0)
            return RoutingKeys.EMPTY;

        int[] txnIdsToKeys = txnIdsToKeys();

        int start = txnIdIndex == 0 ? txnIds.length : txnIdsToKeys[txnIdIndex - 1];
        int end = txnIdsToKeys[txnIdIndex];
        if (start == end)
            return RoutingKeys.EMPTY;

        RoutingKey[] result = new RoutingKey[end - start];
        result[0] = keys.get(txnIdsToKeys[start]).toUnseekable();
        int resultCount = 1;
        for (int i = start + 1 ; i < end ; ++i)
        {
            RoutingKey next = keys.get(txnIdsToKeys[i]).toUnseekable();
            if (!next.equals(result[resultCount - 1]))
                result[resultCount++] = next;
        }

        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);
        return new RoutingKeys(result);
    }

    int[] txnIdsToKeys()
    {
        if (txnIdsToKeys == null)
            txnIdsToKeys = invert(keysToTxnIds, keysToTxnIds.length, keys.size(), txnIds.length);
        return txnIdsToKeys;
    }

    int[] keysToTxnIds()
    {
        if (keysToTxnIds == null)
            keysToTxnIds = invert(txnIdsToKeys, txnIdsToKeys.length, txnIds.length, keys.size());
        return keysToTxnIds;
    }

    public void forEach(Ranges ranges, BiConsumer<RoutingKey, TxnId> forEach)
    {
        int[] keysToTxnIds = keysToTxnIds();
        Routables.foldl(keys, ranges, (key, value, index) -> {
            for (int t = startOffset(index), end = endOffset(index); t < end ; ++t)
            {
                TxnId txnId = txnIds[keysToTxnIds[t]];
                forEach.accept(key, txnId);
            }
            return null;
        }, null);
    }

    /**
     * For each {@link TxnId} that references a key within the {@link Ranges}; the {@link TxnId} will be seen exactly once.
     * @param ranges to match on
     * @param forEach function to call on each unique {@link TxnId}
     */
    public void forEachUniqueTxnId(Ranges ranges, Consumer<TxnId> forEach)
    {
        if (txnIds.length == 0)
            return;

        int[] keysToTxnIds = keysToTxnIds();
        if (txnIds.length <= 64)
        {
            long bitset = Routables.<RoutingKey>foldl(keys, ranges, (key, ignore, value, keyIndex) -> {
                int index = startOffset(keyIndex);
                int end = endOffset(keyIndex);

                while (index < end)
                {
                    long next = keysToTxnIds[index++];
                    if (next >= 64)
                        break;
                    value |= 1L << next;
                }

                return value;
            }, 0L, 0L, -1L >>> (64 - txnIds.length));

            while (bitset != 0)
            {
                int i = Long.numberOfTrailingZeros(bitset);
                TxnId txnId = txnIds[i];
                forEach.accept(txnId);
                bitset ^= Long.lowestOneBit(bitset);
            }
        }
        else
        {
            BitSet bitset = Routables.foldl(keys, ranges, (key, value, keyIndex) -> {
                int index = startOffset(keyIndex);
                int end = endOffset(keyIndex);
                while (index < end)
                    value.set(keysToTxnIds[index++]);
                return value;
            }, new BitSet(txnIds.length));

            int i = -1;
            while ((i = bitset.nextSetBit(i + 1)) >= 0)
                forEach.accept(txnIds[i]);
        }
    }

    public void forEach(RoutingKey key, IndexedConsumer<TxnId> forEach)
    {
        int keyIndex = keys.indexOf(key);
        if (keyIndex < 0)
            return;

        int[] keysToTxnIds = keysToTxnIds();
        int index = startOffset(keyIndex);
        int end = endOffset(keyIndex);
        while (index < end)
        {
            int txnIdx = keysToTxnIds[index++];
            forEach.accept(txnIds[txnIdx], txnIdx);
        }
    }

    public <P1, P2> void forEach(Ranges ranges, int inclIdx, int exclIdx, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        for (int i = 0; i < ranges.size(); ++i)
            forEach(ranges.get(i), inclIdx, exclIdx, p1, p2, forEach);
    }

    public <P1, P2> void forEach(Range range, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        forEach(range, 0, keys.size(), p1, p2, forEach);
    }

    public <P1, P2> void forEach(Range range, int inclKeyIdx, int exclKeyIdx, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        forEach(range, inclKeyIdx, exclKeyIdx, forEach, p1, p2, IndexedBiConsumer::accept);
    }

    public <P1, P2, P3> void forEach(Range range, P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
    {
        forEach(range, 0, keys.size(), p1, p2, p3, forEach);
    }

    public <P1, P2, P3> void forEach(Range range, int inclKeyIdx, int exclKeyIdx, P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
    {
        int[] keysToTxnIds = keysToTxnIds();
        int start = keys.indexOf(range.start());
        if (start < 0) start = -1 - start;
        else if (!range.startInclusive()) ++start;
        start = startOffset(start);

        int end = keys.indexOf(range.end());
        if (end < 0) end = -1 - end;
        else if (range.endInclusive()) ++end;
        end = startOffset(end);

        while (start < end)
        {
            int txnIdx = keysToTxnIds[start++];
            if (txnIdx >= inclKeyIdx && txnIdx < exclKeyIdx)
                forEach.accept(p1, p2, p3, txnIdx);
        }
    }

    public <P1, V> V foldEachKey(int txnIdx, P1 p1, V accumulate, TriFunction<P1, RoutingKey, V, V> fold)
    {
        int[] txnIdsToKeys = txnIdsToKeys();

        int start = txnIdx == 0 ? txnIds.length : txnIdsToKeys[txnIdx - 1];
        int end = txnIdsToKeys[txnIdx];
        for (int i = start; i < end ; ++i)
            accumulate = fold.apply(p1, keys.get(txnIdsToKeys[i]), accumulate);
        return accumulate;
    }

    public RoutingKeys keys()
    {
        return keys;
    }

    public int txnIdCount()
    {
        return txnIds.length;
    }

    public int totalCount()
    {
        return keysToTxnIds.length - keys.size();
    }

    public TxnId txnId(int i)
    {
        return txnIds[i];
    }

    public int indexOf(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId);
    }

    public SortedArrayList<TxnId> txnIds()
    {
        return new SortedArrayList<>(txnIds);
    }

    public SortedRelationList<TxnId> txnIds(RoutingKey key)
    {
        int keyIndex = keys.indexOf(key);
        if (keyIndex < 0)
            return SortedRelationList.EMPTY;

        return txnIdsForKeyIndex(keyIndex);
    }

    public SortedRelationList<TxnId> txnIdsForKeyIndex(int keyIndex)
    {
        int[] keysToTxnIds = keysToTxnIds();
        int start = startOffset(keyIndex);
        int end = endOffset(keyIndex);
        return txnIds(keysToTxnIds, start, end);
    }

    @SuppressWarnings("unchecked")
    public SortedRelationList<TxnId> txnIds(Range range)
    {
        int startIndex = keys.indexOf(range.start());
        if (startIndex < 0) startIndex = -1 - startIndex;
        else if (!range.startInclusive()) ++startIndex;

        int endIndex = keys.indexOf(range.end());
        if (endIndex < 0) endIndex = -1 - endIndex;
        else if (range.endInclusive()) ++endIndex;

        if (startIndex == endIndex)
            return SortedRelationList.EMPTY;

        int[] keysToTxnIds = keysToTxnIds();
        int maxLength = Math.min(txnIds.length, startOffset(endIndex) - startOffset(startIndex));
        int[] scratch = cachedInts().getInts(maxLength);
        int count = 0;
        for (int i = startIndex ; i < endIndex ; ++i)
        {
            int ri = startOffset(i), re = endOffset(i);
            if (ri == re)
                continue;

            System.arraycopy(scratch, 0, scratch, maxLength - count, count);
            int li = maxLength - count, le = maxLength;
            count = 0;
            while (li < le && ri < re)
            {
                int c = scratch[li] - keysToTxnIds[ri];
                if (c <= 0)
                {
                    scratch[count++] = scratch[li++];
                    if (c == 0) ++ri;
                }
                else
                {
                    scratch[count++] = keysToTxnIds[ri++];
                }
            }
            while (li < le)
                scratch[count++] = scratch[li++];
            while (ri < re)
                scratch[count++] = keysToTxnIds[ri++];

            if (count == maxLength)
                break;
        }

        int[] ids = cachedInts().completeAndDiscard(scratch, count);
        return txnIds(ids, 0, count);
    }

    @SuppressWarnings("unchecked")
    private SortedRelationList<TxnId> txnIds(int[] ids, int start, int end)
    {
        if (start == end)
            return SortedRelationList.EMPTY;

        return new SortedRelationList<>(txnIds, ids, start, end);
    }

    private int startOffset(int keyIndex)
    {
        return keyIndex == 0 ? keys.size() : keysToTxnIds[keyIndex - 1];
    }

    private int endOffset(int keyIndex)
    {
        return keysToTxnIds[keyIndex];
    }

    public boolean equals(Object that)
    {
        return this == that || (that instanceof KeyDeps && equals((KeyDeps)that));
    }

    public boolean equals(KeyDeps that)
    {
        return testEquality(this.keys.keys, this.txnIds, this.keysToTxnIds, that.keys.keys, that.txnIds, that.keysToTxnIds);
    }

    @Override
    public Iterator<Map.Entry<RoutingKey, TxnId>> iterator()
    {
        return newIterator(keys.keys, txnIds, keysToTxnIds);
    }

    @Override
    public String toString()
    {
        return toSimpleString(keys.keys, txnIds, keysToTxnIds);
    }

    public String toBriefString()
    {
        return RelationMultiMap.toBriefString(keys.keys, txnIds);
    }

    private static final KeyDepsAdapter ADAPTER = new KeyDepsAdapter();
    static final class KeyDepsAdapter implements MergeAdapter<RoutingKey, TxnId>
    {
        @Override public SymmetricComparator<? super RoutingKey> keyComparator() { return RoutingKey::compareTo; }
        @Override public SymmetricComparator<? super TxnId> valueComparator() { return TxnId::compareTo; }
        @Override public int compareKeys(RoutingKey a, RoutingKey b) { return a.compareTo(b); }
        @Override public int compareValues(TxnId a, TxnId b) { return a.compareTo(b); }
        @Override public ObjectBuffers<RoutingKey> cachedKeys() { return ArrayBuffers.cachedRoutingKeys(); }
        @Override public ObjectBuffers<TxnId> cachedValues() { return ArrayBuffers.cachedTxnIds(); }
    }
}
