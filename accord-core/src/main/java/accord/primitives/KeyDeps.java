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
import accord.api.RoutingKey;
import accord.utils.ArrayBuffers;
import accord.utils.IndexedBiConsumer;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SymmetricComparator;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static accord.utils.ArrayBuffers.*;
import static accord.utils.RelationMultiMap.*;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * A collection of dependencies for a transaction, organised by the key the dependency is adopted via.
 * An inverse map from TxnId to Key may also be constructed and stored in this collection.
 */
// TODO (desired, consider): switch to RoutingKey? Would mean adopting execution dependencies less precisely, but saving ser/deser of large keys
public class KeyDeps implements Iterable<Map.Entry<Key, TxnId>>
{
    public static final KeyDeps NONE = new KeyDeps(Keys.EMPTY, NO_TXNIDS, NO_INTS);

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

        public static KeyDeps create(Keys keys, TxnId[] txnIds, int[] keyToTxnId)
        {
            return new KeyDeps(keys, txnIds, keyToTxnId);
        }
    }

    public static KeyDeps none(Keys keys)
    {
        int[] keysToTxnId = new int[keys.size()];
        Arrays.fill(keysToTxnId, keys.size());
        return new KeyDeps(keys, NO_TXNIDS, keysToTxnId);
    }

    /**
     * Expects Command to be provided in TxnId order
     */
    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder extends AbstractBuilder<Key, TxnId, KeyDeps>
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
        protected KeyDeps build(Key[] keys, TxnId[] txnIds, int[] keysToTxnIds)
        {
            return new KeyDeps(Keys.ofSortedUnique(keys), txnIds, keysToTxnIds);
        }
    }

    public static <T1, T2> KeyDeps merge(List<T1> merge, Function<T1, T2> getter1, Function<T2, KeyDeps> getter2)
    {
        try (LinearMerger<Key, TxnId, KeyDeps> linearMerger = new LinearMerger<>(ADAPTER))
        {
            int mergeIndex = 0, mergeSize = merge.size();
            while (mergeIndex < mergeSize)
            {
                T2 intermediate = getter1.apply(merge.get(mergeIndex++));
                if (intermediate == null)
                    continue;

                KeyDeps deps = getter2.apply(intermediate);
                if (deps == null || deps.isEmpty())
                    continue;

                linearMerger.update(deps, deps.keys.keys, deps.txnIds, deps.keysToTxnIds);
            }

            return linearMerger.get(KeyDeps::new, NONE);
        }
    }

    final Keys keys; // unique Keys
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

    KeyDeps(Key[] keys, TxnId[] txnIds, int[] keysToTxnIds)
    {
        this(Keys.ofSortedUnique(keys), txnIds, keysToTxnIds);
    }

    KeyDeps(Keys keys, TxnId[] txnIds, int[] keysToTxnIds)
    {
        this.keys = keys;
        this.txnIds = txnIds;
        this.keysToTxnIds = keysToTxnIds;
        if (!(keys.isEmpty() || keysToTxnIds[keys.size() - 1] == keysToTxnIds.length))
            throw new IllegalArgumentException(String.format("Last key (%s) in keyToTxnId does not point (%d) to the end of the array (%d);\nkeyToTxnId=%s", keys.get(keys.size() - 1), keysToTxnIds[keys.size() - 1], keysToTxnIds.length, Arrays.toString(keysToTxnIds)));
        checkValid(keys.keys, txnIds, keysToTxnIds);
    }

    public KeyDeps slice(Ranges ranges)
    {
        if (isEmpty())
            return new KeyDeps(keys, txnIds, keysToTxnIds);

        // TODO (low priority, efficiency): can slice in parallel with selecting keyToTxnId contents to avoid duplicate merging
        Keys select = keys.slice(ranges);

        if (select.isEmpty())
            return new KeyDeps(Keys.EMPTY, NO_TXNIDS, NO_INTS);

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
                Key::compareTo, TxnId::compareTo,
                cachedKeys(), cachedTxnIds(), cachedInts(),
                (keys, keysLength, txnIds, txnIdsLength, out, outLength) ->
                        new KeyDeps(Keys.ofSortedUnchecked(cachedKeys().complete(keys, keysLength)),
                                cachedTxnIds().complete(txnIds, txnIdsLength),
                                cachedInts().complete(out, outLength))
                );
    }

    public KeyDeps without(Predicate<TxnId> remove)
    {
        return remove(this, keys.keys, txnIds, keysToTxnIds, remove,
                NONE, TxnId[]::new, keys, KeyDeps::new);
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

    public Keys participatingKeys(TxnId txnId)
    {
        int txnIdIndex = Arrays.binarySearch(txnIds, txnId);
        if (txnIdIndex < 0)
            return Keys.EMPTY;

        int[] txnIdsToKeys = txnIdsToKeys();

        int start = txnIdIndex == 0 ? txnIds.length : txnIdsToKeys[txnIdIndex - 1];
        int end = txnIdsToKeys[txnIdIndex];
        if (start == end)
            return Keys.EMPTY;

        Key[] result = new Key[end - start];
        for (int i = start ; i < end ; ++i)
            result[i - start] = keys.get(txnIdsToKeys[i]);
        return Keys.of(result);
    }

    // TODO (desired): consider optionally not inverting before answering, as a single txnId may be answered more efficiently without inversion
    public RoutingKeys participants(TxnId txnId)
    {
        int txnIdIndex = Arrays.binarySearch(txnIds, txnId);
        if (txnIdIndex < 0)
            throw new IllegalStateException("Cannot create a RouteFragment without any keys");

        int[] txnIdsToKeys = txnIdsToKeys();

        int start = txnIdIndex == 0 ? txnIds.length : txnIdsToKeys[txnIdIndex - 1];
        int end = txnIdsToKeys[txnIdIndex];
        if (start == end)
            throw new IllegalStateException("Cannot create a RouteFragment without any keys");

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

    public void forEach(Ranges ranges, BiConsumer<Key, TxnId> forEach)
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
            long bitset = Routables.foldl(keys, ranges, (key, ignore, value, keyIndex) -> {
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
            }, 0, 0, -1L >>> (64 - txnIds.length));

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

    public void forEach(Key key, Consumer<TxnId> forEach)
    {
        int keyIndex = keys.indexOf(key);
        if (keyIndex < 0)
            return;

        int[] keysToTxnIds = keysToTxnIds();
        int index = startOffset(keyIndex);
        int end = endOffset(keyIndex);
        while (index < end)
            forEach.accept(txnIds[keysToTxnIds[index++]]);
    }

    public <P1, P2> void forEach(Ranges ranges, int inclIdx, int exclIdx, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        int[] keysToTxnIds = keysToTxnIds();
        for (int i = 0; i < ranges.size(); ++i)
        {
            Range range = ranges.get(i);
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
                if (txnIdx >= inclIdx && txnIdx < exclIdx)
                    forEach.accept(p1, p2, txnIdx);
            }
        }
    }

    public Keys keys()
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

    public SortedArrayList<TxnId> txnIds()
    {
        return new SortedArrayList<>(txnIds);
    }

    public List<TxnId> txnIds(Key key)
    {
        int keyIndex = keys.indexOf(key);
        if (keyIndex < 0)
            return Collections.emptyList();

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
    public Iterator<Map.Entry<Key, TxnId>> iterator()
    {
        return newIterator(keys.keys, txnIds, keysToTxnIds);
    }

    @Override
    public String toString()
    {
        return toSimpleString(keys.keys, txnIds, keysToTxnIds);
    }

    private static final KeyDepsAdapter ADAPTER = new KeyDepsAdapter();
    static class KeyDepsAdapter implements Adapter<Key, TxnId>
    {
        @Override
        public final SymmetricComparator<? super Key> keyComparator()
        {
            return Key::compareTo;
        }

        @Override
        public final SymmetricComparator<? super TxnId> valueComparator()
        {
            return TxnId::compareTo;
        }

        @Override
        public final ObjectBuffers<Key> cachedKeys()
        {
            return ArrayBuffers.cachedKeys();
        }

        @Override
        public final ObjectBuffers<TxnId> cachedValues()
        {
            return ArrayBuffers.cachedTxnIds();
        }
    }
}
