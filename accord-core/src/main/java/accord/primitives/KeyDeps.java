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
import accord.primitives.Deps.DepRelationList;
import accord.utils.ArrayBuffers;
import accord.utils.Functions;
import accord.utils.IndexedBiConsumer;
import accord.utils.IndexedConsumer;
import accord.utils.IndexedFunction;
import accord.utils.IndexedTriConsumer;
import accord.utils.Invariants;
import accord.utils.RelationMultiMap;
import accord.utils.LargeBitSet;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SymmetricComparator;
import accord.utils.TriFunction;
import accord.utils.UnhandledEnum;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import static accord.api.ProtocolModifiers.isRangeEndInclusive;
import static accord.api.ProtocolModifiers.isRangeStartExclusive;
import static accord.primitives.RoutingKeys.toRoutingKeys;
import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.*;
import static accord.utils.RelationMultiMap.*;
import static accord.utils.SortedArrays.Search.FAST;

// TODO (expected): improve handling of both projections, and cleanup e.g. startOffset logic
/**
 * A collection of dependencies for a transaction, organised by the key the dependency is adopted via.
 * An inverse map from TxnId to Key may also be constructed and stored in this collection.
 */
public class KeyDeps implements Iterable<Map.Entry<RoutingKey, TxnId>>, KeyOrRangeDeps
{
    public static final KeyDeps NONE = new KeyDeps(RoutingKeys.EMPTY, NO_TXNIDS, NO_INTS, NO_INTS);

    public static class SerializerSupport
    {
        private SerializerSupport() {}

        public static TxnId[] txnIds(KeyDeps deps)
        {
            return deps.txnIds;
        }

        public static int[] keysToTxnIds(KeyDeps deps)
        {
            return deps.keysToTxnIds();
        }

        public static int[] txnIdsToKeys(KeyDeps deps)
        {
            return deps.txnIdsToKeys();
        }

        public static KeyDeps create(RoutingKeys keys, TxnId[] txnIds, int[] keysToTxnIds, int[] txnIdsToKeys)
        {
            return new KeyDeps(keys, txnIds, keysToTxnIds, txnIdsToKeys);
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
            return constructKeysToTxnIds(RoutingKeys.ofSortedUnique(keys), txnIds, keysToTxnIds);
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

                linearMerger.update(deps, deps.keys.keys, deps.txnIds, deps.keysToTxnIds());
            }

            return linearMerger.get(KeyDeps::constructKeysToTxnIds, NONE);
        }
    }

    public static KeyDeps merge(Stream<KeyDeps> merge)
    {
        try (LinearMerger<RoutingKey, TxnId, KeyDeps> linearMerger = newMerger())
        {
            merge.forEach(deps -> {
                if (!deps.isEmpty())
                    linearMerger.update(deps, deps.keys.keys, deps.txnIds, deps.keysToTxnIds());
            });

            return linearMerger.get(KeyDeps::constructKeysToTxnIds, NONE);
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
    int[] keysToTxnIds; // Key -> [TxnId]
    int[] txnIdsToKeys; // TxnId -> [Key]

    private static KeyDeps constructKeysToTxnIds(RoutingKey[] keys, TxnId[] txnIds, @Nullable int[] keysToTxnIds)
    {
        return new KeyDeps(RoutingKeys.of(keys), txnIds, keysToTxnIds, null);
    }

    private static KeyDeps constructKeysToTxnIds(RoutingKeys keys, TxnId[] txnIds, @Nullable int[] keysToTxnIds)
    {
        return new KeyDeps(keys, txnIds, keysToTxnIds, null);
    }

    private static KeyDeps constructTxnIdsToKeys(TxnId[] txnIds, RoutingKey[] keys, @Nullable int[] txnIdsToKeys)
    {
        return new KeyDeps(RoutingKeys.of(keys), txnIds, null, txnIdsToKeys);
    }

    private static KeyDeps constructTxnIdsToKeys(TxnId[] txnIds, RoutingKeys keys, @Nullable int[] txnIdsToKeys)
    {
        return new KeyDeps(keys, txnIds, null, txnIdsToKeys);
    }

    KeyDeps(RoutingKeys keys, TxnId[] txnIds, @Nullable int[] keysToTxnIds, @Nullable int[] txnIdsToKeys)
    {
        this.keys = keys;
        this.txnIds = txnIds;
        this.keysToTxnIds = keysToTxnIds;
        this.txnIdsToKeys = txnIdsToKeys;
        Invariants.require(keysToTxnIds != null || txnIdsToKeys != null);
        Invariants.require(keysToTxnIds == null || keys.isEmpty() || keysToTxnIds[keys.size() - 1] == keysToTxnIds.length);
        Invariants.require(txnIdsToKeys == null || txnIds.length == 0 || txnIdsToKeys[txnIds.length - 1] == txnIdsToKeys.length);
        if (Invariants.isParanoid())
        {
            if (keysToTxnIds != null) checkValid(keys.keys, txnIds, keysToTxnIds);
            if (txnIdsToKeys != null) checkValid(txnIds, keys.keys, txnIdsToKeys);
        }
    }

    public KeyDeps slice(Ranges ranges)
    {
        return select(keys.overlapping(ranges));
    }

    public KeyDeps intersecting(Unseekables<?> participants)
    {
        AbstractUnseekableKeys select = keys.overlapping(participants);
        return select(toRoutingKeys(select));
    }

    KeyDeps withTxnIds(TxnId[] txnIds)
    {
        if (txnIds == this.txnIds)
            return this;
        Invariants.require(txnIds.length == this.txnIds.length);
        return new KeyDeps(keys, txnIds, keysToTxnIds, txnIdsToKeys);
    }

    public @Nullable TxnId minTxnId(Unseekables<?> participants)
    {
        keysToTxnIds();
        return Routables.foldl(keys, participants, (rk, min, index) -> {
            int start = index == 0 ? keys.size() : keysToTxnIds[index - 1];
            int end = keysToTxnIds[index];
            if (start == end)
                return min;
            return TxnId.nonNullOrMin(min, txnIds[start]);
        }, (TxnId)null, Functions.alwaysFalse());
    }

    private KeyDeps select(RoutingKeys select)
    {
        keysToTxnIds();

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
        return constructKeysToTxnIds(select, txnIds, trg);
    }

    private boolean preferByKey(KeyDeps that)
    {
        int byKeyScore = (this.keysToTxnIds != null ? 1 : 0) + (that.keysToTxnIds != null ? 1 : 0);
        int byTxnIdScore = (this.txnIdsToKeys != null ? 1 : 0) + (that.txnIdsToKeys != null ? 1 : 0);
        return byKeyScore >= byTxnIdScore;
    }

    public boolean hasByKey()
    {
        return keysToTxnIds != null;
    }

    public boolean hasByTxnId()
    {
        return txnIdsToKeys != null;
    }

    public KeyDeps with(KeyDeps that)
    {
        if (isEmpty() || that.isEmpty())
            return isEmpty() ? that : this;

        if (preferByKey(that))
        {
            return linearUnion(
                    this.keys.keys, this.keys.keys.length, this.txnIds, this.txnIds.length, this.keysToTxnIds(), this.keysToTxnIds.length,
                    that.keys.keys, that.keys.keys.length, that.txnIds, that.txnIds.length, that.keysToTxnIds(), that.keysToTxnIds.length,
                    RoutingKey::compareTo, TxnId::compareTo, null, TxnId::addFlags,
                    cachedRoutingKeys(), cachedTxnIds(), cachedInts(),
                    (keys, keysLength, txnIds, txnIdsLength, out, outLength) ->
                            constructKeysToTxnIds(RoutingKeys.ofSortedUnique(cachedRoutingKeys().complete(keys, keysLength)),
                                                  cachedTxnIds().complete(txnIds, txnIdsLength),
                                                  cachedInts().complete(out, outLength))
                    );
        }
        else
        {
            return linearUnion(
                this.txnIds, this.txnIds.length, this.keys.keys, this.keys.keys.length, this.txnIdsToKeys(), this.txnIdsToKeys.length,
                that.txnIds, that.txnIds.length, that.keys.keys, that.keys.keys.length, that.txnIdsToKeys(), that.txnIdsToKeys.length,
                TxnId::compareTo, RoutingKey::compareTo, TxnId::addFlags, null,
                cachedTxnIds(), cachedRoutingKeys(), cachedInts(),
                (txnIds, txnIdsLength, keys, keysLength, out, outLength) ->
                constructTxnIdsToKeys(cachedTxnIds().complete(txnIds, txnIdsLength),
                                      RoutingKeys.ofSortedUnique(cachedRoutingKeys().complete(keys, keysLength)),
                                      cachedInts().complete(out, outLength))
            );
        }

    }

    public KeyDeps without(Predicate<TxnId> remove)
    {
        // TODO (desired): if we have txnIdsToKeys != null we can do this more cheaply
        return remove(this, keys.keys, txnIds, keysToTxnIds(), remove,
                      NONE, TxnId[]::new, keys, KeyDeps::constructKeysToTxnIds);
    }

    public KeyDeps without(KeyDeps remove)
    {
        if (isEmpty() || remove.isEmpty()) return this;
        try (Builder builder = new Builder())
        {
            // TODO (desired): if we have keysToTxnIds == null don't need to construct; can build using opposing ordering
            if (!RelationMultiMap.remove(keys.keys, txnIds, keysToTxnIds(),
                                         remove.keys.keys, remove.txnIds, remove.keysToTxnIds(),
                                         RoutingKey::compareTo, TxnId::compareTo, builder::add))
            {
                return this;
            }
            return builder.build();
        }
    }

    public KeyDeps markUnstableBefore(TxnId txnId)
    {
        int i = indexOf(txnId);
        if (i < 0) i = -1 - i;
        if (i == 0)
            return this;

        TxnId[] newTxnIds = new TxnId[txnIds.length];
        System.arraycopy(txnIds, i, newTxnIds, i, newTxnIds.length - i);
        while (--i >= 0) newTxnIds[i] = txnIds[i].addFlag(UNSTABLE);
        return new KeyDeps(keys, newTxnIds, keysToTxnIds, txnIdsToKeys);
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
        if (keysToTxnIds != null) return keysToTxnIds.length == keys.size();
        else return txnIdsToKeys.length == txnIds.length;
    }

    public RoutingKeys participants(TxnId txnId)
    {
        int txnIdx = Arrays.binarySearch(txnIds, txnId);
        if (txnIdx < 0)
            return RoutingKeys.EMPTY;

        return participants(txnIdx);
    }

    public RoutingKeys participants(int txnIdx)
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

    public RoutingKeys participants(Predicate<TxnId> select)
    {
        txnIdsToKeys();
        LargeBitSet bitSet = new LargeBitSet(keys.size(), cachedLongs());
        for (int idIdx = 0 ; idIdx < txnIds.length ; ++idIdx)
        {
            if (!select.test(txnIds[idIdx]))
                continue;

            for (int keyIdx = RelationMultiMap.startOffset(txnIds, txnIdsToKeys, idIdx),
                 endKeyIdx = RelationMultiMap.endOffset(txnIdsToKeys, idIdx); keyIdx < endKeyIdx ; keyIdx++)
                bitSet.set(txnIdsToKeys[keyIdx]);
        }

        if (bitSet.getSetBitCount() == keys.size())
        {
            bitSet.discard(cachedLongs());
            return keys;
        }

        RoutingKey[] keyBuffer = cachedRoutingKeys().get(bitSet.getSetBitCount());
        int count = 0;
        for (int i = bitSet.nextSetBit(0, Integer.MAX_VALUE) ; i < keys.size() ; )
        {
            keyBuffer[count++] = keys.get(i);
            i = bitSet.nextSetBit(i + 1, Integer.MAX_VALUE);
        }
        bitSet.discard(cachedLongs());
        return RoutingKeys.ofSortedUnique(cachedRoutingKeys().completeAndDiscard(keyBuffer, count));
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
        Routables.foldl(keys, (AbstractRanges) ranges, (key, value, index) -> {
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
            BitSet bitset = Routables.foldl(keys, (AbstractRanges) ranges, (key, value, keyIndex) -> {
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

    /**
     * Find the minimum intersecting transaction id for the range
     */
    public TxnId minTxnId(Range range, TxnId orElse)
    {
        keysToTxnIds();
        int startKeyIndex = keys.indexOf(range.start());
        if (startKeyIndex < 0) startKeyIndex = -1 - startKeyIndex;
        else if (isRangeStartExclusive()) ++startKeyIndex;

        int endKeyIndex = keys.indexOf(range.end());
        if (endKeyIndex < 0) endKeyIndex = -1 - startKeyIndex;
        else if (isRangeEndInclusive()) ++endKeyIndex;

        if (endKeyIndex <= startKeyIndex)
            return orElse;

        int minIndex = Integer.MAX_VALUE;
        for (int keyIndex = startKeyIndex ; keyIndex < endKeyIndex ; ++keyIndex)
        {
            int index = startOffset(keyIndex);
            minIndex = Math.min(minIndex, keysToTxnIds[index]);
        }

        if (minIndex == Integer.MAX_VALUE)
            return orElse;

        return txnIds[minIndex];
    }

    /**
     * Find the maximum intersecting transaction id for the range
     */
    public TxnId maxTxnId(Range range, TxnId orElse)
    {
        keysToTxnIds();
        int startKeyIndex = keys.indexOf(range.start());
        if (startKeyIndex < 0) startKeyIndex = -1 - startKeyIndex;
        else if (isRangeStartExclusive()) ++startKeyIndex;

        int endKeyIndex = keys.indexOf(range.end());
        if (endKeyIndex < 0) endKeyIndex = -1 - startKeyIndex;
        else if (isRangeEndInclusive()) ++endKeyIndex;

        if (endKeyIndex <= startKeyIndex)
            return orElse;

        int maxIndex = -1;
        for (int keyIndex = startKeyIndex ; keyIndex < endKeyIndex ; ++keyIndex)
        {
            int index = endOffset(keyIndex) - 1;
            maxIndex = Math.max(maxIndex, keysToTxnIds[index]);
        }

        if (maxIndex == -1)
            return orElse;

        return txnIds[maxIndex];
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

    public <P1, P2> void forEach(Unseekables<?> unseekables, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        Routable.Domain domain = unseekables.domain();
        switch (domain)
        {
            default: throw new UnhandledEnum(domain);
            case Key: forEach((AbstractUnseekableKeys) unseekables, p1, p2, forEach); break;
            case Range: forEach((AbstractRanges) unseekables, p1, p2, forEach); break;
        }
    }
    public <P1, P2> void forEach(AbstractUnseekableKeys keys, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        if (keys.isEmpty())
            return;

        int[] keysToTxnIds = keysToTxnIds();
        int searchFromIndex = -1;

        for (int i = 0; i < keys.size() ; ++i)
        {
            int keyIndex = searchFromIndex < 0 ? this.keys.indexOf(keys.get(i))
                                               : this.keys.findNext(searchFromIndex, keys.get(i), FAST);

            if (keyIndex < 0) searchFromIndex = -1 - keyIndex;
            else
            {
                int index = startOffset(keyIndex);
                int end = endOffset(keyIndex);
                while (index < end)
                {
                    int txnIdx = keysToTxnIds[index++];
                    forEach.accept(p1, p2, txnIdx);
                }
                searchFromIndex = keyIndex + 1;
            }
        }
    }

    public <P1, P2> void forEach(AbstractRanges ranges, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        for (int i = 0; i < ranges.size(); ++i)
            forEach(ranges.get(i), p1, p2, forEach);
    }

    public <P1, P2> int forEach(Range range, P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        return forEach(range, forEach, p1, p2, IndexedBiConsumer::accept);
    }

    public <P1, P2, P3> int forEach(Range range, P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
    {
        int[] keysToTxnIds = keysToTxnIds();
        int start = keys.indexOf(range.start());
        if (start < 0) start = -1 - start;
        else if (isRangeStartExclusive()) ++start;
        start = startOffset(start);

        int end = keys.indexOf(range.end());
        if (end < 0) end = -1 - end;
        else if (isRangeEndInclusive()) ++end;
        end = startOffset(end);

        while (start < end)
        {
            int txnIdx = keysToTxnIds[start++];
            forEach.accept(p1, p2, p3, txnIdx);
        }

        return end;
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
        if (keysToTxnIds != null) return keysToTxnIds.length - keys.size();
        else return txnIdsToKeys.length - txnIds.length;
    }

    public TxnId txnId(int i)
    {
        return txnIds[i].withoutNonIdentityFlags();
    }

    public TxnId txnIdWithFlags(int i)
    {
        return txnIds[i];
    }

    public int indexOf(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId);
    }

    public SortedArrayList<TxnId> txnIdsWithFlags()
    {
        return new SortedArrayList<>(txnIds);
    }

    public DepRelationList txnIdsWithFlags(RoutingKey key)
    {
        int keyIndex = keys.indexOf(key);
        if (keyIndex < 0)
            return DepRelationList.EMPTY;

        return txnIdsForKeyIndex(keyIndex);
    }

    public DepRelationList txnIdsForKeyIndex(int keyIndex)
    {
        int[] keysToTxnIds = keysToTxnIds();
        int start = startOffset(keyIndex);
        int end = endOffset(keyIndex);
        return txnIdsWithFlags(keysToTxnIds, start, end);
    }

    @SuppressWarnings("unchecked")
    public DepRelationList txnIdsWithFlags(Range range)
    {
        int startIndex = keys.indexOf(range.start());
        if (startIndex < 0) startIndex = -1 - startIndex;
        else if (isRangeStartExclusive()) ++startIndex;

        int endIndex = keys.indexOf(range.end());
        if (endIndex < 0) endIndex = -1 - endIndex;
        else if (isRangeEndInclusive()) ++endIndex;

        if (startIndex == endIndex)
            return DepRelationList.EMPTY;

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
        return txnIdsWithFlags(ids, 0, count);
    }

    @SuppressWarnings("unchecked")
    private DepRelationList txnIdsWithFlags(int[] ids, int start, int end)
    {
        if (start == end)
            return DepRelationList.EMPTY;

        return new DepRelationList(txnIds, ids, start, end);
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
        if (preferByKey(that)) return testEquality(this.keys.keys, this.txnIds, this.keysToTxnIds(), that.keys.keys, that.txnIds, that.keysToTxnIds());
        else return testEquality(this.txnIds, this.keys.keys, this.txnIdsToKeys(), that.txnIds, that.keys.keys, that.txnIdsToKeys());
    }

    @Override
    public Iterator<Map.Entry<RoutingKey, TxnId>> iterator()
    {
        return newIterator(keys.keys, txnIds, keysToTxnIds);
    }

    @Override
    public String toString()
    {
        return toSimpleString(keys.keys, txnIds, keysToTxnIds());
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
        @Override public BiFunction<TxnId, TxnId, TxnId> valueMerger() { return TxnId::addFlags; }
        @Override public int compareKeys(RoutingKey a, RoutingKey b) { return a.compareTo(b); }
        @Override public int compareValues(TxnId a, TxnId b) { return a.compareTo(b); }
        @Override public ObjectBuffers<RoutingKey> cachedKeys() { return ArrayBuffers.cachedRoutingKeys(); }
        @Override public ObjectBuffers<TxnId> cachedValues() { return ArrayBuffers.cachedTxnIds(); }
    }
}
