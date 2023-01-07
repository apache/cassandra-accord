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

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import java.util.stream.Collectors;

import accord.api.Key;
import accord.utils.ArrayBuffers;
import accord.api.RoutingKey;
import accord.utils.SortedArrays;
import accord.utils.Invariants;

import static accord.utils.ArrayBuffers.*;
import static accord.utils.SortedArrays.*;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * A collection of dependencies for a transaction, organised by the key the dependency is adopted via.
 * An inverse map from TxnId to Key may also be constructed and stored in this collection.
 */
// TODO (desired, consider): switch to RoutingKey? Would mean adopting execution dependencies less precisely, but saving ser/deser of large keys
public class Deps implements Iterable<Map.Entry<Key, TxnId>>
{
    private static final boolean DEBUG_CHECKS = true;

    private static final TxnId[] NO_TXNIDS = new TxnId[0];
    private static final int[] NO_INTS = new int[0];
    public static final Deps NONE = new Deps(Keys.EMPTY, NO_TXNIDS, NO_INTS);

    public static class SerializerSupport
    {
        private SerializerSupport() {}

        public static int keyToTxnIdCount(Deps deps)
        {
            return deps.keyToTxnId.length;
        }

        public static int keyToTxnId(Deps deps, int idx)
        {
            return deps.keyToTxnId[idx];
        }

        public static Deps create(Keys keys, TxnId[] txnIds, int[] keyToTxnId)
        {
            return new Deps(keys, txnIds, keyToTxnId);
        }
    }

    public static Deps none(Keys keys)
    {
        int[] keysToTxnId = new int[keys.size()];
        Arrays.fill(keysToTxnId, keys.size());
        return new Deps(keys, NO_TXNIDS, keysToTxnId);
    }

    /**
     * Expects Command to be provided in TxnId order
     */
    public static OrderedBuilder orderedBuilder(boolean hasOrderedTxnId)
    {
        return new OrderedBuilder(hasOrderedTxnId);
    }

    // TODO (expected, efficiency): cache this object per thread
    public static abstract class AbstractOrderedBuilder<T extends Deps> implements AutoCloseable
    {
        final ObjectBuffers<TxnId> cachedTxnIds = cachedTxnIds();
        final ObjectBuffers<Key> cachedKeys = cachedKeys();
        final IntBuffers cachedInts = cachedInts();

        final boolean hasOrderedTxnId;
        Key[] keys;
        int[] keyLimits;
        // txnId -> Offset
        TxnId[] keyToTxnId;
        int keyCount;
        int keyOffset;
        int totalCount;

        public AbstractOrderedBuilder(boolean hasOrderedTxnId)
        {
            this.keys = cachedKeys.get(16);
            this.keyLimits = cachedInts.getInts(keys.length);
            this.hasOrderedTxnId = hasOrderedTxnId;
            this.keyToTxnId = cachedTxnIds.get(16);
        }

        public boolean isEmpty()
        {
            return totalCount() == 0;
        }

        private int totalCount()
        {
            return totalCount;
        }

        public void nextKey(Key key)
        {
            if (keyCount > 0 && keys[keyCount - 1].compareTo(key) >= 0)
            {
                throw new IllegalArgumentException("Key " + key + " has already been visited or was provided out of order ("
                        + Arrays.toString(Arrays.copyOf(keys, keyCount)) + ")");
            }

            finishKey();

            if (keyCount == keys.length)
            {
                Key[] newKeys = cachedKeys.get(keyCount * 2);
                System.arraycopy(keys, 0, newKeys, 0, keyCount);
                cachedKeys.forceDiscard(keys, keyCount);
                keys = newKeys;
                int[] newKeyLimits = cachedInts.getInts(keyCount * 2);
                System.arraycopy(keyLimits, 0, newKeyLimits, 0, keyCount);
                cachedInts.forceDiscard(keyLimits, keyCount);
                keyLimits = newKeyLimits;
            }
            keys[keyCount++] = key;
        }

        private void finishKey()
        {
            if (totalCount == keyOffset && keyCount > 0)
                --keyCount; // remove this key; no data

            if (keyCount == 0)
                return;

            if (totalCount != keyOffset && !hasOrderedTxnId)
            {
                // TODO (low priority, efficiency): this allocates a significant amount of memory: would be preferable to be able to sort using a pre-defined scratch buffer
                Arrays.sort(keyToTxnId, keyOffset, totalCount);
                for (int i = keyOffset + 1 ; i < totalCount ; ++i)
                {
                    if (keyToTxnId[i - 1].equals(keyToTxnId[i]))
                        throw new IllegalArgumentException("TxnId for " + keys[keyCount - 1] + " are not unique: " + Arrays.asList(keyToTxnId).subList(keyOffset, totalCount));
                }
            }

            keyLimits[keyCount - 1] = totalCount;
            keyOffset = totalCount;
        }

        public void add(Key key, TxnId txnId)
        {
            if (keyCount == 0 || !keys[keyCount - 1].equals(key))
                nextKey(key);
            add(txnId);
        }

        /**
         * Add this command as a dependency for each intersecting key
         */
        public void add(TxnId txnId)
        {
            if (hasOrderedTxnId && totalCount > keyOffset && keyToTxnId[totalCount - 1].compareTo(txnId) >= 0)
                throw new IllegalArgumentException("TxnId provided out of order");

            if (totalCount >= keyToTxnId.length)
            {
                TxnId[] newTxnIds = cachedTxnIds.get(keyToTxnId.length * 2);
                System.arraycopy(keyToTxnId, 0, newTxnIds, 0, totalCount);
                cachedTxnIds.forceDiscard(keyToTxnId, totalCount);
                keyToTxnId = newTxnIds;
            }

            keyToTxnId[totalCount++] = txnId;
        }

        public T build()
        {
            if (totalCount == 0)
                return build(Keys.EMPTY, NO_TXNIDS, NO_INTS);

            finishKey();

            TxnId[] uniqueTxnId = cachedTxnIds.get(totalCount);
            System.arraycopy(keyToTxnId, 0, uniqueTxnId, 0, totalCount);
            Arrays.sort(uniqueTxnId, 0, totalCount);
            int txnIdCount = 1;
            for (int i = 1 ; i < totalCount ; ++i)
            {
                if (!uniqueTxnId[txnIdCount - 1].equals(uniqueTxnId[i]))
                    uniqueTxnId[txnIdCount++] = uniqueTxnId[i];
            }

            TxnId[] txnIds = cachedTxnIds.complete(uniqueTxnId, txnIdCount);
            cachedTxnIds.discard(uniqueTxnId, totalCount);

            int[] result = new int[keyCount + totalCount];
            int offset = keyCount;
            for (int k = 0 ; k < keyCount ; ++k)
            {
                result[k] = keyCount + keyLimits[k];
                int from = k == 0 ? 0 : keyLimits[k - 1];
                int to = keyLimits[k];
                offset = (int)SortedArrays.foldlIntersection(txnIds, 0, txnIdCount, keyToTxnId, from, to, (key, p, v, li, ri) -> {
                    result[(int)v] = li;
                    return v + 1;
                }, keyCount, offset, -1);
            }

            return build(Keys.ofSortedUnchecked(cachedKeys.complete(keys, keyCount)), txnIds, result);
        }

        abstract T build(Keys keys, TxnId[] txnIds, int[] keyToTxnId);

        @Override
        public void close()
        {
            cachedKeys.discard(keys, keyCount);
            cachedInts.forceDiscard(keyLimits, keyCount);
            cachedTxnIds.forceDiscard(keyToTxnId, totalCount);
        }
    }

    public static class OrderedBuilder extends AbstractOrderedBuilder<Deps>
    {
        public OrderedBuilder(boolean hasOrderedTxnId)
        {
            super(hasOrderedTxnId);
        }

        @Override
        Deps build(Keys keys, TxnId[] txnIds, int[] keysToTxnIds)
        {
            return new Deps(keys, txnIds, keysToTxnIds);
        }
    }

    /**
     * An object for managing a sequence of efficient linear merges Deps objects.
     * Its primary purpose is to manage input and output buffers, so that we reuse output buffers
     * as input to the next merge, and if any input is a superset of the other inputs that this input
     * is returned unmodified.
     *
     * This is achieved by using PassThroughXBuffers so that the result buffers (and their sizes) are returned
     * unmodified, and the buffers are cached as far as possible. In general, the buffers should be taken
     * out of pre-existing caches, but if the buffers are too large then we cache any additional buffers we
     * allocate for the duration of the merge.
     */
    private static class LinearMerger extends PassThroughObjectAndIntBuffers<TxnId> implements DepsConstructor<Key, TxnId, Object>
    {
        final PassThroughObjectBuffers<Key> keyBuffers;
        Key[] bufKeys;
        TxnId[] bufTxnIds;
        int[] buf = null;
        int bufKeysLength, bufTxnIdsLength = 0, bufLength = 0;
        Deps from = null;

        LinearMerger()
        {
            super(cachedTxnIds(), cachedInts());
            keyBuffers = new PassThroughObjectBuffers<>(cachedKeys());
        }

        @Override
        public Object construct(Key[] keys, int keysLength, TxnId[] txnIds, int txnIdsLength, int[] out, int outLength)
        {
            if (from == null)
            {
                // if our input buffers were themselves buffers, we want to discard them unless they have been returned back to us
                discard(keys, txnIds, out);
            }
            else if (buf != out)
            {
                // the output is not equal to a prior input
                from = null;
            }

            if (from == null)
            {
                bufKeys = keys;
                bufKeysLength = keysLength;
                bufTxnIds = txnIds;
                bufTxnIdsLength = txnIdsLength;
                buf = out;
                bufLength = outLength;
            }
            else
            {
                Invariants.checkState(keys == bufKeys && keysLength == bufKeysLength);
                Invariants.checkState(txnIds == bufTxnIds && txnIdsLength == bufTxnIdsLength);
                Invariants.checkState(outLength == bufLength);
            }
            return null;
        }

        void update(Deps deps)
        {
            if (buf == null)
            {
                bufKeys = deps.keys.keys;
                bufKeysLength = deps.keys.keys.length;
                bufTxnIds = deps.txnIds;
                bufTxnIdsLength = deps.txnIds.length;
                buf = deps.keyToTxnId;
                bufLength = deps.keyToTxnId.length;
                from = deps;
                return;
            }

            linearUnion(
                    bufKeys, bufKeysLength, bufTxnIds, bufTxnIdsLength, buf, bufLength,
                    deps.keys.keys, deps.keys.keys.length, deps.txnIds, deps.txnIds.length, deps.keyToTxnId, deps.keyToTxnId.length,
                    keyBuffers, this, this, this
            );
            if (buf == deps.keyToTxnId)
            {
                Invariants.checkState(deps.keys.keys == bufKeys && deps.keys.keys.length == bufKeysLength);
                Invariants.checkState(deps.txnIds == bufTxnIds && deps.txnIds.length == bufTxnIdsLength);
                Invariants.checkState(deps.keyToTxnId.length == bufLength);
                from = deps;
            }
        }

        Deps get()
        {
            if (buf == null)
                return NONE;

            if (from != null)
                return from;

            return new Deps(
                    Keys.ofSortedUnchecked(keyBuffers.realComplete(bufKeys, bufKeysLength)),
                    realComplete(bufTxnIds, bufTxnIdsLength),
                    realComplete(buf, bufLength));
        }

        /**
         * Free any buffers we no longer need
         */
        void discard()
        {
            if (from == null)
                discard(null, null, null);
        }

        /**
         * Free buffers unless they are equal to the corresponding parameter
         */
        void discard(Key[] freeKeysIfNot, TxnId[] freeTxnIdsIfNot, int[] freeBufIfNot)
        {
            if (from != null)
                return;

            if (bufKeys != freeKeysIfNot)
            {
                keyBuffers.realDiscard(bufKeys, bufKeysLength);
                bufKeys = null;
            }
            if (bufTxnIds != freeTxnIdsIfNot)
            {
                realDiscard(bufTxnIds, bufTxnIdsLength);
                bufTxnIds = null;
            }
            if (buf != freeBufIfNot)
            {
                realDiscard(buf, bufLength);
                buf = null;
            }
        }
    }

    public static <T> Deps merge(List<T> merge, Function<T, Deps> getter)
    {
        LinearMerger linearMerger = new LinearMerger();
        try
        {
            int mergeIndex = 0, mergeSize = merge.size();
            while (mergeIndex < mergeSize)
            {
                Deps deps = getter.apply(merge.get(mergeIndex++));
                if (deps == null || deps.isEmpty())
                    continue;

                linearMerger.update(deps);
            }

            return linearMerger.get();
        }
        finally
        {
            linearMerger.discard();
        }
    }

    final Keys keys; // unique Keys
    final TxnId[] txnIds; // unique TxnId TODO (low priority, efficiency): this could be a BTree?

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
    final int[] keyToTxnId; // Key -> [TxnId]
    // Lazy loaded in ensureTxnIdToKey()
    int[] txnIdToKey; // TxnId -> [Key]

    Deps(Keys keys, TxnId[] txnIds, int[] keyToTxnId)
    {
        this.keys = keys;
        this.txnIds = txnIds;
        this.keyToTxnId = keyToTxnId;
        if (!(keys.isEmpty() || keyToTxnId[keys.size() - 1] == keyToTxnId.length))
            throw new IllegalArgumentException(String.format("Last key (%s) in keyToTxnId does not point (%d) to the end of the array (%d);\nkeyToTxnId=%s", keys.get(keys.size() - 1), keyToTxnId[keys.size() - 1], keyToTxnId.length, Arrays.toString(keyToTxnId)));
        if (DEBUG_CHECKS)
            checkValid();
    }

    public PartialDeps slice(Ranges ranges)
    {
        if (isEmpty())
            return new PartialDeps(ranges, keys, txnIds, keyToTxnId);

        Keys select = keys.slice(ranges);

        if (select.isEmpty())
            return new PartialDeps(ranges, Keys.EMPTY, NO_TXNIDS, NO_INTS);

        if (select.size() == keys.size())
            return new PartialDeps(ranges, keys, txnIds, keyToTxnId);

        int i = 0;
        int offset = select.size();
        for (int j = 0 ; j < select.size() ; ++j)
        {
            int findi = keys.findNext(i, select.get(j), FAST);
            if (findi < 0)
                continue;

            i = findi;
            offset += keyToTxnId[i] - (i == 0 ? keys.size() : keyToTxnId[i - 1]);
        }

        int[] src = keyToTxnId;
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

        TxnId[] txnIds = trimUnusedTxnId(select, this.txnIds, trg);
        return new PartialDeps(ranges, select, txnIds, trg);
    }

    /**
     * Returns the set of {@link TxnId}s that are referenced by {@code keysToTxnId}, and <strong>updates</strong>
     * {@code keysToTxnId} to point to the new offsets in the returned set.
     * @param keys object referenced by {@code keysToTxnId} index
     * @param txnIds to trim to the seen {@link TxnId}s
     * @param keysToTxnId to use as reference for trimming, this index will be updated to reflect the trimmed offsets.
     * @return smallest set of {@link TxnId} seen in {@code keysToTxnId}
     */
    private static TxnId[] trimUnusedTxnId(Keys keys, TxnId[] txnIds, int[] keysToTxnId)
    {
        IntBuffers cache = ArrayBuffers.cachedInts();
        // we use remapTxnId twice:
        //  - first we use the end to store a bitmap of those TxnId we are actually using
        //  - then we use it to store the remap index (incrementally replacing the bitmap)
        int bitMapOffset = txnIds.length + 1 - (txnIds.length+31)/32;
        int[] remapTxnId = cache.getInts(txnIds.length + 1);
        try
        {
            Arrays.fill(remapTxnId, bitMapOffset, txnIds.length + 1, 0);
            for (int i = keys.size() ; i < keysToTxnId.length ; ++i)
                setBit(remapTxnId, bitMapOffset, keysToTxnId[i]);

            int offset = 0;
            for (int i = 0 ; i < txnIds.length ; ++i)
            {
                if (hasSetBit(remapTxnId, bitMapOffset, i)) remapTxnId[i] = offset++;
                else remapTxnId[i] = -1;
            }

            TxnId[] result = txnIds;
            if (offset < txnIds.length)
            {
                result = new TxnId[offset];
                for (int i = 0 ; i < txnIds.length ; ++i)
                {
                    if (remapTxnId[i] >= 0)
                        result[remapTxnId[i]] = txnIds[i];
                }
                // Update keysToTxnId to point to the new remapped TxnId offsets
                for (int i = keys.size() ; i < keysToTxnId.length ; ++i)
                    keysToTxnId[i] = remapTxnId[keysToTxnId[i]];
            }

            return result;
        }
        finally
        {
            cache.forceDiscard(remapTxnId, txnIds.length);
        }
    }

    public Deps with(Deps that)
    {
        if (isEmpty() || that.isEmpty())
            return isEmpty() ? that : this;

        return linearUnion(
                this.keys.keys, this.keys.keys.length, this.txnIds, this.txnIds.length, this.keyToTxnId, this.keyToTxnId.length,
                that.keys.keys, that.keys.keys.length, that.txnIds, that.txnIds.length, that.keyToTxnId, that.keyToTxnId.length,
                cachedKeys(), cachedTxnIds(), cachedInts(),
                (keys, keysLength, txnIds, txnIdsLength, out, outLength) ->
                        new Deps(Keys.ofSortedUnchecked(cachedKeys().complete(keys, keysLength)),
                                cachedTxnIds().complete(txnIds, txnIdsLength),
                                cachedInts().complete(out, outLength))
                );
    }

    /**
     * Turn a set of key, value and mapping buffers into a merge result;
     * K and V are either Key and TxnId, or vice versa, depending on which mapping direction was present
     */
    interface DepsConstructor<K, V, T>
    {
        T construct(K[] keys, int keysLength, V[] values, int valuesLength, int[] out, int outLength);
    }

    private static boolean arraysEqual(int[] left, int[] right, int length)
    {
        if (left.length < length || right.length < length)
            return false;

        for (int i=0; i<length; i++)
            if (left[i] !=right[i])
                return false;

        return true;
    }

    private static <T> boolean arraysEqual(T[] left, T[] right, int length)
    {
        if (left.length < length || right.length < length)
            return false;

        for (int i=0; i<length; i++)
            if (!Objects.equals(left[i], right[i]))
                return false;

        return true;
    }

    // TODO (low priority, efficiency): this method supports merging keyToTxnId OR txnIdToKey; we can perhaps save time
    //  and effort when constructing Deps on remote hosts by only producing txnIdToKey with OrderedCollector and serializing
    //  only this, and merging on the recipient before inverting, so that we only have to invert the final assembled deps
    private static <K extends Comparable<? super K>, V extends Comparable<? super V>, T>
    T linearUnion(K[] leftKeys, int leftKeysLength, V[] leftValues, int leftValuesLength, int[] left, int leftLength,
                  K[] rightKeys, int rightKeysLength, V[] rightValues, int rightValuesLength, int[] right, int rightLength,
                  ObjectBuffers<K> keyBuffers, ObjectBuffers<V> valueBuffers, IntBuffers intBuffers, DepsConstructor<K, V, T> constructor)
    {
        K[] outKeys = null;
        V[] outValues = null;
        int[] remapLeft = null, remapRight = null, out = null;
        int outLength = 0, outKeysLength = 0, outTxnIdsLength = 0;

        try
        {
            outKeys = SortedArrays.linearUnion(leftKeys, leftKeysLength, rightKeys, rightKeysLength, keyBuffers);
            outKeysLength = keyBuffers.lengthOfLast(outKeys);
            outValues = SortedArrays.linearUnion(leftValues, leftValuesLength, rightValues, rightValuesLength, valueBuffers);
            outTxnIdsLength = valueBuffers.lengthOfLast(outValues);

            remapLeft = remapToSuperset(leftValues, leftValuesLength, outValues, outTxnIdsLength, intBuffers);
            remapRight = remapToSuperset(rightValues, rightValuesLength, outValues, outTxnIdsLength, intBuffers);

            if (remapLeft == null && remapRight == null && leftLength == rightLength && leftKeysLength == rightKeysLength
                    && arraysEqual(left, right, rightLength)
                    && arraysEqual(leftKeys, rightKeys, rightKeysLength)
                )
            {
                return constructor.construct(leftKeys, leftKeysLength, leftValues, leftValuesLength, left, leftLength);
            }

            int lk = 0, rk = 0, ok = 0, l = leftKeysLength, r = rightKeysLength;
            outLength = outKeysLength;

            if (remapLeft == null && outKeys == leftKeys)
            {
                // "this" knows all the TxnId and Keys already, but do both agree on what Keys map to TxnIds?
                noOp: while (lk < leftKeysLength && rk < rightKeysLength)
                {
                    int ck = leftKeys[lk].compareTo(rightKeys[rk]);
                    if (ck < 0)
                    {
                        // "this" knows of a key not present in "that"
                        outLength += left[lk] - l; // logically append the key's TxnIds to the size
                        l = left[lk];
                        assert outLength == l && ok == lk && left[ok] == outLength;
                        ok++;
                        lk++;
                    }
                    else if (ck > 0)
                    {
                        // if this happened there is a bug with keys.union or keys are not actually sorted
                        throwUnexpectedMissingKeyException(leftKeys, lk, leftKeysLength, rightKeys, rk, rightKeysLength, true);
                    }
                    else
                    {
                        // both "this" and "that" know of the key
                        while (l < left[lk] && r < right[rk])
                        {
                            int nextLeft = left[l];
                            int nextRight = remap(right[r], remapRight);

                            if (nextLeft < nextRight)
                            {
                                // "this" knows of the txn that "that" didn't
                                outLength++;
                                l++;
                            }
                            else if (nextRight < nextLeft)
                            {
                                out = copy(left, outLength, leftLength + rightLength - r, intBuffers);
                                break noOp;
                            }
                            else
                            {
                                outLength++;
                                l++;
                                r++;
                            }
                        }

                        if (l < left[lk])
                        {
                            outLength += left[lk] - l;
                            l = left[lk];
                        }
                        else if (r < right[rk])
                        {
                            // "that" thinks a key includes a TxnId as a dependency but "this" doesn't, need to include this knowledge
                            out = copy(left, outLength, leftLength + rightLength - r, intBuffers);
                            break;
                        }

                        assert outLength == l && ok == lk && left[ok] == outLength;
                        ok++;
                        rk++;
                        lk++;
                    }
                }

                if (out == null)
                    return constructor.construct(leftKeys, leftKeysLength, leftValues, leftValuesLength, left, leftLength);
            }
            else if (remapRight == null && outKeys == rightKeys)
            {
                // "that" knows all the TxnId and keys already, but "this" does not
                noOp: while (lk < leftKeysLength && rk < rightKeysLength)
                {
                    int ck = leftKeys[lk].compareTo(rightKeys[rk]);
                    if (ck < 0)
                    {
                        // if this happened there is a bug with keys.union or keys are not actually sorted
                        throwUnexpectedMissingKeyException(leftKeys, lk, leftKeysLength, rightKeys, rk, rightKeysLength, false);
                    }
                    else if (ck > 0)
                    {
                        outLength += right[rk] - r;
                        r = right[rk];
                        assert outLength == r && ok == rk && right[ok] == outLength;
                        ok++;
                        rk++;
                    }
                    else
                    {
                        // both "this" and "that" know of the key
                        while (l < left[lk] && r < right[rk])
                        {
                            int nextLeft = remap(left[l], remapLeft);
                            int nextRight = right[r];

                            if (nextLeft < nextRight)
                            {
                                // "this" thinks a TxnID depends on Key but "that" doesn't, need to include this knowledge
                                out = copy(right, outLength, rightLength + leftLength - l, intBuffers);
                                break noOp;
                            }
                            else if (nextRight < nextLeft)
                            {
                                // "that" knows of the txn that "this" didn't
                                outLength++;
                                r++;
                            }
                            else
                            {
                                outLength++;
                                l++;
                                r++;
                            }
                        }

                        if (l < left[lk])
                        {
                            out = copy(right, outLength, rightLength + leftLength - l, intBuffers);
                            break;
                        }
                        else if (r < right[rk])
                        {
                            outLength += right[rk] - r;
                            r = right[rk];
                        }

                        assert outLength == r && ok == rk && right[ok] == outLength;
                        ok++;
                        rk++;
                        lk++;
                    }
                }

                if (out == null)
                    return constructor.construct(rightKeys, rightKeysLength, rightValues, rightValuesLength, right, rightLength);
            }
            else
            {
                out = intBuffers.getInts(leftLength + rightLength);
            }

            while (lk < leftKeysLength && rk < rightKeysLength)
            {
                int ck = leftKeys[lk].compareTo(rightKeys[rk]);
                if (ck < 0)
                {
                    while (l < left[lk])
                        out[outLength++] = remap(left[l++], remapLeft);
                    out[ok++] = outLength;
                    lk++;
                }
                else if (ck > 0)
                {
                    while (r < right[rk])
                        out[outLength++] = remap(right[r++], remapRight);
                    out[ok++] = outLength;
                    rk++;
                }
                else
                {
                    while (l < left[lk] && r < right[rk])
                    {
                        int nextLeft = remap(left[l], remapLeft);
                        int nextRight = remap(right[r], remapRight);

                        if (nextLeft <= nextRight)
                        {
                            out[outLength++] = nextLeft;
                            l += 1;
                            r += nextLeft == nextRight ? 1 : 0;
                        }
                        else
                        {
                            out[outLength++] = nextRight;
                            ++r;
                        }
                    }

                    while (l < left[lk])
                        out[outLength++] = remap(left[l++], remapLeft);

                    while (r < right[rk])
                        out[outLength++] = remap(right[r++], remapRight);

                    out[ok++] = outLength;
                    rk++;
                    lk++;
                }
            }

            while (lk < leftKeysLength)
            {
                while (l < left[lk])
                    out[outLength++] = remap(left[l++], remapLeft);
                out[ok++] = outLength;
                lk++;
            }

            while (rk < rightKeysLength)
            {
                while (r < right[rk])
                    out[outLength++] = remap(right[r++], remapRight);
                out[ok++] = outLength;
                rk++;
            }

            return constructor.construct(outKeys, outKeysLength, outValues, outTxnIdsLength, out, outLength);
        }
        finally
        {
            if (outKeys != null)
                keyBuffers.discard(outKeys, outKeysLength);
            if (outValues != null)
                valueBuffers.discard(outValues, outTxnIdsLength);
            if (out != null)
                intBuffers.discard(out, outLength);
            if (remapLeft != null)
                intBuffers.forceDiscard(remapLeft, leftValuesLength);
            if (remapRight != null)
                intBuffers.forceDiscard(remapRight, rightValuesLength);
        }
    }

    private static <A> void throwUnexpectedMissingKeyException(A[] leftKeys, int leftKeyIndex, int leftKeyLength, A[] rightKeys, int rightKeyIndex, int rightKeyLength, boolean isMissingLeft)
    {
        StringBuilder sb = new StringBuilder();
        String missing = isMissingLeft ? "left" : "right";
        String extra = isMissingLeft ? "right" : "left";
        sb.append(missing).append(" knows all keys, yet ").append(extra).append(" knew of an extra key at indexes left[")
                .append(leftKeyIndex).append("] = ").append(leftKeys[leftKeyIndex])
                .append(", right[").append(rightKeyIndex).append("] = ").append(rightKeys[rightKeyIndex]).append("\n");
        sb.append("leftKeys = ").append(Arrays.stream(leftKeys, 0, leftKeyLength).map(Object::toString).collect(Collectors.joining())).append('\n');
        sb.append("rightKeys = ").append(Arrays.stream(rightKeys, 0, rightKeyLength).map(Object::toString).collect(Collectors.joining())).append('\n');
        throw new IllegalStateException(sb.toString());
    }

    private static int[] copy(int[] src, int to, int length, IntBuffers bufferManager)
    {
        if (length == 0)
            return NO_INTS;

        int[] result = bufferManager.getInts(length);
        if (result.length < length)
            throw new IllegalStateException();
        System.arraycopy(src, 0, result, 0, to);
        return result;
    }

    public Deps without(Predicate<TxnId> remove)
    {
        if (isEmpty())
            return this;

        IntBuffers cache = ArrayBuffers.cachedInts();
        TxnId[] oldTxnIds = txnIds;
        int[] oldKeyToTxnId = keyToTxnId;
        int[] remapTxnIds = cache.getInts(oldTxnIds.length);
        int[] newKeyToTxnId = null;
        TxnId[] newTxnIds;
        int o = 0;
        try
        {
            int count = 0;
            for (int i = 0 ; i < oldTxnIds.length ; ++i)
            {
                if (remove.test(oldTxnIds[i])) remapTxnIds[i] = -1;
                else remapTxnIds[i] = count++;
            }

            if (count == oldTxnIds.length)
                return this;

            if (count == 0)
                return NONE;

            newTxnIds = new TxnId[count];
            for (int i = 0 ; i < oldTxnIds.length ; ++i)
            {
                if (remapTxnIds[i] >= 0)
                    newTxnIds[remapTxnIds[i]] = oldTxnIds[i];
            }

            newKeyToTxnId = cache.getInts(oldKeyToTxnId.length);
            int k = 0, i = keys.size();
            o = i;
            while (i < oldKeyToTxnId.length)
            {
                while (oldKeyToTxnId[k] == i)
                    newKeyToTxnId[k++] = o;

                int remapped = remapTxnIds[oldKeyToTxnId[i]];
                if (remapped >= 0)
                    newKeyToTxnId[o++] = remapped;
                ++i;
            }

            while (k < keys.size())
                newKeyToTxnId[k++] = o;
        }
        catch (Throwable t)
        {
            cache.forceDiscard(newKeyToTxnId, o);
            throw t;
        }
        finally
        {
            cache.forceDiscard(remapTxnIds, oldTxnIds.length);
        }

        newKeyToTxnId = cache.completeAndDiscard(newKeyToTxnId, o);
        return new Deps(keys, newTxnIds, newKeyToTxnId);
    }

    public boolean contains(TxnId txnId)
    {
        return Arrays.binarySearch(txnIds, txnId) >= 0;
    }

    // return true iff we map any keys to any txnId
    // if the mapping is empty we return false, whether or not we have any keys or txnId by themselves
    public boolean isEmpty()
    {
        return keyToTxnId.length == keys.size();
    }

    public Keys someKeys(TxnId txnId)
    {
        int txnIdIndex = Arrays.binarySearch(txnIds, txnId);
        if (txnIdIndex < 0)
            return Keys.EMPTY;

        ensureTxnIdToKey();

        int start = txnIdIndex == 0 ? txnIds.length : txnIdToKey[txnIdIndex - 1];
        int end = txnIdToKey[txnIdIndex];
        if (start == end)
            return Keys.EMPTY;

        Key[] result = new Key[end - start];
        for (int i = start ; i < end ; ++i)
            result[i - start] = keys.get(txnIdToKey[i]);
        return Keys.of(result);
    }

    public Unseekables<RoutingKey, ?> someRoutables(TxnId txnId)
    {
        return toUnseekables(txnId, array -> {
            if (array.length == 0)
                throw new IllegalStateException("Cannot create a RouteFragment without any keys");
            return new RoutingKeys(array);
        });
    }

    private <R> R toUnseekables(TxnId txnId, Function<RoutingKey[], R> constructor)
    {
        int txnIdIndex = Arrays.binarySearch(txnIds, txnId);
        if (txnIdIndex < 0)
            constructor.apply(RoutingKeys.EMPTY.keys);

        ensureTxnIdToKey();

        int start = txnIdIndex == 0 ? txnIds.length : txnIdToKey[txnIdIndex - 1];
        int end = txnIdToKey[txnIdIndex];
        RoutingKey[] result = new RoutingKey[end - start];
        if (start == end)
            constructor.apply(RoutingKeys.EMPTY.keys);

        result[0] = keys.get(txnIdToKey[start]).toUnseekable();
        int resultCount = 1;
        for (int i = start + 1 ; i < end ; ++i)
        {
            RoutingKey next = keys.get(txnIdToKey[i]).toUnseekable();
            if (!next.equals(result[resultCount - 1]))
                result[resultCount++] = next;
        }

        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);
        return constructor.apply(result);
    }

    void ensureTxnIdToKey()
    {
        if (txnIdToKey != null)
            return;

        txnIdToKey = invert(keyToTxnId, keyToTxnId.length, keys.size(), txnIds.length);
    }

    private static int[] invert(int[] src, int srcLength, int srcKeyCount, int trgKeyCount)
    {
        int[] trg = new int[trgKeyCount + srcLength - srcKeyCount];

        // first pass, count number of txnId per key
        for (int i = srcKeyCount ; i < srcLength ; ++i)
            trg[src[i]]++;

        // turn into offsets (i.e. add txnIds.size() and then sum them)
        trg[0] += trgKeyCount;
        for (int i = 1; i < trgKeyCount ; ++i)
            trg[i] += trg[i - 1];

        // shuffle forwards one, so we have the start index rather than end
        System.arraycopy(trg, 0, trg, 1, trgKeyCount - 1);
        trg[0] = trgKeyCount;

        // convert the offsets to end, and set the key at the target positions
        int k = 0;
        for (int i = srcKeyCount ; i < srcLength ; ++i)
        {
            // if at the end offset, switch to the next key
            while (i == src[k])
                ++k;

            // find the next key offset for the TxnId and set the offset to this key
            trg[trg[src[i]]++] = k;
        }

        return trg;
    }

    public void forEachOn(Ranges ranges, Predicate<Key> include, BiConsumer<Key, TxnId> forEach)
    {
        Routables.foldl(keys, ranges, (key, value, index) -> {
            if (!include.test(key))
                return null;

            for (int t = startOffset(index), end = endOffset(index); t < end ; ++t)
            {
                TxnId txnId = txnIds[keyToTxnId[t]];
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
    public void forEachOn(Ranges ranges, Consumer<TxnId> forEach)
    {
        // Find all keys within the ranges, but record existence within an int64 bitset.  Since the bitset is limited
        // to 64, this search must be called multiple times searching for different TxnIds in txnIds; this also has
        // the property that forEach is called in TxnId order.
        //TODO (expected, efficiency): reconsider this, probably not worth trying to save allocations at cost of multiple loop
        //                             use BitSet, or perhaps extend so we can have no nested allocations when few bits
        for (int offset = 0 ; offset < txnIds.length ; offset += 64)
        {
            long bitset = Routables.foldl(keys, ranges, (key, off, value, keyIndex) -> {
                int index = startOffset(keyIndex);
                int end = endOffset(keyIndex);
                if (off > 0)
                {
                    // TODO (low priority, efficiency): interpolation search probably great here
                    index = Arrays.binarySearch(keyToTxnId, index, end, (int)off);
                    if (index < 0)
                        index = -1 - index;
                }

                while (index < end)
                {
                    long next = keyToTxnId[index++] - off;
                    if (next >= 64)
                        break;
                    value |= 1L << next;
                }

                return value;
            }, offset, 0, -1L);

            while (bitset != 0)
            {
                int i = Long.numberOfTrailingZeros(bitset);
                TxnId txnId = txnIds[offset + i];
                forEach.accept(txnId);
                bitset ^= Long.lowestOneBit(bitset);
            }
        }
    }

    public void forEach(Key key, Consumer<TxnId> forEach)
    {
        int keyIndex = keys.indexOf(key);
        if (keyIndex < 0)
            return;

        int index = startOffset(keyIndex);
        int end = endOffset(keyIndex);
        while (index < end)
            forEach.accept(txnIds[keyToTxnId[index++]]);
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
        return keyToTxnId.length - keys.size();
    }

    public TxnId txnId(int i)
    {
        return txnIds[i];
    }

    public Collection<TxnId> txnIds()
    {
        return Arrays.asList(txnIds);
    }

    public List<TxnId> txnIds(Key key)
    {
        int keyIndex = keys.indexOf(key);
        if (keyIndex < 0)
            return Collections.emptyList();

        int start = startOffset(keyIndex);
        int end = endOffset(keyIndex);
        int size = end - start;

        return new AbstractList<TxnId>()
        {
            @Override
            public TxnId get(int index)
            {
                if (index > end)
                    throw new IndexOutOfBoundsException();
                return txnIds[keyToTxnId[start + index]];
            }

            @Override
            public int size()
            {
                return size;
            }
        };
    }

    private int startOffset(int keyIndex)
    {
        return keyIndex == 0 ? keys.size() : keyToTxnId[keyIndex - 1];
    }

    private int endOffset(int keyIndex)
    {
        return keyToTxnId[keyIndex];
    }

    @Override
    public Iterator<Map.Entry<Key, TxnId>> iterator()
    {
        return new Iterator<Map.Entry<Key, TxnId>>()
        {
            int i = keys.size(), k = 0;

            @Override
            public boolean hasNext()
            {
                return i < keyToTxnId.length;
            }

            @Override
            public Map.Entry<Key, TxnId> next()
            {
                Entry result = new Entry(keys.get(k), txnIds[keyToTxnId[i++]]);
                if (i == keyToTxnId[k])
                    ++k;
                return result;
            }
        };
    }

    @Override
    public String toString()
    {
        return toSimpleString();
    }

    public String toSimpleString()
    {
        if (keys.isEmpty())
            return "{}";

        StringBuilder builder = new StringBuilder("{");
        for (int k = 0, t = keys.size(); k < keys.size() ; ++k)
        {
            if (builder.length() > 1)
                builder.append(", ");

            builder.append(keys.get(k));
            builder.append(":[");
            boolean first = true;
            while (t < keyToTxnId[k])
            {
                if (first) first = false;
                else builder.append(", ");
                builder.append(txnIds[keyToTxnId[t++]]);
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return equals((Deps) o);
    }

    public boolean equals(Deps that)
    {
        return this.txnIds.length == that.txnIds.length
               && this.keys.size() == that.keys.size()
               && Arrays.equals(this.keyToTxnId, that.keyToTxnId)
               && Arrays.equals(this.txnIds, that.txnIds)
               && this.keys.equals(that.keys);
    }

    public static class Entry implements Map.Entry<Key, TxnId>
    {
        final Key key;
        final TxnId txnId;

        public Entry(Key key, TxnId txnId)
        {
            this.key = key;
            this.txnId = txnId;
        }

        @Override
        public Key getKey()
        {
            return key;
        }

        @Override
        public TxnId getValue()
        {
            return txnId;
        }

        @Override
        public TxnId setValue(TxnId value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return key + "->" + txnId;
        }
    }

    private void checkValid()
    {
        int k = 0;
        for (int i = keys.size() ; i < keyToTxnId.length ; ++i)
        {
            boolean first = true;
            while (i < keyToTxnId[k])
            {
                if (first) first = false;
                else if (keyToTxnId[i - 1] == keyToTxnId[i])
                {
                    Key key = keys.get(i);
                    TxnId txnId = txnIds[keyToTxnId[i]];
                    throw new IllegalStateException(String.format("Duplicate TxnId (%s) found for key %s", txnId, key));
                }
                i++;
            }
            ++k;
        }
    }

    private static void setBit(int[] array, int offset, int index)
    {
        array[offset + index / 32] |= (1 << (index & 31));
    }

    private static boolean hasSetBit(int[] array, int offset, int index)
    {
        return (array[offset + index / 32] & (1 << (index & 31))) != 0;
    }

}
