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

import accord.primitives.*;
import net.nicoulaj.compilecommand.annotations.Inline;

import java.util.*;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static accord.utils.ArrayBuffers.*;
import static accord.utils.Invariants.illegalState;
import static accord.utils.SortedArrays.remap;
import static accord.utils.SortedArrays.remapToSuperset;
import static java.lang.String.format;

/**
 * Instead of creating a parent object and having generic key/value methods, we introduce a bunch of static helper methods.
 * This class helps maintain three sorted arrays: keys (of type K[]), values (of type V[]) and keysToValues (of type int[])
 * <p>
 * keysToValues represents a map of {@code Key -> [Value] } where each Value is a pointer into a sorted values array.
 * The beginning of the array (the first keys.length entries) are offsets into this array.
 * <p>
 * Example:
 * <p>
 * {@code
 *   int keyIdx = Arrays.binarySearch(keys, key);
 *   int startOfValues = keyIdx == 0 ? keys.length : keysToValues[keyIdx - 1];
 *   int endOfValues = keysToValues[keyIdx];
 *   for (int i = startOfValues; i < endOfValues; i++)
 *   {
 *       V id = value[keysToValues[i]]
 *       ...
 *   }
 * }
 *
 * // TODO (expected, performance): it would be trivial to special-case transactions that intersect on all keys (e.g. by setting the top integer bit, and otherwise propagating the prior limit).
 *                                  This would also neatly optimise cases where we only have a single key.
 * // TODO (desired, performance): it would also be simple to bitmask our integers, compressing to e.g. byte or short boundaries as permitted. We could do this for all values, including our offsets header.
 */
public class RelationMultiMap
{
    private static final boolean DEBUG_CHECKS = true;
    public static final int[] NO_INTS = new int[0];

    /**
     * Turn a set of key, value and mapping buffers into a merge result;
     * K and V are either Key and TxnId, or vice versa, depending on which mapping direction was present
     */
    public interface Constructor<K, V, T>
    {
        T construct(K[] keys, int keysLength, V[] values, int valuesLength, int[] out, int outLength);
    }
    public interface SimpleConstructor<K, V, T>
    {
        T construct(K keys, V values, int[] out);
    }

    public interface Adapter<K, V>
    {
        SymmetricComparator<? super K> keyComparator();
        SymmetricComparator<? super V> valueComparator();
        ObjectBuffers<K> cachedKeys();
        ObjectBuffers<V> cachedValues();
    }


    // TODO (expected, efficiency): cache this object per thread
    public static abstract class AbstractBuilder<K, V, T> implements AutoCloseable
    {
        final Adapter<K, V> adapter;
        final ObjectBuffers<K> cachedKeys;
        final ObjectBuffers<V> cachedValues;
        final IntBuffers cachedInts = cachedInts();

        K[] keys;
        int[] keyLimits;
        // txnId -> Offset
        V[] keysToValues;
        int keyCount;
        int keyOffset;
        int totalCount;
        boolean hasOrderedKeys = true;
        boolean hasOrderedValues = true;

        public AbstractBuilder(Adapter<K, V> adapter)
        {
            this.adapter = adapter;
            this.cachedKeys = adapter.cachedKeys();
            this.cachedValues = adapter.cachedValues();
            this.keys = cachedKeys.get(16);
            this.keyLimits = cachedInts.getInts(keys.length);
            this.keysToValues = cachedValues.get(16);
        }

        public boolean isEmpty()
        {
            return totalCount() == 0;
        }

        private int totalCount()
        {
            return totalCount;
        }

        public void nextKey(K key)
        {
            if (keyCount > 0 && adapter.keyComparator().compare(keys[keyCount - 1], key) >= 0)
                hasOrderedKeys = false;

            finishKey();

            if (keyCount == keys.length)
            {
                K[] newKeys = cachedKeys.get(keyCount * 2);
                System.arraycopy(keys, 0, newKeys, 0, keyCount);
                cachedKeys.forceDiscard(keys, keyCount);
                keys = newKeys;
                int[] newKeyLimits = cachedInts.getInts(keyCount * 2);
                System.arraycopy(keyLimits, 0, newKeyLimits, 0, keyCount);
                cachedInts.forceDiscard(keyLimits);
                keyLimits = newKeyLimits;
            }
            keys[keyCount++] = key;
            hasOrderedValues = true;
        }

        private void finishKey()
        {
            if (totalCount == keyOffset && keyCount > 0)
            {
                --keyCount; // remove this key; no data
                return;
            }

            if (keyCount == 0)
                return;

            if (!hasOrderedValues)
            {
                // TODO (low priority, efficiency): this allocates a significant amount of memory: would be preferable to be able to sort using a pre-defined scratch buffer
                Arrays.sort(keysToValues, keyOffset, totalCount);
                int removed = 0;
                for (int i = keyOffset + 1 ; i < totalCount ; ++i)
                {
                    if (keysToValues[i - 1].equals(keysToValues[i])) ++removed;
                    else if (removed > 0) keysToValues[i - removed] = keysToValues[i];
                }
                totalCount -= removed;
            }

            keyLimits[keyCount - 1] = totalCount;
            keyOffset = totalCount;
        }

        public void add(K key, V value)
        {
            if (keyCount == 0 || !keys[keyCount - 1].equals(key))
                nextKey(key);
            add(value);
        }

        /**
         * Add this command as a dependency for each intersecting key
         */
        public void add(V value)
        {
            if (hasOrderedValues && totalCount > keyOffset && adapter.valueComparator().compare(keysToValues[totalCount - 1], value) >= 0)
                hasOrderedValues = false;

            if (totalCount >= keysToValues.length)
            {
                V[] newValues = cachedValues.get(keysToValues.length * 2);
                System.arraycopy(keysToValues, 0, newValues, 0, totalCount);
                cachedValues.forceDiscard(keysToValues, totalCount);
                keysToValues = newValues;
            }

            keysToValues[totalCount++] = value;
        }

        public T build()
        {
            if (totalCount == 0)
                return none();

            finishKey();

            V[] uniqueValues = cachedValues.get(totalCount);
            System.arraycopy(keysToValues, 0, uniqueValues, 0, totalCount);
            Arrays.sort(uniqueValues, 0, totalCount, adapter.valueComparator());
            int valueCount = 1;
            for (int i = 1 ; i < totalCount ; ++i)
            {
                if (!uniqueValues[valueCount - 1].equals(uniqueValues[i]))
                    uniqueValues[valueCount++] = uniqueValues[i];
            }

            V[] values = cachedValues.complete(uniqueValues, valueCount);
            cachedValues.discard(uniqueValues, totalCount);

            int[] sortedKeyIndexes;
            K[] sortedKeys;
            if (hasOrderedKeys)
            {
                sortedKeyIndexes = null;
                sortedKeys = cachedKeys.completeAndDiscard(keys, keyCount);
            }
            else
            {
                sortedKeyIndexes = new int[keyCount];
                sortedKeys = Arrays.copyOf(keys, keyCount);
                Arrays.sort(sortedKeys, adapter.keyComparator());

                for (int i = 1 ; i < keyCount ; ++i)
                {
                    if (sortedKeys[i-1].equals(sortedKeys[i]))
                        throw new IllegalArgumentException("Key " + sortedKeys[i] + " has been visited more than once ("
                                + Arrays.toString(Arrays.copyOf(keys, keyCount)) + ")");
                }
                for (int i = 0 ; i < keyCount ; ++i)
                    sortedKeyIndexes[Arrays.binarySearch(sortedKeys, keys[i], adapter.keyComparator())] = i;
                cachedKeys.forceDiscard(keys, keyCount);
            }

            int[] result = new int[keyCount + totalCount];
            int offset = keyCount;
            for (int ki = 0 ; ki < keyCount ; ++ki)
            {
                int k = sortedKeyIndexes == null ? ki : sortedKeyIndexes[ki];
                int from = k == 0 ? 0 : keyLimits[k - 1];
                int to = keyLimits[k];
                offset = (int)SortedArrays.foldlIntersection(adapter.valueComparator(), values, 0, valueCount, keysToValues, from, to, (key, p, v, li, ri) -> {
                    result[(int)v] = li;
                    return v + 1;
                }, 0, offset, -1);
                result[ki] = offset;
            }

            return build(sortedKeys, values, result);
        }

        protected abstract T none();
        protected abstract T build(K[] keys, V[] values, int[] keyToValue);

        @Override
        public void close()
        {
            cachedInts.forceDiscard(keyLimits);
            cachedValues.forceDiscard(keysToValues, totalCount);
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
    public static class LinearMerger<K, V, T> extends PassThroughObjectAndIntBuffers<V> implements Constructor<K, V, Object>, AutoCloseable
    {
        final Adapter<K, V> adapter;
        final PassThroughObjectBuffers<K> keyBuffers;
        K[] bufKeys;
        V[] bufValues;
        int[] buf = null;
        int bufKeysLength, bufValuesLength = 0, bufLength = 0;
        T from = null;

        public LinearMerger(Adapter<K, V> adapter)
        {
            super(adapter.cachedValues(), cachedInts());
            keyBuffers = new PassThroughObjectBuffers<>(adapter.cachedKeys());
            this.adapter = adapter;
        }

        @Override
        public Object construct(K[] keys, int keysLength, V[] txnIds, int txnIdsLength, int[] out, int outLength)
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
                bufValues = txnIds;
                bufValuesLength = txnIdsLength;
                buf = out;
                bufLength = outLength;
            }
            else
            {
                Invariants.checkState(keys == bufKeys && keysLength == bufKeysLength);
                Invariants.checkState(txnIds == bufValues && txnIdsLength == bufValuesLength);
                Invariants.checkState(outLength == bufLength);
            }
            return null;
        }

        public void update(T merge, K[] keys, V[] values, int[] keysToValues)
        {
            if (buf == null)
            {
                bufKeys = keys;
                bufKeysLength = keys.length;
                bufValues = values;
                bufValuesLength = values.length;
                buf = keysToValues;
                bufLength = keysToValues.length;
                from = merge;
                return;
            }

            linearUnion(
                    bufKeys, bufKeysLength, bufValues, bufValuesLength, buf, bufLength,
                    keys, keys.length, values, values.length, keysToValues, keysToValues.length,
                    adapter.keyComparator(), adapter.valueComparator(),
                    keyBuffers, this, this, this
            );
            if (buf == keysToValues)
            {
                Invariants.checkState(keys == bufKeys && keys.length == bufKeysLength);
                Invariants.checkState(values == bufValues && values.length == bufValuesLength);
                Invariants.checkState(keysToValues.length == bufLength);
                from = merge;
            }
        }

        public T get(SimpleConstructor<K[], V[], T> constructor, T none)
        {
            if (buf == null)
                return none;

            if (from != null)
                return from;

            return constructor.construct(keyBuffers.realComplete(bufKeys, bufKeysLength),
                    realComplete(bufValues, bufValuesLength),
                    realComplete(buf, bufLength));
        }

        /**
         * Free buffers unless they are equal to the corresponding parameter
         */
        void discard(K[] freeKeysIfNot, V[] freeValuesIfNot, int[] freeBufIfNot)
        {
            if (from != null)
                return;

            if (bufKeys != freeKeysIfNot)
            {
                keyBuffers.realDiscard(bufKeys, bufKeysLength);
                bufKeys = null;
            }
            if (bufValues != freeValuesIfNot)
            {
                realDiscard(bufValues, bufValuesLength);
                bufValues = null;
            }
            if (buf != freeBufIfNot)
            {
                realDiscard(buf, bufLength);
                buf = null;
            }
        }

        @Override
        public void close()
        {
            if (from == null)
                discard(null, null, null);
        }
    }

    public static class SortedRelationList<T extends Comparable<? super T>> extends AbstractList<T> implements SortedList<T>
    {
        public static final SortedRelationList EMPTY = new SortedRelationList(new Comparable[0], new int[0], 0, 0);

        final T[] values;
        final int[] ids;
        final int startIndex, endIndex;

        public SortedRelationList(T[] values, int[] ids, int startIndex, int endIndex)
        {
            this.values = values;
            this.ids = ids;
            this.startIndex = startIndex;
            this.endIndex = endIndex;
        }

        @Override
        public T get(int index)
        {
            return values[getValueIndex(index)];
        }

        public int getValueIndex(int index)
        {
            if (index >= endIndex)
                throw new IndexOutOfBoundsException(String.format("%d >= %d", index, endIndex));
            return ids[startIndex + index];
        }

        public T getForValueIndex(int valueIndex)
        {
            return values[valueIndex];
        }

        @Override
        public int size()
        {
            return endIndex - startIndex;
        }

        @Override
        public int findNext(int i, Comparable<? super T> find)
        {
            int idIdx = i + startIndex;
            int valueIdx = ids[idIdx];
            valueIdx = SortedArrays.exponentialSearch(values, valueIdx, values.length, find);
            boolean found = valueIdx >= 0;
            if (!found) valueIdx = -1 -valueIdx;
            return normalise(found, SortedArrays.exponentialSearch(ids, idIdx, endIndex, valueIdx));
        }

        public int findNext(int i, int valueIdx)
        {
            int idIdx = i + startIndex;
            return normalise(true, SortedArrays.exponentialSearch(ids, idIdx, endIndex, valueIdx));
        }

        @Override
        public int find(Comparable<? super T> find)
        {
            int valueIdx = Arrays.binarySearch(values, 0, values.length, find);
            boolean found = valueIdx >= 0;
            if (!found) valueIdx = -1 -valueIdx;
            // TODO (desired): use interpolation search (or binarySearch)
            return normalise(found, Arrays.binarySearch(ids, startIndex, endIndex, valueIdx));
        }

        private int normalise(boolean foundValue, int searchResult)
        {
            if (searchResult < 0)
                return searchResult + startIndex;

            searchResult -= startIndex;
            if (!foundValue) searchResult = -1 - searchResult;
            return searchResult;
        }
    }

    /**
     * Returns the set of {@link TxnId}s that are referenced by {@code keysToTxnId}, and <strong>updates</strong>
     * {@code keysToTxnId} to point to the new offsets in the returned set.
     * @param keys object referenced by {@code keysToTxnId} index
     * @param values to trim to the seen {@link TxnId}s
     * @param keysToTxnId to use as reference for trimming, this index will be updated to reflect the trimmed offsets.
     * @return smallest set of {@link TxnId} seen in {@code keysToTxnId}
     *
     * TODO (desired, efficiency): algorithmic complexity is poor if have shrunk TxnId dramatically.
     */
    public static <K, V> V[] trimUnusedValues(K[] keys, V[] values, int[] keysToTxnId, IntFunction<V[]> valueAllocator)
    {
        IntBuffers cache = ArrayBuffers.cachedInts();
        // we use remapTxnId twice:
        //  - first we use the end to store a bitmap of those TxnId we are actually using
        //  - then we use it to store the remap index (incrementally replacing the bitmap)
        int bitMapOffset = values.length + 1 - (values.length+31)/32;
        int[] remapTxnId = cache.getInts(values.length + 1);
        try
        {
            Arrays.fill(remapTxnId, bitMapOffset, values.length + 1, 0);
            for (int i = keys.length ; i < keysToTxnId.length ; ++i)
                setBit(remapTxnId, bitMapOffset, keysToTxnId[i]);

            int offset = 0;
            for (int i = 0 ; i < values.length ; ++i)
            {
                if (hasSetBit(remapTxnId, bitMapOffset, i)) remapTxnId[i] = offset++;
                else remapTxnId[i] = -1;
            }

            V[] result = values;
            if (offset < values.length)
            {
                result = valueAllocator.apply(offset);
                for (int i = 0 ; i < values.length ; ++i)
                {
                    if (remapTxnId[i] >= 0)
                        result[remapTxnId[i]] = values[i];
                }
                // Update keysToTxnId to point to the new remapped TxnId offsets
                for (int i = keys.length ; i < keysToTxnId.length ; ++i)
                    keysToTxnId[i] = remapTxnId[keysToTxnId[i]];
            }

            return result;
        }
        finally
        {
            cache.forceDiscard(remapTxnId);
        }
    }

    static boolean arraysEqual(int[] left, int[] right, int length)
    {
        if (left.length < length || right.length < length)
            return false;

        for (int i=0; i<length; i++)
            if (left[i] !=right[i])
                return false;

        return true;
    }

    static <T> boolean arraysEqual(T[] left, T[] right, int length)
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
    @Inline
    public static <K, V, T>
    T linearUnion(K[] leftKeys, int leftKeysLength, V[] leftValues, int leftValuesLength, int[] left, int leftLength,
                  K[] rightKeys, int rightKeysLength, V[] rightValues, int rightValuesLength, int[] right, int rightLength,
                  SymmetricComparator<? super K> keyComparator, SymmetricComparator<? super V> valueComparator,
                  ObjectBuffers<K> keyBuffers, ObjectBuffers<V> valueBuffers, IntBuffers intBuffers, Constructor<K, V, T> constructor)
    {
        K[] outKeys = null;
        V[] outValues = null;
        int[] remapLeft = null, remapRight = null, out = null;
        int outLength = 0, outKeysLength = 0, outValuesLength = 0;

        try
        {
            outKeys = SortedArrays.linearUnion(leftKeys, leftKeysLength, rightKeys, rightKeysLength, keyComparator, keyBuffers);
            outKeysLength = keyBuffers.sizeOfLast(outKeys);
            outValues = SortedArrays.linearUnion(leftValues, leftValuesLength, rightValues, rightValuesLength, valueComparator, valueBuffers);
            outValuesLength = valueBuffers.sizeOfLast(outValues);

            remapLeft = remapToSuperset(leftValues, leftValuesLength, outValues, outValuesLength, valueComparator, intBuffers);
            remapRight = remapToSuperset(rightValues, rightValuesLength, outValues, outValuesLength, valueComparator, intBuffers);

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
                    int ck = keyComparator.compare(leftKeys[lk], rightKeys[rk]);
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
                    int ck = keyComparator.compare(leftKeys[lk], rightKeys[rk]);
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
                int ck = keyComparator.compare(leftKeys[lk], rightKeys[rk]);
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

            return constructor.construct(outKeys, outKeysLength, outValues, outValuesLength, out, outLength);
        }
        finally
        {
            if (outKeys != null)
                keyBuffers.discard(outKeys, outKeysLength);
            if (outValues != null)
                valueBuffers.discard(outValues, outValuesLength);
            if (out != null)
                intBuffers.discard(out, outLength);
            if (remapLeft != null)
                intBuffers.forceDiscard(remapLeft);
            if (remapRight != null)
                intBuffers.forceDiscard(remapRight);
        }
    }

    static <A> void throwUnexpectedMissingKeyException(A[] leftKeys, int leftKeyIndex, int leftKeyLength, A[] rightKeys, int rightKeyIndex, int rightKeyLength, boolean isMissingLeft)
    {
        StringBuilder sb = new StringBuilder();
        String missing = isMissingLeft ? "left" : "right";
        String extra = isMissingLeft ? "right" : "left";
        sb.append(missing).append(" knows all keys, yet ").append(extra).append(" knew of an extra key at indexes left[")
                .append(leftKeyIndex).append("] = ").append(leftKeys[leftKeyIndex])
                .append(", right[").append(rightKeyIndex).append("] = ").append(rightKeys[rightKeyIndex]).append("\n");
        sb.append("leftKeys = ").append(Arrays.stream(leftKeys, 0, leftKeyLength).map(Object::toString).collect(Collectors.joining())).append('\n');
        sb.append("rightKeys = ").append(Arrays.stream(rightKeys, 0, rightKeyLength).map(Object::toString).collect(Collectors.joining())).append('\n');
        throw illegalState(sb.toString());
    }

    static int[] copy(int[] src, int to, int length, IntBuffers bufferManager)
    {
        if (length == 0)
            return NO_INTS;

        int[] result = bufferManager.getInts(length);
        if (result.length < length)
            throw new IllegalStateException();
        System.arraycopy(src, 0, result, 0, to);
        return result;
    }

    public static <K, K2, V, T> T remove(T from, K[] keys, V[] oldValues, int[] oldKeysToValues, Predicate<V> remove, T none, IntFunction<V[]> newValueArray,
                                     K2 passthroughKeys, SimpleConstructor<K2, V[], T> constructor)
    {
        if (isEmpty(keys, oldKeysToValues))
            return from;

        IntBuffers cache = ArrayBuffers.cachedInts();
        int[] remapValue = cache.getInts(oldValues.length);
        int[] newKeyToValue = null;
        V[] newValues;
        int o;
        try
        {
            int count = 0;
            for (int i = 0 ; i < oldValues.length ; ++i)
            {
                if (remove.test(oldValues[i])) remapValue[i] = -1;
                else remapValue[i] = count++;
            }

            if (count == oldValues.length)
                return from;

            if (count == 0)
                return none;

            newValues = newValueArray.apply(count);
            for (int i = 0 ; i < oldValues.length ; ++i)
            {
                if (remapValue[i] >= 0)
                    newValues[remapValue[i]] = oldValues[i];
            }

            newKeyToValue = cache.getInts(oldKeysToValues.length);
            int k = 0, i = keys.length;
            o = i;
            while (i < oldKeysToValues.length)
            {
                while (oldKeysToValues[k] == i)
                    newKeyToValue[k++] = o;

                int remapped = remapValue[oldKeysToValues[i]];
                if (remapped >= 0)
                    newKeyToValue[o++] = remapped;
                ++i;
            }

            while (k < keys.length)
                newKeyToValue[k++] = o;
        }
        catch (Throwable t)
        {
            cache.forceDiscard(newKeyToValue);
            throw t;
        }
        finally
        {
            cache.forceDiscard(remapValue);
        }

        newKeyToValue = cache.completeAndDiscard(newKeyToValue, o);
        return constructor.construct(passthroughKeys, newValues, newKeyToValue);
    }

    @Inline
    public static int[] invert(int[] src, int srcLength, int srcKeyCount, int trgKeyCount)
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

    public static <K, V> Iterator<Map.Entry<K, V>> newIterator(K[] keys, V[] values, int[] keysToValues)
    {
        return new Iterator<Map.Entry<K, V>>()
        {
            int i = keys.length, k = 0;

            @Override
            public boolean hasNext()
            {
                return i < keysToValues.length;
            }

            @Override
            public Map.Entry<K, V> next()
            {
                Entry<K, V> result = new Entry<>(keys[k], values[keysToValues[i++]]);
                if (i == keysToValues[k])
                    ++k;
                return result;
            }
        };
    }

    public static <K, V> String toSimpleString(K[] keys, V[] values, int[] keysToValues)
    {
        if (isEmpty(keys, keysToValues))
            return "{}";

        StringBuilder builder = new StringBuilder("{");
        for (int k = 0, t = keys.length; k < keys.length ; ++k)
        {
            if (builder.length() > 1)
                builder.append(", ");

            builder.append(keys[k]);
            builder.append(":[");
            boolean first = true;
            while (t < keysToValues[k])
            {
                if (first) first = false;
                else builder.append(", ");
                builder.append(values[keysToValues[t++]]);
            }
            builder.append("]");
        }
        builder.append("}");
        return builder.toString();
    }

    public static <K, V> String toBriefString(K[] keys, V[] values)
    {
        if (keys.length == 0 && values.length == 0)
            return "{}";

        StringBuilder builder = new StringBuilder("{");
        for (int k = 0 ; k < keys.length ; ++k)
        {
            if (k > 0)
                builder.append(",");
            builder.append(keys[k]);
        }
        builder.append(":[");
        for (int v = 0 ; v < values.length ; ++v)
        {
            if (v > 0)
                builder.append(",");
            builder.append(values[v]);
        }
        builder.append("]");
        return builder.toString();
    }

    public static boolean isEmpty(Object[] keys, int[] keysToValues)
    {
        return keys.length == keysToValues.length;
    }

    public static int startOffset(Object[] keys, int[] keysToValues, int keyIndex)
    {
        return keyIndex == 0 ? keys.length : keysToValues[keyIndex - 1];
    }

    public static int endOffset(int[] keysToValues, int keyIndex)
    {
        return keysToValues[keyIndex];
    }

    @Inline
    public static <K, V> boolean testEquality(K[] thisKeys, V[] thisValues, int[] thisKeysToValues,
                                              K[] thatKeys, V[] thatValues, int[] thatKeysToValues)
    {
        return thisValues.length == thatValues.length
               && thisKeys.length == thatKeys.length
               && Arrays.equals(thisKeysToValues, thatKeysToValues)
               && Arrays.equals(thisValues, thatValues)
               && Arrays.equals(thisKeys, thatKeys);
    }

    public static class Entry<K, V> implements Map.Entry<K, V>
    {
        final K key;
        final V value;

        public Entry(K key, V value)
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey()
        {
            return key;
        }

        @Override
        public V getValue()
        {
            return value;
        }

        @Override
        public V setValue(V value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return key + "->" + value;
        }
    }

    @Inline
    public static <K, V> void checkValid(K[] keys, V[] values, int[] keysToValues)
    {
        if (!DEBUG_CHECKS)
            return;

        int k = 0;
        for (int i = keys.length; i < keysToValues.length ; ++i)
        {
            boolean first = true;
            while (i < keysToValues[k])
            {
                if (first) first = false;
                else if (keysToValues[i - 1] == keysToValues[i])
                {
                    K key = keys[i];
                    V value = values[keysToValues[i]];
                    throw illegalState(format("Duplicate value (%s) found for key %s", value, key));
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
