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
import java.util.function.Function;

import accord.api.Key;
import accord.utils.*;
import accord.utils.ArrayBuffers.ObjectBuffers;

import static accord.utils.ArrayBuffers.cachedKeys;

// TODO (low priority, efficiency): this should probably be a BTree
public class Keys extends AbstractKeys<Key, Keys> implements Seekables<Key, Keys>
{
    public static class SerializationSupport
    {
        public static Keys create(Key[] keys)
        {
            return new Keys(keys);
        }
    }

    public static final Keys EMPTY = new Keys(new Key[0]);

    public Keys(SortedSet<? extends Key> keys)
    {
        super(keys.toArray(new Key[0]));
    }

    /**
     * Creates Keys with the sorted array.  This requires the caller knows that the keys are in fact sorted and should
     * call {@link #of(Key[])} if it isn't known.
     * @param keys sorted
     */
    private Keys(Key[] keys)
    {
        super(keys);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Keys keys1 = (Keys) o;
        return Arrays.equals(keys, keys1.keys);
    }

    @Override
    public Keys union(Keys that)
    {
        return wrap(SortedArrays.linearUnion(keys, that.keys, cachedKeys()), that);
    }

    @Override
    public Keys slice(Ranges ranges)
    {
        return wrap(slice(ranges, Key[]::new));
    }

    public Keys with(Key key)
    {
        int insertPos = Arrays.binarySearch(keys, key);
        if (insertPos >= 0)
            return this;
        insertPos = -1 - insertPos;

        Key[] src = keys;
        Key[] trg = new Key[src.length + 1];
        System.arraycopy(src, 0, trg, 0, insertPos);
        trg[insertPos] = key;
        System.arraycopy(src, insertPos, trg, insertPos + 1, src.length - insertPos);
        return new Keys(trg);
    }

    public static Keys of(Key key)
    {
        return new Keys(new Key[] { key });
    }

    public static Keys of(Key ... keys)
    {
        return dedupSorted(sort(keys));
    }

    public static Keys copyOf(Key[] keys)
    {
        return dedupSorted(sort(keys.clone()));
    }

    public static Keys ofUnique(Key ... keys)
    {
        // check unique
        return ofSorted(sort(keys));
    }

    public static Keys of(Collection<? extends Key> keys)
    {
        return of(keys.toArray(new Key[0]));
    }

    public static <V> Keys of(Collection<V> input, Function<? super V, ? extends Key> transform)
    {
        Key[] array = new Key[input.size()];
        int i = 0;
        for (V v : input)
            array[i++] = transform.apply(v);
        return of(array);
    }

    public static <V> Keys of(List<V> input, Function<? super V, ? extends Key> transform)
    {
        Key[] array = new Key[input.size()];
        for (int i = 0, mi = input.size() ; i < mi ; ++i)
            array[i] = transform.apply(input.get(i));
        return of(array);
    }

    public static <A, B> Keys ofMergeSorted(List<A> as, Function<? super A, ? extends Key> fa, List<B> bs, Function<? super B, ? extends Key> fb)
    {
        ObjectBuffers<Key> cache = ArrayBuffers.cachedKeys();
        int asSize = as.size(), bsSize = bs.size();
        Key[] array = cache.get(asSize + bsSize);
        int count = 0;
        try
        {
            int ai = 0, bi = 0;
            while (ai < asSize && bi < bsSize)
            {
                Key a = fa.apply(as.get(ai));
                Key b = fb.apply(bs.get(bi));
                int c = a.compareTo(b);
                if (c <= 0)
                {
                    array[count++] = a;
                    ai++;
                    bi = c == 0 ? bi + 1 : bi;
                }
                else
                {
                    array[count++] = b;
                    bi++;
                }
            }
            while (ai < asSize)
                array[count++] = fa.apply(as.get(ai++));
            while (bi < bsSize)
                array[count++] = fb.apply(bs.get(bi++));

            Key[] result = cache.complete(array, count);
            cache.discard(array, count);
            return ofSorted(result);
        }
        catch (Throwable t)
        {
            cache.forceDiscard(array, count);
            throw t;
        }
    }

    public static Keys ofUnique(Collection<? extends Key> keys)
    {
        return ofUnique(keys.toArray(new Key[0]));
    }

    public static Keys of(Set<? extends Key> keys)
    {
        return ofUnique(keys.toArray(new Key[0]));
    }

    public static Keys ofSorted(Key ... keys)
    {
        for (int i = 1 ; i < keys.length ; ++i)
        {
            if (keys[i - 1].compareTo(keys[i]) >= 0)
                throw new IllegalArgumentException(Arrays.toString(keys) + " is not sorted");
        }
        return new Keys(keys);
    }

    private static Keys dedupSorted(Key ... keys)
    {
        int removed = 0;
        for (int i = 1 ; i < keys.length ; ++i)
        {
            int c = keys[i - 1].compareTo(keys[i]);
            if (c >= 0)
            {
                if (c > 0)
                    throw new IllegalArgumentException(Arrays.toString(keys) + " is not sorted");

                removed++;
            }
            else if (removed > 0)
            {
                keys[i - removed] = keys[i];
            }
        }
        if (removed > 0)
            keys = Arrays.copyOf(keys, keys.length - removed);
        return new Keys(keys);
    }

    public static Keys ofSorted(Collection<? extends Key> keys)
    {
        return ofSorted(keys.toArray(new Key[0]));
    }

    static Keys ofSortedUnchecked(Key ... keys)
    {
        return new Keys(keys);
    }

    private Keys wrap(Key[] wrap, AbstractKeys<Key, ?> that)
    {
        return wrap == keys ? this : wrap == that.keys && that instanceof Keys ? (Keys)that : new Keys(wrap);
    }

    private Keys wrap(Key[] wrap)
    {
        return wrap == keys ? this : new Keys(wrap);
    }
}
