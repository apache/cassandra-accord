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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Function;

import accord.api.Key;
import accord.primitives.Routable.Domain;
import accord.utils.ArrayBuffers.ObjectBuffers;
import accord.utils.Invariants;
import accord.utils.SortedArrays;

import static accord.utils.ArrayBuffers.cachedKeys;
import static accord.utils.SortedArrays.isSortedUnique;

// TODO (low priority, efficiency): this should probably be a BTree
public class Keys extends AbstractKeys<Key> implements Seekables<Key, Keys>
{
    public static class SerializationSupport
    {
        public static Keys create(Key[] keys)
        {
            return new Keys(keys);
        }
    }

    public static final Keys EMPTY = new Keys(new Key[0]);

    private static final Key[] EMPTY_KEYS_ARRAY = new Key[0];

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
    public Keys with(Keys that)
    {
        return wrap(SortedArrays.linearUnion(keys, that.keys, cachedKeys()), that);
    }

    @Override
    public Keys slice(Ranges ranges, Slice slice)
    {
        return wrap(slice(ranges, Key[]::new));
    }

    @Override
    public final boolean intersectsAll(Unseekables<?> keysOrRanges)
    {
        Invariants.checkArgument(keysOrRanges.domain() == Domain.Key);
        AbstractUnseekableKeys that = (AbstractUnseekableKeys) keysOrRanges;
        return SortedArrays.isSubset((rk, k) -> -k.compareAsRoutingKey(rk), that.keys, 0, that.keys.length, this.keys, 0, this.keys.length);
    }

    @Override
    public Routables<?> slice(int from, int to)
    {
        if (from == 0 && to == size())
            return this;
        return Keys.ofSortedUnique(Arrays.copyOfRange(keys, from, to));
    }

    public final Keys intersecting(Unseekables<?> intersecting)
    {
        switch (intersecting.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + intersecting.domain());
            case Key: return intersecting((AbstractUnseekableKeys) intersecting);
            case Range: return wrap(intersecting((AbstractRanges) intersecting, cachedKeys()));
        }
    }

    public final Keys intersecting(Unseekables<?> intersecting, Slice slice)
    {
        return intersecting(intersecting);
    }

    public final Keys intersecting(Keys that)
    {
        return wrap(SortedArrays.linearIntersection(this.keys, that.keys, cachedKeys()), that);
    }

    public final Keys intersecting(AbstractUnseekableKeys that)
    {
        return wrap(SortedArrays.intersectWithMultipleMatches(this.keys, this.keys.length, that.keys, that.keys.length, Key::compareAsRoutingKey, cachedKeys()), this);
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

    public Keys without(Range range)
    {
        return wrap(subtract(range, keys));
    }

    @Override
    public Keys without(Ranges ranges)
    {
        return wrap(subtract(ranges, keys, Key[]::new));
    }

    @Override
    public Keys without(Keys subtract)
    {
        return wrap(SortedArrays.linearSubtract(keys, subtract.keys, Key[]::new));
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
        return ofSortedUnique(sort(keys));
    }

    public static Keys of(Collection<? extends Key> keys)
    {
        if (keys.isEmpty())
            return Keys.EMPTY;
        return of(keys.toArray(EMPTY_KEYS_ARRAY));
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
        ObjectBuffers<Key> cache = cachedKeys();
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
            return ofSortedUnique(result);
        }
        catch (Throwable t)
        {
            cache.forceDiscard(array, count);
            throw t;
        }
    }

    public static Keys of(Set<? extends Key> keys)
    {
        return ofUnique(keys.toArray(new Key[0]));
    }

    public static Keys ofSortedUnique(Key ... keys)
    {
        if (!isSortedUnique(keys))
            throw new IllegalArgumentException(Arrays.toString(keys) + " is not sorted");
        return new Keys(keys);
    }

    private static Keys dedupSorted(Key ... keys)
    {
        return new Keys(SortedArrays.toUnique(keys));
    }

    public static Keys ofSortedUnique(Collection<? extends Key> keys)
    {
        return ofSortedUnique(keys.toArray(new Key[0]));
    }

    static Keys ofSortedUnchecked(Key ... keys)
    {
        return new Keys(keys);
    }

    private Keys wrap(Key[] wrap, AbstractKeys<Key> that)
    {
        return wrap == keys ? this : wrap == that.keys && that instanceof Keys ? (Keys)that : new Keys(wrap);
    }

    private AbstractKeys<Key> weakWrap(Key[] wrap, AbstractKeys<Key> that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new Keys(wrap);
    }

    private Keys wrap(Key[] wrap)
    {
        return wrap == keys ? this : new Keys(wrap);
    }
}
