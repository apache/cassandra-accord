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
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.ArrayBuffers.ObjectBuffers;
import accord.utils.IndexedBiConsumer;
import accord.utils.IndexedFold;
import accord.utils.IndexedFoldToLong;
import accord.utils.IndexedTriFold;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.primitives.Routable.Domain.Key;

@SuppressWarnings("rawtypes")
// TODO (desired, efficiency): check that foldl call-sites are inlined and optimised by HotSpot
public abstract class AbstractKeys<K extends RoutableKey> implements Iterable<K>, Routables<K>
{
    final K[] keys;

    protected AbstractKeys(K[] keys)
    {
        this.keys = keys;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractKeys that = (AbstractKeys) o;
        return Arrays.equals(keys, that.keys);
    }

    public int indexOf(K key)
    {
        return Arrays.binarySearch(keys, key);
    }

    public int indexOf(RoutingKey key)
    {
        return Arrays.binarySearch(keys, key);
    }

    @Override
    public final K get(int indexOf)
    {
        return keys[indexOf];
    }

    @Override
    public final Routable.Domain domain()
    {
        return Key;
    }

    @Override
    public final boolean isEmpty()
    {
        return keys.length == 0;
    }

    @Override
    public final int size()
    {
        return keys.length;
    }

    @Override
    public final boolean contains(RoutableKey key)
    {
        return Arrays.binarySearch(keys, key) >= 0;
    }

    @Override
    public final boolean containsAll(Routables<?> keysOrRanges)
    {
        return keysOrRanges.size() == Routables.foldl(keysOrRanges, this, (k, p, v, i) -> v + 1, 0, 0, 0);
    }

    @Override
    public final boolean intersects(AbstractKeys<?> keys)
    {
        return findNextIntersection(0, keys, 0) >= 0;
    }

    @Override
    public final boolean intersects(AbstractRanges ranges)
    {
        return findNextIntersection(0, ranges, 0) >= 0;
    }

    @Override
    public final int findNext(int thisIndex, RoutableKey key, SortedArrays.Search search)
    {
        return SortedArrays.exponentialSearch(keys, thisIndex, keys.length, key, RoutableKey::compareTo, search);
    }

    @Override
    public final int findNext(int thisIndex, Range find, SortedArrays.Search search)
    {
        return SortedArrays.exponentialSearch(keys, thisIndex, size(), find, Range::compareTo, search);
    }

    @Override
    public final long findNextIntersection(int thisIdx, AbstractRanges that, int thatIdx)
    {
        return SortedArrays.findNextIntersectionWithMultipleMatches(this.keys, thisIdx, that.ranges, thatIdx, (RoutableKey k, Range r) -> -r.compareTo(k), Range::compareTo);
    }

    @Override
    public final long findNextIntersection(int thisIdx, AbstractKeys<?> that, int thatIdx)
    {
        return SortedArrays.findNextIntersection(this.keys, thisIdx, that.keys, thatIdx, RoutableKey::compareTo);
    }

    @Override
    public final long findNextIntersection(int thisIndex, Routables<K> with, int withIndex)
    {
        return findNextIntersection(thisIndex, (AbstractKeys<?>) with, withIndex);
    }

    public Stream<K> stream()
    {
        return Stream.of(keys);
    }

    @Override
    public Iterator<K> iterator()
    {
        return new Iterator<K>()
        {
            int i = 0;
            @Override
            public boolean hasNext()
            {
                return i < keys.length;
            }

            @Override
            public K next()
            {
                return keys[i++];
            }
        };
    }

    @Override
    public String toString()
    {
        return stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }


    // TODO (expected, efficiency): accept cached buffers
    protected K[] slice(Ranges ranges, IntFunction<K[]> factory)
    {
        return SortedArrays.sliceWithMultipleMatches(keys, ranges.ranges, factory, (k, r) -> -r.compareTo(k), Range::compareTo);
    }

    protected static <K extends RoutableKey> K[] subtract(AbstractRanges ranges, K[] keys, IntFunction<K[]> factory)
    {
        return SortedArrays.subtractWithMultipleMatches(keys, ranges.ranges, factory, (k, r) -> -r.compareTo(k), Range::compareTo);
    }

    protected static <K extends RoutableKey> K[] subtract(Range range, K[] keys)
    {
        boolean isStartInclusive = range.startInclusive();
        int start = Arrays.binarySearch(keys, 0, keys.length, range.start(), RoutableKey::compareTo);
        if (start < 0) start = -1 - start;
        else if (!isStartInclusive) ++start;
        int end = Arrays.binarySearch(keys, 0, keys.length, range.end(), RoutableKey::compareTo);
        if (end < 0) end = -1 - end;
        else if (!isStartInclusive) ++end;
        if (start >= end)
            return Arrays.copyOf(keys, 0);
        return Arrays.copyOfRange(keys, start, end);
    }

    protected K[] intersect(AbstractKeys<K> that, ObjectBuffers<K> buffers)
    {
        return SortedArrays.linearIntersection(this.keys, that.keys, buffers);
    }

    protected K[] intersect(AbstractRanges ranges, ObjectBuffers<K> buffers)
    {
        return SortedArrays.intersectWithMultipleMatches(keys, keys.length, ranges.ranges, ranges.ranges.length, (k, r) -> -r.compareTo(k), buffers);
    }

    public boolean any(Ranges ranges, Predicate<? super K> predicate)
    {
        return 1 == foldl(ranges, (key, p2, prev, index) -> predicate.test(key) ? 1 : 0, 0, 0, 1);
    }

    public boolean any(Predicate<? super K> predicate)
    {
        return 1 == foldl((key, p2, prev, index) -> predicate.test(key) ? 1 : 0, 0, 0, 1);
    }

    public boolean none(Predicate<? super K> predicate)
    {
        return !any(predicate);
    }

    @Inline
    public final <V> V foldl(Ranges rs, IndexedFold<? super K, V> fold, V accumulator)
    {
        return Routables.foldl(this, rs, fold, accumulator);
    }

    @Inline
    public final long foldl(AbstractKeys<K> intersect, IndexedFoldToLong<K> fold, long param, long accumulator, long terminalValue)
    {
        return Routables.foldl(this, intersect, fold, param, accumulator, terminalValue);
    }

    @Inline
    public final <P1, P2, V> V foldl(AbstractKeys<K> intersect, IndexedTriFold<P1, P2, K, V> fold, P1 p1, P2 p2, V accumulator)
    {
        return Routables.foldl(this, intersect, fold, p1, p2, accumulator, i -> false);
    }

    @Inline
    public final <P1, P2, V> V foldl(AbstractRanges intersect, IndexedTriFold<P1, P2, K, V> fold, P1 p1, P2 p2, V accumulator)
    {
        return Routables.foldl(this, intersect, fold, p1, p2, accumulator, i -> false);
    }

    @Inline
    public final void forEach(Ranges rs, Consumer<? super K> forEach)
    {
        Routables.foldl(this, rs, (k, consumer, i) -> { consumer.accept(k); return consumer; }, forEach);
    }

    @Inline
    public final <P1> void forEach(Ranges rs, IndexedBiConsumer<P1, ? super K> forEach, P1 p1)
    {
        Routables.foldl(this, rs, (p, ignore, k, consumer, i) -> {
            consumer.accept(p, k, i);
            return consumer;
        }, p1, null, forEach, i -> false);
    }

    @Inline
    public final long foldl(Ranges rs, IndexedFoldToLong<? super K> fold, long param, long initialValue, long terminalValue)
    {
        return Routables.foldl(this, rs, fold, param, initialValue, terminalValue);
    }

    @Inline
    public final long foldl(IndexedFoldToLong<? super K> fold, long param, long initialValue, long terminalValue)
    {
        for (int i = 0; i < keys.length; i++)
        {
            initialValue = fold.apply(keys[i], param, initialValue, i);
            if (terminalValue == initialValue)
                return initialValue;
        }
        return initialValue;
    }

    public void forEach(Consumer<? super K> forEach)
    {
        for (K key : keys)
            forEach.accept(key);
    }

    public final FullKeyRoute toRoute(RoutingKey homeKey)
    {
        if (isEmpty())
            return new FullKeyRoute(homeKey, false, new RoutingKey[] { homeKey });

        return toRoutingKeysArray(homeKey, (routingKeys, homeKeyIndex, isParticipatingHomeKey) -> new FullKeyRoute(routingKeys[homeKeyIndex], isParticipatingHomeKey, routingKeys));
    }

    protected RoutingKey[] toRoutingKeysArray(RoutingKey withKey)
    {
        return toRoutingKeysArray(withKey, (routingKeys, homeKeyIndex, isParticipatingHomeKey) -> routingKeys);
    }

    interface ToRoutingKeysFactory<T>
    {
        T apply(RoutingKey[] keys, int insertPos, boolean includesKey);
    }

    @SuppressWarnings("SuspiciousSystemArraycopy")
    protected <T> T toRoutingKeysArray(RoutingKey withKey, ToRoutingKeysFactory<T> toRoutingKeysFactory)
    {
        if (keys.getClass() == RoutingKey[].class)
        {
            int insertPos = Arrays.binarySearch(keys, withKey);
            if (insertPos >= 0)
            {
                Invariants.checkState(keys[insertPos].equals(withKey));
                return toRoutingKeysFactory.apply((RoutingKey[])keys, insertPos, true);
            }

            insertPos = -1 - insertPos;
            RoutingKey[] result = new RoutingKey[1 + keys.length];
            System.arraycopy(keys, 0, result, 0, insertPos);
            result[insertPos] = withKey;
            System.arraycopy(keys, insertPos, result, insertPos + 1, keys.length - insertPos);
            return toRoutingKeysFactory.apply(result, insertPos, false);
        }
        else
        {
            RoutingKey[] result = new RoutingKey[keys.length];
            for (int i = 0; i < keys.length; i++)
                result[i] = keys[i].toUnseekable();
            int insertPos = Arrays.binarySearch(result, withKey);
            if (insertPos >= 0)
                return toRoutingKeysFactory.apply(result, insertPos, true);
            else
                insertPos = -1 - insertPos;

            RoutingKey[] newResult = new RoutingKey[1 + keys.length];
            System.arraycopy(result, 0, newResult, 0, insertPos);
            newResult[insertPos] = withKey;
            System.arraycopy(result, insertPos, newResult, insertPos + 1, result.length - insertPos);
            return toRoutingKeysFactory.apply(newResult, insertPos, false);
        }
    }

    public final RoutingKeys toParticipants()
    {
        return toUnseekables(array -> array.length == 0 ? RoutingKeys.EMPTY : new RoutingKeys(array));
    }

    private <R> R toUnseekables(Function<RoutingKey[], R> constructor)
    {
        if (isEmpty())
            constructor.apply(RoutingKeys.EMPTY.keys);

        RoutingKey[] result = new RoutingKey[keys.length];
        int resultCount = copyToRoutingKeys(keys, 0, result, 0, keys.length);
        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);
        return constructor.apply(result);
    }

    private static <K extends RoutableKey> int copyToRoutingKeys(K[] src, int srcPos, RoutingKey[] trg, int trgPos, int count)
    {
        if (count == 0)
            return 0;

        int srcEnd = srcPos + count;
        int trgStart = trgPos;
        if (trgPos == 0)
            trg[trgPos++] = src[srcPos++].toUnseekable();

        while (srcPos < srcEnd)
        {
            RoutingKey next = src[srcPos++].toUnseekable();
            if (!next.equals(trg[trgPos - 1]))
                trg[trgPos++] = next;
        }

        return trgPos - trgStart;
    }

    static <T extends RoutableKey> T[] sort(T[] keys)
    {
        Arrays.sort(keys);
        return keys;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(keys);
    }

}
