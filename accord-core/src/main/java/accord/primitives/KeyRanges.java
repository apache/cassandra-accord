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
import accord.utils.SortedArrays;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.*;
import java.util.function.Predicate;

import static accord.utils.SortedArrays.Search.FAST;
import static accord.utils.SortedArrays.swapHighLow32b;
import static accord.utils.Utils.toArray;

public class KeyRanges implements Iterable<KeyRange>
{
    public static final KeyRanges EMPTY = ofSortedAndDeoverlappedUnchecked(new KeyRange[0]);

    final KeyRange[] ranges;

    private KeyRanges(KeyRange[] ranges)
    {
        Preconditions.checkNotNull(ranges);
        this.ranges = ranges;
    }

    public KeyRanges(List<KeyRange> ranges)
    {
        this(toArray(ranges, KeyRange[]::new));
    }

    @Override
    public String toString()
    {
        return Arrays.toString(ranges);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyRanges ranges1 = (KeyRanges) o;
        return Arrays.equals(ranges, ranges1.ranges);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(ranges);
    }

    @Override
    public Iterator<KeyRange> iterator()
    {
        return Iterators.forArray(ranges);
    }

    // TODO: reconsider users of this method, in light of newer facilities like foldl, findNext etc
    public int rangeIndexForKey(int lowerBound, int upperBound, RoutingKey key)
    {
        return SortedArrays.binarySearch(ranges, lowerBound, upperBound, key, (k, r) -> r.compareKey(k), FAST);
    }

    public int rangeIndexForKey(RoutingKey key)
    {
        return rangeIndexForKey(0, ranges.length, key);
    }

    public boolean contains(RoutingKey key)
    {
        return rangeIndexForKey(key) >= 0;
    }

    public boolean containsAll(AbstractKeys<?, ?> keys)
    {
        return keys.rangeFoldl(this, (from, to, p, v) -> v + (to - from), 0, 0, 0) == keys.size();
    }

    public int size()
    {
        return ranges.length;
    }

    public KeyRange get(int i)
    {
        return ranges[i];
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public KeyRanges select(int[] indexes)
    {
        KeyRange[] selection = new KeyRange[indexes.length];
        for (int i=0; i<indexes.length; i++)
            selection[i] = ranges[indexes[i]];
        return ofSortedAndDeoverlapped(selection);
    }

    public boolean intersects(AbstractKeys<?, ?> keys)
    {
        return findNextIntersection(0, keys, 0) >= 0;
    }

    public <K extends RoutingKey> boolean intersects(AbstractKeys<K, ?> keys, Predicate<? super K> matches)
    {
        int ri = 0, ki = 0;
        while (true)
        {
            long rki = findNextIntersection(ri, keys, ki);
            if (rki < 0)
                return false;

            ri = (int) (rki >>> 32);
            ki = (int) (rki);

            if (matches.test(keys.get(ki)))
                return true;

            ki++;
        }
    }

    public boolean intersects(KeyRanges that)
    {
        return SortedArrays.findNextIntersection(this.ranges, 0, that.ranges, 0, KeyRange::compareIntersecting) >= 0;
    }

    public int findFirstKey(AbstractKeys<?, ?> keys)
    {
        return findNextKey(0, keys, 0);
    }

    public int findNextKey(int ri, AbstractKeys<?, ?> keys, int ki)
    {
        return (int) findNextIntersection(ri, keys, ki);
    }

    // returns ri in top 32 bits, ki in bottom, or -1 if no match found
    // TODO (now): inconsistent bits order vs SortedArrays
    public long findNextIntersection(int ri, AbstractKeys<?, ?> keys, int ki)
    {
        return swapHighLow32b(SortedArrays.findNextIntersectionWithMultipleMatches(keys.keys, ki, ranges, ri));
    }

    public int findFirstKey(RoutingKey[] keys)
    {
        return findNextKey(0, keys, 0);
    }

    public int findNextKey(int ri, RoutingKey[] keys, int ki)
    {
        return (int) (findNextIntersection(ri, keys, ki));
    }

    // returns ri in top 32 bits, ki in bottom, or -1 if no match found
    public long findNextIntersection(int ri, RoutingKey[] keys, int ki)
    {
        return SortedArrays.findNextIntersectionWithMultipleMatches(keys, ki, ranges, ri);
    }

    /**
     * Subtracts the given set of key ranges from this
     * @param that
     * @return
     */
    public KeyRanges difference(KeyRanges that)
    {
        if (that == this)
            return KeyRanges.EMPTY;

        List<KeyRange> result = new ArrayList<>(this.size() + that.size());
        int thatIdx = 0;

        for (int thisIdx=0; thisIdx<this.size(); thisIdx++)
        {
            KeyRange thisRange = this.ranges[thisIdx];
            while (thatIdx < that.size())
            {
                KeyRange thatRange = that.ranges[thatIdx];

                int cmp = thisRange.compareIntersecting(thatRange);
                if (cmp > 0)
                {
                    thatIdx++;
                    continue;
                }
                if (cmp < 0) break;

                int scmp = thisRange.start().compareTo(thatRange.start());
                int ecmp = thisRange.end().compareTo(thatRange.end());

                if (scmp < 0)
                    result.add(thisRange.subRange(thisRange.start(), thatRange.start()));

                if (ecmp <= 0)
                {
                    thisRange = null;
                    break;
                }
                else
                {
                    thisRange = thisRange.subRange(thatRange.end(), thisRange.end());
                    thatIdx++;
                }
            }
            if (thisRange != null)
                result.add(thisRange);
        }
        return new KeyRanges(toArray(result, KeyRange[]::new));
    }

    /**
     * attempts a linear merge where {@code as} is expected to be a superset of {@code bs},
     * terminating at the first indexes where this ceases to be true
     * @return index of {@code as} in upper 32bits, {@code bs} in lower 32bits
     *
     * TODO: better support for merging runs of overlapping or adjacent ranges
     */
    private static long supersetLinearMerge(KeyRange[] as, KeyRange[] bs)
    {
        int ai = 0, bi = 0;
        out: while (ai < as.length && bi < bs.length)
        {
            KeyRange a = as[ai];
            KeyRange b = bs[bi];

            int c = a.compareIntersecting(b);
            if (c < 0)
            {
                ai++;
            }
            else if (c > 0)
            {
                break;
            }
            else if (b.start().compareTo(a.start()) < 0)
            {
                break;
            }
            else if ((c = b.end().compareTo(a.end())) <= 0)
            {
                bi++;
                if (c == 0) ai++;
            }
            else
            {
                // use a temporary counter, so that if we don't find a run of ranges that enforce the superset
                // condition we exit at the start of the mismatch run (and permit it to be merged)
                // TODO: use exponentialSearch
                int tmpai = ai;
                do
                {
                    if (++tmpai == as.length || !a.end().equals(as[tmpai].start()))
                        break out;
                    a = as[tmpai];
                }
                while (a.end().compareTo(b.end()) < 0);
                bi++;
                ai = tmpai;
            }
        }

        return ((long)ai << 32) | bi;
    }

    /**
     * @return true iff {@code that} is a subset of {@code this}
     */
    public boolean contains(KeyRanges that)
    {
        if (this.isEmpty()) return that.isEmpty();
        if (that.isEmpty()) return true;

        return ((int) supersetLinearMerge(this.ranges, that.ranges)) == that.size();
    }

    /**
     * @return the union of {@code this} and {@code that}, returning one of the two inputs if possible
     */
    public KeyRanges union(KeyRanges that)
    {
        if (this == that) return this;
        if (this.isEmpty()) return that;
        if (that.isEmpty()) return this;

        KeyRange[] as = this.ranges, bs = that.ranges;
        {
            // make sure as/ai represent the ranges that might fully contain the other
            int c = as[0].start().compareTo(bs[0].start());
            if (c > 0 || c == 0 && as[as.length - 1].end().compareTo(bs[bs.length - 1].end()) < 0)
            {
                KeyRange[] tmp = as; as = bs; bs = tmp;
            }
        }

        int ai, bi; {
            long tmp = supersetLinearMerge(as, bs);
            ai = (int)(tmp >>> 32);
            bi = (int)tmp;
        }

        if (bi == bs.length)
            return as == this.ranges ? this : that;

        KeyRange[] result = new KeyRange[as.length + (bs.length - bi)];
        int resultCount = copyAndMergeTouching(as, 0, result, 0, ai);

        while (ai < as.length && bi < bs.length)
        {
            KeyRange a = as[ai];
            KeyRange b = bs[bi];

            int c = a.compareIntersecting(b);
            if (c < 0)
            {
                result[resultCount++] = a;
                ai++;
            }
            else if (c > 0)
            {
                result[resultCount++] = b;
                bi++;
            }
            else
            {
                RoutingKey start = a.start().compareTo(b.start()) <= 0 ? a.start() : b.start();
                RoutingKey end = a.end().compareTo(b.end()) >= 0 ? a.end() : b.end();
                ai++;
                bi++;
                while (ai < as.length || bi < bs.length)
                {
                    KeyRange min;
                    if (ai == as.length) min = bs[bi];
                    else if (bi == bs.length) min = a = as[ai];
                    else min = as[ai].start().compareTo(bs[bi].start()) < 0 ? a = as[ai] : bs[bi];
                    if (min.start().compareTo(end) > 0)
                        break;
                    if (min.end().compareTo(end) > 0)
                        end = min.end();
                    if (a == min) ai++;
                    else bi++;
                }
                result[resultCount++] = a.subRange(start, end);
            }
        }

        while (ai < as.length)
            result[resultCount++] = as[ai++];

        while (bi < bs.length)
            result[resultCount++] = bs[bi++];

        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);

        return new KeyRanges(result);
    }

    public KeyRanges mergeTouching()
    {
        if (ranges.length == 0)
            return this;

        KeyRange[] result = new KeyRange[ranges.length];
        int count = copyAndMergeTouching(ranges, 0, result, 0, ranges.length);
        if (count == result.length)
            return this;
        result = Arrays.copyOf(result, count);
        return new KeyRanges(result);
    }

    private static int copyAndMergeTouching(KeyRange[] src, int srcPosition, KeyRange[] trg, int trgPosition, int srcCount)
    {
        if (srcCount == 0)
            return 0;

        int count = 0;
        KeyRange prev = src[srcPosition];
        RoutingKey end = prev.end();
        for (int i = 1 ; i < srcCount ; ++i)
        {
            KeyRange next = src[srcPosition + i];
            if (!end.equals(next.start()))
            {
                trg[trgPosition + count++] = maybeUpdateEnd(prev, end);
                prev = next;
            }
            end = next.end();
        }
        trg[trgPosition + count++] = maybeUpdateEnd(prev, end);
        return count;
    }

    private static KeyRange maybeUpdateEnd(KeyRange range, RoutingKey withEnd)
    {
        return withEnd == range.end() ? range : range.subRange(range.start(), withEnd);
    }

    public static KeyRanges of(KeyRange ... ranges)
    {
        if (ranges.length == 0)
            return EMPTY;

        return sortAndDeoverlap(ranges, ranges.length);
    }

    private static KeyRanges sortAndDeoverlap(KeyRange[] ranges, int count)
    {
        if (count == 0)
            return EMPTY;

        if (count == 1)
        {
            if (ranges.length == 1)
                return new KeyRanges(ranges);

            return new KeyRanges(Arrays.copyOf(ranges, count));
        }

        Arrays.sort(ranges, 0, count, Comparator.comparing(KeyRange::start));
        KeyRange prev = ranges[0];
        int removed = 0;
        for (int i = 1 ; i < count ; ++i)
        {
            KeyRange next = ranges[i];
            if (prev.end().compareTo(next.start()) > 0)
            {
                prev = prev.subRange(prev.start(), next.start());
                if (prev.end().compareTo(next.end()) >= 0)
                {
                    removed++;
                }
                else if (removed > 0)
                {
                    ranges[i - removed] = prev = next.subRange(prev.end(), next.end());
                }
            }
            else if (removed > 0)
            {
                ranges[i - removed] = prev = next;
            }
        }

        count -= removed;
        if (count != ranges.length)
            ranges = Arrays.copyOf(ranges, count);

        return new KeyRanges(ranges);
    }

    public static KeyRanges ofSortedAndDeoverlapped(KeyRange ... ranges)
    {
        for (int i = 1 ; i < ranges.length ; ++i)
        {
            if (ranges[i - 1].end().compareTo(ranges[i].start()) > 0)
                throw new IllegalArgumentException(Arrays.toString(ranges) + " is not correctly sorted or deoverlapped");
        }

        return new KeyRanges(ranges);
    }

    static KeyRanges ofSortedAndDeoverlappedUnchecked(KeyRange ... ranges)
    {
        return new KeyRanges(ranges);
    }

    public static KeyRanges single(KeyRange range)
    {
        return new KeyRanges(new KeyRange[]{range});
    }
}
