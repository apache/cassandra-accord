package accord.primitives;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.SortedArrays;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.*;

public class KeyRanges implements Iterable<KeyRange>
{
    public static final KeyRanges EMPTY = new KeyRanges(new KeyRange[0]);

    // TODO: fix raw parameterized use
    final KeyRange[] ranges;

    public KeyRanges(KeyRange[] ranges)
    {
        Preconditions.checkNotNull(ranges);
        this.ranges = ranges;
    }

    public KeyRanges(List<KeyRange> ranges)
    {
        this(ranges.toArray(KeyRange[]::new));
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

    public int rangeIndexForKey(int lowerBound, int upperBound, Key key)
    {
        return Arrays.binarySearch(ranges, lowerBound, upperBound, key,
                                   (r, k) -> -((KeyRange) r).compareKey((Key) k));
    }

    public int rangeIndexForKey(Key key)
    {
        return rangeIndexForKey(0, ranges.length, key);
    }

    public boolean contains(Key key)
    {
        return rangeIndexForKey(key) >= 0;
    }

    public boolean containsAll(Keys keys)
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
        return new KeyRanges(selection);
    }

    public boolean intersects(Keys keys)
    {
        return findNextIntersection(0, keys, 0) >= 0;
    }

    public boolean intersects(KeyRange range)
    {
        return Arrays.binarySearch(ranges, range, KeyRange::compareIntersecting) >= 0;
    }

    public boolean intersects(KeyRanges that)
    {
        return SortedArrays.findNextIntersection(this.ranges, 0, that.ranges, 0, KeyRange::compareIntersecting) >= 0;
    }

    public int findFirstKey(Keys keys)
    {
        return findNextKey(0, keys, 0);
    }

    public int findNextKey(int ri, Keys keys, int ki)
    {
        return (int) (findNextIntersection(ri, keys, ki) >> 32);
    }

    // returns ki in top 32 bits, ri in bottom, or -1 if no match found
    public long findNextIntersection(int ri, Keys keys, int ki)
    {
        return SortedArrays.findNextIntersectionWithOverlaps(keys.keys, ki, ranges, ri);
    }

    public int findFirstKey(Key[] keys)
    {
        return findNextKey(0, keys, 0);
    }

    public int findNextKey(int ri, Key[] keys, int ki)
    {
        return (int) (findNextIntersection(ri, keys, ki) >> 32);
    }

    // returns ki in top 32 bits, ri in bottom, or -1 if no match found
    public long findNextIntersection(int ri, Key[] keys, int ki)
    {
        return SortedArrays.findNextIntersectionWithOverlaps(keys, ki, ranges, ri);
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
        return new KeyRanges(result.toArray(KeyRange[]::new));
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
                Key start = a.start().compareTo(b.start()) <= 0 ? a.start() : b.start();
                Key end = a.end().compareTo(b.end()) >= 0 ? a.end() : b.end();
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
        Key end = prev.end();
        for (int i = 1 ; i < srcCount ; ++i)
        {
            KeyRange next = src[srcPosition + i];
            if (end.equals(next.start()))
            {
                end = next.end();
            }
            else
            {
                trg[trgPosition + count++] = maybeUpdateEnd(prev, end);
                prev = next;
                end = next.end();
            }
        }
        trg[trgPosition + count++] = maybeUpdateEnd(prev, end);
        return count;
    }

    private static KeyRange maybeUpdateEnd(KeyRange range, Key withEnd)
    {
        return withEnd == range.end() ? range : range.subRange(range.start(), withEnd);
    }

    public static KeyRanges single(KeyRange range)
    {
        return new KeyRanges(new KeyRange[]{range});
    }

}
