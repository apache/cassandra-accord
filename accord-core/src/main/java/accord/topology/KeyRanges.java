package accord.topology;

import accord.api.Key;
import accord.txn.Keys;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.*;

public class KeyRanges implements Iterable<KeyRange>
{
    public static final KeyRanges EMPTY = new KeyRanges(new KeyRange[0]);

    // TODO: fix raw parameterized use
    private final KeyRange[] ranges;

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

    public KeyRanges intersection(Keys keys)
    {
        List<KeyRange<?>> result = null;

        int keyLB = 0;
        int keyHB = keys.size();
        int rangeLB = 0;
        int rangeHB = rangeIndexForKey(keys.get(keyHB-1));
        rangeHB = rangeHB < 0 ? -1 - rangeHB : rangeHB + 1;

        for (;rangeLB<rangeHB && keyLB<keyHB;)
        {
            Key key = keys.get(keyLB);
            rangeLB = rangeIndexForKey(rangeLB, size(), key);

            if (rangeLB < 0)
            {
                rangeLB = -1 -rangeLB;
                if (rangeLB >= rangeHB)
                    break;
                keyLB = ranges[rangeLB].lowKeyIndex(keys, keyLB, keyHB);
            }
            else
            {
                if (result == null)
                    result = new ArrayList<>(Math.min(rangeHB - rangeLB, keyHB - keyLB));
                KeyRange<?> range = ranges[rangeLB];
                result.add(range);
                keyLB = range.higherKeyIndex(keys, keyLB, keyHB);
                rangeLB++;
            }

            if (keyLB < 0)
                keyLB = -1 - keyLB;
        }

        return result != null ? new KeyRanges(result.toArray(KeyRange[]::new)) : EMPTY;
    }

    public boolean intersects(Keys keys)
    {
        for (int i=0; i<ranges.length; i++)
            if (ranges[i].intersects(keys))
                return true;
        return false;
    }

    public boolean intersects(KeyRange range)
    {
        for (int i=0; i<ranges.length; i++)
            if (ranges[i].compareIntersecting(range) == 0)
                return true;
        return false;
    }

    public boolean intersects(KeyRanges ranges)
    {
        // TODO: efficiency
        for (KeyRange thisRange : this.ranges)
        {
            for (KeyRange thatRange : ranges)
            {
                if (thisRange.intersects(thatRange))
                    return true;
            }
        }
        return false;
    }

    public int findFirstIntersecting(Keys keys)
    {
        for (int i=0; i<ranges.length; i++)
        {
            int lowKeyIndex = ranges[i].lowKeyIndex(keys);
            if (lowKeyIndex >= 0)
                return lowKeyIndex;
        }
        return -1;
    }

    /**
     * Subtracts the given set of key ranges from this
     * @param that
     * @return
     */
    public KeyRanges difference(KeyRanges that)
    {
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
     * Adds a set of non-overlapping ranges
     */
    public KeyRanges combine(KeyRanges that)
    {
        KeyRange[] combined = new KeyRange[this.ranges.length + that.ranges.length];
        System.arraycopy(this.ranges, 0, combined, 0, this.ranges.length);
        System.arraycopy(that.ranges, 0, combined, this.ranges.length, that.ranges.length);
        Arrays.sort(combined, Comparator.comparing(KeyRange::start));

        for (int i=1; i<combined.length; i++)
            Preconditions.checkArgument(combined[i].compareIntersecting(combined[i -1]) != 0);

        return new KeyRanges(combined);
    }

    public KeyRanges combine(KeyRange range)
    {
        return combine(new KeyRanges(new KeyRange[]{ range}));
    }

    private static KeyRange tryMerge(KeyRange range1, KeyRange range2)
    {
        if (range1 == null || range2 == null)
            return null;
        return range1.tryMerge(range2);
    }

    // optimised for case where one contains the other
    public KeyRanges union(KeyRanges that)
    {
        // pick the larger one
        KeyRange[] as = this.ranges, bs = that.ranges;
        if (as.length < bs.length) { KeyRange[] tmp = as; as = bs; bs = tmp; }

        int ai = 0, bi = 0;
        while (ai < as.length && bi < bs.length)
        {
            KeyRange a = as[ai];
            KeyRange b = bs[bi];
            int c = a.compareIntersecting(b);
            if (c < 0) ai++;
            else if (c > 0 || !a.fullyContains(b)) break;
            else bi++;
        }

        if (bi == bs.length)
            return as == this.ranges ? this : that;

        KeyRange[] result = new KeyRange[as.length + (bs.length - bi)];
        System.arraycopy(as, 0, result, 0, ai);
        int count = ai;

        while (ai < as.length && bi < bs.length)
        {
            KeyRange a = as[ai];
            KeyRange b = bs[bi];

            int c = a.compareIntersecting(b);
            if (c < 0)
            {
                result[count++] = a;
                ai++;
            }
            else if (c > 0)
            {
                result[count++] = b;
                bi++;
            }
            else
            {
                c = a.start().compareTo(b.start());
                if (c < 0 && a.fullyContains(b))
                {
                    bi++;
                    continue;
                }
                else if (c > 0 && b.fullyContains(a))
                {
                    ai++;
                    continue;
                }
                else
                {
                    KeyRange merged = a.subRange(c < 0 ? a.start() : b.start(), a.end().compareTo(b.end()) > 0 ? a.end() : b.end());
                    ai++;
                    bi++;
                    while (ai < as.length || bi < bs.length)
                    {
                        KeyRange min;
                        if (ai == as.length) min = b = bs[bi];
                        else if (bi == bs.length) min = a = as[ai];
                        else min = as[ai].start().compareTo(bs[bi].start()) < 0 ? as[ai] : bs[bi];
                        if (min.start().compareTo(merged.end()) > 0)
                            break;
                        if (min.end().compareTo(merged.end()) > 0)
                            merged = merged.subRange(merged.start(), min.end());
                        if (a == min) ai++;
                        else bi++;
                    }
                    result[count++] = merged;
                }
            }
        }

        while (ai < as.length)
            result[count++] = as[ai++];

        while (bi < bs.length)
            result[count++] = bs[bi++];

        if (count < result.length)
            result = Arrays.copyOf(result, count);

        return new KeyRanges(result);
    }

    public KeyRanges mergeTouching()
    {
        if (ranges.length == 0)
            return this;
        List<KeyRange> result = new ArrayList<>(ranges.length);
        KeyRange current = ranges[0];
        for (int i=1; i<ranges.length; i++)
        {
            KeyRange merged = current.tryMerge(ranges[i]);
            if (merged != null)
            {
                current = merged;
            }
            else
            {
                result.add(current);
                current = ranges[i];
            }
        }
        result.add(current);
        return new KeyRanges(result.toArray(KeyRange[]::new));
    }

    public static KeyRanges singleton(KeyRange range)
    {
        return new KeyRanges(new KeyRange[]{range});
    }

}
