package accord.topology;

import accord.api.Key;
import accord.api.KeyRange;
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
        for (int i=0; i<ranges.length; i++)
            if (ranges[i].intersects(keys))
                return true;
        return false;
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
    public KeyRanges union(KeyRanges that)
    {
        KeyRange[] combined = new KeyRange[this.ranges.length + that.ranges.length];
        System.arraycopy(this.ranges, 0, combined, 0, this.ranges.length);
        System.arraycopy(that.ranges, 0, combined, this.ranges.length, that.ranges.length);
        Arrays.sort(combined, Comparator.comparing(KeyRange::start));

        for (int i=1; i<combined.length; i++)
            Preconditions.checkArgument(combined[i].compareIntersecting(combined[i -1]) != 0);

        return new KeyRanges(combined);
    }

    public KeyRanges union(KeyRange range)
    {
        return union(new KeyRanges(new KeyRange[]{range}));
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

}
