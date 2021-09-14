package accord.topology;

import accord.api.Key;
import accord.api.KeyRange;

import java.util.Arrays;

public class KeyRanges
{
    public static final KeyRanges EMPTY = new KeyRanges(new KeyRange[0]);

    private final KeyRange[] ranges;

    public KeyRanges(KeyRange[] ranges)
    {
        this.ranges = ranges;
    }

    @Override
    public String toString()
    {
        return Arrays.toString(ranges);
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

    public KeyRanges select(int[] indexes)
    {
        KeyRange[] selection = new KeyRange[indexes.length];
        for (int i=0; i<indexes.length; i++)
            selection[i] = ranges[i];
        return new KeyRanges(selection);
    }
}
