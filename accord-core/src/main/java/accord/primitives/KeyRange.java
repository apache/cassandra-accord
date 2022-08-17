package accord.primitives;

import accord.api.Key;

import accord.utils.SortedArrays;
import com.google.common.base.Preconditions;

import java.util.Objects;

import static accord.utils.SortedArrays.Search.*;

/**
 * A range of keys
 */
public abstract class KeyRange implements Comparable<Key>
{
    public static class EndInclusive extends KeyRange
    {
        public EndInclusive(Key start, Key end)
        {
            super(start, end);
        }

        @Override
        public int compareTo(Key key)
        {
            if (key.compareTo(start()) <= 0)
                return 1;
            if (key.compareTo(end()) > 0)
                return -1;
            return 0;
        }

        @Override
        public boolean startInclusive()
        {
            return false;
        }

        @Override
        public boolean endInclusive()
        {
            return true;
        }

        @Override
        public KeyRange subRange(Key start, Key end)
        {
            return new EndInclusive(start, end);
        }

        @Override
        public KeyRange tryMerge(KeyRange that)
        {
            return KeyRange.tryMergeExclusiveInclusive(this, that);
        }
    }

    public static class StartInclusive extends KeyRange
    {
        public StartInclusive(Key start, Key end)
        {
            super(start, end);
        }

        @Override
        public int compareTo(Key key)
        {
            if (key.compareTo(start()) < 0)
                return 1;
            if (key.compareTo(end()) >= 0)
                return -1;
            return 0;
        }

        @Override
        public boolean startInclusive()
        {
            return true;
        }

        @Override
        public boolean endInclusive()
        {
            return false;
        }

        @Override
        public KeyRange subRange(Key start, Key end)
        {
            return new StartInclusive(start, end);
        }

        @Override
        public KeyRange tryMerge(KeyRange that)
        {
            return KeyRange.tryMergeExclusiveInclusive(this, that);
        }
    }

    public static KeyRange range(Key start, Key end, boolean startInclusive, boolean endInclusive)
    {
        return new KeyRange(start, end) {

            @Override
            public boolean startInclusive()
            {
                return startInclusive;
            }

            @Override
            public boolean endInclusive()
            {
                return endInclusive;
            }

            @Override
            public KeyRange tryMerge(KeyRange that)
            {
                return KeyRange.tryMergeExclusiveInclusive(this, that);
            }

            @Override
            public KeyRange subRange(Key start, Key end)
            {
                throw new UnsupportedOperationException("subRange");
            }

            @Override
            public int compareTo(Key key)
            {
                if (startInclusive)
                {
                    if (key.compareTo(start()) < 0)
                        return 1;
                }
                else
                {
                    if (key.compareTo(start()) <= 0)
                        return 1;
                }
                if (endInclusive)
                {
                    if (key.compareTo(end()) > 0)
                        return -1;
                }
                else
                {
                    if (key.compareTo(end()) >= 0)
                        return -1;
                }
                return 0;
            }
        };
    }

    private static KeyRange tryMergeExclusiveInclusive(KeyRange left, KeyRange right)
    {
        if (left.getClass() != right.getClass())
            return null;

        Preconditions.checkArgument(left instanceof EndInclusive || left instanceof StartInclusive);

        int cmp = left.compareIntersecting(right);

        if (cmp == 0)
            return left.subRange(left.start.compareTo(right.start) < 0 ? left.start : right.start,
                                 left.end.compareTo(right.end) > 0 ? left.end : right.end);

        if (cmp > 0 && right.end.equals(left.start))
            return left.subRange(right.start, left.end);

        if (cmp < 0 && left.end.equals(right.start))
            return left.subRange(left.start, right.end);

        return null;
    }

    private final Key start;
    private final Key end;

    private KeyRange(Key start, Key end)
    {
        if (start.compareTo(end) >= 0)
            throw new IllegalArgumentException(start + " >= " + end);
        if (startInclusive() == endInclusive())
            throw new IllegalStateException("KeyRange must have one side inclusive, and the other exclusive. KeyRange of different types should not be mixed.");
        this.start = start;
        this.end = end;
    }

    public final Key start()
    {
        return start;
    }

    public final Key end()
    {
        return end;
    }

    public abstract boolean startInclusive();

    public abstract boolean endInclusive();

    /**
     * Return a new range covering this and the given range if the ranges are intersecting or touching. That is,
     * no keys can exist between the touching ends of the range.
     */
    public abstract KeyRange tryMerge(KeyRange that);

    public abstract KeyRange subRange(Key start, Key end);

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyRange that = (KeyRange) o;
        return Objects.equals(start, that.start) && Objects.equals(end, that.end);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(start, end);
    }

    @Override
    public String toString()
    {
        return "Range[" + start + ", " + end + ']';
    }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is less than, contained by,
     * or greater than this range.
     */
    public int compareKey(Key key)
    {
        return -compareTo(key);
    }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is greater than, contained by,
     * or less than this range.
     */
    @Override
    public abstract int compareTo(Key key);

    public boolean containsKey(Key key)
    {
        return compareKey(key) == 0;
    }

    /**
     * Returns a negative integer, zero, or a positive integer if both points of the provided range are less than, the
     * range intersects this range, or both points are greater than this range
     */
    public int compareIntersecting(KeyRange that)
    {
        if (that.getClass() != this.getClass())
            throw new IllegalArgumentException("Cannot mix KeyRange of different types");
        if (this.start.compareTo(that.end) >= 0)
            return 1;
        if (this.end.compareTo(that.start) <= 0)
            return -1;
        return 0;
    }

    public boolean intersects(KeyRange that)
    {
        return compareIntersecting(that) == 0;
    }

    public boolean fullyContains(KeyRange that)
    {
        return that.start.compareTo(this.start) >= 0 && that.end.compareTo(this.end) <= 0;
    }

    public boolean intersects(Keys keys)
    {
        return SortedArrays.binarySearch(keys.keys, 0, keys.size(), this, KeyRange::compareTo, FAST) >= 0;
    }

    /**
     * Returns a range covering the overlapping parts of this and the provided range, returns
     * null if the ranges do not overlap
     */
    public KeyRange intersection(KeyRange that)
    {
        if (this.compareIntersecting(that) != 0)
            return null;

        Key start = this.start.compareTo(that.start) > 0 ? this.start : that.start;
        Key end = this.end.compareTo(that.end) < 0 ? this.end : that.end;
        return subRange(start, end);
    }

    /**
     * returns the index of the first key larger than what's covered by this range
     */
    public int nextHigherKeyIndex(Keys keys, int from)
    {
        int i = SortedArrays.exponentialSearch(keys.keys, from, keys.size(), this, KeyRange::compareTo, FLOOR);
        if (i < 0) i = -1 - i;
        else i = i + 1;
        return i;
    }

    /**
     * returns the index of the lowest key contained in this range. If the keys object contains no intersecting
     * keys, <code>(-(<i>insertion point</i>) - 1)</code> is returned. Where <i>insertion point</i> is where an
     * intersecting key would be inserted into the keys array
     * @param keys
     */
    public int nextCeilKeyIndex(Keys keys, int from)
    {
        return SortedArrays.exponentialSearch(keys.keys, from, keys.size(), this, KeyRange::compareTo, CEIL);
    }
}
