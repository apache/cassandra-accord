package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.SortedArrays;
import accord.utils.SortedArrays.Search;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * A range of keys
 */
public abstract class KeyRange implements Comparable<RoutingKey>
{
    public static class EndInclusive extends KeyRange
    {
        public EndInclusive(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        @Override
        public int compareTo(RoutingKey key)
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
        public KeyRange subRange(RoutingKey start, RoutingKey end)
        {
            return new EndInclusive(start, end);
        }
    }

    public static class StartInclusive extends KeyRange
    {
        public StartInclusive(RoutingKey start, RoutingKey end)
        {
            super(start, end);
        }

        @Override
        public int compareTo(RoutingKey key)
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
        public KeyRange subRange(RoutingKey start, RoutingKey end)
        {
            return new StartInclusive(start, end);
        }
    }

    private final RoutingKey start;
    private final RoutingKey end;

    private KeyRange(RoutingKey start, RoutingKey end)
    {
        Preconditions.checkArgument(start.compareTo(end) < 0);
        Preconditions.checkState(startInclusive() != endInclusive()); // TODO: relax this restriction
        this.start = start;
        this.end = end;
    }

    public final RoutingKey start()
    {
        return start;
    }
    public final RoutingKey end()
    {
        return end;
    }

    public abstract boolean startInclusive();
    public abstract boolean endInclusive();

    public abstract KeyRange subRange(RoutingKey start, RoutingKey end);

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
        return start.hashCode() * 31 + end.hashCode();
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
    public int compareKey(RoutingKey key)
    {
        return -compareTo(key);
    }

    /**
     * Returns a negative integer, zero, or a positive integer as the provided key is greater than, contained by,
     * or less than this range.
     */
    public abstract int compareTo(RoutingKey key);

    public boolean containsKey(RoutingKey key)
    {
        return compareKey(key) == 0;
    }

    /**
     * Returns a negative integer, zero, or a positive integer if both points of the provided range are less than, the
     * range intersects this range, or both points are greater than this range
     */
    public int compareIntersecting(KeyRange that)
    {
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
        return lowKeyIndex(keys) >= 0;
    }

    /**
     * Returns a range covering the overlapping parts of this and the provided range, returns
     * null if the ranges do not overlap
     */
    public KeyRange intersection(KeyRange that)
    {
        if (this.compareIntersecting(that) != 0)
            return null;

        RoutingKey start = this.start.compareTo(that.start) > 0 ? this.start : that.start;
        RoutingKey end = this.end.compareTo(that.end) < 0 ? this.end : that.end;
        return subRange(start, end);
    }

    /**
     * returns the index of the first key larger than what's covered by this range
     */
    public int higherKeyIndex(AbstractKeys<?, ?> keys, int from, int to)
    {
        int i = SortedArrays.exponentialSearch(keys.keys, from, to, this, KeyRange::compareTo, Search.FLOOR);
        if (i < 0) i = -1 - i;
        else i += 1;
        return i;
    }

    public int higherKeyIndex(Keys keys)
    {
        return higherKeyIndex(keys, 0, keys.size());
    }

    /**
     * returns the index of the lowest key contained in this range. If the keys object contains no intersecting
     * keys, <code>(-(<i>insertion point</i>) - 1)</code> is returned. Where <i>insertion point</i> is where an
     * intersecting key would be inserted into the keys array
     * @param keys
     */
    public int lowKeyIndex(AbstractKeys<?, ?> keys, int lowerBound, int upperBound)
    {
        if (keys.isEmpty()) return -1;

        int i = keys.search(lowerBound, upperBound, this,
                            (k, r) -> ((KeyRange) r).compareKey((RoutingKey) k) < 0 ? -1 : 1);

        int minIdx = -1 - i;

        return (minIdx < keys.size() && containsKey(keys.get(minIdx))) ? minIdx : i;
    }

    public int lowKeyIndex(Keys keys)
    {
        return lowKeyIndex(keys, 0, keys.size());
    }
}
