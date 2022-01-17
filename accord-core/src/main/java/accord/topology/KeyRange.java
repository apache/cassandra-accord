package accord.topology;

import accord.api.Key;
import accord.txn.Keys;
import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * A range of keys
 * @param <K>
 */
public abstract class KeyRange<K extends Key<K>>
{
    public static abstract class EndInclusive<K extends Key<K>> extends KeyRange<K>
    {
        public EndInclusive(K start, K end)
        {
            super(start, end);
        }

        @Override
        public int compareKey(K key)
        {
            if (key.compareTo(start()) <= 0)
                return -1;
            if (key.compareTo(end()) > 0)
                return 1;
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
        public KeyRange<K> tryMerge(KeyRange<K> that)
        {
            return KeyRange.tryMergeExclusiveInclusive(this, that);
        }
    }

    public static abstract class StartInclusive<K extends Key<K>> extends KeyRange<K>
    {
        public StartInclusive(K start, K end)
        {
            super(start, end);
        }

        @Override
        public int compareKey(K key)
        {
            if (key.compareTo(start()) < 0)
                return -1;
            if (key.compareTo(end()) >= 0)
                return 1;
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
        public KeyRange<K> tryMerge(KeyRange<K> that)
        {
            return KeyRange.tryMergeExclusiveInclusive(this, that);
        }
    }

    private static <K extends Key<K>> KeyRange<K> tryMergeExclusiveInclusive(KeyRange<K> left, KeyRange<K> right)
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

    private final K start;
    private final K end;

    private KeyRange(K start, K end)
    {
        Preconditions.checkArgument(start.compareTo(end) < 0);
        this.start = start;
        this.end = end;
    }

    public final K start()
    {
        return start;
    }

    public final K end()
    {
        return end;
    }

    public abstract boolean startInclusive();

    public abstract boolean endInclusive();

    /**
     * Return a new range covering this and the given range if the ranges are intersecting or touching. That is,
     * no keys can exist between the touching ends of the range.
     */
    public abstract KeyRange<K> tryMerge(KeyRange<K> that);

    public abstract KeyRange<K> subRange(K start, K end);

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyRange<?> that = (KeyRange<?>) o;
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
    public abstract int compareKey(K key);

    public boolean containsKey(K key)
    {
        return compareKey(key) == 0;
    }


    /**
     * Returns a negative integer, zero, or a positive integer if both points of the provided range are less than, the
     * range intersects this range, or both points are greater than this range
     */
    public int compareIntersecting(KeyRange<K> that)
    {
        if (this.start.compareTo(that.end) >= 0)
            return 1;
        if (this.end.compareTo(that.start) <= 0)
            return -1;
        return 0;
    }

    public boolean intersects(KeyRange<K> that)
    {
        return compareIntersecting(that) == 0;
    }

    public boolean fullyContains(KeyRange<K> that)
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
    public KeyRange<K> intersection(KeyRange<K> that)
    {
        if (this.compareIntersecting(that) != 0)
            return null;

        K start = this.start.compareTo(that.start) > 0 ? this.start : that.start;
        K end = this.end.compareTo(that.end) < 0 ? this.end : that.end;
        return subRange(start, end);
    }

    /**
     * returns the index of the first key larger than what's covered by this range
     */
    public int higherKeyIndex(Keys keys, int lowerBound, int upperBound)
    {
        int i = keys.search(lowerBound, upperBound, this,
                            (k, r) -> ((KeyRange) r).compareKey((Key) k) <= 0 ? -1 : 1);
        if (i < 0) i = -1 - i;
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
    public int lowKeyIndex(Keys keys, int lowerBound, int upperBound)
    {
        if (keys.isEmpty()) return -1;

        int i = keys.search(lowerBound, upperBound, this,
                            (k, r) -> ((KeyRange) r).compareKey((Key) k) < 0 ? -1 : 1);

        int minIdx = -1 - i;

        return (minIdx < keys.size() && containsKey((K) keys.get(minIdx))) ? minIdx : i;
    }

    public int lowKeyIndex(Keys keys)
    {
        return lowKeyIndex(keys, 0, keys.size());
    }
}
