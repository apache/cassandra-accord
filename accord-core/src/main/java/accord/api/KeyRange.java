package accord.api;

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
     * returns the index of the lowest key contained in this range
     * @param keys
     */
    public int lowKeyIndex(Keys keys)
    {
        if (keys.isEmpty()) return -1;

        int i = keys.search(0, keys.size(), this,
                            (k, r) -> ((KeyRange) r).compareKey((Key) k) < 0 ? -1 : 1);

        if (i < 0) i = -1 - i;

        if (i == 0 && !containsKey((K) keys.get(0))) i = -1;

        return i;
    }
}
