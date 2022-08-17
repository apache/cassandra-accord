package accord.primitives;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.api.Key;
import accord.utils.*;
import org.apache.cassandra.utils.concurrent.Inline;

import static accord.utils.ArrayBuffers.cachedKeys;

@SuppressWarnings("rawtypes")
// TODO: this should probably be a BTree
// TODO: check that foldl call-sites are inlined and optimised by HotSpot
public class Keys implements Iterable<Key>
{
    public static final Keys EMPTY = new Keys(new Key[0]);

    final Key[] keys;

    public Keys(SortedSet<? extends Key> keys)
    {
        this.keys = keys.toArray(Key[]::new);
    }

    public Keys(Collection<? extends Key> keys)
    {
        this.keys = keys.toArray(Key[]::new);
        Arrays.sort(this.keys);
    }

    /**
     * Creates Keys with the sorted array.  This requires the caller knows that the keys are in fact sorted and should
     * call {@link #of(Key[])} if it isn't known.
     * @param keys sorted
     */
    private Keys(Key[] keys)
    {
        this.keys = keys;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Keys keys1 = (Keys) o;
        return Arrays.equals(keys, keys1.keys);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(keys);
    }

    public int indexOf(Key key)
    {
        return Arrays.binarySearch(keys, key);
    }

    public boolean contains(Key key)
    {
        return indexOf(key) >= 0;
    }

    public Key get(int indexOf)
    {
        return keys[indexOf];
    }

    public boolean isEmpty()
    {
        return keys.length == 0;
    }

    public int size()
    {
        return keys.length;
    }

    /**
     * return true if this keys collection contains all keys found in the given keys
     */
    public boolean containsAll(Keys that)
    {
        if (that.isEmpty())
            return true;

        return foldlIntersect(that, (li, ri, k, p, v) -> v + 1, 0, 0, 0) == that.size();
    }

    public Keys union(Keys that)
    {
        return wrap(SortedArrays.linearUnion(keys, that.keys, cachedKeys()), that);
    }

    public Keys intersect(Keys that)
    {
        return wrap(SortedArrays.linearIntersection(keys, that.keys, cachedKeys()), that);
    }

    public Keys slice(KeyRanges ranges)
    {
        return wrap(SortedArrays.sliceWithMultipleMatches(keys, ranges.ranges, Key[]::new, (k, r) -> -r.compareTo(k), KeyRange::compareTo));
    }

    public int findNext(Key key, int startIndex)
    {
        return SortedArrays.exponentialSearch(keys, startIndex, keys.length, key);
    }

    public Keys with(Key key)
    {
        int insertPos = Arrays.binarySearch(keys, key);
        if (insertPos >= 0)
            return this;
        insertPos = -1 - insertPos;

        Key[] src = keys;
        Key[] trg = new Key[src.length + 1];
        System.arraycopy(src, 0, trg, 0, insertPos);
        trg[insertPos] = key;
        System.arraycopy(src, insertPos, trg, insertPos + 1, src.length - insertPos);
        return new Keys(trg);
    }

    public Stream<Key> stream()
    {
        return Stream.of(keys);
    }

    @Override
    public Iterator<Key> iterator()
    {
        return new Iterator<>()
        {
            int i = 0;
            @Override
            public boolean hasNext()
            {
                return i < keys.length;
            }

            @Override
            public Key next()
            {
                return keys[i++];
            }
        };
    }

    public static Keys of(Key ... keys)
    {
        Arrays.sort(keys);
        return new Keys(keys);
    }

    public static Keys ofSorted(Key ... keys)
    {
        for (int i = 1 ; i < keys.length ; ++i)
        {
            if (keys[i - 1].compareTo(keys[i]) >= 0)
                throw new IllegalArgumentException(Arrays.toString(keys) + " is not sorted");
        }
        return new Keys(keys);
    }

    static Keys ofSortedUnchecked(Key ... keys)
    {
        return new Keys(keys);
    }

    public boolean any(KeyRanges ranges, Predicate<Key> predicate)
    {
        return 1 == foldl(ranges, (i1, key, i2, i3) -> predicate.test(key) ? 1 : 0, 0, 0, 1);
    }

    public boolean any(IndexedPredicate<Key> predicate)
    {
        return 1 == foldl((i, key, p, v) -> predicate.test(i, key) ? 1 : 0, 0, 0, 1);
    }

    /**
     * Count the number of keys matching the predicate and intersecting with the given ranges.
     * If terminateAfter is greater than 0, the method will return once terminateAfter matches are encountered
     */
    @Inline
    public <V> V foldl(KeyRanges rs, IndexedFold<Key, V> fold, V accumulator)
    {
        int ai = 0, ri = 0;
        while (true)
        {
            long ari = rs.findNextIntersection(ri, this, ai);
            if (ari < 0)
                break;

            ai = (int)(ari >>> 32);
            ri = (int)ari;
            KeyRange range = rs.get(ri);
            int nextai = range.nextHigherKeyIndex(this, ai + 1);
            while (ai < nextai)
            {
                accumulator = fold.apply(ai, get(ai), accumulator);
                ++ai;
            }
        }

        return accumulator;
    }

    @Inline
    public long foldl(KeyRanges rs, IndexedFoldToLong<Key> fold, long param, long initialValue, long terminalValue)
    {
        int ai = 0, ri = 0;
        done: while (true)
        {
            long ari = rs.findNextIntersection(ri, this, ai);
            if (ari < 0)
                break;

            ai = (int)(ari >>> 32);
            ri = (int)ari;
            KeyRange range = rs.get(ri);
            int nextai = range.nextHigherKeyIndex(this, ai + 1);
            while (ai < nextai)
            {
                initialValue = fold.apply(ai, get(ai), param, initialValue);
                if (initialValue == terminalValue)
                    break done;
                ++ai;
            }
        }

        return initialValue;
    }

    /**
     * A fold variation permitting more efficient operation over indices only, by providing ranges of matching indices
     */
    @Inline
    public long rangeFoldl(KeyRanges rs, IndexedRangeFoldToLong fold, long param, long initialValue, long terminalValue)
    {
        int ai = 0, ri = 0;
        while (true)
        {
            long ari = rs.findNextIntersection(ri, this, ai);
            if (ari < 0)
                break;

            ai = (int)(ari >>> 32);
            ri = (int)ari;
            KeyRange range = rs.get(ri);
            int nextai = range.nextHigherKeyIndex(this, ai + 1);
            initialValue = fold.apply(ai, nextai, param, initialValue);
            if (initialValue == terminalValue)
                break;
            ai = nextai;
        }

        return initialValue;
    }

    @Inline
    public long foldl(IndexedFoldToLong<Key> fold, long param, long initialValue, long terminalValue)
    {
        for (int i = 0; i < keys.length; i++)
        {
            initialValue = fold.apply(i, keys[i], param, initialValue);
            if (terminalValue == initialValue)
                return initialValue;
        }
        return initialValue;
    }

    /**
     * A fold variation that intersects two key sets, invoking the fold function only on those
     * items that are members of both sets (with their corresponding indices).
     */
    @Inline
    public long foldlIntersect(Keys intersect, IndexedFoldIntersectToLong<Key> fold, long param, long initialValue, long terminalValue)
    {
        return SortedArrays.foldlIntersection(this.keys, intersect.keys, fold, param, initialValue, terminalValue);
    }

    /**
     * A fold variation that invokes the fold function only on those items that are members of this set
     * and NOT the provided set.
     */
    @Inline
    public long foldlDifference(Keys subtract, IndexedFoldToLong<Key> fold, long param, long initialValue, long terminalValue)
    {
        return SortedArrays.foldlDifference(keys, subtract.keys, fold, param, initialValue, terminalValue);
    }

    private Keys wrap(Key[] wrap, Keys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new Keys(wrap);
    }

    private Keys wrap(Key[] wrap)
    {
        return wrap == keys ? this : new Keys(wrap);
    }

    public static Keys union(Keys as, Keys bs)
    {
        return as == null ? bs : bs == null ? as : as.union(bs);
    }

    @Override
    public String toString()
    {
        return stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }
}
