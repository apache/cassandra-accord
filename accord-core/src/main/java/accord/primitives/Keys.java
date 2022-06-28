package accord.primitives;

import java.util.*;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.api.Key;
import accord.utils.IndexedFold;
import accord.utils.IndexedFoldIntersectToLong;
import accord.utils.IndexedFoldToLong;
import accord.utils.IndexedPredicate;
import accord.utils.IndexedRangeFoldToLong;
import accord.utils.SortedArrays;
import org.apache.cassandra.utils.concurrent.Inline;

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

    public Keys(Key[] keys)
    {
        this.keys = keys;
        Arrays.sort(keys);
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

    public Keys select(int[] indexes)
    {
        Key[] selection = new Key[indexes.length];
        for (int i = 0 ; i < indexes.length ; ++i)
            selection[i] = keys[indexes[i]];
        return new Keys(selection);
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
        return wrap(SortedArrays.linearUnion(keys, that.keys, Key[]::new), that);
    }

    public Keys intersect(Keys that)
    {
        return wrap(SortedArrays.linearIntersection(keys, that.keys, Key[]::new), that);
    }

    public Keys slice(KeyRanges ranges)
    {
        return wrap(SortedArrays.sliceWithOverlaps(keys, ranges.ranges, Key[]::new, (k, r) -> -r.compareTo(k), KeyRange::compareTo));
    }

    public int search(int lowerBound, int upperBound, Object key, Comparator<Object> comparator)
    {
        return Arrays.binarySearch(keys, lowerBound, upperBound, key, comparator);
    }

    public int findNext(Key key, int startIndex)
    {
        return SortedArrays.exponentialSearch(keys, startIndex, keys.length, key);
    }

    // returns thisIdx in top 32 bits, thatIdx in bottom
    public long findNextIntersection(int thisIdx, Keys that, int thatIdx)
    {
        return SortedArrays.findNextIntersection(this.keys, thisIdx, that.keys, thatIdx);
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

    public static Keys of(Key k0, Key... kn)
    {
        Key[] keys = new Key[kn.length + 1];
        keys[0] = k0;
        for (int i=0; i<kn.length; i++)
            keys[i + 1] = kn[i];

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
            int nextai = range.higherKeyIndex(this, ai + 1, keys.length);
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
            int nextai = range.higherKeyIndex(this, ai + 1, keys.length);
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
            int nextai = range.higherKeyIndex(this, ai + 1, keys.length);
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

    public boolean intersects(KeyRanges ranges, Keys keys)
    {
        return foldlIntersect(ranges, keys, (li, ri, k, p, v) -> 1, 0, 0, 1) == 1;
    }

    /**
     * A fold variation that intersects two key sets for some set of ranges, invoking the fold function only on those
     * items that are members of both sets (with their corresponding indices).
     */
    @Inline
    public long foldlIntersect(KeyRanges rs, Keys intersect, IndexedFoldIntersectToLong<Key> fold, long param, long initialValue, long terminalValue)
    {
        Keys as = this, bs = intersect;
        int ai = 0, bi = 0, ri = 0;
        done: while (true)
        {
            // first, find the next intersecting range with each key, looping until they match the same range
            long ari = rs.findNextIntersection(ri, as, ai);
            if (ari < 0)
                break;

            long bri = rs.findNextIntersection(ri, bs, bi);
            if (bri < 0)
                break;

            ai = (int) (ari >>> 32);
            bi = (int) (bri >>> 32);
            if ((int)ari == (int)bri)
            {
                // once a matching range is found, find the upper limit key index, and loop intersecting on keys until we exceed this point
                ri = (int)ari;
                int endai = rs.get(ri).higherKeyIndex(this, ai + 1, keys.length);
                while (true)
                {
                    long abi = as.findNextIntersection(ai, bs, bi);
                    if (abi < 0)
                        break done;

                    ai = (int)(abi >>> 32);
                    bi = (int)abi;
                    if (ai >= endai)
                        break;

                    initialValue = fold.apply(ai, bi, as.get(ai), param, initialValue);
                    if (initialValue == terminalValue)
                        break done;

                    ++ai;
                    ++bi;
                }
            }
            else
            {
                ri = Math.max((int)ari, (int)bri);
            }
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
        Keys as = this, bs = intersect;
        int ai = 0, bi = 0;
        while (true)
        {
            long abi = as.findNextIntersection(ai, bs, bi);
            if (abi < 0)
                break;

            ai = (int)(abi >>> 32);
            bi = (int)abi;

            initialValue = fold.apply(ai, bi, as.get(ai), param, initialValue);
            if (initialValue == terminalValue)
                break;

            ++ai;
            ++bi;
        }

        return initialValue;
    }

    /**
     * A fold variation that invokes the fold function only on those items that are members of this set
     * and NOT the provided set.
     */
    @Inline
    public long foldlDifference(Keys subtract, IndexedFoldToLong<Key> fold, long param, long initialValue, long terminalValue)
    {
        Keys as = this, bs = subtract;
        int ai = 0, bi = 0;
        while (ai < as.size() && bi < bs.size())
        {
            long abi = as.findNextIntersection(ai, bs, bi);
            int next;
            if (abi < 0)
                break;

            // TODO: perform fast search for next different

            next = (int)(abi >>> 32);
            bi = (int)abi;

            while (ai < next)
            {
                initialValue = fold.apply(ai, as.get(ai), param, initialValue);
                if (initialValue == terminalValue)
                    return initialValue;

                ++ai;
            }

            ++ai; ++bi;
        }

        while (ai < as.size())
        {
            initialValue = fold.apply(ai, as.get(ai), param, initialValue);
            if (initialValue == terminalValue)
                break;
            ai++;
        }

        return initialValue;
    }

    private Keys wrap(Key[] wrap, Keys that)
    {
        return wrap == keys ? this : wrap == that.keys ? that : new Keys(wrap);
    }

    private Keys wrap(Key[] wrap)
    {
        return wrap == keys ? this : new Keys(wrap);
    }

    public int[] remapper(Keys target, boolean isTargetKnownSuperset)
    {
        return SortedArrays.remapper(keys, target.keys, isTargetKnownSuperset);
    }

    public static Keys union(Keys as, Keys bs)
    {
        return as == null ? bs : bs == null ? as : as.union(bs);
    }

    public static Keys intersect(Keys as, Keys bs)
    {
        return as == null ? bs : bs == null ? as : as.intersect(bs);
    }

    @Override
    public String toString()
    {
        return stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }
}
