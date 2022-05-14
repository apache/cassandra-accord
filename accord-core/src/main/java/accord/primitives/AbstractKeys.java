package accord.primitives;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.IndexedFold;
import accord.utils.IndexedFoldIntersectToLong;
import accord.utils.IndexedFoldToLong;
import accord.utils.IndexedPredicate;
import accord.utils.IndexedRangeFoldToLong;
import accord.utils.SortedArrays;
import org.apache.cassandra.utils.concurrent.Inline;

@SuppressWarnings("rawtypes")
// TODO: check that foldl call-sites are inlined and optimised by HotSpot
// TODO (now): randomised testing
public abstract class AbstractKeys<K extends RoutingKey, KS extends AbstractKeys<K, KS>> implements Iterable<K>
{
    final K[] keys;

    protected AbstractKeys(K[] keys)
    {
        this.keys = keys;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractKeys that = (AbstractKeys) o;
        return Arrays.equals(keys, that.keys);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(keys);
    }

    public int indexOf(K key)
    {
        return Arrays.binarySearch(keys, key);
    }

    public boolean contains(K key)
    {
        return indexOf(key) >= 0;
    }

    public K get(int indexOf)
    {
        return keys[indexOf];
    }

    /**
     * return true if this keys collection contains all keys found in the given keys
     */
    public boolean containsAll(AbstractKeys<K, ?> that)
    {
        if (that.isEmpty())
            return true;

        return foldlIntersect(that, (li, ri, k, p, v) -> v + 1, 0, 0, 0) == that.size();
    }


    public boolean isEmpty()
    {
        return keys.length == 0;
    }

    public int size()
    {
        return keys.length;
    }

    public int search(int lowerBound, int upperBound, Object key, Comparator<Object> comparator)
    {
        return Arrays.binarySearch(keys, lowerBound, upperBound, key, comparator);
    }

    public int find(K key, int startIndex)
    {
        return SortedArrays.exponentialSearch(keys, startIndex, keys.length, key);
    }

    // returns thisIdx in top 32 bits, thatIdx in bottom
    public long findNextIntersection(int thisIdx, AbstractKeys<K, ?> that, int thatIdx)
    {
        return SortedArrays.findNextIntersection(this.keys, thisIdx, that.keys, thatIdx);
    }

    public Stream<K> stream()
    {
        return Stream.of(keys);
    }

    @Override
    public Iterator<K> iterator()
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
            public K next()
            {
                return keys[i++];
            }
        };
    }

    @Override
    public String toString()
    {
        return stream().map(Object::toString).collect(Collectors.joining(",", "[", "]"));
    }

    protected K[] slice(KeyRanges ranges, IntFunction<K[]> factory)
    {
        return SortedArrays.sliceWithOverlaps(keys, ranges.ranges, factory, (k, r) -> -r.compareTo(k), KeyRange::compareTo);
    }

    /**
     * Count the number of keys matching the predicate and intersecting with the given ranges.
     * If terminateAfter is greater than 0, the method will return once terminateAfter matches are encountered
     */
    @Inline
    public <V> V foldl(KeyRanges rs, IndexedFold<K, V> fold, V accumulator)
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

    public boolean any(KeyRanges ranges, Predicate<K> predicate)
    {
        return 1 == foldl(ranges, (i1, key, i2, i3) -> predicate.test(key) ? 1 : 0, 0, 0, 1);
    }

    public boolean any(IndexedPredicate<K> predicate)
    {
        return 1 == foldl((i, key, p, v) -> predicate.test(i, key) ? 1 : 0, 0, 0, 1);
    }

    @Inline
    public long foldl(KeyRanges rs, IndexedFoldToLong<K> fold, long param, long initialValue, long terminalValue)
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
    public long foldl(IndexedFoldToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        for (int i = 0; i < keys.length; i++)
        {
            initialValue = fold.apply(i, keys[i], param, initialValue);
            if (terminalValue == initialValue)
                return initialValue;
        }
        return initialValue;
    }

    public boolean intersects(KeyRanges ranges, KS keys)
    {
        return foldlIntersect(ranges, keys, (li, ri, k, p, v) -> 1, 0, 0, 1) == 1;
    }

    /**
     * A fold variation that intersects two key sets for some set of ranges, invoking the fold function only on those
     * items that are members of both sets (with their corresponding indices).
     */
    @Inline
    public long foldlIntersect(KeyRanges rs, AbstractKeys<K, ?> intersect, IndexedFoldIntersectToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        AbstractKeys<K, ?> as = this, bs = intersect;
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
    public long foldlIntersect(AbstractKeys<K, ?> intersect, IndexedFoldIntersectToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        AbstractKeys<K, ?> as = this, bs = intersect;
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
    public long foldlDifference(AbstractKeys<K, ?> subtract, IndexedFoldToLong<K> fold, long param, long initialValue, long terminalValue)
    {
        AbstractKeys<K, ?> as = this, bs = subtract;
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

    public Route toRoute(RoutingKey homeKey)
    {
        if (isEmpty())
            return new Route(homeKey, new RoutingKey[] { homeKey });

        RoutingKey[] result = toRoutingKeysArray(homeKey);
        int pos = Arrays.binarySearch(result, homeKey);
        return new Route(result[pos], result);
    }

    public PartialRoute toPartialRoute(KeyRanges ranges, RoutingKey homeKey)
    {
        if (isEmpty())
            return new PartialRoute(ranges, homeKey, new RoutingKey[] { homeKey });

        RoutingKey[] result = toRoutingKeysArray(homeKey);
        int pos = Arrays.binarySearch(result, homeKey);
        return new PartialRoute(ranges, result[pos], result);
    }

    private RoutingKey[] toRoutingKeysArray(RoutingKey homeKey)
    {
        RoutingKey[] result;
        int resultCount;
        int insertPos = Arrays.binarySearch(keys, homeKey);
        if (insertPos < 0)
            insertPos = -1 - insertPos;

        if (insertPos < keys.length && keys[insertPos].toRoutingKey().equals(homeKey))
        {
            result = new RoutingKey[keys.length];
            resultCount = copyToRoutingKeys(keys, 0, result, 0, keys.length);
        }
        else
        {
            result = new RoutingKey[1 + keys.length];
            resultCount = copyToRoutingKeys(keys, 0, result, 0, insertPos);
            if (resultCount == 0 || !homeKey.equals(result[resultCount - 1]))
                result[resultCount++] = homeKey;
            resultCount += copyToRoutingKeys(keys, insertPos, result, resultCount, keys.length - insertPos);
        }

        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);

        return result;
    }

    public RoutingKeys toRoutingKeys()
    {
        if (isEmpty())
            return RoutingKeys.EMPTY;

        RoutingKey[] result = new RoutingKey[keys.length];
        int resultCount = copyToRoutingKeys(keys, 0, result, 0, keys.length);
        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);
        return new RoutingKeys(result);
    }

    private static <K extends RoutingKey> int copyToRoutingKeys(K[] src, int srcPos, RoutingKey[] trg, int trgPos, int count)
    {
        if (count == 0)
            return 0;

        int srcEnd = srcPos + count;
        int trgStart = trgPos;
        if (trgPos == 0)
            trg[trgPos++] = src[srcPos++].toRoutingKey();

        while (srcPos < srcEnd)
        {
            RoutingKey next = src[srcPos++].toRoutingKey();
            if (!next.equals(trg[trgPos - 1]))
                trg[trgPos++] = next;
        }

        return trgPos - trgStart;
    }

    public int[] remapper(KS target, boolean isTargetKnownSuperset)
    {
        return SortedArrays.remapper(keys, target.keys, isTargetKnownSuperset);
    }
}
