package accord.primitives;

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import accord.api.RoutingKey;
import accord.utils.IndexedFold;
import accord.utils.IndexedFoldIntersectToLong;
import accord.utils.IndexedFoldToLong;
import accord.utils.IndexedPredicate;
import accord.utils.IndexedRangeFoldToLong;
import accord.utils.SortedArrays;
import net.nicoulaj.compilecommand.annotations.Inline;

@SuppressWarnings("rawtypes")
// TODO: check that foldl call-sites are inlined and optimised by HotSpot
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

    public int findNext(K key, int startIndex)
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
        return new Iterator<K>()
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
        return SortedArrays.sliceWithMultipleMatches(keys, ranges.ranges, factory, (k, r) -> -r.compareTo(k), KeyRange::compareTo);
    }

    public boolean any(KeyRanges ranges, Predicate<? super K> predicate)
    {
        return 1 == foldl(ranges, (i1, key, i2, i3) -> predicate.test(key) ? 1 : 0, 0, 0, 1);
    }

    public boolean any(Predicate<? super K> predicate)
    {
        return 1 == foldl((i, key, p, v) -> predicate.test(key) ? 1 : 0, 0, 0, 1);
    }

    public boolean none(Predicate<? super K> predicate)
    {
        return !any(predicate);
    }

    /**
     * Count the number of keys matching the predicate and intersecting with the given ranges.
     * If terminateAfter is greater than 0, the method will return once terminateAfter matches are encountered
     */
    @Inline
    public <V> V foldl(KeyRanges rs, IndexedFold<? super K, V> fold, V accumulator)
    {
        int ki = 0, ri = 0;
        while (true)
        {
            long rki = rs.findNextIntersection(ri, this, ki);
            if (rki < 0)
                break;

            ri = (int)(rki >>> 32);
            ki = (int)(rki);
            KeyRange range = rs.get(ri);
            int nextai = range.nextHigherKeyIndex(this, ki + 1);
            while (ki < nextai)
            {
                accumulator = fold.apply(ki, get(ki), accumulator);
                ++ki;
            }
        }

        return accumulator;
    }

    /**
     * Count the number of keys matching the predicate and intersecting with the given ranges.
     * If terminateAfter is greater than 0, the method will return once terminateAfter matches are encountered
     */
    @Inline
    public <V> V foldl(IndexedFold<? super K, V> fold, V accumulator)
    {
        for (int ki = 0; ki < size() ; ++ki)
            accumulator = fold.apply(ki, get(ki), accumulator);
        return accumulator;
    }

    @Inline
    public long foldl(KeyRanges rs, IndexedFoldToLong<? super K> fold, long param, long initialValue, long terminalValue)
    {
        int ki = 0, ri = 0;
        done: while (true)
        {
            long rki = rs.findNextIntersection(ri, this, ki);
            if (rki < 0)
                break;

            ri = (int)(rki >>> 32);
            ki = (int)(rki);
            KeyRange range = rs.get(ri);
            int nextai = range.nextHigherKeyIndex(this, ki + 1);
            while (ki < nextai)
            {
                initialValue = fold.apply(ki, get(ki), param, initialValue);
                if (initialValue == terminalValue)
                    break done;
                ++ki;
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
        int ki = 0, ri = 0;
        while (true)
        {
            long rki = rs.findNextIntersection(ri, this, ki);
            if (rki < 0)
                break;

            ri = (int)(rki >>> 32);
            ki = (int)(rki);
            KeyRange range = rs.get(ri);
            int nextai = range.nextHigherKeyIndex(this, ki + 1);
            initialValue = fold.apply(ki, nextai, param, initialValue);
            if (initialValue == terminalValue)
                break;
            ki = nextai;
        }

        return initialValue;
    }

    @Inline
    public long foldl(IndexedFoldToLong<? super K> fold, long param, long initialValue, long terminalValue)
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
    public long foldlIntersect(AbstractKeys<K, ?> intersect, IndexedFoldIntersectToLong<? super K> fold, long param, long initialValue, long terminalValue)
    {
        return SortedArrays.foldlIntersection(this.keys, intersect.keys, fold, param, initialValue, terminalValue);
    }

    /**
     * A fold variation that invokes the fold function only on those items that are members of this set
     * and NOT the provided set.
     */
    @Inline
    public long foldlDifference(AbstractKeys<K, ?> subtract, IndexedFoldToLong<? super K> fold, long param, long initialValue, long terminalValue)
    {
        return SortedArrays.foldlDifference(keys, subtract.keys, fold, param, initialValue, terminalValue);
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

}
