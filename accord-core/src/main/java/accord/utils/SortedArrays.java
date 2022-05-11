package accord.utils;

import java.util.Arrays;
import java.util.function.IntFunction;

import org.apache.cassandra.utils.concurrent.Inline;

import static java.util.Arrays.*;

public class SortedArrays
{
    /**
     * Given two sorted arrays, return an array containing the elements present in either, preferentially returning one
     * of the inputs if it contains all elements of the other.
     *
     * TODO: introduce exponential search optimised version
     */
    public static <T extends Comparable<? super T>> T[] linearUnion(T[] left, T[] right, IntFunction<T[]> allocate)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        T[] result = null;
        int resultSize = 0;

        // first, pick the superset candidate and merge the two until we find the first missing item
        // if none found, return the superset candidate
        if (left.length >= right.length)
        {
            while (leftIdx < left.length && rightIdx < right.length)
            {
                int cmp = left[leftIdx].compareTo(right[rightIdx]);
                if (cmp <= 0)
                {
                    leftIdx += 1;
                    rightIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = leftIdx;
                    result = allocate.apply(resultSize + (left.length - leftIdx) + (right.length - (rightIdx - 1)));
                    System.arraycopy(left, 0, result, 0, resultSize);
                    result[resultSize++] = right[rightIdx++];
                    break;
                }
            }

            if (result == null)
            {
                if (rightIdx == right.length)
                    return left;
                result = allocate.apply(left.length + (right.length - rightIdx));
                resultSize = leftIdx;
                System.arraycopy(left, 0, result, 0, resultSize);
            }
        }
        else
        {
            while (leftIdx < left.length && rightIdx < right.length)
            {
                int cmp = left[leftIdx].compareTo(right[rightIdx]);
                if (cmp >= 0)
                {
                    rightIdx += 1;
                    leftIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = rightIdx;
                    result = allocate.apply(resultSize + (left.length - (leftIdx - 1)) + (right.length - rightIdx));
                    System.arraycopy(right, 0, result, 0, resultSize);
                    result[resultSize++] = left[leftIdx++];
                    break;
                }
            }

            if (result == null)
            {
                if (leftIdx == left.length)
                    return right;
                result = allocate.apply(right.length + (left.length - leftIdx));
                resultSize = rightIdx;
                System.arraycopy(right, 0, result, 0, resultSize);
            }
        }

        while (leftIdx < left.length && rightIdx < right.length)
        {
            T leftKey = left[leftIdx];
            T rightKey = right[rightIdx];
            int cmp = leftKey.compareTo(rightKey);
            T minKey;
            if (cmp == 0)
            {
                leftIdx++;
                rightIdx++;
                minKey = leftKey;
            }
            else if (cmp < 0)
            {
                leftIdx++;
                minKey = leftKey;
            }
            else
            {
                rightIdx++;
                minKey = rightKey;
            }
            result[resultSize++] = minKey;
        }

        while (leftIdx < left.length)
            result[resultSize++] = left[leftIdx++];

        while (rightIdx < right.length)
            result[resultSize++] = right[rightIdx++];

        if (resultSize < result.length)
            result = copyOf(result, resultSize);

        return result;
    }

    /**
     * Given two sorted arrays, return the elements present only in both, preferentially returning one of the inputs if
     * it contains no elements not present in the other.
     *
     * TODO: introduce exponential search optimised version
     */
    @SuppressWarnings("unused") // was used until recently, might be used again?
    public static <T extends Comparable<? super T>> T[] linearIntersection(T[] left, T[] right, IntFunction<T[]> allocate)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        T[] result = null;
        int resultSize = 0;

        // first pick a subset candidate, and merge both until we encounter an element not present in the other array
        if (left.length <= right.length)
        {
            while (leftIdx < left.length && rightIdx < right.length)
            {
                int cmp = left[leftIdx].compareTo(right[rightIdx]);
                if (cmp >= 0)
                {
                    rightIdx += 1;
                    leftIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = leftIdx++;
                    result = allocate.apply(resultSize + Math.min(left.length - leftIdx, right.length - rightIdx));
                    System.arraycopy(left, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return left;
        }
        else
        {
            while (leftIdx < left.length && rightIdx < right.length)
            {
                int cmp = left[leftIdx].compareTo(right[rightIdx]);
                if (cmp <= 0)
                {
                    leftIdx += 1;
                    rightIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = rightIdx++;
                    result = allocate.apply(resultSize + Math.min(left.length - leftIdx, right.length - rightIdx));
                    System.arraycopy(right, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return right;
        }

        while (leftIdx < left.length && rightIdx < right.length)
        {
            T leftKey = left[leftIdx];
            int cmp = leftKey.compareTo(right[rightIdx]);
            if (cmp == 0)
            {
                leftIdx++;
                rightIdx++;
                result[resultSize++] = leftKey;
            }
            else
            {
                if (cmp < 0) leftIdx++;
                else rightIdx++;
            }
        }

        if (resultSize < result.length)
            result = Arrays.copyOf(result, resultSize);
        
        return result;
    }

    /**
     * Given two sorted arrays, return the elements present only in the first, preferentially returning the first array
     * itself if possible
     */
    @SuppressWarnings("unused") // was used until recently, might be used again?
    public static <T extends Comparable<? super T>> T[] linearDifference(T[] left, T[] right, IntFunction<T[]> allocate)
    {
        int rightIdx = 0;
        int leftIdx = 0;

        T[] result = null;
        int resultSize = 0;

        while (leftIdx < left.length && rightIdx < right.length)
        {
            int cmp = left[leftIdx].compareTo(right[rightIdx]);
            if (cmp == 0)
            {
                resultSize = leftIdx++;
                ++rightIdx;
                result = allocate.apply(resultSize + left.length - leftIdx);
                System.arraycopy(left, 0, result, 0, resultSize);
                break;
            }
            else if (cmp < 0)
            {
                ++leftIdx;
            }
            else
            {
                ++rightIdx;
            }
        }

        if (result == null)
            return left;

        while (leftIdx < left.length && rightIdx < right.length)
        {
            int cmp = left[leftIdx].compareTo(right[rightIdx]);
            if (cmp > 0)
            {
                result[resultSize++] = left[leftIdx++];
            }
            else if (cmp < 0)
            {
                ++rightIdx;
            }
            else
            {
                ++leftIdx;
                ++rightIdx;
            }
        }

        if (resultSize < result.length)
            result = Arrays.copyOf(result, resultSize);

        return result;
    }

    public static <A, R> A[] sliceWithOverlaps(A[] slice, R[] select, IntFunction<A[]> factory, AsymmetricComparator<A, R> cmp1, AsymmetricComparator<R, A> cmp2)
    {
        A[] result;
        int resultCount;
        int ai = 0, ri = 0;
        while (true)
        {
            long ari = findNextIntersection(slice, ai, select, ri, cmp1, cmp2, Search.CEIL);
            if (ari < 0)
            {
                if (ai == slice.length)
                    return slice;

                return Arrays.copyOf(slice, ai);
            }

            int nextai = (int)(ari >>> 32);
            if (ai != nextai)
            {
                resultCount = ai;
                result = factory.apply(ai + (slice.length - nextai));
                System.arraycopy(slice, 0, result, 0, resultCount);
                ai = nextai;
                break;
            }

            ri = (int)ari;
            ai = exponentialSearch(slice, nextai, slice.length, select[ri], cmp2, Search.FLOOR) + 1;
        }

        while (true)
        {
            int nextai = exponentialSearch(slice, ai, slice.length, select[ri], cmp2, Search.FLOOR) + 1;
            while (ai < nextai)
                result[resultCount++] = slice[ai++];

            long ari = findNextIntersection(slice, ai, select, ri, cmp1, cmp2, Search.CEIL);
            if (ari < 0)
            {
                if (resultCount < result.length)
                    result = Arrays.copyOf(result, resultCount);

                return result;
            }

            ai = (int)(ari >>> 32);
            ri = (int)ari;
        }
    }

    /**
     * Copy-on-write insert into the provided array; returns the same array if item already present, or a new array
     * with the item in the correct position if not. Linear time complexity.
     */
    public static <T extends Comparable<? super T>> T[] insert(T[] src, T item, IntFunction<T[]> factory)
    {
        int insertPos = Arrays.binarySearch(src, item);
        if (insertPos >= 0)
            return src;
        insertPos = -1 - insertPos;

        T[] trg = factory.apply(src.length + 1);
        System.arraycopy(src, 0, trg, 0, insertPos);
        trg[insertPos] = item;
        System.arraycopy(src, insertPos, trg, insertPos + 1, src.length - insertPos);
        return trg;
    }

    /**
     * Equivalent to {@link Arrays#binarySearch}, only more efficient algorithmically for linear merges.
     * Binary search has worst case complexity {@code O(n.lg n)} for a linear merge, whereas exponential search
     * has a worst case of {@code O(n)}. However compared to a simple linear merge, the best case for exponential
     * search is {@code O(lg(n))} instead of {@code O(n)}.
     */
    public static <T1, T2 extends Comparable<? super T1>> int exponentialSearch(T1[] in, int from, int to, T2 find)
    {
        return exponentialSearch(in, from, to, find, Comparable::compareTo, Search.FAST);
    }

    // TODO: check inlining elides this
    public enum Search { FLOOR, CEIL, FAST }

    @Inline
    public static <T1, T2> int exponentialSearch(T2[] in, int from, int to, T1 find, AsymmetricComparator<T1, T2> comparator, Search op)
    {
        int step = 0;
        loop: while (from + step < to)
        {
            int i = from + step;
            int c = comparator.compare(find, in[i]);
            if (c < 0)
            {
                to = i;
                break;
            }
            if (c > 0)
            {
                from = i + 1;
            }
            else
            {
                switch (op)
                {
                    case FAST:
                        return i;

                    case CEIL:
                        if (step == 0)
                            return from;
                        to = i + 1; // could in theory avoid one extra comparison in this case, but would uglify things
                        break loop;

                    case FLOOR:
                        from = i;
                }
            }
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return binarySearch(in, from, to, find, comparator, op);
    }

    @Inline
    public static int exponentialSearch(int[] in, int from, int to, int find)
    {
        int step = 0;
        while (from + step < to)
        {
            int i = from + step;
            int c = Integer.compare(find, in[i]);
            if (c < 0)
            {
                to = i;
                break;
            }
            if (c > 0)
            {
                from = i + 1;
            }
            else
            {
                return i;
            }
            step = step * 2 + 1; // jump in perfect binary search increments
        }
        return Arrays.binarySearch(in, from, to, find);
    }

    @Inline
    public static <T1, T2> int binarySearch(T2[] in, int from, int to, T1 find, AsymmetricComparator<T1, T2> comparator, Search op)
    {
        int found = -1;
        while (from < to)
        {
            int i = (from + to) >>> 1;
            int c = comparator.compare(find, in[i]);
            if (c < 0)
            {
                to = i;
            }
            else if (c > 0)
            {
                from = i + 1;
            }
            else
            {
                switch (op)
                {
                    default: throw new IllegalStateException();
                    case FAST:
                        return i;

                    case CEIL:
                        to = found = i;
                        break;

                    case FLOOR:
                        found = i;
                        from = i + 1;
                }
            }
        }
        return found >= 0 ? found : -1 - to;
    }

    public static <T1, T2 extends Comparable<T1>> long findNextIntersectionWithOverlaps(T1[] as, int ai, T2[] bs, int bi)
    {
        return findNextIntersectionWithOverlaps(as, ai, bs, bi, (a, b) -> -b.compareTo(a), Comparable::compareTo);
    }

    public static <T1, T2> long findNextIntersectionWithOverlaps(T1[] as, int ai, T2[] bs, int bi, AsymmetricComparator<T1, T2> cmp1, AsymmetricComparator<T2, T1> cmp2)
    {
        return findNextIntersection(as, ai, bs, bi, cmp1, cmp2, Search.CEIL);
    }

    public static <T extends Comparable<? super T>> long findNextIntersection(T[] as, int ai, T[] bs, int bi)
    {
        return findNextIntersection(as, ai, bs, bi, Comparable::compareTo);
    }

    public static <T> long findNextIntersection(T[] as, int ai, T[] bs, int bi, AsymmetricComparator<T, T> comparator)
    {
        return findNextIntersection(as, ai, bs, bi, comparator, comparator, Search.FAST);
    }

    /**
     * Given two sorted arrays, find the next index in each array containing an equal item.
     *
     * Works with CEIL or FAST; FAST to be used if precisely one match for each item in either list, CEIL if one item
     *
     * in either list may be matched to multiple in the other list.
     */
    private static <T1, T2> long findNextIntersection(T1[] as, int ai, T2[] bs, int bi, AsymmetricComparator<T1, T2> cmp1, AsymmetricComparator<T2, T1> cmp2, Search op)
    {
        if (ai == as.length)
            return -1;

        while (true)
        {
            bi = SortedArrays.exponentialSearch(bs, bi, bs.length, as[ai], cmp1, op);
            if (bi >= 0)
                break;

            bi = -1 - bi;
            if (bi == bs.length)
                return -1;

            ai = SortedArrays.exponentialSearch(as, ai, as.length, bs[bi], cmp2, op);
            if (ai >= 0)
                break;

            ai = -1 - ai;
            if (ai == as.length)
                return -1;
        }
        return ((long)ai << 32) | bi;
    }

    public static <T extends Comparable<? super T>> int[] remapper(T[] src, T[] trg, boolean trgIsKnownSuperset)
    {
        return remapper(src, src.length, trg, trg.length, trgIsKnownSuperset);
    }

    /**
     * Given two sorted arrays, where one is a subset of the other, return an int[] of the same size as
     * the {@code src} parameter, with the index within {@code trg} of the corresponding element within {@code src},
     * or -1 otherwise.
     * That is, {@code src[i] == -1 || src[i].equals(trg[result[i]])}
     */
    public static <T extends Comparable<? super T>> int[] remapper(T[] src, int srcLength, T[] trg, int trgLength, boolean trgIsSuperset)
    {
        if (src == trg || (trgIsSuperset && trgLength == srcLength)) return null;
        int[] result = new int[srcLength];
        for (int i = 0, j = 0 ; i < srcLength && j < trgLength ;)
        {
            int c = src[i].compareTo(trg[j]);
            if (c < 0) result[i++] = -1;
            else if (c > 0) ++j;
            else result[i++] = j++;
        }
        return result;
    }

    public static int remap(int i, int[] remapper)
    {
        return remapper == null ? i : remapper[i];
    }
}
