/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.utils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.IntFunction;

import accord.utils.ArrayBuffers.ObjectBuffers;
import accord.utils.ArrayBuffers.IntBufferAllocator;
import net.nicoulaj.compilecommand.annotations.Inline;

import javax.annotation.Nullable;

import static accord.utils.ArrayBuffers.uncached;
import static accord.utils.SortedArrays.Search.FAST;

// TODO (low priority, efficiency): improvements:
//        - Either by manually duplicating or using compiler inlining directives to
//           - compile separate versions for Comparators vs Comparable.compareTo
//           - compile dedicated binarySearch and exponentialSearch functions for FLOOR, CEIL, HIGHER, LOWER
//        - Exploit exponentialSearch in union/intersection/etc
public class SortedArrays
{
    /**
     * {@link #linearUnion(Comparable[], int, Comparable[], int, ObjectBuffers)}
     */
    public static <T extends Comparable<? super T>> T[] linearUnion(T[] left, T[] right, IntFunction<T[]> allocator)
    {
        return linearUnion(left, right, uncached(allocator));
    }

    /**
     * {@link #linearUnion(Comparable[], int, Comparable[], int, ObjectBuffers)}
     */
    public static <T extends Comparable<? super T>> T[] linearUnion(T[] left, T[] right, ObjectBuffers<T> buffers)
    {
        return linearUnion(left, left.length, right, right.length, buffers);
    }

    /**
     * Given two sorted buffers where the contents within each array are unique, but may duplicate each other,
     * return a sorted array containing the result of merging the two input buffers.
     *
     * If one of the two input buffers represents a superset of the other, this buffer will be returned unmodified.
     *
     * Otherwise, depending on {@code buffers}, a result buffer may itself be returned or a new array.
     *
     * TODO (low priority, efficiency): introduce exponential search optimised version
     *                                  also compare with Hwang and Lin algorithm
     *                                  could also compare with a recursive partitioning scheme like quicksort
     * (note that dual exponential search is also an optimal algorithm, just seemingly ignored by the literature,
     * and may be in practice faster for lists that are more often similar in size, and only occasionally very different.
     * Without performing extensive analysis, exponential search likely has higher constant factors in terms of the
     * constant multiplier on number of comparisons performed, but lower constant factors for managing the algorithm state
     * unless we implemented the static Hwang and Lin that does not re-assess the relative sizes of the remaining inputs)
     *
     * We could also improve performance with instruction parallelism, by e.g. merging the front and backs of the
     * two input arrays independently, copying to the front/back of each buffer. Since most results must be array-copied
     * to be minimised this would only be costlier in situations where we are returning the output buffer for re-use,
     * and it would not be much costlier.
     */
    public static <T extends Comparable<? super T>> T[] linearUnion(T[] left, int leftLength, T[] right, int rightLength, ObjectBuffers<T> buffers)
    {
        return linearUnion(left, leftLength, right, rightLength, Comparable::compareTo, buffers);
    }

    public static <T> T[] linearUnion(T[] left, int leftLength, T[] right, int rightLength, AsymmetricComparator<? super T, ? super T> comparator, ObjectBuffers<T> buffers)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        T[] result = null;
        int resultSize = 0;

        // first, pick the superset candidate and merge the two until we find the first missing item
        // if none found, return the superset candidate
        if (leftLength >= rightLength)
        {
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp <= 0)
                {
                    leftIdx += 1;
                    rightIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = leftIdx;
                    result = buffers.get(resultSize + (leftLength - leftIdx) + (rightLength - (rightIdx - 1)));
                    System.arraycopy(left, 0, result, 0, resultSize);
                    result[resultSize++] = right[rightIdx++];
                    break;
                }
            }

            if (result == null)
            {
                if (rightIdx == rightLength) // all elements matched, so can return the other array
                    return buffers.completeWithExisting(left, leftLength);
                // no elements matched or only a subset matched
                result = buffers.get(leftLength + (rightLength - rightIdx));
                resultSize = leftIdx;
                System.arraycopy(left, 0, result, 0, resultSize);
            }
        }
        else
        {
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp >= 0)
                {
                    rightIdx += 1;
                    leftIdx += cmp == 0 ? 1 : 0;
                }
                else
                {
                    resultSize = rightIdx;
                    result = buffers.get(resultSize + (leftLength - (leftIdx - 1)) + (rightLength - rightIdx));
                    System.arraycopy(right, 0, result, 0, resultSize);
                    result[resultSize++] = left[leftIdx++];
                    break;
                }
            }

            if (result == null)
            {
                if (leftIdx == leftLength) // all elements matched, so can return the other array
                    return buffers.completeWithExisting(right, rightLength);
                // no elements matched or only a subset matched
                result = buffers.get(rightLength + (leftLength - leftIdx));
                resultSize = rightIdx;
                System.arraycopy(right, 0, result, 0, resultSize);
            }
        }

        try
        {
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

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

            while (leftIdx < leftLength)
                result[resultSize++] = left[leftIdx++];

            while (rightIdx < rightLength)
                result[resultSize++] = right[rightIdx++];

            return buffers.complete(result, resultSize);
        }
        finally
        {
            buffers.discard(result, resultSize);
        }
    }

    /**
     * {@link #linearIntersection(Comparable[], int, Comparable[], int, ObjectBuffers)}
     */
    public static <T extends Comparable<? super T>> T[] linearIntersection(T[] left, T[] right, IntFunction<T[]> allocator)
    {
        return linearIntersection(left, right, uncached(allocator));
    }

    /**
     * {@link #linearIntersection(Comparable[], int, Comparable[], int, ObjectBuffers)}
     */
    public static <T extends Comparable<? super T>> T[] linearIntersection(T[] left, T[] right, ObjectBuffers<T> buffers)
    {
        return linearIntersection(left, left.length, right, right.length, buffers);
    }

    public static <T extends Comparable<? super T>> T[] linearIntersection(T[] left, int leftLength, T[] right, int rightLength, ObjectBuffers<T> buffers)
    {
        return linearIntersection(left, leftLength, right, rightLength, Comparable::compareTo, buffers);
    }

    /**
     * Given two sorted buffers where the contents within each array are unique, but may duplicate each other,
     * return a sorted sorted array containing the elements present in both input buffers.
     *
     * If one of the two input buffers represents a superset of the other, this buffer will be returned unmodified.
     *
     * Otherwise, depending on {@code buffers}, a result buffer may itself be returned or a new array.
     *
     * TODO (low priority, efficiency): introduce exponential search optimised version
     */
    public static <T> T[] linearIntersection(T[] left, int leftLength, T[] right, int rightLength, AsymmetricComparator<? super T, ? super T> comparator, ObjectBuffers<T> buffers)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        T[] result = null;
        int resultSize = 0;

        // first pick a subset candidate, and merge both until we encounter an element not present in the other array
        if (leftLength <= rightLength)
        {
            boolean hasMatch = false;
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp >= 0)
                {
                    rightIdx += 1;
                    leftIdx += cmp == 0 ? 1 : 0;
                    if (cmp == 0)
                        hasMatch = true;
                }
                else
                {
                    resultSize = leftIdx++;
                    result = buffers.get(resultSize + Math.min(leftLength - leftIdx, rightLength - rightIdx));
                    System.arraycopy(left, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return hasMatch ? buffers.completeWithExisting(left, leftLength) : buffers.complete(buffers.get(0), 0);
        }
        else
        {
            boolean hasMatch = false;
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp <= 0)
                {
                    leftIdx += 1;
                    rightIdx += cmp == 0 ? 1 : 0;
                    if (cmp == 0)
                        hasMatch = true;
                }
                else
                {
                    resultSize = rightIdx++;
                    result = buffers.get(resultSize + Math.min(leftLength - leftIdx, rightLength - rightIdx));
                    System.arraycopy(right, 0, result, 0, resultSize);
                    break;
                }
            }

            if (result == null)
                return hasMatch ? buffers.completeWithExisting(right, rightLength) : buffers.complete(buffers.get(0), 0);
        }

        try
        {

            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T leftKey = left[leftIdx];
                T rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : comparator.compare(leftKey, rightKey);

                if (cmp == 0)
                {
                    leftIdx++;
                    rightIdx++;
                    result[resultSize++] = leftKey;
                }
                else if (cmp < 0) leftIdx++;
                else rightIdx++;
            }

            return buffers.complete(result, resultSize);
        }
        finally
        {
            buffers.discard(result, resultSize);
        }
    }

    /**
     * Given two sorted buffers where the contents within each array are unique, but may duplicate each other,
     * return a sorted sorted array containing the elements present in both input buffers.
     *
     * If one of the two input buffers represents a superset of the other, this buffer will be returned unmodified.
     *
     * Otherwise, depending on {@code buffers}, a result buffer may itself be returned or a new array.
     *
     * TODO (low priority, efficiency): introduce exponential search optimised version
     */
    public static <T2, T1 extends Comparable<? super T2>> T1[] linearIntersection(T1[] left, int leftLength, T2[] right, int rightLength, ObjectBuffers<T1> buffers)
    {
        int leftIdx = 0;
        int rightIdx = 0;

        T1[] result = null;
        int resultSize = 0;

        boolean hasMatch = false;
        while (leftIdx < leftLength && rightIdx < rightLength)
        {
            T1 leftKey = left[leftIdx];
            T2 rightKey = right[rightIdx];
            int cmp = leftKey == rightKey ? 0 : leftKey.compareTo(rightKey);

            if (cmp >= 0)
            {
                rightIdx += 1;
                leftIdx += cmp == 0 ? 1 : 0;
                if (cmp == 0)
                    hasMatch = true;
            }
            else
            {
                resultSize = leftIdx++;
                result = buffers.get(resultSize + Math.min(leftLength - leftIdx, rightLength - rightIdx));
                System.arraycopy(left, 0, result, 0, resultSize);
                break;
            }
        }

        if (result == null)
            return hasMatch ? buffers.completeWithExisting(left, leftLength) : buffers.complete(buffers.get(0), 0);

        try
        {
            while (leftIdx < leftLength && rightIdx < rightLength)
            {
                T1 leftKey = left[leftIdx];
                T2 rightKey = right[rightIdx];
                int cmp = leftKey == rightKey ? 0 : leftKey.compareTo(rightKey);

                if (cmp == 0)
                {
                    leftIdx++;
                    rightIdx++;
                    result[resultSize++] = leftKey;
                }
                else if (cmp < 0) leftIdx++;
                else rightIdx++;
            }

            return buffers.complete(result, resultSize);
        }
        finally
        {
            buffers.discard(result, resultSize);
        }
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
            T leftKey = left[leftIdx];
            T rightKey = right[rightIdx];
            int cmp = leftKey == rightKey ? 0 : leftKey.compareTo(rightKey);

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
            T leftKey = left[leftIdx];
            T rightKey = right[rightIdx];
            int cmp = leftKey == rightKey ? 0 : leftKey.compareTo(rightKey);

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
        while (leftIdx < left.length)
            result[resultSize++] = left[leftIdx++];

        if (resultSize < result.length)
            result = Arrays.copyOf(result, resultSize);

        return result;
    }

    /**
     * Given two sorted arrays {@code slice} and {@code select}, where each array's contents is unique and non-overlapping
     * with itself, but may match multiple entries in the other array, return a new array containing the elements of {@code slice}
     * that match elements of {@code select} as per the provided comparators.
     */
    public static <A, R> A[] sliceWithMultipleMatches(A[] slice, R[] select, IntFunction<A[]> factory, AsymmetricComparator<A, R> cmp1, AsymmetricComparator<R, A> cmp2)
    {
        A[] result;
        int resultCount;
        int ai = 0, ri = 0;
        while (true)
        {
            long ari = findNextIntersection(slice, ai, slice.length, select, ri, select.length, cmp1, cmp2, Search.CEIL);
            if (ari < 0)
            {
                if (ai == slice.length)
                    return slice; // all elements of slice were found in select, so can return the array unchanged

                // The first (ai - 1) elements are present (without a gap), so copy just that subset
                return Arrays.copyOf(slice, ai);
            }

            int nextai = (int)(ari);
            if (ai != nextai)
            {
                // A gap is detected in slice!
                // When ai == nextai we "consume" it and move to the last instance of slice[ai], then choose the next element,
                // this means that ai currently points to an element in slice where it is not known if its present in select,
                // so != implies a gap is detected!
                resultCount = ai;
                result = factory.apply(ai + (slice.length - nextai));
                System.arraycopy(slice, 0, result, 0, resultCount);
                ai = nextai;
                ri = (int)(ari >>> 32);
                break;
            }

            ri = (int)(ari >>> 32);
            // In cases where duplicates are present in slice, find the last instance of slice[ai], and move past it.
            // slice[ai] is known to be present, so need to check the next element.
            ai = exponentialSearch(slice, nextai, slice.length, select[ri], cmp2, Search.FLOOR) + 1;
        }

        while (true)
        {
            // Find the next element after the last element matching select[ri] and copy from slice into result
            // nextai may be negative (such as -1), so the +1 may keep it negative OR set 0, since 0 < 0 is false
            // it is safe to avoid checking for negative values
            int nextai = exponentialSearch(slice, ai, slice.length, select[ri], cmp2, Search.FLOOR) + 1;
            while (ai < nextai)
                result[resultCount++] = slice[ai++];

            long ari = findNextIntersection(slice, ai, slice.length, select, ri, select.length, cmp1, cmp2, Search.CEIL);
            if (ari < 0)
            {
                if (resultCount < result.length)
                    result = Arrays.copyOf(result, resultCount);

                return result;
            }

            ai = (int)(ari);
            ri = (int)(ari >>> 32);
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
        return exponentialSearch(in, from, to, find, Comparable::compareTo, FAST);
    }

    public enum Search
    {
        /**
         * If no matches, return -1 - [the highest index of any element that sorts before]
         * If multiple matches, return the one with the highest index
         */
        FLOOR,

        /**
         * If no matches, return -1 - [the lowest index of any element that sorts after]
         * If multiple matches, return the one with the lowest index
         */
        CEIL,

        /**
         * If no matches, return -1 - [the lowest index of any element that sorts after]
         * If multiple matches, return an arbitrary matching index
         */
        FAST
    }

    /**
     * Given a sorted array and an item to locate, use exponentialSearch to find a position in the array containing the item,
     * or if not present an index relative to the item's position were it to be inserted. exponentialSearch offers greater
     * efficiency than binarySearch when recursing over a list sequentially, finding matches within it.
     *
     * If multiple entries match, return either:
     *  FAST: the first we encounter
     *  FLOOR: the highest matching array index
     *  CEIL: the lowest matching array index
     *
     * If no entries match, similar to Arrays.binarySearch return either:
     *  FAST, CEIL: the entry following {@code find}, i.e. -1 - insertPos (== Arrays.binarySearch)
     *  FLOOR:      the entry preceding {@code find}, i.e. -2 - insertPos
     */
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

    /**
     * Given a sorted array and an item to locate, use exponentialSearch to find a position in the array containing the item,
     * or if not present an index relative to the item's position were it to be inserted. exponentialSearch offers greater
     * efficiency than binarySearch when recursing over a list sequentially, finding matches within it.
     *
     * exponentialSearch offers greater efficiency than binarySearch when recursing over a list sequentially,
     * finding matches within it.
     *
     * If multiple entries match, return either:
     *  FAST: the first we encounter
     *  FLOOR: the highest matching array index
     *  CEIL: the lowest matching array index
     *
     * If no entries match, similar to Arrays.binarySearch return either:
     *  FAST, CEIL: the entry following {@code find}, i.e. -1 - insertPos (== Arrays.binarySearch)
     *  FLOOR:      the entry preceding {@code find}, i.e. -2 - insertPos
     */
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

    /**
     * Given a sorted array and an item to locate, use binarySearch to find a position in the array containing the item,
     * or if not present an index relative to the item's position were it to be inserted.
     *
     * If multiple entries match, return either:
     *  FAST: the first we encounter
     *  FLOOR: the highest matching array index
     *  CEIL: the lowest matching array index
     *
     * If no entries match, similar to Arrays.binarySearch return either:
     *  FAST, CEIL: the entry following {@code find}, i.e. -1 - insertPos (== Arrays.binarySearch)
     *  FLOOR:      the entry preceding {@code find}, i.e. -2 - insertPos
     */
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

    /**
     * Given two sorted arrays where an item in each array may match multiple in the other, find the next
     * index in each array containing an equal item.
     */
    public static <T1, T2 extends Comparable<T1>> long findNextIntersectionWithMultipleMatches(T1[] as, int ai, T2[] bs, int bi)
    {
        return findNextIntersectionWithMultipleMatches(as, ai, bs, bi, (a, b) -> -b.compareTo(a), Comparable::compareTo);
    }

    /**
     * Given two sorted arrays where an item in each array may match multiple in the other, find the next
     * index in each array containing an equal item.
     */
    public static <T1, T2> long findNextIntersectionWithMultipleMatches(T1[] as, int ai, T2[] bs, int bi, AsymmetricComparator<T1, T2> cmp1, AsymmetricComparator<T2, T1> cmp2)
    {
        return findNextIntersection(as, ai, as.length, bs, bi, bs.length, cmp1, cmp2, Search.CEIL);
    }

    /**
     * Given two sorted arrays where an item in each array may match at most one in the other, find the next
     * index in each array containing an equal item.
     */
    @Inline
    public static <T extends Comparable<? super T>> long findNextIntersection(T[] as, int ai, T[] bs, int bi)
    {
        return findNextIntersection(as, ai, as.length, bs, bi, bs.length);
    }

    /**
     * Given two sorted arrays where an item in each array may match at most one in the other, find the next
     * index in each array containing an equal item.
     */
    @Inline
    public static <T extends Comparable<? super T>> long findNextIntersection(T[] as, int ai, int alim, T[] bs, int bi, int blim)
    {
        return findNextIntersection(as, ai, alim, bs, bi, blim, Comparable::compareTo, Comparable::compareTo, FAST);
    }

    /**
     * Given two sorted arrays where an item in each array may match at most one in the other, find the next
     * index in each array containing an equal item.
     */
    @Inline
    public static <T> long findNextIntersection(T[] as, int ai, int alim, T[] bs, int bi, int blim, AsymmetricComparator<? super T, ? super T> comparator)
    {
        return findNextIntersection(as, ai, alim, bs, bi, blim, comparator, comparator, FAST);
    }

    /**
     * Given two sorted arrays where an item in each array may match at most one in the other, find the next
     * index in each array containing an equal item.
     */
    @Inline
    public static <T> long findNextIntersection(T[] as, int ai, T[] bs, int bi, AsymmetricComparator<T, T> comparator)
    {
        return findNextIntersection(as, ai, as.length, bs, bi, bs.length, comparator, comparator, FAST);
    }

    /**
     * Given two sorted arrays, find the next index in each array containing an equal item.
     *
     * Works with CEIL or FAST; FAST to be used if precisely one match for each item in either list, CEIL if one item
     * in either list may be matched to multiple in the other list.
     */
    @Inline
    private static <T1, T2> long findNextIntersection(T1[] as, int ai, int asLength, T2[] bs, int bi, int bsLength, AsymmetricComparator<? super T1, ? super T2> cmp1, AsymmetricComparator<? super T2, ? super T1> cmp2, Search op)
    {
        if (ai == asLength)
            return -1;

        while (true)
        {
            bi = SortedArrays.exponentialSearch(bs, bi, bsLength, as[ai], cmp1, op);
            if (bi >= 0)
                break;

            bi = -1 - bi;
            if (bi == bsLength)
                return -1;

            ai = SortedArrays.exponentialSearch(as, ai, asLength, bs[bi], cmp2, op);
            if (ai >= 0)
                break;

            ai = -1 - ai;
            if (ai == asLength)
                return -1;
        }
        return ai | ((long)bi << 32);
    }

    public static long swapHighLow32b(long v)
    {
        return (v << 32) | (v >>> 32);
    }

    /**
     * Given two portions of sorted arrays with unique elements, where {@code trg} is a subset of the {@code src},
     * return an int[] with its initial {@code srcLength} elements populated, with the index within {@code trg}
     * of the corresponding element within {@code src}.
     *
     * That is, {@code src[i].equals(trg[result[i]])}
     *
     * @return null if {@code src.equals(trg)} or map of offsets within trg
     */
    @Nullable
    public static <T extends Comparable<? super T>> int[] remapToSuperset(T[] src, int srcLength, T[] trg, int trgLength,
                                                                          IntBufferAllocator allocator)
    {
        return remapToSuperset(src, srcLength, trg, trgLength, Comparable::compareTo, allocator);
    }

    @Nullable
    public static <T> int[] remapToSuperset(T[] src, int srcLength, T[] trg, int trgLength, AsymmetricComparator<? super T, ? super T> comparator,
                                            IntBufferAllocator allocator)
    {
        if (src == trg || trgLength == srcLength)
            return null;

        int[] result = allocator.getInts(srcLength);

        int i = 0, j = 0;
        while (i < srcLength && j < trgLength)
        {
            if (src[i] != trg[j] && !src[i].equals(trg[j]))
            {
                j = SortedArrays.exponentialSearch(trg, j, trgLength, src[i], comparator, FAST);
                if (j < 0)
                {
                    if (i > 0 && src[i] == src[i-1])
                        throw new IllegalStateException("Unexpected value in source: " + src[i] + " at index " + i + " duplicates index " + (i - 1));
                    throw new IllegalStateException("Unexpected value in source: " + src[i] + " at index " + i + " does not exist in target array");
                }
            }
            result[i++] = j++;
        }
        if (i != srcLength)
            throw new IllegalStateException("Unexpected value in source: " + src[i] + " at index " + i + " does not exist in target array");
        return result;
    }

    public static int remap(int i, int[] remapper)
    {
        return remapper == null ? i : remapper[i];
    }

    /**
     * A fold variation that intersects two key sets, invoking the fold function only on those
     * items that are members of both sets (with their corresponding indices).
     */
    @Inline
    public static <T extends Comparable<? super T>> long foldlIntersection(T[] as, int ai, int alim, T[] bs, int bi, int blim, IndexedFoldIntersectToLong<? super T> fold, long param, long initialValue, long terminalValue)
    {
        return foldlIntersection(Comparable::compareTo, as, ai, alim, bs, bi, blim, fold, param, initialValue, terminalValue);
    }

    @Inline
    public static <T> long foldlIntersection(AsymmetricComparator<? super T, ? super T> comparator, T[] as, int ai, int alim, T[] bs, int bi, int blim, IndexedFoldIntersectToLong<? super T> fold, long param, long initialValue, long terminalValue)
    {
        while (true)
        {
            long abi = findNextIntersection(as, ai, alim, bs, bi, blim, comparator);
            if (abi < 0)
                break;

            ai = (int)(abi);
            bi = (int)(abi >>> 32);

            initialValue = fold.apply(as[ai], param, initialValue, ai, bi);
            if (initialValue == terminalValue)
                break;

            ++ai;
            ++bi;
        }

        return initialValue;
    }

    public static <T extends Comparable<T>> void assertSorted(T[] array)
    {
        if (!isSorted(array))
            throw new IllegalArgumentException(Arrays.toString(array) + " is not sorted");
    }

    public static <T extends Comparable<T>> boolean isSorted(T[] array)
    {
        return isSorted(array, Comparable::compareTo);
    }

    public static <T> boolean isSorted(T[] array, Comparator<T> comparator)
    {
        return isSorted(array, comparator, 1);
    }


    public static <T extends Comparable<T>> boolean isSortedUnique(T[] array)
    {
        return isSortedUnique(array, Comparable::compareTo);
    }

    public static  <T> boolean isSortedUnique(T[] array, Comparator<T> comparator)
    {
        return isSorted(array, comparator, 0);
    }

    private static <T> boolean isSorted(T[] array, Comparator<T> comparator, int compareTo)
    {
        for (int i = 1 ; i < array.length ; ++i)
        {
            if (comparator.compare(array[i - 1], array[i]) >= compareTo)
                return false;
        }
        return true;
    }


}
