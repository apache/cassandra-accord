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

package accord.primitives;

import accord.api.RoutingKey;
import accord.utils.ArrayBuffers.ObjectBuffers;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static accord.utils.ArrayBuffers.cachedRanges;
import static accord.utils.SortedArrays.Search.FAST;
import static accord.utils.SortedArrays.swapHighLow32b;

public abstract class AbstractRanges<RS extends Routables<Range, ?>> implements Iterable<Range>, Routables<Range, RS>
{
    static final Range[] NO_RANGES = new Range[0];

    final Range[] ranges;

    AbstractRanges(@Nonnull Range[] ranges)
    {
        this.ranges = Invariants.nonNull(ranges);
    }

    public int indexOf(RoutableKey key)
    {
        return SortedArrays.binarySearch(ranges, 0, ranges.length, key, (k, r) -> -r.compareTo(k), FAST);
    }

    public int indexOf(Range find)
    {
        return SortedArrays.binarySearch(ranges, 0, ranges.length, find, Range::compareIntersecting, FAST);
    }

    public boolean contains(RoutableKey key)
    {
        return indexOf(key) >= 0;
    }

    public boolean containsAll(Routables<?, ?> that)
    {
        switch (that.kindOfContents())
        {
            default: throw new AssertionError();
            case Key: return containsAll((AbstractKeys<?, ?>) that);
            case Range: return containsAll((AbstractRanges<?>) that);
        }
    }

    /**
     * @return true iff {@code that} is fully contained within {@code this}
     */
    public boolean containsAll(AbstractKeys<?, ?> that)
    {
        if (this.isEmpty()) return that.isEmpty();
        if (that.isEmpty()) return true;
        return Routables.rangeFoldl(that, this, (from, to, p, v) -> v + (to - from), 0, 0, 0) == that.size();
    }

    /**
     * @return true iff {@code that} is a subset of {@code this}
     */
    public boolean containsAll(AbstractRanges<?> that)
    {
        if (this.isEmpty()) return that.isEmpty();
        if (that.isEmpty()) return true;
        return ((int) supersetLinearMerge(this.ranges, that.ranges)) == that.size();
    }

    public int size()
    {
        return ranges.length;
    }

    @Override
    public final Unseekable.Kind kindOfContents()
    {
        return Unseekable.Kind.Range;
    }

    @Override
    public final Range get(int i)
    {
        return ranges[i];
    }

    @Override
    public final boolean isEmpty()
    {
        return size() == 0;
    }

    public final boolean intersects(AbstractKeys<?, ?> keys)
    {
        return findNextIntersection(0, keys, 0) >= 0;
    }

    public final <K extends RoutableKey> boolean intersects(AbstractKeys<K, ?> keys, Predicate<? super K> matches)
    {
        int ri = 0, ki = 0;
        while (true)
        {
            long rki = findNextIntersection(ri, keys, ki);
            if (rki < 0)
                return false;

            ri = (int) (rki);
            ki = (int) (rki >>> 32);

            if (matches.test(keys.get(ki)))
                return true;

            ki++;
        }
    }

    public boolean intersects(AbstractRanges<?> that)
    {
        return SortedArrays.findNextIntersection(this.ranges, 0, that.ranges, 0, Range::compareIntersecting) >= 0;
    }

    public boolean intersects(Range that)
    {
        return SortedArrays.binarySearch(ranges, 0, ranges.length, that, Range::compareIntersecting, SortedArrays.Search.FAST) >= 0;
    }

    // returns ri in low 32 bits, ki in top, or -1 if no match found
    public long findNextIntersection(int ri, AbstractKeys<?, ?> keys, int ki)
    {
        return swapHighLow32b(SortedArrays.findNextIntersectionWithMultipleMatches(keys.keys, ki, ranges, ri));
    }

    // returns ki in bottom 32 bits, ri in top, or -1 if no match found
    public long findNextIntersection(int thisi, AbstractRanges<?> that, int thati)
    {
        return SortedArrays.findNextIntersectionWithMultipleMatches(ranges, thisi, that.ranges, thati, Range::compareIntersecting, Range::compareIntersecting);
    }

    @Override
    public final long findNextIntersection(int thisIndex, Routables<Range, ?> with, int withIndex)
    {
        return findNextIntersection(thisIndex, (AbstractRanges<?>) with, withIndex);
    }

    @Override
    public int findNext(int thisIndex, Range find, SortedArrays.Search search)
    {
        return SortedArrays.exponentialSearch(ranges, thisIndex, size(), find, Range::compareIntersecting, search);
    }

    /**
     * Returns the ranges that intersect with any of the members of the parameter.
     * DOES NOT MODIFY THE RANGES.
     */
    static <RS extends AbstractRanges<?>, P> RS intersect(RS input, Unseekables<?, ?> keysOrRanges, P param, BiFunction<P, Range[], RS> constructor)
    {
        switch (keysOrRanges.kindOfContents())
        {
            default: throw new AssertionError();
            case Range:
            {
                AbstractRanges<?> that = (AbstractRanges<?>) keysOrRanges;
                Range[] result = SortedArrays.linearIntersection(input.ranges, input.ranges.length, that.ranges, that.ranges.length, Range::compareIntersecting, cachedRanges());
                return result == input.ranges ? input : constructor.apply(param, result);
            }
            case Key:
            {
                AbstractKeys<?, ?> that = (AbstractKeys<?, ?>) keysOrRanges;
                Range[] result = SortedArrays.linearIntersection(input.ranges, input.ranges.length, that.keys, that.keys.length, cachedRanges());
                return result == input.ranges ? input : constructor.apply(param, result);
            }
        }
    }

    interface SliceConstructor<P, RS extends AbstractRanges<?>>
    {
        RS construct(Ranges covering, P param, Range[] ranges);
    }

    static <RS extends AbstractRanges<?>, P> RS slice(Ranges covering, AbstractRanges<?> input, P param, SliceConstructor<P, RS> constructor)
    {
        ObjectBuffers<Range> cachedRanges = cachedRanges();

        Range[] buffer = cachedRanges.get(covering.ranges.length + input.ranges.length);
        int bufferCount = 0;
        try
        {
            int li = 0, ri = 0;
            while (true)
            {
                long lri = covering.findNextIntersection(li, input, ri);
                if (lri < 0)
                    break;

                li = (int) (lri);
                ri = (int) (lri >>> 32);

                Range l = covering.ranges[li], r = input.ranges[ri];
                buffer[bufferCount++] = Range.slice(l, r);
                if (l.end().compareTo(r.end()) >= 0) ri++;
                else li++;
            }
            Range[] result = cachedRanges.complete(buffer, bufferCount);
            cachedRanges.discard(buffer, bufferCount);
            return constructor.construct(covering, param, result);
        }
        catch (Throwable t)
        {
            cachedRanges.forceDiscard(buffer, bufferCount);
            throw t;
        }
    }

    /**
     * attempts a linear merge where {@code as} is expected to be a superset of {@code bs},
     * terminating at the first indexes where this ceases to be true
     * @return index of {@code as} in upper 32bits, {@code bs} in lower 32bits
     *
     * TODO: better support for merging runs of overlapping or adjacent ranges
     */
    static long supersetLinearMerge(Range[] as, Range[] bs)
    {
        int ai = 0, bi = 0;
        out: while (ai < as.length && bi < bs.length)
        {
            Range a = as[ai];
            Range b = bs[bi];

            int c = a.compareIntersecting(b);
            if (c < 0)
            {
                ai++;
            }
            else if (c > 0)
            {
                break;
            }
            else if (b.start().compareTo(a.start()) < 0)
            {
                break;
            }
            else if ((c = b.end().compareTo(a.end())) <= 0)
            {
                bi++;
                if (c == 0) ai++;
            }
            else
            {
                // use a temporary counter, so that if we don't find a run of ranges that enforce the superset
                // condition we exit at the start of the mismatch run (and permit it to be merged)
                // TODO: use exponentialSearch
                int tmpai = ai;
                do
                {
                    if (++tmpai == as.length || !a.end().equals(as[tmpai].start()))
                        break out;
                    a = as[tmpai];
                }
                while (a.end().compareTo(b.end()) < 0);
                bi++;
                ai = tmpai;
            }
        }

        return ((long)ai << 32) | bi;
    }

    interface UnionConstructor<P1, P2, RS extends AbstractRanges<?>>
    {
        RS construct(P1 param1, P2 param2, Range[] ranges);
    }

    public enum UnionMode { MERGE_ADJACENT, MERGE_OVERLAPPING }

    /**
     * @return the union of {@code left} and {@code right}, returning one of the two inputs if possible
     */
    static <P1, P2, RS extends AbstractRanges<?>> RS union(UnionMode mode, AbstractRanges<?> left, AbstractRanges<?> right, P1 param1, P2 param2, UnionConstructor<P1, P2, RS> constructor)
    {
        if (left == right || right.isEmpty()) return constructor.construct(param1, param2, left.ranges);
        if (left.isEmpty()) return constructor.construct(param1, param2, right.ranges);

        Range[] as = left.ranges, bs = right.ranges;
        {
            // make sure as/ai represent the ranges right might fully contain the other
            int c = as[0].start().compareTo(bs[0].start());
            if (c > 0 || c == 0 && as[as.length - 1].end().compareTo(bs[bs.length - 1].end()) < 0)
            {
                Range[] tmp = as; as = bs; bs = tmp;
            }
        }

        int ai, bi; {
            long tmp = supersetLinearMerge(as, bs);
            ai = (int)(tmp >>> 32);
            bi = (int)tmp;
        }

        if (bi == bs.length)
            return constructor.construct(param1, param2, (as == left.ranges ? left : right).ranges);

        // TODO (now): caching
        Range[] result = new Range[as.length + (bs.length - bi)];
        int resultCount;
        switch (mode)
        {
            default: throw new AssertionError();
            case MERGE_ADJACENT:
                resultCount = copyAndMergeTouching(as, 0, result, 0, ai);
                break;
            case MERGE_OVERLAPPING:
                System.arraycopy(as, 0, result, 0, ai);
                resultCount = ai;
        }

        while (ai < as.length && bi < bs.length)
        {
            Range a = as[ai];
            Range b = bs[bi];

            int c = a.compareIntersecting(b);
            if (c < 0)
            {
                result[resultCount++] = a;
                ai++;
            }
            else if (c > 0)
            {
                result[resultCount++] = b;
                bi++;
            }
            else
            {
                // TODO: we don't seem to currently merge adjacent (but non-overlapping)
                RoutingKey start = a.start().compareTo(b.start()) <= 0 ? a.start() : b.start();
                RoutingKey end = a.end().compareTo(b.end()) >= 0 ? a.end() : b.end();
                ai++;
                bi++;
                while (ai < as.length || bi < bs.length)
                {
                    Range min;
                    if (ai == as.length) min = bs[bi];
                    else if (bi == bs.length) min = a = as[ai];
                    else min = as[ai].start().compareTo(bs[bi].start()) < 0 ? a = as[ai] : bs[bi];
                    if (min.start().compareTo(end) > 0)
                        break;
                    if (min.end().compareTo(end) > 0)
                        end = min.end();
                    if (a == min) ai++;
                    else bi++;
                }
                result[resultCount++] = a.subRange(start, end);
            }
        }

        while (ai < as.length)
            result[resultCount++] = as[ai++];

        while (bi < bs.length)
            result[resultCount++] = bs[bi++];

        if (resultCount < result.length)
            result = Arrays.copyOf(result, resultCount);

        return constructor.construct(param1, param2, result);
    }

    @Override
    public String toString()
    {
        return Arrays.toString(ranges);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(ranges);
    }

    @Override
    public boolean equals(Object that)
    {
        if (that == null || this.getClass() != that.getClass())
            return false;
        return Arrays.equals(this.ranges, ((AbstractRanges<?>) that).ranges);
    }

    @Override
    public Iterator<Range> iterator()
    {
        return Iterators.forArray(ranges);
    }

    static <RS extends AbstractRanges<?>> RS mergeTouching(RS input, Function<Range[], RS> constructor)
    {
        Range[] ranges = input.ranges;
        if (ranges.length == 0)
            return input;

        // TODO: use cache
        ObjectBuffers<Range> cachedKeyRanges = cachedRanges();
        Range[] buffer = cachedKeyRanges.get(ranges.length);
        try
        {
            int count = copyAndMergeTouching(ranges, 0, buffer, 0, ranges.length);
            if (count == buffer.length)
                return input;
            Range[] result = cachedKeyRanges.complete(buffer, count);
            cachedKeyRanges.discard(buffer, count);
            return constructor.apply(result);
        }
        catch (Throwable t)
        {
            cachedKeyRanges.forceDiscard(buffer, ranges.length);
            throw t;
        }
    }

    static int copyAndMergeTouching(Range[] src, int srcPosition, Range[] trg, int trgPosition, int srcCount)
    {
        if (srcCount == 0)
            return 0;

        int count = 0;
        Range prev = src[srcPosition];
        RoutingKey end = prev.end();
        for (int i = 1 ; i < srcCount ; ++i)
        {
            Range next = src[srcPosition + i];
            if (!end.equals(next.start()))
            {
                trg[trgPosition + count++] = maybeUpdateEnd(prev, end);
                prev = next;
            }
            end = next.end();
        }
        trg[trgPosition + count++] = maybeUpdateEnd(prev, end);
        return count;
    }

    static Range maybeUpdateEnd(Range range, RoutingKey withEnd)
    {
        return withEnd == range.end() ? range : range.subRange(range.start(), withEnd);
    }

    static <RS extends AbstractRanges<?>> RS of(Function<Range[], RS> constructor, Range... ranges)
    {
        if (ranges.length == 0)
            return constructor.apply(NO_RANGES);

        return sortAndDeoverlap(constructor, ranges, ranges.length);
    }

    static <RS extends AbstractRanges<?>> RS sortAndDeoverlap(Function<Range[], RS> constructor, Range[] ranges, int count)
    {
        if (count == 0)
            return constructor.apply(NO_RANGES);

        if (count == 1)
        {
            if (ranges.length == 1)
                return constructor.apply(ranges);

            return constructor.apply(Arrays.copyOf(ranges, count));
        }

        Arrays.sort(ranges, 0, count, Comparator.comparing(Range::start));
        Range prev = ranges[0];
        int removed = 0;
        for (int i = 1 ; i < count ; ++i)
        {
            Range next = ranges[i];
            if (prev.end().compareTo(next.start()) > 0)
            {
                prev = prev.subRange(prev.start(), next.start());
                if (prev.end().compareTo(next.end()) >= 0)
                {
                    removed++;
                }
                else if (removed > 0)
                {
                    ranges[i - removed] = prev = next.subRange(prev.end(), next.end());
                }
            }
            else if (removed > 0)
            {
                ranges[i - removed] = prev = next;
            }
        }

        count -= removed;
        if (count != ranges.length)
            ranges = Arrays.copyOf(ranges, count);

        return constructor.apply(ranges);
    }

    static <RS extends AbstractRanges<?>> RS ofSortedAndDeoverlapped(Function<Range[], RS> constructor, Range... ranges)
    {
        for (int i = 1 ; i < ranges.length ; ++i)
        {
            if (ranges[i - 1].end().compareTo(ranges[i].start()) > 0)
                throw new IllegalArgumentException(Arrays.toString(ranges) + " is not correctly sorted or deoverlapped");
        }

        return constructor.apply(ranges);
    }
}