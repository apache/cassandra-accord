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

import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import javax.annotation.Nonnull;

import com.google.common.collect.Iterators;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.utils.ArrayBuffers.ObjectBuffers;
import accord.utils.IndexedFoldToLong;
import accord.utils.IndexedTriFold;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.UnhandledEnum;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.primitives.Ranges.EMPTY;
import static accord.utils.ArrayBuffers.cachedRanges;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FAST;
import static accord.utils.SortedArrays.isSorted;
import static accord.utils.SortedArrays.swapHighLow32b;

public abstract class AbstractRanges implements Iterable<Range>, Routables<Range>
{
    static final Range[] NO_RANGES = new Range[0];

    final Range[] ranges;

    AbstractRanges(@Nonnull Range[] ranges)
    {
        // TODO (simple, validation): check ranges are non-overlapping (or make sure it's safe for all methods that they aren't)
        this.ranges = Invariants.nonNull(ranges);
    }

    @Override
    public final Routable.Kind domainKind()
    {
        return Routable.Kind.Range;
    }

    public int indexOf(RoutableKey key)
    {
        return SortedArrays.binarySearch(ranges, 0, ranges.length, key, (k, r) -> -r.compareTo(k), FAST);
    }

    public int indexOf(Range find, SortedArrays.Search op)
    {
        return SortedArrays.binarySearch(ranges, 0, ranges.length, find, Range::compareIntersecting, op);
    }

    public int ceilIndexOf(Range find)
    {
        return indexOf(find, CEIL);
    }

    public final boolean contains(RoutingKey key)
    {
        return indexOf(key) >= 0;
    }

    public final boolean contains(Key key)
    {
        return indexOf(key) >= 0;
    }

    public final boolean contains(RoutableKey key)
    {
        return indexOf(key) >= 0;
    }

    /**
     * @return true iff {@code that} is fully contained within {@code this}
     */
    public final boolean containsAll(Keys that)
    {
        return containsAll((AbstractKeys<?>) that);
    }

    /**
     * @return true iff {@code that} is fully contained within {@code this}
     */
    public final boolean containsAll(AbstractUnseekableKeys that)
    {
        return containsAll((AbstractKeys<?>) that);
    }

    /**
     * @return true iff {@code that} is fully contained within {@code this}
     */
    public final boolean containsAll(AbstractKeys<?> that)
    {
        if (this.isEmpty()) return that.isEmpty();
        if (that.isEmpty()) return true;
        return Routables.rangeFoldl(that, this, (p, v, from, to) -> v + (to - from), 0, 0, 0) == that.size();
    }

    /**
     * @return true iff {@code that} is a subset of {@code this}
     */
    public final boolean containsAll(AbstractRanges that)
    {
        if (this.isEmpty()) return that.isEmpty();
        if (that.isEmpty()) return true;
        return ((int) supersetLinearMerge(this.ranges, that.ranges)) == that.size();
    }

    public boolean contains(Range range)
    {
        return indexOf(range, FAST) >= 0;
    }

    @Override
    public int size()
    {
        return ranges.length;
    }

    @Override
    public final Routable.Domain domain()
    {
        return Routable.Domain.Range;
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

    @Override
    public final boolean intersects(AbstractUnseekableKeys keys)
    {
        return findNextIntersection(0, keys, 0) >= 0;
    }

    @Override
    public final boolean intersects(AbstractRanges that)
    {
        return SortedArrays.findNextIntersection(this.ranges, 0, that.ranges, 0, Range::compareIntersecting) >= 0;
    }

    @Override
    public final boolean intersects(Range that)
    {
        return indexOf(that, FAST) >= 0;
    }

    public boolean intersects(Routable key)
    {
        switch (key.domain())
        {
            default: throw new AssertionError();
            case Range: return intersects((Range)key);
            case Key: return contains((RoutableKey) key);
        }
    }

    // returns ri in low 32 bits, ki in top, or -1 if no match found
    @Override
    public final long findNextIntersection(int ri, Keys keys, int ki)
    {
        return swapHighLow32b(SortedArrays.findNextIntersectionWithMultipleMatches(keys.keys, ki, ranges, ri));
    }

    @Override
    public final long findNextIntersection(int ri, AbstractUnseekableKeys keys, int ki)
    {
        return swapHighLow32b(SortedArrays.findNextIntersectionWithMultipleMatches(keys.keys, ki, ranges, ri));
    }

    // returns ki in bottom 32 bits, ri in top, or -1 if no match found
    @Override
    public final long findNextIntersection(int thisi, AbstractRanges that, int thati)
    {
        return SortedArrays.findNextIntersectionWithMultipleMatches(ranges, thisi, that.ranges, thati, Range::compareIntersecting, Range::compareIntersecting);
    }

    // returns ki in bottom 32 bits, ri in top, or -1 if no match found
    public final long findNextSameKindIntersection(int thisi, Routables<Range> that, int thati)
    {
        return findNextIntersection(thisi, (AbstractRanges) that, thati);
    }

    @Override
    public final long findNextIntersection(int thisIndex, Routables<Range> with, int withIndex)
    {
        return findNextIntersection(thisIndex, (AbstractRanges) with, withIndex);
    }

    @Override
    public final int find(Range find, SortedArrays.Search search)
    {
        return SortedArrays.binarySearch(ranges, 0, size(), find, Range::compareIntersecting, search);
    }

    @Override
    public final int findNext(int thisIndex, Range find, SortedArrays.Search search)
    {
        return SortedArrays.exponentialSearch(ranges, thisIndex, size(), find, Range::compareIntersecting, search);
    }

    public final int find(RoutingKey find, SortedArrays.Search search)
    {
        return SortedArrays.binarySearch(ranges, 0, size(), find, (k, r) -> -r.compareTo(k), search);
    }

    public final int findNext(int thisIndex, RoutingKey find, SortedArrays.Search search)
    {
        return SortedArrays.exponentialSearch(ranges, thisIndex, size(), find, (k, r) -> -r.compareTo(k), search);
    }

    public Ranges toRanges()
    {
        return Ranges.ofSortedAndDeoverlappedUnchecked(ranges);
    }

    /**
     * Subtracts the given set of ranges from this
     */
    Range[] withoutInternal(AbstractRanges that)
    {
        if (that.isEmpty())
            return ranges;

        if (isEmpty() || that == this)
            return EMPTY.ranges;

        ObjectBuffers<Range> cachedRanges = cachedRanges();
        Range[] result = null;

        int count = 0;
        int i = 0, j = 0;
        Range iv = ranges[0];
        while (true)
        {
            j = that.findNext(j, iv, CEIL);
            if (j < 0)
            {
                j = -1 - j;
                int nexti = j == that.size() ? size() : findNext(i + 1, that.ranges[j], CEIL);
                if (nexti < 0) nexti = -1 - nexti;
                if (count == 0)
                    result = cachedRanges.get(1 + (this.size() - i) + (that.size() - j));
                else if (count == result.length)
                    result = cachedRanges.resize(result, count, count * 2);

                result[count] = iv;
                if (nexti > i + 1)
                    System.arraycopy(ranges, i + 1, result, count + 1, nexti - (i + 1));
                count += nexti - i;

                if (nexti == ranges.length)
                    break;
                iv = ranges[i = nexti];
                continue;
            }

            Range jv = that.ranges[j];
            if (jv.start().compareTo(iv.start()) > 0)
            {
                if (count == 0)
                    result = cachedRanges.get(1 + (this.size() - i) + (that.size() - j));
                else if (count == result.length)
                    result = cachedRanges.resize(result, count, count * 2);

                result[count++] = iv.newRange(iv.start(), jv.start());
            }

            if (jv.end().compareTo(iv.end()) >= 0)
            {
                if (++i == ranges.length)
                    break;
                iv = ranges[i];
            }
            else
            {
                iv = iv.newRange(jv.end(), iv.end());
            }
        }

        if (count == 0)
            return EMPTY.ranges;

        return cachedRanges.completeAndDiscard(result, count);
    }

    /**
     * Returns the inputs that intersect with any of the members of the keysOrRanges.
     * DOES NOT MODIFY THE INPUT.
     */
    static <I extends AbstractRanges, P> I intersecting(AbstractKeys<?> intersecting, I input, P param, SliceConstructor<I, P, I> constructor, Slice slice)
    {
        switch (slice)
        {
            default: throw new UnhandledEnum(slice);
            case Minimal:
                RoutableKey[] newKeys = SortedArrays.sliceWithMultipleMatches(intersecting.keys, input.ranges, RoutableKey[]::new, (k, r) -> -r.compareTo(k), Range::compareTo);
                if (newKeys.length == 0)
                    return constructor.construct(input, param, Ranges.NO_RANGES);
                Range[] newRanges = new Range[newKeys.length];
                for (int i = 0 ; i < newKeys.length ; ++i)
                    newRanges[i] = newKeys[i].toUnseekable().asRange();
                return constructor.construct(input, param, newRanges);

            case Maximal:
            case Overlapping:
                Range[] result = SortedArrays.intersectWithMultipleMatches(input.ranges, input.ranges.length, intersecting.keys, intersecting.keys.length, Range::compareTo, cachedRanges());
                return result == input.ranges ? input : constructor.construct(input, param, result);
        }
    }

    interface SliceConstructor<I extends AbstractRanges, P, RS>
    {
        RS construct(I covering, P param, Range[] ranges);
    }

    static <I extends AbstractRanges, P, O> O slice(I covering, Slice slice, AbstractRanges input, P param, SliceConstructor<I, P, O> constructor)
    {
        switch (slice)
        {
            default: throw new AssertionError();
            case Overlapping: return sliceOverlapping(covering, input, param, constructor);
            case Minimal: return sliceMinimal(covering, input, param, constructor);
            case Maximal: return sliceMaximal(covering, input, param, constructor);
        }
    }

    static <C extends AbstractRanges, P, O> O sliceOverlapping(C covering, AbstractRanges input, P param, SliceConstructor<? super C, P, O> constructor)
    {
        Range[] result = SortedArrays.intersectWithMultipleMatches(input.ranges, input.ranges.length, covering.ranges, covering.ranges.length, Range::compareIntersecting, cachedRanges());
        return constructor.construct(covering, param, result);
    }

    static <C extends AbstractRanges, P, O> O sliceMinimal(C covering, AbstractRanges input, P param, SliceConstructor<C, P, O> constructor)
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

                if (bufferCount == buffer.length)
                    buffer = cachedRanges.resize(buffer, bufferCount, bufferCount + 1 + (bufferCount/2));

                li = (int) (lri);
                ri = (int) (lri >>> 32);

                Range l = covering.ranges[li], r = input.ranges[ri];
                RoutingKey ls = l.start(), rs = r.start(), le = l.end(), re = r.end();
                int cs = rs.compareTo(ls), ce = re.compareTo(le);
                if (cs >= 0 && ce <= 0)
                {
                    buffer[bufferCount++] = r;
                    ++ri;
                }
                else
                {
                    buffer[bufferCount++] = r.newRange(cs >= 0 ? rs : ls, ce <= 0 ? re : le);
                    if (ce <= 0) ++ri;
                }
                if (ce >= 0) li++; // le <= re
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

    static <C extends AbstractRanges, P, O> O sliceMaximal(C covering, AbstractRanges input, P param, SliceConstructor<C, P, O> constructor)
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

                if (bufferCount == buffer.length)
                    buffer = cachedRanges.resize(buffer, bufferCount, bufferCount + 1 + (bufferCount/2));

                li = (int) (lri);
                ri = (int) (lri >>> 32);

                Range l = covering.ranges[li], r = input.ranges[ri]; // l(eft), r(right)
                RoutingKey ls = l.start(), rs = r.start(), le = l.end(), re = r.end(); // l(eft),r(ight) s(tart),e(nd)
                int cs = rs.compareTo(ls), ce = re.compareTo(le); // c(ompare) s(tart),e(nd)
                if (cs <= 0 && ce >= 0)
                {
                    buffer[bufferCount++] = r;
                }
                else
                {
                    buffer[bufferCount++] = r.newRange(cs <= 0 ? rs : ls, ce >= 0 ? re : le);
                }
                ++li;
                ++ri;
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
     * TODO (low priority, efficiency): better support for merging runs of overlapping or adjacent ranges
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
                // TODO (easy, efficiency): use exponentialSearch
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

    interface UnionConstructor<P1, P2, RS>
    {
        RS construct(P1 param1, P2 param2, Range[] ranges);
    }

    public enum UnionMode { MERGE_ADJACENT, MERGE_OVERLAPPING }

    /**
     * @return the union of {@code left} and {@code right}, returning one of the two inputs if possible
     */
    static <P1, P2, RS> RS union(UnionMode mode, AbstractRanges left, AbstractRanges right, P1 p1, P2 p2, UnionConstructor<P1, P2, RS> constructor)
    {
        switch (mode)
        {
            default: throw new UnhandledEnum(mode);
            case MERGE_ADJACENT:
            {
                if (left == right || right.isEmpty()) return mergeTouching(left.ranges, p1, p2, constructor);
                if (left.isEmpty()) return mergeTouching(right.ranges, p1, p2, constructor);

                return unionInternal(left.ranges, right.ranges, 0, 0, -1, p1, p2, constructor);
            }
            case MERGE_OVERLAPPING:
            {
                if (left == right || right.isEmpty()) return constructor.construct(p1, p2, left.ranges);
                if (left.isEmpty()) return constructor.construct(p1, p2, right.ranges);
                Range[] as = left.ranges, bs = right.ranges;
                int c = as[0].start().compareTo(bs[0].start());
                if (c > 0 || c == 0 && as[as.length - 1].end().compareTo(bs[bs.length - 1].end()) < 0)
                {
                    Range[] tmp = as; as = bs; bs = tmp;
                }

                int ai, bi; {
                    long tmp = supersetLinearMerge(as, bs);
                    ai = (int)(tmp >>> 32);
                    bi = (int)tmp;
                }

                if (bi == bs.length)
                    return constructor.construct(p1, p2, (as == left.ranges ? left : right).ranges);

                return unionInternal(as, bs, ai, bi, 0, p1, p2, constructor);
            }
        }
    }

    /**
     * @return the union of {@code left} and {@code right}, returning one of the two inputs if possible
     */
    private static <P1, P2, RS> RS unionInternal(Range[] as, Range[] bs, int ai, int bi, int cmp, P1 p1, P2 p2, UnionConstructor<P1, P2, RS> constructor)
    {
        Range[] result = cachedRanges().get(as.length + bs.length - bi);
        int resultCount = 0;
        if (ai > 0)
        {
            if (ai >= as.length)
                --ai;
            System.arraycopy(as, 0, result, 0, resultCount = ai);
        }
        Range prev = as[ai].start().compareTo(bs[bi].start()) <= 0 ? as[ai++] : bs[bi++];
        while (ai < as.length || bi < bs.length)
        {
            Range next;
            if (ai == as.length) next = bs[bi++];
            else if (bi == bs.length) next = as[ai++];
            else
            {
                int c = as[ai].start().compareTo(bs[bi].start());
                if (c < 0) next = as[ai++];
                else if (c > 0) next = bs[bi++];
                else if (as[ai].end().compareTo(bs[bi].end()) >= 0) next = as[ai++];
                else next = bs[bi++];
            }

            if (prev.end().compareTo(next.start()) <= cmp)
            {
                result[resultCount++] = prev;
                prev = next;
            }
            else if (next.end().compareTo(prev.end()) > 0)
            {
                prev = prev.newRange(prev.start(), next.end());
            }
        }
        result[resultCount++] = prev;

        if (resultCount == as.length && Arrays.equals(as, 0, as.length, result, 0, resultCount))
        {
            cachedRanges().forceDiscard(result, resultCount);
            return constructor.construct(p1, p2, as);
        }
        if (resultCount == bs.length && Arrays.equals(bs, 0, bs.length, result, 0, resultCount))
        {
            cachedRanges().forceDiscard(result, resultCount);
            return constructor.construct(p1, p2, bs);
        }
        result = cachedRanges().completeAndDiscard(result, resultCount);
        return constructor.construct(p1, p2, result);
    }

    @Override
    public String toString()
    {
        if (isEmpty()) return "[]";
        if (ranges[0].start().prefix() == null)
            return Arrays.toString(ranges);

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        int i = 0;
        while (i < ranges.length)
        {
            if (i > 0) sb.append(", ");
            Object prefix = ranges[i].start().prefix();
            int j = i + 1;
            while (j < ranges.length && prefix.equals(ranges[j].end().prefix()))
                ++j;
            sb.append(prefix);
            sb.append(':');
            sb.append('[');
            while (i < j)
            {
                sb.append(ranges[i++].toSuffixString());
                if (i < j) sb.append(", ");
            }
            sb.append(']');
        }
        sb.append(']');
        return sb.toString();
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
        return Arrays.equals(this.ranges, ((AbstractRanges) that).ranges);
    }

    @Override
    public Iterator<Range> iterator()
    {
        return Iterators.forArray(ranges);
    }

    static <RS extends AbstractRanges> RS mergeTouching(RS input, Function<Range[], RS> constructor)
    {
        return mergeTouching(input.ranges, input, constructor, (in, c, out) -> out == in.ranges ? in : c.apply(out));
    }

    static <RS, P1, P2> RS mergeTouching(Range[] ranges, P1 p1, P2 p2, UnionConstructor<P1, P2, RS> constructor)
    {
        if (ranges.length == 0)
            return constructor.construct(p1, p2, ranges);

        ObjectBuffers<Range> cachedRanges = cachedRanges();
        Range[] buffer = cachedRanges.get(ranges.length);
        try
        {
            int count = copyAndMergeTouching(ranges, 0, buffer, 0, ranges.length);
            if (count == ranges.length)
                return constructor.construct(p1, p2, ranges);
            Range[] result = cachedRanges.completeAndDiscard(buffer, count);
            return constructor.construct(p1, p2, result);
        }
        catch (Throwable t)
        {
            cachedRanges.forceDiscard(buffer, ranges.length);
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
        return withEnd == range.end() ? range : range.newRange(range.start(), withEnd);
    }

    static <RS extends AbstractRanges> RS of(Function<Range[], RS> constructor, Range... ranges)
    {
        if (ranges.length == 0)
            return constructor.apply(NO_RANGES);

        return sortAndDeoverlap(constructor, ranges, ranges.length, UnionMode.MERGE_OVERLAPPING);
    }

    static <RS extends AbstractRanges> RS sortAndDeoverlap(Function<Range[], RS> constructor, Range[] ranges, int count, UnionMode mode)
    {
        boolean copyOnWrite = true;
        if (count > 1 && !isSorted(ranges, Range::compareTo))
        {
            ranges = ranges.clone();
            Arrays.sort(ranges, 0, count, Range::compare);
            copyOnWrite = false;
        }

        return deoverlapSorted(constructor, ranges, count, mode, copyOnWrite);
    }

    static <RS extends AbstractRanges> RS deoverlapSorted(Function<Range[], RS> constructor, Range[] ranges, int count, UnionMode mode)
    {
        return deoverlapSorted(constructor, ranges, count, mode, true);
    }

    private static <RS extends AbstractRanges> RS deoverlapSorted(Function<Range[], RS> constructor, Range[] ranges, int count, UnionMode mode, boolean copyOnWrite)
    {
        if (count == 0)
            return constructor.apply(NO_RANGES);

        if (count == 1)
        {
            if (ranges.length == 1)
                return constructor.apply(ranges);

            return constructor.apply(Arrays.copyOf(ranges, count));
        }

        Range prev = ranges[0];
        Range[] out = null;
        int removed = 0;
        int compareTo = mode == UnionMode.MERGE_OVERLAPPING ? 1 : 0;
        for (int i = 1 ; i < count ; ++i)
        {
            Range next = ranges[i];
            if (prev.end().compareTo(next.start()) >= compareTo)
            {
                RoutingKey end = max(prev.end(), next.end());
                boolean copy = removed == 0;
                ++removed;
                while (++i < count && end.compareTo((next = ranges[i]).start()) >= compareTo)
                {
                    end = max(end, next.end());
                    ++removed;
                }

                if (copy)
                {
                    if (i == count && end == prev.end())
                        break;

                    if (copyOnWrite)
                    {
                        out = new Range[count - removed];
                        System.arraycopy(ranges, 0, out, 0, i - removed);
                    }
                    else
                    {
                        out = ranges;
                    }
                }

                if (end != prev.end())
                {
                    prev = prev.newRange(prev.start(), end);
                    out[i - (1 + removed)] = prev;
                }

                if (i < count)
                    out[i - removed] = prev = next;
                continue;
            }

            if (removed > 0)
                out[i - removed] = next;
            prev = next;
        }

        if (out == null) out = ranges;
        count -= removed;
        if (count != out.length)
            out = Arrays.copyOf(out, count);
        return constructor.apply(out);
    }

    private static RoutingKey max(RoutingKey a, RoutingKey b)
    {
        return a.compareTo(b) >= 0 ? a : b;
    }

    static <RS extends AbstractRanges> RS ofSortedAndDeoverlapped(Function<Range[], RS> constructor, Range... ranges)
    {
        Invariants.requireArgument(ranges.length == 0 || ranges[0] != null);
        for (int i = 1 ; i < ranges.length ; ++i)
        {
            if (ranges[i - 1].end().compareTo(ranges[i].start()) > 0)
                throw illegalArgument(Arrays.toString(ranges) + " is not correctly sorted or deoverlapped");
        }

        return constructor.apply(ranges);
    }

    @Inline
    public final long foldl(AbstractRanges intersect, IndexedFoldToLong<Range> fold, long param, long accumulator, long terminalValue)
    {
        return Routables.foldl(this, intersect, fold, param, accumulator, terminalValue);
    }

    @Inline
    public final <P1, P2, V> V foldl(AbstractRanges intersect, IndexedTriFold<P1, P2, Range, V> fold, P1 p1, P2 p2, V accumulator)
    {
        return Routables.foldl(this, intersect, fold, p1, p2, accumulator, i -> false);
    }
}