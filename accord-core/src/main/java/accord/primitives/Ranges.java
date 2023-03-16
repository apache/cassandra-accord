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

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;
import static accord.primitives.Routables.Slice.Overlapping;
import static accord.utils.ArrayBuffers.cachedRanges;
import static accord.utils.SortedArrays.Search.CEIL;

public class Ranges extends AbstractRanges<Ranges> implements Iterable<Range>, Seekables<Range, Ranges>, Unseekables<Range, Ranges>
{
    public static final Ranges EMPTY = ofSortedAndDeoverlappedUnchecked();

    Ranges(@Nonnull Range[] ranges)
    {
        super(ranges);
    }

    public static Ranges of(Range... ranges)
    {
        return AbstractRanges.of(Ranges::construct, ranges);
    }

    public static Ranges ofSortedAndDeoverlapped(Range... ranges)
    {
        return AbstractRanges.ofSortedAndDeoverlapped(Ranges::construct, ranges);
    }

    static Ranges ofSortedAndDeoverlappedUnchecked(Range... ranges)
    {
        return new Ranges(ranges);
    }

    public static Ranges single(Range range)
    {
        return new Ranges(new Range[]{range});
    }

    private static Ranges construct(Range[] ranges)
    {
        if (ranges.length == 0)
            return EMPTY;

        return new Ranges(ranges);
    }

    public Ranges select(int[] indexes)
    {
        Range[] selection = new Range[indexes.length];
        for (int i=0; i<indexes.length; i++)
            selection[i] = ranges[indexes[i]];
        return ofSortedAndDeoverlapped(Ranges::construct, selection);
    }

    public Stream<Range> stream()
    {
        return Stream.of(ranges);
    }

    @Override
    public Ranges slice(Ranges ranges)
    {
        return slice(ranges, Overlapping);
    }

    public Ranges intersecting(Routables<?, ?> keysOrRanges)
    {
        return intersecting(this, keysOrRanges, this, (i1, i2, rs) -> i2.ranges == rs ? i2 : new Ranges(rs));
    }

    @Override
    public Ranges with(Unseekables<Range, ?> that)
    {
        return with((AbstractRanges<?>) that);
    }

    public Ranges with(Ranges that)
    {
        return union(MERGE_OVERLAPPING, that);
    }

    public Ranges with(AbstractRanges<?> that)
    {
        return union(MERGE_OVERLAPPING, that);
    }

    @Override
    public Unseekables<Range, ?> with(RoutingKey withKey)
    {
        if (contains(withKey))
            return this;

        return with(Ranges.of(withKey.asRange()));
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.RoutingRanges;
    }

    @Override
    public Ranges toUnseekables()
    {
        return this;
    }

    @Override
    public FullRangeRoute toRoute(RoutingKey homeKey)
    {
        if (!contains(homeKey))
            return with(Ranges.of(homeKey.asRange())).toRoute(homeKey);
        return new FullRangeRoute(homeKey, ranges);
    }

    public Ranges union(UnionMode mode, Ranges that)
    {
        return union(mode, this, that, this, that, (left, right, ranges) -> {
            if (ranges == left.ranges) return left;
            if (ranges == right.ranges) return right;
            return new Ranges(ranges);
        });
    }

    public Ranges union(UnionMode mode, AbstractRanges<?> that)
    {
        return union(mode, this, that, this, that, (left, right, ranges) -> {
            if (ranges == left.ranges) return left;
            return new Ranges(ranges);
        });
    }

    public Ranges mergeTouching()
    {
        return mergeTouching(this, Ranges::new);
    }

    /**
     * Subtracts the given set of ranges from this
     */
    public Ranges difference(AbstractRanges<?> that)
    {
        if (that.isEmpty())
            return this;

        if (isEmpty() || that == this)
            return EMPTY;

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
            return EMPTY;

        return construct(cachedRanges.completeAndDiscard(result, count));
    }

}
