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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import accord.api.RoutingKey;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;
import static accord.primitives.Routables.Slice.Overlapping;
import static accord.utils.Utils.toArray;

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
        if (that == this)
            return construct(NO_RANGES);

        List<Range> result = new ArrayList<>(this.size() + that.size());
        int thatIdx = 0;

        for (int thisIdx=0; thisIdx<this.size(); thisIdx++)
        {
            Range thisRange = this.ranges[thisIdx];
            while (thatIdx < that.size())
            {
                Range thatRange = that.ranges[thatIdx];

                int cmp = thisRange.compareIntersecting(thatRange);
                if (cmp > 0)
                {
                    thatIdx++;
                    continue;
                }
                if (cmp < 0) break;

                int scmp = thisRange.start().compareTo(thatRange.start());
                int ecmp = thisRange.end().compareTo(thatRange.end());

                if (scmp < 0)
                    result.add(thisRange.newRange(thisRange.start(), thatRange.start()));

                if (ecmp <= 0)
                {
                    thisRange = null;
                    break;
                }
                else
                {
                    thisRange = thisRange.newRange(thatRange.end(), thisRange.end());
                    thatIdx++;
                }
            }
            if (thisRange != null)
                result.add(thisRange);
        }
        return construct(toArray(result, Range[]::new));
    }
}
