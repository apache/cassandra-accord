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
import accord.utils.IndexedFoldToLong;
import accord.utils.IndexedTriFold;
import net.nicoulaj.compilecommand.annotations.Inline;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;
import static accord.primitives.Routables.Slice.Overlapping;

public class Ranges extends AbstractRanges implements Iterable<Range>, Seekables<Range, Ranges>, Unseekables<Range>, Participants<Range>
{
    public static final Ranges EMPTY = new Ranges(new Range[0]);

    private Ranges(@Nonnull Range[] ranges)
    {
        super(ranges);
    }

    public static Ranges of(Range... ranges)
    {
        return AbstractRanges.of(Ranges::ofSortedAndDeoverlappedUnchecked, ranges);
    }

    public static Ranges ofSorted(Range... ranges)
    {
        return AbstractRanges.deoverlapSorted(Ranges::ofSortedAndDeoverlappedUnchecked, ranges, ranges.length, MERGE_OVERLAPPING);
    }

    public static Ranges ofSortedAndDeoverlapped(Range... ranges)
    {
        return AbstractRanges.ofSortedAndDeoverlapped(Ranges::ofSortedAndDeoverlappedUnchecked, ranges);
    }

    static Ranges ofSortedAndDeoverlappedUnchecked(Range... ranges)
    {
        if (ranges.length == 0)
            return EMPTY;

        return new Ranges(ranges);
    }

    public static Ranges single(Range range)
    {
        return new Ranges(new Range[]{range});
    }

    public Ranges select(int[] indexes)
    {
        Range[] selection = new Range[indexes.length];
        for (int i=0; i<indexes.length; i++)
            selection[i] = ranges[indexes[i]];
        return ofSortedAndDeoverlapped(Ranges::ofSortedAndDeoverlappedUnchecked, selection);
    }

    public Stream<Range> stream()
    {
        return Stream.of(ranges);
    }

    @Override
    public final Ranges slice(Ranges ranges)
    {
        return slice(ranges, Overlapping);
    }

    @Override
    public final Ranges slice(int from, int to)
    {
        if (from == 0 && to == size())
            return this;
        return Ranges.ofSortedAndDeoverlapped(Arrays.copyOfRange(ranges, from, to));
    }

    @Override
    public final Ranges slice(Ranges ranges, Slice slice)
    {
        return slice(ranges, slice, this, null, (i1, i2, rs) -> i1.ranges == rs ? i1 : Ranges.ofSortedAndDeoverlapped(rs));
    }

    private Ranges slice(AbstractRanges ranges, Slice slice)
    {
        return slice(ranges, slice, this, this, (i1, i2, rs) -> i2.ranges == rs ? i2 : Ranges.ofSortedAndDeoverlapped(rs));
    }

    @Override
    public Ranges intersecting(Unseekables<?> intersecting)
    {
        return intersecting(intersecting, Overlapping);
    }

    @Override
    public final Ranges intersecting(Unseekables<?> intersecting, Slice slice)
    {
        switch (intersecting.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + intersecting.domain());
            case Range: return slice((AbstractRanges) intersecting, slice);
            case Key: return intersecting((AbstractUnseekableKeys) intersecting, this, null, (i1, i2, rs) -> i1.ranges == rs ? i1 : new Ranges(rs));
        }
    }

    @Override
    public Ranges with(Unseekables<Range> with)
    {
        return with((AbstractRanges) with);
    }

    @Override
    public Participants<Range> with(Participants<Range> with)
    {
        return with((AbstractRanges) with);
    }

    @Override
    public Ranges with(Ranges that)
    {
        return union(MERGE_OVERLAPPING, that);
    }

    public Ranges with(AbstractRanges that)
    {
        return union(MERGE_OVERLAPPING, that);
    }

    @Override
    public Unseekables<Range> with(RoutingKey withKey)
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
    public Ranges toParticipants()
    {
        return this;
    }

    @Override
    public Ranges toRanges()
    {
        return this;
    }

    @Override
    public FullRangeRoute toRoute(RoutingKey homeKey)
    {
        if (!contains(homeKey))
            throw new IllegalArgumentException("Home key must be contained in the route: " + homeKey + " not in " + this);

        return new FullRangeRoute(homeKey, ranges);
    }

    public Ranges union(UnionMode mode, Ranges that)
    {
        return union(mode, this, that, this, that, (left, right, ranges) -> {
            if (ranges == left.ranges) return left;
            if (ranges == right.ranges) return right;
            return Ranges.ofSortedAndDeoverlapped(ranges);
        });
    }

    public Ranges union(UnionMode mode, AbstractRanges that)
    {
        return union(mode, this, that, this, that, (left, right, ranges) -> {
            if (ranges == left.ranges) return left;
            return Ranges.ofSortedAndDeoverlapped(ranges);
        });
    }

    public Ranges mergeTouching()
    {
        return mergeTouching(this, Ranges::ofSortedAndDeoverlapped);
    }

    public Map<Boolean, Ranges> partitioningBy(Predicate<? super Range> test)
    {
        if (isEmpty())
            return Collections.emptyMap();
        List<Range> trues = new ArrayList<>();
        List<Range> falses = new ArrayList<>();
        for (Range range : this)
            (test.test(range) ? trues : falses).add(range);
        if (trues.isEmpty()) return ImmutableMap.of(Boolean.FALSE, this);
        if (falses.isEmpty()) return ImmutableMap.of(Boolean.TRUE, this);
        return ImmutableMap.of(Boolean.TRUE, Ranges.ofSortedAndDeoverlapped(trues.toArray(new Range[0])),
                               Boolean.FALSE, Ranges.ofSortedAndDeoverlapped(falses.toArray(new Range[0])));
    }

    @Inline
    public final long foldl(Ranges intersect, IndexedFoldToLong<Range> fold, long param, long accumulator, long terminalValue)
    {
        return Routables.foldl(this, intersect, fold, param, accumulator, terminalValue);
    }

    @Inline
    public final <P1, P2, V> V foldl(Ranges intersect, IndexedTriFold<P1, P2, Range, V> fold, P1 p1, P2 p2, V accumulator)
    {
        return Routables.foldl(this, intersect, fold, p1, p2, accumulator, i -> false);
    }
}
