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
import accord.utils.Invariants;

import javax.annotation.Nonnull;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;
import static accord.primitives.Routables.Slice.Overlapping;

public abstract class RangeRoute extends AbstractRanges implements Route<Range>, Unseekables<Range>, Participants<Range>
{
    public final RoutingKey homeKey;

    RangeRoute(@Nonnull RoutingKey homeKey, Range[] ranges)
    {
        super(ranges);
        this.homeKey = Invariants.nonNull(homeKey);
    }

    @Override
    public RangeRoute with(Unseekables<Range> with)
    {
        AbstractRanges that = (AbstractRanges) with;
        return AbstractRanges.union(MERGE_OVERLAPPING, this, that, this, that, (p1, p2, rs) -> {
            if (rs == p1.ranges)
                return p1;
            Invariants.checkState(p1.getClass() == PartialRangeRoute.class);
            if (rs == p2.ranges && p2 instanceof RangeRoute)
                return (RangeRoute)p2;
            return new PartialRangeRoute(homeKey, rs);
        });
    }

    @Override
    public RangeRoute with(RoutingKey withKey)
    {
        if (contains(withKey))
            return this;

        return with((Unseekables<Range>) Ranges.of(withKey.asRange()));
    }

    @Override
    public Route<Range> withHomeKey()
    {
        return with(homeKey);
    }

    @Override
    public boolean participatesIn(Ranges ranges)
    {
        return intersects(ranges);
    }

    @Override
    public PartialRangeRoute slice(Ranges ranges)
    {
        return slice(ranges, Overlapping);
    }

    @Override
    public PartialRangeRoute slice(Ranges ranges, Slice slice)
    {
        return slice(ranges, slice, this, homeKey, (ignore, hk, rs) -> new PartialRangeRoute(hk, rs));
    }

    @Override
    public RangeRoute intersecting(Unseekables<?> intersecting)
    {
        return intersecting(intersecting, Overlapping);
    }

    @Override
    public RangeRoute intersecting(Unseekables<?> intersecting, Slice slice)
    {
        switch (intersecting.domain())
        {
            default: throw new AssertionError("Unhandled domain: " + intersecting.domain());
            case Key: return intersecting((AbstractUnseekableKeys)intersecting, this, homeKey, (ignore, hk, ranges) -> new PartialRangeRoute(hk, ranges));
            case Range: return slice((Ranges)intersecting, slice, this, homeKey, (ignore, homeKey, ranges) -> new PartialRangeRoute(homeKey, ranges));
        }
    }

    @Override
    public Participants<Range> participants()
    {
        return this;
    }

    @Override
    public Participants<Range> participants(Ranges slice)
    {
        return participants(slice, Overlapping);
    }

    public Participants<Range> participants(Ranges slice, Slice kind)
    {
        return slice(slice, kind, this, null, (i1, i2, rs) -> i1.ranges == rs ? i1 : Ranges.ofSortedAndDeoverlapped(rs));
    }

    public Ranges toRanges()
    {
        return Ranges.ofSortedAndDeoverlapped(ranges);
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    public Route<Range> homeKeyOnlyRoute()
    {
        Range asRange = homeKey.asRange();
        if (ranges.length == 1 && ranges[0].contains(homeKey) && ranges[0].equals(asRange))
            return this;
        return new PartialRangeRoute(homeKey, new Range[] { asRange });
    }

    @Override
    public boolean isHomeKeyOnlyRoute()
    {
        return ranges.length == 1 && ranges[0].contains(homeKey) && ranges[0].equals(homeKey.asRange());
    }

    @Override
    public boolean equals(Object that)
    {
        return super.equals(that) && homeKey.equals(((RangeRoute)that).homeKey);
    }

}
