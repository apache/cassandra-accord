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
    public Unseekables<Range> with(Unseekables<Range> with)
    {
        if (isEmpty())
            return with;

        return merge((AbstractRanges) with);
    }

    @Override
    public Participants<Range> with(Participants<Range> with)
    {
        if (isEmpty())
            return with;

        return merge((AbstractRanges) with);
    }

    private Ranges merge(AbstractRanges with)
    {
        return union(MERGE_OVERLAPPING, this, with, null, null,
                (left, right, rs) -> Ranges.ofSortedAndDeoverlapped(rs));
    }

    @Override
    public Unseekables<Range> with(RoutingKey withKey)
    {
        if (withKey.equals(homeKey))
            return withHomeKey();

        if (contains(withKey))
            return this;

        return with(Ranges.of(withKey.asRange()));
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
    public boolean equals(Object that)
    {
        return super.equals(that) && homeKey.equals(((RangeRoute)that).homeKey);
    }

}
