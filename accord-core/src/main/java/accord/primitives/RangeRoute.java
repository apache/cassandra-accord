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

public abstract class RangeRoute extends AbstractRanges<Route<Range>> implements Route<Range>
{
    public final RoutingKey homeKey;
    public final boolean isParticipatingHomeKey;

    RangeRoute(@Nonnull RoutingKey homeKey, boolean isParticipatingHomeKey, Range[] ranges)
    {
        super(ranges);
        this.homeKey = Invariants.nonNull(homeKey);
        this.isParticipatingHomeKey = isParticipatingHomeKey;
        Invariants.checkArgument(isParticipatingHomeKey || !contains(homeKey) || get(indexOf(homeKey)).equals(homeKey.asRange()));
    }

    @Override
    public Unseekables<Range, ?> with(Unseekables<Range, ?> with)
    {
        if (isEmpty())
            return with;

        return union(MERGE_OVERLAPPING, this, (AbstractRanges<?>) with, null, null,
                (left, right, rs) -> Ranges.ofSortedAndDeoverlapped(rs));
    }

    @Override
    public Unseekables<Range, ?> with(RoutingKey withKey)
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
        if (isParticipatingHomeKey())
            return intersects(ranges);

        long ij = findNextIntersection(0, ranges, 0);
        if (ij < 0)
            return false;

        int i = (int)ij;
        if (!get(i).contains(homeKey))
            return true;

        Invariants.checkState(get(i).equals(homeKey.asRange()));
        int j = (int)(ij >>> 32);
        return findNextIntersection(i + 1, ranges, j) >= 0;
    }

    @Override
    public PartialRangeRoute slice(Ranges ranges)
    {
        return slice(ranges, Overlapping, this, homeKey,
                     isParticipatingHomeKey ? PartialRangeRoute::withParticipatingHomeKey
                                            : PartialRangeRoute::withNonParticipatingHomeKey);
    }

    @Override
    public Unseekables<Range, ?> participants()
    {
        if (isParticipatingHomeKey || !contains(homeKey))
            return this;

        // TODO (desired): efficiency (lots of unnecessary allocations)
        return ranges().subtract(Ranges.of(homeKey().asRange()));
    }

    public Ranges ranges()
    {
        return Ranges.ofSortedAndDeoverlapped(ranges);
    }

    @Override
    public RoutingKey homeKey()
    {
        return homeKey;
    }

    @Override
    public boolean isParticipatingHomeKey()
    {
        return isParticipatingHomeKey;
    }

    @Override
    public boolean equals(Object that)
    {
        return super.equals(that) && homeKey.equals(((RangeRoute)that).homeKey);
    }

}
