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

import javax.annotation.Nonnull;

import accord.api.RoutingKey;
import accord.utils.Invariants;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;

/**
 * A slice of a Route that covers
 */
public class PartialRangeRoute extends RangeRoute implements PartialRoute<Range>
{
    public static class SerializationSupport
    {
        public static PartialRangeRoute create(Ranges covering, RoutingKey homeKey, boolean isParticipatingHomeKey, Range[] ranges)
        {
            return new PartialRangeRoute(covering, homeKey, isParticipatingHomeKey, ranges);
        }
    }

    public final Ranges covering;

    public PartialRangeRoute(Ranges covering, RoutingKey homeKey, boolean isParticipatingHomeKey, Range[] ranges)
    {
        super(homeKey, isParticipatingHomeKey, ranges);
        this.covering = covering;
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.PartialRangeRoute;
    }

    @Override
    public Ranges covering()
    {
        return covering;
    }

    @Override
    public boolean covers(Ranges ranges)
    {
        return covering.containsAll(ranges);
    }

    @Override
    public PartialRangeRoute sliceStrict(Ranges newRanges)
    {
        if (!covering.containsAll(newRanges))
            throw new IllegalArgumentException("Not covered");

        return slice(newRanges);
    }

    @Override
    public PartialRangeRoute withHomeKey()
    {
        if (contains(homeKey))
            return this;

        Ranges with = Ranges.of(homeKey.asRange());
        return new PartialRangeRoute(covering.with(with), homeKey, isParticipatingHomeKey, union(MERGE_OVERLAPPING, this, with, null, null, (i1, i2, rs) -> rs));
    }

    @Override
    public Route<Range> union(Route<Range> that)
    {
        if (Route.isFullRoute(that)) return that;
        return union((PartialRangeRoute) that);
    }

    @Override
    public PartialRangeRoute union(PartialRoute<Range> with)
    {
        if (!(with instanceof PartialRangeRoute))
            throw new IllegalArgumentException();

        PartialRangeRoute that = (PartialRangeRoute) with;
        Invariants.checkState(homeKey.equals(that.homeKey));
        Ranges covering = this.covering.with(that.covering);
        if (covering == this.covering) return this;
        else if (covering == that.covering) return that;

        return union(MERGE_OVERLAPPING, this, that, covering, homeKey,
                     isParticipatingHomeKey ? PartialRangeRoute::withParticipatingHomeKey
                                            : PartialRangeRoute::withNonParticipatingHomeKey);
    }

    @Override
    public boolean equals(Object that)
    {
        return super.equals(that) && covering.equals(((PartialRangeRoute)that).covering);
    }

    static PartialRangeRoute withParticipatingHomeKey(Ranges covering, @Nonnull RoutingKey homeKey, Range[] ranges)
    {
        return new PartialRangeRoute(covering, homeKey, true, ranges);
    }

    static PartialRangeRoute withNonParticipatingHomeKey(Ranges covering, @Nonnull RoutingKey homeKey, Range[] ranges)
    {
        return new PartialRangeRoute(covering, homeKey, false, ranges);
    }
}
