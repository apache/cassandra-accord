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

import accord.api.RoutingKey;
import accord.utils.Invariants;

import static accord.primitives.AbstractRanges.UnionMode.MERGE_OVERLAPPING;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Invariants.illegalState;

/**
 * A slice of a Route that covers
 */
public class PartialRangeRoute extends RangeRoute implements PartialRoute<Range>
{
    public static class SerializationSupport
    {
        public static PartialRangeRoute create(RoutingKey homeKey, Range[] ranges)
        {
            return new PartialRangeRoute(homeKey, ranges);
        }
    }

    public PartialRangeRoute(RoutingKey homeKey, Range[] ranges)
    {
        super(homeKey, ranges);
    }

    @Override
    public UnseekablesKind kind()
    {
        return UnseekablesKind.PartialRangeRoute;
    }

    @Override
    public Route<Range> with(Participants<Range> that)
    {
        Unseekables.UnseekablesKind kind = that.kind();
        switch (kind)
        {
            default: throw new AssertionError("Unhandled kind: " + kind);
            case FullKeyRoute:
            case PartialKeyRoute:
            case RoutingKeys:
                throw illegalState("Incompatible route/participants: %s vs %s", kind(), kind);

            case FullRangeRoute:
                return (FullRangeRoute) that;

            case PartialRangeRoute:
                return union((PartialRangeRoute) that);

            case RoutingRanges:
                return union((AbstractRanges) that);
        }
    }

    @Override
    public Route<Range> slice(int from, int to)
    {
        if (from == 0 && to == size())
            return this;
        return new PartialRangeRoute(homeKey, Arrays.copyOfRange(ranges, from, to));
    }

    @Override
    public PartialRangeRoute with(PartialRoute<Range> that)
    {
        if (!(that instanceof PartialRangeRoute))
            throw illegalArgument("Unexpected PartialRoute<Range> type: " + (that == null ? null : that.getClass()));

        return union((PartialRangeRoute) that);
    }

    public PartialRangeRoute union(PartialRangeRoute that)
    {
        Invariants.checkState(homeKey.equals(that.homeKey));
        return union(MERGE_OVERLAPPING, this, that, this, that, (in1, in2, ranges) -> {
            if (in1.ranges == ranges) return in1;
            if (in2.ranges == ranges) return in2;
            return new PartialRangeRoute(in1.homeKey, ranges);
        });
    }

    public PartialRangeRoute union(AbstractRanges that)
    {
        return union(MERGE_OVERLAPPING, this, that, this, homeKey, (in, homeKey, ranges) -> in.ranges == ranges ? in : new PartialRangeRoute(homeKey, ranges));
    }
}
