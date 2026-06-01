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

package accord.local.durability;

import javax.annotation.Nullable;

import accord.coordinate.FailureAccumulator;
import accord.local.Node;
import accord.primitives.MinimalSyncPoint;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.SortedArrays.SortedArrayList;

public class DurabilityResult
{
    public final TxnId syncId;
    public final Ranges ranges;
    public final ReducingRangeMap<DurabilityLevel> achieved;
    public final DurabilityLevel min;
    public final SortedArrayList<Node.Id> including;
    public final @Nullable SortedArrayList<Node.Id> readable;
    public final Throwable failure;

    public DurabilityResult(MinimalSyncPoint syncPoint, DurabilityLevel result, SortedArrayList<Node.Id> including, SortedArrayList<Node.Id> readable, Throwable failure)
    {
        this(syncPoint.syncId, syncPoint.route.toRanges(), result, including, readable, failure);
    }

    public DurabilityResult(TxnId syncId, Ranges ranges, DurabilityLevel result, SortedArrayList<Node.Id> including, SortedArrayList<Node.Id> readable, Throwable failure)
    {
        this(syncId, ranges, ReducingRangeMap.create(ranges, result), including, readable, failure);
    }

    public DurabilityResult(MinimalSyncPoint syncPoint, ReducingRangeMap<DurabilityLevel> achieved, SortedArrayList<Node.Id> including, SortedArrayList<Node.Id> readable, Throwable failure)
    {
        this(syncPoint.syncId, syncPoint.route.toRanges(), achieved, including, readable, failure);
    }

    public DurabilityResult(TxnId syncId, Ranges ranges, ReducingRangeMap<DurabilityLevel> achieved, SortedArrayList<Node.Id> including, SortedArrayList<Node.Id> readable, Throwable failure)
    {
        this.syncId = syncId;
        this.ranges = ranges;
        this.achieved = achieved;
        this.min = achieved.foldl(DurabilityLevel::min);
        this.including = including;
        this.readable = readable;
        this.failure = failure;
    }

    public DurabilityResult min(DurabilityResult that)
    {
        Invariants.require(this.syncId.equals(that.syncId));
        Throwable failure = this.failure == null ? that.failure
                                                 : that.failure == null ? this.failure
                                                                        : FailureAccumulator.append(this.failure, that.failure);
        SortedArrayList<Node.Id> including = this.including.intersecting(that.including);
        SortedArrayList<Node.Id> readable = SortedArrayList.intersection(this.readable, that.readable);
        return new DurabilityResult(syncId, ranges,
                                    ReducingRangeMap.merge(this.achieved, that.achieved, DurabilityLevel::min),
                                    including, readable, failure);
    }

    public DurabilityResult max(DurabilityResult that)
    {
        Invariants.require(this.syncId.equals(that.syncId));
        Throwable failure = this.failure == null || that.failure == null ? null : FailureAccumulator.append(this.failure, that.failure);
        SortedArrayList<Node.Id> including = this.including.with(that.including);
        SortedArrayList<Node.Id> readable = SortedArrayList.union(this.readable, that.readable);
        return new DurabilityResult(syncId, ranges,
                                    ReducingRangeMap.merge(this.achieved, that.achieved, DurabilityLevel::max),
                                    including, readable, failure);
    }

    @Override
    public String toString()
    {
        return syncId + " achieved " + achieved;
    }

    public Ranges satisfies(DurabilityLevel require)
    {
        if (require.isSatisfiedBy(min))
            return ranges;

        return achieved.foldlWithBounds((l, rs, s, e) -> {
            if (require.isSatisfiedBy(l))
                rs = rs.with(Ranges.of(s.rangeFactory().newRange(s, e)));
            return rs;
        }, Ranges.EMPTY);
    }
}
