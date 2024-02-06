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

package accord.utils;

import java.util.Comparator;

import accord.primitives.Range;
import accord.primitives.RoutableKey;

import static accord.utils.SearchableRangeList.MAX_SCAN_DISTANCE;

/**
 * Builder for {@link SearchableRangeList}
 */
public class SearchableRangeListBuilder extends CheckpointIntervalArrayBuilder<Range[], Range, RoutableKey>
{
    public static final Accessor<Range[], Range, RoutableKey> RANGE_ACCESSOR = new Accessor<>()
    {
        @Override
        public boolean endInclusive(Range[] ranges)
        {
            return ranges[0].endInclusive();
        }

        @Override
        public int size(Range[] ranges)
        {
            return ranges.length;
        }

        @Override
        public Range get(Range[] ranges, int index)
        {
            return ranges[index];
        }

        @Override
        public RoutableKey start(Range[] ranges, int index)
        {
            return ranges[index].start();
        }

        @Override
        public RoutableKey start(Range range)
        {
            return range.start();
        }

        @Override
        public RoutableKey end(Range[] ranges, int index)
        {
            return ranges[index].end();
        }

        @Override
        public RoutableKey end(Range range)
        {
            return range.end();
        }

        @Override
        public Comparator<RoutableKey> keyComparator()
        {
            return Comparator.naturalOrder();
        }

        @Override
        public int binarySearch(Range[] ranges, int from, int to, RoutableKey find, AsymmetricComparator<RoutableKey, Range> comparator, SortedArrays.Search op)
        {
            return SortedArrays.binarySearch(ranges, from, to, find, comparator, op);
        }
    };

    public SearchableRangeListBuilder(Range[] ranges, Strategy strategy, Links links)
    {
        super(RANGE_ACCESSOR, ranges, strategy, links);
    }

    public SearchableRangeListBuilder(Range[] ranges, int goalScanDistance, Strategy strategy, Links links)
    {
        super(RANGE_ACCESSOR, ranges, goalScanDistance, strategy, links);
        Invariants.checkArgument(goalScanDistance <= MAX_SCAN_DISTANCE);
    }

    @Override
    public SearchableRangeList build()
    {
        return build((ranges, bounds, headers, lists, maxScanAndCheckpointMatches) ->
                     new SearchableRangeList(ranges, bounds, headers, lists, maxScanAndCheckpointMatches));
    }
}
