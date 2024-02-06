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

import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.utils.CheckpointIntervalArrayBuilder.Links;
import accord.utils.CheckpointIntervalArrayBuilder.Strategy;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.utils.CheckpointIntervalArrayBuilder.Links.LINKS;
import static accord.utils.CheckpointIntervalArrayBuilder.Strategy.ACCURATE;
import static accord.utils.SortedArrays.Search.*;

/**
 * Based on CINTIA, the Checkpoint INTerval Array
 * [Transactions on Big Data; Ruslan Mavlyutov and Philippe Cudre-Mauroux of U. of Fribourg]
 * <p>
 * CINTIA is a very simple augmentation of a sorted list of intervals that permits efficient intersection queries.
 * The key insight is to augment the list of intervals (sorted by their start bound) with periodic checkpoints
 * containing a list of the intervals that remain open from an earlier point in the list.
 * The sorted interval list can easily return all intersections whose start overlaps a provided interval, and a
 * backwards walk through the list to the prior checkpoint, plus a walk of the checkpoint, can answer all intersections
 * that started prior, but whose ends overlap the query interval.
 * <p>
 * Besides this key insight, which honestly isn't that different to Cassandra packing active range tombstone bounds
 * into index files, we largely take our own approach.
 * <p>
 * <ul>
 * <li> We sort the intervals by start then end bound, so that we may easily find all overlaps that <i>start</i> in a queried range
 * <li> Checkpoints are maintained in an auxiliary structure, keyed by the index of the first range they cover so that
 *      we have good locality and may perform fast binary search on the lower range index we find in step one
 * <li> By being maintained separately we may dynamically decide when to insert a checkpoint.
 *      This potentially permits a single checkpoint to cover a large area.
 * <li> Our scan runs for some fixed distance from the first overlapping range we find by simple binary search,
 *      with each checkpoint defining the scan distance. This permits checkpoints to cover longer regions, and
 *      also to avoid serializing ranges that occur before the checkpoint but that are easily scanned.
 * <li> We write our dynamic checkpoints based on some simple maths that yield linear space overhead and O(k)
 *      additional work where k is the number of overlapping ranges with any given point
 *   <ul>
 *     <li> We perform a greedy iteration of the ranges, with some initial {@code goalScanDistance}; any range that can be found
 *          from every overlapping range index by a backwards scan shorter than this will record only the actual distance required.
 *     <li> Other ranges are "tenured" and will trigger a checkpoint to be written either immediately (if the last checkpoint was
 *          long ago enough), or else within the maximum scan distance from the first location the range was first encountered
 *     <li> The maximum scan distance is proportional to the larger of: the logarithm of the total number of ranges in the collection,
 *          and the number of tenured ranges written to the last checkpoint, discounted by the number of ranges since removed.
 *          This guarantees O(lg(N)+k) where k is the size of the result set for a query, while ensuring our O(N) space requirements.
 *          Essentially, we require that the distance between two checkpoints be at least as large as the number of duplicate
 *          elements we may serialize, so we can write at most O(N) duplicate entries, and we can write unique entries at most
 *          once, i.e. O(N) in total. Since we sort the checkpoints in descending order, we will never visit more than O(k)
 *          entries, where k is the number of matches. The number of scanned entries is similarly capped by the same logic;
 *          for each entry we remove from the tenured collection we bring our threshold for writing a new checkpoint down,
 *          so that by the time we are permitted to write a checkpoint we must have fewer tenured entries than we may scan.
 *          TODO (low priority, efficiency): probably we want to take the maximum of the current tenured.size() and the
 *           discounted minimum span when deciding IF we want to write a new checkpoint, even if our complexity permits
 *           writing solely based on the discounted minimum span.
 *    </ul>
 *    <li>We additionally permit our checkpoints to reference earlier checkpoints whose end bounds are still open.
 *        We permit this to a depth of at most one, i.e. we do not transitively explore checkpoints referenced by
 *        earlier checkpoints.
 * </ul>
 */
public class SearchableRangeList extends CheckpointIntervalArray<Range[], Range, RoutableKey>
{
    private static final SearchableRangeList EMPTY_CHECKPOINTS = new SearchableRangeList(new Range[0], new int[0], new int[] { 0, 0 }, new int[0], 0);

    public SearchableRangeList(Range[] ranges, int[] lowerBounds, int[] headers, int[] checkpointLists, int maxScanAndCheckpointMatches)
    {
        super(SearchableRangeListBuilder.RANGE_ACCESSOR, ranges, lowerBounds, headers, checkpointLists, maxScanAndCheckpointMatches);
    }

    @Inline
    public <P1, P2, P3, P4> int forEach(RoutableKey key, IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange, P1 p1, P2 p2, P3 p3, P4 p4, int minIndex)
    {
        if (ranges.length == 0 || minIndex == ranges.length)
            return minIndex;

        int end = SortedArrays.binarySearch(ranges, minIndex, ranges.length, key, (a, b) -> -b.compareStartTo(a), FLOOR);
        if (end < 0) end = -1 - end;
        if (end <= minIndex) return minIndex;

        int floor = SortedArrays.binarySearch(ranges, minIndex, ranges.length, key, (a, b) -> -b.compareStartTo(a), CEIL);
        int start = floor;
        if (floor < 0)
        {
            // if there's no precise match on start, step backwards;
            // if this range does not overlap us, step forwards again for start
            // but retain the floor index for performing scan and checkpoint searches from
            // as this contains all ranges that might overlap us (whereas those that end
            // after us but before the next range's start would be missed by the next range index)
            start = floor = -2 - floor;
            if (start < 0)
                start = floor = 0;
            else if (ranges[start].compareEndTo(key) < 0)
                ++start;
        }

        int bound = ranges[0].endInclusive() ? -1 : 0;
        return forEach(start, end, floor, key, bound, forEachScanOrCheckpoint, forEachRange, p1, p2, p3, p4, minIndex);
    }

    public static SearchableRangeList build(Range[] ranges)
    {
        if (ranges.length == 0)
            return EMPTY_CHECKPOINTS;

        return new SearchableRangeListBuilder(ranges, ACCURATE, LINKS).build();
    }

    public static SearchableRangeList build(Range[] ranges, int maxScanDistance, Strategy strategy, Links links)
    {
        if (ranges.length == 0)
            return EMPTY_CHECKPOINTS;

        return new SearchableRangeListBuilder(ranges, maxScanDistance, strategy, links).build();
    }
}
