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

import java.util.Arrays;

import accord.utils.CheckpointIntervalArrayBuilder.Accessor;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.utils.SortedArrays.Search.CEIL;

public class CheckpointIntervalArray<Ranges, Range, Key>
{
    // scan distance can be kept very small as we guarantee to use at most linear extra space even with a scan distance of zero
    static final int MAX_SCAN_DISTANCE = 255;
    protected static final int BIT30 = 0x40000000;
    protected static final int BIT29 = 0x20000000;

    final Ranges ranges;

    /**
     * The lower bound for each checkpoint.
     * The checkpoint {@code i} applies to all ranges (incl) starting from {@code lowerBounds[i]},
     * but before (excl) {@code lowerBounds[i+1]}.
     */
    final int[] lowerBounds;

    /**
     * Logically one entry per checkpoint, mapping {@link #lowerBounds} to {@link #checkpointLists},
     * however we also encode an additional byte per entry representing the scan distance for the
     * ranges handled by this checkpoint. These are grouped into an integer per four mappings, i.e.
     * we encode batches of five ints, with the first int containing the four scan distances for the
     * next four checkpoints, and the following four ints containing the respective offsets into
     * {@link #checkpointLists}.
     * <p>
     * [0.........32b.........64b.........96b........128b........160b........192b]
     * [ d1 d2 d3 d4  mapping1    mapping2   mapping3    mapping4    d5 d6 d7 d8 ]
     */
    final int[] headers;

    /**
     * A list of indexes in {@link #ranges} contained by each checkpoint; checkpoints are
     * mapped from {@link #lowerBounds} by {@link #headers}.
     * <p>
     * Entries are sorted in descending order by the end of the range they cover, so that
     * a search of this collection my terminate as soon as it encounters a range that does
     * not cover the item we are searching for.
     * <p>
     * This collection may contain negative values, in which case these point to other
     * checkpoints, whose <i>direct</i> contents (i.e. the positive values of) we may
     * search.
     * <ul> if negative, points to an earlier checkpoint, and:
     *   <li>if the 30th bit is set, the low 20 bits point to checkpointsList,
     *       and the 9 bits in-between provide the length of the range</li>
     *   <li>otherwise, if the 29th bit is set, the lower 29 bits points to checkpointsList,
     *       and can be iterated safely without an endIndex</li>
     *   <li>otherwise, the low 29 bits provide the length of the run, and the low 31 bits
     *       of the following entry (which will also be negative) provide a pointer to
     *       checkpointsList</li>
     * </ul>
     */
    final int[] checkpointLists;

    public final int maxScanAndCheckpointMatches;
    private final Accessor<Ranges, Range, Key> accessor;

    public CheckpointIntervalArray(Accessor<Ranges, Range, Key> accessor, Ranges ranges,
                                   int[] lowerBounds, int[] headers, int[] checkpointLists, int maxScanAndCheckpointMatches)
    {
        this.accessor = accessor;
        this.ranges = ranges;
        this.lowerBounds = lowerBounds;
        this.headers = headers;
        this.checkpointLists = checkpointLists;
        this.maxScanAndCheckpointMatches = maxScanAndCheckpointMatches;
    }

    @Inline
    public <P1, P2, P3, P4> int forEach(Range range, IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange, P1 p1, P2 p2, P3 p3, P4 p4, int minIndex)
    {
        return forEach(accessor.start(range), accessor.end(range), forEachScanOrCheckpoint, forEachRange, p1, p2, p3, p4, minIndex);
    }

    public <P1, P2, P3, P4> int forEach(Key startKey, Key endKey, IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange, P1 p1, P2 p2, P3 p3, P4 p4, int minIndex)
    {
        if (accessor.size(ranges) == 0 || minIndex == accessor.size(ranges))
            return minIndex;

        var c = accessor.keyComparator();
        int end = accessor.binarySearch(ranges, minIndex, accessor.size(ranges), endKey, (a, b) -> c.compare(a, accessor.start(b)), CEIL);
        if (end < 0) end = -1 - end;
        if (end <= minIndex) return minIndex;

        int floor = accessor.binarySearch(ranges, minIndex, accessor.size(ranges), startKey, (a, b) -> c.compare(a, accessor.start(b)), CEIL);
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
            else if (c.compare(accessor.end(ranges, start), startKey) <= 0)
                ++start;
        }

        // Since endInclusive() != startInclusive(), so no need to adjust start/end comparisons
        return forEach(start, end, floor, startKey, 0, forEachScanOrCheckpoint, forEachRange, p1, p2, p3, p4, minIndex);
    }

    @Inline
    protected <P1, P2, P3, P4> int forEach(int start, int end, int floor, Key startBound, int cmpStartBoundWithEnd,
                                           IndexedQuadConsumer<P1, P2, P3, P4> forEachScanOrCheckpoint, IndexedRangeQuadConsumer<P1, P2, P3, P4> forEachRange,
                                           P1 p1, P2 p2, P3 p3, P4 p4, int minIndex)
    {
        if (start < minIndex) start = minIndex;

        // find the checkpoint array, so we know how far to step back
        int checkpoint = Arrays.binarySearch(lowerBounds, floor);
        if (checkpoint < 0) checkpoint = -2 - checkpoint;
        if (checkpoint < 0) return end;

        int headerBaseIndex = (checkpoint / 4) * 5;
        int headerSubIndex = checkpoint & 3;
        int headerListIndex = headerBaseIndex + 1 + headerSubIndex;

        int scanDistance = (headers[headerBaseIndex] >>> (8 * headerSubIndex)) & 0xff;
        int checkpointStart = headers[headerListIndex];
        int checkpointEnd = headers[headerListIndex + (headerSubIndex + 5)/4]; // skip the next header

        if (scanDistance == MAX_SCAN_DISTANCE)
        {
            scanDistance = -checkpointLists[checkpointStart++];
            Invariants.checkState(scanDistance >= MAX_SCAN_DISTANCE);
        }

        // NOTE: we visit in approximately ascending order, and this is a requirement for correctness of RangeDeps builders
        //       Only the checkpoint is visited in uncertain order, but it is visited entirely, before the scan matches
        //       and the range matches
        int minScanIndex = Math.max(floor - scanDistance, minIndex);
        var c = accessor.keyComparator();
        for (int i = checkpointStart; i < checkpointEnd ; ++i)
        {
            int ri = checkpointLists[i];
            if (ri < 0)
            {
                int subStart, subEnd;
                if ((ri & BIT30) != 0)
                {
                    subStart = ri & 0xfffff;
                    subEnd = subStart + ((ri >>> 20) & 0x1ff);
                }
                else if ((ri & BIT29) != 0)
                {
                    subStart = ri & 0x1fffffff;
                    subEnd = Integer.MAX_VALUE;
                }
                else
                {
                    int length = ri & 0x1fffffff;
                    subStart = checkpointLists[++i];
                    subEnd = subStart + length;
                }

                for (int j = subStart ; j < subEnd ; ++j)
                {
                    ri = checkpointLists[j];
                    if (ri < 0)
                        continue;

                    if (c.compare(accessor.end(ranges, ri), startBound) <= cmpStartBoundWithEnd)
                        break;

                    if (ri >= minIndex && ri < minScanIndex)
                        forEachScanOrCheckpoint.accept(p1, p2, p3, p4, ri);
                }
            }
            else
            {
                // if startBound is key, we cannot be equal to it;
                // if startBound is a Range start, we also cannot be equal to it due to the requirement that
                // endInclusive() != startInclusive(), so equality really means inequality
                if (c.compare(accessor.end(ranges, ri), startBound) <= cmpStartBoundWithEnd)
                    break;

                if (ri >= minIndex && ri < minScanIndex)
                    forEachScanOrCheckpoint.accept(p1, p2, p3, p4, ri);
            }
        }

        for (int i = minScanIndex; i < floor ; ++i)
        {
            if (c.compare(accessor.end(ranges, i), startBound) > cmpStartBoundWithEnd)
                forEachScanOrCheckpoint.accept(p1, p2, p3, p4, i);
        }

        if (start == end)
            return end;

        forEachRange.accept(p1, p2, p3, p4, start, end);
        return end;
    }
}
