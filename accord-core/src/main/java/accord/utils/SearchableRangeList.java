package accord.utils;

import accord.api.Key;
import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.utils.SearchableRangeListBuilder.Links;
import accord.utils.SearchableRangeListBuilder.Strategy;
import net.nicoulaj.compilecommand.annotations.Inline;

import java.util.*;

import static accord.utils.SearchableRangeListBuilder.Links.LINKS;
import static accord.utils.SearchableRangeListBuilder.Strategy.ACCURATE;
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
public class SearchableRangeList
{
    // scan distance can be kept very small as we guarantee to use at most linear extra space even with a scan distance of zero
    static final int MAX_SCAN_DISTANCE = 255;
    private static final int BIT30 = 0x40000000;
    private static final int BIT29 = 0x20000000;

    private static final SearchableRangeList EMPTY_CHECKPOINTS = new SearchableRangeList(new Range[0], new int[0], new int[] { 0, 0 }, new int[0], 0);

    final Range[] ranges;

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

    SearchableRangeList(Range[] ranges, int[] lowerBounds, int[] headers, int[] checkpointLists, int maxScanAndCheckpointMatches)
    {
        this.ranges = ranges;
        this.lowerBounds = lowerBounds;
        this.headers = headers;
        this.checkpointLists = checkpointLists;
        this.maxScanAndCheckpointMatches = maxScanAndCheckpointMatches;
    }

    @Inline
    public <P1, P2, P3> int forEach(Range range, IndexedTriConsumer<P1, P2, P3> forEachScanOrCheckpoint, IndexedRangeTriConsumer<P1, P2, P3> forEachRange, P1 p1, P2 p2, P3 p3, int minIndex)
    {
        if (ranges.length == 0 || minIndex == ranges.length)
            return minIndex;

        int end = SortedArrays.binarySearch(ranges, minIndex, ranges.length, range.end(), (a, b) -> a.compareTo(b.start()), CEIL);
        if (end < 0) end = -1 - end;
        if (end <= minIndex) return minIndex;

        int floor = SortedArrays.binarySearch(ranges, minIndex, ranges.length, range.start(), (a, b) -> a.compareTo(b.start()), CEIL);
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
            else if (ranges[start].end().compareTo(range.start()) <= 0)
                ++start;
        }

        return forEach(start, end, floor, range.start(), forEachScanOrCheckpoint, forEachRange, p1, p2, p3, minIndex);
    }

    @Inline
    public <P1, P2, P3> int forEach(Key key, IndexedTriConsumer<P1, P2, P3> forEachScanOrCheckpoint, IndexedRangeTriConsumer<P1, P2, P3> forEachRange, P1 p1, P2 p2, P3 p3, int minIndex)
    {
        if (ranges.length == 0 || minIndex == ranges.length)
            return minIndex;

        int end = SortedArrays.binarySearch(ranges, minIndex, ranges.length, key, (a, b) -> a.compareTo(b.start()), FLOOR);
        if (end < 0) end = -1 - end;
        if (end <= minIndex) return minIndex;

        int floor = SortedArrays.binarySearch(ranges, minIndex, ranges.length, key, (a, b) -> a.compareTo(b.start()), CEIL);
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
            else if (ranges[start].end().compareTo(key) <= 0)
                ++start;
        }

        // Key cannot equal RoutingKey, so can ignore inclusivity/exclusivity of end bound
        return forEach(start, end, floor, key, forEachScanOrCheckpoint, forEachRange, p1, p2, p3, minIndex);
    }

    @Inline
    private <P1, P2, P3> int forEach(int start, int end, int floor, RoutableKey startBound,
                                     IndexedTriConsumer<P1, P2, P3> forEachScanOrCheckpoint, IndexedRangeTriConsumer<P1, P2, P3> forEachRange,
                                     P1 p1, P2 p2, P3 p3, int minIndex)
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

                    if (ranges[ri].end().compareTo(startBound) <= 0)
                        break;

                    if (ri >= minIndex && ri < minScanIndex)
                        forEachScanOrCheckpoint.accept(p1, p2, p3, ri);
                }
            }
            else
            {
                // if startBound is key, we cannot be equal to it;
                // if startBound is a Range start, we also cannot be equal to it due to the requirement that
                // endInclusive() != startInclusive(), so equality really means inequality
                if (ranges[ri].end().compareTo(startBound) <= 0)
                    break;

                if (ri >= minIndex && ri < minScanIndex)
                    forEachScanOrCheckpoint.accept(p1, p2, p3, ri);
            }
        }

        for (int i = minScanIndex; i < floor ; ++i)
        {
            if (ranges[i].end().compareTo(startBound) > 0)
                forEachScanOrCheckpoint.accept(p1, p2, p3, i);
        }

        forEachRange.accept(p1, p2, p3, start, end);
        return end;
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
