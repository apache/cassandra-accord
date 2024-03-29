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
import java.util.Comparator;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static accord.utils.ArrayBuffers.cachedInts;
import static accord.utils.CheckpointIntervalArray.MAX_SCAN_DISTANCE;
import static accord.utils.SortedArrays.Search.CEIL;

public class CheckpointIntervalArrayBuilder<Ranges, Range, RoutingKey>
{
    public enum Strategy
    {
        /**
         * Do not tenure any ranges that are scannable from the currently in-effect max scan distance.
         * This means we probably do less work on construction, but that our measure of the match count
         * at any point is inaccurate, and so our heuristics for when to write checkpoints may be wrong,
         * leading to more checkpoints than necessary.
         */
        FAST,

        /**
         * Tenure every range covering more than goalScanDistance. Any within max scan distance will also
         * update the scan distance, so that they will be filtered when a checkpoint is written. But
         * in the meantime they permit accurate tracking of the number of matches a query can return,
         * permitting our complexity calculations (that determine when checkpoints should be written, and
         * what our maximum scan distance should be), to be accurate. This can avoid bouncing between
         * two extremes, where a low max scan distance tenures and correctly detects a desirable larger scan
         * distance, which we rollover and this prevents us tenuring and tracking the number of matches, so
         * that we then pick a low max scan distance (and thereby also write a new checkpoint)
         */
        ACCURATE
    }

    /**
     * Should we maintain pointers to prior checkpoints that we may reference instead of reserializing
     * the remaining contents. This is cheap to visit as we stop enumerating as soon as we encounter
     * an entry that no longer covers us. We use some simple heuristics when deciding whether to do
     * this, namely that there are at least two entries (so we save one checkpoint entry) and that
     * there is at least one direct entry for each indirect/link entry in the range we will link.
     */
    public enum Links
    {
        LINKS,
        NO_LINKS
    }

    public interface Accessor<Ranges, Range, RoutingKey>
    {
        int size(Ranges ranges);
        Range get(Ranges ranges, int index);
        RoutingKey start(Ranges ranges, int index);
        RoutingKey start(Range range);
        RoutingKey end(Ranges ranges, int index);
        RoutingKey end(Range range);
        Comparator<RoutingKey> keyComparator();
        int binarySearch(Ranges ranges, int from, int to, RoutingKey find, AsymmetricComparator<RoutingKey, Range> comparator, SortedArrays.Search op);
    }

    private static final int BIT31 = 0x80000000;
    private static final int BIT30 = 0x40000000;
    private static final int BIT29 = 0x20000000;
    static final int MIN_INDIRECT_LINK_LENGTH = 2;

    final Accessor<Ranges, Range, RoutingKey> accessor;
    final boolean isAccurate;
    final boolean withLinks;
    final Ranges ranges;

    int[] bounds;
    int[] headers;
    int[] lists;
    int checkpointCount, headerPointer, listCount;

    final Scan<Ranges, Range, RoutingKey> scan;
    final TenuredSet<Ranges, Range, RoutingKey> tenured;
    final PendingCheckpoint<Ranges, Range, RoutingKey> pending = new PendingCheckpoint<>();

    // track the maximum possible number of entries we can match with both a scan + checkpoint lookup
    // this is an over-estimate and may be used by consumers to allocate out-of-order buffers for visitations
    int maxScanAndCheckpointMatches;

    public CheckpointIntervalArrayBuilder(Accessor<Ranges, Range, RoutingKey> accessor,
                                          Ranges ranges,
                                          Strategy strategy, Links links)
    {
        this(accessor, ranges, Math.min(MAX_SCAN_DISTANCE, 34 - Integer.numberOfLeadingZeros(accessor.size(ranges))), strategy, links);
    }

    public CheckpointIntervalArrayBuilder(Accessor<Ranges, Range, RoutingKey> accessor,
                                          Ranges ranges,
                                          int goalScanDistance,
                                          Strategy strategy, Links links)
    {
        this.accessor = accessor;
        this.isAccurate = strategy == Strategy.ACCURATE;
        this.withLinks = links == Links.LINKS;
        Invariants.checkArgument(goalScanDistance <= MAX_SCAN_DISTANCE);
        Invariants.checkArgument(goalScanDistance > 0);
        this.ranges = ranges;
        this.scan = new Scan<>(accessor);
        this.tenured = new TenuredSet<>(accessor);
        init(ranges, goalScanDistance);
    }

    void init(Ranges ranges, int goalScanDistance)
    {
        // we write checkpoints at least goalScanDistance apart
        scan.init(goalScanDistance);
        ArrayBuffers.IntBuffers cachedInts = cachedInts();
        // ask for int buffers in descending order of size
        int size = accessor.size(ranges);
        this.lists = cachedInts.getInts(size); // this one might need to grow
        // +2 to round-up each division, and +2 to account for the final entry (which might require an empty scan distance header)
        this.headers = cachedInts.getInts(((size / goalScanDistance) * 5) / 4 + 4);
        this.bounds = cachedInts.getInts(size / goalScanDistance + 1);
    }

    public interface Factory<T, Ranges>
    {
        T build(Ranges ranges, int[] bounds, int[] headers, int[] lists, int maxScanAndCheckpointMatches);
    }

    /**
     * Walk over each range, looking ahead by {@link #maxScanDistance} to decide if a range should
     * be tenured (written to a checkpoint) or scanned; the maximum scan distance is determined by the
     * number of open tenured entries, i.e. the minimum number of results we can expect to be returned
     * (or, if greater, the logarithm of the number of ranges in the collection).
     * <p>
     * Once we encounter a range that should be tenured, either write a checkpoint immediately
     * or make a note of the position we must scan to from the last entry in this checkpoint
     * and wait until it is permitted to write a checkpoint. This range will be tenured either
     * way for the following checkpoint.
     * <p>
     * The only reason not to write a checkpoint immediately is in the case we would breach
     * our linear space complexity limit, which is imposed by ensuring we have a space between
     * checkpoints at least as large as the number of entries written to the last checkpoint,
     * discounted by the number of entries we have removed from the tenured collection since
     * the last checkpoint.
     */
    public CheckpointIntervalArray<Ranges, Range, RoutingKey> build()
    {
        return build((ranges, bounds, headers, lists, maxScanAndCheckpointMatches) -> new CheckpointIntervalArray<>(accessor, ranges, bounds, headers, lists, maxScanAndCheckpointMatches));
    }

    public <T> T build(Factory<T, Ranges> factory)
    {
        int size = accessor.size(ranges);
        for (int ri = 0 ; ri < size ; ++ri)
        {
            // write a checkpoint if we meet our linear space complexity requirements
            // and we either have a tenured range that we must scan,
            // or the scan distance is now much larger than the minimum number of search results
            if (shouldWriteCheckpoint(ri))
                writeCheckpoint(ri);

            // either tenure or update scan distance, potentially writing a checkpoint
            tenureOrScan(ri);
            tenured.untenure(ri);
        }

        // write our final pending checkpoint
        writeCheckpoint(size);
        closeHeaders();

        ArrayBuffers.IntBuffers cachedInts = cachedInts();
        int[] lists = cachedInts.completeAndDiscard(this.lists, listCount);
        int[] headers = cachedInts.completeAndDiscard(this.headers, headerPointer);
        int[] bounds = cachedInts.completeAndDiscard(this.bounds, checkpointCount);
        return factory.build(ranges, bounds, headers, lists, maxScanAndCheckpointMatches);
    }

    /**
     * Categorise the candidateIdx as either scannable, and if so update the scan distance;
     * or unscannable, in which case add it to the {@link #tenured} collection.
     * Note, that in ACCURATE mode we tenure the item if it is outside of the goalScanDistance
     * so we may track O(k) accurately above the O(lg2(N)) search and default scan distance,
     * but we still update the scan distance so that the checkpoint will exclude this entry.
     */
    private void tenureOrScan(int index)
    {
        Invariants.checkArgument(index >= 0);

        // then either migrate the index to pendingTenured, or ensure it will be scanned
        RoutingKey end = accessor.end(ranges, index);
        int scanLimit = scanLimit(index, isAccurate ? scan.goal : maxScanDistance());
        if (shouldTenure(end, scanLimit))
        {
            int lastIndex = tenured.tenure(end, index, ranges, scanLimit + 1);
            if (lastIndex - index > maxScanDistance()) scan.tenured(index);
            else if (!isAccurate) throw new IllegalStateException();
            else scan.updateScanDistance(index, lastIndex - index, this);
        }
        else
        {
            // TODO (low priority, efficiency): if the prior checkpoint has a scan distance >= this one,
            //  and <= 50% more than this one and there's no scanMustReachIndex nor tenuredRanges, don't
            //  write a new checkpoint (perhaps split shouldWriteCheckpoint logic in two)
            scan.update(end, index, ranges, scanLimit, this);
        }
    }

    /**
     * We are forbidden from writing a checkpoint nearer than this to a prior checkpoint.
     * This imposes our linear space complexity bounds, while not harming our O(log2(N) + K)
     * complexity bounds, as we guarantee minimumSpan is never more than the number of query
     * results.
     */
    private int minimumSpan()
    {
        return Math.max(scan.goal(), tenured.minimumSpan());
    }

    private int maxScanDistance()
    {
        // minimumSpan() reduces overtime, but there is no reason to reduce our scan distance
        // for tenuring below the scan distance we will write
        return Math.max(scan.watermark(), minimumSpan());
    }

    /**
     * The index after the last index we can scan from {@code atIndex} with at most {@code maxScanDistance}.
     */
    private int scanLimit(int atIndex, int maxScanDistance)
    {
        return Math.min(1 + atIndex + maxScanDistance, accessor.size(ranges));
    }

    private boolean shouldTenure(RoutingKey end, int scanLimit)
    {
        return scanLimit < accessor.size(ranges) && accessor.keyComparator().compare(end, accessor.start(ranges, scanLimit)) > 0;
    }

    private boolean canWriteCheckpoint(int atIndex)
    {
        return atIndex - pending.atIndex >= minimumSpan();
    }

    private boolean shouldWriteCheckpoint(int atIndex)
    {
        if (!canWriteCheckpoint(atIndex))
            return false;

        // TODO (desired, efficiency): consider these triggers
        if (scan.mustCheckpointToScanTenured(atIndex, maxScanDistance()))
            return true;

        return scan.hasMaybeDivergedFromMatchSize(tenured);
    }

    /**
     * Write a checkpoint for ranges[prevCheckpointIndex...ri)
     *
     * 1) Finalise the scan distance
     * 2) Write the header
     * 3) Filter the pending tenured ranges to remove those we can scan
     * 4) Write this list out
     * 5) Setup a link to this list, if it is large enough
     * 6) Rollover the scan, tenured and pending structures for the new pending checkpoint
     */
    private void writeCheckpoint(int nextCheckpointIndex)
    {
        int lastIndex = nextCheckpointIndex - 1;
        int scanDistance = scan.finalise(lastIndex);
        scanDistance = extendScanDistance(lastIndex, scanDistance);

        if (pending.atIndex < 0)
        {
            // we don't have any checkpoints pending, so don't try to finalise it
            // but if the new checkpoint doesn't cover index 0, insert a new empty
            // checkpoint for the scan distance
            if (nextCheckpointIndex > 0)
            {
                // setup an initial empty checkpoint to store the first scan distance
                maxScanAndCheckpointMatches = scanDistance;
                writeHeader(scanDistance, 0);
            }
        }
        else
        {
            writeHeader(scanDistance, pending.atIndex);
            int maxCheckpointMatchCount = pending.filter(scanDistance, lastIndex);
            int listIndex = writeList(pending);
            if (withLinks)
                pending.setupLinkChain(tenured, listIndex, listCount);
            maxScanAndCheckpointMatches = Math.max(maxScanAndCheckpointMatches, scanDistance + maxCheckpointMatchCount);
        }

        savePendingCheckpointAndResetScanDistance(nextCheckpointIndex);
    }

    private void savePendingCheckpointAndResetScanDistance(int checkpointIndex)
    {
        // use the tail of checkpointListBuf to buffer ranges we plan to tenure
        ensureCapacity(tenured.count() + scan.watermark());

        scan.reset();

        if (isAccurate)
        {
            // TODO (low priority, efficiency): we can shift back the existing scanDistance if it's far enough from
            //  the next checkpoint. this might permit us to skip some comparisons
            scan.resetPeakMax(tenured);
            for (Tenured<Ranges, Range, RoutingKey> tenured : this.tenured)
            {
                int distanceToEnd = (tenured.lastIndex - checkpointIndex);
                if (distanceToEnd >= scan.peakMax)
                    break;

                int scanDistance = tenured.lastIndex - tenured.index;
                if (scanDistance <= scan.peakMax)
                    scan.updateScanDistance(tenured.index, scanDistance, null);
            }

            if (scan.watermark() < scan.goal)
            {
                int ri = Scan.minScanIndex(checkpointIndex, scan.goal);
                while (ri < checkpointIndex)
                {
                    RoutingKey end = accessor.end(ranges, ri);
                    int scanLimit = scanLimit(ri, scan.peakMax);
                    if (!shouldTenure(end, scanLimit))
                        scan.update(end, ri, ranges, scanLimit, null);
                    ++ri;
                }
            }
        }
        else
        {
            // the maximum scan distance that could ever have been adopted for last chunk
            int oldPeakMax = scan.peakMax();
            // the minimum scan distance we will start with for processing the proceeding ranges
            // note: this may increase if we decide to tenure additional ranges, at which point it will be the actual newPeakMax
            int newMinPeakMax = scan.newPeakMax(tenured);
            int minUntenuredIndex = scan.minUntenuredIndex(checkpointIndex, tenured);
            int minScanIndex = Scan.minScanIndex(checkpointIndex, newMinPeakMax);

            // we now make sure tenured and scan are correct for the new parameters.
            // 1) if our peakMax is lower then we need to go back and find items to tenure that we previously marked for scanning
            // 2) we must also reset our scan distances

            // since our peakMax is determined by tenured.count(), but we are tenuring items here we keep things simple
            // and do not account for those items we tenure but would later permit to scan as our peakMax grows

            int ri = Math.min(minUntenuredIndex, minScanIndex);
            while (ri < checkpointIndex)
            {
                RoutingKey end = accessor.end(ranges, ri);
                int newPeakMax = scan.newPeakMax(tenured);
                int scanLimit = scanLimit(ri, newPeakMax);
                if (shouldTenure(end, scanLimit))
                {
                    // note: might have already been tenured
                    // in this case our untenureLimit may be incorrect, but we won't use it
                    if (ri >= minUntenuredIndex && newPeakMax < oldPeakMax)
                        tenured.tenure(end, ri, ranges, scanLimit + 1, scanLimit(ri, oldPeakMax));
                }
                else
                {
                    // this might effectively remove a previously tenured item
                    scan.update(end, ri, ranges, scanLimit, null);
                }
                ++ri;
            }

            scan.resetPeakMax(tenured);
        }

        pending.atIndex = checkpointIndex;
        pending.clear();
        tenured.rollover(pending);
    }

    private int extendScanDistance(int lastIndex, int scanDistance)
    {
        // now we've established our lower bound on scan distance, see how many checkpoints we can remove
        // by increasing our scan distance so that it remains proportional to the number of results returned
        // TODO (low priority, efficiency): can reduce cost here by using scanDistances array for upper bounds to scan distance
        int maxScanDistance = scan.goal() + 2 * Math.min(tenured.count(), tenured.countAtPrevCheckpoint());
        if (maxScanDistance >= 1 + scanDistance + scanDistance/4 && pending.count() >= (maxScanDistance - scanDistance)/2)
        {
            int removeCount = 0;
            int extendedScanDistance = scanDistance;
            int target = (maxScanDistance - scanDistance)/2;
            for (int i = 0 ; i < pending.count() ; ++i)
            {
                Tenured<Ranges, Range, RoutingKey> t = pending.get(i);
                if (t.index < 0)
                    continue;

                int distance = Math.min(lastIndex, t.lastIndex) - t.index;
                if (distance <= scanDistance)
                    continue; // already scanned or untenured

                if (distance <= maxScanDistance)
                {
                    ++removeCount;
                    extendedScanDistance = Math.max(extendedScanDistance, distance);
                    if (extendedScanDistance == maxScanDistance && removeCount >= target)
                        break;
                }
            }

            // TODO (low priority, efficiency): should perhaps also gate this decision on the span we're covering
            //  algorithmically, however, so long as we are under maxScanDistance we are fine
            if (removeCount >= (extendedScanDistance - scanDistance)/2)
                scanDistance = extendedScanDistance;
        }
        return scanDistance;
    }

    int writeList(PendingCheckpoint<Ranges, Range, RoutingKey> pending)
    {
        int startIndex = listCount;
        for (int i = pending.count() - 1 ; i >= 0 ; --i)
        {
            Tenured<Ranges, Range, RoutingKey> t = pending.get(i);
            if (t.index >= 0)
            {
                lists[listCount++] = t.index;
            }
            else
            {
                int index = t.index & ~BIT31;
                int length = t.linkLength & ~BIT31;
                if (length <= 0xff && index <= 0xfffff)
                {
                    lists[listCount++] = BIT31 | BIT30 | (length << 20) | index;
                }
                else if (t.linkLength >= 0 && length < BIT30)
                {
                    lists[listCount++] = BIT31 | BIT29 | index;
                }
                else
                {
                    lists[listCount++] = BIT31 | length;
                    lists[listCount++] = BIT31 | pending.count();
                }
            }
        }
        return startIndex;
    }

    void writeHeader(int scanDistance, int lowerBound)
    {
        int headerScanDistance = Math.min(scanDistance, MAX_SCAN_DISTANCE);

        if ((checkpointCount & 3) == 0)
            headers[headerPointer++] = headerScanDistance;
        else
            headers[headerPointer - (1 + (checkpointCount & 3))] |= headerScanDistance << (8 * (checkpointCount & 3));

        bounds[checkpointCount++] = lowerBound;
        headers[headerPointer++] = listCount;

        if (scanDistance >= MAX_SCAN_DISTANCE)
            lists[listCount++] = -scanDistance; // serialize as a negative value so we ignore it in most cases automatically
    }

    void closeHeaders()
    {
        // write our final checkpoint header
        if ((checkpointCount & 3) == 0) headers[headerPointer++] = 0;
        headers[headerPointer++] = listCount;
    }

    void ensureCapacity(int maxPendingSize)
    {
        if (listCount + maxPendingSize >= lists.length)
            lists = cachedInts().resize(lists, listCount, lists.length + lists.length/2 + maxPendingSize);
    }

    static class Scan<Ranges, Range, RoutingKey>
    {
        final Accessor<Ranges, Range, RoutingKey> accessor;
        /** the scan distance we are aiming for; should be proportional to log2(N) */
        int goal;

        /** the indexes at which we increased the scan distance, and the new scan distance */
        int[] distances = new int[16];
        /** the number of unique scan distances we have adopted since the last checkpoint */
        int count;
        /** the highest scan distance we have adopted (==scanDistance(scanDistanceCount-1)) */
        int watermark;
        /**
         * the first index we have tenured a range from, but for which we did not immediately write a new checkpoint
         * we *must* scan at least from the last index in the checkpoint to here
         */
        int scanTenuredAtIndex = -1;

        /** The maximum (i.e. initial) scan distance limit we have used since the last attempted checkpoint write */
        int peakMax;

        Scan(Accessor<Ranges, Range, RoutingKey> accessor)
        {
            this.accessor = accessor;
        }

        void init(int goalScanDistance)
        {
            goal = peakMax = goalScanDistance;
        }

        private void update(RoutingKey end, int atIndex, Ranges ranges, int scanLimit, CheckpointIntervalArrayBuilder<Ranges, Range, RoutingKey> checkpoint)
        {
            int newScanDistance = find(end, atIndex, ranges, scanLimit, watermark);
            updateScanDistance(atIndex, newScanDistance, checkpoint);
        }

        private void updateScanDistance(int atIndex, int newScanDistance, CheckpointIntervalArrayBuilder<Ranges, Range, RoutingKey> checkpoint)
        {
            if (newScanDistance > watermark)
            {
                // TODO (desired, efficiency): we don't mind slight increases to the watermark;
                //  should really look at scan distance history and ensure we haven't e.g. doubled since
                //  some earlier point (and should track the match count + scan distance at each bump
                //  to check overall work hasn't increased too much)
                if (checkpoint != null && checkpoint.canWriteCheckpoint(atIndex))
                    checkpoint.writeCheckpoint(atIndex);

                watermark = newScanDistance;
                if (count * 2 == distances.length)
                    distances = Arrays.copyOf(distances, distances.length * 2);
                distances[count * 2] = newScanDistance;
                distances[count * 2 + 1] = atIndex;
                ++count;
            }
        }

        private int find(RoutingKey end, int atIndex, Ranges ranges, int scanLimit, int currentScanDistance)
        {
            var c = accessor.keyComparator();
            int lowerIndex = accessor.binarySearch(ranges, atIndex + currentScanDistance, scanLimit, end, (e, s) -> c.compare(e, accessor.start(s)), CEIL);
            if (lowerIndex < 0) lowerIndex = -2 - lowerIndex;
            else lowerIndex -= 1;
            return lowerIndex - atIndex;
        }

        boolean isAboveGoal()
        {
            return watermark > goal;
        }

        int watermark()
        {
            return watermark;
        }

        int goal()
        {
            return goal;
        }

        int distanceToTenured(int lastIndex)
        {
            return scanTenuredAtIndex >= 0 ? lastIndex - scanTenuredAtIndex : 0;
        }

        boolean mustCheckpointToScanTenured(int checkpointIndex, int maxScanDistance)
        {
            return scanTenuredAtIndex >= 0 && checkpointIndex - scanTenuredAtIndex >= maxScanDistance;
        }

        /**
         * Are we scanning a much longer distance than the minimum number of matches we know a query will return?
         * Note: with Strategy.FAST, {@code tenured.count()} gets less accurate as scan distance increases, so this
         * will bounce around triggering checkpoints due to the larger scan distance, resetting the scan distance
         * and starting again
         */
        boolean hasMaybeDivergedFromMatchSize(TenuredSet<Ranges, Range, RoutingKey> tenured)
        {
            return isAboveGoal() && tenured.count() < watermark()/2;
        }

        private int distance(int i)
        {
            return distances[i*2];
        }

        private int index(int i)
        {
            return distances[i*2+1];
        }

        int finalise(int lastIndex)
        {
            Invariants.checkState(distanceToTenured(lastIndex) <= Math.max(watermark(), peakMax()));

            int scanDistance = watermark;
            // then, compute the minimum scan distance implied by any tenured ranges we did not immediately
            // write a checkpoint for - we *must* scan back as far as this record
            int minScanDistance = scanTenuredAtIndex >= 0 ? lastIndex - scanTenuredAtIndex : 0;
            if (minScanDistance > scanDistance)
            {
                // if this minimum is larger than the largest scan distance we picked up for non-tenured ranges
                // then we are done, as there's nothing we can save
                scanDistance = minScanDistance;
            }
            else if (scanDistance > 0)
            {
                // otherwise, we can look to see if any of the scan distances we computed overflow the checkpoint,
                // i.e. where no records served by this checkpoint need to scan the full distance to reach it
                int distanceToLastScanIndex = lastIndex - index(count -1);
                // if the distance to the last scan index is larger than its scan distance, we have overflowed;
                if (distanceToLastScanIndex < scanDistance)
                {
                    minScanDistance = Math.max(distanceToLastScanIndex, minScanDistance);
                    // loop until we find one that doesn't overflow, as this is another minimum scan distance
                    int i = count - 1;
                    while (--i >= 0)
                    {
                        int distance = lastIndex - index(i);
                        if (distance >= distance(i)) break;
                        else if (distance > minScanDistance) minScanDistance = distance;
                    }
                    if (i >= 0) scanDistance = Math.max(minScanDistance, distance(i));
                    else scanDistance = minScanDistance;
                }
            }

            return scanDistance;
        }

        void reset()
        {
            // we could in theory reset our scan distance using the contents of scanDistance[]
            // but it's a bit complicated, as we want to have the first item to increment the scan distance
            // so that we can use it in writeScanDistance to shrink the scan distance;
            // jumping straight to the highest scan distance breaks this
            count = 0;
            scanTenuredAtIndex = -1;
            watermark = 0;
        }

        void resetPeakMax(TenuredSet<Ranges, Range, RoutingKey> tenured)
        {
            peakMax = newPeakMax(tenured);
        }

        int peakMax()
        {
            return peakMax;
        }

        int newPeakMax(TenuredSet<Ranges, Range, RoutingKey> tenured)
        {
            return Math.max(goal, tenured.count());
        }

        /**
         * The minimum index containing a range that might need to be tenured, if we have a smaller max scan distance than before
         */
        int minUntenuredIndex(int checkpointIndex, TenuredSet<Ranges, Range, RoutingKey> tenured)
        {
            int minUntenuredIndex = Math.max(0, (checkpointIndex - 1) - watermark());
            // the maximum scan distance that cxould ever have been adopted for the ranges processed since last checkpoint
            int oldPeakMax = peakMax;
            int newMinPeakMax = newPeakMax(tenured);
            if (newMinPeakMax < oldPeakMax)
            {
                // minimise range we unnecessarily re-tenure over
                // TODO (low priority, efficiency): see if can also use to reduce range we re-scan e.g. can recycle
                //  scanDistances contents if we know we won't need to step back further at next checkpoint
                for (int i = count - 1; i >= 0 ; --i)
                {
                    if (index(i) <= minUntenuredIndex)
                        break;
                    if (distance(i) <= newMinPeakMax)
                        return i + 1 == count ? index(i) : index(i + 1) - 1;
                }
            }
            return minUntenuredIndex;
        }

        /**
         * Record that a range at this index has been tenured, so that we can track how far back
         * we need to scan to determine how long we can defer writing a new checkpoint while still
         * being able to scan it.
         *
         * TODO (low priority, efficiency): when a checkpoint is written, we should consider moving it
         *  earlier if the scan distance is increased primarily because of this index, and the tenured
         *  collection is otherwise unchanged (so can be written with minimal overhead)
         */
        void tenured(int atIndex)
        {
            if (scanTenuredAtIndex < 0)
                scanTenuredAtIndex = atIndex;
        }

        static int minScanIndex(int checkpointIndex, int scanDistance)
        {
            return Math.max(0, (checkpointIndex - 1) - scanDistance);
        }

        @Override
        public String toString()
        {
            return "Scan{watermark=" + watermark + ", tenured=" + scanTenuredAtIndex + '}';
        }
    }

    /**
     * Record-keeping for a range we have decided is not scannable
     */
    static class Tenured<Ranges, Range, RoutingKey> implements Comparable<Tenured<Ranges, Range, RoutingKey>>
    {
        final Accessor<Ranges, Range, RoutingKey> accessor;
        /**
         * The end of the tenured range covered by the contents referred to be {@link #index}
         */
        RoutingKey end;

        /**
         * <ul>
         * <li>If positive, this points to {@code ranges[index]}</li>
         * <li>If negative, this points to an entry in {@link #lists};
         * see {@link SearchableRangeList#checkpointLists}</li>
         * </ul>
         */
        int index;

        /**
         * The last index in {@link #ranges} covered by this tenured range
         */
        int lastIndex;

        /**
         * set when this record is serialized in a checkpoint list to either:
         * <ul>
         * <li>point to itself, in which case no action should be
         *     taken on removal (it is only retained for size bookkeeping); or</li>
         * <li>point to the next item in the checkpoint list; the first
         *     such element removed triggers the clearing of the checkpoint
         *     list so that its entries are re-inserted in the next checkpoint</li>
         * </ul>
         */
        Tenured<Ranges, Range, RoutingKey> next;

        /**
         * Only set for link entries, i.e. where {@code index < 0}.
         * <ul>
         * <li>if positive, the length is optional as we will terminate safely using the end bound filtering</li>
         * <li>if negative, the low 31 bits <b>must</b> be retrieved as the length for safe iteration</li>
         * </ul>
         */
        int linkLength;

        Tenured(Accessor<Ranges, Range, RoutingKey> accessor, RoutingKey end, int index)
        {
            this.accessor = accessor;
            this.end = end;
            this.index = index;
        }

        @Override
        public int compareTo(@Nonnull Tenured<Ranges, Range, RoutingKey> that)
        {
            int c = accessor.keyComparator().compare(this.end, that.end);
            // we sort indexes in reverse order so later tenured items find the earlier ones with same end when searching
            // for higher entries for the range of indexes to search, and
            if (c == 0) c = -Integer.compare(this.index, that.index);
            return c;
        }

        @Override
        public String toString()
        {
            return "Tenured{end=" + end + ", index=" + index + '}';
        }
    }

    /**
     * The set of ranges that we intend to write to checkpoints that remain open at the current point in the iteration
     * This collection may be filtered before serialization, but every member will be visited either by scanning
     * or visiting the checkpoint list
     * TODO (low priority, efficiency): save garbage by using an insertion-sorted array for collections where
     *  this is sufficient. later, introduce a mutable b-tree supporting object recycling. we would also like
     *  to use a collection that permits us to insert and return a finger into the tree so we can find the
     *  successor as part of insertion, and that permits constant-time first() calls
     */
    static class TenuredSet<Ranges, Range, RoutingKey> extends TreeSet<Tenured<Ranges, Range, RoutingKey>>
    {
        final Accessor<Ranges, Range, RoutingKey> accessor;
        /**
         * the number of direct tenured entries (i.e. ignoring link entries)
         * this is used to provide a minimum bound on the number of results a range query can return
         * note: with Strategy.FAST this gets less accurate as the span distance increases
         */
        int directCount;
        int directCountAtPrevCheckpoint;
        int minSpan;

        // a stack of recently used EndAndIndex objects - used only for the duration of a single build
        Tenured<Ranges, Range, RoutingKey> reuse, pendingReuse, pendingReuseTail;

        TenuredSet(Accessor<Ranges, Range, RoutingKey> accessor)
        {
            this.accessor = accessor;
        }

        int count()
        {
            return directCount;
        }

        int countAtPrevCheckpoint()
        {
            return directCountAtPrevCheckpoint;
        }

        /**
         * We require a checkpoint to cover a distance at least as large as the number of tenured ranges leftover
         * since the prior checkpoint, to ensure these require at most linear additional space, while not requiring
         * more than O(k) additional complexity on search (i.e., we will scan a number of elements at most equal
         * to the number we have to visit in the checkpoint).
         *
         * We achieve this by recording the minimum number of match results as of the prior checkpoint (i.e. {@link #count()})
         * and discounting it by one each time we untenure a range, so that for each tenured range from the prior checkpoint
         * we have either untenured a range or processed at least one additional input.
         */
        int minimumSpan()
        {
            return minSpan;
        }

        private int tenure(RoutingKey end, int index, Ranges ranges, int minUntenureIndex)
        {
            return tenure(newTenured(end, index), ranges, minUntenureIndex, accessor.size(ranges));
        }

        private void tenure(RoutingKey end, int index, Ranges ranges, int minUntenureIndex, int untenureLimit)
        {
            tenure(newTenured(end, index), ranges, minUntenureIndex, untenureLimit);
        }

        private int tenure(Tenured<Ranges, Range, RoutingKey> tenure, Ranges ranges, int untenureMinIndex, int untenureLimit)
        {
            if (!add(tenure))
                return tenure.lastIndex;

            Tenured<Ranges, Range, RoutingKey> next = higher(tenure);
            if (next != null)
                untenureLimit = Math.min(untenureLimit, next.lastIndex + 1);
            var c = accessor.keyComparator();
            int untenureIndex = accessor.binarySearch(ranges, untenureMinIndex, untenureLimit, tenure.end, (e, s) -> c.compare(e, accessor.start(s)), CEIL);
            if (untenureIndex < 0) untenureIndex = -1 - untenureIndex;
            tenure.lastIndex = untenureIndex - 1;
            Invariants.checkState(c.compare(tenure.end, accessor.start(ranges, tenure.lastIndex)) > 0);
            Invariants.checkState(tenure.lastIndex + 1 == accessor.size(ranges) || c.compare(tenure.end, accessor.start(ranges, tenure.lastIndex + 1)) <= 0);
            ++directCount;
            return untenureIndex - 1;
        }

        private Tenured<Ranges, Range, RoutingKey> newTenured(RoutingKey end, int index)
        {
            Tenured<Ranges, Range, RoutingKey> result = reuse;
            if (result == null)
                return new Tenured<>(accessor, end, index);

            reuse = result.next;
            result.end = end;
            result.index = index;
            result.lastIndex = 0;
            result.next = null;
            return result;
        }

        private Tenured<Ranges, Range, RoutingKey> addLinkEntry(RoutingKey end, int index, int lastIndex, int length)
        {
            Invariants.checkArgument(index < 0);
            Tenured<Ranges, Range, RoutingKey> result = newTenured(end, index);
            result.linkLength = length;
            result.lastIndex = lastIndex;
            add(result);
            return result;
        }

        /**
         * Retire any active tenured ranges that no longer cover the pointer into ranges;
         * if this crosses our checkpoint threshold, write a new checkpoint.
         */
        void untenure(int index)
        {
            while (!isEmpty() && first().lastIndex < index)
            {
                Tenured<Ranges, Range, RoutingKey> removed = pollFirst();

                // if removed.next == null, this is not referenced by a link
                // if removed.next == removed, it is referenced by a link but does not modify the link on removal
                if (removed.next != null && removed.next != removed)
                {
                    // this is a member of a link's chain, which may serve one of two purposes:
                    // 1) it may be the entry nominated to invalidate the link, due to the link
                    //    membership shrinking below the required threshold; in which case we
                    //    must clear the chain to reactivate its members for insertion into the
                    //    next checkpoint, and remove the chain link itself
                    // 2) it may be nominated as an entry to update the chain link info, to make
                    //    it more succinct: if every entry of the chain remains active, and there
                    //    are *many* entries then we need two integers to represent the chain, but
                    //    as soon as any entry is invalid we can rely on this entry to terminate
                    //    iteration, so we update the bookkeeping on the first entry we remove in
                    //    this case

                    // first clear the chain starting at the removed entry
                    Tenured<Ranges, Range, RoutingKey> prev = removed, next = removed.next;
                    while (next.next != null)
                    {
                        prev = next;
                        next = next.next;
                        prev.next = null;
                    }
                    Invariants.checkState(next.index < 0);
                    if (prev.end == next.end)
                    {
                        // if this is the last entry in the link, the link is expired and should be removed/reused
                        remove(next);
                        if (pendingReuseTail == null)
                            pendingReuseTail = next;
                        next.next = pendingReuse;
                        pendingReuse = next;
                    }
                    else if (next.linkLength < 0)
                    {
                        // otherwise, flag the link as safely consumed without knowing the length
                        next.linkLength = next.linkLength & Integer.MAX_VALUE;
                    }
                }

                // this was not a link reference; update our bookkeeping and save it for reuse
                Invariants.checkState(removed.index >= 0);
                --directCount;
                --minSpan;
                if (pendingReuseTail == null)
                    pendingReuseTail = removed;
                removed.next = pendingReuse;
                pendingReuse = removed;
            }
        }

        /**
         * Write out any direct entries that are not pointed to by a chain entry, and any chain entries;
         * rollover any per-checkpoint data and free up for reuse discarded Tenured objects
         */
        void rollover(PendingCheckpoint<Ranges, Range, RoutingKey> pending)
        {
            for (Tenured<Ranges, Range, RoutingKey> tenured : this)
            {
                if (tenured.next == null)
                    pending.add(tenured);
            }
            // make freed Tenured objects available for reuse
            if (pendingReuse != null)
            {
                pendingReuseTail.next = reuse;
                reuse = pendingReuse;
                pendingReuseTail = pendingReuse = null;
            }
            directCountAtPrevCheckpoint = minSpan = directCount;
        }
    }

    /**
     * we write checkpoints out before knowing the scan distance needed for the range, as a checkpoint precedes
     * the ranges it covers; so we record the position and contents of the checkpoint, and once the scan distance is
     * known (i.e. when the next checkpoint is written) we re-process the list to remove items we can now scan before
     * serializing to checkpointListsBuf.
     */
    static class PendingCheckpoint<Ranges, Range, RoutingKey>
    {
        int atIndex = -1;
        int count;

        Tenured<Ranges, Range, RoutingKey>[] contents = new Tenured[10];

        int openDirectCount, firstOpenDirect, openIndirectCount;
        boolean hasClosedDirect;

        int count()
        {
            return count;
        }

        Tenured<Ranges, Range, RoutingKey> get(int i)
        {
            return contents[i];
        }

        void add(Tenured<Ranges, Range, RoutingKey> tenured)
        {
            if (contents.length == count)
                contents = Arrays.copyOf(contents, 2 * contents.length);
            contents[count++] = tenured;
        }

        void clear()
        {
            count = 0;
        }

        /**
         * Remove pending entries that will be scanned by the scanDistance, and update
         * our bookkeeping for creating links
         */
        int filter(int scanDistance, int lastIndex)
        {
            int matchCountModifier = 0;
            int maxi = count;
            count = 0;
            openDirectCount = 0;
            openIndirectCount = 0;
            firstOpenDirect = -1;
//            lastClosedDirect = -1;

            for (int i = 0; i < maxi ; ++i)
            {
                Tenured<Ranges, Range, RoutingKey> t = get(i);
                if (t.index >= 0)
                {
                    if (t.index + scanDistance >= lastIndex)
                        continue; // last index will find it with a scan

                    if (t.lastIndex <= t.index + scanDistance)
                        continue; // all indexes will find it with a scan

                    if (t.lastIndex > lastIndex)
                    {
                        // this range remains open for the next checkpoint;
                        // we may want to reference this list from there
                        // so track count and position of first one to make a determination
                        ++openDirectCount;
                        if (firstOpenDirect < 0) firstOpenDirect = count;
                    }
                    else hasClosedDirect = true;
                }
                else
                {
                    // note: we over count here, as we count pointers within the chain
                    matchCountModifier += (t.linkLength & Integer.MAX_VALUE) - 1; // (subtract 1 to discount the pointer)
                    if (t.lastIndex > lastIndex)
                        ++openIndirectCount;
                }

                if (i == count) ++count;
                else contents[count++] = t;
            }

            return count + matchCountModifier;
        }

        /**
         * Setup a link for referencing this chain later, if permitted.
         * Must have at least two items, and at least as many direct records as indirect
         */
        void setupLinkChain(TenuredSet<Ranges, Range, RoutingKey> tenured, int startIndex, int endIndex)
        {
            int minSizeToReference = openIndirectCount + MIN_INDIRECT_LINK_LENGTH;
            if (openDirectCount >= minSizeToReference)
            {
                int i = firstOpenDirect;
                Tenured<Ranges, Range, RoutingKey> prev = get(i++);

                while (openDirectCount > minSizeToReference)
                {
                    Tenured<Ranges, Range, RoutingKey> e = get(i++);
                    if (e.index < 0)
                    {
                        --minSizeToReference;
                        continue;
                    }

                    Invariants.checkState(prev.next == null);
                    prev.next = prev;
                    prev = e;
                    --openDirectCount;
                }

                while (i < count)
                {
                    Tenured<Ranges, Range, RoutingKey> next = get(i++);
                    if (next.index < 0)
                        continue;

                    Invariants.checkState(prev.next == null);
                    prev.next = next;
                    prev = next;
                }

                // may be more than one entry per item (though usually not)
                int length = endIndex - startIndex;
                Tenured<Ranges, Range, RoutingKey> chainEntry = tenured.addLinkEntry(prev.end, BIT31 | startIndex, prev.lastIndex, length);
                prev.next = chainEntry;
                if (hasClosedDirect && (startIndex > 0xfffff || (length > 0xff)))
                {
                    // TODO (expected, testing): make sure this is tested, as not a common code path (may never be executed in normal operation)
                    // we have no closed ranges so iteration needs to know the end bound, but we cannot encode our bounds cheaply
                    // so link the first bound to the chain entry, so that on removal it triggers an update of endIndex to note
                    // that it can be iterated safely without an end bound
                    get(firstOpenDirect).next = chainEntry;
                }
            }
        }

        @Override
        public String toString()
        {
            return Arrays.stream(contents, 0, count)
                         .map(Objects::toString)
                         .collect(Collectors.joining(",", "[", "]"));
        }
    }
}
