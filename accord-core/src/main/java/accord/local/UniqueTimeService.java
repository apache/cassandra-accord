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

package accord.local;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.google.common.annotations.VisibleForTesting;

import accord.utils.Invariants;

public interface UniqueTimeService
{
    /**
     * Expected to provide a value > all prior values
     */
    default long uniqueNow() { return uniqueNow(0); }

    /**
     * Expected to provide a value > all prior values, and greater than the parameter
     */
    long uniqueNow(long greaterThan);

    default long uniqueStale(long greaterThan) { return uniqueNow(greaterThan); }

    /**
     * Allocate from a special reserve extra-stale reservation
     */
    default long uniqueStaleReserved(long greaterThan) { return uniqueStale(greaterThan); }

    class AtomicUniqueTime extends AtomicLong implements UniqueTimeService
    {
        final TimeService time;

        public AtomicUniqueTime(TimeService time)
        {
            this.time = time;
        }

        @Override
        public long uniqueNow(long greaterThan)
        {
            long now = time.now();
            return accumulateAndGet(Math.max(now - 1, greaterThan), (a, b) -> Math.max(a, b) + 1);
        }
    }

    class AtomicUniqueTimeWithStaleReservation extends AtomicLong implements UniqueTimeService
    {
        // 1 in every ~1k hlc are reserved for stale hlc by default
        private static final long RESERVATION_MASK = 0x3ff;
        final TimeService time;
        final AtomicLong lastStale = new AtomicLong();

        public AtomicUniqueTimeWithStaleReservation(TimeService time)
        {
            this.time = time;
        }

        @Override
        public long uniqueNow(long greaterThan)
        {
            long now = time.now();
            long min = Math.max(now, greaterThan + 1);
            while (true)
            {
                long cur = get();
                if (cur >= min)
                    break;

                long next = Math.max(min, cur + 1);
                if (!isSafeNow(next))
                    ++next;

                if (compareAndSet(cur, next))
                    return next;
            }

            while (true)
            {
                long result = incrementAndGet();
                if (isSafeNow(result))
                    return result;
            }
        }

        @Override
        public long uniqueStale(long greaterThan)
        {
            long now = time.now();
            long min = greaterThan + 1;
            while (true)
            {
                long cur = lastStale.get();
                if (cur >= now)
                    return uniqueNow(greaterThan);

                long next = ensureStale(Math.max(min, cur + 1));
                if (lastStale.compareAndSet(cur, next))
                    return next;
            }
        }

        protected long ensureStale(long stale)
        {
            if ((stale & RESERVATION_MASK) == 0)
                return stale;

            stale += 1 + RESERVATION_MASK - (stale & RESERVATION_MASK);
            Invariants.require((stale & RESERVATION_MASK) == 0);
            return stale;
        }

        protected boolean isSafeNow(long now)
        {
            return (now & RESERVATION_MASK) != 0;
        }
    }

    /**
     * Allocates equal slices of HLC bits to a number of buckets so that we can manage trailing/stale counters,
     * but delegates the process of distributing requests among the buckets to extensions.
     */
    abstract class AtomicUniqueStaleTimes extends AtomicLongArray implements UniqueTimeService
    {
        final TimeService time;

        private volatile long lastNow;
        private static final AtomicLongFieldUpdater<AtomicUniqueStaleTimes> lastNowUpdater = AtomicLongFieldUpdater.newUpdater(AtomicUniqueStaleTimes.class, "lastNow");

        public AtomicUniqueStaleTimes(TimeService time, int buckets)
        {
            super(buckets);
            Invariants.require(Integer.bitCount(buckets) == 1);
            this.time = time;
            for (int i = 0 ; i < length() ; ++i)
                set(i, ensureInBucket(i, buckets, 0));
        }

        @Override
        public final long uniqueStaleReserved(long greaterThan)
        {
            return unique(reservedBucket(), greaterThan);
        }

        /** the top bucket is reserved for {@link #uniqueStaleReserved} */
        protected final int reservedBucket()
        {
            return length() - 1;
        }

        protected final int unreservedBuckets()
        {
            return length() - 1;
        }

        @Override
        public final long uniqueNow(long greaterThan)
        {
            long now = time.now();
            long min = ensureInNowRange(Math.max(now, greaterThan + 1));
            while (true)
            {
                long cur = lastNow;
                if (cur + safeNowIncrement() >= min)
                    break;

                if (lastNowUpdater.compareAndSet(this, cur, min))
                    return min;
            }

            return lastNowUpdater.addAndGet(this, safeNowIncrement());
        }

        protected final long unique(int bucket, long greaterThan)
        {
            long min = ensureInBucket(bucket, length(), greaterThan + 1);
            long increment = safeBucketIncrement();
            while (true)
            {
                long cur = get(bucket);
                if (cur + increment >= min)
                    break;

                if (compareAndSet(bucket, cur, min))
                    return min;
            }

            return addAndGet(bucket, increment);
        }

        private static long ensureInBucket(int bucket, int buckets, long candidate)
        {
            long bucketLowBits = ((long)bucket << 1) | 1;
            long lowBitsMask = (2L * buckets) - 1;

            long lowBits = lowBitsMask & candidate;
            long delta = (bucketLowBits - lowBits) & lowBitsMask;
            candidate += delta;
            Invariants.require((candidate & lowBitsMask) == bucketLowBits);
            return candidate;
        }

        private long safeBucketIncrement()
        {
            return 2L * length();
        }

        private static long ensureInNowRange(long candidate)
        {
            return candidate + (candidate & 1);
        }

        private static long safeNowIncrement()
        {
            return 2;
        }
    }

    /**
     * Routes stale timestamp requests to buckets using a 3-level (7 node) recursive bisection tree
     * packed into a single atomic long. Each node is a 9-bit frugal median estimator[1], so that
     * the root represents the median, its left and right nodes represent p25 and p75 respectively,
     * and the leaf level represents p125, p375, p625 and p875, so that altogether we approximate:
     * p125, p250, p375, p500, p625, p750 and p875.
     *
     * We additionally interpolate one additional layer, taking the average of adjacent estimated medians
     * and bucketing any entry into buckets either side of this average, giving 16 buckets in total.
     *
     * To maximise the use of bits, we have two modes of operation: warmup and normal.
     * During warmup we track only the top 2 levels (3 nodes), and track a separate sample count for each node.
     * During this period, we are aiming to get a fast initial approximation of the medians and so move the measurement
     * by an amount proportional to the number of samples collected so far.
     *
     * Once all 3 nodes have processed 512 samples we flip to normal mode, initialising each new node by interpolating
     * the successor/predecessor nodes from warmup, after which we no longer track how many samples we receive.
     *
     * In both modes we have the same structure, with 7 9-bit integers packed into a long, with the top bit used to
     * switch the mode - since this is the sign bit, we only need to compare with zero to determine the mode.
     *
     * This was developed with design input from Claude, which originated the underlying idea of using frugal estimators,
     * and to refine this into a recursive bisection tree, however the specifics beyond this are predominantly original.
     *
     * [1] Frugal Streaming for Estimating Quantiles
     *     Ma, Muthukrishnan and Sandler
     *     https://arxiv.org/pdf/1407.1121
     */
    class AtomicUniqueAutoStaleTimes extends AtomicUniqueStaleTimes
    {
        private static final long MODE_BIT = Long.MIN_VALUE;

        private static final int NODE_BITS = 9;
        private static final long NODE_MASK = (1 << NODE_BITS) - 1;

        private static final long WARMUP_COUNT_MASK = setNode(NODE_MASK, 3)
                                                    | setNode(NODE_MASK, 4)
                                                    | setNode(NODE_MASK, 5);

        private final int granularityShift;

        private volatile long quantiles;
        private static final AtomicLongFieldUpdater<AtomicUniqueAutoStaleTimes> quantilesUpdater =
            AtomicLongFieldUpdater.newUpdater(AtomicUniqueAutoStaleTimes.class, "quantiles");

        public AtomicUniqueAutoStaleTimes(TimeService time, long range, TimeUnit units)
        {
            this(time, Math.max(0, (64 - NODE_BITS) - Long.numberOfLeadingZeros(units.toMicros(range))));
        }

        public AtomicUniqueAutoStaleTimes(TimeService time, int granularityShift)
        {
            super(time, 16);
            this.granularityShift = granularityShift;
        }

        @Override
        public long uniqueStale(long greaterThan)
        {
            return unique(pickBucket(greaterThan), greaterThan);
        }

        private int pickBucket(long greaterThan)
        {
            long age = Math.max(0, time.now() - greaterThan);
            long quantized = Math.min(NODE_MASK, Math.max(0, age >>> granularityShift));

            long cur = quantiles;
            if (cur < 0) return pickBucket(quantized, cur);
            else return pickBucketWarmup(quantized, cur);
        }

        private int pickBucket(long quantized, long cur)
        {
            long updated = cur;
            int nodeIndex = 0;
            // top bucket is reserved, so we fix upper bound to ensure we never overflow to top bucket
            long lb = -1, ub = 2 * NODE_MASK;
            for (int level = 0; level < 3; level++)
            {
                long median = node(cur, nodeIndex);
                long nudge = setNode(1L, nodeIndex);
                nodeIndex *= 2;
                if (quantized < median)
                {
                    ub = median;
                    updated -= nudge;
                }
                else if (quantized > median)
                {
                    lb = median;
                    updated += nudge;
                    nodeIndex += 1;
                }
                else lb = ub = median;
                nodeIndex += 1;
            }

            if (lb == -1) lb = lb(cur);
            quantilesUpdater.weakCompareAndSet(this, cur, updated);
            return (nodeIndex - 7) * 2 + (quantized > (lb + ub) / 2 ? 1 : 0);
        }

        private static long lb(long cur)
        {
            long min = node(cur, 3);
            long next = node(cur, 1);
            return min - (next-min)/2;
        }

        private int pickBucketWarmup(long quantized, long cur)
        {
            if (cur == 0)
            {
                long upd = setNode(1L, 3) | setNode(quantized, 0);
                quantilesUpdater.compareAndSet(this, cur, upd);
                return uniformBucket(quantized);
            }

            while (true)
            {
                long upd = cur;
                int levels = node(cur, 3) < NODE_MASK ? 1 : 2;

                int nodeIndex = 0;
                for (int level = 0; level < levels; level++)
                {
                    int countIndex = 3 + nodeIndex;
                    long count = node(cur, countIndex);
                    if (node(cur, countIndex) < NODE_MASK)
                        upd += setNode(1L, countIndex);

                    long node = node(cur, nodeIndex);
                    long nudge = warmupStep(node, count);
                    if (quantized < node)
                    {
                        if (nudge > node)
                            nudge = node;
                        upd -= setNode(nudge, nodeIndex);
                        nodeIndex = 2 * nodeIndex + 1;
                    }
                    else if (quantized > node)
                    {
                        if (node + nudge > NODE_MASK)
                            nudge = NODE_MASK - node;
                        upd += setNode(nudge, nodeIndex);
                        nodeIndex = 2 * nodeIndex + 2;
                    }
                    else
                    {
                        nodeIndex = 2 * nodeIndex + 1;
                    }
                }

                if ((upd & WARMUP_COUNT_MASK) != WARMUP_COUNT_MASK)
                {
                    if (quantilesUpdater.compareAndSet(this, cur, upd))
                        return levels > 1 ? nodeIndex - 3 : uniformBucket(quantized);
                }
                else
                {
                    long warmed = finishWarmup(upd);
                    if (quantilesUpdater.compareAndSet(this, cur, warmed))
                        return levels > 1 ? nodeIndex - 3 : uniformBucket(quantized);
                }

                cur = quantiles;
            }
        }

        // while warming up, ensure we don't mix wildly different stale requests
        private int uniformBucket(long quantized)
        {
            return (int) ((quantized * unreservedBuckets()) >>> NODE_BITS);
        }

        private long finishWarmup(long cur)
        {
            long n0 = node(cur, 0);
            long n1 = node(cur, 1);
            long n2 = node(cur, 2);

            long upd = MODE_BIT;
            upd |= setNode(n0, 0);
            upd |= setNode(n1, 1);
            upd |= setNode(n2, 2);
            upd |= setNode(n1, 3);
            upd |= setNode((n0 + n1)/2, 4);
            upd |= setNode((n0 + n2)/2, 5);
            upd |= setNode(n2, 6);
            return upd;
        }

        private static long warmupStep(long median, long count)
        {
            long fromMedian = median >> Math.min(3 + (count >> 2), NODE_BITS);
            long fromCount = (1L << NODE_BITS) >> Math.min(count >> 2, NODE_BITS);
            return Math.max(1, (fromMedian + fromCount) >> 1);
        }

        private static int nodeShift(int nodeIndex)
        {
            return nodeIndex * NODE_BITS;
        }

        private static long setNode(long bits, int nodeIndex)
        {
            return bits << nodeShift(nodeIndex);
        }

        @VisibleForTesting
        static long node(long packed, int nodeIndex)
        {
            return (packed >>> nodeShift(nodeIndex)) & NODE_MASK;
        }

        long quantiles()
        {
            return quantiles;
        }
    }
}
