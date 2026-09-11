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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.ToLongFunction;

import accord.utils.SortedArrays;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import accord.local.UniqueTimeService.AtomicUniqueAutoStaleTimes;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;

import static accord.local.UniqueTimeService.AtomicUniqueAutoStaleTimes.node;

/**
 * Randomised validation of {@link AtomicUniqueAutoStaleTimes}.
 *
 * Two layers of properties are checked:
 *  1. Functional contract (post-warmup):
 *       - every {@code uniqueStale} result is strictly greater than {@code greaterThan}
 *       - every result is unique across calls
 *       - every result encodes a valid bucket index in [0, 16)
 *  2. Distribution behaviour (post-warmup):
 *       - for several stationary input distributions, the empirical rank of each of the 7 packed
 *         estimator nodes (over the input sample stream) is approximately its target octile:
 *           heap[0]=p50, heap[1]=p25, heap[2]=p75, heap[3]=p12.5, heap[4]=p37.5, heap[5]=p62.5, heap[6]=p87.5
 *       - across the 16 output buckets the chosen-bucket distribution is approximately uniform
 */
public class AtomicUniqueAutoStaleTimesTest
{
    // mirror production constants
    private static final int NODE_BITS = 9;
    private static final long NODE_MASK = (1L << NODE_BITS) - 1;
    private static final int BUCKETS = 16;
    private static final long BUCKET_LOW_BITS_MASK = 2L * BUCKETS - 1;
    private static final long MODE_BIT = Long.MIN_VALUE;

    /** Target empirical rank for each of the 7 packed nodes in heap order. */
    private static final double[] TARGET = { 0.500, 0.250, 0.750, 0.125, 0.375, 0.625, 0.875 };
    private static final String[] LABEL  = { "p50", "p25", "p75", "p12.5", "p37.5", "p62.5", "p87.5" };

    /** Recover the bucket index from a returned timestamp using the bucket-low-bits invariant
     *  enforced by {@code AtomicUniqueStaleTimes.ensureInBucket}: low bits == (bucket << 1) | 1. */
    private static int bucketOf(long out)
    {
        return (int) ((out & BUCKET_LOW_BITS_MASK) >>> 1);
    }

    private static class FixedTime implements TimeService
    {
        long now;
        @Override public long now() { return now; }
        @Override public long elapsed(TimeUnit unit) { throw new UnsupportedOperationException(); }
    }

    // --------------------------------------------------------------------- functional contract

    /** Post-warmup: uniqueStale returns unique values > greaterThan with valid bucket bits. */
    @Test
    public void functionalContract()
    {
        RandomTestRunner.test().check(rs ->
        {
            FixedTime time = new FixedTime();
            // shift = 0 means age in microseconds is the raw quantized value; ages < 512 avoid clamping
            AtomicUniqueAutoStaleTimes t = new AtomicUniqueAutoStaleTimes(time, 0);
            time.now = 1_000_000_000L;

            int n = 50_000;
            Set<Long> seen = new HashSet<>(n * 2);
            for (int i = 0; i < n; i++)
            {
                long greaterThan = time.now - rs.nextLong(1, NODE_MASK);
                long out = t.uniqueStale(greaterThan);

                Assertions.assertThat(out)
                          .as("uniqueStale must return value > greaterThan (i=%d)", i)
                          .isGreaterThan(greaterThan);

                int bucket = bucketOf(out);
                Assertions.assertThat(bucket)
                          .as("bucket index in [0,16) (i=%d, out=%d)", i, out)
                          .isBetween(0, BUCKETS - 1);

                Assertions.assertThat(seen.add(out))
                          .as("uniqueStale results must be unique (i=%d, out=%d)", i, out)
                          .isTrue();
            }
        });
    }

    // --------------------------------------------------------------------- distribution behaviour

    /** Uniform input over the full quantized range. Uniform is the *adversarial* case for
     *  frugal-1: the restoring force toward each quantile is tiny because the density is flat,
     *  so the estimator wanders more freely. Tolerance reflects this. */
    @Test
    public void octileTracking_uniform()
    {
        // tolerance is in *empirical-rank* units (a fraction in [0,1])
        distributionTest(rs -> rs.nextLong(1, NODE_MASK), 100_000, 0.10);
    }

    /** Narrow uniform: exercises behaviour when octiles are tightly spaced. */
    @Test
    public void octileTracking_narrowUniform()
    {
        distributionTest(rs -> rs.nextLong(100, 400), 100_000, 0.10);
    }

    /** Triangular distribution skewed toward small ages (min of two uniforms). */
    @Test
    public void octileTracking_triangularLowSkew()
    {
        distributionTest(rs -> Math.min(rs.nextLong(1, NODE_MASK), rs.nextLong(1, NODE_MASK)),
                         100_000, 0.10);
    }

    /** Bimodal mixture: the algorithmic median is degenerate (a flat region in the CDF) but
     *  the estimator must still settle inside that region and the outer octiles must track. */
    @Test
    public void octileTracking_bimodal()
    {
        distributionTest(rs -> rs.nextBoolean() ? rs.nextLong(40, 140)
                                                : rs.nextLong(300, 460),
                         100_000, 0.10);
    }

    /** Smooth bell-shaped distribution (sum of 4 uniforms ~ approx Gaussian by CLT). This is
     *  the *favourable* case for frugal-1: restoring force at each quantile is strong because
     *  the density there is high, so the estimator concentrates more tightly. */
    @Test
    public void octileTracking_smoothBell()
    {
        distributionTest(rs -> {
            long s = 0;
            for (int k = 0; k < 4; k++) s += rs.nextLong(0, NODE_MASK);
            return s / 4; // mean ~ 255, sd ~ 73
        }, 100_000, 0.12);
    }

    /**
     * Drives {@code n} samples from {@code ageGen} through {@code uniqueStale} and asserts that for
     * each of the 7 internal nodes the fraction of input samples whose quantized age is below the
     * node's value (mid-rank, ties counted at half-mass) is within {@code tol} of the target octile.
     */
    private static void distributionTest(ToLongFunction<RandomSource> ageGen, int n, double tol)
    {
        RandomTestRunner.test().check(rs ->
        {
            FixedTime time = new FixedTime();
            AtomicUniqueAutoStaleTimes t = new AtomicUniqueAutoStaleTimes(time, 0);
            time.now = 1_000_000_000L;

            long[] samples = new long[n];
            for (int i = 0; i < n; i++)
            {
                // clip to the representable quantized range so the empirical CDF and the estimator's
                // view of the stream agree (no silent clamping bias)
                long age = Math.max(0, Math.min(NODE_MASK, ageGen.applyAsLong(rs)));
                samples[i] = age;
                t.uniqueStale(time.now - age);
            }

            long packed = t.quantiles();
            Assertions.assertThat(packed)
                      .as("must remain in normal mode for n=%d", n)
                      .isNegative();

            long[] sorted = samples.clone();
            Arrays.sort(sorted);

            for (int i = 0; i < 7; i++)
            {
                long v = node(packed, i);
                double rank = empiricalRankOf(sorted, v);
                double err = rank - TARGET[i];
                Assertions.assertThat(Math.abs(err))
                          .as("node[%d] %s rank=%.4f target=%.4f err=%+.4f tol=%.3f",
                              i, LABEL[i], rank, TARGET[i], err, tol)
                          .isLessThanOrEqualTo(tol);
            }
        });
    }

    /** Mid-rank empirical CDF: fraction of {@code sorted} strictly below {@code v}, plus half the
     *  mass exactly equal to {@code v}. Robust to the heavy ties produced by quantization. */
    private static double empiricalRankOf(long[] sorted, long v)
    {
        // first index with sorted[idx] >= v
        int firstGe = SortedArrays.binarySearch(sorted, 0, sorted.length, v, SortedArrays.Search.CEIL);
        int firstGt;
        if (firstGe < 0)
        {
            firstGe = -1 - firstGe;
            firstGt = firstGe;
        }
        else
        {
            firstGt = SortedArrays.binarySearch(sorted, firstGe + 1, sorted.length, v + 1, SortedArrays.Search.CEIL);
            if (firstGt < 0) firstGt = -1 - firstGt;
        }

        return (firstGe + 0.5 * (firstGt - firstGe)) / sorted.length;
    }

    /**
     * Once converged, a uniform input must spread evenly across the 8 estimator leaves
     * (each leaf = a pair of adjacent output buckets). Within a leaf, the split between its
     * two buckets is determined by the midpoint of the leaf's [lb, ub] range; for the two
     * boundary leaves (0+1 and 14+15) {@code lb}/{@code ub} are linearly extrapolated below 0
     * / above NODE_MASK, which inflates the outermost buckets relative to their inner siblings
     * when the data is saturated at the range's extremes. We therefore assert:
     *   - each of the 8 leaf-pairs receives ~1/8 of traffic (tight tolerance), AND
     *   - no individual bucket is starved or runaway (loose tolerance).
     */
    @Test
    public void bucketEquidistribution()
    {
        RandomTestRunner.test().check(rs ->
        {
            FixedTime time = new FixedTime();
            AtomicUniqueAutoStaleTimes t = new AtomicUniqueAutoStaleTimes(time, 0);
            time.now = 1_000_000_000L;

            // let the estimator settle on the actual octiles before measuring
            for (int i = 0; i < 20_000; i++)
                t.uniqueStale(time.now - rs.nextLong(1, NODE_MASK));

            int n = 200_000;
            int[] hits = new int[BUCKETS];
            for (int i = 0; i < n; i++)
            {
                long out = t.uniqueStale(time.now - rs.nextLong(1, NODE_MASK));
                hits[bucketOf(out)]++;
            }

            // (1) leaf-pair equidistribution: each pair of adjacent buckets is one estimator
            //     leaf and should get exactly ~1/8 of traffic (this is the property the design
            //     actually delivers, independent of the lb/ub extrapolation skew).
            double leafExpected = n / 8.0; // 25_000
            double leafTol = 0.10 * leafExpected;
            for (int leaf = 0; leaf < 8; leaf++)
            {
                double leafHits = hits[2 * leaf] + hits[2 * leaf + 1];
                Assertions.assertThat(leafHits)
                          .as("leaf[%d] (buckets %d+%d) hits=%.0f expected≈%.0f distribution=%s",
                              leaf, 2 * leaf, 2 * leaf + 1, leafHits, leafExpected, Arrays.toString(hits))
                          .isCloseTo(leafExpected, Offset.offset(leafTol));
            }

            // (2) per-bucket sanity: no bucket is starved or exploded. Inner buckets sit close
            //     to the per-bucket expectation (n/16); the four boundary buckets (0, 1, 14, 15)
            //     are looser due to the documented lb/ub extrapolation effect.
            double bucketExpected = n / (double) BUCKETS;
            for (int i = 0; i < BUCKETS; i++)
            {
                boolean boundary = (i <= 1) || (i >= BUCKETS - 2);
                double tol = (boundary ? 0.75 : 0.25) * bucketExpected;
                Assertions.assertThat((double) hits[i])
                          .as("bucket[%d]%s hits=%d expected≈%.0f distribution=%s",
                              i, boundary ? " (boundary)" : "", hits[i], bucketExpected, Arrays.toString(hits))
                          .isCloseTo(bucketExpected, Offset.offset(tol));
            }
        });
    }

    @Test
    public void warmupCompletes()
    {
        RandomTestRunner.test().check(rs ->
        {
            FixedTime time = new FixedTime();
            AtomicUniqueAutoStaleTimes t = new AtomicUniqueAutoStaleTimes(time, 0);
            time.now = 1_000_000_000L;
            Assertions.assertThat(t.quantiles()).isGreaterThanOrEqualTo(0L);

            for (int i = 0; i < 5_000; i++)
            {
                t.uniqueStale(time.now - rs.nextLong(1, NODE_MASK));
            }

            Assertions.assertThat(t.quantiles()).as("warmup must complete").isNegative();
        });
    }
}
