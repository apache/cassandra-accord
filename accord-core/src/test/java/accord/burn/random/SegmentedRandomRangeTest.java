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

package accord.burn.random;

import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.agrona.collections.Long2LongHashMap;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.LongStream;

import static accord.utils.Property.qt;

class SegmentedRandomRangeTest
{
    enum Type
    {
        IntRange,
        RandomWalkRange
    }
    
    private static final class TestCase
    {
        private final int minSmall, maxSmall, minLarge, maxLarge, largeRatio;
        private final Type type;

        private TestCase(int minSmall, int maxSmall, int minLarge, int maxLarge, int largeRatio, Type type)
        {
            this.minSmall = minSmall;
            this.maxSmall = maxSmall;
            this.minLarge = minLarge;
            this.maxLarge = maxLarge;
            this.largeRatio = largeRatio;
            this.type = type;
        }

        Gen.LongGen min(RandomSource random)
        {
            return create(random, minSmall, maxSmall);
        }

        Gen.LongGen max(RandomSource random)
        {
            return create(random, minLarge, maxLarge);
        }

        private Gen.LongGen create(RandomSource random, int min, int max)
        {
            switch (type)
            {
                case IntRange: return new IntRange(min, max);
                case RandomWalkRange: return new RandomWalkRange(random, min, max);
                default: throw new UnsupportedOperationException("type " + type);
            }
        }

        double ratio()
        {
            return 1 / (double) largeRatio;
        }

        @Override
        public String toString()
        {
            return "TestCase{" +
                    "minSmall=" + minSmall +
                    ", maxSmall=" + maxSmall +
                    ", minLarge=" + minLarge +
                    ", maxLarge=" + maxLarge +
                    ", largeRatio=" + largeRatio +
                    ", type=" + type +
                    '}';
        }
    }

    @Test
    void disjoint()
    {
        Gen<Integer> range = Gens.ints().between(100, 100000000);
        Gen<Integer> ratio = Gens.ints().between(6, 20);
        Gen<Type> typeGen = Gens.enums().all(Type.class);
        int[] ints = new int[3];
        Gen<TestCase> test = rs -> {
            ints[0] = range.next(rs);
            ints[1] = range.next(rs);
            ints[2] = range.next(rs);
            Arrays.sort(ints);
            int largeRatio = ratio.next(rs);
            return new TestCase(0, ints[0], ints[1], ints[2], largeRatio, typeGen.next(rs));
        };
        qt().forAll(Gens.random(), test).check(SegmentedRandomRangeTest::test);
    }

    private static void test(RandomSource rs, TestCase tc)
    {
        double ratio = tc.ratio();
        FrequentLargeRange period = new FrequentLargeRange(tc.min(rs), tc.max(rs), ratio);
        int numSamples = 1000;
        int maxResamples = 1000;

        double target = ratio * 100.0D;
        double upperBounds = target * 1.2;
        double lowerBounds = target * .8;

        int largeSeq = 0;
        int largeCount = 0;
        Long2LongHashMap largeSeqCounts = new Long2LongHashMap(0);
        int resamples = 0;
        for (int i = 0; i < numSamples; i++)
        {
            long size = period.nextLong(rs);
            if (size > tc.maxSmall)
            {
                largeCount++;
                largeSeq++;
            }
            else
            {
                largeSeqCounts.compute(largeSeq, (ignore, accm) -> accm + 1);
                largeSeq = 0;
            }
            if (i == numSamples - 1)
            {
                // keep going if ratio is off...
                double actual = (double) largeCount / numSamples * 100.0;
                if (actual < lowerBounds || actual  > upperBounds)
                {
                    if (resamples == maxResamples)
                        throw new AssertionError(String.format("Unable to match target rate in %d re-samples; actual=%f, target=%f, bounds=(%f, %f)", resamples, actual, target, lowerBounds, upperBounds));
                    numSamples += 100;
                    resamples++;
                }
            }
        }

        checkSequences(largeSeqCounts);
        assertRatio(numSamples, upperBounds, lowerBounds, largeCount);
    }

    private static void checkSequences(Long2LongHashMap largeSeqCounts)
    {
        long[] keys = new long[largeSeqCounts.size()];
        Long2LongHashMap.KeyIterator it = largeSeqCounts.keySet().iterator();
        int idx = 0;
        while (it.hasNext())
            keys[idx++] = it.nextValue();
        if (LongStream.of(keys).anyMatch(seq -> seq > 5))
            return;
        StringBuilder sb = new StringBuilder("No large sequences detected; saw\n");
        Arrays.sort(keys);
        for (long key : keys)
            sb.append('\t').append(key).append('\t').append(largeSeqCounts.get(key)).append('\n');
        throw new AssertionError(sb.toString());
    }

    private static void assertRatio(int numSamples, double upperBounds, double lowerBounds, long largeCount)
    {
        double largePercent = largeCount / (double) numSamples * 100.0;
        Assertions.assertThat(largePercent)
                .describedAs("Expected ratio was not respected")
                .isBetween(lowerBounds, upperBounds);
    }
}