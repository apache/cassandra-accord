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
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomSourceTest
{
    private static final Logger logger = LoggerFactory.getLogger(RandomSourceTest.class);

    @Test
    public void testBiasedInts()
    {
        RandomSource random = RandomSource.wrap(new Random());
        long seed = random.nextLong();
        logger.info("Seed: {}", seed);
        random.setSeed(seed);
        testBiasedInts(random, 1000, 100000, 0.01, 0.1);
    }

    private void testBiasedInts(RandomSource random, int tests, int perTest, double fudge, double perTestFudge)
    {
        int[] buffer = new int[perTest];
        double overallDrift = 0;
        for (int c1 = 0 ; c1 < tests ; ++ c1)
        {
            int median = random.nextInt(1, 100000);
            int min = random.nextInt(0, median);
            int max = random.nextInt(median + 1, 100001);
            overallDrift += testOneBiasedInts(random, min, median, max, buffer, perTestFudge);
        }
        overallDrift /= tests;
        Assertions.assertTrue(overallDrift < fudge);
        Assertions.assertTrue(overallDrift > -fudge);
        System.out.println(overallDrift);
    }

    private double testOneBiasedInts(RandomSource random, int min, int median, int max, int[] results, double fudge)
    {
        for (int i = 0 ; i < results.length ; i++)
            results[i] = random.nextBiasedInt(min, median, max);

        Arrays.sort(results);
        int i = Arrays.binarySearch(results, median);
        if (i < 0) i = -1 - i;
        int j = Arrays.binarySearch(results, median + 1);
        if (j < 0) j = -2 - j;
        else --j;
        i -= results.length/2;
        j -= results.length/2;

        // find minimum distance of the target median value from the actual median value
        double distance = Math.abs(i) < Math.abs(j) ? i : j;
        double ratio = distance / results.length;
        Assertions.assertTrue(ratio < fudge);
        return ratio;
    }

    @Test
    public void testBiasedLongs()
    {
        RandomSource random = RandomSource.wrap(new Random());
        long seed = random.nextLong();
        logger.info("Seed: {}", seed);
        random.setSeed(seed);
        testBiasedLongs(random, 1000, 100000, 0.01, 0.1);
    }

    private void testBiasedLongs(RandomSource random, int tests, int perTest, double fudge, double perTestFudge)
    {
        long[] buffer = new long[perTest];
        double overallDrift = 0;
        for (int c1 = 0 ; c1 < tests ; ++ c1)
        {
            int median = random.nextInt(1, 100000);
            int min = random.nextInt(0, median);
            int max = random.nextInt(median + 1, 100001);
            overallDrift += testOneBiasedLongs(random, min, median, max, buffer, perTestFudge);
        }
        overallDrift /= tests;
        Assertions.assertTrue(overallDrift < fudge);
        Assertions.assertTrue(overallDrift > -fudge);
        System.out.println(overallDrift);
    }

    private double testOneBiasedLongs(RandomSource random, int min, int median, int max, long[] results, double fudge)
    {
        for (int i = 0 ; i < results.length ; i++)
            results[i] = random.nextBiasedInt(min, median, max);

        Arrays.sort(results);
        int i = Arrays.binarySearch(results, median);
        if (i < 0) i = -1 - i;
        int j = Arrays.binarySearch(results, median + 1);
        if (j < 0) j = -2 - j;
        else --j;
        i -= results.length/2;
        j -= results.length/2;

        // find minimum distance of the target median value from the actual median value
        double distance = Math.abs(i) < Math.abs(j) ? i : j;
        double ratio = distance / results.length;
        Assertions.assertTrue(ratio < fudge);
        return ratio;
    }
}
