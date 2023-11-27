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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.utils.RandomTestRunner.test;

public class RandomSourceTest
{
    private static final Logger logger = LoggerFactory.getLogger(RandomSourceTest.class);

    @Test
    public void testBiasedInts()
    {
        test().check(random -> testBiasedInts(random, 1000, 100000, 0.01, 0.1));
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
        logger.info("{}", overallDrift);
    }

    private double testOneBiasedInts(RandomSource random, int min, int median, int max, int[] results, double fudge)
    {
        for (int i = 0 ; i < results.length ; i++)
            results[i] = random.nextBiasedInt(min, median, max);

        Arrays.sort(results);
        int i = firstBinarySearch(results, median);
        if (i < 0) i = -1 - i;
        int j = firstBinarySearch(results, median + 1);
        if (j < 0) j = -2 - j;
        else --j;
        i -= results.length/2;
        j -= results.length/2;

        // find minimum distance of the target median value from the actual median value
        double distance = Math.abs(i) < Math.abs(j) ? i : j;
        double ratio = distance / results.length;
        Assertions.assertTrue(ratio < fudge, () -> String.format("ratio (%,2f) >= fudge (%,2f); results.length (%,d)", ratio, fudge, results.length));
        return ratio;
    }

    @Test
    public void testBiasedLongs()
    {
        test().check(random -> testBiasedLongs(random, 1000, 100000, 0.01, 0.1));
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
        logger.info("{}", overallDrift);
    }

    private double testOneBiasedLongs(RandomSource random, int min, int median, int max, long[] results, double fudge)
    {
        for (int i = 0 ; i < results.length ; i++)
            results[i] = random.nextBiasedInt(min, median, max);

        Arrays.sort(results);
        int i = firstBinarySearch(results, median);
        if (i < 0) i = -1 - i;
        int j = firstBinarySearch(results, median + 1);
        if (j < 0) j = -2 - j;
        else --j;
        i -= Math.abs(results.length/2);
        j -= Math.abs(results.length/2);

        // find minimum distance of the target median value from the actual median value
        double distance = Math.min(i, j);

        double ratio = distance / results.length;
        Assertions.assertTrue(ratio < fudge, () -> String.format("ratio (%,2f) >= fudge (%,2f); results.length (%,d)", ratio, fudge, results.length));
        return distance / results.length;
    }

    private static int firstBinarySearch(int[] array, int target)
    {
        int i = Arrays.binarySearch(array, target);
        return i > 0 ? ceil(array, target, 0, i) : i;
    }

    private static int firstBinarySearch(long[] array, long target)
    {
        int i = Arrays.binarySearch(array, target);
        return i > 0 ? ceil(array, target, 0, i) : i;
    }

    /**
     * Yields the minimum index in the range <code>a[fromIndex, toIndex)</code> containing a value that is greater than or equal to the provided key.
     * The method requires (but does not check) that the range is sorted in ascending order; a result of toIndex indicates no value greater than or
     * equal to the key exists in the range
     *
     * @param a list to look in, where this.isOrdered(a) holds
     * @param key key to find
     * @param fromIndex first index to look in
     * @param toIndex first index to exclude from search (i.e. exclusive upper bound)
     * @return minimum index in the range containing a value that is greater than or equal to the provided key
     */
    private static int ceil(final int[] a, final int key, final int fromIndex, final int toIndex)
    {
        // This was taken from https://github.com/belliottsmith/jjoost/blob/0b40ae494af408dfecd4527ac9e9d1ec323315e3/jjoost-base/src/org/jjoost/util/order/IntOrder.java#L80
        int i = fromIndex - 1;
        int j = toIndex;

        while (i < j - 1)
        {

            // { a[i] < v ^ a[j] >= v }

            final int m = (i + j) >>> 1;
            final long v = a[m];

            if (v >= key) j = m;
            else i = m;

            // { a[m] >= v  =>        a[j] >= v       =>      a[i] < v ^ a[j] >= v }
            // { a[m] < v   =>        a[i] < v        =>      a[i] < v ^ a[j] >= v }

        }
        return j;
    }

    /**
     * Yields the minimum index in the range <code>a[fromIndex, toIndex)</code> containing a value that is greater than or equal to the provided key.
     * The method requires (but does not check) that the range is sorted in ascending order; a result of toIndex indicates no value greater than or
     * equal to the key exists in the range
     *
     * @param a list to look in, where this.isOrdered(a) holds
     * @param key key to find
     * @param fromIndex first index to look in
     * @param toIndex first index to exclude from search (i.e. exclusive upper bound)
     * @return minimum index in the range containing a value that is greater than or equal to the provided key
     */
    private static int ceil(final long[] a, final long key, final int fromIndex, final int toIndex)
    {
        // This was taken from https://github.com/belliottsmith/jjoost/blob/0b40ae494af408dfecd4527ac9e9d1ec323315e3/jjoost-base/src/org/jjoost/util/order/IntOrder.java#L80
        int i = fromIndex - 1;
        int j = toIndex;

        while (i < j - 1)
        {

            // { a[i] < v ^ a[j] >= v }

            final int m = (i + j) >>> 1;
            final long v = a[m];

            if (v >= key) j = m;
            else i = m;

            // { a[m] >= v  =>        a[j] >= v       =>      a[i] < v ^ a[j] >= v }
            // { a[m] < v   =>        a[i] < v        =>      a[i] < v ^ a[j] >= v }

        }
        return j;
    }
}
