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

import org.junit.jupiter.api.Test;

import org.assertj.core.api.Assertions;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

class GensTest
{
    private static final Answer REJECT_ALL = ignore -> {
        throw new UnsupportedOperationException(ignore.toString());
    };

    @Test
    void intMixedDistributionWithBucketsOutOfBounds()
    {
        Assertions.assertThatThrownBy(() -> Gens.mixedDistribution(0, 100, 101))
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessage("Num buckets must be between 1 and 100; given 101");
    }

    @Test
    void intMixedDistributionWithBucketsUniform()
    {
        Gen<Gen.IntGen> distroGren = Gens.mixedDistribution(0, 100, 10);
        RandomSource rs = Mockito.mock(RandomSource.class, REJECT_ALL);
        Mockito.doReturn(0).when(rs).nextInt(0, 4); // uniform disto for bucket selection
        Mockito.doReturn(0).when(rs).nextInt(0, 2); // uniform distro for within the bucket

        Gen.IntGen gen = distroGren.next(rs);

        // to avoid conflicts, need to have i=0 as its own thing
        Mockito.reset(rs);
        Mockito.doReturn(0, 42).when(rs).nextInt(0, 10);
        Assertions.assertThat(gen.next(rs)).isEqualTo(42);

        for (int i = 1; i < 10; i++)
        {
            int start = i * 10;
            int end = start + 10;
            Mockito.reset(rs);
            Mockito.doReturn(i).when(rs).nextInt(0, 10);
            Mockito.doReturn(42).when(rs).nextInt(start, end); // this is the value returned
            Assertions.assertThat(gen.next(rs)).isEqualTo(42);
        }
    }

    @Test
    void intMixedDistributionWithBucketsMedian()
    {
        Gen<Gen.IntGen> distroGren = Gens.mixedDistribution(0, 100, 10);
        RandomSource rs = Mockito.mock(RandomSource.class, REJECT_ALL);
        Mockito.doReturn(0).when(rs).nextInt(0, 4); // uniform disto for bucket selection
        Mockito.doReturn(1).when(rs).nextInt(0, 2); // median distro for within the bucket
        // populate the median buckets
        for (int start = 0; start < 100; start += 10)
            Mockito.doReturn(start).when(rs).nextInt(start, start + 10);

        Gen.IntGen gen = distroGren.next(rs);

        for (int i = 1; i < 10; i++)
        {
            int start = i * 10;
            int end = start + 10;
            Mockito.reset(rs);
            Mockito.doReturn(i).when(rs).nextInt(0, 10);
            Mockito.doReturn(42).when(rs).nextBiasedInt(start, start, end); // this is the value returned
            Assertions.assertThat(gen.next(rs)).isEqualTo(42);
        }
    }
}