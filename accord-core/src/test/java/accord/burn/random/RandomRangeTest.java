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
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static accord.utils.Property.qt;

class RandomRangeTest
{
    @Test
    void intRange()
    {
        test((ignore, min, max) -> new IntRange(min, max));
    }

    @Test
    void randomWalkRange()
    {
        test(RandomWalkRange::new);
    }

    void test(Factory factory)
    {
        int samples = 1000;
        qt().forAll(Gens.random(), ranges()).check((rs, range) -> {
            Gen.LongGen randRange = factory.create(rs, range.min, range.max);
            for (int i = 0; i < samples; i++)
                Assertions.assertThat(randRange.nextLong(rs)).isBetween((long) range.min, (long) range.max);
        });
    }

    private static Gen<Range> ranges()
    {
        Range range = new Range();
        Gen<Integer> numGen = Gens.ints().between(0, 1000);
        int[] buffer = new int[2];
        return rs -> {
            buffer[0] = numGen.next(rs);
            do
            {
                buffer[1] = numGen.next(rs);
            }
            while (buffer[0] == buffer[1]);
            Arrays.sort(buffer);
            range.min = buffer[0];
            range.max = buffer[1];
            return range;
        };
    }

    private static class Range
    {
        int min, max;
    }

    private interface Factory
    {
        Gen.LongGen create(RandomSource random, int min, int max);
    }
}