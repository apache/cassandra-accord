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

import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public interface RandomSource extends JDKRandomAPI
{
    static RandomSource wrap(Random random)
    {
        return new WrappedRandomSource(random);
    }

    RandomSource fork();

    default IntStream ints()
    {
        return IntStream.generate(this::nextInt);
    }

    default IntStream ints(int maxExclusive)
    {
        return IntStream.generate(() -> nextInt(maxExclusive));
    }

    default IntStream ints(int minInclusive, int maxExclusive)
    {
        return IntStream.generate(() -> nextInt(minInclusive, maxExclusive));
    }

    @Override
    default int nextInt(int maxExclusive)
    {
        return nextInt(0, maxExclusive);
    }

    default int nextInt(int minInclusive, int maxExclusive)
    {
        // this is diff behavior than ThreadLocalRandom, which returns nextInt
        if (minInclusive >= maxExclusive)
            throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", minInclusive, maxExclusive));

        int result = nextInt();
        int delta = maxExclusive - minInclusive;
        int mask = delta - 1;
        if ((delta & mask) == 0) // power of two
            result = (result & mask) + minInclusive;
        else if (delta > 0)
        {
            // reject over-represented candidates
            for (int u = result >>> 1;                  // ensure nonnegative
                 u + mask - (result = u % delta) < 0;   // rejection check
                 u = nextInt() >>> 1)                   // retry
                ;
            result += minInclusive;
        }
        else
        {
            // range not representable as int
            while (result < minInclusive || result >= maxExclusive)
                result = nextInt();
        }
        return result;
    }

    default LongStream longs()
    {
        return LongStream.generate(this::nextLong);
    }

    default LongStream longs(long maxExclusive)
    {
        return LongStream.generate(() -> nextLong(maxExclusive));
    }

    default LongStream longs(long minInclusive, long maxExclusive)
    {
        return LongStream.generate(() -> nextLong(minInclusive, maxExclusive));
    }

    default long nextLong(long maxExclusive)
    {
        return nextLong(0, maxExclusive);
    }

    default long nextLong(long minInclusive, long maxExclusive)
    {
        // this is diff behavior than ThreadLocalRandom, which returns nextLong
        if (minInclusive >= maxExclusive)
            throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", minInclusive, maxExclusive));

        long result = nextLong();
        long delta = maxExclusive - minInclusive;
        long mask = delta - 1;
        if ((delta & mask) == 0L) // power of two
            result = (result & mask) + minInclusive;
        else if (delta > 0L)
        {
            // reject over-represented candidates
            for (long u = result >>> 1;                 // ensure nonnegative
                 u + mask - (result = u % delta) < 0L;  // rejection check
                 u = nextLong() >>> 1)                  // retry
                ;
            result += minInclusive;
        }
        else
        {
            // range not representable as long
            while (result < minInclusive || result >= maxExclusive)
                result = nextLong();
        }
        return result;
    }

    default DoubleStream doubles()
    {
        return DoubleStream.generate(this::nextDouble);
    }

    default DoubleStream doubles(double maxExclusive)
    {
        return DoubleStream.generate(() -> nextDouble(maxExclusive));
    }

    default DoubleStream doubles(double minInclusive, double maxExclusive)
    {
        return DoubleStream.generate(() -> nextDouble(minInclusive, maxExclusive));
    }

    default double nextDouble(double maxExclusive)
    {
        return nextDouble(0, maxExclusive);
    }

    default double nextDouble(double minInclusive, double maxExclusive) {
        if (minInclusive >= maxExclusive)
            throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", minInclusive, maxExclusive));

        double result = nextDouble();
        result = result * (maxExclusive - minInclusive) + minInclusive;
        if (result >= maxExclusive) // correct for rounding
            result = Double.longBitsToDouble(Double.doubleToLongBits(maxExclusive) - 1);
        return result;
    }

    default boolean decide(float chance)
    {
        return nextFloat() < chance;
    }

    default boolean decide(double chance)
    {
        return nextDouble() < chance;
    }

    default long reset()
    {
        long seed = nextLong();
        setSeed(seed);
        return seed;
    }
}
