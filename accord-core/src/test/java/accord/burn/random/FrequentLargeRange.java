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
import accord.utils.Gen.LongGen;
import accord.utils.Gens;
import accord.utils.RandomSource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class FrequentLargeRange implements LongGen
{
    private final LongGen small, large;
    private final Gen<Boolean> runs;

    public FrequentLargeRange(LongGen small, LongGen large, double ratio)
    {
        this.small = small;
        this.large = large;
        this.runs = Gens.bools().runs(ratio);
    }

    @Override
    public long nextLong(RandomSource randomSource)
    {
        if (runs.next(randomSource)) return large.nextLong(randomSource);
        else                         return small.nextLong(randomSource);
    }

    public static Builder builder(RandomSource randomSource)
    {
        return new Builder(randomSource);
    }

    public static class Builder
    {
        private final RandomSource random;
        private Double ratio;
        private LongGen small, large;

        public Builder(RandomSource random)
        {
            this.random = random;
        }

        public Builder raitio(double ratio)
        {
            this.ratio = ratio;
            return this;
        }

        public Builder raitio(int min, int max)
        {
            this.ratio = ratio = random.nextInt(min, max) / 100.0D;
            return this;
        }

        public Builder small(Duration min, Duration max)
        {
            small = create(min, max);
            return this;
        }

        public Builder small(long min, long max, TimeUnit unit)
        {
            small = create(min, max, unit);
            return this;
        }

        public Builder small(long min, TimeUnit minUnit, long max, TimeUnit maxUnit)
        {
            small = create(min, minUnit, max, maxUnit);
            return this;
        }

        public Builder large(Duration min, Duration max)
        {
            large = create(min, max);
            return this;
        }

        public Builder large(long min, long max, TimeUnit unit)
        {
            large = create(min, max, unit);
            return this;
        }

        public Builder large(long min, TimeUnit minUnit, long max, TimeUnit maxUnit)
        {
            large = create(min, minUnit, max, maxUnit);
            return this;
        }

        private RandomWalkRange create(Duration min, Duration max)
        {
            return new RandomWalkRange(random, min.toNanos(), max.toNanos());
        }

        private RandomWalkRange create(long min, long max, TimeUnit unit)
        {
            return create(min, unit, max, unit);
        }

        private RandomWalkRange create(long min, TimeUnit minUnit, long max, TimeUnit maxUnit)
        {
            return new RandomWalkRange(random, minUnit.toNanos(min), maxUnit.toNanos(max));
        }

        public FrequentLargeRange build()
        {
            if (small == null)
                throw new IllegalStateException("Small range undefined");
            if (large == null)
                throw new IllegalStateException("Large range undefined");
            if (ratio == null)
                raitio(1, 11);
            return new FrequentLargeRange(small, large, ratio);
        }
    }
}
