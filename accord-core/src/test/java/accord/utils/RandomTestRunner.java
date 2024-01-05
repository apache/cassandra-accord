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

import java.util.function.Consumer;

/**
 * The main difference between this class and {@link Property} is that this class does not reference {@link Gen} and some
 * authors wish to avoid that interface, but need to correctly work with randomness in a test that is reproducable when
 * issues are found from CI.
 *
 * {@link Builder#check(Consumer)} is functinally the same as the following from {@link Property}
 *
 * {@code qt().withExamples(1).check(testcase); }
 */
public class RandomTestRunner
{
    public static Builder test()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Long seed = null;

        /**
         * When a test failure is detected, set the seed for the test using this method to cause it to get repeated
         */
        @SuppressWarnings("unused")
        public Builder withSeed(long seed)
        {
            this.seed = seed;
            return this;
        }

        public void check(Consumer<RandomSource> fn)
        {
            RandomSource random = new DefaultRandom();
            if (seed == null)
                seed = random.nextLong();
            random.setSeed(seed);
            try
            {
                fn.accept(random);
            }
            catch (Throwable t)
            {
                throw new AssertionError("Unexpected error for seed " + seed, t);
            }
        }
    }
}
