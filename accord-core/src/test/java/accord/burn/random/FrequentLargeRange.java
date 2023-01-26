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

import accord.utils.Invariants;

import java.util.Random;

public class FrequentLargeRange implements RandomLong
{
    private final RandomLong small, large;
    private final double ratio;
    private final int steps;
    private final double lower, upper;
    private int run = -1;
    private long smallCount = 0, largeCount = 0;

    public FrequentLargeRange(RandomLong small, RandomLong large, double ratio)
    {
        Invariants.checkArgument(ratio > 0 && ratio <= 1);
        this.small = small;
        this.large = large;
        this.ratio = ratio;
        this.steps = (int) (1 / ratio);
        this.lower = ratio * .8;
        this.upper = ratio * 1.2;
    }

    @Override
    public long getLong(Random randomSource)
    {
        if (run != -1)
        {
            run--;
            largeCount++;
            return large.getLong(randomSource);
        }
        double currentRatio = largeCount / (double) (smallCount + largeCount);
        if (currentRatio < lower)
        {
            // not enough large
            largeCount++;
            return large.getLong(randomSource);
        }
        if (currentRatio > upper)
        {
            // not enough small
            smallCount++;
            return small.getLong(randomSource);
        }
        if (randomSource.nextDouble() < ratio)
        {
            run = randomSource.nextInt(steps);
            run--;
            largeCount++;
            return large.getLong(randomSource);
        }
        smallCount++;
        return small.getLong(randomSource);
    }
}
