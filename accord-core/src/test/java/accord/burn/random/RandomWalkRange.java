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

import accord.utils.Gen.LongGen;
import accord.utils.RandomSource;

public class RandomWalkRange implements LongGen
{
    public final long min, max;
    private final long maxStepSize;
    long cur;

    public RandomWalkRange(RandomSource random, long min, long max)
    {
        this.min = min;
        this.max = max;
        this.maxStepSize = maxStepSize(random, min, max);
        this.cur = random.nextLong(min, max + 1);
    }

    @Override
    public long nextLong(RandomSource randomSource)
    {
        long step = randomSource.nextLong(-maxStepSize, maxStepSize + 1);
        long cur = this.cur;
        this.cur = step > 0 ? Math.min(max, cur + step)
                            : Math.max(min, cur + step);
        return cur;
    }

    private static long maxStepSize(RandomSource random, long min, long max)
    {
        switch (random.nextInt(3))
        {
            case 0:
                return Math.max(1, (max/32) - (min/32));
            case 1:
                return Math.max(1, (max/256) - (min/256));
            case 2:
                return Math.max(1, (max/2048) - (min/2048));
            default:
                return Math.max(1, (max/16384) - (min/16384));
        }
    }
}
