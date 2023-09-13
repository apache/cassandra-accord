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
import accord.utils.RandomSource;

public class IntRange implements Gen.LongGen
{
    public final int min, max;
    private final int maxDelta;

    public IntRange(int min, int max)
    {
        if (min >= max) throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", min, max));
        this.min = min;
        this.max = max;
        this.maxDelta = max - min + 1;
    }

    @Override
    public long nextLong(RandomSource randomSource)
    {
        return min + randomSource.nextInt(maxDelta);
    }
}
