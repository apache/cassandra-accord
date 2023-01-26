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

import java.util.Random;

public class Randoms
{
    private Randoms() {}

    public static int nextInt(Random random, int min, int max)
    {
        if (min >= max) throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", min, max));
        return min + random.nextInt(max - min + 1);
    }

    public static long nextLong(Random random, long min, long max)
    {
        if (min >= max) throw new IllegalArgumentException(String.format("Min (%s) should be less than max (%d).", min, max));

        long delta = max - min;
        if (delta < Integer.MAX_VALUE) return min + random.nextInt(((int) delta) + 1);
        if (delta == 1) return min;
        if (delta == Long.MIN_VALUE && max == Long.MAX_VALUE) return random.nextLong();
        if (delta < 0) return random.longs(min, max).iterator().nextLong();
        //            if (delta <= Integer.MAX_VALUE) return min + uniform(0, (int) delta);

        long result = min + 1 == max ? min : min + ((random.nextLong() & 0x7fffffff) % (max - min));
        assert result >= min && result < max;
        return result;
    }
}
