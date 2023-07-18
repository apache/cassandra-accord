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

package accord.local;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.ToLongFunction;

import accord.primitives.Timestamp;
import accord.utils.Invariants;

public interface NodeTimeService
{
    Node.Id id();
    long epoch();

    /**
     * Current time in some time unit that may be simulated and not match system time
     */
    long now();

    /**
     * Return the current time since the Unix epoch in the specified time unit. May still be simulated time and not
     * real time.
     *
     * Throws IllegalArgumentException for TimeUnit.NANOSECONDS because a long isn't big enough.
     */
    long unix(TimeUnit unit);

    Timestamp uniqueNow(Timestamp atLeast);

    static ToLongFunction<TimeUnit> unixWrapper(TimeUnit sourceUnit, LongSupplier nowSupplier)
    {
        return resultUnit -> {
            Invariants.checkArgument(resultUnit != TimeUnit.NANOSECONDS, "Nanoseconds since epoch doesn't fit in a long");
            return resultUnit.convert(nowSupplier.getAsLong(), sourceUnit);
        };
    }
}
