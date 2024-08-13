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

import com.google.common.annotations.VisibleForTesting;

import accord.primitives.Timestamp;

public interface NodeTimeService
{
    Node.Id id();
    long epoch();

    /**
     * Current time in some time unit that may be simulated and not match system time
     */
    long now();

    /**
     * Return the current time since some arbitrary epoch in the specified time unit. May be simulated time and not
     * real time. This clock should not go backwards, nor should it generally correct for clock skew - it should
     * only track time elapsed between two points.
     */
    long elapsed(TimeUnit unit);

    Timestamp uniqueNow(Timestamp atLeast);

    static ToLongFunction<TimeUnit> elapsedWrapperFromMonotonicSource(TimeUnit sourceUnit, LongSupplier monotonicNowSupplier)
    {
        long epoch = Math.max(0, monotonicNowSupplier.getAsLong() - sourceUnit.convert(1L, TimeUnit.DAYS));
        return resultUnit ->  resultUnit.convert(monotonicNowSupplier.getAsLong() - epoch, sourceUnit);
    }

    /**
     * Allow time progression to be controlled using a potentially non-monotonic time source for testing with simulated
     * time sources. This is not designed to be fast and real usage should be with a monotonic time source.
     */
    @VisibleForTesting
    static ToLongFunction<TimeUnit> elapsedWrapperFromNonMonotonicSource(TimeUnit sourceUnit, LongSupplier nonMonotonicNowSupplier)
    {
        return elapsedWrapperFromMonotonicSource(sourceUnit, new MonotonicWrapper(sourceUnit, nonMonotonicNowSupplier));
    }

    class MonotonicWrapper implements LongSupplier
    {
       private final LongSupplier nowSupplier;
       private long lastNow = Long.MIN_VALUE;
       private long delta = 0;

       // Use an arbitrary epoch
       private final long epoch;

       private MonotonicWrapper(TimeUnit nowUnit, LongSupplier nowSupplier)
       {
           this.nowSupplier = nowSupplier;
           // pick an arbitrary epoch we can safely convert back to any time unit from the source unit
           this.epoch = nowUnit.convert(Long.MAX_VALUE / 4, TimeUnit.NANOSECONDS);
       }

       @Override
       public synchronized long getAsLong()
       {
           // Only use now as a source of forward progression
           long now = nowSupplier.getAsLong();
           // If it moves backwards, remember how far backwards and always add that
           if (now < lastNow)
               delta = lastNow - now;
           lastNow = now;

           return now + delta + epoch;
       }
    };
}
