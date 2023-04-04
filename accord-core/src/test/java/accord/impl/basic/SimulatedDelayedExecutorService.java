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

package accord.impl.basic;

import java.util.concurrent.TimeUnit;

import accord.burn.random.FrequentLargeRange;
import accord.burn.random.RandomLong;
import accord.burn.random.RandomWalkRange;
import accord.utils.RandomSource;

public class SimulatedDelayedExecutorService extends TaskExecutorService
{
    private final PendingQueue pending;
    private final RandomSource random;
    private final RandomLong jitterInNano;

    public SimulatedDelayedExecutorService(PendingQueue pending, RandomSource random)
    {
        this.pending = pending;
        this.random = random;
        // this is different from Apache Cassandra Simulator as this is computed differently for each executor
        // rather than being a global config
        double ratio = random.nextInt(1, 11) / 100.0D;
        this.jitterInNano = new FrequentLargeRange(new RandomWalkRange(random, microToNanos(0), microToNanos(50)),
                                                   new RandomWalkRange(random, microToNanos(50), msToNanos(5)),
                                                   ratio);
    }

    private static int msToNanos(int value)
    {
        return Math.toIntExact(TimeUnit.MILLISECONDS.toNanos(value));
    }

    private static int microToNanos(int value)
    {
        return Math.toIntExact(TimeUnit.MICROSECONDS.toNanos(value));
    }

    @Override
    public void execute(Task<?> task)
    {
        pending.add(task, jitterInNano.getLong(random), TimeUnit.NANOSECONDS);
    }
}