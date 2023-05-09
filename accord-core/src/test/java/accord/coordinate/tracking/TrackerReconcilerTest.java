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

package accord.coordinate.tracking;

import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;

import accord.topology.Topologies;
import accord.utils.RandomSource;

public class TrackerReconcilerTest
{
    @Test
    public void testReadTracker()
    {
        test(10000, ReadTrackerReconciler::new, 1);
    }

    @Test
    public void testQuorumTracker()
    {
        test(10000, QuorumTrackerReconciler::new);
    }
    
    @Test
    public void testFastPathTracker()
    {
        test(10000, FastPathTrackerReconciler::new);
    }

    @Test
    public void testRecoveryTracker()
    {
        test(10000, RecoveryTrackerReconciler::new);
    }

    @Test
    public void testInvalidationTracker()
    {
        test(10000, InvalidationTrackerReconciler::new);
    }

    static <ST extends ShardTracker, T extends AbstractTracker<ST, ?>, E extends Enum<E>>
    void test(int count, BiFunction<RandomSource, Topologies, ? extends TrackerReconciler<ST, T, E>> constructor)
    {
        long seed = System.currentTimeMillis();
        while (--count >= 0)
            test(seed++, constructor);
    }

    static <ST extends ShardTracker, T extends AbstractTracker<ST, ?>, E extends Enum<E>>
    void test(long seed, BiFunction<RandomSource, Topologies, ? extends TrackerReconciler<ST, T, E>> constructor)
    {
        test(seed, constructor, Integer.MAX_VALUE);
    }

    static <ST extends ShardTracker, T extends AbstractTracker<ST, ?>, E extends Enum<E>>
    void test(long seed, BiFunction<RandomSource, Topologies, ? extends TrackerReconciler<ST, T, E>> constructor, int maxTopologies)
    {
        for (TrackerReconciler<?, ?, ?> test : TrackerReconciler.generate(seed, constructor, maxTopologies))
            test.test();
    }
}
