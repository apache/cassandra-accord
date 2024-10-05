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

import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.primitives.Timestamp;
import accord.topology.TopologyUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeTest
{
    private static Timestamp ts(long epoch, long hlc, int node)
    {
        return Timestamp.fromValues(epoch, hlc, new Node.Id(node));
    }

    @Test
    void uniqueNowTest()
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        try (MockCluster cluster = MockCluster.builder().time(clock).build())
        {
            Node node = cluster.get(1);

            clock.increment();
            Timestamp timestamp1 = node.uniqueNow();
            Timestamp timestamp2 = node.uniqueNow();

            clock.increment();
            clock.increment();
            clock.increment();
            Timestamp timestamp3 = node.uniqueNow();

            Assertions.assertEquals(ts(1, 101, 1), timestamp1);
            Assertions.assertEquals(ts(1, 102, 1), timestamp2);
            Assertions.assertEquals(ts(1, 104, 1), timestamp3);
        }
    }

    @Test
    void uniqueNowEpochUpdate()
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        try (MockCluster cluster = MockCluster.builder().time(clock).build())
        {
            Node node = cluster.get(1);
            MockConfigurationService configService = (MockConfigurationService) node.configService();

            clock.increment();
            Timestamp timestamp1 = node.uniqueNow();
            Assertions.assertEquals(ts(1, 101, 1), timestamp1);

            configService.reportTopology(TopologyUtils.withEpoch(node.topology().current(), 2));
            Timestamp timestamp2 = node.uniqueNow();
            Assertions.assertEquals(ts(2, 102, 1), timestamp2);
        }
    }

    @Test
    void uniqueNowAtLeastTest()
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        try (MockCluster cluster = MockCluster.builder().time(clock).build())
        {
            Node node = cluster.get(1);

            clock.increment();
            Timestamp timestamp1 = node.uniqueNow();
            Assertions.assertEquals(ts(1, 101, 1), timestamp1);

            // atLeast equal to most recent ts, should simply increment
            Assertions.assertEquals(ts(1, 102, 1),
                                    node.uniqueNow(timestamp1));

            // atLeast greater than most recent ts
            Assertions.assertEquals(ts(1, 111, 1),
                                    node.uniqueNow(ts(1, 110, 2)));
        }
    }
}
