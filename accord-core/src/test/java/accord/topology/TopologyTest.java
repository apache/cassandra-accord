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

package accord.topology;

import accord.Utils;
import accord.api.Key;
import accord.impl.IntKey;
import accord.impl.TopologyFactory;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Keys;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static accord.impl.IntKey.*;

public class TopologyTest
{
    private static void assertRangeForKey(Topology topology, int key, int start, int end)
    {
        Key expectedKey = key(key);
        Shard shard = topology.forKey(routing(key));
        Range expectedRange = range(start, end);
        Assertions.assertTrue(expectedRange.contains(expectedKey));
        Assertions.assertTrue(shard.range.contains(expectedKey));
        Assertions.assertEquals(expectedRange, shard.range);

        Topology subTopology = topology.forSelection(Keys.of(expectedKey).toParticipants());
        shard = Iterables.getOnlyElement(subTopology.shards());
        Assertions.assertTrue(shard.range.contains(expectedKey));
        Assertions.assertEquals(expectedRange, shard.range);
    }

    private static Topology topology(List<Node.Id> ids, int rf, Range... ranges)
    {
        TopologyFactory topologyFactory = new TopologyFactory(rf, ranges);
        return topologyFactory.toTopology(ids);
    }

    private static Topology topology(int numNodes, int rf, Range... ranges)
    {
        return topology(Utils.ids(numNodes), rf, ranges);
    }

    private static Topology topology(Range... ranges)
    {
        return topology(1, 1, ranges);
    }

    private static Range r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    private static void assertNoRangeForKey(Topology topology, int key)
    {
        try
        {
            topology.forKey(routing(key));
            Assertions.fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // noop
        }
    }

    @Test
    void forKeyTest()
    {
        Topology topology = topology(r(0, 100), r(100, 200), r(300, 400));
        assertNoRangeForKey(topology, -50);
        assertRangeForKey(topology, 50, 0, 100);
        assertRangeForKey(topology, 100, 0, 100);
        assertNoRangeForKey(topology, 250);
        assertRangeForKey(topology, 350, 300, 400);
    }

    @Test
    void forRangesTest()
    {

    }
}
