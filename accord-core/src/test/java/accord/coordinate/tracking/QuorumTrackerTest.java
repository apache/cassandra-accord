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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.TopologyUtils;
import accord.local.Node;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.topology.Shard;
import accord.topology.Topology;

import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.ids;
import static accord.Utils.shard;
import static accord.Utils.topologies;
import static accord.Utils.topology;
import static accord.impl.IntKey.range;
import static accord.utils.Utils.toArray;

public class QuorumTrackerTest
{
    private static final Node.Id[] ids = toArray(ids(5), Node.Id[]::new);
    private static final Ranges ranges = TopologyUtils.initialRanges(5, 500);
    private static final Topology topology = TopologyUtils.initialTopology(ids, ranges, 3);
        /*
        (000, 100](100, 200](200, 300](300, 400](400, 500]
        [1, 2, 3] [2, 3, 4] [3, 4, 5] [4, 5, 1] [5, 1, 2]
         */

    private static void assertResponseState(QuorumTracker responses,
                                            boolean quorumReached,
                                            boolean failed,
                                            boolean hasOutstandingResponses)
    {
        Assertions.assertEquals(quorumReached, responses.hasReachedQuorum());
        Assertions.assertEquals(failed, responses.hasFailed());
        Assertions.assertEquals(hasOutstandingResponses, responses.hasInFlight());
    }

    @Test
    void singleShard()
    {
        Topology subTopology = topology(topology.get(0));
        QuorumTracker responses = new QuorumTracker(topologies(subTopology));

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, true);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, true, false, true);

        responses.recordSuccess(ids[2]);
        assertResponseState(responses, true, false, false);
    }

    @Test
    void singleShardDataConsistencyLevelAll()
    {
        Topology subTopology = topology(topology.get(0));
        QuorumTracker responses = new QuorumTracker(topologies(subTopology), DataConsistencyLevel.ALL);

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, true);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, false, false, true);

        responses.recordSuccess(ids[2]);
        assertResponseState(responses, true, false, false);
    }

    /**
     * responses from unexpected endpoints should be ignored
     */
    @Test
    void unexpectedResponsesAreIgnored()
    {
        Topology subTopology = topology(topology.get(0));
        QuorumTracker responses = new QuorumTracker(topologies(subTopology));

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, true);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, true, false, true);

        Assertions.assertFalse(subTopology.get(0).nodes.contains(ids[4]));
        responses.recordSuccess(ids[4]);
        assertResponseState(responses, true, false, true);
    }

    @Test
    void failure()
    {
        Topology subTopology = topology(topology.get(0));
        QuorumTracker responses = new QuorumTracker(topologies(subTopology));

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, true);

        responses.recordFailure(ids[1]);
        assertResponseState(responses, false, false, true);

        responses.recordFailure(ids[2]);
        assertResponseState(responses, false, true, false);
    }

    @Test
    void failureDataConsistencyLevelALL()
    {
        Topology subTopology = topology(topology.get(0));
        QuorumTracker responses = new QuorumTracker(topologies(subTopology), DataConsistencyLevel.ALL);

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, true);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, false, false, true);

        responses.recordFailure(ids[2]);
        assertResponseState(responses, false, true, false);

        responses = new QuorumTracker(topologies(subTopology), DataConsistencyLevel.ALL);
        responses.recordFailure(ids[2]);
        assertResponseState(responses, false, true, true);
    }

    @Test
    void multiShard()
    {
        Topology subTopology = new Topology(1, new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        QuorumTracker responses = new QuorumTracker(topologies(subTopology));
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        Assertions.assertSame(subTopology.get(0), responses.get(0).shard);
        Assertions.assertSame(subTopology.get(1), responses.get(1).shard);
        Assertions.assertSame(subTopology.get(2), responses.get(2).shard);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, false, false, true);
        responses.recordSuccess(ids[2]);
        assertResponseState(responses, false, false, true);
        responses.recordSuccess(ids[3]);
        assertResponseState(responses, true, false, true);
    }

    @Test
    void multiTopology()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(4, 5, 6), idSet(4, 5)));

        QuorumTracker responses = new QuorumTracker(topologies(topology2, topology1));

        responses.recordSuccess(id(1));
        assertResponseState(responses, false, false, true);
        responses.recordSuccess(id(2));
        assertResponseState(responses, false, false, true);

        responses.recordSuccess(id(4));
        assertResponseState(responses, false, false, true);
        responses.recordSuccess(id(5));
        assertResponseState(responses, true, false, true);
    }

    @Test
    void multiTopologyFailure()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(4, 5, 6), idSet(4, 5)));

        QuorumTracker responses = new QuorumTracker(topologies(topology2, topology1));

        responses.recordSuccess(id(1));
        assertResponseState(responses, false, false, true);
        responses.recordSuccess(id(2));
        assertResponseState(responses, false, false, true);

        responses.recordFailure(id(4));
        assertResponseState(responses, false, false, true);
        responses.recordFailure(id(5));
        assertResponseState(responses, false, true, true);
    }
}
