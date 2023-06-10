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

package accord.messages;

import accord.api.TopologySorter;
import accord.primitives.Range;
import accord.primitives.FullKeyRoute;
import accord.primitives.PartialKeyRoute;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.primitives.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.impl.IntKey.scope;

public class TxnRequestScopeTest
{
    @Test
    void createDisjointScopeTest()
    {
        Keys keys = keys(150);
        FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(3, 4, 5), idSet(4, 5)));

        Topologies.Multi topologies = new Topologies.Multi((TopologySorter.StaticSorter)(a, b, s)->0);
        topologies.add(topology2);
        topologies.add(topology1);

        // 3 remains a member across both topologies, so can process requests without waiting for latest topology data
        Assertions.assertEquals(scope(150), ((PartialKeyRoute)TxnRequest.computeScope(id(3), topologies, route)).toParticipants());
        Assertions.assertEquals(1, TxnRequest.computeWaitForEpoch(id(3), topologies, route));

        // 1 leaves the shard, and 4 joins, so both need the latest information
        Assertions.assertEquals(scope(150), ((PartialKeyRoute)TxnRequest.computeScope(id(1), topologies, route)).toParticipants());
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(1), topologies, route));
        Assertions.assertEquals(scope(150), ((PartialKeyRoute)TxnRequest.computeScope(id(4), topologies, route)).toParticipants());
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(4), topologies, route));
    }

    @Test
    void movingRangeTest()
    {
        Keys keys = keys(150, 250);
        FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());

        Range range1 = range(100, 200);
        Range range2 = range(200, 300);
        Topology topology1 = topology(1,
                                      shard(range1, idList(1, 2, 3), idSet(1, 2)),
                                      shard(range2, idList(4, 5, 6), idSet(4, 5)) );
        // reverse ownership
        Topology topology2 = topology(2,
                                      shard(range1, idList(4, 5, 6), idSet(4, 5)),
                                      shard(range2, idList(1, 2, 3), idSet(1, 2)) );

        Topologies.Multi topologies = new Topologies.Multi((TopologySorter.StaticSorter)(a,b,s)->0);
        topologies.add(topology2);
        topologies.add(topology1);

        Assertions.assertEquals(scope(150, 250), ((PartialKeyRoute)TxnRequest.computeScope(id(1), topologies, route)).toParticipants());
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(1), topologies, route));
        Assertions.assertEquals(scope(150, 250), ((PartialKeyRoute)TxnRequest.computeScope(id(4), topologies, route)).toParticipants());
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(4), topologies, route));
    }
}
