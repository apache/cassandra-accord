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

import accord.primitives.KeyRange;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.primitives.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.Utils.idSet;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;

public class TxnRequestScopeTest
{
    @Test
    void createDisjointScopeTest()
    {
        Keys keys = keys(150);
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(4, 5, 6), idSet(4, 5)));

        Topologies.Multi topologies = new Topologies.Multi();
        topologies.add(topology2);
        topologies.add(topology1);

        Assertions.assertEquals(keys(150), TxnRequest.computeScope(id(1), topologies, keys));
        Assertions.assertEquals(1, TxnRequest.computeWaitForEpoch(id(1), topologies, keys));
        Assertions.assertEquals(keys(150), TxnRequest.computeScope(id(4), topologies, keys));
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(4), topologies, keys));
    }

    @Test
    void movingRangeTest()
    {
        Keys keys = keys(150, 250);
        KeyRange range1 = range(100, 200);
        KeyRange range2 = range(200, 300);
        Topology topology1 = topology(1,
                                      shard(range1, idList(1, 2, 3), idSet(1, 2)),
                                      shard(range2, idList(4, 5, 6), idSet(4, 5)) );
        // reverse ownership
        Topology topology2 = topology(2,
                                      shard(range1, idList(4, 5, 6), idSet(4, 5)),
                                      shard(range2, idList(1, 2, 3), idSet(1, 2)) );

        Topologies.Multi topologies = new Topologies.Multi();
        topologies.add(topology2);
        topologies.add(topology1);

        Assertions.assertEquals(keys(150, 250), TxnRequest.computeScope(id(1), topologies, keys));
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(1), topologies, keys));
        Assertions.assertEquals(keys(150, 250), TxnRequest.computeScope(id(4), topologies, keys));
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(4), topologies, keys));
    }
}
