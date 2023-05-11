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

import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.RoutingKeys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.impl.SizeOfIntersectionSorter.SUPPLIER;

public class TopologyManagerTest
{
    private static final Node.Id ID = new Node.Id(1);

    @Test
    void fastPathReconfiguration()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        service.onEpochSyncComplete(id(1), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        service.onEpochSyncComplete(id(2), 2);
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
    }

    private static TopologyManager tracker()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(3, 4)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);

        return service;
    }

    @Test
    void syncCompleteFor()
    {
        TopologyManager service = tracker();

        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        service.onEpochSyncComplete(id(1), 2);
        service.onEpochSyncComplete(id(2), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncCompleteFor(keys(150).toUnseekables()));
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncCompleteFor(keys(250).toUnseekables()));
    }

    /**
     * Epochs should only report being synced if every preceding epoch is also reporting synced
     */
    @Test
    void existingEpochPendingSync()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));
        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(1, 2)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);
        service.onTopologyUpdate(topology3);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(3).syncComplete());

        // sync epoch 3
        service.onEpochSyncComplete(id(1), 3);
        service.onEpochSyncComplete(id(2), 3);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(3).syncComplete());

        // sync epoch 2
        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(3).syncComplete());
    }

    /**
     * If a node receives sync acks for epochs it's not aware of, it should apply them when it finds out about the epoch
     */
    @Test
    void futureEpochPendingSync()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));
//        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(3, 4)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1);

        // sync epoch 2
        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);

        // learn of epoch 2
        service.onTopologyUpdate(topology2);
        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
//        Assertions.assertTrue(service.getEpochStateUnsafe(3).syncComplete());
    }

    @Test
    void forKeys()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);
        service.onTopologyUpdate(topology3);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        RoutingKeys keys = keys(150).toUnseekables();
        Assertions.assertEquals(topologies(topology3.forSelection(keys), topology2.forSelection(keys), topology1.withEmptySubset()),
                                service.withUnsyncedEpochs(keys, 3, 3));

        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);
        service.onEpochSyncComplete(id(2), 3);
        service.onEpochSyncComplete(id(3), 3);
        Assertions.assertEquals(topologies(topology3.forSelection(keys)),
                                service.withUnsyncedEpochs(keys, 3, 3));
    }

    /**
     * Previous epoch topologies should only be included if they haven't been acknowledged, even
     * if the previous epoch is awaiting acknowledgement from all nodes
     */
    @Test
    void forKeysPartiallySynced()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology3 = topology(3,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(5, 6)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);
        service.onTopologyUpdate(topology3);

        // no acks, so all epoch 2 shards should be included
        Assertions.assertEquals(topologies(topology3, topology2, topology1.withEmptySubset()),
                                service.withUnsyncedEpochs(keys(150, 250).toUnseekables(), 3, 3));

        // first topology acked, so only the second shard should be included
        service.onEpochSyncComplete(id(1), 2);
        service.onEpochSyncComplete(id(2), 2);
        Topologies actual = service.withUnsyncedEpochs(keys(150, 250).toUnseekables(), 3, 3);
        Assertions.assertEquals(topologies(topology3, topology(2, shard(range(200, 300), idList(4, 5, 6), idSet(4, 5))), topology1.withEmptySubset()),
                                actual);
    }

    @Test
    void incompleteTopologyHistory()
    {
        Topology topology5 = topology(5,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology6 = topology(6,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(5, 6)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology5);
        service.onTopologyUpdate(topology6);

        Assertions.assertSame(topology6, service.getEpochStateUnsafe(6).global());
        Assertions.assertSame(topology5, service.getEpochStateUnsafe(5).global());
        for (int i=1; i<=6; i++) service.onEpochSyncComplete(id(i), 5);
        Assertions.assertTrue(service.getEpochStateUnsafe(5).syncComplete());
        Assertions.assertNull(service.getEpochStateUnsafe(4));

        service.onEpochSyncComplete(id(1), 4);
    }

    private static void markTopologySynced(TopologyManager service, long epoch)
    {
        service.getEpochStateUnsafe(epoch).global().nodes().forEach(id -> service.onEpochSyncComplete(id, epoch));
    }

    private static void addAndMarkSynced(TopologyManager service, Topology topology)
    {
        service.onTopologyUpdate(topology);
        markTopologySynced(service, topology.epoch());
    }

    @Test
    void truncateTopologyHistory()
    {
        Range range = range(100, 200);
        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        addAndMarkSynced(service, topology(1, shard(range, idList(1, 2, 3), idSet(1, 2))));
        addAndMarkSynced(service, topology(2, shard(range, idList(1, 2, 3), idSet(2, 3))));
        addAndMarkSynced(service, topology(3, shard(range, idList(1, 2, 3), idSet(1, 2))));
        addAndMarkSynced(service, topology(4, shard(range, idList(1, 2, 3), idSet(1, 3))));

        Assertions.assertTrue(service.hasEpoch(1));
        Assertions.assertTrue(service.hasEpoch(2));
        Assertions.assertTrue(service.hasEpoch(3));
        Assertions.assertTrue(service.hasEpoch(4));

        service.truncateTopologyUntil(3);
        Assertions.assertFalse(service.hasEpoch(1));
        Assertions.assertFalse(service.hasEpoch(2));
        Assertions.assertTrue(service.hasEpoch(3));
        Assertions.assertTrue(service.hasEpoch(4));

    }

    @Test
    void truncateTopologyCantTruncateUnsyncedEpochs()
    {

    }
}
