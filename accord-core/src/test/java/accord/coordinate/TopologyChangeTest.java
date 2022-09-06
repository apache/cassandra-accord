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

package accord.coordinate;

import accord.impl.mock.EpochSync;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.impl.mock.RecordingMessageSink;
import accord.local.Command;
import accord.local.Node;
import accord.local.Status;
import accord.messages.Accept;
import accord.primitives.KeyRange;
import accord.topology.Topology;
import accord.primitives.Keys;
import accord.txn.Txn;
import accord.primitives.TxnId;
import accord.utils.EpochFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

import static accord.Utils.*;
import static accord.impl.InMemoryCommandStores.inMemory;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;

public class TopologyChangeTest
{
    private static TxnId coordinate(Node node, Keys keys) throws Throwable
    {
        TxnId txnId = node.nextTxnId();
        Txn txn = writeTxn(keys);
        node.coordinate(txnId, txn).get();
        return txnId;
    }

    @Test
    void disjointElectorate() throws Throwable
    {
        Keys keys = keys(150);
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(4, 5, 6), idSet(4, 5)));
        EpochFunction<MockConfigurationService> fetchTopology = (epoch, service) -> {
            Assertions.assertEquals(2, epoch);
            service.reportTopology(topology2);
        };
        try (MockCluster cluster = MockCluster.builder()
                                              .nodes(6)
                                              .topology(topology1)
                                              .setOnFetchTopology(fetchTopology)
                                              .build())
        {
            Node node1 = cluster.get(1);
            TxnId txnId1 = node1.nextTxnId();
            Txn txn1 = writeTxn(keys);
            node1.coordinate(txnId1, txn1).get();
            inMemory(node1).forEachLocal(keys, 1, commands -> {
                Command command = commands.command(txnId1);
                Assertions.assertTrue(command.savedDeps().isEmpty());
            });

            cluster.configServices(4, 5, 6).forEach(config -> config.reportTopology(topology2));

            Node node4 = cluster.get(4);
            TxnId txnId2 = node4.nextTxnId();
            Txn txn2 = writeTxn(keys);
            node4.coordinate(txnId2, txn2).get();

            // new nodes should have the previous epochs operation as a dependency
            cluster.nodes(4, 5, 6).forEach(node -> {
                inMemory(node).forEachLocal(keys, 2, commands -> {
                    Command command = commands.command(txnId2);
                    Assertions.assertTrue(command.savedDeps().contains(txnId1));
                });
            });

            // ...and participated in consensus
            cluster.nodes(1, 2, 3).forEach(node -> {
                inMemory(node).forEachLocal(keys, 1, commands -> {
                    Command command = commands.command(txnId2);
                    Assertions.assertTrue(command.hasBeen(Status.Accepted));
                });
            });
        }
    }

    @Test
    void fastPathSkippedUntilSync() throws Throwable
    {
        Keys keys = keys(150);
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));
        try (MockCluster cluster = MockCluster.builder().nodes(3)
                                                        .messageSink(RecordingMessageSink::new)
                                                        .topology(topology1).build())
        {
            Node node1 = cluster.get(1);
            RecordingMessageSink messageSink = (RecordingMessageSink) node1.messageSink();
            messageSink.clearHistory();
            TxnId txnId1 = coordinate(node1, keys);
            inMemory(node1).forEachLocal(keys, 1, commands -> {
                Command command = commands.command(txnId1);
                Assertions.assertTrue(command.savedDeps().isEmpty());
            });

            // check there was no accept phase
            Assertions.assertFalse(new ArrayList<>(messageSink.requests).stream().anyMatch(env -> env.payload instanceof Accept));

            cluster.configServices(1, 2, 3).forEach(config -> config.reportTopology(topology2));
            messageSink.clearHistory();

            // post epoch change, there _should_ be accepts, but with the original timestamp
            TxnId txnId2 = coordinate(node1, keys);
            Set<Node.Id> accepts = new ArrayList<>(messageSink.requests).stream()
                    .filter(env -> env.payload instanceof Accept).map(env -> {
                        Accept accept = (Accept) env.payload;
                        Assertions.assertEquals(txnId2, accept.txnId);
                        return env.to;
            }).collect(Collectors.toSet());
            Assertions.assertEquals(idSet(1, 2, 3), accepts);

            inMemory(node1).forEachLocal(keys, 2, commands -> {
                Command command = commands.command(txnId2);
                Assertions.assertTrue(command.hasBeen(Status.Committed));
                Assertions.assertTrue(command.savedDeps().contains(txnId1));
                Assertions.assertEquals(txnId2, command.executeAt());
            });

            EpochSync.sync(cluster, 1);

            // post sync, fast path should be working again, and there should be no accept phase
            messageSink.clearHistory();
            TxnId txnId3 = coordinate(node1, keys);
            Assertions.assertFalse(new ArrayList<>(messageSink.requests).stream().anyMatch(env -> env.payload instanceof Accept));
            inMemory(node1).forEachLocal(keys, 2, commands -> {
                Command command = commands.command(txnId3);
                Assertions.assertTrue(command.hasBeen(Status.Committed));
                Assertions.assertTrue(command.savedDeps().contains(txnId1));
                Assertions.assertTrue(command.savedDeps().contains(txnId2));
                Assertions.assertEquals(txnId3, command.executeAt());
            });
        }
    }
}
