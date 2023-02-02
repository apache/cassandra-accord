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

import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.local.Command;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.Status;
import accord.primitives.Range;
import accord.topology.Topology;
import accord.primitives.Keys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.EpochFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;

import static accord.local.PreLoadContext.contextFor;
import static accord.utils.async.AsyncChains.getUninterruptibly;

public class TopologyChangeTest
{
    @Test
    void disjointElectorate() throws Throwable
    {
        Keys keys = keys(150);
        Range range = range(100, 200);
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
            TxnId txnId1 = node1.nextTxnId(Write, Key);
            Txn txn1 = writeTxn(keys);
            getUninterruptibly(node1.coordinate(txnId1, txn1));
            getUninterruptibly(node1.commandStores().forEach(contextFor(txnId1), keys, 1, 1, commands -> {
                Command command = commands.command(txnId1).current();
                Assertions.assertTrue(command.partialDeps().isEmpty());
            }));

            cluster.configServices(4, 5, 6).forEach(config -> config.reportTopology(topology2));

            Node node4 = cluster.get(4);
            TxnId txnId2 = node4.nextTxnId(Write, Key);
            Txn txn2 = writeTxn(keys);
            getUninterruptibly(node4.coordinate(txnId2, txn2));

            // new nodes should have the previous epochs operation as a dependency
            cluster.nodes(4, 5, 6).forEach(node -> {
                try
                {
                    getUninterruptibly(node.commandStores().forEach(contextFor(txnId1, txnId2), keys, 2, 2, commands -> {
                        Command command = commands.command(txnId2).current();
                        Assertions.assertTrue(command.partialDeps().contains(txnId1));
                    }));
                }
                catch (ExecutionException e)
                {
                    throw new AssertionError(e.getCause());
                }
            });
        }
    }
}
