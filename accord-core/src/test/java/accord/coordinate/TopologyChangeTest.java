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
import accord.local.StoreParticipants;
import accord.messages.Message;
import accord.messages.PreAccept;
import accord.primitives.*;
import accord.topology.Topology;
import accord.utils.EpochFunction;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import com.google.common.base.Predicates;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
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
        Topology topology2 = topology(2, shard(range, idList(3, 4, 5), idSet(4, 5)));
        EpochFunction<MockConfigurationService> fetchTopology = (epoch, service) -> {
            Assertions.assertEquals(2, epoch);
            service.reportTopology(topology2);
        };
        try (MockCluster cluster = MockCluster.builder()
                                              .nodes(5)
                                              .topology(topology1)
                                              .setOnFetchTopology(fetchTopology)
                                              .build())
        {
            Node node1 = cluster.get(1);
            TxnId txnId1 = node1.nextTxnId(Write, Key);
            Txn txn1 = writeTxn(keys);
            getUninterruptibly(node1.coordinate(txnId1, txn1));
            getUninterruptibly(node1.commandStores().forEach(contextFor(txnId1), keys, 1, 1, safeStore -> {
                StoreParticipants participants = StoreParticipants.read(safeStore, keys.toParticipants(), txnId1);
                Command command = safeStore.get(txnId1, participants).current();
                Assertions.assertTrue(command.partialDeps().isEmpty());
            }));

            cluster.configServices(4).forEach(config -> {
                try
                {
                    config.fetchTopologyForEpoch(2);
                    getUninterruptibly(config.ackFor(topology2.epoch()).fastPath);
                }
                catch (ExecutionException e)
                {
                    throw new AssertionError(e);
                }
            });

            Node node4 = cluster.get(4);
            TxnId txnId2 = node4.nextTxnId(Write, Key);
            Txn txn2 = writeTxn(keys);
            getUninterruptibly(node4.coordinate(txnId2, txn2));

            // new nodes should have the previous epoch's operation as a dependency
            cluster.nodes(4, 5).forEach(node -> {
                try
                {
                    getUninterruptibly(node.commandStores().forEach(contextFor(txnId1, txnId2), keys, 2, 2, safeStore -> {
                        StoreParticipants participants = StoreParticipants.update(safeStore, keys.toParticipants(), txnId2.epoch(), txnId2, txnId2.epoch());
                        Command command = safeStore.get(txnId2, participants).current();
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

    private static boolean isExclSyncPoint(Message message)
    {
        if (!(message instanceof PreAccept))
            return false;

        PreAccept preAccept = (PreAccept) message;
        return preAccept.txnId.is(ExclusiveSyncPoint);
    }

    private static void assertEpochRejection(Node node, Keys keys, long epoch, boolean rejectionExpected)
    {
        RoutingKeys participants = keys.toParticipants();
        try
        {
            if (node.epoch() < epoch)
                throw new AssertionError(String.format("node[%s] epoch %s is less than check epoch %s", node.id(), node.epoch(), epoch));

            node.forEachLocal(PreLoadContext.contextFor(keys.toParticipants()), participants, 1, node.epoch(), safeStore -> {
                boolean rejected = safeStore.commandStore().isRejectedIfNotPreAccepted(TxnId.minForEpoch(epoch), participants);
                if (rejected != rejectionExpected)
                {
                    String msg = String.format("Epoch %s %s rejected on node %s and a rejection %s expected",
                                               epoch,
                                               rejected? "was" : "was not",
                                               node.id(),
                                               rejectionExpected? "was" : "was not");
                    throw new AssertionError(msg);
                }
            });
        }
        catch (Throwable e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <V> V getUncheckedTimeout(AsyncChain<V> chain, long timeout, TimeUnit unit)
    {
        try
        {
            return AsyncChains.getUninterruptibly(chain, timeout, unit);
        }
        catch (ExecutionException | TimeoutException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Confirm that a new node won't start coordinating before it's applied its own bootstrap exclusiveSync point
     */
    @Test
    void lateBarrierTest() throws Throwable
    {
        Keys keys = keys(150);
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(2, 3, 4), idSet(2, 3)));
        Topology topology3 = topology(3, shard(range, idList(3, 4, 5), idSet(3, 4)));
        try (MockCluster cluster = MockCluster.builder()
                .nodes(5)
                .topology(topology1)
                .build())
        {
            cluster.nodes(1, 2, 3).forEach(node -> assertEpochRejection(node, keys, 1, false));
            cluster.networkFilter.addFilter(Predicates.alwaysTrue(), to -> id(4).equals(to), TopologyChangeTest::isExclSyncPoint);

            cluster.configServices(1, 2, 3, 4, 5).forEach(configService -> configService.reportTopology(topology2));
            cluster.nodes(4).forEach(node -> {
                MockConfigurationService configService = (MockConfigurationService) node.configService();
                getUncheckedTimeout(configService.ackFor(2).fastPath, 5, TimeUnit.SECONDS);
                assertEpochRejection(node, keys, 1, false);  // shouldn't have received the sync point preaccept
            });

            cluster.nodes(2, 3).forEach(node -> {
                MockConfigurationService configService = (MockConfigurationService) node.configService();
                getUncheckedTimeout(configService.ackFor(2).fastPath, 5, TimeUnit.SECONDS);
                assertEpochRejection(node, keys, 1, true);
            });

            Node node4 = cluster.get(4);
            TxnId epoch2txnId = node4.nextTxnId(Write, Key);
            Assertions.assertEquals(2, epoch2txnId.epoch());

            cluster.configServices(1, 2, 3, 4, 5).forEach(configService -> configService.reportTopology(topology3));
            cluster.nodes(2, 3).forEach(node -> {
                MockConfigurationService configService = (MockConfigurationService) node.configService();
                getUncheckedTimeout(configService.ackFor(3).fastPath, 5, TimeUnit.SECONDS);
                assertEpochRejection(node, keys, 2, true);
            });

            // node 4 shouldn't have up to date rejectBefore data
            cluster.nodes(4).forEach(node -> {
                MockConfigurationService configService = (MockConfigurationService) node.configService();
                getUncheckedTimeout(configService.ackFor(3).fastPath, 5, TimeUnit.SECONDS);
                assertEpochRejection(node, keys, 1, false);  // shouldn't have received the sync point preaccept
                assertEpochRejection(node, keys, 2, false);  // shouldn't have received the sync point preaccept
            });

            // but if it tries to coordinate a txn with a txnid from epoch2 it should be rejected by the other nodes in the cluster
            try
            {
                AsyncChains.getUninterruptibly(node4.coordinate(epoch2txnId, writeTxn(keys)), 5, TimeUnit.SECONDS);
                Assertions.fail("Expected to be invalidated");
            }
            catch (ExecutionException e)
            {
                Throwable cause = e.getCause();
                Assertions.assertTrue(cause instanceof Invalidated, "Expected failure to be txn invalidation");
            }
        }
    }

    /**
     * bootstrapping nodes should not be able to coordinate if their bootstrap sync points don't reach a quorum
     */
    @Test
    void lostBarrierTest()
    {
        Keys keys = keys(150);
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(2, 3, 4), idSet(2, 3)));
        Topology topology3 = topology(3, shard(range, idList(3, 4, 5), idSet(3, 4)));
        try (MockCluster cluster = MockCluster.builder()
                .nodes(5)
                .topology(topology1)
                .build())
        {
            cluster.nodes(1, 2, 3).forEach(node -> assertEpochRejection(node, keys, 1, false));
            cluster.networkFilter.addFilter(Predicates.alwaysTrue(), to -> id(3).equals(to), TopologyChangeTest::isExclSyncPoint);
            cluster.networkFilter.addFilter(Predicates.alwaysTrue(), to -> id(4).equals(to), TopologyChangeTest::isExclSyncPoint);

            cluster.configServices(1, 2, 3, 4, 5).forEach(configService -> configService.reportTopology(topology2));
            cluster.nodes(   4).forEach(node -> {
                MockConfigurationService configService = (MockConfigurationService) node.configService();
                try
                {
                    AsyncChains.getUninterruptibly(configService.ackFor(2).fastPath, 5, TimeUnit.SECONDS);
                    Assertions.fail("Expected to timeout on node " + node.id());
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e);
                }
                catch (TimeoutException e)
                {
                    // expected
                }
            });
        }
    }
}
