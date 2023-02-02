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

import accord.local.Node;
import accord.impl.mock.MockCluster;
import accord.api.Result;
import accord.impl.mock.MockStore;
import accord.primitives.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.async.AsyncChains.getUninterruptibly;

public class CoordinateTest
{
    @Test
    void simpleTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Key);
            Keys keys = keys(10);
            Txn txn = writeTxn(keys);
            FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, route));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }
    @Test
    void simpleRangeTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Key);
            Ranges keys = ranges(range(1, 2));
            Txn txn = writeTxn(keys);
            FullRangeRoute route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, route));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void slowPathTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(7).replication(7).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            Txn txn = writeTxn(keys(10));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    private TxnId coordinate(Node node, long clock, Keys keys) throws Throwable
    {
        TxnId txnId = node.nextTxnId(Write, Key);
        txnId = new TxnId(txnId.epoch(), txnId.hlc() + clock, Write, Key, txnId.node);
        Txn txn = writeTxn(keys);
        Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, node.computeRoute(txnId, txn.keys())));
        Assertions.assertEquals(MockStore.RESULT, result);
        return txnId;
    }

    @Test
    void multiKeyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(6).maxKey(600).build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId1 = coordinate(node, 100, keys(50, 350, 550));
            TxnId txnId2 = coordinate(node, 150, keys(250, 350, 450));
            TxnId txnId3 = coordinate(node, 125, keys(50, 60, 70, 80, 350, 550));
        }
    }

    @Test
    void writeOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            Keys keys = keys(10);
            Txn txn = new Txn.InMemory(keys, MockStore.read(Keys.EMPTY), MockStore.QUERY, MockStore.update(keys));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void readOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            Keys keys = keys(10);
            Txn txn = new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY, MockStore.update(Keys.EMPTY));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void simpleTxnThenReadOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Key);
            Keys oneKey = keys(10);
            Keys twoKeys = keys(10, 20);
            Txn txn = new Txn.InMemory(oneKey, MockStore.read(oneKey), MockStore.QUERY, MockStore.update(twoKeys));
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, txn.keys().toRoute(oneKey.get(0).toUnseekable())));
            Assertions.assertEquals(MockStore.RESULT, result);

            txn = new Txn.InMemory(oneKey, MockStore.read(oneKey), MockStore.QUERY, MockStore.update(Keys.EMPTY));
            result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }
}
