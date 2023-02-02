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
import accord.api.Key;
import accord.local.*;
import accord.primitives.TxnId;

import org.junit.jupiter.api.Assertions;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static accord.impl.InMemoryCommandStore.inMemory;

public class RecoverTest
{
    private static CommandStore getCommandShard(Node node, Key key)
    {
        return node.unsafeForKey(key);
    }

    private static Command getCommand(Node node, Key key, TxnId txnId)
    {
        CommandStore commandStore = getCommandShard(node, key);
        Assertions.assertTrue(inMemory(commandStore).hasCommand(txnId));
        return inMemory(commandStore).command(txnId).value();
    }

    private static void assertStatus(Node node, Key key, TxnId txnId, Status status)
    {
        Command command = getCommand(node, key, txnId);

        Assertions.assertNotNull(command);
        Assertions.assertEquals(status, command.status());
    }

    private static void assertMissing(Node node, Key key, TxnId txnId)
    {
        CommandStore commandStore = getCommandShard(node, key);
        Assertions.assertFalse(inMemory(commandStore).hasCommand(txnId));
    }

    private static void assertTimeout(Future<?> f)
    {
        try
        {
            f.get();
            Assertions.fail("expected timeout");
        }
        catch (ExecutionException e)
        {
            // timeout expected
            Assertions.assertEquals(Timeout.class, e.getCause().getClass());
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
    }

    // TODO
//    void conflictTest() throws Throwable
//    {
//        Key key = IntKey.key(10);
//        try (MockCluster cluster = MockCluster.builder().nodes(9).replication(9).build())
//        {
//            cluster.networkFilter.isolate(ids(7, 9));
//            cluster.networkFilter.addFilter(anyId(), isId(ids(5, 6)), notMessageType(PreAccept.class));
//
//            TxnId txnId1 = new TxnId(1, 100, 0, id(100));
//            Txn txn1 = writeTxn(Keys.of(key));
//            assertTimeout(Coordinate.coordinate(cluster.get(1), txnId1, txn1, key));
//
//            TxnId txnId2 = new TxnId(1, 50, 0, id(101));
//            Txn txn2 = writeTxn(Keys.of(key));
//            cluster.networkFilter.clear();
//            cluster.networkFilter.isolate(ids(1, 7));
//            assertTimeout(Coordinate.coordinate(cluster.get(9), txnId2, txn2, key));
//
//            cluster.nodes(ids(1, 4)).forEach(n -> assertStatus(n, key, txnId1, Status.Accepted));
//            cluster.nodes(ids(5, 6)).forEach(n -> assertStatus(n, key, txnId1, Status.PreAccepted));
//            cluster.nodes(ids(7, 9)).forEach(n -> assertMissing(n, key, txnId1));
//
//            cluster.nodes(ids(1, 7)).forEach(n -> assertMissing(n, key, txnId2));
//            cluster.nodes(ids(8, 9)).forEach(n -> assertStatus(n, key, txnId2, Status.PreAccepted));
//
//            //
//            cluster.networkFilter.clear();
//            cluster.networkFilter.isolate(ids(1, 4));
//            Recover.recover(cluster.get(8), txnId2, txn2, key).get();
//
//            List<Node> nodes = cluster.nodes(ids(5, 9));
//            Assertions.assertTrue(txnId2.compareTo(txnId1) < 0);
//            nodes.forEach(n -> assertStatus(n, key, txnId2, Status.Applied));
//            nodes.forEach(n -> {
//                assertStatus(n, key, txnId2, Status.Applied);
//                Command command = getCommand(n, key, txnId2);
//                Assertions.assertEquals(txnId1, command.executeAt());
//            });
//        }
//    }
}
