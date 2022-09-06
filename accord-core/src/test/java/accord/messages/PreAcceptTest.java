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

import accord.api.Key;
import accord.impl.*;
import accord.impl.mock.*;
import accord.impl.SimpleProgressLog;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.impl.mock.MockCluster.Clock;
import accord.topology.Topology;
import accord.primitives.Deps;
import accord.txn.Txn;
import accord.primitives.TxnId;
import accord.utils.EpochFunction;
import accord.utils.ThreadPoolScheduler;
import accord.local.*;
import accord.primitives.Keys;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.TopologyFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.InMemoryCommandStore.inMemory;
import static accord.impl.mock.MockCluster.configService;

public class PreAcceptTest
{
    private static final Id ID1 = id(1);
    private static final Id ID2 = id(2);
    private static final Id ID3 = id(3);
    private static final List<Id> IDS = List.of(ID1, ID2, ID3);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, IntKey.range(0, 100));

    private static final ReplyContext REPLY_CONTEXT = Network.replyCtxFor(0);

    private static Node createNode(Id nodeId, MessageSink messageSink, Clock clock)
    {
        MockStore store = new MockStore();
        Scheduler scheduler = new ThreadPoolScheduler();
        return new Node(nodeId,
                        messageSink,
                        new MockConfigurationService(messageSink, EpochFunction.noop(), TOPOLOGY),
                        clock,
                        () -> store,
                        new TestAgent(),
                        new Random(),
                        scheduler,
                        SimpleProgressLog::new,
                        InMemoryCommandStores.SingleThread::new);
    }

    private static PreAccept preAccept(TxnId txnId, Txn txn, Key homeKey)
    {
        return new PreAccept(txn.keys(), txnId.epoch, txnId, txn, homeKey);
    }

    @Test
    void initialCommandTest()
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, messageSink, clock);
        messageSink.clearHistory();

        try
        {
            IntKey key = IntKey.key(10);
            Keys keys = Keys.of(key);
            CommandStore commandStore = node.unsafeForKey(key);
            Assertions.assertFalse(inMemory(commandStore).hasCommandsForKey(key));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = preAccept(txnId, txn, key);
            clock.increment(10);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            Command command = commandStore.commandsForKey(key).uncommitted().get(txnId);
            Assertions.assertEquals(Status.PreAccepted, command.status());

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, txnId, Deps.NONE),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void nackTest()
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, messageSink, clock);
        try
        {
            IntKey key = IntKey.key(10);
            CommandStore commandStore = node.unsafeForKey(key);
            Assertions.assertFalse(inMemory(commandStore).hasCommandsForKey(key));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = preAccept(txnId, txn, key);
            preAccept.process(node, ID2, REPLY_CONTEXT);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void singleKeyTimestampUpdate()
    {
    }

    @Test
    void multiKeyTimestampUpdate()
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, messageSink, clock);
        try
        {
            IntKey key1 = IntKey.key(10);
            PreAccept preAccept1 = preAccept(clock.idForNode(1, ID2), writeTxn(Keys.of(key1)), key1);
            preAccept1.process(node, ID2, REPLY_CONTEXT);

            messageSink.clearHistory();
            IntKey key2 = IntKey.key(11);
            Keys keys = Keys.of(key1, key2);
            TxnId txnId2 = new TxnId(1, 50, 0, ID3);
            PreAccept preAccept2 = preAccept(txnId2, writeTxn(keys), key2);
            clock.increment(10);
            preAccept2.process(node, ID3, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID3, messageSink.responses.get(0).to);
            Deps expectedDeps = Deps.NONE;
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId2, new TxnId(1, 110, 0, ID1), expectedDeps),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }

    /**
     * Test that a preaccept with a larger timestampe than previously seen is accepted
     * with the proposed timestamp
     */
    @Test
    void singleKeyNewerTimestamp()
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, messageSink, clock);
        messageSink.clearHistory();
        IntKey key = IntKey.key(10);
        try
        {
            Keys keys = Keys.of(key);
            TxnId txnId = new TxnId(1, 110, 0, ID2);
            PreAccept preAccept = preAccept(txnId, writeTxn(keys), key);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Deps expectedDeps = Deps.NONE;
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, txnId, expectedDeps),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void supersedingEpochPrecludesFastPath()
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, messageSink, clock);

        try
        {
            IntKey key = IntKey.key(10);
            Keys keys = Keys.of(key);
            CommandStore commandStore = node.unsafeForKey(key);

            configService(node).reportTopology(node.topology().current().withEpoch(2));
            messageSink.clearHistory();

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(keys);
            PreAccept preAccept = preAccept(txnId, txn, key);

            clock.increment(10);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            Command command = commandStore.commandsForKey(key).uncommitted().get(txnId);
            Assertions.assertEquals(Status.PreAccepted, command.status());

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, new TxnId(2, 110, 0, ID1), Deps.NONE),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }
}
