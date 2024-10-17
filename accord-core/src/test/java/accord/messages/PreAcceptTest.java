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

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.IntKey.Raw;
import accord.impl.TestAgent.RethrowAgent;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster.Clock;
import accord.impl.mock.Network;
import accord.impl.mock.RecordingMessageSink;
import accord.local.Command;
import accord.local.CommandStore;
import accord.primitives.SaveStatus;
import accord.local.cfk.CommandsForKey;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.primitives.Status;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.topology.TopologyUtils;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.InMemoryCommandStore.inMemory;
import static accord.impl.IntKey.range;
import static accord.impl.IntKey.routing;
import static accord.impl.mock.MockCluster.configService;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Utils.listOf;
import static org.assertj.core.api.Assertions.assertThat;

public class PreAcceptTest
{
    private static final Id ID1 = id(1);
    private static final Id ID2 = id(2);
    private static final Id ID3 = id(3);
    private static final List<Id> IDS = listOf(ID1, ID2, ID3);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, IntKey.range(0, 100));
    private static final Ranges FULL_RANGE = Ranges.single(IntKey.range(routing(Integer.MIN_VALUE), routing(Integer.MAX_VALUE)));

    private static final ReplyContext REPLY_CONTEXT = Network.replyCtxFor(0);

    private static PreAccept preAccept(TxnId txnId, Txn txn, RoutingKey homeKey)
    {
        FullRoute<?> route = txn.keys().toRoute(homeKey);
        return PreAccept.SerializerSupport.create(txnId, route.slice(FULL_RANGE), txnId.epoch(), txnId.epoch(), txnId.epoch(), txn.slice(FULL_RANGE, true), route);
    }

    @Test
    void initialCommandTest() throws ExecutionException
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, TOPOLOGY, messageSink, clock);
        messageSink.clearHistory();

        try
        {
            Raw key = IntKey.key(10);
            CommandStore commandStore = node.unsafeForKey(key.toUnseekable());
            Assertions.assertFalse(inMemory(commandStore).hasCommandsForKey(key.toUnseekable()));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = preAccept(txnId, txn, key.toUnseekable());
            clock.increment(10);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            commandStore.execute(PreLoadContext.contextFor(txnId, txn.keys().toParticipants()), safeStore -> {
                CommandsForKey cfk = safeStore.get(key.toUnseekable()).current();
                TxnId commandId = cfk.get(0).plainTxnId();
                Command command = safeStore.ifInitialised(commandId).current();
                Assertions.assertEquals(Status.PreAccepted, command.status());
            }).begin(new RethrowAgent());

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            PartialDeps expectedDeps = new PartialDeps(Ranges.of(range(0, 12)), KeyDeps.NONE, RangeDeps.NONE, KeyDeps.NONE);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, txnId, expectedDeps),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void nackTest() throws ExecutionException
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, TOPOLOGY, messageSink, clock);
        try
        {
            Raw key = IntKey.key(10);
            CommandStore commandStore = node.unsafeForKey(key.toUnseekable());
            Assertions.assertFalse(inMemory(commandStore).hasCommandsForKey(key.toUnseekable()));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = preAccept(txnId, txn, key.toUnseekable());
            preAccept.process(node, ID2, REPLY_CONTEXT);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void invalidatedTest()
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, TOPOLOGY, messageSink, clock);
        try
        {
            Raw key = IntKey.key(10);
            CommandStore commandStore = node.unsafeForKey(key.toUnseekable());
            Assertions.assertFalse(inMemory(commandStore).hasCommandsForKey(key.toUnseekable()));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));

            Participants<?> invalidateWith = txn.keys().toParticipants();
            BeginInvalidation invalidate = new BeginInvalidation(ID1, node.topology().forEpoch(invalidateWith, txnId.epoch()), txnId, invalidateWith, Ballot.fromValues(txnId.epoch(), txnId.hlc(), txnId.node));
            invalidate.process(node, ID2, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            assertThat(messageSink.responses.get(0).payload).isEqualTo(new BeginInvalidation.InvalidateReply(null, Ballot.ZERO, SaveStatus.NotDefined, false, null, null, null));
            messageSink.clearHistory();

            PreAccept preAccept = preAccept(txnId, txn, key.toUnseekable());
            preAccept.process(node, ID2, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            assertThat(messageSink.responses.get(0).payload).isEqualTo(PreAccept.PreAcceptNack.INSTANCE);
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
    void multiKeyTimestampUpdate() throws ExecutionException
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, TOPOLOGY, messageSink, clock);
        try
        {
            Raw key1 = IntKey.key(10);
            PreAccept preAccept1 = preAccept(clock.idForNode(1, ID2), writeTxn(Keys.of(key1)), key1.toUnseekable());
            preAccept1.process(node, ID2, REPLY_CONTEXT);

            messageSink.clearHistory();
            Raw key2 = IntKey.key(11);
            Keys keys = Keys.of(key1, key2);
            TxnId txnId2 = new TxnId(1, 50, Write, Key, ID3);
            PreAccept preAccept2 = preAccept(txnId2, writeTxn(keys), key2.toUnseekable());
            clock.increment(10);
            preAccept2.process(node, ID3, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID3, messageSink.responses.get(0).to);
            PartialDeps expectedDeps = new PartialDeps(Ranges.of(range(0, 12)), KeyDeps.NONE, RangeDeps.NONE, KeyDeps.NONE);
            Timestamp expectedTs = Timestamp.fromValues(1, 110, ID1);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId2, expectedTs, expectedDeps),
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
    void singleKeyNewerTimestamp() throws ExecutionException
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, TOPOLOGY, messageSink, clock);
        messageSink.clearHistory();
        Raw key = IntKey.key(10);
        try
        {
            Keys keys = Keys.of(key);
            TxnId txnId = new TxnId(1, 110, Write, Key, ID2);
            PreAccept preAccept = preAccept(txnId, writeTxn(keys), key.toUnseekable());
            preAccept.process(node, ID2, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            PartialDeps expectedDeps = new PartialDeps(Ranges.of(range(0, 12)), KeyDeps.NONE, RangeDeps.NONE, KeyDeps.NONE);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, txnId, expectedDeps),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }

    @Test
    void supersedingEpochPrecludesFastPath() throws ExecutionException
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, TOPOLOGY, messageSink, clock);

        try
        {
            Raw key = IntKey.key(10);
            Keys keys = Keys.of(key);
            CommandStore commandStore = node.unsafeForKey(key.toUnseekable());

            configService(node).reportTopology(TopologyUtils.withEpoch(node.topology().current(), 2));
            messageSink.clearHistory();

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(keys);
            PreAccept preAccept = preAccept(txnId, txn, key.toUnseekable());

            clock.increment(10);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            commandStore.execute(PreLoadContext.contextFor(txnId, txn.keys().toParticipants()), safeStore -> {
                CommandsForKey cfk = safeStore.get(key.toUnseekable()).current();
                TxnId commandId = cfk.get(0).plainTxnId();
                Command command = safeStore.ifInitialised(commandId).current();
                Assertions.assertEquals(Status.PreAccepted, command.status());
            }).begin(new RethrowAgent());

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            PartialDeps expectedDeps = new PartialDeps(Ranges.of(range(0, 12)), KeyDeps.NONE, RangeDeps.NONE, KeyDeps.NONE);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, Timestamp.fromValues(2, 110, ID1), expectedDeps),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }
}
