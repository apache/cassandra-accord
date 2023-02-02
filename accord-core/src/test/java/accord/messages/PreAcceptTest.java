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

import accord.api.RoutingKey;
import accord.impl.*;
import accord.impl.CommandsForKey.CommandLoader;
import accord.impl.CommandsForKey.CommandTimeseries;
import accord.impl.IntKey.Raw;
import accord.impl.mock.*;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.impl.mock.MockCluster.Clock;
import accord.primitives.*;
import accord.topology.Topology;
import accord.utils.EpochFunction;
import accord.utils.ThreadPoolScheduler;
import accord.local.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static accord.Utils.*;
import static accord.impl.InMemoryCommandStore.inMemory;
import static accord.impl.IntKey.range;
import static accord.impl.IntKey.routing;
import static accord.impl.mock.MockCluster.configService;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Utils.listOf;

public class PreAcceptTest
{
    private static final Id ID1 = id(1);
    private static final Id ID2 = id(2);
    private static final Id ID3 = id(3);
    private static final List<Id> IDS = listOf(ID1, ID2, ID3);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, IntKey.range(0, 100));
    private static final Ranges RANGE = Ranges.single(IntKey.range(0, 100));
    private static final Ranges FULL_RANGE = Ranges.single(IntKey.range(routing(Integer.MIN_VALUE), routing(Integer.MAX_VALUE)));

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
                        new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter()),
                        new TestAgent(),
                        new Random(),
                        scheduler,
                        SizeOfIntersectionSorter.SUPPLIER,
                        SimpleProgressLog::new,
                        InMemoryCommandStores.Synchronized::new);
    }

    private static PreAccept preAccept(TxnId txnId, Txn txn, RoutingKey homeKey)
    {
        FullRoute<?> route = txn.keys().toRoute(homeKey);
        return PreAccept.SerializerSupport.create(txnId, route.slice(FULL_RANGE), txnId.epoch(), txnId.epoch(), false, txnId.epoch(), txn.slice(FULL_RANGE, true), route);
    }

    static <T, D> Stream<T> convert(CommandTimeseries<D> timeseries, BiFunction<CommandLoader<D>, D, T> get)
    {
        return timeseries.all().map(d -> get.apply(timeseries.loader(), d));
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
            Raw key = IntKey.key(10);
            CommandStore commandStore = node.unsafeForKey(key);
            Assertions.assertFalse(inMemory(commandStore).hasCommandsForKey(key));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = preAccept(txnId, txn, key.toUnseekable());
            clock.increment(10);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            commandStore.execute(PreLoadContext.contextFor(txnId, txn.keys()), safeStore -> {
                CommandsForKey cfk = ((AbstractSafeCommandStore) safeStore).commandsForKey(key).current();
                TxnId commandId = convert(cfk.byId(), CommandLoader::txnId).findFirst().get();
                Command command = safeStore.command(commandId).current();
                Assertions.assertEquals(Status.PreAccepted, command.status());
            });

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            PartialDeps expectedDeps = new PartialDeps(Ranges.of(range(0, 12)), KeyDeps.NONE, RangeDeps.NONE);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, txnId, expectedDeps),
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
            Raw key = IntKey.key(10);
            CommandStore commandStore = node.unsafeForKey(key);
            Assertions.assertFalse(inMemory(commandStore).hasCommandsForKey(key));

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
            PartialDeps expectedDeps = new PartialDeps(Ranges.of(range(0, 12)), KeyDeps.NONE, RangeDeps.NONE);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId2, Timestamp.fromValues(1, 110, ID1), expectedDeps),
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
        Raw key = IntKey.key(10);
        try
        {
            Keys keys = Keys.of(key);
            TxnId txnId = new TxnId(1, 110, Write, Key, ID2);
            PreAccept preAccept = preAccept(txnId, writeTxn(keys), key.toUnseekable());
            preAccept.process(node, ID2, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            PartialDeps expectedDeps = new PartialDeps(Ranges.of(range(0, 12)), KeyDeps.NONE, RangeDeps.NONE);
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
            Raw key = IntKey.key(10);
            Keys keys = Keys.of(key);
            CommandStore commandStore = node.unsafeForKey(key);

            configService(node).reportTopology(node.topology().current().withEpoch(2));
            messageSink.clearHistory();

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(keys);
            PreAccept preAccept = preAccept(txnId, txn, key.toUnseekable());

            clock.increment(10);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            commandStore.execute(PreLoadContext.contextFor(txnId, txn.keys()), safeStore -> {
                CommandsForKey cfk = ((AbstractSafeCommandStore) safeStore).commandsForKey(key).current();
                TxnId commandId = convert(cfk.byId(), CommandLoader::txnId).findFirst().get();
                Command command = safeStore.command(commandId).current();
                Assertions.assertEquals(Status.PreAccepted, command.status());
            });

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            PartialDeps expectedDeps = new PartialDeps(Ranges.of(range(0, 12)), KeyDeps.NONE, RangeDeps.NONE);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, Timestamp.fromValues(2, 110, ID1), expectedDeps),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }
}
