package accord.messages;

import accord.local.Node;
import accord.local.Node.Id;
import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.impl.mock.MockCluster.Clock;
import accord.topology.Topology;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.utils.ThreadPoolScheduler;
import accord.local.*;
import accord.txn.Keys;
import accord.impl.IntKey;
import accord.impl.mock.Network;
import accord.impl.mock.RecordingMessageSink;
import accord.impl.TestAgent;
import accord.impl.mock.MockStore;
import accord.impl.TopologyFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

import static accord.Utils.id;
import static accord.Utils.writeTxn;

public class PreAcceptTest
{
    private static final Id ID1 = id(1);
    private static final Id ID2 = id(2);
    private static final Id ID3 = id(3);
    private static final List<Id> IDS = List.of(ID1, ID2, ID3);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, IntKey.range(0, 100));

    private static Node createNode(Id nodeId, MessageSink messageSink, Clock clock)
    {
        Random random = new Random();
        MockStore store = new MockStore();
        Scheduler scheduler = new ThreadPoolScheduler();
        return new Node(nodeId, TOPOLOGY, messageSink, random, clock, () -> store,
                        new TestAgent(), scheduler, CommandStore.Factory.SINGLE_THREAD);
    }

    @Test
    void initialCommandTest()
    {
        RecordingMessageSink messageSink = new RecordingMessageSink(ID1, Network.BLACK_HOLE);
        Clock clock = new Clock(100);
        Node node = createNode(ID1, messageSink, clock);

        try
        {
            IntKey key = IntKey.key(10);
            CommandStore commandStore = node.local(key).orElseThrow();
            Assertions.assertFalse(commandStore.hasCommandsForKey(key));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = new PreAccept(txnId, txn);
            clock.increment(10);
            preAccept.process(node, ID2, 0);

            Command command = commandStore.commandsForKey(key).uncommitted.get(txnId);
            Assertions.assertEquals(Status.PreAccepted, command.status());

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, new Dependencies()),
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
            CommandStore commandStore = node.local(key).orElseThrow();
            Assertions.assertFalse(commandStore.hasCommandsForKey(key));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = new PreAccept(txnId, txn);
            preAccept.process(node, ID2, 0);
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
            PreAccept preAccept1 = new PreAccept(clock.idForNode(1, ID2), writeTxn(Keys.of(key1)));
            preAccept1.process(node, ID2, 0);

            messageSink.clearHistory();
            IntKey key2 = IntKey.key(11);
            TxnId txnId2 = new TxnId(1, 50, 0, ID3);
            PreAccept preAccept2 = new PreAccept(txnId2, writeTxn(Keys.of(key1, key2)));
            clock.increment(10);
            preAccept2.process(node, ID3, 0);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID3, messageSink.responses.get(0).to);
            Dependencies expectedDeps = new Dependencies();
            Assertions.assertEquals(new PreAccept.PreAcceptOk(new TxnId(1, 110, 0, ID1), expectedDeps),
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
        IntKey key = IntKey.key(10);
        try
        {
            TxnId txnId = new TxnId(1, 110, 0, ID2);
            PreAccept preAccept = new PreAccept(txnId, writeTxn(Keys.of(key)));
            preAccept.process(node, ID2, 0);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Dependencies expectedDeps = new Dependencies();
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, expectedDeps),
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
            CommandStore commandStore = node.local(key).orElseThrow();
            node.updateTopology(node.clusterTopology().withEpoch(2));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = new PreAccept(txnId, txn);

            clock.increment(10);
            preAccept.process(node, ID2, 0);

            Command command = commandStore.commandsForKey(key).uncommitted.get(txnId);
            Assertions.assertEquals(Status.PreAccepted, command.status());

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(new TxnId(2, 110, 0, ID1), new Dependencies()),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }
}
