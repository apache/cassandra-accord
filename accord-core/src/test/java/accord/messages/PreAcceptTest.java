package accord.messages;

import accord.impl.mock.*;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.impl.mock.MockCluster.Clock;
import accord.topology.Topology;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.utils.EpochFunction;
import accord.utils.ThreadPoolScheduler;
import accord.local.*;
import accord.txn.Keys;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.TopologyFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static accord.Utils.id;
import static accord.Utils.writeTxn;
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
                        scheduler,
                        CommandStore.Factory.SINGLE_THREAD);
    }

    private static TxnRequest.Scope scope(TxnId txnId, Txn txn)
    {
        return new TxnRequest.Scope(txnId.epoch, new TxnRequest.Scope.KeysForEpoch(txnId.epoch, txn.keys()));
    }

    private static PreAccept preAccept(TxnId txnId, Txn txn)
    {
        return new PreAccept(scope(txnId, txn), txnId, txn);
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
            CommandStore commandStore = node.local(key).orElseThrow();
            Assertions.assertFalse(commandStore.hasCommandsForKey(key));

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = preAccept(txnId, txn);
            clock.increment(10);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            Command command = commandStore.commandsForKey(key).uncommitted.get(txnId);
            Assertions.assertEquals(Status.PreAccepted, command.status());

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, txnId, new Dependencies()),
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
            PreAccept preAccept = preAccept(txnId, txn);
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
            PreAccept preAccept1 = preAccept(clock.idForNode(1, ID2), writeTxn(Keys.of(key1)));
            preAccept1.process(node, ID2, REPLY_CONTEXT);

            messageSink.clearHistory();
            IntKey key2 = IntKey.key(11);
            TxnId txnId2 = new TxnId(1, 50, 0, ID3);
            PreAccept preAccept2 = preAccept(txnId2, writeTxn(Keys.of(key1, key2)));
            clock.increment(10);
            preAccept2.process(node, ID3, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID3, messageSink.responses.get(0).to);
            Dependencies expectedDeps = new Dependencies();
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
            TxnId txnId = new TxnId(1, 110, 0, ID2);
            PreAccept preAccept = preAccept(txnId, writeTxn(Keys.of(key)));
            preAccept.process(node, ID2, REPLY_CONTEXT);

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Dependencies expectedDeps = new Dependencies();
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
            CommandStore commandStore = node.local(key).orElseThrow();

            configService(node).reportTopology(node.topology().current().withEpoch(2));
            messageSink.clearHistory();

            TxnId txnId = clock.idForNode(1, ID2);
            Txn txn = writeTxn(Keys.of(key));
            PreAccept preAccept = preAccept(txnId, txn);

            clock.increment(10);
            preAccept.process(node, ID2, REPLY_CONTEXT);

            Command command = commandStore.commandsForKey(key).uncommitted.get(txnId);
            Assertions.assertEquals(Status.PreAccepted, command.status());

            messageSink.assertHistorySizes(0, 1);
            Assertions.assertEquals(ID2, messageSink.responses.get(0).to);
            Assertions.assertEquals(new PreAccept.PreAcceptOk(txnId, new TxnId(2, 110, 0, ID1), new Dependencies()),
                                    messageSink.responses.get(0).payload);
        }
        finally
        {
            node.shutdown();
        }
    }
}
