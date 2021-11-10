package accord.local;

import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockStore;
import accord.topology.Topology;
import accord.topology.TopologyTracker;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.Utils.id;
import static accord.Utils.writeTxn;

public class CommandTest
{
    private static final Node.Id ID1 = id(1);
    private static final Node.Id ID2 = id(2);
    private static final Node.Id ID3 = id(3);
    private static final List<Node.Id> IDS = List.of(ID1, ID2, ID3);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, IntKey.range(0, 100));
    private static final IntKey KEY = IntKey.key(10);

    private static class CommandStoreSupport
    {
        final AtomicReference<RangeMapping> mapping = new AtomicReference<>(RangeMapping.EMPTY);
        final MockStore data = new MockStore();
        final AtomicReference<Timestamp> nextTimestamp = new AtomicReference<>(Timestamp.NONE);
        final Function<Timestamp, Timestamp> uniqueNow = atleast -> {
            Timestamp next = nextTimestamp.get();
            Assertions.assertTrue(next.compareTo(atleast) >= 0);
            return next;
        };
    }

    private static void setMappingEpoch(AtomicReference<RangeMapping> mapping, long epoch)
    {
        RangeMapping current = mapping.get();
        RangeMapping next = new RangeMapping(current.ranges, current.topology.withEpoch(epoch));
        mapping.set(next);
    }

    private static CommandStore createStore(CommandStoreSupport storeSupport)
    {
        return new CommandStore.Synchronized(0,
                                             ID1,
                                             storeSupport.uniqueNow,
                                             new TestAgent(),
                                             storeSupport.data,
                                             i -> storeSupport.mapping.get());
    }

    @Test
    void noConflictWitnessTest()
    {
        CommandStoreSupport support = new CommandStoreSupport();
        CommandStore commands = createStore(support);
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = clock.idForNode(1, 1);
        Txn txn = writeTxn(Keys.of(KEY));

        Command command = new Command(commands, txnId);
        Assertions.assertEquals(Status.NotWitnessed, command.status());
        Assertions.assertNull(command.executeAt());

        command.witness(txn);
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(txnId, command.executeAt());
    }

    @Test
    void supersedingEpochWitnessTest()
    {
        CommandStoreSupport support = new CommandStoreSupport();
        CommandStore commands = createStore(support);
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = clock.idForNode(1, 1);
        Txn txn = writeTxn(Keys.of(KEY));


        Command command = new Command(commands, txnId);
        Assertions.assertEquals(Status.NotWitnessed, command.status());
        Assertions.assertNull(command.executeAt());

        setMappingEpoch(support.mapping, 2);
        Timestamp expectedTimestamp = new Timestamp(2, 110, 0, ID1);
        support.nextTimestamp.set(expectedTimestamp);
        commands.process((Consumer<? super CommandStore>) cstore -> command.witness(txn));
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(expectedTimestamp, command.executeAt());
    }
}
