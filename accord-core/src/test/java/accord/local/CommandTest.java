package accord.local;

import accord.api.ProgressLog;
import accord.api.TestableConfigurationService;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.impl.mock.MockStore;
import accord.local.CommandStores.Synchronized;
import accord.local.Node.Id;
import accord.topology.Topology;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.txn.Txn;
import accord.primitives.TxnId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;

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
        final AtomicReference<Topology> local = new AtomicReference<>(TOPOLOGY);
        final MockStore data = new MockStore();
    }

    private static void setTopologyEpoch(AtomicReference<Topology> topology, long epoch)
    {
        topology.set(topology.get().withEpoch(epoch));
    }

    private static CommandStore createStore(CommandStoreSupport storeSupport)
    {
        return createNode(ID1, storeSupport).unsafeByIndex(0);
    }

    private static class NoOpProgressLog implements ProgressLog
    {
        @Override
        public void preaccept(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
        }

        @Override
        public void accept(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
        }

        @Override
        public void commit(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
        }

        @Override
        public void readyToExecute(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
        }

        @Override
        public void execute(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
        }

        @Override
        public void invalidate(TxnId txnId, boolean isProgressShard, boolean isHomeShard)
        {
        }

        @Override
        public void executedOnAllShards(TxnId txnId, Set<Id> persistedOn)
        {
        }

        @Override
        public void waiting(TxnId blockedBy, @Nullable Keys someKeys)
        {
        }
    }

    private static Node createNode(Id id, CommandStoreSupport storeSupport)
    {
        return new Node(id, null, new MockConfigurationService(null, (epoch, service) -> { }, storeSupport.local.get()),
                        new MockCluster.Clock(100), () -> storeSupport.data, new TestAgent(), new Random(), null,
                        ignore -> ignore2 -> new NoOpProgressLog(), Synchronized::new);
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

        command.preaccept(txn, KEY, KEY);
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(txnId, command.executeAt());
    }

    @Test
    void supersedingEpochWitnessTest()
    {
        CommandStoreSupport support = new CommandStoreSupport();
        CommandStore commands = createStore(support);
        TxnId txnId = commands.node().nextTxnId();
        ((MockCluster.Clock)commands.node().unsafeGetNowSupplier()).increment(10);
        Txn txn = writeTxn(Keys.of(KEY));

        Command command = new Command(commands, txnId);
        Assertions.assertEquals(Status.NotWitnessed, command.status());
        Assertions.assertNull(command.executeAt());

        setTopologyEpoch(support.local, 2);
        ((TestableConfigurationService)commands.node().configService()).reportTopology(support.local.get().withEpoch(2));
        Timestamp expectedTimestamp = new Timestamp(2, 110, 0, ID1);
        commands.process((Consumer<? super CommandStore>) cstore -> command.preaccept(txn, KEY, KEY));
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(expectedTimestamp, command.executeAt());
    }
}
