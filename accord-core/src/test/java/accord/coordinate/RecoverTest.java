package accord.coordinate;

import accord.local.Node;
import accord.impl.mock.MockCluster;
import accord.impl.IntKey;
import accord.api.Key;
import accord.local.*;
import accord.txn.Keys;
import accord.messages.PreAccept;
import accord.txn.Txn;
import accord.txn.TxnId;

import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static accord.NetworkFilter.*;
import static accord.Utils.id;
import static accord.Utils.ids;
import static accord.Utils.writeTxn;

public class RecoverTest
{
    private static CommandStore getCommandShard(Node node, Key key)
    {
        return node.local(key).orElseThrow();
    }

    private static Command getCommand(Node node, Key key, TxnId txnId)
    {
        CommandStore commandStore = getCommandShard(node, key);
        Assertions.assertTrue(commandStore.hasCommand(txnId));
        return commandStore.command(txnId);
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
        Assertions.assertFalse(commandStore.hasCommand(txnId));
    }

    private static void assertTimeout(CompletionStage<?> f)
    {
        try
        {
            f.toCompletableFuture().get();
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
    void conflictTest() throws Throwable
    {
        Key key = IntKey.key(10);
        try (MockCluster cluster = MockCluster.builder().nodes(9).replication(9).build())
        {
            cluster.networkFilter.isolate(ids(7, 9));
            cluster.networkFilter.addFilter(anyId(), isId(ids(5, 6)), notMessageType(PreAccept.class));

            TxnId txnId1 = new TxnId(1, 100, 0, id(100));
            Txn txn1 = writeTxn(Keys.of(key));
            assertTimeout(Coordinate.execute(cluster.get(1), txnId1, txn1));

            TxnId txnId2 = new TxnId(1, 50, 0, id(101));
            Txn txn2 = writeTxn(Keys.of(key));
            cluster.networkFilter.clear();
            cluster.networkFilter.isolate(ids(1, 7));
            assertTimeout(Coordinate.execute(cluster.get(9), txnId2, txn2));

            cluster.nodes(ids(1, 4)).forEach(n -> assertStatus(n, key, txnId1, Status.Accepted));
            cluster.nodes(ids(5, 6)).forEach(n -> assertStatus(n, key, txnId1, Status.PreAccepted));
            cluster.nodes(ids(7, 9)).forEach(n -> assertMissing(n, key, txnId1));

            cluster.nodes(ids(1, 7)).forEach(n -> assertMissing(n, key, txnId2));
            cluster.nodes(ids(8, 9)).forEach(n -> assertStatus(n, key, txnId2, Status.PreAccepted));

            //
            cluster.networkFilter.clear();
            cluster.networkFilter.isolate(ids(1, 4));
            Coordinate.recover(cluster.get(8), txnId2, txn2).toCompletableFuture().get();

            List<Node> nodes = cluster.nodes(ids(5, 9));
            Assertions.assertTrue(txnId2.compareTo(txnId1) < 0);
            nodes.forEach(n -> assertStatus(n, key, txnId2, Status.Applied));
            nodes.forEach(n -> {
                assertStatus(n, key, txnId2, Status.Applied);
                Command command = getCommand(n, key, txnId2);
                Assertions.assertEquals(txnId1, command.executeAt());
            });
        }
    }
}
