package accord.coordinate;

import accord.local.Node;
import accord.impl.mock.MockCluster;
import accord.impl.IntKey;
import accord.api.Result;
import accord.impl.mock.MockStore;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.ids;
import static accord.Utils.writeTxn;

public class CoordinateTest
{
    @Test
    void simpleTest() throws Throwable
    {
        MockCluster cluster = MockCluster.builder().build();
        Node node = cluster.get(1);
        Assertions.assertNotNull(node);

        TxnId txnId = new TxnId(100, 0, node.id());
        Txn txn = writeTxn(IntKey.keys(10));
        Result result = Coordinate.execute(node, txnId, txn).toCompletableFuture().get();
        Assertions.assertEquals(MockStore.RESULT, result);
    }

    @Test
    void slowPathTest() throws Throwable
    {
        MockCluster cluster = MockCluster.builder().nodes(7).replication(7).build();
        cluster.networkFilter.isolate(ids(5, 7));

        Node node = cluster.get(1);
        Assertions.assertNotNull(node);

        TxnId txnId = new TxnId(100, 0, node.id());
        Txn txn = writeTxn(IntKey.keys(10));
        Result result = Coordinate.execute(node, txnId, txn).toCompletableFuture().get();
        Assertions.assertEquals(MockStore.RESULT, result);
    }
}
