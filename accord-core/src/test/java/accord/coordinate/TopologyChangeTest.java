package accord.coordinate;

import accord.api.KeyRange;
import accord.impl.mock.MockCluster;
import accord.local.Command;
import accord.local.Node;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;

public class TopologyChangeTest
{
    /**
     * If a replica receives a preaccept request from a node on an earlier epoch, it's response should
     * include the more recent topologies
     */
    @Test
    void updateOnPreaccept()
    {

    }

    /**
     * If a coordinator discovers a replica is on an earlier epoch during preaccept, it's accept message
     * should include the more recent topologies
     */
    @Test
    void updateOnAccept()
    {

    }

//    @Test
//    void disjointElectorate() throws Throwable
//    {
//        Keys keys = keys(150);
//        KeyRange range = range(100, 200);
//        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
//        Topology topology2 = topology(2, shard(range, idList(4, 5, 6), idSet(4, 5)));
//        try (MockCluster cluster = MockCluster.builder().nodes(6).topology(topology1).build())
//        {
//            Node node1 = cluster.get(1);
//            TxnId txnId1 = node1.nextTxnId();
//            Txn txn1 = writeTxn(keys);
//            node1.coordinate(txnId1, txn1).toCompletableFuture().get();
//            node1.local(keys).forEach(commands -> {
//                Command command = commands.command(txnId1);
//                Assertions.assertTrue(command.savedDeps().isEmpty());
//            });
//
//            cluster.get(4).updateTopology(topology2);
//            cluster.get(5).updateTopology(topology2);
//            cluster.get(6).updateTopology(topology2);
//            Node node4 = cluster.get(4);
//
//            TxnId txnId2 = node1.nextTxnId();
//            Txn txn2 = writeTxn(keys);
//            node4.coordinate(txnId2, txn2).toCompletableFuture().get();
//            node4.local(keys).forEach(commands -> {
//                Command command = commands.command(txnId2);
//                Assertions.assertTrue(command.savedDeps().contains(txnId1));
//            });
//        }
//    }
}
