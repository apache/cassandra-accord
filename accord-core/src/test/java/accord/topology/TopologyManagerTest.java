package accord.topology;

import accord.local.Node;
import accord.txn.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;

public class TopologyManagerTest
{
    private static final Node.Id ID = new Node.Id(1);

    @Test
    void fastPathReconfiguration()
    {
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = new TopologyManager(ID, epoch -> {});

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1);
        Assertions.assertFalse(service.getEpochStateUnsafe(1).syncComplete());

        service.onTopologyUpdate(topology2);
        Assertions.assertFalse(service.getEpochStateUnsafe(1).syncComplete());

        service.onEpochSyncComplete(id(1), 1);
        Assertions.assertFalse(service.getEpochStateUnsafe(1).syncComplete());

        service.onEpochSyncComplete(id(2), 1);
        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
    }

    private static TopologyManager tracker()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(3, 4)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));

        TopologyManager service = new TopologyManager(ID, epoch -> {});
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);

        return service;
    }

    @Test
    void syncCompleteFor()
    {
        TopologyManager service = tracker();

        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        service.onEpochSyncComplete(id(1), 1);
        service.onEpochSyncComplete(id(2), 1);
        Assertions.assertFalse(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncCompleteFor(keys(150)));
        Assertions.assertFalse(service.getEpochStateUnsafe(1).syncCompleteFor(keys(250)));
    }

    /**
     * Epochs should only report being synced if every preceding epoch is also reporting synced
     */
    @Test
    void existingEpochPendingSync()
    {
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));
        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(1, 2)));

        TopologyManager service = new TopologyManager(ID, epoch -> {});
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);
        service.onTopologyUpdate(topology3);

        Assertions.assertFalse(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        // sync epoch 2
        service.onEpochSyncComplete(id(1), 2);
        service.onEpochSyncComplete(id(2), 2);

        Assertions.assertFalse(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        // sync epoch 1
        service.onEpochSyncComplete(id(1), 1);
        service.onEpochSyncComplete(id(2), 1);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
    }

    /**
     * If a node receives sync acks for epochs it's not aware of, it should apply them when it finds out about the epoch
     */
    @Test
    void futureEpochPendingSync()
    {
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = new TopologyManager(ID, epoch -> {});
        service.onTopologyUpdate(topology1);

        // sync epoch 2
        service.onEpochSyncComplete(id(1), 1);
        service.onEpochSyncComplete(id(2), 1);

        // learn of epoch 2
        service.onTopologyUpdate(topology2);
        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
    }

    @Test
    void forKeys()
    {
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = new TopologyManager(ID, epoch -> {});

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1);

        service.onTopologyUpdate(topology2);
        Assertions.assertFalse(service.getEpochStateUnsafe(1).syncComplete());

        Keys keys = keys(150);
        Assertions.assertEquals(topologies(topology2.forKeys(keys), topology1.forKeys(keys)),
                                service.syncForKeys(keys, 0, Long.MAX_VALUE));

        service.onEpochSyncComplete(id(1), 1);
        service.onEpochSyncComplete(id(2), 1);
        Assertions.assertEquals(topologies(topology2.forKeys(keys)),
                                service.syncForKeys(keys, 0, Long.MAX_VALUE));
    }

    /**
     * Previous epoch topologies should only be included if they haven't been acknowledged, even
     * if the previous epoch is awaiting acknowledgement from all nodes
     */
    @Test
    void forKeysPartiallySynced()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(5, 6)));

        TopologyManager service = new TopologyManager(ID, epoch -> {});
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);

        // no acks, so all epoch 1 shards should be included
        Assertions.assertEquals(topologies(topology2, topology1),
                                service.syncForKeys(keys(150, 250), 0, Long.MAX_VALUE));

        // first topology acked, so only the second shard should be included
        service.onEpochSyncComplete(id(1), 1);
        service.onEpochSyncComplete(id(2), 1);
        Topologies actual = service.syncForKeys(keys(150, 250), 0, Long.MAX_VALUE);
        Assertions.assertEquals(topologies(topology2, topology(1, shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)))),
                                actual);
        Assertions.assertFalse(actual.fastPathPermitted());
    }
}
