package accord.topology;

import accord.api.KeyRange;
import accord.txn.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;

public class TopologyTrackerTest
{
    @Test
    void fastPathReconfiguration()
    {
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyTracker service = new TopologyTracker();

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1);
        Assertions.assertTrue(service.getEpochStateUnsafe(1).acknowledged());

        service.onTopologyUpdate(topology2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).acknowledged());

        service.onEpochAcknowledgement(id(1), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).acknowledged());

        service.onEpochAcknowledgement(id(2), 2);
        Assertions.assertTrue(service.getEpochStateUnsafe(2).acknowledged());

        // TODO: test fast path enabling
    }

    private static TopologyTracker tracker()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(3, 4)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));

        TopologyTracker service = new TopologyTracker();
        service.onTopologyUpdate(topology1);
        service.onTopologyUpdate(topology2);

        return service;
    }

    @Test
    void acknowledgedFor()
    {
        TopologyTracker service = tracker();

        Assertions.assertFalse(service.getEpochStateUnsafe(2).acknowledged());
        service.onEpochAcknowledgement(id(1), 2);
        service.onEpochAcknowledgement(id(2), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).acknowledged());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).acknowledgedFor(keys(150)));
        Assertions.assertFalse(service.getEpochStateUnsafe(2).acknowledgedFor(keys(250)));
    }

    @Test
    void syncCompleteFor()
    {
        TopologyTracker service = tracker();

        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        service.onEpochAcknowledgement(id(1), 2);
        service.onEpochSyncComplete(id(1), 2);
        service.onEpochAcknowledgement(id(2), 2);
        service.onEpochSyncComplete(id(2), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncCompleteFor(keys(150)));
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncCompleteFor(keys(250)));
    }

    @Test
    void forKeys()
    {
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyTracker service = new TopologyTracker();

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1);
        Assertions.assertTrue(service.getEpochStateUnsafe(1).acknowledged());

        service.onTopologyUpdate(topology2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).acknowledged());

        Keys keys = keys(150);
        Assertions.assertEquals(topologies(topology2.forKeys(keys), topology1.forKeys(keys)),
                                service.forKeys(keys));

        service.onEpochAcknowledgement(id(1), 2);
        service.onEpochAcknowledgement(id(2), 2);
        Assertions.assertEquals(topologies(topology2.forKeys(keys)),
                                service.forKeys(keys));
    }

    /**
     * Previous epoch topologies should only be included if they haven't been acknowledged, even
     * if the previous epoch is awaiting acknowledgement from all nodes
     */
    @Test
    void forKeysPartiallyAcknowledged()
    {

    }

    /**
     * nodes from
     */
    @Test
    void nodesFor()
    {

    }
}
