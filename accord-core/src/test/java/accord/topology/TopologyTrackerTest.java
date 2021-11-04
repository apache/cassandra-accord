package accord.topology;

import accord.api.KeyRange;
import accord.txn.Keys;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

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
        Assertions.assertTrue(service.getEpochState(1).acknowledged());

        service.onTopologyUpdate(topology2);
        Assertions.assertFalse(service.getEpochState(2).acknowledged());

        service.onEpochAcknowledgement(id(1), 2);
        Assertions.assertFalse(service.getEpochState(2).acknowledged());

        service.onEpochAcknowledgement(id(2), 2);
        Assertions.assertTrue(service.getEpochState(2).acknowledged());

        // TODO: test fast path enabling
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
        Assertions.assertTrue(service.getEpochState(1).acknowledged());

        service.onTopologyUpdate(topology2);
        Assertions.assertFalse(service.getEpochState(2).acknowledged());

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
