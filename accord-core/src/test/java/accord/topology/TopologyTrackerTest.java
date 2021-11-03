package accord.topology;

import accord.api.KeyRange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.impl.IntKey.range;

public class TopologyTrackerTest
{
    @Test
    void fastPathReconfiguration()
    {
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyTracker service = new TopologyTracker(id(1));

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
}
