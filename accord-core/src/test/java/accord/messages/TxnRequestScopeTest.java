package accord.messages;

import accord.primitives.KeyRange;
import accord.primitives.Route;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.primitives.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.Utils.idSet;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.impl.IntKey.scope;

public class TxnRequestScopeTest
{
    @Test
    void createDisjointScopeTest()
    {
        Keys keys = keys(150);
        Route route = keys.toRoute(keys.get(0).toRoutingKey());
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(4, 5, 6), idSet(4, 5)));

        Topologies.Multi topologies = new Topologies.Multi();
        topologies.add(topology2);
        topologies.add(topology1);

        Assertions.assertEquals(scope(150), TxnRequest.computeScope(id(1), topologies, route).toRoutingKeys());
        Assertions.assertEquals(1, TxnRequest.computeWaitForEpoch(id(1), topologies, keys));
        Assertions.assertEquals(scope(150), TxnRequest.computeScope(id(4), topologies, route).toRoutingKeys());
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(4), topologies, keys));
    }

    @Test
    void movingRangeTest()
    {
        Keys keys = keys(150, 250);
        Route route = keys.toRoute(keys.get(0).toRoutingKey());

        KeyRange range1 = range(100, 200);
        KeyRange range2 = range(200, 300);
        Topology topology1 = topology(1,
                                      shard(range1, idList(1, 2, 3), idSet(1, 2)),
                                      shard(range2, idList(4, 5, 6), idSet(4, 5)) );
        // reverse ownership
        Topology topology2 = topology(2,
                                      shard(range1, idList(4, 5, 6), idSet(4, 5)),
                                      shard(range2, idList(1, 2, 3), idSet(1, 2)) );

        Topologies.Multi topologies = new Topologies.Multi();
        topologies.add(topology2);
        topologies.add(topology1);

        Assertions.assertEquals(scope(150, 250), TxnRequest.computeScope(id(1), topologies, route).toRoutingKeys());
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(1), topologies, keys));
        Assertions.assertEquals(scope(150, 250), TxnRequest.computeScope(id(4), topologies, route).toRoutingKeys());
        Assertions.assertEquals(2, TxnRequest.computeWaitForEpoch(id(4), topologies, keys));
    }
}
