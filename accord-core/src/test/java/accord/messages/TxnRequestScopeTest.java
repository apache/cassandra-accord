package accord.messages;

import accord.api.KeyRange;
import accord.impl.IntKey;
import accord.messages.TxnRequest.Scope;
import accord.messages.TxnRequest.Scope.KeysForEpoch;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.txn.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.Utils.idSet;
import static accord.impl.IntKey.range;

public class TxnRequestScopeTest
{
    private static KeysForEpoch epochRanges(long epoch, Keys keys)
    {
        return new KeysForEpoch(epoch, keys);
    }

    private static KeysForEpoch epochRanges(long epoch, int... keys)
    {
        return epochRanges(epoch, IntKey.keys(keys));
    }

    private static Scope scope(long epoch, KeysForEpoch... epochKeys)
    {
        return new Scope(epoch, epochKeys);
    }

    @Test
    void createDisjointScopeTest()
    {
        Keys keys = IntKey.keys(150);
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(4, 5, 6), idSet(4, 5)));

        Topologies.Multi topologies = new Topologies.Multi();
        topologies.add(topology2);
        topologies.add(topology1);

        Assertions.assertEquals(scope(2, epochRanges(1, 150)),
                                Scope.forTopologies(id(1), topologies, keys));
        Assertions.assertEquals(scope(2, epochRanges(2, 150)),
                                Scope.forTopologies(id(4), topologies, keys));
    }

    @Test
    void movingRangeTest()
    {
        Keys keys = IntKey.keys(150, 250);
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
        Assertions.assertEquals(scope(2, epochRanges(1, 150), epochRanges(2, 250)),
                                Scope.forTopologies(id(1), topologies, keys));
        Assertions.assertEquals(scope(2, epochRanges(1, 250), epochRanges(2, 150)),
                                Scope.forTopologies(id(4), topologies, keys));
    }
}
