package accord.messages;

import accord.api.KeyRange;
import accord.impl.IntKey;
import accord.topology.KeyRanges;
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
    private static TxnRequestScope.EpochRanges epochRanges(long epoch, KeyRanges ranges)
    {
        return new TxnRequestScope.EpochRanges(epoch, ranges);
    }

    private static TxnRequestScope.EpochRanges epochRanges(long epoch, KeyRange... ranges)
    {
        return epochRanges(epoch, ranges(ranges));
    }

    private static TxnRequestScope scope(long epoch, TxnRequestScope.EpochRanges... ranges)
    {
        return new TxnRequestScope(epoch, ranges);
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

        Assertions.assertEquals(scope(2, epochRanges(1, range)),
                                TxnRequestScope.forTopologies(id(1), topologies, keys));
        Assertions.assertEquals(scope(2, epochRanges(2, range)),
                                TxnRequestScope.forTopologies(id(4), topologies, keys));
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
        Assertions.assertEquals(scope(2, epochRanges(1, range1), epochRanges(2, range2)),
                                TxnRequestScope.forTopologies(id(1), topologies, keys));
        Assertions.assertEquals(scope(2, epochRanges(1, range2), epochRanges(2, range1)),
                                TxnRequestScope.forTopologies(id(4), topologies, keys));
    }
}
