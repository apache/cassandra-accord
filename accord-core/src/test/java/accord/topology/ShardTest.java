package accord.topology;

import accord.local.Node;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static accord.Utils.ids;
import static accord.impl.IntKey.range;

public class ShardTest
{
    @Test
    void toleratedFailures()
    {
        Assertions.assertEquals(0, Shard.maxToleratedFailures(1));
        Assertions.assertEquals(0, Shard.maxToleratedFailures(2));
        Assertions.assertEquals(1, Shard.maxToleratedFailures(3));
        Assertions.assertEquals(1, Shard.maxToleratedFailures(4));
        Assertions.assertEquals(2, Shard.maxToleratedFailures(5));
        Assertions.assertEquals(2, Shard.maxToleratedFailures(6));
        Assertions.assertEquals(3, Shard.maxToleratedFailures(7));
        Assertions.assertEquals(3, Shard.maxToleratedFailures(8));
        Assertions.assertEquals(4, Shard.maxToleratedFailures(9));
        Assertions.assertEquals(4, Shard.maxToleratedFailures(10));
        Assertions.assertEquals(5, Shard.maxToleratedFailures(11));
        Assertions.assertEquals(5, Shard.maxToleratedFailures(12));
        Assertions.assertEquals(6, Shard.maxToleratedFailures(13));
        Assertions.assertEquals(6, Shard.maxToleratedFailures(14));
        Assertions.assertEquals(7, Shard.maxToleratedFailures(15));
        Assertions.assertEquals(7, Shard.maxToleratedFailures(16));
        Assertions.assertEquals(8, Shard.maxToleratedFailures(17));
        Assertions.assertEquals(8, Shard.maxToleratedFailures(18));
        Assertions.assertEquals(9, Shard.maxToleratedFailures(19));
        Assertions.assertEquals(9, Shard.maxToleratedFailures(20));
    }

    int fastPathQuorumSize(int allReplicas, int electorateSize)
    {
        int f = Shard.maxToleratedFailures(allReplicas);
        return (int) Math.ceil((electorateSize + f + 1) / 2.0);
    }

    void assertFastPathQuorumSize(int expected, int replicas, int fpElectorate)
    {
        int f = Shard.maxToleratedFailures(replicas);
        int actual = Shard.fastPathQuorumSize(replicas, fpElectorate, f);
        Assertions.assertEquals(fastPathQuorumSize(replicas, fpElectorate), actual);
        Assertions.assertEquals(expected, actual);
    }

    void assertIllegalArgument(Runnable runnable, String msg)
    {
        try
        {
            runnable.run();
            Assertions.fail(msg);
        }
        catch (IllegalArgumentException e)
        {
            // noop
        }
    }

    void assertInvalidFastPathElectorateSize(int replicas, int fpElectorate)
    {
        int f = Shard.maxToleratedFailures(replicas);
        assertIllegalArgument(() -> Shard.fastPathQuorumSize(replicas, fpElectorate, f),
                              String.format("Expected exception for fp electorate size %s for replica set size %s (f %s)",
                                            fpElectorate, replicas, f));
    }

    @Test
    void fastPathQuorumSizeTest()
    {
        // rf=3
        assertFastPathQuorumSize(3, 3, 3);
        assertFastPathQuorumSize(2, 3, 2);
        assertInvalidFastPathElectorateSize(3, 1);

        // rf=4
        assertFastPathQuorumSize(3, 4, 4);
        assertFastPathQuorumSize(3, 4, 3);
        assertInvalidFastPathElectorateSize(4, 2);

        // rf=5
        assertFastPathQuorumSize(4, 5, 5);
        assertFastPathQuorumSize(4, 5, 4);
        assertFastPathQuorumSize(3, 5, 3);
        assertInvalidFastPathElectorateSize(5, 2);

        // rf=6
        assertFastPathQuorumSize(5, 6, 6);
        assertFastPathQuorumSize(4, 6, 5);
        assertFastPathQuorumSize(4, 6, 4);
        assertInvalidFastPathElectorateSize(6, 3);

        // rf=7
        assertFastPathQuorumSize(6, 7, 7);
        assertFastPathQuorumSize(5, 7, 6);
        assertFastPathQuorumSize(5, 7, 5);
        assertFastPathQuorumSize(4, 7, 4);
        assertInvalidFastPathElectorateSize(7, 3);

        // rf=8
        assertFastPathQuorumSize(6, 8, 8);
        assertFastPathQuorumSize(6, 8, 7);
        assertFastPathQuorumSize(5, 8, 6);
        assertInvalidFastPathElectorateSize(8, 4);

        // rf=9
        assertFastPathQuorumSize(7, 9, 9);
        assertFastPathQuorumSize(7, 9, 8);
        assertFastPathQuorumSize(6, 9, 7);
        assertFastPathQuorumSize(6, 9, 6);
        assertFastPathQuorumSize(5, 9, 5);
        assertInvalidFastPathElectorateSize(9, 4);
    }

    @Test
    void pendingNodeValidation()
    {
        List<Node.Id> nodes = ids(0, 3);
        Set<Node.Id> fpNodes = new HashSet<>(ids(0, 2));
        // pending nodes are part of electorate
        new Shard(range(0, 100), nodes, fpNodes, new HashSet<>(ids(3, 3)));
        // pending nodes are not part of electorate
        assertIllegalArgument(() -> new Shard(range(0, 100), nodes, fpNodes, new HashSet<>(ids(4, 4))),
                              "Expected exception for non-electorate pending nodes");
    }
}
