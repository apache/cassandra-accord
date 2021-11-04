package accord.local;

import accord.impl.mock.MockCluster;
import accord.txn.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NodeTest
{
    private static Timestamp ts(long epoch, long real, int logical, int node)
    {
        return new Timestamp(epoch, real, logical, new Node.Id(node));
    }

    @Test
    void uniqueNowTest()
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        MockCluster cluster = MockCluster.builder().nowSupplier(clock).build();
        Node node = cluster.get(1);

        clock.increment();
        Timestamp timestamp1 = node.uniqueNow();
        Timestamp timestamp2 = node.uniqueNow();

        clock.increment();
        Timestamp timestamp3 = node.uniqueNow();

        Assertions.assertEquals(ts(1, 101, 0, 1), timestamp1);
        Assertions.assertEquals(ts(1, 101, 1, 1), timestamp2);

        Assertions.assertEquals(ts(1, 102, 0, 1), timestamp3);
    }

    @Test
    void uniqueNowEpochUpdate()
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        MockCluster cluster = MockCluster.builder().nowSupplier(clock).build();
        Node node = cluster.get(1);

        clock.increment();
        Timestamp timestamp1 = node.uniqueNow();
        Assertions.assertEquals(ts(1, 101, 0, 1), timestamp1);

        node.onTopologyUpdate(node.topologyTracker().current().withEpoch(2));
        Timestamp timestamp2 = node.uniqueNow();
        Assertions.assertEquals(ts(2, 101, 1, 1), timestamp2);
    }

    @Test
    void uniqueNowAtLeastTest()
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        MockCluster cluster = MockCluster.builder().nowSupplier(clock).build();
        Node node = cluster.get(1);

        clock.increment();
        Timestamp timestamp1 = node.uniqueNow();
        Assertions.assertEquals(ts(1, 101, 0, 1), timestamp1);

        // atLeast equal to most recent ts, logical should increment
        Assertions.assertEquals(ts(1, 101, 1, 1),
                                node.uniqueNow(timestamp1));

        // atLeast less than most recent ts
        Assertions.assertEquals(ts(1, 101, 2, 1),
                                node.uniqueNow(ts(1, 99, 0, 1)));

        // atLeast greater than most recent ts
        Assertions.assertEquals(ts(1, 110, 1, 1),
                                node.uniqueNow(ts(1, 110, 0, 2)));
    }
}
