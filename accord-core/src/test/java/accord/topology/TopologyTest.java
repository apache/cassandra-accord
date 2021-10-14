package accord.topology;

import accord.Utils;
import accord.api.Key;
import accord.api.KeyRange;
import accord.impl.IntKey;
import accord.impl.TopologyFactory;
import accord.local.Node;
import accord.txn.Keys;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static accord.impl.IntKey.key;
import static accord.impl.IntKey.range;

public class TopologyTest
{
    private static void assertRangeForKey(Topology topology, int key, int start, int end)
    {
        Key expectedKey = key(key);
        Shard shard = topology.forKey(key(key));
        KeyRange expectedRange = range(start, end);
        Assertions.assertTrue(expectedRange.containsKey(expectedKey));
        Assertions.assertTrue(shard.range.containsKey(expectedKey));
        Assertions.assertEquals(expectedRange, shard.range);

        Topology subTopology = topology.forKeys(Keys.of(expectedKey));
        shard = Iterables.getOnlyElement(subTopology);
        Assertions.assertTrue(shard.range.containsKey(expectedKey));
        Assertions.assertEquals(expectedRange, shard.range);
    }

    private static Topology topology(List<Node.Id> ids, int rf, KeyRange... ranges)
    {
        TopologyFactory<IntKey> topologyFactory = new TopologyFactory<>(rf, ranges);
        return topologyFactory.toTopology(ids);
    }

    private static Topology topology(int numNodes, int rf, KeyRange... ranges)
    {
        return topology(Utils.ids(numNodes), rf, ranges);
    }

    private static Topology topology(KeyRange... ranges)
    {
        return topology(1, 1, ranges);
    }

    private static KeyRange<IntKey> r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    private static void assertNoRangeForKey(Topology topology, int key)
    {
        try
        {
            topology.forKey(key(key));
            Assertions.fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // noop
        }
    }

    @Test
    void forKeyTest()
    {
        Topology topology = topology(r(0, 100), r(100, 200), r(300, 400));
        assertNoRangeForKey(topology, -50);
        assertRangeForKey(topology, 50, 0, 100);
        assertRangeForKey(topology, 100, 0, 100);
        assertNoRangeForKey(topology, 250);
        assertRangeForKey(topology, 350, 300, 400);
    }

    @Test
    void forRangesTest()
    {

    }
}
