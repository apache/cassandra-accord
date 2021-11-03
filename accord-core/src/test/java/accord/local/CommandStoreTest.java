package accord.local;

import accord.Utils;
import accord.api.KeyRange;
import accord.impl.IntKey;
import accord.impl.TopologyUtils;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static accord.impl.IntKey.key;

public class CommandStoreTest
{
//    private static void topologyUpdate(CommandStore commandStore, KeyRanges ranges, Topology topology, AtomicReference<RangeMapping> mapping)
//    {
//        RangeMapping prev = mapping.get();
//        mapping.set(RangeMapping.mapRanges(ranges, topology));
//        commandStore.process((Consumer<CommandStore>) commands -> commandStore.onTopologyChange(prev, mapping.get()));
//    }

    // FIXME: convert range conversion to post-repair range cleanup and test
//    @Test
//    void topologyChangeTest()
//    {
//        List<Node.Id> ids = Utils.ids(5);
//        KeyRanges ranges = TopologyUtils.initialRanges(5, 500);
//        Topology topology = TopologyUtils.initialTopology(ids, ranges, 3);
//        Topology local = topology.forNode(ids.get(0));
//
//        KeyRanges shards = CommandStores.shardRanges(local.ranges(), 10).get(0);
//        Assertions.assertEquals(ranges(r(0, 10), r(300, 310), r(400, 410)), shards);
//
//        AtomicReference<RangeMapping> mapping = new AtomicReference<>(RangeMapping.mapRanges(shards, topology));
//        CommandStore commandStore = new CommandStore.Synchronized(0,
//                                                                  ids.get(0),
//                                                                  null,
//                                                                  null,
//                                                                  null,
//                                                                  i -> mapping.get());
//
//        shards = shards.union(r(350, 360));
//        topologyUpdate(commandStore, shards, topology, mapping);
//
//        commandStore.commandsForKey(key(355));
//        commandStore.commandsForKey(key(356));
//        commandStore.commandsForKey(key(357));
//
//        Assertions.assertTrue(commandStore.hasCommandsForKey(key(355)));
//        Assertions.assertTrue(commandStore.hasCommandsForKey(key(356)));
//        Assertions.assertTrue(commandStore.hasCommandsForKey(key(357)));
//
//        shards = shards.difference(ranges(r(355, 360)));
//        topologyUpdate(commandStore, shards, topology, mapping);
//
//        Assertions.assertTrue(commandStore.hasCommandsForKey(key(355)));
//        Assertions.assertFalse(commandStore.hasCommandsForKey(key(356)));
//        Assertions.assertFalse(commandStore.hasCommandsForKey(key(357)));
//    }
//
    private static KeyRanges ranges(KeyRange... ranges)
    {
        return new KeyRanges(ranges);
    }

    private static KeyRange<IntKey> r(int start, int end)
    {
        return IntKey.range(start, end);
    }

}
