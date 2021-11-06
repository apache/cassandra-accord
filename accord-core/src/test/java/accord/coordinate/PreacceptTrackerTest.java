package accord.coordinate;

import accord.Utils;
import accord.api.KeyRange;
import accord.coordinate.tracking.FastPathTracker;
import accord.impl.TopologyUtils;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.txn.Keys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.*;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;

public class PreacceptTrackerTest
{
    private static final Node.Id[] ids = Utils.ids(5).toArray(Node.Id[]::new);
    private static final KeyRanges ranges = TopologyUtils.initialRanges(5, 500);
    private static final Topology topology = TopologyUtils.initialTopology(ids, ranges, 3);
        /*
        (000, 100](100, 200](200, 300](300, 400](400, 500]
        [1, 2, 3] [2, 3, 4] [3, 4, 5] [4, 5, 1] [5, 1, 2]
         */

    private static void assertResponseState(FastPathTracker<?> responses,
                                            boolean quorumReached,
                                            boolean fastPathAccepted,
                                            boolean failed,
                                            boolean hasOutstandingResponses)
    {
        Assertions.assertEquals(quorumReached, responses.hasReachedQuorum());
        Assertions.assertEquals(fastPathAccepted, responses.hasMetFastPathCriteria());
        Assertions.assertEquals(failed, responses.hasFailed());
        Assertions.assertEquals(hasOutstandingResponses, responses.hasInFlight());
    }

    @Test
    void singleShard()
    {
        Topology subTopology = topology(topology.get(0));
        FastPathTracker responses = new FastPathTracker<>(topologies(subTopology), Agree.ShardTracker[]::new, Agree.ShardTracker::new);

        responses.recordSuccess(ids[0], false);
        assertResponseState(responses, false, false, false, true);

        responses.recordSuccess(ids[1], false);
        assertResponseState(responses, true, false, false, true);

        responses.recordSuccess(ids[2], false);
        assertResponseState(responses, true, false, false, false);
    }

    @Test
    void singleShardFastPath()
    {
        Topology subTopology = topology(topology.get(0));
        FastPathTracker responses = new FastPathTracker<>(topologies(subTopology), Agree.ShardTracker[]::new, Agree.ShardTracker::new);

        responses.recordSuccess(ids[0], true);
        assertResponseState(responses, false, false, false, true);

        responses.recordSuccess(ids[1], true);
        assertResponseState(responses, true, false, false, true);

        responses.recordSuccess(ids[2], true);
        assertResponseState(responses, true, true, false, false);
    }

    /**
     * responses from unexpected endpoints should be ignored
     */
    @Test
    void unexpectedResponsesAreIgnored()
    {
        Topology subTopology = topology(topology.get(0));
        FastPathTracker responses = new FastPathTracker<>(topologies(subTopology), Agree.ShardTracker[]::new, Agree.ShardTracker::new);

        responses.recordSuccess(ids[0], false);
        assertResponseState(responses, false, false, false, true);

        responses.recordSuccess(ids[1], false);
        assertResponseState(responses, true, false, false, true);

        Assertions.assertFalse(subTopology.get(0).nodes.contains(ids[4]));
        responses.recordSuccess(ids[4], false);
        assertResponseState(responses, true, false, false, true);
    }

    @Test
    void failure()
    {
        Topology subTopology = topology(topology.get(0));
        FastPathTracker<?> responses = new FastPathTracker<>(topologies(subTopology), Agree.ShardTracker[]::new, Agree.ShardTracker::new);

        responses.recordSuccess(ids[0], true);
        assertResponseState(responses, false, false, false, true);

        responses.recordFailure(ids[1]);
        assertResponseState(responses, false, false, false, true);

        responses.recordFailure(ids[2]);
        assertResponseState(responses, false, false, true, false);
    }

    @Test
    void multiShard()
    {
        Topology subTopology = new Topology(1, new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        FastPathTracker<Agree.ShardTracker> responses = new FastPathTracker<>(topologies(subTopology), Agree.ShardTracker[]::new, Agree.ShardTracker::new);
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        Assertions.assertSame(subTopology.get(0), responses.unsafeGet(0).shard);
        Assertions.assertSame(subTopology.get(1), responses.unsafeGet(1).shard);
        Assertions.assertSame(subTopology.get(2), responses.unsafeGet(2).shard);

        responses.recordSuccess(ids[1], true);
        assertResponseState(responses, false, false, false, true);

        responses.recordSuccess(ids[2], true);
        assertResponseState(responses, false, false, false, true);

        responses.recordSuccess(ids[3], true);
        // the middle shard will have reached fast path
        Assertions.assertTrue(responses.unsafeGet(1).hasMetFastPathCriteria());
        // but since the others haven't, it won't report it as accepted
        assertResponseState(responses, true, false, false, true);

        responses.recordSuccess(ids[0], true);
        responses.recordSuccess(ids[4], true);
        assertResponseState(responses, true, true, false, false);
    }

    @Test
    void multiTopology()
    {
        Keys keys = keys(150);
        KeyRange range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(4, 5, 6), idSet(4, 5)));

    }
}
