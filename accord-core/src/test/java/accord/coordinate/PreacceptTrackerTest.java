package accord.coordinate;

import accord.Utils;
import accord.coordinate.tracking.FastPathTracker;
import accord.impl.TopologyUtils;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Shards;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.shards;

public class PreacceptTrackerTest
{
    private static final Node.Id[] ids = Utils.ids(5).toArray(Node.Id[]::new);
    private static final KeyRanges ranges = TopologyUtils.initialRanges(5, 500);
    private static final Shards topology = TopologyUtils.initialTopology(ids, ranges, 3);
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
        Shards subShards = shards(topology.get(0));
        FastPathTracker responses = new FastPathTracker<>(subShards, Agree.ShardTracker[]::new, Agree.ShardTracker::new);

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
        Shards subShards = shards(topology.get(0));
        FastPathTracker responses = new FastPathTracker<>(subShards, Agree.ShardTracker[]::new, Agree.ShardTracker::new);

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
        Shards subShards = shards(topology.get(0));
        FastPathTracker responses = new FastPathTracker<>(subShards, Agree.ShardTracker[]::new, Agree.ShardTracker::new);

        responses.recordSuccess(ids[0], false);
        assertResponseState(responses, false, false, false, true);

        responses.recordSuccess(ids[1], false);
        assertResponseState(responses, true, false, false, true);

        Assertions.assertFalse(subShards.get(0).nodes.contains(ids[4]));
        responses.recordSuccess(ids[4], false);
        assertResponseState(responses, true, false, false, true);
    }

    @Test
    void failure()
    {
        Shards subShards = shards(topology.get(0));
        FastPathTracker<?> responses = new FastPathTracker<>(subShards, Agree.ShardTracker[]::new, Agree.ShardTracker::new);

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
        Shards subShards = new Shards(1, new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        FastPathTracker<Agree.ShardTracker> responses = new FastPathTracker<>(subShards, Agree.ShardTracker[]::new, Agree.ShardTracker::new);
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        Assertions.assertSame(subShards.get(0), responses.unsafeGet(0).shard);
        Assertions.assertSame(subShards.get(1), responses.unsafeGet(1).shard);
        Assertions.assertSame(subShards.get(2), responses.unsafeGet(2).shard);

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
}
