package accord.coordinate.tracking;

import accord.Utils;
import accord.impl.TopologyUtils;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Shards;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.shards;

public class QuorumTrackerTest
{
    private static final Node.Id[] ids = Utils.ids(5).toArray(Node.Id[]::new);
    private static final KeyRanges ranges = TopologyUtils.initialRanges(5, 500);
    private static final Shards topology = TopologyUtils.initialTopology(ids, ranges, 3);
        /*
        (000, 100](100, 200](200, 300](300, 400](400, 500]
        [1, 2, 3] [2, 3, 4] [3, 4, 5] [4, 5, 1] [5, 1, 2]
         */

    private static void assertResponseState(QuorumTracker responses,
                                            boolean quorumReached,
                                            boolean failed,
                                            boolean hasOutstandingResponses)
    {
        Assertions.assertEquals(quorumReached, responses.hasReachedQuorum());
        Assertions.assertEquals(failed, responses.hasFailed());
        Assertions.assertEquals(hasOutstandingResponses, responses.hasInFlight());
    }

    @Test
    void singleShard()
    {
        Shards subShards = shards(topology.get(0));
        QuorumTracker responses = new QuorumTracker(subShards);

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, true);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, true, false, true);

        responses.recordSuccess(ids[2]);
        assertResponseState(responses, true, false, false);
    }

    /**
     * responses from unexpected endpoints should be ignored
     */
    @Test
    void unexpectedResponsesAreIgnored()
    {
        Shards subShards = shards(topology.get(0));
        QuorumTracker responses = new QuorumTracker(subShards);

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, true);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, true, false, true);

        Assertions.assertFalse(subShards.get(0).nodes.contains(ids[4]));
        responses.recordSuccess(ids[4]);
        assertResponseState(responses, true, false, true);
    }

    @Test
    void failure()
    {
        Shards subShards = shards(topology.get(0));
        QuorumTracker responses = new QuorumTracker(subShards);

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, true);

        responses.recordFailure(ids[1]);
        assertResponseState(responses, false, false, true);

        responses.recordFailure(ids[2]);
        assertResponseState(responses, false, true, false);
    }

    @Test
    void multiShard()
    {
        Shards subShards = new Shards(new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        QuorumTracker responses = new QuorumTracker(subShards);
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        Assertions.assertSame(subShards.get(0), responses.unsafeGet(0).shard);
        Assertions.assertSame(subShards.get(1), responses.unsafeGet(1).shard);
        Assertions.assertSame(subShards.get(2), responses.unsafeGet(2).shard);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, false, false, true);
        responses.recordSuccess(ids[2]);
        assertResponseState(responses, false, false, true);
        responses.recordSuccess(ids[3]);
        assertResponseState(responses, true, false, true);
    }
}
