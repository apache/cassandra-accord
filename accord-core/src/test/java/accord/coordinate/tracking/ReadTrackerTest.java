package accord.coordinate.tracking;

import accord.Utils;
import accord.impl.TopologyUtils;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static accord.Utils.topologies;
import static accord.Utils.topology;

public class ReadTrackerTest
{
    private static final Node.Id[] ids = Utils.ids(5).toArray(Node.Id[]::new);
    private static final KeyRanges ranges = TopologyUtils.initialRanges(5, 500);
    private static final Topology topology = TopologyUtils.initialTopology(ids, ranges, 3);
        /*
        (000, 100](100, 200](200, 300](300, 400](400, 500]
        [1, 2, 3] [2, 3, 4] [3, 4, 5] [4, 5, 1] [5, 1, 2]
         */

    private static void assertResponseState(ReadTracker responses,
                                            boolean complete,
                                            boolean failed)
    {
        Assertions.assertEquals(complete, responses.hasCompletedRead());
        Assertions.assertEquals(failed, responses.hasFailed());
    }

    @Test
    void singleShard()
    {
        Topology subTopology = topology(topology.get(0));
        ReadTracker tracker = new ReadTracker(topologies(subTopology));

        tracker.recordInflightRead(ids[0]);
        assertResponseState(tracker, false, false);

        tracker.recordReadSuccess(ids[0]);
        assertResponseState(tracker, true, false);
    }

    @Test
    void singleShardRetry()
    {
        Topology subTopology = topology(topology.get(0));
        ReadTracker tracker = new ReadTracker(topologies(subTopology));

        tracker.recordInflightRead(ids[0]);
        assertResponseState(tracker, false, false);

        tracker.recordReadFailure(ids[0]);
        assertResponseState(tracker, false, false);

        tracker.recordInflightRead(ids[1]);
        assertResponseState(tracker, false, false);

        tracker.recordReadSuccess(ids[1]);
        assertResponseState(tracker, true, false);
    }

    @Test
    void singleShardFailure()
    {
        Topology subTopology = topology(topology.get(0));
        ReadTracker tracker = new ReadTracker(topologies(subTopology));

        tracker.recordInflightRead(ids[0]);
        tracker.recordReadFailure(ids[0]);
        assertResponseState(tracker, false, false);

        tracker.recordInflightRead(ids[1]);
        tracker.recordReadFailure(ids[1]);
        assertResponseState(tracker, false, false);

        tracker.recordInflightRead(ids[2]);
        tracker.recordReadFailure(ids[2]);
        assertResponseState(tracker, false, true);
    }

    @Test
    void multiShardSuccess()
    {
        Topology subTopology = new Topology(1, new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        ReadTracker responses = new ReadTracker(topologies(subTopology));
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        responses.recordInflightRead(ids[2]);
        responses.recordReadSuccess(ids[2]);
        assertResponseState(responses, true, false);
    }

    @Test
    void multiShardRetryAndReadSet()
    {
        Topology subTopology = new Topology(1, new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        ReadTracker responses = new ReadTracker(topologies(subTopology));
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        Assertions.assertEquals(Sets.newHashSet(ids[2]), responses.computeMinimalReadSetAndMarkInflight());

        assertResponseState(responses, false, false);

        responses.recordReadFailure(ids[2]);
        assertResponseState(responses, false, false);

        Assertions.assertEquals(Sets.newHashSet(ids[1], ids[3]), responses.computeMinimalReadSetAndMarkInflight());
        assertResponseState(responses, false, false);

        responses.recordReadFailure(ids[1]);
        Assertions.assertEquals(Sets.newHashSet(ids[0]), responses.computeMinimalReadSetAndMarkInflight());

        responses.recordReadSuccess(ids[3]);
        assertResponseState(responses, false, false);
        Assertions.assertEquals(Collections.emptySet(), responses.computeMinimalReadSetAndMarkInflight());

        responses.recordReadSuccess(ids[0]);
        assertResponseState(responses, true, false);
    }
}
