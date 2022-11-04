package accord.coordinate.tracking;

import accord.coordinate.tracking.FastPathTracker.FastPathShardTracker;
import accord.coordinate.tracking.QuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.topology.Topologies;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Random;

public class FastPathTrackerReconciler extends TrackerReconciler<FastPathShardTracker, FastPathTracker, FastPathTrackerReconciler.Rsp>
{
    enum Rsp { FAST, SLOW, FAIL }

    FastPathTrackerReconciler(Random random, Topologies topologies)
    {
        this(random, new FastPathTracker(topologies));
    }

    private FastPathTrackerReconciler(Random random, FastPathTracker tracker)
    {
        super(random, Rsp.class, tracker, new ArrayList<>(tracker.nodes()));
    }

    @Override
    RequestStatus invoke(Rsp event, FastPathTracker tracker, Node.Id from)
    {
        switch (event)
        {
            default: throw new AssertionError();
            case FAST: inflight.remove(from); return tracker.recordSuccess(from, true);
            case SLOW: inflight.remove(from); return tracker.recordSuccess(from, false);
            case FAIL: inflight.remove(from); return tracker.recordFailure(from);
        }
    }

    @Override
    void validate(RequestStatus status)
    {
        switch (status)
        {
            case Failed:
                Assertions.assertTrue(tracker.any(ShardTracker::hasFailed));
                Assertions.assertFalse(tracker.all(FastPathShardTracker::hasReachedQuorum));
                break;

            case Success:
                Assertions.assertTrue(tracker.all(FastPathShardTracker::hasReachedQuorum));
                Assertions.assertTrue(tracker.all(shard -> shard.hasRejectedFastPath() || shard.hasMetFastPathCriteria()));
                Assertions.assertFalse(tracker.any(ShardTracker::hasFailed));
                break;

            case NoChange:
                Assertions.assertFalse(tracker.all(shard -> shard.hasRejectedFastPath() || shard.hasMetFastPathCriteria()) && tracker.all(FastPathShardTracker::hasReachedQuorum));
                Assertions.assertFalse(tracker.any(ShardTracker::hasFailed));
        }
    }
}
