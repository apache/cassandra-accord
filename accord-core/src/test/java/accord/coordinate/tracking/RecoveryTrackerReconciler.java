package accord.coordinate.tracking;

import accord.coordinate.tracking.RecoveryTracker.RecoveryShardTracker;
import accord.local.Node;
import accord.topology.Topologies;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Random;

// TODO (required, testing): check fast path accounting
public class RecoveryTrackerReconciler extends TrackerReconciler<RecoveryShardTracker, RecoveryTracker, RecoveryTrackerReconciler.Rsp>
{
    enum Rsp { FAST, SLOW, FAIL }

    RecoveryTrackerReconciler(Random random, Topologies topologies)
    {
        this(random, new RecoveryTracker(topologies));
    }

    private RecoveryTrackerReconciler(Random random, RecoveryTracker tracker)
    {
        super(random, Rsp.class, tracker, new ArrayList<>(tracker.nodes()));
    }

    @Override
    RequestStatus invoke(Rsp event, RecoveryTracker tracker, Node.Id from)
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
                Assertions.assertTrue(tracker.any(RecoveryShardTracker::hasFailed));
                Assertions.assertFalse(tracker.all(RecoveryShardTracker::hasReachedQuorum));
                break;

            case Success:
                Assertions.assertTrue(tracker.all(RecoveryShardTracker::hasReachedQuorum));
                Assertions.assertFalse(tracker.any(RecoveryShardTracker::hasFailed));
                break;

            case NoChange:
                Assertions.assertFalse(tracker.all(RecoveryShardTracker::hasReachedQuorum));
                Assertions.assertFalse(tracker.any(RecoveryShardTracker::hasFailed));
        }
    }
}
