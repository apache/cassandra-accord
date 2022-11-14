package accord.coordinate.tracking;

import accord.coordinate.tracking.QuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.topology.Topologies;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Random;

public class QuorumTrackerReconciler extends TrackerReconciler<QuorumShardTracker, QuorumTracker, QuorumTrackerReconciler.Rsp>
{
    enum Rsp { QUORUM, FAIL }

    QuorumTrackerReconciler(Random random, Topologies topologies)
    {
        this(random, new QuorumTracker(topologies));
    }

    private QuorumTrackerReconciler(Random random, QuorumTracker tracker)
    {
        super(random, Rsp.class, tracker, new ArrayList<>(tracker.nodes()));
    }

    @Override
    RequestStatus invoke(Rsp event, QuorumTracker tracker, Node.Id from)
    {
        switch (event)
        {
            default: throw new AssertionError();
            case QUORUM: inflight.remove(from); return tracker.recordSuccess(from);
            case FAIL:   inflight.remove(from); return tracker.recordFailure(from);
        }
    }

    @Override
    void validate(RequestStatus status)
    {
        switch (status)
        {
            case Failed:
                Assertions.assertTrue(tracker.any(QuorumShardTracker::hasFailed));
                Assertions.assertFalse(tracker.all(QuorumShardTracker::hasReachedQuorum));
                break;

            case Success:
                Assertions.assertTrue(tracker.all(QuorumShardTracker::hasReachedQuorum));
                Assertions.assertFalse(tracker.any(QuorumShardTracker::hasFailed));
                break;

            case NoChange:
                Assertions.assertFalse(tracker.all(QuorumShardTracker::hasReachedQuorum));
                Assertions.assertFalse(tracker.any(QuorumShardTracker::hasFailed));
        }
    }
}
