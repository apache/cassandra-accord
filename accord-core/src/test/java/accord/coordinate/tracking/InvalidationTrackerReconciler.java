package accord.coordinate.tracking;

import accord.coordinate.tracking.InvalidationTracker.InvalidationShardTracker;
import accord.local.Node;
import accord.topology.Topologies;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Random;

public class InvalidationTrackerReconciler extends TrackerReconciler<InvalidationShardTracker, InvalidationTracker, InvalidationTrackerReconciler.Rsp>
{
    enum Rsp { PROMISED_FAST, NOT_PROMISED_FAST, PROMISED_SLOW, NOT_PROMISED_SLOW, FAIL }

    InvalidationTrackerReconciler(Random random, Topologies topologies)
    {
        this(random, new InvalidationTracker(topologies));
    }

    private InvalidationTrackerReconciler(Random random, InvalidationTracker tracker)
    {
        super(random, Rsp.class, tracker, new ArrayList<>(tracker.nodes()));
    }

    @Override
    RequestStatus invoke(Rsp event, InvalidationTracker tracker, Node.Id from)
    {
        switch (event)
        {
            default: throw new AssertionError();
            case PROMISED_FAST: inflight.remove(from); return tracker.recordSuccess(from, true, true);
            case PROMISED_SLOW: inflight.remove(from); return tracker.recordSuccess(from, true, false);
            case NOT_PROMISED_FAST: inflight.remove(from); return tracker.recordSuccess(from, false, true);
            case NOT_PROMISED_SLOW: inflight.remove(from); return tracker.recordSuccess(from, false, false);
            case FAIL: inflight.remove(from); return tracker.recordFailure(from);
        }
    }

    @Override
    void validate(RequestStatus status)
    {
        switch (status)
        {
            case Failed:
                Assertions.assertTrue(tracker.all(InvalidationShardTracker::isDecided));
                Assertions.assertTrue(tracker.any(InvalidationShardTracker::isPromiseRejected));
                Assertions.assertFalse(tracker.any(InvalidationShardTracker::isPromised) && tracker.any(InvalidationShardTracker::isFastPathRejected));
                break;

            case Success:
                Assertions.assertTrue(tracker.any(InvalidationShardTracker::isPromised));
                Assertions.assertTrue(tracker.isPromised());
                Assertions.assertTrue(tracker.isSafeToInvalidate() || tracker.all(InvalidationShardTracker::isPromised));
                if (tracker.any(InvalidationShardTracker::isFastPathRejected))
                    Assertions.assertTrue(tracker.isSafeToInvalidate());
                break;

            case NoChange:
                Assertions.assertFalse(tracker.any(InvalidationShardTracker::isPromised)
                        && tracker.any(InvalidationShardTracker::isFastPathRejected));
                // TODO (low priority): it would be nice for InvalidationShardTracker to respond as soon as no shards are able to promise, but would require significant refactoring
//                Assertions.assertTrue(tracker.any(InvalidationShardTracker::canPromise));
                Assertions.assertFalse(tracker.all(InvalidationShardTracker::isDecided));
        }
    }
}
