package accord.coordinate.tracking;

import accord.coordinate.tracking.ReadTracker.ReadShardTracker;
import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

public class ReadTrackerReconciler extends TrackerReconciler<ReadShardTracker, ReadTracker, ReadTrackerReconciler.Rsp>
{
    enum Rsp { DATA, QUORUM, SLOW, FAIL }

    static class InFlightCapturingReadTracker extends ReadTracker
    {
        final List<Node.Id> inflight = new ArrayList<>();
        public InFlightCapturingReadTracker(Topologies topologies)
        {
            super(topologies);
        }

        @Override
        protected RequestStatus trySendMore()
        {
            return super.trySendMore(List::add, inflight);
        }
    }

    ReadTrackerReconciler(Random random, Topologies topologies)
    {
        this(random, new InFlightCapturingReadTracker(topologies));
    }

    private ReadTrackerReconciler(Random random, InFlightCapturingReadTracker tracker)
    {
        super(random, Rsp.class, tracker, tracker.inflight);
    }

    @Override
    void test()
    {
        tracker.trySendMore();
        super.test();
    }

    @Override
    RequestStatus invoke(Rsp event, ReadTracker tracker, Node.Id from)
    {
        switch (event)
        {
            default: throw new AssertionError();
            case DATA:   inflight.remove(from); return tracker.recordReadSuccess(from);
            case QUORUM: inflight.remove(from); return tracker.recordQuorumReadSuccess(from);
            case FAIL:   inflight.remove(from); return tracker.recordReadFailure(from);
            case SLOW:   return tracker.recordSlowResponse(from);
        }
    }

    @Override
    void validate(RequestStatus status)
    {
        switch (status)
        {
            case Failed:
                Assertions.assertTrue(tracker.any(ReadShardTracker::hasFailed));
                Assertions.assertFalse(tracker.all(ReadShardTracker::hasSucceeded));
                break;

            case Success:
                Assertions.assertTrue(tracker.all(ReadShardTracker::hasSucceeded));
                Assertions.assertFalse(tracker.any(ReadShardTracker::hasFailed));
                break;

            case NoChange:
                Assertions.assertFalse(tracker.all(ReadShardTracker::hasSucceeded));
                Assertions.assertFalse(tracker.any(ReadShardTracker::hasFailed));
        }
    }
}
