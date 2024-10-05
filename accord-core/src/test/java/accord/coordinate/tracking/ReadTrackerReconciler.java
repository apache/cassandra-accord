/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.coordinate.tracking;

import accord.utils.RandomSource;
import accord.coordinate.tracking.ReadTracker.ReadShardTracker;
import accord.local.Node;
import accord.topology.Topologies;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.List;

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

    ReadTrackerReconciler(RandomSource random, Topologies topologies)
    {
        this(random, new InFlightCapturingReadTracker(topologies));
    }

    private ReadTrackerReconciler(RandomSource random, InFlightCapturingReadTracker tracker)
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
            case FAIL:   inflight.remove(from); return tracker.recordFailure(from);
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
