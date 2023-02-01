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
