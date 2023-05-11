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
import accord.coordinate.tracking.InvalidationTracker.InvalidationShardTracker;
import accord.local.Node;
import accord.topology.Topologies;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;

public class InvalidationTrackerReconciler extends TrackerReconciler<InvalidationShardTracker, InvalidationTracker, InvalidationTrackerReconciler.Rsp>
{
    enum Rsp { PROMISED_FAST, NOT_PROMISED_FAST, PROMISED_SLOW, NOT_PROMISED_SLOW, FAIL }

    InvalidationTrackerReconciler(RandomSource random, Topologies topologies)
    {
        this(random, new InvalidationTracker(topologies));
    }

    private InvalidationTrackerReconciler(RandomSource random, InvalidationTracker tracker)
    {
        super(random, Rsp.class, tracker, new ArrayList<>(tracker.nodes()));
    }

    @Override
    RequestStatus invoke(Rsp event, InvalidationTracker tracker, Node.Id from)
    {
        switch (event)
        {
            default: throw new AssertionError();
            case PROMISED_FAST: inflight.remove(from); return tracker.recordSuccess(from, true, false, true);
            case PROMISED_SLOW: inflight.remove(from); return tracker.recordSuccess(from, true, false, false);
            case NOT_PROMISED_FAST: inflight.remove(from); return tracker.recordSuccess(from, false, false, true);
            case NOT_PROMISED_SLOW: inflight.remove(from); return tracker.recordSuccess(from, false, false, false);
            case FAIL: inflight.remove(from); return tracker.recordFailure(from);
        }
    }

    @Override
    void validate(RequestStatus status)
    {
        switch (status)
        {
            case Failed:
                Assertions.assertTrue(tracker.all(InvalidationShardTracker::isFinal));
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
                Assertions.assertFalse(tracker.all(InvalidationShardTracker::isFinal));
        }
    }
}
