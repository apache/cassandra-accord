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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.junit.jupiter.api.Assertions;

import accord.api.RoutingKey;
import accord.coordinate.tracking.ReadTracker.ReadShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.DataConsistencyLevel;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.RandomSource;

public class ReadTrackerReconciler extends TrackerReconciler<ReadShardTracker, ReadTracker, ReadTrackerReconciler.Rsp>
{
    enum Rsp { DATA, QUORUM, SLOW, FAIL }

    static class InFlightCapturingReadTracker extends ReadTracker
    {
        final List<Id> inflight = new ArrayList<>();
        final boolean quorumRead;
        public InFlightCapturingReadTracker(boolean quorumRead, Topologies topologies)
        {
            super(topologies, quorumRead ? DataConsistencyLevel.QUORUM : DataConsistencyLevel.INVALID);
            this.quorumRead = quorumRead;
        }

        @Override
        protected RequestStatus trySendMore()
        {
            if (quorumRead)
            {
                ListMultimap<Shard, RoutingKey> shardToDataReadKeys = ArrayListMultimap.create();
                topologies.get(0).shards().forEach(s -> shardToDataReadKeys.put(s, s.range.start()));
                return super.trySendMore((list, to, dataKeys) -> list.add(to), inflight, shardToDataReadKeys);
            }
            else
            {
                return super.trySendMore((list, to, dataKeys) -> list.add(to), inflight);
            }
        }

        @Override
        protected RequestStatus recordReadSuccess(Id from)
        {
            if (quorumRead)
                return recordQuorumReadSuccess(from);
            else
                return super.recordReadSuccess(from);
        }
    }

    ReadTrackerReconciler(RandomSource random, Topologies topologies)
    {
        this(random, new InFlightCapturingReadTracker(random.nextBoolean(), topologies));
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
                if (!tracker.any(ReadShardTracker::hasFailed))
                    System.out.println("woops");
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
