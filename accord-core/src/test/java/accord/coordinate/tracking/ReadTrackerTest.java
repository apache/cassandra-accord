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

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;
import accord.impl.TopologyUtils;
import accord.local.Node.Id;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.Ranges;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;

import static accord.Utils.ids;
import static accord.Utils.topologies;
import static accord.Utils.topology;
import static accord.utils.Utils.toArray;

public class ReadTrackerTest
{
    private static final Id[] ids = toArray(ids(5), Id[]::new);
    private static final Ranges ranges = TopologyUtils.initialRanges(5, 500);
    private static final Topology topology = TopologyUtils.initialTopology(ids, ranges, 3);
        /*
        (000, 100](100, 200](200, 300](300, 400](400, 500]
        [1, 2, 3] [2, 3, 4] [3, 4, 5] [4, 5, 1] [5, 1, 2]
         */

    static class AutoReadTracker extends ReadTracker
    {
        public AutoReadTracker(Topologies topologies)
        {
            super(topologies);
        }

        @Override
        public RequestStatus trySendMore()
        {
            return super.trySendMore((inflight, id, dataReadSeekables) -> inflight.add(id), inflight);
        }
    }

    static class TestReadTracker extends ReadTracker
    {
        private final boolean quorum;
        public TestReadTracker(Topologies topologies)
        {
            this(topologies, false);
        }

        public TestReadTracker(Topologies topologies, boolean quorum)
        {
            super(topologies, quorum ? DataConsistencyLevel.QUORUM : DataConsistencyLevel.INVALID);
            this.quorum = quorum;
        }

        @Override
        public RequestStatus trySendMore()
        {
            return RequestStatus.NoChange;
        }

        @Override
        protected RequestStatus recordReadSuccess(Id from)
        {
            if (quorum)
                return recordQuorumReadSuccess(from);
            else
                return super.recordReadSuccess(from);
        }
    }

    private static void assertResponseState(ReadTracker responses,
                                            boolean complete,
                                            boolean failed)
    {
        Assertions.assertEquals(complete, responses.hasData());
        Assertions.assertEquals(failed, responses.hasFailed());
    }

    @Test
    void singleShard()
    {
        Topology subTopology = topology(topology.get(0));
        ReadTracker tracker = new ReadTracker(topologies(subTopology));

        tracker.recordInFlightRead(ids[0]);
        assertResponseState(tracker, false, false);

        tracker.recordReadSuccess(ids[0]);
        assertResponseState(tracker, true, false);
    }

    @Test
    void singleShardRetry()
    {
        Topology subTopology = topology(topology.get(0));
        ReadTracker tracker = new AutoReadTracker(topologies(subTopology));

        tracker.recordInFlightRead(ids[0]);
        assertResponseState(tracker, false, false);

        tracker.recordReadFailure(ids[0]);
        assertResponseState(tracker, false, false);

        tracker.recordInFlightRead(ids[1]);
        assertResponseState(tracker, false, false);

        tracker.recordReadSuccess(ids[1]);
        assertResponseState(tracker, true, false);
    }

    @Test
    void singleShardFailure()
    {
        Topology subTopology = topology(topology.get(0));
        ReadTracker tracker = new TestReadTracker(topologies(subTopology));

        tracker.recordInFlightRead(ids[0]);
        tracker.recordReadFailure(ids[0]);
        assertResponseState(tracker, false, false);

        tracker.recordInFlightRead(ids[1]);
        tracker.recordReadFailure(ids[1]);
        assertResponseState(tracker, false, false);

        tracker.recordInFlightRead(ids[2]);
        tracker.recordReadFailure(ids[2]);
        assertResponseState(tracker, false, true);
    }

    @Test
    void multiShardSuccess()
    {
        Topology subTopology = new Topology(1, new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        ReadTracker responses = new AutoReadTracker(topologies(subTopology));
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        responses.recordInFlightRead(ids[2]);
        responses.recordReadSuccess(ids[2]);
        assertResponseState(responses, true, false);
    }

    @Test
    void multiShardRetryAndReadSet()
    {
        Topology subTopology = new Topology(1, new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        ReadTracker responses = new TestReadTracker(topologies(subTopology));
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        assertContacts(Sets.newHashSet(ids[2]), responses);

        assertResponseState(responses, false, false);

        responses.recordReadFailure(ids[2]);
        assertResponseState(responses, false, false);

        assertContacts(Sets.newHashSet(ids[1], ids[3]), responses);
        assertResponseState(responses, false, false);

        responses.recordReadFailure(ids[1]);
        assertContacts(Sets.newHashSet(ids[0]), responses);

        responses.recordReadSuccess(ids[3]);
        assertResponseState(responses, false, false);
        try
        {
            responses.trySendMore((i,j,k)->{}, null);
            Assertions.fail();
        }
        catch (IllegalStateException t)
        {
        }

        responses.recordReadSuccess(ids[0]);
        assertResponseState(responses, true, false);
    }

    @Test
    void multiShardQuorumAndDigestFailure()
    {
        multishardQuorumAndDigest(true);
    }

    @Test
    void multiShardQuorumAndDigestSuccess()
    {
        multishardQuorumAndDigest(false);
    }

    private void multishardQuorumAndDigest(boolean fail)
    {
        Id[] ids = toArray(ids(8), Id[]::new);
        Ranges ranges = TopologyUtils.initialRanges(8, 500);
        Topology topology = TopologyUtils.initialTopology(ids, ranges, 3);

        Topology subTopology = new Topology(1, new Shard[]{topology.get(0), topology.get(1), topology.get(5)});
        ReadTracker responses = new TestReadTracker(topologies(subTopology), true);

        assertInitialContacts(Sets.newHashSet(ids[1], ids[2], ids[6], ids[7]), responses);

        assertResponseState(responses, false, false);

        responses.recordReadFailure(ids[1]);
        assertResponseState(responses, false, false);

        assertContacts(Sets.newHashSet(ids[0], ids[3]), responses);
        assertResponseState(responses, false, false);

        responses.recordReadFailure(ids[7]);
        assertContacts(Sets.newHashSet(ids[5]), responses);

        responses.recordReadSuccess(ids[2]);
        assertResponseState(responses, false, false);
        try
        {
            responses.trySendMore((i,j,k)->{}, null);
            Assertions.fail();
        }
        catch (IllegalStateException t)
        {
        }

        responses.recordReadSuccess(ids[0]);
        assertResponseState(responses, false, false);
        responses.recordReadSuccess(ids[5]);
        assertResponseState(responses, false, false);
        responses.recordReadSuccess(ids[6]);
        assertResponseState(responses, false, false);
        if (fail)
        {
            responses.recordReadFailure(ids[3]);
            assertResponseState(responses, false, true);
        }
        else
        {
            responses.recordReadSuccess(ids[3]);
            assertResponseState(responses, true, false);
        }
    }


    private static void assertContacts(Set<Id> expect, ReadTracker tracker)
    {
        Set<Id> actual = new HashSet<>();
        tracker.trySendMore((set, to, dataKeys) -> set.add(to), actual);
        Assertions.assertEquals(expect, actual);
    }

    private static void assertInitialContacts(Set<Id> expect, ReadTracker tracker)
    {
        ListMultimap<Shard, RoutingKey> shardToDataReadKeys = ArrayListMultimap.create();
        tracker.topologies.get(0).shards().forEach(s -> shardToDataReadKeys.put(s, s.range.start()));
        Set<Id> actual = new HashSet<>();
        tracker.trySendMore((set, to, dataKeys) -> set.add(to), actual, shardToDataReadKeys);
        Assertions.assertEquals(expect, actual);
    }
}
