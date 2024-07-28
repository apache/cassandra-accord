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

package accord.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import accord.api.ConfigurationService.EpochReady;
import accord.primitives.Ranges;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import com.google.common.collect.ImmutableSet;

import accord.api.ConfigurationService;
import accord.impl.AbstractConfigurationService.Minimal.EpochHistory;
import accord.local.Node.Id;
import accord.primitives.Range;
import accord.topology.Shard;
import accord.topology.Topology;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AbstractConfigurationServiceTest
{
    // C* uses this, so we can't use the jupiter assertions
    public static class TestListener implements ConfigurationService.Listener
    {
        private final ConfigurationService parent;
        private final boolean ackTopologies;
        final Map<Long, Topology> topologies = new HashMap<>();
        final Map<Long, Set<Id>> syncCompletes = new HashMap<>();
        final Set<Long> truncates = new HashSet<>();

        public TestListener(ConfigurationService parent, boolean ackTopologies)
        {
            this.parent = parent;
            this.ackTopologies = ackTopologies;
        }

        @Override
        public AsyncResult<Void> onTopologyUpdate(Topology topology, boolean startSync)
        {
            if (topologies.put(topology.epoch(), topology) != null)
                Assertions.fail("Received topology twice for epoch " + topology.epoch());
            if (ackTopologies)
                parent.acknowledgeEpoch(EpochReady.done(topology.epoch()), true);
            return AsyncResults.success(null);
        }

        @Override
        public void onRemoteSyncComplete(Id node, long epoch)
        {
            Set<Id> synced = syncCompletes.computeIfAbsent(epoch, e -> new HashSet<>());
            if (!synced.add(node))
                throw new AssertionError(String.format("Recieved multiple syncs for epoch %s from %s", epoch, node));
        }

        @Override
        public void truncateTopologyUntil(long epoch)
        {
            if (!truncates.add(epoch))
                throw new AssertionError(String.format("Recieved multiple truncates for epoch", epoch));
        }

        @Override
        public void onEpochClosed(Ranges ranges, long epoch)
        {
        }

        @Override
        public void onEpochRedundant(Ranges ranges, long epoch)
        {
        }

        public void assertNoTruncates()
        {
            assert truncates.isEmpty() : "truncates is not empty";
        }

        private static void assertEquals(Object expected, Object actual)
        {
            if (!Objects.equals(expected, actual))
                throw new AssertionError(String.format("Expected %s, but was %s", expected, actual));
        }

        public void assertTruncates(Long... epochs)
        {
            assertEquals(ImmutableSet.copyOf(epochs), truncates);
        }

        public void assertSyncsFor(Long... epochs)
        {
            assertEquals(ImmutableSet.copyOf(epochs), syncCompletes.keySet());
        }

        public void assertSyncsForEpoch(long epoch, Id... nodes)
        {
            assertEquals(ImmutableSet.copyOf(nodes), syncCompletes.get(epoch));
        }

        public void assertTopologiesFor(Long... epochs)
        {
            assertEquals(ImmutableSet.copyOf(epochs), topologies.keySet());
        }

        public void assertTopologyForEpoch(long epoch, Topology topology)
        {
            assertEquals(topology, topologies.get(epoch));
        }
    }

    private static class TestableConfigurationService extends AbstractConfigurationService.Minimal
    {
        final Set<Long> syncStarted = new HashSet<>();
        final Set<Long> epochsFetched = new HashSet<>();

        public TestableConfigurationService(Id node)
        {
            super(node);
        }

        @Override
        protected void fetchTopologyInternal(long epoch)
        {
            epochsFetched.add(epoch);
        }

        @Override
        protected void localSyncComplete(Topology topology, boolean startSync)
        {
            if (!syncStarted.add(topology.epoch()))
                Assertions.fail("Sync started multiple times for " + topology.epoch());
        }

        @Override
        protected void topologyUpdatePostListenerNotify(Topology topology)
        {
            acknowledgeEpoch(EpochReady.done(topology.epoch()), true);
        }

        @Override
        public void reportEpochClosed(Ranges ranges, long epoch)
        {
        }

        @Override
        public void reportEpochRedundant(Ranges ranges, long epoch)
        {
        }
    }

    private static final Id ID1 = new Id(1);
    private static final Id ID2 = new Id(2);
    private static final Id ID3 = new Id(3);
    private static final SortedArrayList<Id> NODES = new SortedArrayList<>(new Id[] { ID1, ID2, ID3 });
    private static final Range RANGE = IntKey.range(0, 100);

    private static Shard shard(Range range, SortedArrayList<Id> nodes, Set<Id> fastPath)
    {
        return new Shard(range, nodes, fastPath);
    }

    private static Topology topology(long epoch, Range range, SortedArrayList<Id> nodes, Set<Id> fastPath)
    {
        return new Topology(epoch, shard(range, nodes, fastPath));
    }

    private static Topology topology(long epoch, Id... fastPath)
    {
        return topology(epoch, RANGE, NODES, ImmutableSet.copyOf(fastPath));
    }

    private static Topology topology(long epoch, int... fastPath)
    {
        Set<Id> fpSet = Arrays.stream(fastPath).mapToObj(Id::new).collect(Collectors.toSet());
        return topology(epoch, RANGE, NODES, fpSet);
    }

    private static final Topology TOPOLOGY1 = topology(1, 1, 2, 3);
    private static final Topology TOPOLOGY2 = topology(2, 1, 2);
    private static final Topology TOPOLOGY3 = topology(3, 1, 3);
    private static final Topology TOPOLOGY4 = topology(4, 2, 3);

    @Test
    public void getTopologyTest()
    {
        TestableConfigurationService service = new TestableConfigurationService(ID1);
        TestListener listener = new TestListener(service, false);
        service.registerListener(listener);
        service.reportTopology(TOPOLOGY1);
        service.reportTopology(TOPOLOGY2);
        service.reportTopology(TOPOLOGY3);
        service.reportTopology(TOPOLOGY4);

        listener.assertNoTruncates();
        listener.assertTopologiesFor(1L, 2L, 3L, 4L);
        Assertions.assertSame(TOPOLOGY1, service.getTopologyForEpoch(1));
        Assertions.assertSame(TOPOLOGY2, service.getTopologyForEpoch(2));
        Assertions.assertSame(TOPOLOGY3, service.getTopologyForEpoch(3));
        Assertions.assertSame(TOPOLOGY4, service.getTopologyForEpoch(4));
    }

    /**
     * check everything works properly if we start loading after epoch 1 has
     * been removed
     */
    @Test
    public void loadAfterTruncate()
    {
        TestableConfigurationService service = new TestableConfigurationService(ID1);
        TestListener listener = new TestListener(service, false);
        service.registerListener(listener);
        service.reportTopology(TOPOLOGY3);
        service.reportTopology(TOPOLOGY4);

        listener.assertNoTruncates();
        listener.assertTopologiesFor(3L, 4L);
        Assertions.assertSame(TOPOLOGY3, service.getTopologyForEpoch(3));
        Assertions.assertSame(TOPOLOGY4, service.getTopologyForEpoch(4));
    }

    /**
     * If we receive topology epochs out of order for some reason, we should
     * reorder with callbacks
     */
    @Test
    public void awaitOutOfOrderTopologies()
    {
        TestableConfigurationService service = new TestableConfigurationService(ID1);

        TestListener listener = new TestListener(service, false);
        service.registerListener(listener);

        service.reportTopology(TOPOLOGY1);
        service.reportTopology(TOPOLOGY3);
        listener.assertTopologiesFor(1L);
        Assertions.assertEquals(ImmutableSet.of(2L), service.epochsFetched);

        service.reportTopology(TOPOLOGY2);
        listener.assertTopologiesFor(1L, 2L, 3L);

    }

    private static void assertHistoryEpochs(EpochHistory history, long... expected)
    {
        Assertions.assertEquals(history.size(), expected.length);
        if (expected.length == 0)
            return;

        Assertions.assertEquals(expected[0], history.minEpoch());
        Assertions.assertEquals(expected[expected.length - 1], history.maxEpoch());

        for (int i=0; i<expected.length; i++)
            Assertions.assertEquals(expected[i], history.atIndex(i).epoch());
    }

    @Test
    public void epochHistoryAppend()
    {
        EpochHistory history = new EpochHistory();
        Assertions.assertEquals(0, history.size());

        history.getOrCreate(5);
        assertHistoryEpochs(history, 5);

        history.getOrCreate(6);
        assertHistoryEpochs(history, 5, 6);

        history.getOrCreate(8);
        assertHistoryEpochs(history, 5, 6, 7, 8);
    }

    @Test
    public void epochHistoryPrepend()
    {
        EpochHistory history = new EpochHistory();
        Assertions.assertEquals(0, history.size());

        history.getOrCreate(5);
        history.getOrCreate(6);
        assertHistoryEpochs(history, 5, 6);

        history.getOrCreate(3);
        assertHistoryEpochs(history, 3, 4, 5, 6);
    }

    @Test
    public void epochHistoryTruncate()
    {
        EpochHistory history = new EpochHistory();
        Assertions.assertEquals(0, history.size());

        history.getOrCreate(1);
        history.getOrCreate(2);
        history.getOrCreate(3);
        history.getOrCreate(4);
        history.getOrCreate(5);
        history.getOrCreate(6);

        assertHistoryEpochs(history, 1, 2, 3, 4, 5, 6);

        history.truncateUntil(4);
        assertHistoryEpochs(history, 4, 5, 6);

        history.getOrCreate(7);
        assertHistoryEpochs(history, 4, 5, 6, 7);
    }
}
