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

package accord.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.MessageSink;
import accord.burn.TopologyUpdates;
import accord.impl.AbstractAsyncExecutor;
import accord.impl.DefaultTimeouts;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.local.Node;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Unseekables;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;

import accord.utils.SortedArrays.SortedArrayList;
import org.agrona.collections.Long2ObjectHashMap;

import static accord.Utils.createNode;
import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.shard;
import static accord.Utils.topologies;
import static accord.Utils.topology;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.impl.SizeOfIntersectionSorter.SUPPLIER;
import static accord.messages.RouteRequest.computeWaitForEpoch;
import static accord.topology.SelectShards.ALL;
import static accord.utils.ExtendedAssertions.assertThat;
import static accord.utils.Property.qt;

public class TopologyManagerTest
{
    private static final Node.Id ID = new Node.Id(1);

    @Test
    void rangeMovement() throws TopologyException
    {
        Topology t1 = topology(1,
                               shard(range(0, 100), idList(1, 2, 3), idSet(1, 2, 3)),
                               shard(range(100, 200), idList(3, 4, 5), idSet(3, 4, 5)));
        // 2 and 4 flip
        Topology t2 = topology(2,
                               shard(range(0, 100), idList(1, 3, 4), idSet(1, 3, 4)),
                               shard(range(100, 200), idList(2, 3, 5), idSet(2, 3, 5)));
        int[] unmoved = { 1, 3, 5 };
        int[] moved = { 2, 4 };

        TopologyManager tm = createTopologyManager();
        tm.reportTopology(t1);
        tm.reportTopology(t2);

        for (Unseekables<?> select : Arrays.asList(Ranges.ofSortedAndDeoverlapped(range(10, 20)), Ranges.ofSortedAndDeoverlapped(range(110, 120))))
        {
            Topologies t = tm.active().preciseEpochs(select, 1, 2, ALL);
            for (int i : unmoved)
                org.assertj.core.api.Assertions.assertThat(computeWaitForEpoch(new Node.Id(i), t, select)).isEqualTo(1);
            for (int i : moved)
                org.assertj.core.api.Assertions.assertThat(computeWaitForEpoch(new Node.Id(i), t, select)).isEqualTo(2);
            t = tm.active().withUnsyncedEpochs(select, 1, 2, ALL);
            for (int i : unmoved)
                org.assertj.core.api.Assertions.assertThat(computeWaitForEpoch(new Node.Id(i), t, select)).isEqualTo(1);
            for (int i : moved)
                org.assertj.core.api.Assertions.assertThat(computeWaitForEpoch(new Node.Id(i), t, select)).isEqualTo(2);
        }
    }

    @Test
    void fastPathReconfiguration()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager tm = createTopologyManager();

        Assertions.assertSame(Topology.EMPTY, tm.current());
        tm.reportTopology(topology1);
        tm.reportTopology(topology2);

        Assertions.assertTrue(tm.unsafeGetActiveEpoch(1).isQuorumReady());
        Assertions.assertFalse(tm.unsafeGetActiveEpoch(2).isQuorumReady());

        tm.onReadyToCoordinate(id(1), 2);
        Assertions.assertFalse(tm.unsafeGetActiveEpoch(2).isQuorumReady());

        tm.onReadyToCoordinate(id(2), 2);
        Assertions.assertTrue(tm.unsafeGetActiveEpoch(2).isQuorumReady());
    }

    private static TopologyManager tracker()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(2, 3)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(5, 6)));

        TopologyManager tm = createTopologyManager();
        tm.reportTopology(topology1);
        tm.reportTopology(topology2);

        return tm;
    }

    @Test
    void quorumReadyFor()
    {
        TopologyManager service = tracker();

        Assertions.assertFalse(service.unsafeGetActiveEpoch(2).isQuorumReady());
        // shards to nodes: [[1, 2, 3], [4, 5, 6]]
        // by syncing node 1/2 shard 1 has reached quorum, but not shard 2
        service.onReadyToCoordinate(id(1), 2);
        service.onReadyToCoordinate(id(2), 2);
        Assertions.assertFalse(service.unsafeGetActiveEpoch(2).isQuorumReady());
        Assertions.assertTrue(service.unsafeGetActiveEpoch(2).isQuorumReady(keys(150).toParticipants()));
        Assertions.assertFalse(service.unsafeGetActiveEpoch(2).isQuorumReady(keys(250).toParticipants()));
    }

    @Test
    void quorumReadyPastEpochs()
    {
        TopologyManager service = createTopologyManager();
        Shard[] shards = { shard(range(0, 100), idList(1, 2, 3), idSet(1, 2, 3)),
                           shard(range(100, 200), idList(3, 4, 5), idSet(3, 4, 5)) };

        service.reportTopology(topology(1, shards));
        service.reportTopology(topology(2, shards));
        service.reportTopology(topology(3, shards));

        for (int i = 1; i <= 5; i++)
            service.onReadyToCoordinate(id(i), service.epoch());

        Ranges expected = service.current().ranges().mergeTouching();
        org.assertj.core.api.Assertions.assertThat(service.unsafeQuorumReady(3)).describedAs("Unexpected sync complte for node 3").isEqualTo(expected);
        org.assertj.core.api.Assertions.assertThat(service.unsafeQuorumReady(2)).describedAs("Unexpected sync complte for node 2").isEqualTo(expected);
        org.assertj.core.api.Assertions.assertThat(service.unsafeQuorumReady(1)).describedAs("Unexpected sync complte for node 1").isEqualTo(expected);
    }

    /**
     * If a node receives sync acks for epochs it's not aware of, it should apply them when it finds out about the epoch
     */
    @Test
    void futureEpochPendingSync()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager tm = createTopologyManager();
        tm.reportTopology(topology1);

        // sync epoch 2
        tm.onReadyToCoordinate(id(2), 2);
        tm.onReadyToCoordinate(id(3), 2);

        // learn of epoch 2
        tm.reportTopology(topology2);
        Assertions.assertTrue(tm.unsafeGetActiveEpoch(1).isQuorumReady());
        Assertions.assertTrue(tm.unsafeGetActiveEpoch(2).isQuorumReady());
    }

    @Test
    void forKeys() throws TopologyException
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = createTopologyManager();

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.reportTopology(topology1);
        service.reportTopology(topology2);
        service.reportTopology(topology3);
        Assertions.assertFalse(service.unsafeGetActiveEpoch(2).isQuorumReady());

        RoutingKeys keys = keys(150).toParticipants();
        Assertions.assertEquals(topologies(topology3.select(keys), topology2.select(keys), topology1.select(keys)),
                                service.active().withUnsyncedEpochs(keys, 3, 3, ALL));

        service.onReadyToCoordinate(id(2), 2);
        service.onReadyToCoordinate(id(3), 2);
        service.onReadyToCoordinate(id(2), 3);
        service.onReadyToCoordinate(id(3), 3);
        Assertions.assertEquals(topologies(topology3.select(keys)),
                                service.active().withUnsyncedEpochs(keys, 3, 3, ALL));
    }

    @Test
    void incompleteTopologyHistory()
    {
        Topology topology5 = topology(5,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology6 = topology(6,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(5, 6)));

        TopologyManager service = createTopologyManager();
        service.reportTopology(topology5);
        service.reportTopology(topology6);

        Assertions.assertSame(topology6, service.unsafeGetActiveEpoch(6L).all());
        Assertions.assertSame(topology5, service.unsafeGetActiveEpoch(5L).all());
        for (int i=1; i<=6; i++) service.onReadyToCoordinate(id(i), 6);
        Assertions.assertTrue(service.unsafeGetActiveEpoch(5).isQuorumReady());
        try { service.active().get(4); Assertions.fail(); }
        catch (TopologyException e) {}

        service.onReadyToCoordinate(id(1), 4);
    }

    private static void markTopologySynced(TopologyManager service, long epoch)
    {
        service.unsafeGetActiveEpoch(epoch).all().nodes().forEach(id -> service.onReadyToCoordinate(id, epoch));
    }

    private static void addAndMarkSynced(TopologyManager service, Topology topology)
    {
        service.reportTopology(topology);
        markTopologySynced(service, topology.epoch());
    }

    @Test
    void truncateTopologyHistory()
    {
        Range range = range(100, 200);
        TopologyManager service = createTopologyManager();
        addAndMarkSynced(service, topology(1, shard(range, idList(1, 2, 3), idSet(1, 2))));
        addAndMarkSynced(service, topology(2, shard(range, idList(1, 2, 3), idSet(2, 3))));
        addAndMarkSynced(service, topology(3, shard(range, idList(1, 2, 3), idSet(1, 2))));
        addAndMarkSynced(service, topology(4, shard(range, idList(1, 2, 3), idSet(1, 3))));

        Assertions.assertTrue(service.active().hasEpoch(1));
        Assertions.assertTrue(service.active().hasEpoch(2));
        Assertions.assertTrue(service.active().hasEpoch(3));
        Assertions.assertTrue(service.active().hasEpoch(4));
    }

    @Test
    void truncateTopologyCantTruncateUnsyncedEpochs()
    {

    }

    @Test
    void removeRanges()
    {
        qt().withExamples(100).check(rs -> {
            long epochCounter = rs.nextInt(1, 42);
            boolean withUnchange = rs.nextBoolean();
            List<Topology> topologies = new ArrayList<>(withUnchange ? 3 : 2);
            topologies.add(topology(epochCounter++,
                                    shard(PrefixedIntHashKey.range(0, 0, 100), idList(1, 2, 3), idSet(1, 2)),
                                    shard(PrefixedIntHashKey.range(1, 0, 100), idList(1, 2, 3), idSet(1, 2))));
            if (withUnchange)
                topologies.add(topology(epochCounter++,
                                        shard(PrefixedIntHashKey.range(0, 0, 100), idList(1, 2, 3), idSet(1, 2)),
                                        shard(PrefixedIntHashKey.range(1, 0, 100), idList(1, 2, 3), idSet(1, 2))));
            topologies.add(topology(epochCounter++,
                                    shard(PrefixedIntHashKey.range(1, 0, 100), idList(1, 2, 3), idSet(1, 2))));;
            History history = new History(createTopologyManager(new ShardDistributor.EvenSplit<>(1, ignore -> new PrefixedIntHashKey.Splitter())), topologies.iterator()) {

                @Override
                protected void postTopologyUpdate(int id, Topology t) throws TopologyException
                {
                    test(t);
                }

                @Override
                protected void postEpochisQuorumReady(int id, long epoch, Node.Id node) throws TopologyException
                {
                    test(tm.active().globalForEpoch(epoch));
                }

                private void test(Topology topology) throws TopologyException
                {
                    Ranges ranges = topology.ranges();
                    for (int i = 0; i < 10; i++)
                    {
                        Unseekables<?> unseekables = TopologyUtils.select(ranges, rs);
                        long maxEpoch = topology.epoch();
                        long minEpoch = tm.minEpoch() == maxEpoch ? maxEpoch : rs.nextLong(tm.minEpoch(), maxEpoch + 1);
                        assertThat(tm.active().preciseEpochs(unseekables, minEpoch, maxEpoch, ALL))
                                .isNotEmpty()
                                .epochsBetween(minEpoch, maxEpoch)
                                .containsAll(unseekables)
                                .topology(maxEpoch, a -> a.isNotEmpty());

                        assertThat(tm.active().withUnsyncedEpochs(unseekables, minEpoch, maxEpoch, ALL))
                                .isNotEmpty()
                                .epochsBetween(minEpoch, maxEpoch, false) // older epochs are allowed
                                .containsAll(unseekables)
                                .topology(maxEpoch, a -> a.isNotEmpty());
                    }
                }
            };
            history.run(rs);
        });
    }

    /**
     * The ABA problem is a problem with registers where you set the value A, then B, then A again; when you observe you see A... which A?
     *
     * TODO (testing): we don't want to support this. Ranges should be one use - if you want to create new ranges again, use a different prefix.
     */
    @Test
    void aba() throws Exception
    {
        TopologyManager service = createTopologyManager(new ShardDistributor.EvenSplit<>(1, ignore -> new PrefixedIntHashKey.Splitter()));
        SortedArrayList<Node.Id> dc1Nodes = idList(1, 2, 3);
        Set<Node.Id> dc1Fp = idSet(1, 2);
        SortedArrayList<Node.Id> dc2Nodes = idList(4, 5, 6);
        Set<Node.Id> dc2Fp = idSet(4, 5);
        addAndMarkSynced(service, topology(1,
                shard(PrefixedIntHashKey.range(0, 0, 100), dc2Nodes, dc2Fp),
                shard(PrefixedIntHashKey.range(1, 0, 100), dc1Nodes, dc1Fp)));
        addAndMarkSynced(service, topology(2,
                shard(PrefixedIntHashKey.range(1, 0, 100), dc1Nodes, dc1Fp)));
        addAndMarkSynced(service, topology(3,
                shard(PrefixedIntHashKey.range(0, 0, 100), dc2Nodes, dc2Fp),
                shard(PrefixedIntHashKey.range(1, 0, 100), dc1Nodes, dc1Fp)));

        // prefix=0 was added in epoch=1, removed in epoch=2, and added back to epoch=3; the ABA problem
        RoutingKeys unseekables = RoutingKeys.of(PrefixedIntHashKey.forHash(0, 42));

        for (Callable<Topologies> fn : Arrays.<Callable<Topologies>>asList(() -> service.active().preciseEpochs(unseekables, 1, 3, ALL),
                                                                           () -> service.active().withUnsyncedEpochs(unseekables, 1, 3, ALL)))
        {
            assertThat(fn.call())
                    .isNotEmpty()
                    .epochsBetween(1, 3)
                    .containsAll(unseekables)
                    .topology(1, a -> a.isEmpty())
                    .topology(2, a -> a.isEmpty())
                    .topology(3, a -> a.isNotEmpty()
                                       .isRangesEqualTo(PrefixedIntHashKey.range(0, 0, 100))
                                       .isHostsEqualTo(dc2Nodes));
        }
    }

    @Test
    void fuzz()
    {
        Gen<Topology> firstTopology = AccordGens.topologys(Gens.longs().between(1, 1024)); // limit the epochs between 1-1024, so it is easier to tell the difference while in a debugger
        AbstractAsyncExecutor executor = command -> { throw new IllegalStateException("Attempted to perform async operation"); };

        qt().withExamples(20).check(rs -> {
            int[] prefixes = IntStream.generate(rs::nextInt).limit(10).toArray();
            TopologyRandomizer randomizer = new TopologyRandomizer(() -> rs, prefixes, firstTopology.next(rs), new TopologyUpdates(ignore -> executor), null, TopologyRandomizer.Listeners.NOOP);
            Iterator<Topology> next = Iterators.limit(new AbstractIterator<Topology>()
            {
                @Override
                protected Topology computeNext()
                {
                    Topology t = randomizer.updateTopology();
                    for (int attempt = 0, maxAttempt = TopologyRandomizer.UpdateType.values().length * 2; t == null && attempt < maxAttempt; attempt++)
                        t = randomizer.updateTopology();
                    return t == null ? endOfData() : t;
                }
            }, 42);
            History history = new History(createTopologyManager(), next) {

                @Override
                protected void postTopologyUpdate(int id, Topology t) throws TopologyException
                {
                    check(tm, rs);
                }

                @Override
                protected void postEpochisQuorumReady(int id, long epoch, Node.Id node) throws TopologyException
                {
                    check(tm, rs);
                }
            };
            history.run(rs);
        });
    }

    private static void check(TopologyManager service, RandomSource rand) throws TopologyException
    {
        for (int i = 0; i < 2; i++)
        {
            EpochRange range = EpochRange.from(service, rand);
            Unseekables<?> select = select(service, range, rand);

            assertThat(service.active().preciseEpochs(select, range.min, range.max, ALL))
                    .isNotEmpty()
                    .epochsBetween(range.min, range.max)
                    .containsAll(select);

            assertThat(service.active().withUnsyncedEpochs(select, range.min, range.max, ALL))
                    .isNotEmpty()
                    .epochsBetween(range.min, range.max, false) // older epochs are allowed
                    .containsAll(select);
        }
    }

    private static Unseekables<?> select(TopologyManager service, EpochRange range, RandomSource rs) throws TopologyException
    {
        long epoch = range.min == range.max ?
                     range.min :
                     rs.pickLong(range.min, range.max);
        Ranges ranges = service.active().globalForEpoch(epoch).ranges();
        return TopologyUtils.select(ranges, rs);
    }

    private static class EpochRange
    {
        final long min, max;

        private EpochRange(long min, long max)
        {
            this.min = min;
            this.max = max;
        }

        static EpochRange from(TopologyManager service, RandomSource rand)
        {
            if (service.minEpoch() == service.epoch())
                return new EpochRange(service.epoch(), service.epoch());
            long min = rand.nextLong(service.minEpoch(), service.epoch() + 1);
            long max = rand.nextLong(service.minEpoch(), service.epoch() + 1);
            if (min > max)
            {
                long tmp = max;
                max = min;
                min = tmp;
            }
            return new EpochRange(min, max);
        }

        @Override
        public String toString()
        {
            return "[" + min + ", " + max + "]";
        }
    }

    private static class History
    {
        private enum Action { OnEpochisQuorumReady, OnTopologyUpdate;}

        protected final TopologyManager tm;
        private final Iterator<Topology> next;
        private final Long2ObjectHashMap<Set<Node.Id>> pendingisQuorumReady = new Long2ObjectHashMap<>();
        private final Map<EnumMap<Action, Integer>, Gen<Action>> cache = new HashMap<>();
        private int id = 0;

        public History(TopologyManager tm, Iterator<Topology> next)
        {
            this.tm = tm;
            this.next = next;
        }

        protected void preTopologyUpdate(int id, Topology t)
        {

        }

        protected void postTopologyUpdate(int id, Topology t) throws TopologyException
        {

        }

        protected void preEpochisQuorumReady(int id, long epoch, Node.Id node)
        {

        }

        protected void postEpochisQuorumReady(int id, long epoch, Node.Id node) throws TopologyException
        {

        }

        public void run(RandomSource rs) throws TopologyException
        {
            //noinspection StatementWithEmptyBody
            while (process(rs));
        }

        private boolean process(RandomSource rs) throws TopologyException
        {
            EnumMap<Action, Integer> possibleActions = new EnumMap<>(Action.class);
            if (!pendingisQuorumReady.isEmpty())
                possibleActions.put(Action.OnEpochisQuorumReady, 10); // TODO (correctness): should the weight be based off the backlog?
            if (next.hasNext())
                possibleActions.put(Action.OnTopologyUpdate, 1);
            if (possibleActions.isEmpty())
            {
                if (id == 0)
                    throw new IllegalArgumentException("No history processed");
                return false;
            }
            int id = this.id++;
            Gen<Action> actionGen = cache.computeIfAbsent(possibleActions, Gens::pick);
            Action action = actionGen.next(rs);
            switch (action)
            {
                case OnTopologyUpdate:
                    Topology t = next.next();
                    preTopologyUpdate(id, t);
                    tm.reportTopology(t);
                    pendingisQuorumReady.put(t.epoch, new HashSet<>(t.nodes()));
                    postTopologyUpdate(id, t);
                    break;
                case OnEpochisQuorumReady:
                    long epoch = rs.pickUnorderedSet(pendingisQuorumReady.keySet());
                    Set<Node.Id> pendingNodes = pendingisQuorumReady.get(epoch);
                    Node.Id node = rs.pickUnorderedSet(pendingNodes);
                    pendingNodes.remove(node);
                    if (pendingNodes.isEmpty())
                        pendingisQuorumReady.remove(epoch);
                    preEpochisQuorumReady(id, epoch, node);
                    tm.onReadyToCoordinate(node, epoch);
                    postEpochisQuorumReady(id, epoch, node);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown action: " + action);
            }
            return true;
        }
    }

    private static TopologyManager createTopologyManager()
    {
        return createTopologyManager(null);
    }

    private static TopologyManager createTopologyManager(@Nullable ShardDistributor shardDistributor)
    {
        MockCluster.Clock time = new MockCluster.Clock(0);
        Node node = createNode(ID, Topology.EMPTY, new MessageSink.NoOpSink(), time, new TestAgent(time), shardDistributor);
        return new TopologyManager(SUPPLIER, node, ignore -> {throw new UnsupportedOperationException();}, time, new DefaultTimeouts(time, Runnable::run));
    }
}
