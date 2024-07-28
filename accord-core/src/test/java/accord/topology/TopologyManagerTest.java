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

import accord.burn.TopologyUpdates;
import accord.impl.PrefixedIntHashKey;
import accord.impl.TestAgent;
import accord.local.AgentExecutor;
import accord.primitives.Ranges;
import accord.primitives.Unseekables;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import accord.utils.SortedArrays.SortedArrayList;
import org.agrona.collections.Long2ObjectHashMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.RoutingKeys;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.Utils.idSet;
import static accord.Utils.shard;
import static accord.Utils.topologies;
import static accord.Utils.topology;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.impl.SizeOfIntersectionSorter.SUPPLIER;
import static accord.messages.TxnRequest.computeWaitForEpoch;
import static accord.utils.ExtendedAssertions.assertThat;
import static accord.utils.Property.qt;

public class TopologyManagerTest
{
    private static final Node.Id ID = new Node.Id(1);

    @Test
    void rangeMovement()
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
        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(t1, () -> null);
        service.onTopologyUpdate(t2, () -> null);

        for (Unseekables<?> select : Arrays.asList(Ranges.ofSortedAndDeoverlapped(range(10, 20)), Ranges.ofSortedAndDeoverlapped(range(110, 120))))
        {
            Topologies t = service.preciseEpochs(select, 1, 2);
            for (int i : unmoved)
                org.assertj.core.api.Assertions.assertThat(computeWaitForEpoch(new Node.Id(i), t, select)).isEqualTo(1);
            for (int i : moved)
                org.assertj.core.api.Assertions.assertThat(computeWaitForEpoch(new Node.Id(i), t, select)).isEqualTo(2);
            t = service.withUnsyncedEpochs(select, 1, 2);
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

        TopologyManager service = new TopologyManager(SUPPLIER, ID);

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1, () -> null);
        service.onTopologyUpdate(topology2, () -> null);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        service.onEpochSyncComplete(id(1), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        service.onEpochSyncComplete(id(2), 2);
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
    }

    private static TopologyManager tracker()
    {
        Topology topology1 = topology(1,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(1, 2)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));
        Topology topology2 = topology(2,
                                      shard(range(100, 200), idList(1, 2, 3), idSet(3, 4)),
                                      shard(range(200, 300), idList(4, 5, 6), idSet(4, 5)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1, () -> null);
        service.onTopologyUpdate(topology2, () -> null);

        return service;
    }

    @Test
    void syncCompleteFor()
    {
        TopologyManager service = tracker();

        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        service.onEpochSyncComplete(id(1), 2);
        service.onEpochSyncComplete(id(2), 2);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncCompleteFor(keys(150).toParticipants()));
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncCompleteFor(keys(250).toParticipants()));
    }

    /**
     * Epochs should only report being synced if every preceding epoch is also reporting synced
     */
    @Test
    void existingEpochPendingSync()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(2, 3)));
        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(1, 2)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1, () -> null);
        service.onTopologyUpdate(topology2, () -> null);
        service.onTopologyUpdate(topology3, () -> null);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(3).syncComplete());

        // sync epoch 3
        service.onEpochSyncComplete(id(1), 3);
        service.onEpochSyncComplete(id(2), 3);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertFalse(service.getEpochStateUnsafe(3).syncComplete());

        // sync epoch 2
        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);

        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(3).syncComplete());
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
//        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(3, 4)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology1, () -> null);

        // sync epoch 2
        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);

        // learn of epoch 2
        service.onTopologyUpdate(topology2, () -> null);
        Assertions.assertTrue(service.getEpochStateUnsafe(1).syncComplete());
        Assertions.assertTrue(service.getEpochStateUnsafe(2).syncComplete());
//        Assertions.assertTrue(service.getEpochStateUnsafe(3).syncComplete());
    }

    @Test
    void forKeys()
    {
        Range range = range(100, 200);
        Topology topology1 = topology(1, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology2 = topology(2, shard(range, idList(1, 2, 3), idSet(1, 2)));
        Topology topology3 = topology(3, shard(range, idList(1, 2, 3), idSet(2, 3)));

        TopologyManager service = new TopologyManager(SUPPLIER, ID);

        Assertions.assertSame(Topology.EMPTY, service.current());
        service.onTopologyUpdate(topology1, () -> null);
        service.onTopologyUpdate(topology2, () -> null);
        service.onTopologyUpdate(topology3, () -> null);
        Assertions.assertFalse(service.getEpochStateUnsafe(2).syncComplete());

        RoutingKeys keys = keys(150).toParticipants();
        Assertions.assertEquals(topologies(topology3.forSelection(keys), topology2.forSelection(keys), topology1.forSelection(keys)),
                                service.withUnsyncedEpochs(keys, 3, 3));

        service.onEpochSyncComplete(id(2), 2);
        service.onEpochSyncComplete(id(3), 2);
        service.onEpochSyncComplete(id(2), 3);
        service.onEpochSyncComplete(id(3), 3);
        Assertions.assertEquals(topologies(topology3.forSelection(keys)),
                                service.withUnsyncedEpochs(keys, 3, 3));
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

        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        service.onTopologyUpdate(topology5, () -> null);
        service.onTopologyUpdate(topology6, () -> null);

        Assertions.assertSame(topology6, service.getEpochStateUnsafe(6).global());
        Assertions.assertSame(topology5, service.getEpochStateUnsafe(5).global());
        for (int i=1; i<=6; i++) service.onEpochSyncComplete(id(i), 6);
        Assertions.assertTrue(service.getEpochStateUnsafe(5).syncComplete());
        Assertions.assertNull(service.getEpochStateUnsafe(4));

        service.onEpochSyncComplete(id(1), 4);
    }

    private static void markTopologySynced(TopologyManager service, long epoch)
    {
        service.getEpochStateUnsafe(epoch).global().nodes().forEach(id -> service.onEpochSyncComplete(id, epoch));
    }

    private static void addAndMarkSynced(TopologyManager service, Topology topology)
    {
        service.onTopologyUpdate(topology, () -> null);
        markTopologySynced(service, topology.epoch());
    }

    @Test
    void truncateTopologyHistory()
    {
        Range range = range(100, 200);
        TopologyManager service = new TopologyManager(SUPPLIER, ID);
        addAndMarkSynced(service, topology(1, shard(range, idList(1, 2, 3), idSet(1, 2))));
        addAndMarkSynced(service, topology(2, shard(range, idList(1, 2, 3), idSet(2, 3))));
        addAndMarkSynced(service, topology(3, shard(range, idList(1, 2, 3), idSet(1, 2))));
        addAndMarkSynced(service, topology(4, shard(range, idList(1, 2, 3), idSet(1, 3))));

        Assertions.assertTrue(service.hasEpoch(1));
        Assertions.assertTrue(service.hasEpoch(2));
        Assertions.assertTrue(service.hasEpoch(3));
        Assertions.assertTrue(service.hasEpoch(4));

        service.truncateTopologyUntil(3);
        Assertions.assertFalse(service.hasEpoch(1));
        Assertions.assertFalse(service.hasEpoch(2));
        Assertions.assertTrue(service.hasEpoch(3));
        Assertions.assertTrue(service.hasEpoch(4));

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
            History history = new History(new TopologyManager(SUPPLIER, ID), topologies.iterator()) {

                @Override
                protected void postTopologyUpdate(int id, Topology t)
                {
                    test(t);
                }

                @Override
                protected void postEpochSyncComplete(int id, long epoch, Node.Id node)
                {
                    test(tm.globalForEpoch(epoch));
                }

                private void test(Topology topology)
                {
                    Ranges ranges = topology.ranges();
                    for (int i = 0; i < 10; i++)
                    {
                        Unseekables<?> unseekables = TopologyUtils.select(ranges, rs);
                        long maxEpoch = topology.epoch();
                        long minEpoch = tm.minEpoch() == maxEpoch ? maxEpoch : rs.nextLong(tm.minEpoch(), maxEpoch + 1);
                        assertThat(tm.preciseEpochs(unseekables, minEpoch, maxEpoch))
                                .isNotEmpty()
                                .epochsBetween(minEpoch, maxEpoch)
                                .containsAll(unseekables)
                                .topology(maxEpoch, a -> a.isNotEmpty());

                        assertThat(tm.withUnsyncedEpochs(unseekables, minEpoch, maxEpoch))
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
     */
    @Test
    void aba()
    {
        TopologyManager service = new TopologyManager(SUPPLIER, ID);
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

        for (Supplier<Topologies> fn : Arrays.<Supplier<Topologies>>asList(() -> service.preciseEpochs(unseekables, 1, 3),
                                                                           () -> service.withUnsyncedEpochs(unseekables, 1, 3)))
        {
            assertThat(fn.get())
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
        AgentExecutor executor = Mockito.mock(AgentExecutor.class, Mockito.withSettings().defaultAnswer(ignore -> { throw new IllegalStateException("Attempted to perform async operation"); }));
        Mockito.doReturn(new TestAgent.RethrowAgent()).when(executor).agent();
        qt().withExamples(20).check(rs -> {
            TopologyRandomizer randomizer = new TopologyRandomizer(() -> rs, firstTopology.next(rs), new TopologyUpdates(ignore -> executor), null, TopologyRandomizer.Listeners.NOOP);
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
            History history = new History(new TopologyManager(SUPPLIER, ID), next) {

                @Override
                protected void postTopologyUpdate(int id, Topology t)
                {
                    check(tm, rs);
                }

                @Override
                protected void postEpochSyncComplete(int id, long epoch, Node.Id node)
                {
                    check(tm, rs);
                }
            };
            history.run(rs);
        });
    }

    private static void check(TopologyManager service, RandomSource rand)
    {
        for (int i = 0; i < 2; i++)
        {
            EpochRange range = EpochRange.from(service, rand);
            Unseekables<?> select = select(service, range, rand);

            assertThat(service.preciseEpochs(select, range.min, range.max))
                    .isNotEmpty()
                    .epochsBetween(range.min, range.max)
                    .containsAll(select);

            assertThat(service.withUnsyncedEpochs(select, range.min, range.max))
                    .isNotEmpty()
                    .epochsBetween(range.min, range.max, false) // older epochs are allowed
                    .containsAll(select);
        }
    }

    private static Unseekables<?> select(TopologyManager service, EpochRange range, RandomSource rs)
    {
        long epoch = range.min == range.max ?
                     range.min :
                     rs.pickLong(range.min, range.max);
        Ranges ranges = service.globalForEpoch(epoch).ranges();
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
        private enum Action { OnEpochSyncComplete, OnTopologyUpdate;}

        protected final TopologyManager tm;
        private final Iterator<Topology> next;
        private final Long2ObjectHashMap<Set<Node.Id>> pendingSyncComplete = new Long2ObjectHashMap<>();
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

        protected void postTopologyUpdate(int id, Topology t)
        {

        }

        protected void preEpochSyncComplete(int id, long epoch, Node.Id node)
        {

        }

        protected void postEpochSyncComplete(int id, long epoch, Node.Id node)
        {

        }

        public void run(RandomSource rs)
        {
            //noinspection StatementWithEmptyBody
            while (process(rs));
        }

        private boolean process(RandomSource rs)
        {
            EnumMap<Action, Integer> possibleActions = new EnumMap<>(Action.class);
            if (!pendingSyncComplete.isEmpty())
                possibleActions.put(Action.OnEpochSyncComplete, 10); // TODO (correctness): should the weight be based off the backlog?
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
                    tm.onTopologyUpdate(t, () -> null);
                    pendingSyncComplete.put(t.epoch, new HashSet<>(t.nodes()));
                    postTopologyUpdate(id, t);
                    break;
                case OnEpochSyncComplete:
                    long epoch = rs.pickUnorderedSet(pendingSyncComplete.keySet());
                    Set<Node.Id> pendingNodes = pendingSyncComplete.get(epoch);
                    Node.Id node = rs.pickUnorderedSet(pendingNodes);
                    pendingNodes.remove(node);
                    if (pendingNodes.isEmpty())
                        pendingSyncComplete.remove(epoch);
                    preEpochSyncComplete(id, epoch, node);
                    tm.onEpochSyncComplete(node, epoch);
                    postEpochSyncComplete(id, epoch, node);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown action: " + action);
            }
            return true;
        }
    }
}
