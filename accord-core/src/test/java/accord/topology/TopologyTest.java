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

import accord.Utils;
import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.TopologyFactory;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Unseekables;
import accord.utils.Gens;
import accord.utils.RandomSource;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Consumer;

import static accord.topology.TopologyUtils.routingKey;
import static accord.topology.TopologyUtils.routingKeyOutsideRange;
import static accord.topology.TopologyUtils.withEpoch;
import static accord.utils.AccordGens.topologys;
import static org.assertj.core.api.Assertions.assertThat;
import static accord.utils.ExtendedAssertions.assertThat;
import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TopologyTest
{
    private static void assertRangeForKey(Topology topology, int key, int start, int end)
    {
        Key expectedKey = IntKey.key(key);
        Shard shard = topology.forKey(IntKey.routing(key));
        Range expectedRange = IntKey.range(start, end);
        Assertions.assertTrue(expectedRange.contains(expectedKey));
        Assertions.assertTrue(shard.range.contains(expectedKey));
        Assertions.assertEquals(expectedRange, shard.range);

        Topology subTopology = topology.forSelection(Keys.of(expectedKey).toParticipants());
        shard = Iterables.getOnlyElement(subTopology.shards());
        Assertions.assertTrue(shard.range.contains(expectedKey));
        Assertions.assertEquals(expectedRange, shard.range);
    }

    private static Topology topology(List<Node.Id> ids, int rf, Range... ranges)
    {
        TopologyFactory topologyFactory = new TopologyFactory(rf, ranges);
        return topologyFactory.toTopology(ids);
    }

    private static Topology topology(int numNodes, int rf, Range... ranges)
    {
        return topology(Utils.ids(numNodes), rf, ranges);
    }

    private static Topology topology(Range... ranges)
    {
        return topology(1, 1, ranges);
    }

    private static Range r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    private static void assertNoRangeForKey(Topology topology, int key)
    {
        try
        {
            topology.forKey(IntKey.routing(key));
            Assertions.fail("Expected exception");
        }
        catch (IllegalArgumentException e)
        {
            // noop
        }
    }

    @Test
    void forKeyTest()
    {
        Topology topology = topology(r(0, 100), r(100, 200), r(300, 400));
        assertNoRangeForKey(topology, -50);
        assertRangeForKey(topology, 50, 0, 100);
        assertRangeForKey(topology, 100, 0, 100);
        assertNoRangeForKey(topology, 250);
        assertRangeForKey(topology, 350, 300, 400);
    }

    @Test
    void basic()
    {
        qt().forAll(topologys(), Gens.random()).check((topology, rs) -> {
            assertThat(topology)
                    .isNotSubset()
                    .isEqualTo(withEpoch(topology, topology.epoch))
                    .hasSameHashCodeAs(withEpoch(topology, topology.epoch));

            checkTopology(topology, rs);

            for (int i = 0; i < topology.size(); i++)
            {
                Shard shard = topology.get(i);
                for (boolean withNodes : Arrays.asList(true, false))
                {
                    Topology subset = withNodes ?
                                      topology.forSubset(new int[] {i}, topology.nodes()) : // TODO (correctness): should this drop non-overlapping nodes, or reject?
                                      topology.forSubset(new int[] {i});
                    Topology trimmed = subset.trim();

                    assertThat(subset)
                            .isSubset()
                            .isEqualTo(trimmed)
                            .hasSameHashCodeAs(trimmed)
                            // this is slightly redundant as trimmed model should catch this... it is here in case trim breaks
                            .hasSize(1)
                            .isShardsEqualTo(shard)
                            .isHostsEqualTo(shard.nodes)
                            .isRangesEqualTo(shard.range);

                    checkTopology(subset, rs);
                    {
                        List<Shard> forEachShard = new ArrayList<>(1);
                        subset.forEach(s -> forEachShard.add(s)); // cant do forEachShard::add due ambiguous signature (multiple matches in topology)
                        assertThat(forEachShard).isEqualTo(Collections.singletonList(shard));
                    }

                    Consumer<Unseekables<?>> foldl = unseekables -> assertThat(subset.foldl(unseekables, (s, accum, indexed) -> accum + System.identityHashCode(s), 0))
                            .isEqualTo(trimmed.foldl(unseekables, (s, accum, indexed) -> accum + System.identityHashCode(s), 0))
                            .isEqualTo(System.identityHashCode(shard));
                    Consumer<Unseekables<?>> visitNodeForKeysOnceOrMore = unseekables -> {
                        List<Node.Id> actual = new ArrayList<>(shard.nodes.size());
                        subset.visitNodeForKeysOnceOrMore(unseekables, actual::add);
                        assertThat(actual).isEqualTo(shard.nodes);
                    };
                    for (Range range : subset.ranges())
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            RoutingKey key = routingKey(range, rs);
                            assertThat(subset.forKey(key)).isEqualTo(shard);

                            RoutingKeys unseekables = RoutingKeys.of(key);
                            foldl.accept(unseekables);
                            visitNodeForKeysOnceOrMore.accept(unseekables);
                        }
                        Ranges unseekables = Ranges.single(range);
                        foldl.accept(unseekables);
                        visitNodeForKeysOnceOrMore.accept(unseekables);
                    }

                    for (Node.Id node : new TreeSet<>(subset.nodes()))
                    {
                        assertThat(subset.forNode(node))
                                .isEqualTo(trimmed.forNode(node))
                                .isRangesEqualTo(subset.rangesForNode(node))
                                .isRangesEqualTo(trimmed.rangesForNode(node));
                    }
                }
            }
        });
    }

    private static void checkTopology(Topology topology, RandomSource rs)
    {
        for (Node.Id node : topology.nodes())
        {
            Topology subset = topology.forNode(node);
            assertThat(subset).isRangesEqualTo(topology.rangesForNode(node));

            assertThat(topology.mapReduceOn(node, 0, (p1, p2, p3, idx) -> System.identityHashCode(topology.get(idx)), 0, 0, 0, Integer::sum, 0))
                    .isEqualTo(subset.shards().stream().mapToInt(System::identityHashCode).sum())
                    .isEqualTo(subset.mapReduceOn(node, 0, (p1, p2, p3, idx) -> System.identityHashCode(subset.get(idx)), 0, 0, 0, Integer::sum, 0))
                    .isEqualTo(topology.foldlIntOn(node, (p1, accum, idx) -> accum + System.identityHashCode(topology.get(idx)), 0, 0, 0, Integer.MIN_VALUE))
                    .isEqualTo(subset.foldlIntOn(node, (p1, accum, idx) -> accum + System.identityHashCode(subset.get(idx)), 0, 0, 0, Integer.MIN_VALUE));
        }
        for (Range range : topology.ranges())
        {
            Topology subset = topology.forSelection(Ranges.single(range));
            for (int i = 0; i < 10; i++)
            {
                RoutingKey key = routingKey(range, rs);

                assertThat(topology.forSelection(RoutingKeys.of(key))).isEqualTo(subset);

                assertThat(topology.forKey(key))
                        .describedAs("forKey(key) != get(indexForKey(key)) for key %s", key)
                        .isEqualTo(topology.get(topology.indexForKey(key)))
                        .describedAs("forKey(key) != forSelection(range).forKey(key): key=%s, range=%s", key, range)
                        .isEqualTo(subset.forKey(key))
                        .contains(key);
            }
            for (int i = 0; i < 10; i++)
            {
                RoutingKey outsideRange = routingKeyOutsideRange(range, rs);
                if (outsideRange == null) break;
                assertThatThrownBy(() -> subset.forKey(outsideRange))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage("Range not found for %s", outsideRange);
            }
        }
        assertThat(topology.forSelection(topology.ranges())).isEqualTo(topology);
    }
}
