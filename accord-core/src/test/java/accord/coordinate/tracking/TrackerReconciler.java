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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.EnumMap;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;

import accord.burn.TopologyUpdates;
import accord.impl.IntHashKey;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TopologyFactory;
import accord.local.Node.Id;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.topology.TopologyRandomizer;
import accord.utils.DefaultRandom;
import accord.utils.RandomSource;

public abstract class TrackerReconciler<ST extends ShardTracker, T extends AbstractTracker<ST, ?>, E extends Enum<E>>
{
    final RandomSource random;
    final E[] events;
    final EnumMap<E, Integer>[] counts;
    final T tracker;
    final List<Id> inflight;

    protected TrackerReconciler(RandomSource random, Class<E> events, T tracker, List<Id> inflight)
    {
        this.random = random;
        this.events = events.getEnumConstants();
        this.tracker = tracker;
        this.inflight = inflight;
        this.counts = new EnumMap[tracker.trackers.length];
        for (int i = 0 ; i < counts.length ; ++i)
        {
            counts[i] = new EnumMap<>(events);
            for (E event : this.events)
                counts[i].put(event, 0);
        }
    }

    Topologies topologies()
    {
        return tracker.topologies;
    }

    void test()
    {
        while (true)
        {
            if (inflight.isEmpty())
                System.out.println("Oops");
            Assertions.assertFalse(inflight.isEmpty());
            E next = events[random.nextInt(events.length)];
            Id from = inflight.get(random.nextInt(inflight.size()));
            RequestStatus newStatus = invoke(next, tracker, from);
            for (int i = 0 ; i < topologies().size() ; ++i)
            {
                topologies().get(i).forEachOn(from, (s, si) -> {
                    counts[si].compute(next, (ignore, cur) -> cur + 1);
                });
            }

            validate(newStatus);
            if (newStatus != RequestStatus.NoChange)
                break;
        }
    }

    abstract RequestStatus invoke(E event, T tracker, Id from);
    abstract void validate(RequestStatus status);

    protected static <ST extends ShardTracker, T extends AbstractTracker<ST, ?>, E extends Enum<E>>
    List<TrackerReconciler<ST, T, E>> generate(long seed, BiFunction<RandomSource, Topologies, ? extends TrackerReconciler<ST, T, E>> constructor, int maxTopologies)
    {
        System.out.println("seed: " + seed);
        RandomSource random = new DefaultRandom(seed);
        return topologies(random, maxTopologies).map(topologies -> constructor.apply(random, topologies))
                .collect(Collectors.toList());
    }

    // TODO (required, testing): generalise and parameterise topology generation a bit more
    //                           also, select a subset of the generated topologies to correctly simulate topology consumption logic
    private static Stream<Topologies> topologies(RandomSource random, int maxTopologies)
    {
        TopologyFactory factory = new TopologyFactory(2 + random.nextInt(3), IntHashKey.ranges(4 + random.nextInt(12)));
        List<Id> nodes = cluster(factory.rf * (1 + random.nextInt(factory.shardRanges.length - 1)));
        Topology topology = factory.toTopology(nodes);
        int count = Math.min(maxTopologies, 1 + random.nextInt(3));

        List<Topologies> result = new ArrayList<>();
        result.add(new Topologies.Single(SizeOfIntersectionSorter.SUPPLIER, topology));

        if (count == 1)
            return result.stream();

        Deque<Topology> topologies = new ArrayDeque<>();
        topologies.add(topology);
        TopologyUpdates topologyUpdates = new TopologyUpdates();
        TopologyRandomizer configRandomizer = new TopologyRandomizer(() -> random, topology, topologyUpdates, (id, top) -> {});
        while (--count > 0)
        {
            Topology next = configRandomizer.updateTopology();
            while (next == null)
                next = configRandomizer.updateTopology();
            topologies.addFirst(next);
            result.add(new Topologies.Multi(SizeOfIntersectionSorter.SUPPLIER, topologies.toArray(new Topology[0])));
        }
        return result.stream();
    }

    private static List<Id> cluster(int count)
    {
        List<Id> cluster = new ArrayList<>();
        for (int i = 1 ; i <= count ; ++i)
            cluster.add(new Id(i));
        return cluster;
    }
}
