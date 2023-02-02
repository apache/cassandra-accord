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
import accord.impl.IntHashKey;
import accord.impl.IntHashKey.Hash;
import accord.local.Node;
import accord.primitives.Ranges;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.*;

// TODO (required, testing): add change replication factor
public class TopologyRandomizer
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyRandomizer.class);
    private final Random random;
    private final BiConsumer<Node.Id, Topology> notifier;
    private final List<Topology> epochs = new ArrayList<>();
    private final Map<Node.Id, Ranges> previouslyReplicated = new HashMap<>();
    private final TopologyUpdates topologyUpdates;

    public TopologyRandomizer(Supplier<Random> randomSupplier, Topology initialTopology, TopologyUpdates topologyUpdates, Function<Node.Id, Node> lookup)
    {
        this(randomSupplier, initialTopology, topologyUpdates, (id, topology) -> topologyUpdates.notify(lookup.apply(id), topology.nodes(), topology));
    }
    public TopologyRandomizer(Supplier<Random> randomSupplier, Topology initialTopology, TopologyUpdates topologyUpdates, BiConsumer<Node.Id, Topology> notifier)
    {
        this.random = randomSupplier.get();
        this.topologyUpdates = topologyUpdates;
        this.epochs.add(Topology.EMPTY);
        this.epochs.add(initialTopology);
        for (Node.Id node : initialTopology.nodes())
            previouslyReplicated.put(node, initialTopology.rangesForNode(node));
        this.notifier = notifier;
    }

    private enum UpdateType
    {
        BOUNDARY(TopologyRandomizer::updateBoundary),
        MEMBERSHIP(TopologyRandomizer::updateMembership),
        FASTPATH(TopologyRandomizer::updateFastPath);

        private final BiFunction<Shard[], Random, Shard[]> function;

        UpdateType(BiFunction<Shard[], Random, Shard[]> function)
        {
            this.function = function;
        }

        public Shard[] apply(Shard[] shards, Random random)
        {
            return function.apply(shards, random);
        }

        static UpdateType kind(Random random)
        {
            int idx = random.nextInt(values().length);
            return values()[idx];
        }
    }

    private static Shard[] updateBoundary(Shard[] shards, Random random)
    {
        int idx = random.nextInt(shards.length - 1);
        Shard left = shards[idx];
        IntHashKey.Range leftRange = (IntHashKey.Range) left.range;
        Shard right = shards[idx + 1];
        IntHashKey.Range rightRange = (IntHashKey.Range) right.range;
        IntHashKey minBound = (IntHashKey) leftRange.split(2).get(0).end();
        IntHashKey maxBound = (IntHashKey) rightRange.split(2).get(0).start();

        if (minBound.hash == maxBound.hash)
            // no adjustment is possible
            return shards;

        Hash newBound = IntHashKey.forHash(minBound.hash + random.nextInt(maxBound.hash - minBound.hash));

        shards[idx] = new Shard(IntHashKey.range((Hash)leftRange.start(), newBound), left.nodes, left.fastPathElectorate, left.joining);
        shards[idx+1] = new Shard(IntHashKey.range(newBound, (Hash)rightRange.end()), right.nodes, right.fastPathElectorate, right.joining);
//        logger.debug("Updated boundary on {} & {} {} {} to {} {}", idx, idx + 1, left, right,
//                     shards[idx].toString(true), shards[idx + 1].toString(true));

        return shards;
    }

    private static Shard[] updateMembership(Shard[] shards, Random random)
    {
        if (shards.length <= 1)
            return shards;

        int idxLeft = random.nextInt(shards.length);
        Shard shardLeft = shards[idxLeft];

        // bail out if all shards have the same membership
        if (Arrays.stream(shards).allMatch(shard -> shard.nodeSet.equals(shardLeft.nodeSet)))
            return shards;

        int idxRight;
        Shard shardRight;
        do {
            idxRight = random.nextInt(shards.length);
            shardRight = shards[idxRight];
        } while (idxRight == idxLeft || shardLeft.nodeSet.equals(shardRight.nodeSet));

        List<Node.Id> nodesLeft;
        Node.Id toRight;
        for (;;)
        {
            nodesLeft = new ArrayList<>(shardLeft.nodes);
            toRight = nodesLeft.remove(random.nextInt(nodesLeft.size()));
            if (!shardRight.nodes.contains(toRight))
                break;
        }

        List<Node.Id> nodesRight;
        Node.Id toLeft;
        for (;;)
        {
            nodesRight = new ArrayList<>(shardRight.nodes);
            toLeft = nodesRight.remove(random.nextInt(nodesRight.size()));
            if (!nodesLeft.contains(toLeft))
                break;
        }

        nodesLeft.add(toLeft);
        nodesRight.add(toRight);

        shards[idxLeft] = new Shard(shardLeft.range, nodesLeft, newFastPath(nodesLeft, random), shardLeft.joining);
        shards[idxRight] = new Shard(shardRight.range, nodesRight, newFastPath(nodesRight, random), shardLeft.joining);
//        logger.debug("updated membership on {} & {} {} {} to {} {}",
//                    idxLeft, idxRight,
//                    shardLeft.toString(true), shardRight.toString(true),
//                    shards[idxLeft].toString(true), shards[idxRight].toString(true));

        return shards;
    }

    private static Set<Node.Id> newFastPath(List<Node.Id> nodes, Random random)
    {
        List<Node.Id> available = new ArrayList<>(nodes);
        int rf = available.size();
        int f = Shard.maxToleratedFailures(rf);
        int minSize = rf - f;
        int newSize = minSize + random.nextInt(f + 1);

        Set<Node.Id> fastPath = new HashSet<>();
        for (int i=0; i<newSize; i++)
        {
            int idx = random.nextInt(available.size());
            fastPath.add(available.remove(idx));
        }

        return fastPath;
    }

    private static Shard[] updateFastPath(Shard[] shards, Random random)
    {
        int idx = random.nextInt(shards.length);
        Shard shard = shards[idx];
        shards[idx] = new Shard(shard.range, shard.nodes, newFastPath(shard.nodes, random), shard.joining);
//        logger.debug("Updated fast path on {} {} to {}", idx, shard.toString(true), shards[idx].toString(true));
        return shards;
    }

    private static Map<Node.Id, Ranges> getAdditions(Topology current, Topology next)
    {
        Map<Node.Id, Ranges> additions = new HashMap<>();
        for (Node.Id node : next.nodes())
        {
            Ranges prev = current.rangesForNode(node);
            if (prev == null) prev = Ranges.EMPTY;

            Ranges added = next.rangesForNode(node).difference(prev);
            if (added.isEmpty())
                continue;

            additions.put(node, added);
        }
        return additions;
    }

    private static boolean reassignsRanges(Topology current, Shard[] nextShards, Map<Node.Id, Ranges> previouslyReplicated)
    {
        Topology next = new Topology(current.epoch + 1, nextShards);
        Map<Node.Id, Ranges> additions = getAdditions(current, next);

        for (Map.Entry<Node.Id, Ranges> entry : additions.entrySet())
        {
            if (previouslyReplicated.getOrDefault(entry.getKey(), Ranges.EMPTY).intersects(entry.getValue()))
                return true;
        }
        return false;
    }

    public synchronized void maybeUpdateTopology() {
        // if we don't limit the number of pending topology changes in flight,
        // the topology randomizer will keep the burn test busy indefinitely
        if (topologyUpdates.pendingTopologies() > 5 || random.nextInt(200) != 0)
            return;

        updateTopology();
    }

    public synchronized Topology updateTopology()
    {
        Topology current = epochs.get(epochs.size() - 1);
        Shard[] shards = current.unsafeGetShards().clone();
        int mutations = random.nextInt(current.size());
//        logger.debug("Updating topology with {} mutations", mutations);
        for (int i=0; i<mutations; i++)
        {
            shards = UpdateType.kind(random).apply(shards, random);
        }

        Topology nextTopology = new Topology(current.epoch + 1, shards);

        // TODO (expected, testing): remove this (and the corresponding check in CommandStores) once lower bounds are implemented.
        //  In the meantime, the logic needed to support acquiring ranges that we previously replicated is pretty
        //  convoluted without the ability to jettison epochs.
        if (reassignsRanges(current, shards, previouslyReplicated))
            return null;

        Map<Node.Id, Ranges> nextAdditions = getAdditions(current, nextTopology);
        for (Map.Entry<Node.Id, Ranges> entry : nextAdditions.entrySet())
        {
            Ranges previous = previouslyReplicated.getOrDefault(entry.getKey(), Ranges.EMPTY);
            Ranges added = entry.getValue();
            Ranges merged = previous.with(added).mergeTouching();
            previouslyReplicated.put(entry.getKey(), merged);
        }

//        logger.debug("topology update to: {} from: {}", nextTopology, current);
        epochs.add(nextTopology);

        List<Node.Id> nodes = new ArrayList<>(nextTopology.nodes());

        int originatorIdx = random.nextInt(nodes.size());
        notifier.accept(nodes.remove(originatorIdx), nextTopology);
        return nextTopology;
    }
}
