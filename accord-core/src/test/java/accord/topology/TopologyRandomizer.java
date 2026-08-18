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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.OwnershipEventListener;
import accord.burn.TopologyUpdates;
import accord.impl.PrefixedIntHashKey;
import accord.impl.PrefixedIntHashKey.Hash;
import accord.impl.PrefixedIntHashKey.PrefixedIntRoutingKey;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.durability.DurabilityService;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.SortedArrays.SortedArrayList;
import org.agrona.collections.IntHashSet;

import static accord.burn.BurnTestBase.HASH_RANGE_END;
import static accord.burn.BurnTestBase.HASH_RANGE_START;
import static accord.local.durability.DurabilityService.SyncLocal.NoLocal;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;

// TODO (testing): add change replication factor
public class TopologyRandomizer
{
    // TODO (expected): relax this once we can recover from availability loss
    public static final int MIN_RF = 3;

    public interface Listener
    {
        void onUpdate(Topology topology);
    }

    public enum Listeners implements Listener
    {
        NOOP
        {
            @Override
            public void onUpdate(Topology topology)
            {

            }
        }
    }

    private static class State
    {
        Queue<Integer> newPrefixes;
        Shard[] shards;
    }

    private static final Logger logger = LoggerFactory.getLogger(TopologyRandomizer.class);
    private static final Id[] EMPTY_NODES = new Id[0];
    private static final Shard[] EMPTY_SHARDS = new Shard[0];

    private final RandomSource random;
    private final List<Topology> epochs = new ArrayList<>();
    private final @Nullable Function<Id, Node> nodeLookup;
    private final Map<Id, Map<Long, Ranges>> bootstrapping = new HashMap<>();
    private final Map<Id, Integer> bootstrappingGeneration = new HashMap<>();
    private final ConcurrentLinkedQueue<Integer> newPrefixes = new ConcurrentLinkedQueue<>();
    // TODO (required): remove this restriction, we should be able to replicate previously owned ranges just fine
    private final Map<Id, Ranges> previouslyReplicated = new HashMap<>();
    private final TopologyUpdates topologyUpdates;
    private final Listener listener;

    public TopologyRandomizer(Supplier<RandomSource> randomSupplier, int[] prefixes, Topology initialTopology, TopologyUpdates topologyUpdates, @Nullable Function<Id, Node> nodeLookup, Listener listener)
    {
        this.random = randomSupplier.get();
        this.topologyUpdates = topologyUpdates;
        this.epochs.add(Topology.EMPTY);
        this.epochs.add(initialTopology);
        for (Id node : initialTopology.nodes())
            previouslyReplicated.put(node, initialTopology.rangesForNode(node));
        this.nodeLookup = nodeLookup;
        this.listener = listener;
        for (int prefix : prefixes)
            newPrefixes.add(prefix);
    }

    @VisibleForTesting
    enum UpdateType
    {
//        BOUNDARY(TopologyRandomizer::updateBoundary),
        SPLIT(TopologyRandomizer::split),
        MERGE(TopologyRandomizer::merge),
        MEMBERSHIP(TopologyRandomizer::updateMembership),
        FASTPATH(TopologyRandomizer::updateFastPath),
        ADD_PREFIX(TopologyRandomizer::addPrefix);

        private final BiFunction<State, RandomSource, Shard[]> function;

        UpdateType(BiFunction<State, RandomSource, Shard[]> function)
        {
            this.function = function;
        }

        public Shard[] apply(State state, RandomSource random)
        {
            return function.apply(state, random);
        }

        static UpdateType kind(RandomSource random)
        {
            int idx = random.nextInt(values().length);
            return values()[idx];
        }
    }

    public void markRebootstrapping(Node node)
    {
        long epoch = node.topology().epoch();
        Ranges ranges = node.topology().currentLocal().ranges();
        bootstrapping.remove(node.id());
        bootstrapping.put(node.id(), new TreeMap<>());
        bootstrapping.get(node.id()).put(epoch, ranges);
    }

    private static Shard[] updateBoundary(Shard[] shards, RandomSource random)
    {
        int idx = random.nextInt(shards.length - 1);
        Shard left = shards[idx];
        PrefixedIntHashKey.Range leftRange = (PrefixedIntHashKey.Range) left.range;
        Shard right = shards[idx + 1];
        PrefixedIntHashKey.Range rightRange = (PrefixedIntHashKey.Range) right.range;
        PrefixedIntHashKey minBound = (PrefixedIntHashKey) leftRange.split(2).get(0).end();
        PrefixedIntHashKey maxBound = (PrefixedIntHashKey) rightRange.split(2).get(0).start();

        if (minBound.hash == maxBound.hash)
            // no adjustment is possible
            return shards;

        Hash newBound = PrefixedIntHashKey.forHash(minBound.prefix, minBound.hash + random.nextInt(maxBound.hash - minBound.hash));

        shards[idx] = Shard.create(PrefixedIntHashKey.range((Hash)leftRange.start(), newBound), left.nodes, left.notInFastPath);
        shards[idx+1] = Shard.create(PrefixedIntHashKey.range(newBound, (Hash)rightRange.end()), right.nodes, right.notInFastPath);
//        logger.debug("Updated boundary on {} & {} {} {} to {} {}", idx, idx + 1, left, right,
//                     shards[idx].toString(true), shards[idx + 1].toString(true));

        return shards;
    }

    private static Shard[] split(State state, RandomSource random)
    {
        Shard[] shards = state.shards;
        if (shards.length == 0)
            throw new IllegalArgumentException("Unable to split an empty array");
        int idx = shards.length == 1 ? 0 : random.nextInt(shards.length - 1);
        Shard split = shards[idx];
        PrefixedIntHashKey.Range splitRange = (PrefixedIntHashKey.Range) split.range;
        PrefixedIntRoutingKey minBound = (PrefixedIntRoutingKey) splitRange.start();
        PrefixedIntRoutingKey maxBound = (PrefixedIntRoutingKey) splitRange.end();

        if (minBound.hash + 1 == maxBound.hash)
            // no split is possible
            return shards;

        Hash newBound = PrefixedIntHashKey.forHash(minBound.prefix, random.nextInt(minBound.hash + 1, maxBound.hash));

        Shard[] result = new Shard[shards.length + 1];
        System.arraycopy(shards, 0, result, 0, idx);
        System.arraycopy(shards, idx, result, idx + 1, shards.length - idx);
        result[idx] = new Shard(PrefixedIntHashKey.range(minBound, newBound), split.nodes, split.notInFastPath, Shard.NO_NODES, split.flags());
        result[idx+1] = new Shard(PrefixedIntHashKey.range(newBound, maxBound), split.nodes, split.notInFastPath, Shard.NO_NODES, split.flags());
        logger.debug("Split boundary on {} & {} {} to {} {}", idx, idx + 1, split,
                     result[idx].toString(true), result[idx + 1].toString(true));

        return result;
    }

    private static Shard[] merge(State state, RandomSource random)
    {
        Shard[] shards = state.shards;
        if (shards.length <= 1)
            return shards;

        int idx = shards.length == 2 ? 0 : random.nextInt(shards.length - 2);
        Shard left = shards[idx];
        Shard right = shards[idx + 1];
        while (prefix(left) != prefix(right))
        {
            // shards are a single prefix, so can't merge
            if (idx + 2 == shards.length)
                return shards;
            idx++;
            left = shards[idx];
            right = shards[idx + 1];
        }

        Shard[] result = new Shard[shards.length - 1];
        System.arraycopy(shards, 0, result, 0, idx);
        System.arraycopy(shards, idx + 2, result, idx + 1, shards.length - (idx + 2));
        Range range = PrefixedIntHashKey.range((Hash)left.range.start(), (Hash)right.range.end());
        SortedArrayList<Id> nodes; {
            TreeSet<Id> tmp = new TreeSet<>();
            tmp.addAll(left.nodes);
            tmp.addAll(right.nodes);
            nodes = new SortedArrayList<>(tmp.toArray(new Id[0]));
        }

        result[idx] = Shard.create(range, nodes, newFastPath(nodes, random));
        logger.debug("Merging at {} & {} {} {} to {}", idx, idx + 1, left, right,
                     shards[idx].toString(true));
        return result;
    }

    private static Shard[] updateMembership(State state, RandomSource random)
    {
        Shard[] shards = state.shards;
        if (shards.length <= 1)
            return shards;

        int idxLeft = random.nextInt(shards.length);
        Shard shardLeft = shards[idxLeft];

        // bail out if all shards have the same membership
        if (Arrays.stream(shards).allMatch(shard -> shard.nodes.containsAll(shardLeft.nodes) || shardLeft.containsAll(shard.nodes)))
            return shards;

        int idxRight;
        Shard shardRight;
        do {
            idxRight = random.nextInt(shards.length);
            shardRight = shards[idxRight];
        } while (idxRight == idxLeft || shardLeft.nodes.containsAll(shardRight.nodes) || shardRight.nodes.containsAll(shardLeft.nodes));

        List<Id> nodesLeft;
        Id toRight;
        for (;;)
        {
            nodesLeft = new ArrayList<>(shardLeft.nodes);
            toRight = nodesLeft.remove(random.nextInt(nodesLeft.size()));
            if (!shardRight.contains(toRight))
                break;
        }

        List<Id> nodesRight;
        Id toLeft;
        for (;;)
        {
            nodesRight = new ArrayList<>(shardRight.nodes);
            toLeft = nodesRight.remove(random.nextInt(nodesRight.size()));
            if (!nodesLeft.contains(toLeft))
                break;
        }

        nodesLeft.add(toLeft);
        nodesRight.add(toRight);

        Shard[] newShards = shards.clone();
        newShards[idxLeft] = Shard.create(shardLeft.range, SortedArrayList.copyUnsorted(nodesLeft, Id[]::new), newFastPath(nodesLeft, random));
        newShards[idxRight] = Shard.create(shardRight.range, SortedArrayList.copyUnsorted(nodesRight, Id[]::new), newFastPath(nodesRight, random));
        logger.debug("updated membership on {} & {} {} {} to {} {}",
                    idxLeft, idxRight,
                    shardLeft.toString(true), shardRight.toString(true),
                    newShards[idxLeft].toString(true), newShards[idxRight].toString(true));

        return newShards;
    }

    private static Set<Id> newFastPath(List<Id> nodes, RandomSource random)
    {
        List<Id> available = new ArrayList<>(nodes);
        int rf = available.size();
        int f = Shard.maxToleratedFailures(rf);
        int minSize = rf - f;
        int newSize = minSize + random.nextInt(f + 1);

        Set<Id> fastPath = new HashSet<>();
        for (int i=0; i<newSize; i++)
        {
            int idx = random.nextInt(available.size());
            fastPath.add(available.remove(idx));
        }

        return fastPath;
    }

    private static Shard[] updateFastPath(State state, RandomSource random)
    {
        Shard[] shards = state.shards;
        int idx = random.nextInt(shards.length);
        Shard shard = shards[idx];
        shards[idx] = Shard.create(shard.range, shard.nodes, newFastPath(shard.nodes, random));
//        logger.debug("Updated fast path on {} {} to {}", idx, shard.toString(true), shards[idx].toString(true));
        return shards;
    }

    private static Shard[] addPrefix(State state, RandomSource random)
    {
        Shard[] shards = state.shards;
        // Future work will add a new "removePrefix" method, that will cause prefixes to be dropped over time, when that happens the ABA problem
        // could pop up (add prefix=0, drop prefix=0, add prefix=0) which is not the focus of this logic, so attempt to also generate a higher
        // prefix than seen before.
        // TODO (coverage): add support for bringing prefixes back after removal
        // In implementations (such as Apache Cassandra) its possible that a range exists, gets removed, then added back (CREATE KEYSPACE, DROP KEYSPACE, CREATE KEYSPACE),
        // in this case the old prefix should be "cleared".
        Integer prefix = state.newPrefixes.poll();
        if (prefix == null)
            return shards;

        Id[] nodes;
        {
            Set<Id> uniq = new HashSet<>();
            for (Shard shard : shards)
                uniq.addAll(shard.nodes);
            Id[] result = uniq.toArray(EMPTY_NODES);
            Arrays.sort(result);
            nodes = result;
        }

        int rf;
        if (nodes.length <= 3)
        {
            rf = nodes.length;
        }
        else
        {
            float chance = random.nextFloat();
            if (chance < 0.2f)       { rf = random.nextInt(MIN_RF, Math.min(random.nextInt(5, 9), nodes.length)); }
            else if (chance < 0.4f)  { rf = 3; }
            else if (chance < 0.7f)  { rf = Math.min(5, nodes.length); }
            else if (chance < 0.95f) { rf = Math.min(7, nodes.length); }
            else                     { rf = Math.min(9, nodes.length); }
        }
        List<Shard> result = new ArrayList<>(shards.length + nodes.length);
        result.addAll(Arrays.asList(shards));
        Range[] ranges = PrefixedIntHashKey.ranges(prefix, HASH_RANGE_START, HASH_RANGE_END, nodes.length);
        for (int i = 0; i < ranges.length; i++)
        {
            Range range = ranges[i];
            SortedArrayList<Id> replicas = select(nodes, rf, random);
            Set<Id> fastPath = newFastPath(replicas, random);
            result.add(Shard.create(range, replicas, fastPath));
        }
        return result.toArray(EMPTY_SHARDS);
    }

    private static int[] prefixes(Shard[] shards)
    {
        IntHashSet uniq = new IntHashSet();
        for (Shard shard : shards)
            uniq.add(((PrefixedIntHashKey) shard.range.start()).prefix);
        int[] prefixes = new int[uniq.size()];
        IntHashSet.IntIterator it = uniq.iterator();
        for (int i = 0; it.hasNext(); i++)
            prefixes[i] = it.nextValue();
        Arrays.sort(prefixes);
        return prefixes;
    }

    private static SortedArrayList<Id> select(Id[] nodes, int rf, RandomSource random)
    {
        Invariants.requireArgument(nodes.length >= rf, "Given %d nodes, which is < rf of %d", nodes.length, rf);
        List<Id> result = new ArrayList<>(rf);
        while (result.size() < rf)
        {
            Id id = random.pick(nodes);
            // TODO (efficiency) : rf is normally "small", so is it worth it to have a set, bitset, or another structure?
            if (!result.contains(id))
                result.add(id);
        }
        return SortedArrayList.copyUnsorted(result, Id[]::new);
    }

    private static int prefix(Shard shard)
    {
        return ((PrefixedIntHashKey) shard.range.start()).prefix;
    }

    private boolean validToReassignRange(Topology current, Shard[] nextShards, Map<Id, Ranges> previouslyReplicated)
    {
        Topology next = new Topology(current.epoch + 1, nextShards);
        Map<Id, Ranges> additions = Topology.computeNodeAdditions(current, next);

        for (Map.Entry<Id, Ranges> entry : additions.entrySet())
        {
            if (previouslyReplicated.getOrDefault(entry.getKey(), Ranges.EMPTY).intersects(entry.getValue())
                    && !(previousEpochForRegainedRangeRetired(current, entry.getValue())))
                return false;
        }

        return true;
    }

    private boolean previousEpochForRegainedRangeRetired(Topology current, Ranges regainingRanges)
    {
        // When nodeLookup isn't defined we are unable to get node state, so
        // assume that the calling test doesn't care about retired ranges
        if (this.nodeLookup == null)
            return true;

        for (Id id : current.nodes())
        {
            Node node = this.nodeLookup.apply(id);
            boolean isRetiredForNode = true;
            for (ActiveEpoch epoch : node.topology().active())
            {
                if (epoch.all().ranges.intersects(regainingRanges) && !epoch.allRetired())
                    isRetiredForNode = false;
            }

            if (isRetiredForNode)
                return true;
        }

        return false;
    }

    public synchronized void maybeUpdateTopology()
    {
        // if we don't limit the number of pending topology changes in flight,
        // the topology randomizer will keep the burn test busy indefinitely
        if (topologyUpdates.pendingTopologies() > 5)
            return;

        updateTopology();
    }

    private int counter = 0;
    public synchronized Topology updateTopology()
    {
        Topology current = epochs.get(epochs.size() - 1);
        Shard[] oldShards = current.unsafeGetShards().clone();
        int remainingMutations = random.nextInt(Math.min(current.size() + 1, 10));
        int rejectedMutations = 0;
        logger.debug("Updating topology with {} mutations", remainingMutations);
        Shard[] newShards = oldShards;
        State state = new State();
        state.newPrefixes = newPrefixes;
        while (remainingMutations > 0 && rejectedMutations < 10)
        {
            ++counter;
            UpdateType type = UpdateType.kind(random);
            state.shards = newShards;
            Shard[] testShards = type.apply(state, random);
            Arrays.sort(testShards, (a, b) -> a.range.compareTo(b.range));
            if (!everyShardHasQuorumOverlaps(oldShards, testShards)
                || !validToReassignRange(current, testShards, previouslyReplicated))
            {
                ++rejectedMutations;
            }
            else
            {
                newShards = testShards;
                --remainingMutations;
            }
        }

        if (newShards == oldShards)
            return null;

        Topology nextTopology = new Topology(current.epoch + 1, newShards);

        Map<Id, Ranges> nextAdditions = Topology.computeNodeAdditions(current, nextTopology);
        for (Map.Entry<Id, Ranges> entry : nextAdditions.entrySet())
        {
            previouslyReplicated.merge(entry.getKey(), entry.getValue(), (a, b) -> a.union(MERGE_ADJACENT, b));
            bootstrapping.putIfAbsent(entry.getKey(), new TreeMap<>());
            bootstrapping.get(entry.getKey()).put(nextTopology.epoch, entry.getValue());
        }

        epochs.add(nextTopology);
        listener.onUpdate(nextTopology);

        if (nodeLookup != null)
        {
            List<Id> nodes = new ArrayList<>(nextTopology.nodes());
            int originatorIdx = random.nextInt(nodes.size());
            topologyUpdates.notify(nodeLookup.apply(nodes.get(originatorIdx)), current, nextTopology);
        }
        return nextTopology;
    }

    // TODO (expected): relax this to checking only that we have fewer than maxFailures intersections
    public boolean unsafeToRebootstrap(Ranges ranges)
    {
        for (Map<Long, Ranges> map : bootstrapping.values())
        {
            for (Ranges test : map.values())
            {
                if (ranges.intersects(test))
                    return true;
            }
        }
        return false;
    }

    private boolean everyShardHasQuorumOverlaps(Shard[] in, Shard[] out)
    {
        return testChanges(in, out, (iv, ov) -> {
            // TODO (expected): support availability loss and recovery, and minority quorums
            int common = (int) ov.nodes.stream().filter(iv::contains).count();
            int commonBootstrap = (int) ov.nodes.stream().filter(iv::contains).filter(id -> {
                return bootstrapping.getOrDefault(id, Collections.emptyMap()).values().stream()
                                    .anyMatch(ranges -> ranges.intersects(iv.range) || ranges.intersects(ov.range));
            }).count();
            int inBootstrap = (int) iv.nodes.stream().filter(id -> bootstrapping.getOrDefault(id, Collections.emptyMap()).values().stream()
                                                                                .anyMatch(ranges -> ranges.intersects(iv.range)))
                                            .count();
            return inBootstrap <= iv.maxFailures && commonBootstrap + (ov.rf - common) <= ov.maxFailures;
        });
    }

    private boolean testChanges(Shard[] in, Shard[] out, BiPredicate<Shard, Shard> consumer)
    {
        int i = 0, o = 0;
        while (i < in.length && o < out.length)
        {
            Shard iv = in[i];
            Shard ov = out[o];
            if (ov != iv)
            {
                if (!consumer.test(iv, ov))
                    return false;
            }
            int c = iv.range.end().compareTo(ov.range.end());
            if (c <= 0) ++i;
            if (c >= 0) ++o;
        }
        return true;
    }

    public void rotateBootstrapping(Id id)
    {
        bootstrappingGeneration.compute(id, (ignore, cur) -> cur == null ? 1 : cur + 1);
    }

    public OwnershipEventListener listener(Id id)
    {
        return new OwnershipEventListener()
        {
            @Override
            public void onFailedBootstrap(int attempt, String phase, Ranges ranges, Runnable retry, Runnable fail, Throwable failure)
            {
            }

            @Override
            public void onSuccessfulBootstrap(CommandStore commandStore, int attempt, long epoch, Ranges ranges)
            {
                Node node = nodeLookup.apply(id);
                Integer generation = bootstrappingGeneration.get(id);
                node.durability().sync("Rebootstrap", Txn.Kind.ExclusiveSyncPoint, ranges, NoLocal, DurabilityService.SyncRemote.Quorum, 100L, TimeUnit.DAYS)
                    .flatMap(ignore -> commandStore.awaitVisibility(epoch, ranges))
                    .invoke((success, fail) -> {
                        Invariants.require(fail == null);
                        bootstrapping.compute(id, (ignore, map) -> {
                            if (generation != bootstrappingGeneration.get(id))
                                return map;

                            Invariants.require(map != null);
                            map.compute(epoch, (ignore0, rs) -> {
                                Invariants.require(rs != null && rs.containsAll(ranges));
                                rs = rs.without(ranges);
                                return rs.isEmpty() ? null : rs;
                            });
                            return map.isEmpty() ? null : map;
                        });
                    });
            }

            @Override
            public void onStale(Timestamp sinceAtLeast, Ranges ranges)
            {
                int epoch = (int) sinceAtLeast.epoch();
                Invariants.require(epochs.get(epoch).nodeLookup.get(id.id).ranges.containsAll(ranges));
                while (++epoch < epochs.size())
                {
                    ranges = ranges.slice(epochs.get(epoch).nodeLookup.get(id.id).ranges, Routables.Slice.Minimal);
                    if (ranges.isEmpty())
                        return;
                }
                Invariants.illegalState("Stale ranges: " + ranges + ", but "+ id + " still replicates these ranges");
            }
        };
    }

}
