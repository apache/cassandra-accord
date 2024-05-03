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

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.impl.PrefixedIntHashKey;
import accord.primitives.Range;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Unseekables;
import accord.primitives.Unseekables.UnseekablesKind;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.Utils;
import accord.utils.WrapAroundList;
import accord.utils.WrapAroundSet;

import javax.annotation.Nullable;
import java.util.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;

import static accord.utils.Utils.toArray;

public class TopologyUtils
{
    public static Ranges initialRanges(int num, int maxKey)
    {
        int rangeSize = maxKey / num;
        Range[] ranges = new Range[num];
        int end = 0;
        for (int i=0; i<num; i++)
        {
            int start = end;
            end = i == num - 1 ? maxKey : start + rangeSize;
            ranges[i] = IntKey.range(start, end);
        }
        return Ranges.of(ranges);
    }

    public static Topology withEpoch(Topology topology, long epoch)
    {
        return new Topology(topology.global == null ? null : withEpoch(topology.global, epoch), epoch, topology.shards, topology.ranges, topology.nodeLookup, topology.subsetOfRanges, topology.supersetIndexes);
    }

    public static Topology topology(long epoch, List<Node.Id> cluster, Ranges ranges, int rf)
    {
        return topology(epoch, toArray(cluster, Node.Id[]::new), ranges, rf);
    }

    public static Topology topology(long epoch, Node.Id[] cluster, Ranges ranges, int rf)
    {
        final Map<Node.Id, Integer> lookup = new HashMap<>();
        for (int i = 0; i < cluster.length ; ++i)
            lookup.put(cluster[i], i);

        List<WrapAroundList<Node.Id>> electorates = new ArrayList<>();
        List<Set<Node.Id>> fastPathElectorates = new ArrayList<>();

        for (int i = 0; i < cluster.length + rf - 1 ; ++i)
        {
            WrapAroundList<Node.Id> electorate = new WrapAroundList<>(cluster, i % cluster.length, (i + rf) % cluster.length);
            Set<Node.Id> fastPathElectorate = new WrapAroundSet<>(lookup, electorate);
            electorates.add(electorate);
            fastPathElectorates.add(fastPathElectorate);
        }

        final List<Shard> shards = new ArrayList<>();
        Set<Node.Id> noShard = new HashSet<>(Arrays.asList(cluster));
        for (int i = 0; i < ranges.size() ; ++i)
        {
            shards.add(new Shard(ranges.get(i), electorates.get(i % electorates.size()), fastPathElectorates.get(i % fastPathElectorates.size())));
            noShard.removeAll(electorates.get(i % electorates.size()));
        }
        if (!noShard.isEmpty())
            throw new AssertionError(String.format("The following electorates were found without a shard: %s", noShard));

        return new Topology(epoch, toArray(shards, Shard[]::new));
    }

    public static Topology initialTopology(Node.Id[] cluster, Ranges ranges, int rf)
    {
        return topology(1, cluster, ranges, rf);
    }

    public static Topology initialTopology(List<Node.Id> cluster, Ranges ranges, int rf)
    {
        return initialTopology(toArray(cluster, Node.Id[]::new), ranges, rf);
    }

    public static RoutingKey routingKey(Range range, RandomSource rs)
    {
        if (range.start() instanceof PrefixedIntHashKey.Hash)
        {
            PrefixedIntHashKey.Hash start = (PrefixedIntHashKey.Hash) range.start();
            PrefixedIntHashKey.Hash end = (PrefixedIntHashKey.Hash) range.end();
            int value = rs.nextInt(start.hash, end.hash);
            if (range.endInclusive()) // exclude start, but include end... so +1
                value++;
            return PrefixedIntHashKey.forHash(start.prefix, value);
        }
        else if (range.start() instanceof IntKey.Routing)
        {
            IntKey.Routing start = (IntKey.Routing) range.start();
            IntKey.Routing end = (IntKey.Routing) range.end();
            int value = rs.nextInt(start.key, end.key);
            if (range.endInclusive()) // exclude start, but include end... so +1
                value++;
            return IntKey.routing(value);
        }
        else
        {
            throw new IllegalArgumentException("Key type " + range.start().getClass() + " is not supported");
        }
    }

    private enum Outside { BEFORE, AFTER }

    @Nullable
    public static RoutingKey routingKeyOutsideRange(Range range, RandomSource rs)
    {
        if (range.start() instanceof PrefixedIntHashKey)
        {
            int minHash = Integer.MIN_VALUE;
            int maxHash = Integer.MAX_VALUE;
            PrefixedIntHashKey.Hash start = (PrefixedIntHashKey.Hash) range.start();
            PrefixedIntHashKey.Hash end = (PrefixedIntHashKey.Hash) range.end();

            EnumSet<Outside> allowed = EnumSet.allOf(Outside.class);
            if (start.hash == minHash)
                allowed.remove(Outside.BEFORE);
            if (end.hash == maxHash)
                allowed.remove(Outside.AFTER);
            if (allowed.isEmpty()) return null;
            Outside next = Gens.pick(new ArrayList<>(allowed)).next(rs);
            int value;
            switch (next)
            {
                case BEFORE:
                    value = rs.nextInt(minHash, range.startInclusive() ? start.hash : start.hash + 1);
                    break;
                case AFTER:
                    value = rs.nextInt(range.endInclusive() ? end.hash + 1 : end.hash, maxHash);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown value " + next);
            }

            return PrefixedIntHashKey.forHash(start.prefix, value);
        }
        else
        {
            throw new IllegalArgumentException("Key type " + range.start().getClass() + " is not supported");
        }
    }

    public static Unseekables<?> select(Ranges ranges, RandomSource rs)
    {
        UnseekablesKind kind = rs.pick(UnseekablesKind.RoutingKeys, UnseekablesKind.RoutingRanges);
        switch (kind)
        {
            default: throw new IllegalStateException("Unexpected kind: " + kind);
            case RoutingKeys:
                Set<RoutingKey> keys = new TreeSet<>();
                for (int i = 0, attempts = rs.nextInt(1, 10); i < attempts; i++)
                    keys.add(routingKey(ranges.get(rs.nextInt(ranges.size())), rs));
                return RoutingKeys.ofSortedUnique(Utils.toArray(new ArrayList<>(keys), RoutingKey[]::new));
            case RoutingRanges:
                Set<Range> selected = new TreeSet<>(Range::compare);
                for (int i = 0, attempts = rs.nextInt(1, 10); i < attempts; i++)
                    selected.add(ranges.get(rs.nextInt(ranges.size()))); // TODO sub-ranges
                return Ranges.ofSortedAndDeoverlapped(Utils.toArray(new ArrayList<>(selected), Range[]::new));
        }
    }

    public static String diff(Topology left, Topology right)
    {
        // topology is just range -> (nodes, fast path, joining)
        // what is in left that isn't in right?
        Iterator<Shard> outter = left.shards().iterator();
        PeekingIterator<Shard> inner = Iterators.peekingIterator(right.shards().iterator());
        // ranges in left
        // ranges in right
        // intersects...
        Set<Range> leftRanges = new LinkedHashSet<>();
        Set<Range> rightRanges = new LinkedHashSet<>();
//        Set<Range> splitRanges = new LinkedHashSet<>();
        Multimap<Range, Range> splitRanges = LinkedHashMultimap.create();
        Map<Range, Map<String, Map<String, ?>>> eqRanges = new LinkedHashMap<>();
        for (int i = 0; outter.hasNext(); i++)
        {
            Shard next = outter.next();
            if (!inner.hasNext()) {
                leftRanges.add(next.range);
                continue;
            }
            Shard other = inner.peek();
            if (next.equals(other))
            {
                inner.next();
                continue;
            }
            // why are they different?
            if (next.range.compareIntersecting(other.range) == 0)
            {
                // shards intersect
                if (next.range.equals(other.range))
                {
                    // ranges are equal, but their nodes differ
                    ImmutableMap.Builder<String, Map<String, ?>> builder = ImmutableMap.builder();
                    Map<String, Set<Node.Id>> nodes = diff(new HashSet<>(next.nodes), new HashSet<>(other.nodes));
                    if (!nodes.isEmpty())
                        builder.put("nodes", nodes);
                    Map<String, Set<Node.Id>> fastPathElectorate = diff(next.fastPathElectorate, other.fastPathElectorate);
                    if (!fastPathElectorate.isEmpty())
                        builder.put("fastPathElectorate", fastPathElectorate);
                    Map<String, Set<Node.Id>> joining = diff(next.joining, other.joining);
                    if (!joining.isEmpty())
                        builder.put("joining", joining);
                    eqRanges.put(next.range, builder.build());
                }
                else
                {
                    splitRanges.put(next.range, other.range);
                }
                if (next.range.end().compareTo(other.range.end()) >= 0)
                    inner.next();
            }
            else if (next.range.compare(other.range) < 0)
            {
                leftRanges.add(next.range);
            }
            else
            {
                rightRanges.add(inner.next().range);
            }
        }
        while (inner.hasNext())
            rightRanges.add(inner.next().range);
        Map<String, Object> details = new LinkedHashMap<>();
//        StringBuilder sb = new StringBuilder();
        if (!leftRanges.isEmpty())
            details.put("Left Ranges: ", leftRanges);
        if (!rightRanges.isEmpty())
            details.put("Right Ranges: ", rightRanges);
        if (!splitRanges.isEmpty())
            details.put("Split Ranges: ", splitRanges);
        if (!eqRanges.isEmpty())
            details.put("Eq Ranges: ", eqRanges);
//        return sb.toString();
        return details.toString();
    }

    private static <E> Map<String, Set<E>> diff(Set<E> left, Set<E> right)
    {
        Sets.SetView<E> rightMissing = Sets.difference(left, right);
        Sets.SetView<E> leftMissing = Sets.difference(right, left);
        if (rightMissing.isEmpty() && leftMissing.isEmpty())
            return Collections.emptyMap();
        ImmutableMap.Builder<String, Set<E>> builder = ImmutableMap.builder();
        if (!rightMissing.isEmpty())
            builder.put("left", rightMissing);
        if (!leftMissing.isEmpty())
            builder.put("right", leftMissing);
        return builder.build();
    }
}
