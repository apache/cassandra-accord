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

package accord.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.IntHashKey;
import accord.impl.IntKey;
import accord.impl.PrefixedIntHashKey;
import accord.local.Node;
import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topology;
import org.agrona.collections.IntHashSet;

import static accord.utils.Utils.toArray;

public class AccordGens
{
    public static Gen.LongGen epochs()
    {
        return Gens.longs().between(0, Timestamp.MAX_EPOCH);
    }

    public static Gen<Node.Id> nodes()
    {
        return nodes(RandomSource::nextInt);
    }

    public static Gen<Node.Id> nodes(Gen.IntGen nodes)
    {
        return nodes.map(Node.Id::new);
    }

    public static Gen<TxnId> txnIds()
    {
        return txnIds(epochs()::nextLong, rs -> rs.nextLong(0, Long.MAX_VALUE), RandomSource::nextInt);
    }

    public static Gen<TxnId> txnIds(Gen.LongGen epochs, Gen.LongGen hlcs, Gen.IntGen nodes)
    {
        Gen<Txn.Kind> kinds = Gens.enums().all(Txn.Kind.class);
        Gen<Routable.Domain> domains = Gens.enums().all(Routable.Domain.class);
        return rs -> new TxnId(epochs.nextLong(rs), hlcs.nextLong(rs), kinds.next(rs), domains.next(rs), new Node.Id(nodes.nextInt(rs)));
    }

    public static Gen<Key> intKeys()
    {
        return rs -> new IntKey.Raw(rs.nextInt());
    }

    public static Gen<Key> intHashKeys()
    {
        return rs -> IntHashKey.key(rs.nextInt());
    }

    public static Gen<KeyDeps> keyDeps(Gen<? extends Key> keyGen)
    {
        return keyDeps(keyGen, txnIds());
    }

    public static Gen<KeyDeps> keyDeps(Gen<? extends Key> keyGen, Gen<TxnId> idGen)
    {
        double emptyProb = .2D;
        return rs -> {
            if (rs.decide(emptyProb)) return KeyDeps.NONE;
            Set<Key> seenKeys = new HashSet<>();
            Set<TxnId> seenTxn = new HashSet<>();
            Gen<? extends Key> uniqKeyGen = keyGen.filter(seenKeys::add);
            Gen<TxnId> uniqIdGen = idGen.filter(seenTxn::add);
            try (KeyDeps.Builder builder = KeyDeps.builder())
            {
                for (int i = 0, numKeys = rs.nextInt(1, 10); i < numKeys; i++)
                {
                    builder.nextKey(uniqKeyGen.next(rs));
                    seenTxn.clear();
                    for (int j = 0, numTxn = rs.nextInt(1, 10); j < numTxn; j++)
                        builder.add(uniqIdGen.next(rs));
                }
                return builder.build();
            }
        };
    }

    public static Gen<Shard> shards(Gen<Range> rangeGen, Gen<List<Node.Id>> nodesGen)
    {
        return rs -> {
            Range range = rangeGen.next(rs);
            List<Node.Id> nodes = nodesGen.next(rs);
            int maxFailures = (nodes.size() - 1) / 2;
            Set<Node.Id> fastPath = new HashSet<>();
            if (maxFailures == 0)
            {
                fastPath.addAll(nodes);
            }
            else
            {
                int minElectorate = nodes.size() - maxFailures;
                for (int i = 0, size = rs.nextInt(minElectorate, nodes.size()); i < size; i++)
                {
                    //noinspection StatementWithEmptyBody
                    while (!fastPath.add(nodes.get(rs.nextInt(nodes.size()))));
                }
            }
            Set<Node.Id> joining = new HashSet<>();
            for (int i = 0, size = rs.nextInt(nodes.size()); i < size; i++)
            {
                //noinspection StatementWithEmptyBody
                while (!joining.add(nodes.get(rs.nextInt(nodes.size()))));
            }
            return new Shard(range, nodes, fastPath, joining);
        };
    }

    public static Gen<Topology> topologys()
    {
        return topologys(epochs(), nodes());
    }

    public static Gen<Topology> topologys(Gen.LongGen epochGen)
    {
        return topologys(epochGen, nodes());
    }

    public static Gen<Topology> topologys(Gen.LongGen epochGen, Gen<Node.Id> nodeGen)
    {
        return topologys(epochGen, nodeGen, AccordGens::prefixedIntHashKeyRanges);
    }

    public static Gen<Topology> topologys(Gen.LongGen epochGen, Gen<Node.Id> nodeGen, RangesGenFactory rangesGenFactory)
    {
        return rs -> {
            long epoch = epochGen.nextLong(rs);
            Node.Id[] nodes = Utils.toArray(Gens.lists(nodeGen).unique().ofSizeBetween(1, 10).next(rs), Node.Id[]::new);
            int rf = Math.min(nodes.length, 3);
            Ranges ranges = rangesGenFactory.apply(nodes.length, rf).next(rs);

            int numElectorate = nodes.length + rf - 1;
            List<WrapAroundList<Node.Id>> electorates = new ArrayList<>(numElectorate);
            for (int i = 0; i < numElectorate; i++)
                electorates.add(new WrapAroundList<>(nodes, i % nodes.length, (i + rf) % nodes.length));

            final List<Shard> shards = new ArrayList<>();
            Set<Node.Id> noShard = new HashSet<>(Arrays.asList(nodes));
            for (int i = 0; i < ranges.size() ; ++i)
            {
                WrapAroundList<Node.Id> replicas = electorates.get(i % electorates.size());
                Range range = ranges.get(i);
                shards.add(shards(ignore -> range, ignore -> replicas).next(rs));
                replicas.forEach(noShard::remove);
            }
            if (!noShard.isEmpty())
                throw new AssertionError(String.format("The following electorates were found without a shard: %s", noShard));

            return new Topology(epoch, toArray(shards, Shard[]::new));
        };
    }

    public interface RangesGenFactory
    {
        Gen<Ranges> apply(int nodeCount, int rf);
    }

    public interface RangeFactory
    {
        Range create(RandomSource rs, RoutingKey a, RoutingKey b);
    }

    public static Gen<Range> ranges(Gen<? extends RoutingKey> keyGen, BiFunction<? super RoutingKey, ? super RoutingKey, ? extends Range> factory)
    {
        return ranges(keyGen, (ignore, a, b) -> factory.apply(a, b));
    }

    public static Gen<Range> ranges(Gen<? extends RoutingKey> keyGen)
    {
        return ranges(keyGen, (rs, a, b) -> {
            boolean left = rs.nextBoolean();
            return Range.range(a, b, left, !left);
        });
    }

    public static Gen<Range> ranges(Gen<? extends RoutingKey> keyGen, RangeFactory factory)
    {
        RoutingKey[] keys = new RoutingKey[2];
        return rs -> {
            keys[0] = keyGen.next(rs);
            // range doesn't allow a=b
            do keys[1] = keyGen.next(rs);
            while (Objects.equals(keys[0], keys[1]));
            Arrays.sort(keys);
            return factory.create(rs, keys[0], keys[1]);
        };
    }

    public static Gen<Ranges> prefixedIntHashKeyRanges(int numNodes, int rf)
    {
        return rs -> {
            int numPrefixes = rs.nextInt(1, 10);
            List<Range> ranges = new ArrayList<>(numPrefixes * numNodes * 3);
            IntHashSet prefixes = new IntHashSet();
            for (int i = 0; i < numPrefixes; i++)
            {
                int prefix;
                //noinspection StatementWithEmptyBody
                while (!prefixes.add(prefix = rs.nextInt(0, 100)));
                ranges.addAll(Arrays.asList(PrefixedIntHashKey.ranges(prefix, rs.nextInt(Math.max(numNodes + 1, rf), numNodes * 3))));
            }
            ranges.sort(Range::compare);
            return Ranges.ofSortedAndDeoverlapped(Utils.toArray(ranges, Range[]::new));
        };
    }

    public static Gen<RangeDeps> rangeDeps(Gen<? extends Range> rangeGen)
    {
        return rangeDeps(rangeGen, txnIds());
    }

    public static Gen<RangeDeps> rangeDeps(Gen<? extends Range> rangeGen, Gen<TxnId> idGen)
    {
        double emptyProb = .2D;
        return rs -> {
            if (rs.decide(emptyProb)) return RangeDeps.NONE;
            RangeDeps.Builder builder = RangeDeps.builder();
            for (int i = 0, numKeys = rs.nextInt(1, 10); i < numKeys; i++)
            {
                builder.nextKey(rangeGen.next(rs));
                for (int j = 0, numTxn = rs.nextInt(1, 10); j < numTxn; j++)
                    builder.add(idGen.next(rs));
            }
            return builder.build();
        };
    }

    public static Gen<Deps> deps(Gen<KeyDeps> keyDepsGen, Gen<RangeDeps> rangeDepsGen)
    {
        return rs -> new Deps(keyDepsGen.next(rs), rangeDepsGen.next(rs));
    }
}
