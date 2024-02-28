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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.IntHashKey;
import accord.impl.IntKey;
import accord.impl.PrefixedIntHashKey;
import accord.local.Node;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
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

    public static Gen.IntGen flags()
    {
        return rs -> rs.nextInt(0, 1 << 16);
    }

    public static Gen<Timestamp> timestamps()
    {
        return timestamps(epochs()::nextLong, rs -> rs.nextLong(0, Long.MAX_VALUE), flags(), RandomSource::nextInt);
    }

    public static Gen<Timestamp> timestamps(Gen.LongGen epochs, Gen.LongGen hlcs, Gen.IntGen flags, Gen.IntGen nodes)
    {
        return rs -> Timestamp.fromValues(epochs.nextLong(rs), hlcs.nextLong(rs), flags.nextInt(rs), new Node.Id(nodes.nextInt(rs)));
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

    public static Gen<Ballot> ballot()
    {
        return ballot(epochs()::nextLong, rs -> rs.nextLong(0, Long.MAX_VALUE), flags(), RandomSource::nextInt);
    }

    public static Gen<Ballot> ballot(Gen.LongGen epochs, Gen.LongGen hlcs, Gen.IntGen flags, Gen.IntGen nodes)
    {
        return rs -> Ballot.fromValues(epochs.nextLong(rs), hlcs.nextLong(rs), flags.nextInt(rs), new Node.Id(nodes.nextInt(rs)));
    }

    public static Gen<Key> intKeys()
    {
        return rs -> new IntKey.Raw(rs.nextInt());
    }

    public static Gen<Key> intKeysInsideRanges(Ranges ranges)
    {
        return rs -> {
            Range range = ranges.get(rs.nextInt(0, ranges.size()));
            int start = intKey(range.start());
            int end = intKey(range.end());
            // end inclusive, so +1 the result to include end and exclude start
            return IntKey.key(rs.nextInt(start, end) + 1);
        };
    }

    public static Gen<Key> intKeysOutsideRanges(Ranges ranges)
    {
        return rs -> {
            for (int i = 0; i < 10; i++)
            {
                int index = rs.nextInt(0, ranges.size());
                Range first = index == 0 ? null : ranges.get(index - 1);
                Range second = ranges.get(index);
                // retries
                for (int j = 0; j < 10; j++)
                {
                    Key key = intKeyOutside(rs, first, second);
                    if (key == null)
                        continue;
                    return key;
                }
            }
            throw new AssertionError("Unable to find keys within the range " + ranges);
        };
    }

    private static Key intKeyOutside(RandomSource rs, @Nullable Range first, Range second)
    {
        int start;
        int end;
        if (first == null)
        {
            start = Integer.MIN_VALUE;
            end = intKey(second.start()); // start is not inclusive, so can use
        }
        else
        {
            start = intKey(first.end()) + 1; // end is inclusive, so +1 to skip
            end = intKey(second.start()); // start is not inclusive, so can use
        }
        if (start == end)
            return null;
        return IntKey.key(rs.nextInt(start, end + 1));
    }

    private static int intKey(RoutableKey key)
    {
        return ((IntKey.Routing) key).key;
    }

    public static Gen<IntKey.Routing> intRoutingKey()
    {
        return rs -> IntKey.routing(rs.nextInt());
    }

    public static Gen<Key> intHashKeys()
    {
        return rs -> IntHashKey.key(rs.nextInt());
    }

    public static Gen<Key> prefixedIntHashKey()
    {
        return prefixedIntHashKey(RandomSource::nextInt, rs -> rs.nextInt(PrefixedIntHashKey.MIN_KEY, Integer.MAX_VALUE));
    }

    public static Gen<Key> prefixedIntHashKey(Gen.IntGen prefixGen)
    {
        return prefixedIntHashKey(prefixGen, rs -> rs.nextInt(PrefixedIntHashKey.MIN_KEY, Integer.MAX_VALUE));
    }

    public static Gen<Key> prefixedIntHashKey(Gen.IntGen prefixGen, Gen.IntGen keyGen)
    {
        return rs -> {
            int prefix = prefixGen.nextInt(rs);
            int key = keyGen.nextInt(rs);
            int hash = PrefixedIntHashKey.hash(key);
            return PrefixedIntHashKey.key(prefix, key, hash);
        };
    }

    public static Gen<Key> prefixedIntHashKeyInsideRanges(Ranges ranges)
    {
        return rs -> {
            Range range = ranges.get(rs.nextInt(0, ranges.size()));
            PrefixedIntHashKey start = (PrefixedIntHashKey) range.start();
            PrefixedIntHashKey end = (PrefixedIntHashKey) range.end();
            // end inclusive, so +1 the result to include end and exclude start
            int hash = rs.nextInt(start.hash, end.hash) + 1;
            int key = CRCUtils.reverseCRC32LittleEnding(hash);
            PrefixedIntHashKey.Key ret = PrefixedIntHashKey.key(start.prefix, key, hash);
            // we have tests to make sure this doesn't fail... just a safety check
            assert ret.hash == hash;
            return ret;
        };
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
            float chance = rs.nextFloat();
            int rf;
            if (chance < 0.2f)      { rf = rs.nextInt(2, 9); }
            else if (chance < 0.4f) { rf = 3; }
            else if (chance < 0.7f) { rf = 5; }
            else if (chance < 0.8f) { rf = 7; }
            else                    { rf = 9; }
            Node.Id[] nodes = Utils.toArray(Gens.lists(nodeGen).unique().ofSizeBetween(rf, rf * 3).next(rs), Node.Id[]::new);
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

    public interface RangeFactory<T extends RoutingKey>
    {
        Range create(RandomSource rs, T a, T b);
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

    public static <T extends RoutingKey> Gen<Range> ranges(Gen<T> keyGen, RangeFactory<T> factory)
    {
        List<T> keys = Arrays.asList(null, null);
        return rs -> {
            keys.set(0, keyGen.next(rs));
            // range doesn't allow a=b
            do keys.set(1, keyGen.next(rs));
            while (Objects.equals(keys.get(0), keys.get(1)));
            keys.sort(Comparator.naturalOrder());
            return factory.create(rs, keys.get(0), keys.get(1));
        };
    }

    public static <T extends RoutingKey> Gen<Ranges> ranges(Gen.IntGen sizeGen, Gen<T> keyGen, RangeFactory<T> factory)
    {
        Gen<Range> rangeGen = ranges(keyGen, factory);
        return rs -> {
            int size = sizeGen.nextInt(rs);
            Range[] ranges = new Range[size];
            for (int i = 0; i < size; i++)
                ranges[i] = rangeGen.next(rs);
            return Ranges.of(ranges);
        };
    }

    public static <T extends RoutingKey> Gen<Ranges> ranges(Gen.IntGen sizeGen, Gen<T> keyGen, BiFunction<? super T, ? super T, ? extends Range> factory)
    {
        return ranges(sizeGen, keyGen, (ignore, a, b) -> factory.apply(a, b));
    }

    public static Gen<Ranges> prefixedIntHashKeyRanges(int numNodes, int rf)
    {
        return rs -> {
            int numPrefixes = rs.nextInt(1, 10);
            List<Range> ranges = new ArrayList<>(numPrefixes * numNodes * rf);
            IntHashSet prefixes = new IntHashSet();
            for (int i = 0; i < numPrefixes; i++)
            {
                int prefix;
                //noinspection StatementWithEmptyBody
                while (!prefixes.add(prefix = rs.nextInt(0, 100)));
                ranges.addAll(Arrays.asList(PrefixedIntHashKey.ranges(prefix, numNodes * rf)));
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
