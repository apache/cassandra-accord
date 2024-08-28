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
import java.util.function.ToIntBiFunction;

import javax.annotation.Nullable;

import com.google.common.collect.Iterators;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.IntHashKey;
import accord.impl.IntKey;
import accord.impl.PrefixedIntHashKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.RedundantBefore;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
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
import accord.utils.SortedArrays.SortedArrayList;
import org.agrona.collections.IntHashSet;

import static accord.utils.Utils.toArray;

public class AccordGens
{
    public static Gen.LongGen epochs()
    {
        return Gens.longs().between(0, Timestamp.MAX_EPOCH);
    }

    public static Gen.LongGen epochs(long min)
    {
        return Gens.longs().between(min, Timestamp.MAX_EPOCH);
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

    public static Gen.LongGen hlcs()
    {
        return rs -> rs.nextLong(0, Long.MAX_VALUE);
    }

    public static Gen.LongGen hlcs(long min)
    {
        return rs -> rs.nextLong(0, Long.MAX_VALUE);
    }

    public static Gen<Timestamp> timestamps()
    {
        return timestamps(epochs()::nextLong, hlcs(), flags(), RandomSource::nextInt);
    }

    public static Gen<Timestamp> timestamps(Gen.LongGen epochs, Gen.LongGen hlcs, Gen.IntGen flags, Gen.IntGen nodes)
    {
        return rs -> Timestamp.fromValues(epochs.nextLong(rs), hlcs.nextLong(rs), flags.nextInt(rs), new Node.Id(nodes.nextInt(rs)));
    }

    public static Gen<TxnId> txnIds()
    {
        return txnIds(Gens.enums().all(Txn.Kind.class));
    }

    public static Gen<TxnId> txnIds(Gen<Txn.Kind> kinds)
    {
        return txnIds(epochs(), hlcs(), RandomSource::nextInt, kinds);
    }

    public static Gen<TxnId> txnIds(Gen<Txn.Kind> kinds, Gen<Routable.Domain> domains)
    {
        return txnIds(epochs(), hlcs(), RandomSource::nextInt, kinds, domains);
    }

    public static Gen<TxnId> txnIds(Gen.LongGen epochs, Gen.LongGen hlcs, Gen.IntGen nodes)
    {
        return txnIds(epochs, hlcs, nodes, Gens.enums().all(Txn.Kind.class));
    }

    public static Gen<TxnId> txnIds(Gen.LongGen epochs, Gen.LongGen hlcs, Gen.IntGen nodes, Gen<Txn.Kind> kinds)
    {
        return txnIds(epochs, hlcs, nodes, kinds, Gens.enums().all(Routable.Domain.class));
    }

    public static Gen<TxnId> txnIds(Gen.LongGen epochs, Gen.LongGen hlcs, Gen.IntGen nodes, Gen<Txn.Kind> kinds, Gen<Routable.Domain> domains)
    {
        return rs -> new TxnId(epochs.nextLong(rs), hlcs.nextLong(rs), kinds.next(rs), domains.next(rs), new Node.Id(nodes.nextInt(rs)));
    }

    public static Gen<Ballot> ballot()
    {
        return ballot(epochs()::nextLong, hlcs(), flags(), RandomSource::nextInt);
    }

    public static Gen<Ballot> ballot(Gen.LongGen epochs, Gen.LongGen hlcs, Gen.IntGen flags, Gen.IntGen nodes)
    {
        return rs -> Ballot.fromValues(epochs.nextLong(rs), hlcs.nextLong(rs), flags.nextInt(rs), new Node.Id(nodes.nextInt(rs)));
    }

    public static Gen<Key> intKeys()
    {
        return intKeys(RandomSource::nextInt);
    }

    public static Gen<Key> intKeys(Gen.IntGen keyGen)
    {
        return rs -> new IntKey.Raw(keyGen.nextInt(rs));
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

    public static Gen<Key> keysInsideRanges(Ranges ranges)
    {
        Invariants.checkArgument(!ranges.isEmpty(), "Ranges empty");
        RoutingKey sample = ranges.get(0).end();
        if (sample instanceof PrefixedIntHashKey)
            return prefixedIntHashKeyInsideRanges(ranges);
        if (sample instanceof IntKey.Routing)
            return intKeysInsideRanges(ranges);
        throw new IllegalArgumentException("Unsupported key type " + sample.getClass() + "; supported = PrefixedIntHashKey, IntKey");
    }

    public static Gen<KeyDeps> keyDeps(Gen<? extends Key> keyGen)
    {
        Gen<Txn.Kind> kinds = Gens.pick(Txn.Kind.Write, Txn.Kind.Read);
        return keyDeps(keyGen, txnIds(kinds, ignore -> Routable.Domain.Key));
    }

    public static Gen<KeyDeps> directKeyDeps(Gen<? extends Key> keyGen)
    {
        Gen<Txn.Kind> kinds = Gens.pick(Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint);
        return keyDeps(keyGen, txnIds(kinds, ignore -> Routable.Domain.Key));
    }

    public static Gen<KeyDeps> keyDeps(Gen<? extends Key> keyGen, Gen<TxnId> idGen)
    {
        double emptyProb = .2D;
        return rs -> {
            if (rs.decide(emptyProb)) return KeyDeps.NONE;
            Set<Key> seenKeys = new HashSet<>();
            Set<TxnId> seenTxn = new HashSet<>();
            Gen<? extends Key> uniqKeyGen = keyGen.filter(42, null, seenKeys::add);
            Gen<TxnId> uniqIdGen = idGen.filter(42, null, seenTxn::add);
            try (KeyDeps.Builder builder = KeyDeps.builder())
            {
                for (int i = 0, numKeys = rs.nextInt(1, 10); i < numKeys; i++)
                {
                    Key next = uniqKeyGen.next(rs);
                    if (next == null)
                        break;
                    builder.nextKey(next);
                    seenTxn.clear();
                    for (int j = 0, numTxn = rs.nextInt(1, 10); j < numTxn; j++)
                    {
                        TxnId txnId = uniqIdGen.next(rs);
                        if (txnId == null)
                            break;
                        builder.add(txnId);
                    }
                }
                return builder.build();
            }
        };
    }

    public static Gen<Shard> shards(Gen<Range> rangeGen, Gen<SortedArrayList<Node.Id>> nodesGen)
    {
        return rs -> {
            Range range = rangeGen.next(rs);
            SortedArrayList<Node.Id> nodes = nodesGen.next(rs);
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
                SortedArrayList<Node.Id> sortedIds = SortedArrayList.copyUnsorted(replicas, Node.Id[]::new);
                Range range = ranges.get(i);
                shards.add(shards(ignore -> range, ignore -> sortedIds).next(rs));
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

    public static Gen<Range> rangeInsideRange(Range range)
    {
        if (range.end() instanceof PrefixedIntHashKey)
            return prefixedIntHashKeyRangeInsideRange(range);
        throw new IllegalArgumentException("Unsupported type: " + range.start().getClass());
    }

    public static Gen<Ranges> rangesInsideRanges(Ranges ranges, ToIntBiFunction<RandomSource, Range> numSplits)
    {
        if (ranges.isEmpty())
            return ignore -> ranges;
        List<Gen<Range>> subsets = new ArrayList<>(ranges.size());
        ranges.forEach(r -> subsets.add(rangeInsideRange(r)));
        return rs -> {
            List<Range> result = new ArrayList<>(subsets.size());
            for (int i = 0; i < subsets.size(); i++)
            {
                Range range = ranges.get(i);
                int splits = numSplits.applyAsInt(rs, range);
                if (splits < 0) throw new IllegalArgumentException("numSplits is less than 0: given " + splits);
                if (splits == 0) continue;
                Gen<Range> gen = subsets.get(i);
                for (int s = 0; s < splits; s++)
                    result.add(gen.next(rs));
            }
            return Ranges.of(result.toArray(Range[]::new));
        };
    }

    public static Gen<Range> prefixedIntHashKeyRangeInsideRange(Range range)
    {
        if (!(range.end() instanceof PrefixedIntHashKey))
            throw new IllegalArgumentException("Only PrefixedIntHashKey supported; saw " + range.end().getClass());
        PrefixedIntHashKey start = (PrefixedIntHashKey) range.start();
        PrefixedIntHashKey end = (PrefixedIntHashKey) range.end();
        if (start.hash + 1 == end.hash)
        {
            // range is of size 1, so can not split into a smaller range...
            return ignore -> range;
        }
        return rs -> {
            int a = rs.nextInt(start.hash, end.hash);
            int b = rs.nextInt(start.hash, end.hash);
            while (a == b)
                b = rs.nextInt(start.hash, end.hash);
            if (a > b)
            {
                int tmp = a;
                a = b;
                b = tmp;
            }
            return PrefixedIntHashKey.range(start.prefix, a, b);
        };
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
        Gen<Txn.Kind> kinds = Gens.pick(Txn.Kind.Write, Txn.Kind.Read, Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint);
        return rangeDeps(rangeGen, txnIds(kinds, ignore -> Routable.Domain.Range));
    }

    public static Gen<RangeDeps> rangeDeps(Gen<? extends Range> rangeGen, Gen<TxnId> idGen)
    {
        double emptyProb = .2D;
        return rs -> {
            if (rs.decide(emptyProb)) return RangeDeps.NONE;
            RangeDeps.Builder builder = RangeDeps.builder();
            List<? extends Range> uniqRanges = Gens.lists(rangeGen).uniqueBestEffort().ofSize(rs.nextInt(1, 10)).next(rs);
            for (Range range : uniqRanges)
            {
                builder.nextKey(range);
                for (int j = 0, numTxn = rs.nextInt(1, 10); j < numTxn; j++)
                    builder.add(idGen.next(rs));
            }
            return builder.build();
        };
    }

    public static Gen<Deps> deps(Gen<KeyDeps> keyDepsGen, Gen<RangeDeps> rangeDepsGen, Gen<KeyDeps> directKeyDepsGen)
    {
        return rs -> new Deps(keyDepsGen.next(rs), rangeDepsGen.next(rs), directKeyDepsGen.next(rs));
    }

    public static Gen<Deps> depsFromKey(Gen<? extends Key> keyGen, Gen<? extends Range> rangeGen, Gen<? extends Key> directKeyGen)
    {
        return deps(keyDeps(keyGen), rangeDeps(rangeGen), directKeyDeps(directKeyGen));
    }

    public static Gen<Deps> depsFromKey(Gen<? extends Key> keyGen, Gen<? extends Range> rangeGen)
    {
        return depsFromKey(keyGen, rangeGen, keyGen);
    }

    public static Gen<Deps> depsFor(TxnId txnId, Txn txn)
    {
        Gen<KeyDeps> keyDepsGen;
        Gen<RangeDeps> rangeDepsGen;
        Gen<KeyDeps> directKeyDepsGen;
        switch (txnId.kind())
        {
            case Write:
            case Read:
            case EphemeralRead:
            {
                Gen<? extends Key> keyGen = Gens.pick(Iterators.toArray(((Keys) txn.keys()).iterator(), Key.class));
                keyDepsGen = AccordGens.keyDeps(keyGen, AccordGens.txnIds(Gens.longs().between(0, txnId.epoch()),
                                                                          Gens.longs().between(0, txnId.hlc()),
                                                                          RandomSource::nextInt,
                                                                          Gens.pick(Txn.Kind.Write, Txn.Kind.Read),
                                                                          ignore -> Routable.Domain.Key));
                rangeDepsGen = i -> RangeDeps.NONE;
                directKeyDepsGen = i -> KeyDeps.NONE;
            }
            break;
            case ExclusiveSyncPoint:
            case SyncPoint:
            case LocalOnly:
                //TODO (coverage, now):
                keyDepsGen = i -> KeyDeps.NONE;
                rangeDepsGen = i -> RangeDeps.NONE;
                directKeyDepsGen = i -> KeyDeps.NONE;
                break;
            default:throw new UnsupportedOperationException(txn.kind().name());
        }
        return AccordGens.deps(keyDepsGen, rangeDepsGen, directKeyDepsGen);
    }

    public static Gen<Command.WaitingOn> waitingOn(Gen<Deps> depsGen, Gen<Boolean> emptyGen,
                                                   Gen<Boolean> rangeSetGen,
                                                   Gen<Boolean> directKeySetGen,
                                                   Gen<Boolean> keySetGen)
    {
        return rs -> {
            Deps deps = depsGen.next(rs);
            if (deps.isEmpty()) return Command.WaitingOn.empty(Routable.Domain.Key);
            if (emptyGen.next(rs)) return Command.WaitingOn.none(Routable.Domain.Key, deps);
            int size = deps.rangeDeps.txnIdCount() + deps.directKeyDeps.txnIdCount() + deps.keyDeps.keys().size();
            SimpleBitSet set = new SimpleBitSet(size);
            int directKeyOffset = deps.rangeDeps.txnIdCount();
            int keysOffset = directKeyOffset + deps.directKeyDeps.txnIdCount();
            for (int i = 0; i < size; i++)
            {
                Gen<Boolean> gen = i < directKeyOffset ? rangeSetGen :
                                   i < keysOffset ? directKeySetGen :
                                   keySetGen;
                if (gen.next(rs)) set.set(i);
            }
            return new Command.WaitingOn(deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps, new ImmutableBitSet(set), null);
        };
    }

    public static Gen<RedundantBefore> redundantBefore(Gen<Ranges> rangesGen,
                                                       BiFunction<RandomSource, Range, RedundantBefore.Entry> entryGen)
    {
        return rs -> {
            Ranges ranges = rangesGen.next(rs);
            if (ranges.isEmpty()) return RedundantBefore.EMPTY;
            RedundantBefore.Builder builder = new RedundantBefore.Builder(ranges.get(0).endInclusive(), ranges.size());
            ranges.forEach(r -> builder.append(r.start(), r.end(), entryGen.apply(rs, r)));
            return builder.build();
        };
    }
}
