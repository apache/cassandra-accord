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

package accord.primitives;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.IntKey;
import accord.local.Node;
import accord.primitives.Routable.Domain;

import static accord.primitives.Txn.Kind.Write;
import static org.assertj.core.api.Assertions.assertThat;

public class RangeDepsTest
{
    static class GenerateRanges
    {
        final int rangeDomain;
        final float minRangesDomain, maxRangesDomain, minRangesSpan, maxRangesSpan;

        GenerateRanges(int rangeDomain, float minRangesDomain, float maxRangesDomain, float minRangesSpan, float maxRangesSpan)
        {
            this.rangeDomain = rangeDomain;
            this.minRangesDomain = minRangesDomain;
            this.maxRangesDomain = maxRangesDomain;
            this.minRangesSpan = minRangesSpan;
            this.maxRangesSpan = maxRangesSpan;
        }

        private Range[] generateRanges(Random random, int rangeCount)
        {
            Range[] ranges = new Range[rangeCount];
            int txnDomain = Math.max(ranges.length, (int) (((random.nextFloat() * (maxRangesDomain - minRangesDomain)) + minRangesDomain) * rangeDomain));
            int txnSpan = Math.max(txnDomain, (int) (((random.nextFloat() * (maxRangesSpan - minRangesSpan)) + minRangesSpan) * rangeDomain));
            int gapSpan = txnSpan - txnDomain;
            int start = rangeDomain == txnSpan ? 0 : random.nextInt(rangeDomain - txnSpan);
            int end = start + txnSpan;
            int[] gaps = new int[ranges.length];
            int[] gapSpans = new int[ranges.length];
            for (int i = 0 ; i < ranges.length ; ++i)
            {
                gaps[i] = start + random.nextInt(end - start);
                gapSpans[i] = i == ranges.length - 1 || gapSpan <= 1 ? gapSpan : 1 + random.nextInt(Math.max(1, 2 * gapSpan/(ranges.length - i)));
            }
            Arrays.sort(gaps);
            for (int i = 0 ; i < ranges.length ; ++i)
            {
                end = Math.max(start + 1, gaps[i]);
                ranges[i] = r(start, end);
                start = end + gapSpans[i];
            }
            return ranges;
        }
    }

    static class Validate
    {
        final GenerateRanges generate;
        final Map<TxnId, Ranges> canonical;
        final RangeDeps test;

        Validate(GenerateRanges generate, Map<TxnId, Ranges> canonical, RangeDeps test)
        {
            this.generate = generate;
            this.canonical = canonical;
            this.test = test;
        }

        Set<TxnId> canonicalOverlaps(Range range)
        {
            Set<TxnId> set = new TreeSet<>();
            for (Map.Entry<TxnId, Ranges> e : canonical.entrySet())
            {
                if (e.getValue().intersects(range))
                    set.add(e.getKey());
            }
            return set;
        }

        Set<TxnId> testOverlaps(Range range)
        {
            List<TxnId> uniq = new ArrayList<>();
            test.forEachUniqueTxnId(range, uniq::add);
            Set<TxnId> set = new TreeSet<>();
            test.forEach(range, set::add);
            assertThat(uniq).doesNotHaveDuplicates()
                            .containsExactlyInAnyOrderElementsOf(set);
            return set;
        }

        Set<TxnId> canonicalSlice(Ranges ranges)
        {
            Set<TxnId> set = new TreeSet<>();
            for (Map.Entry<TxnId, Ranges> e : canonical.entrySet())
            {
                for (Range r : ranges)
                {
                    for (Range cR : e.getValue())
                    {
                        if (cR.intersection(r) != null)
                        {
                            set.add(e.getKey());
                            break;
                        }
                    }
                }
            }
            return set;
        }

        Set<TxnId> testSlice(Ranges ranges)
        {
            RangeDeps slice = test.slice(ranges);
            List<TxnId> uniq = new ArrayList<>();
            slice.forEachUniqueTxnId(ranges, uniq::add);
            Set<TxnId> set = new TreeSet<>();
            slice.forEach((AbstractRanges) ranges, null, (ignored, txnId) -> set.add(txnId));
            assertThat(uniq).doesNotHaveDuplicates()
                            .containsExactlyInAnyOrderElementsOf(set);
            return set;
        }

        Set<TxnId> canonicalOverlaps(RoutableKey key)
        {
            Set<TxnId> set = new TreeSet<>();
            for (Map.Entry<TxnId, Ranges> e : canonical.entrySet())
            {
                if (e.getValue().contains(key))
                    set.add(e.getKey());
            }
            return set;
        }

        Set<TxnId> testOverlaps(RoutableKey key)
        {
            List<TxnId> uniq = new ArrayList<>();
            test.forEachUniqueTxnId(key, uniq::add);
            Set<TxnId> set = new TreeSet<>();
            test.forEach(key, set::add);
            assertThat(uniq).doesNotHaveDuplicates()
                            .containsExactlyInAnyOrderElementsOf(set);
            return set;
        }

        void validate(Random random)
        {
            Assertions.assertEquals(canonical.size(), test.txnIdCount());
            Assertions.assertArrayEquals(canonical.keySet().stream().toArray(TxnId[]::new), test.txnIds);
            for (int i = 0 ; i < test.rangeCount() ; ++i)
            {
                Assertions.assertEquals(canonicalOverlaps(test.range(i)), testOverlaps(test.range(i)));
                Assertions.assertEquals(canonicalOverlaps(test.range(i).start()), testOverlaps(test.range(i).start()));
                Assertions.assertEquals(canonicalOverlaps(test.range(i).end()), testOverlaps(test.range(i).end()));
            }
            for (int i = 0 ; i < test.rangeCount() ; ++i)
            {
                Range range = generate.generateRanges(random, 1)[0];
                Assertions.assertEquals(canonicalOverlaps(range), testOverlaps(range));
                Assertions.assertEquals(canonicalOverlaps(range.start()), testOverlaps(range.start()));
                Assertions.assertEquals(canonicalOverlaps(range.end()), testOverlaps(range.end()));
            }
            for (int i = 0; i < 10; i++)
            {
                Ranges ranges = Ranges.of(generate.generateRanges(random, random.nextInt(10) + 1));
                Assertions.assertEquals(canonicalSlice(ranges), testSlice(ranges));
            }
        }
    }

    private static final boolean EXHAUSTIVE = false;

    // TODO (testing): generate ranges of different sizes at different ratios
    private static Validate generate(Random random, GenerateRanges generate, int txnIdCount, int rangeCount)
    {
        Map<TxnId, Ranges> map = new TreeMap<>();
        for (int txnId = 0; txnId < txnIdCount ; ++txnId)
        {
            int thisRangeCount = txnIdCount == 1 ? rangeCount : 1 + random.nextInt((2 * rangeCount / txnIdCount) - 1);
            Range[] ranges = generate.generateRanges(random, thisRangeCount);
            map.put(id(txnId), Ranges.of(ranges));
        }
        return new Validate(generate, map, RangeDeps.of(map));
    }

    private static Validate generateIdenticalTxns(Random random, GenerateRanges generate, int txnIdCount, int rangeCount)
    {
        Map<TxnId, Ranges> map = new TreeMap<>();
        Range[] ranges = generate.generateRanges(random, rangeCount / txnIdCount);
        for (int txnId = 0; txnId < txnIdCount ; ++txnId)
            map.put(id(txnId), Ranges.of(ranges));
        return new Validate(generate, map, RangeDeps.of(map));
    }

    private static Validate generateNemesisRanges(int width, int nemesisTxnIdCount, int rangeCount, int nonNemesisEntryPerNemesisEntry)
    {
        Map<Integer, List<Range>> build = new TreeMap<>();
        int nonNemesisTxnIdCount = nemesisTxnIdCount * nonNemesisEntryPerNemesisEntry;
        rangeCount /= (1 + nonNemesisEntryPerNemesisEntry);
        int rangeDomain = 0;
        for (int i = 0 ; i < rangeCount ; ++i)
        {
            build.computeIfAbsent(i % nemesisTxnIdCount, ignore -> new ArrayList<>()).add(r(i, i + width));
            for (int j = 0 ; j < nonNemesisEntryPerNemesisEntry ; ++j)
                build.computeIfAbsent(nemesisTxnIdCount + (i % nonNemesisTxnIdCount), ignore -> new ArrayList<>()).add(r(i, i + 1));
            rangeDomain = i + width;
        }
        Map<TxnId, Ranges> result = new TreeMap<>();
        for (int txnId = 0; txnId < nemesisTxnIdCount + nonNemesisTxnIdCount ; ++txnId)
            result.put(id(txnId), Ranges.of(build.get(txnId).toArray(new Range[0])));
        return new Validate(new GenerateRanges(rangeDomain, 0f, 1f, 0f, 1f), result, RangeDeps.of(result));
    }


    // TODO (testing): broaden patterns of random contents, sizes of collection etc.
    @Test
    public void testRandom()
    {
        Random random = new Random();
//        long[] seeds = new long[]{-6268194734307361517L, -1531261279965735959L};
//        Validate[] validates = new Validate[seeds.length];
        for (int i = 0 ; i < 1000 ; ++i)
        {
            long seed = random.nextLong();
//            long seed = 1953755836248097851L;
            System.out.println("Seed: " + seed);
            random.setSeed(seed);
            generate(random, new GenerateRanges(1000, 0.01f, 0.3f, 0.1f, 1f), 100, 1000)
                    .validate(random);
        }
    }

    @Test
    public void testIdenticalTransactions()
    {
        Random random = new Random();
        for (int copies = 1 ; copies < 500 ; ++copies)
        {
            long seed = random.nextLong();
//            long seed = -4951029115911714505L;
            System.out.println("Seed: " + seed + ", copies: " + copies);
            random.setSeed(seed);
            generateIdenticalTxns(random, new GenerateRanges(1000, 0.01f, 0.3f, 0.1f, 1f), copies, 1000)
                    .validate(random);
        }
    }

    @Test
    public void testNemesisRanges()
    {
        testNemesisRanges(0);
    }

    @Test
    public void testHalfNemesisRanges()
    {
        testNemesisRanges(1);
    }

    private static void testNemesisRanges(int nonNemesisEntryPerNemesisEntry)
    {
        Random random = new Random();
        long seed = random.nextLong();
//        long seed = 2005526220972215410L;
        System.out.println("Seed: " + seed);
        random.setSeed(seed);

        if (EXHAUSTIVE)
        {
            for (int width = 1 ; width < 512 ; ++width)
            {
                for (int txnIdCount = 1; txnIdCount < 100 ; txnIdCount *= 2)
                {
                    // TODO (testing): verify that we write only a small number of checkpoints
                    generateNemesisRanges(width, txnIdCount, 1000, nonNemesisEntryPerNemesisEntry)
                            .validate(random);
                }
            }
        }
        else
        {
            for (int i = 0 ; i < 1000 ; ++i)
            {
                generateNemesisRanges(1 + random.nextInt(511), 1 + random.nextInt(99), 1000, nonNemesisEntryPerNemesisEntry)
                        .validate(random);
            }
        }
    }

    @Test
    public void testUnionFlags()
    {
        Range[] ranges = { r(0, 1), r(2, 3) };
        TxnId[] txnIds = {
            new TxnId(1, 1, 0, Txn.Kind.ExclusiveSyncPoint, Domain.Range, new Node.Id(1)),
            new TxnId(1, 2, 0, Txn.Kind.ExclusiveSyncPoint, Domain.Range, new Node.Id(1))
        };
        int[] flags = { 0, Timestamp.Flag.SHARD_BOUND.bit, Timestamp.Flag.HLC_BOUND.bit, Timestamp.Flag.UNSTABLE.bit };
        int combinations = 1 << (ranges.length * txnIds.length);
        for (int leftMask = 0 ; leftMask < combinations ; ++leftMask)
        for (int rightMask = 0 ; rightMask < combinations ; ++rightMask)
        for (int leftLayout = 0 ; leftLayout < 3 ; ++leftLayout)
        for (int rightLayout = 0 ; rightLayout < 3 ; ++rightLayout)
        for (int leftFlags : flags)
        for (int rightFlags : flags)
        {
            RangeDeps left = flaggedDeps(ranges, txnIds, leftMask, leftFlags, leftLayout);
            RangeDeps right = flaggedDeps(ranges, txnIds, rightMask, rightFlags, rightLayout);
            RangeDeps result = left.with(right);
            assertUnion(ranges, txnIds, leftMask, leftFlags, rightMask, rightFlags, result);

            // Use the same thread's buffers again while retaining the result and both inputs.
            flaggedDeps(ranges, txnIds, combinations - 1, 0, 0)
                    .with(flaggedDeps(ranges, txnIds, combinations - 1, Timestamp.Flag.UNSTABLE.bit, 1));
            assertUnion(ranges, txnIds, leftMask, leftFlags, rightMask, rightFlags, result);
            assertUnion(ranges, txnIds, leftMask, leftFlags, 0, 0, left);
            assertUnion(ranges, txnIds, rightMask, rightFlags, 0, 0, right);
        }
    }

    @Test
    public void testMergeFlags()
    {
        Range[] ranges = { r(0, 1), r(2, 3) };
        TxnId[] txnIds = { new TxnId(1, 1, 0, Txn.Kind.ExclusiveSyncPoint, Domain.Range, new Node.Id(1)) };
        for (int leftMask = 1 ; leftMask < (1 << ranges.length) ; ++leftMask)
        for (int rightMask = 1 ; rightMask < (1 << ranges.length) ; ++rightMask)
        {
            RangeDeps left = flaggedDeps(ranges, txnIds, leftMask, Timestamp.Flag.SHARD_BOUND.bit, 0);
            RangeDeps right = flaggedDeps(ranges, txnIds, rightMask, Timestamp.Flag.HLC_BOUND.bit, 0);
            RangeDeps result = RangeDeps.merge(java.util.stream.Stream.of(left, right, left));
            assertUnion(ranges, txnIds, leftMask, Timestamp.Flag.SHARD_BOUND.bit, rightMask, Timestamp.Flag.HLC_BOUND.bit, result);

            RangeDeps superset = flaggedDeps(ranges, txnIds, leftMask | rightMask,
                                            Timestamp.Flag.SHARD_BOUND.bit | Timestamp.Flag.HLC_BOUND.bit, 0);
            if ((leftMask | rightMask) != leftMask)
            {
                Assertions.assertSame(superset, RangeDeps.merge(java.util.stream.Stream.of(left, superset, left)));
                List<RangeDeps> inputs = Arrays.asList(left, superset, left);
                Assertions.assertSame(superset, RangeDeps.merge(inputs, inputs.size(), List::get, d -> d, d -> d));
            }
        }
    }

    private static RangeDeps flaggedDeps(Range[] ranges, TxnId[] txnIds, int mask, int flags, int layout)
    {
        RangeDeps result;
        try (RangeDeps.AbstractRangeBuilder<?, ?> builder = layout == 1 ? RangeDeps.byTxnIdBuilder() : RangeDeps.builderByRange())
        {
            // Group by the builder's key so each key is visited only once.
            for (int key = 0 ; key < (layout == 1 ? txnIds.length : ranges.length) ; ++key)
            for (int value = 0 ; value < (layout == 1 ? ranges.length : txnIds.length) ; ++value)
            {
                int range = layout == 1 ? value : key;
                int txn = layout == 1 ? key : value;
                if ((mask & (1 << (range * txnIds.length + txn))) != 0)
                    builder.add(ranges[range], txnIds[txn].addFlags(flags));
            }
            result = builder.build();
        }
        if (layout == 2)
            RangeDeps.SerializerSupport.txnIdsToRanges(result);
        return result;
    }

    private static void assertUnion(Range[] ranges, TxnId[] txnIds, int leftMask, int leftFlags, int rightMask, int rightFlags, RangeDeps actual)
    {
        Map<TxnId, Set<Range>> expected = new TreeMap<>();
        for (int r = 0 ; r < ranges.length ; ++r)
        for (int t = 0 ; t < txnIds.length ; ++t)
        {
            if (((leftMask | rightMask) & (1 << (r * txnIds.length + t))) != 0)
                expected.computeIfAbsent(txnIds[t], ignore -> new TreeSet<>(Range::compare)).add(ranges[r]);
        }
        Map<TxnId, Set<Range>> found = new TreeMap<>();
        actual.forEach(e -> found.computeIfAbsent(e.getValue(), ignore -> new TreeSet<>(Range::compare)).add(e.getKey()));
        Assertions.assertEquals(expected, found);
        Assertions.assertEquals(expected.size(), actual.txnIdsWithFlags().size());
        for (TxnId stored : actual.txnIdsWithFlags())
        {
            int expectedFlags = stored.withoutNonIdentityFlags().flags();
            for (int r = 0 ; r < ranges.length ; ++r)
            for (int t = 0 ; t < txnIds.length ; ++t)
            {
                if (!txnIds[t].equals(stored)) continue;
                int bit = 1 << (r * txnIds.length + t);
                if ((leftMask & bit) != 0) expectedFlags |= leftFlags;
                if ((rightMask & bit) != 0) expectedFlags |= rightFlags;
            }
            Assertions.assertEquals(expectedFlags, stored.flags(), "Flags for " + stored + " in union of " + leftMask + " and " + rightMask);
        }
    }

    private static TxnId id(int i)
    {
        return new TxnId(1, i, 0, Write, Domain.Key, new Node.Id(1));
    }

    private static Range r(int i, int j)
    {
        return new IntKey.Range(new IntKey.Routing(i), new IntKey.Routing(j));
    }
}
