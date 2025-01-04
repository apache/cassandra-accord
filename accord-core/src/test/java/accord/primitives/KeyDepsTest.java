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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import accord.api.RoutingKey;
import accord.impl.IntHashKey.Hash;
import accord.primitives.KeyDeps.Builder;
import accord.utils.AccordGens;
import accord.utils.DefaultRandom;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.RelationMultiMap.Entry;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.impl.IntHashKey;
import accord.local.Node.Id;

import static accord.utils.Gens.lists;
import static accord.utils.Property.qt;
import static accord.utils.Utils.toArray;
import static org.assertj.core.api.Assertions.assertThat;

// TODO (expected, testing): test Keys with no contents, "without", "with" where TxnId and Keys are the same, but Key -> [TxnId] does not match;
//  ensure high code coverage
public class KeyDepsTest
{
    private static final Logger logger = LoggerFactory.getLogger(KeyDepsTest.class);

    @Test
    public void testFoldEachKey()
    {
        Gen<Gen<? extends RoutingKey>> uniqueKeys = Gens.lists(AccordGens.intRoutingKey()).uniqueBestEffort().ofSizeBetween(1, 10).map(l -> Gens.pick(l));
        Gen<Gen<? extends RoutingKey>> keyDistro = rs -> rs.nextBoolean() ? uniqueKeys.next(rs) : AccordGens.intRoutingKey();
        Gen<Gen<TxnId>> uniqueTxnIds = Gens.lists(AccordGens.txnIds()).uniqueBestEffort().ofSizeBetween(1, 10).map(l -> Gens.pick(l));
        Gen<Gen<TxnId>> txnIdDistro = rs -> rs.nextBoolean() ? uniqueTxnIds.next(rs) : AccordGens.txnIds();
        qt().check(rs -> {
            Gen<KeyDeps> gen = AccordGens.keyDeps(keyDistro.next(rs), txnIdDistro.next(rs));
            KeyDeps keyDeps = gen.next(rs);
            Map<TxnId, List<RoutingKey>> reverseLookup = new HashMap<>();
            for (RoutingKey key : keyDeps.keys)
            {
                for (TxnId id : keyDeps.txnIds(key))
                    reverseLookup.computeIfAbsent(id, ignore -> new ArrayList<>()).add(key);
            }
            for (RoutingKey key : keyDeps.keys)
            {
                keyDeps.forEach(key.asRange(), null, null, null, (i1, i2, i3, index) -> {
                    List<RoutingKey> expected = reverseLookup.get(keyDeps.txnId(index));
                    List<RoutingKey> matches = keyDeps.foldEachKey(index, null, new ArrayList<>(), (ignore, k, accum) -> {
                        accum.add(k);
                        return accum;
                    });
                    assertThat(matches).isEqualTo(expected);
                });
            }
        });
    }

    @Test
    public void testRandom()
    {
        testOneRandom(seed(), 1000, 3, 500, 4, 100, 10, 200, 1000);
        testOneRandom(seed(), 1000, 3, 500, 4, 10, 2, 200, 1000);
        testOneRandom(seed(), 100, 3, 500, 4, 10, 2, 200, 100);
    }

    @Test
    public void testMerge()
    {
        testMerge(seed(), 100, 3, 500, 4, 10, 5, 200, 100, 10);
        testMerge(seed(), 1000, 3, 500, 4, 100, 10, 200, 1000, 10);
    }

    private static void testMerge(long seed, int uniqueTxnIdsRange, int epochRange, int hlcRange, int nodeRange,
                                 int uniqueKeysRange, int emptyKeysRange, int keyRange, int totalCountRange, int mergeCountRange)
    {
        RandomSource random = random(seed);
        Supplier<Deps> supplier = supplier(random, uniqueTxnIdsRange, epochRange, hlcRange, 0, nodeRange,
                                           uniqueKeysRange, emptyKeysRange, keyRange, totalCountRange);
        int count = 1 + random.nextInt(mergeCountRange);
        List<Deps> deps = new ArrayList<>(count);
        while (count-- > 0)
            deps.add(supplier.get());
        testOneDeps(random, KeyDepsTest.Deps.merge(deps), 200);
    }

    @Test
    public void testWith()
    {
        testWith(seed(), 1000, 3, 500, 4, 100, 10, 200, 1000, 10);
    }

    private static void testWith(long seed, int uniqueTxnIdsRange, int epochRange, int hlcRange, int nodeRange,
                                 int uniqueKeysRange, int emptyKeysRange, int keyRange, int totalCountRange, int mergeCountRange)
    {
        RandomSource random = random(seed);
        Supplier<Deps> supplier = supplier(random, uniqueTxnIdsRange, epochRange, hlcRange, 0, nodeRange,
                                           uniqueKeysRange, emptyKeysRange, keyRange, totalCountRange);
        Deps cur = supplier.get();
        int count = 1 + random.nextInt(mergeCountRange);
        while (count-- > 0)
        {
            cur = cur.with(supplier.get());
            testOneDeps(random, cur, 200);
        }
    }

    @Test
    public void testWithout()
    {
        qt().forAll(Deps::generate).check(deps -> {
            // no matches
            Assertions.assertSame(deps.test, deps.test.without(KeyDeps.NONE));
            // all match
            Assertions.assertSame(accord.primitives.KeyDeps.NONE, deps.test.without(deps.test));

            // remove specific key
            for (RoutingKey key : deps.test.keys())
            {
                RoutingKeys withoutKeys = RoutingKeys.of(key);
                RoutingKeys expectedKeys = (RoutingKeys) deps.test.keys.without(withoutKeys);
                accord.primitives.KeyDeps without = deps.test.without(deps.test.intersecting(expectedKeys));
                Assertions.assertEquals(expectedKeys, without.keys());

                // all other keys are fine?
                for (RoutingKey otherKey : expectedKeys)
                    Assertions.assertEquals(deps.test.txnIds(key), without.txnIds(key), "txnIds(" + otherKey + ")");

                // check each TxnId
                for (TxnId txnId : deps.test.txnIds())
                {
                    RoutingKeys expected = (RoutingKeys) deps.test.participatingKeys(txnId).without(withoutKeys);
                    Assertions.assertEquals(expected, get(without, key), () -> "TxnId " + txnId + " is expected to be removed for key " + key);
                }
            }
        });
    }

    private static List<TxnId> get(accord.primitives.KeyDeps deps, RoutingKey key)
    {
        List<TxnId> ids = new ArrayList<>();
        deps.forEach(key, (id, i) -> ids.add(id));
        return ids;
    }

    @Test
    public void testIterator()
    {
        qt().forAll(Deps::generate).check(deps -> {
            try (Builder builder = accord.primitives.KeyDeps.builder())
            {
                for (Map.Entry<RoutingKey, TxnId> e : deps.test)
                    builder.add(e.getKey(), e.getValue());
                Assertions.assertEquals(deps.test, builder.build());
            }
        });
    }

    @Test
    public void testForEachOnUniqueEndInclusive()
    {
        qt().forAll(Gen.of(Deps::generate).filter(d -> d.test.keys().size() >= 2)).check(deps -> {
            RoutingKeys keys = deps.test.keys();
            RoutingKey start = keys.get(0);
            RoutingKey end = keys.get(keys.size() - 1);
            if (start.equals(end))
                throw new AssertionError(start + " == " + end);

            TreeSet<TxnId> seen = new TreeSet<>();
            deps.test.forEachUniqueTxnId(Ranges.of(Range.range(start.toUnseekable(), end.toUnseekable(), false, true)), txnId -> {
                if (!seen.add(txnId))
                    throw new AssertionError("Seen " + txnId + " multiple times");
            });
            Set<TxnId> notExpected = deps.canonical.get(start);
            for (int i = 1; i < keys.size(); i++)
            {
                Set<TxnId> ids = deps.canonical.get(keys.get(i));
                notExpected = Sets.difference(notExpected, ids);
            }
            TreeSet<TxnId> expected = new TreeSet<>(Sets.difference(deps.invertCanonical().keySet(), notExpected));
            Assertions.assertEquals(expected, seen);
        });
    }

    @Test
    public void testForEachOnUniqueStartInclusive()
    {
        qt().forAll(Gen.of(Deps::generate).filter(d -> d.test.keys().size() >= 2)).check(deps -> {
            RoutingKeys keys = deps.test.keys();
            RoutingKey start = keys.get(0);
            RoutingKey end = keys.get(keys.size() - 1);

            TreeSet<TxnId> seen = new TreeSet<>();
            deps.test.forEachUniqueTxnId(Ranges.of(Range.range(start.toUnseekable(), end.toUnseekable(), true, false)), txnId -> {
                if (!seen.add(txnId))
                    throw new AssertionError("Seen " + txnId + " multiple times");
            });
            Set<TxnId> notExpected = deps.canonical.get(end);
            for (int i = 0; i < keys.size() - 1; i++)
            {
                Set<TxnId> ids = deps.canonical.get(keys.get(i));
                notExpected = Sets.difference(notExpected, ids);
            }
            TreeSet<TxnId> expected = new TreeSet<>(Sets.difference(deps.invertCanonical().keySet(), notExpected));
            Assertions.assertEquals(expected, seen);
        });
    }

    @Test
    public void testForEachOnUniqueNoMatch()
    {
        qt().forAll(Gen.of(Deps::generate).filter(d -> d.test.keys().size() >= 2)).check(deps -> {
            RoutingKeys keys = deps.test.keys();
            Hash start = IntHashKey.forHash(Integer.MIN_VALUE);
            RoutingKey end = keys.get(0);

            TreeSet<TxnId> seen = new TreeSet<>();
            deps.test.forEachUniqueTxnId(Ranges.of(Range.range(start.toUnseekable(), end.toUnseekable(), true, false)), txnId -> {
                if (!seen.add(txnId))
                    throw new AssertionError("Seen " + txnId + " multiple times");
            });
            Assertions.assertEquals(Collections.emptySet(), seen);
        });
    }

    @Test
    public void testMergeFull()
    {
        qt().forAll(lists(Deps::generate).ofSizeBetween(0, 20)).check(KeyDepsTest::testMergedProperty);
    }

    @Test
    public void testMergeFullConcurrent()
    {
        // pure=false due to shared buffer pool
        Runnable test = () -> qt().withPure(false).forAll(lists(Deps::generate).ofSizeBetween(0, 20)).check(KeyDepsTest::testMergedProperty);
        int numThreads = Runtime.getRuntime().availableProcessors() * 4;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++)
            futures.add(executor.submit(test));
        for (Future<?> f : futures)
        {
            try {
                f.get();
            } catch (InterruptedException e) {
                executor.shutdownNow();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                executor.shutdownNow();
                Throwable cause = e.getCause();
                if (!(cause instanceof InterruptedException))
                    throw new RuntimeException(cause);
            }
        }
        executor.shutdown();
    }

    private static void testMergedProperty(List<Deps> list)
    {
        Deps expected = Deps.merge(list);
        expected.testSimpleEquality();

        // slightly redundant due to Deps.merge using this method... it is here for completeness
        Assertions.assertEquals(expected.test, accord.primitives.KeyDeps.merge(list, list.size(), List::get, d -> d.test, Function.identity()));
        Assertions.assertEquals(expected.test, list.stream().map(a -> a.test).reduce(accord.primitives.KeyDeps.NONE, accord.primitives.KeyDeps::with));
    }

    @Test
    public void builder()
    {
        qt().forAll(Deps::generate, Gens.random()).check((deps, random) -> {
            try (Builder builder = accord.primitives.KeyDeps.builder())
            {
                for (RoutingKey key : deps.canonical.keySet())
                {
                    builder.nextKey(key);
                    List<TxnId> ids = new ArrayList<>(deps.canonical.get(key));
                    Collections.shuffle(ids, random.asJdkRandom());
                    ids.forEach(builder::add);
                }

                Assertions.assertEquals(deps.test, builder.build());
            }
        });
    }

    static class Deps
    {
        final Map<RoutingKey, NavigableSet<TxnId>> canonical;
        final accord.primitives.KeyDeps test;

        Deps(Map<RoutingKey, NavigableSet<TxnId>> canonical, accord.primitives.KeyDeps test)
        {
            this.canonical = canonical;
            this.test = test;
        }

        static Deps generate(RandomSource random)
        {
            int epochRange = 3;
            int hlcRange = 500;
            double uniqueTxnIdsPercentage = 0.66D;
            int uniqueTxnIds = random.nextInt(1, (int) ((hlcRange * epochRange) * uniqueTxnIdsPercentage));

            int nodeRange = random.nextInt(1, 4);
            int uniqueKeys = random.nextInt(2, 200);
            int emptyKeys = random.nextInt(0, 10);
            int keyRange = random.nextInt(uniqueKeys + emptyKeys, 400);
            int totalCount = random.nextInt(1, 1000);
            Deps deps = generate(random, uniqueTxnIds, epochRange, hlcRange, 0, nodeRange, uniqueKeys, emptyKeys, keyRange, totalCount);
            deps.testSimpleEquality();
            return deps;
        }

        static Deps generate(RandomSource random, int uniqueTxnIds, int epochRange, int hlcRange, int flagsRange, int nodeRange,
                             int uniqueKeys, int emptyKeys, int keyRange, int totalCount)
        {
            // populateKeys is a subset of keys
            RoutingKeys populateKeys, keys;
            {
                TreeSet<RoutingKey> tmp = new TreeSet<>();
                while (tmp.size() < uniqueKeys)
                    tmp.add(IntHashKey.key(random.nextInt(keyRange)).toUnseekable());
                populateKeys = new RoutingKeys(tmp.toArray(RoutingKey[]::new));
                while (tmp.size() < uniqueKeys + emptyKeys)
                    tmp.add(IntHashKey.key(random.nextInt(keyRange)).toUnseekable());
                keys = new RoutingKeys(tmp.toArray(RoutingKey[]::new));
            }

            List<TxnId> txnIds; {
                TreeSet<TxnId> tmp = new TreeSet<>();
                while (tmp.size() < uniqueTxnIds)
                    tmp.add(TxnId.fromValues(random.nextInt(epochRange), random.nextInt(hlcRange), flagsRange == 0 ? 0 : random.nextInt(flagsRange), new Id(random.nextInt(nodeRange))));
                txnIds = new ArrayList<>(tmp);
            }

            TreeMap<RoutingKey, NavigableSet<TxnId>> canonical = new TreeMap<>();
            for (int i = 0 ; i < totalCount ; ++i)
            {
                RoutingKey key = populateKeys.get(random.nextInt(uniqueKeys));
                TxnId txnId = txnIds.get(random.nextInt(uniqueTxnIds));
                canonical.computeIfAbsent(key, ignore -> new TreeSet<>()).add(txnId);
            }

            boolean inOrderKeys = random.nextBoolean();
            boolean inOrderValues = random.nextBoolean();
            try (Builder builder = accord.primitives.KeyDeps.builder())
            {
                (inOrderKeys ? canonical : canonical.descendingMap()).forEach((key, ids) -> {
                    builder.nextKey(key);
                    (inOrderValues ? ids : ids.descendingSet()).forEach(builder::add);
                });

                accord.primitives.KeyDeps test = builder.build();
                return new Deps(canonical, test);
            }
        }

        Deps select(Ranges ranges)
        {
            Map<RoutingKey, NavigableSet<TxnId>> canonical = new TreeMap<>();
            for (Map.Entry<RoutingKey, NavigableSet<TxnId>> e : this.canonical.entrySet())
            {
                if (ranges.contains(e.getKey()))
                    canonical.put(e.getKey(), e.getValue());
            }

            return new Deps(canonical, test.slice(ranges));
        }

        Deps with(Deps that)
        {
            Map<RoutingKey, NavigableSet<TxnId>> canonical = new TreeMap<>();
            for (Map.Entry<RoutingKey, NavigableSet<TxnId>> e : this.canonical.entrySet())
                canonical.computeIfAbsent(e.getKey(), ignore -> new TreeSet<>()).addAll(e.getValue());
            for (Map.Entry<RoutingKey, NavigableSet<TxnId>> e : that.canonical.entrySet())
                canonical.computeIfAbsent(e.getKey(), ignore -> new TreeSet<>()).addAll(e.getValue());

            return new Deps(canonical, test.with(that.test));
        }

        void testSimpleEquality()
        {
            Assertions.assertArrayEquals(canonical.keySet().toArray(new RoutingKey[0]), test.keys().stream().toArray(RoutingKey[]::new));
            for (Map.Entry<RoutingKey, NavigableSet<TxnId>> e : canonical.entrySet())
            {
                List<TxnId> canonical = new ArrayList<>(e.getValue());
                List<TxnId> test = new ArrayList<>();
                this.test.forEach(e.getKey(), (id, i) -> test.add(id));
                Assertions.assertEquals(canonical, test);
            }

            TreeMap<TxnId, List<RoutingKey>> canonicalInverted = invertCanonical();
            Assertions.assertArrayEquals(toArray(canonicalInverted.keySet(), TxnId[]::new),
                                         IntStream.range(0, test.txnIdCount()).mapToObj(test::txnId).toArray(TxnId[]::new));
            for (Map.Entry<TxnId, List<RoutingKey>> e : canonicalInverted.entrySet())
            {
                Assertions.assertArrayEquals(toArray(e.getValue(), RoutingKey[]::new),
                                             test.participatingKeys(e.getKey()).stream().toArray(RoutingKey[]::new));
            }

            StringBuilder builder = new StringBuilder();
            builder.append("{");
            for (Map.Entry<RoutingKey, NavigableSet<TxnId>> e : canonical.entrySet())
            {
                if (builder.length() > 1)
                    builder.append(", ");
                builder.append(e.getKey());
                builder.append(":");
                builder.append(e.getValue());
            }
            builder.append("}");
            Assertions.assertEquals(builder.toString(), test.toString());
        }

        TreeMap<TxnId, List<RoutingKey>> invertCanonical()
        {
            TreeMap<TxnId, List<RoutingKey>> result = new TreeMap<>();
            for (Map.Entry<RoutingKey, NavigableSet<TxnId>> e : canonical.entrySet())
            {
                e.getValue().forEach(txnId -> result.computeIfAbsent(txnId, ignore -> new ArrayList<>())
                                                    .add(e.getKey()));

            }
            return result;
        }

        static Deps merge(List<Deps> deps)
        {
            Map<RoutingKey, NavigableSet<TxnId>> canonical = new TreeMap<>();
            for (Deps that : deps)
            {
                for (Map.Entry<RoutingKey, NavigableSet<TxnId>> e : that.canonical.entrySet())
                    canonical.computeIfAbsent(e.getKey(), ignore -> new TreeSet<>()).addAll(e.getValue());
            }

            return new Deps(canonical, accord.primitives.KeyDeps.merge(deps, deps.size(), List::get, d -> d.test, Function.identity()));
        }
    }

    private static Ranges randomKeyRanges(RandomSource random, int countRange, int valueRange)
    {
        int count = countRange == 1 ? 1 : 1 + random.nextInt(countRange - 1);
        Hash[] hashes;
        {
            TreeSet<Hash> tmp = new TreeSet<>();
            while (tmp.size() < count * 2)
            {
                Hash key = IntHashKey.forHash(random.nextInt());
                tmp.add(key);
            }
            hashes = toArray(tmp, Hash[]::new);
        }
        Range[] ranges = new Range[count];
        for (int i = 0 ; i < count ; i++)
            ranges[i] = IntHashKey.range(hashes[i * 2], hashes[i * 2 + 1]);
        return Ranges.ofSortedAndDeoverlapped(ranges);
    }

    private static void testOneRandom(long seed, int uniqueTxnIds, int epochRange, int hlcRange, int nodeRange,
                                      int uniqueKeys, int emptyKeys, int keyRange, int totalCountRange)
    {
        RandomSource random = random(seed);
        int totalCount = 1 + random.nextInt(totalCountRange - 1);
        testOneDeps(random,
                    KeyDepsTest.Deps.generate(random, uniqueTxnIds, epochRange, hlcRange, 0, nodeRange, uniqueKeys, emptyKeys, keyRange, totalCount),
                    keyRange);
    }

    private static Supplier<Deps> supplier(RandomSource random, int uniqueTxnIdsRange, int epochRange, int hlcRange, int flagRange, int nodeRange,
                                           int uniqueKeysRange, int emptyKeysRange, int keyRange, int totalCountRange)
    {
        return () -> {
            if (random.nextInt(100) == 0)
                return new Deps(new TreeMap<>(), accord.primitives.KeyDeps.NONE);

            int uniqueTxnIds = 1 + random.nextInt(uniqueTxnIdsRange - 1);
            int uniqueKeys = 1 + random.nextInt(uniqueKeysRange - 1);
            int emptyKeys = 1 + random.nextInt(emptyKeysRange - 1);
            int totalCount = random.nextInt(Math.min(totalCountRange, uniqueKeys * uniqueTxnIds));
            return KeyDepsTest.Deps.generate(random, uniqueTxnIds,
                                                  epochRange, hlcRange, flagRange, nodeRange,
                                                  uniqueKeys, emptyKeys, keyRange, totalCount);
        };
    }

    private static void testOneDeps(RandomSource random, Deps deps, int keyRange)
    {
        deps.testSimpleEquality();
        {
            Deps nestedSelect = null;
            // generate some random Ranges, and slice using them
            for (int i = 0, count = 1 + random.nextInt(4); i < count ; ++i)
            {
                Ranges ranges = randomKeyRanges(random, 1 + random.nextInt(5), keyRange);

                {   // test forEach(key, txnId)
                    List<Entry<RoutingKey, TxnId>> canonical = new ArrayList<>();
                    for (RoutingKey key : deps.canonical.keySet())
                    {
                        if (ranges.contains(key))
                            deps.canonical.get(key).forEach(txnId -> canonical.add(new Entry<>(key, txnId)));
                    }
                    deps.test.forEach(ranges, new BiConsumer<RoutingKey, TxnId>()
                    {
                        int i = 0;
                        @Override
                        public void accept(RoutingKey key, TxnId txnId)
                        {
                            Entry<RoutingKey, TxnId> entry = canonical.get(i);
                            Assertions.assertEquals(entry.getKey(), key);
                            Assertions.assertEquals(entry.getValue(), txnId);
                            ++i;
                        }
                    });
                }

                {   // test forEach(txnId)
                    Set<TxnId> canonical = new TreeSet<>();
                    List<TxnId> test = new ArrayList<>();
                    for (RoutingKey key : deps.canonical.keySet())
                    {
                        if (ranges.contains(key))
                            canonical.addAll(deps.canonical.get(key));
                    }
                    deps.test.forEachUniqueTxnId(ranges, test::add);
                    test.sort(Timestamp::compareTo);
                    Assertions.assertEquals(new ArrayList<>(canonical), test);
                }

                Deps select = deps.select(ranges);
                select.testSimpleEquality();
                Deps noOpMerge = new Deps(deps.canonical, random.nextBoolean() ? deps.test.with(select.test) : select.test.with(deps.test));
                noOpMerge.testSimpleEquality();

                if (nestedSelect == null)
                {
                    nestedSelect = select;
                }
                else
                {
                    nestedSelect = nestedSelect.select(ranges);
                    nestedSelect.testSimpleEquality();
                }
            }
        }
    }

    private static long seed()
    {
        return ThreadLocalRandom.current().nextLong();
    }

    private static RandomSource random(long seed)
    {
        logger.info("Seed {}", seed);
        return new DefaultRandom(seed);
    }

    public static void main(String[] args)
    {
        for (long seed = 0 ; seed < 10000 ; ++seed)
            testMerge(seed, 100, 3, 50, 4, 4, 2, 100, 10, 4);

        for (long seed = 0 ; seed < 10000 ; ++seed)
            testMerge(seed, 1000, 3, 50, 4, 20, 5, 200, 100, 4);
    }

}
