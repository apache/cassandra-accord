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

package accord.txn;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import accord.primitives.*;
import accord.primitives.Deps.OrderedBuilder;
import accord.utils.Gen;
import accord.utils.Gens;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.impl.IntHashKey;
import accord.local.Node.Id;
import accord.primitives.Deps.Entry;

import static accord.utils.Gens.lists;
import static accord.utils.Property.qt;
import static accord.utils.Utils.toArray;

// TODO: test Keys with no contents
// TODO: test #without
public class DepsTest
{
    private static final Logger logger = LoggerFactory.getLogger(DepsTest.class);

    @Test
    public void testRandom()
    {
        testOneRandom(seed(), 1000, 3, 50, 10, 4, 100, 10, 200, 1000);
        testOneRandom(seed(), 1000, 3, 50, 10, 4, 10, 2, 200, 1000);
        testOneRandom(seed(), 100, 3, 50, 10, 4, 10, 2, 200, 100);
    }

    @Test
    public void testMerge()
    {
        testMerge(seed(), 100, 3, 50, 10, 4, 10, 5, 200, 100, 10);
        testMerge(seed(), 1000, 3, 50, 10, 4, 100, 10, 200, 1000, 10);
    }

    private static void testMerge(long seed, int uniqueTxnIdsRange, int epochRange, int realRange, int logicalRange, int nodeRange,
                                 int uniqueKeysRange, int emptyKeysRange, int keyRange, int totalCountRange, int mergeCountRange)
    {
        Random random = random(seed);
        Supplier<Deps> supplier = supplier(random, uniqueTxnIdsRange, epochRange, realRange, logicalRange, nodeRange,
                                           uniqueKeysRange, emptyKeysRange, keyRange, totalCountRange);
        int count = 1 + random.nextInt(mergeCountRange);
        List<Deps> deps = new ArrayList<>(count);
        while (count-- > 0)
            deps.add(supplier.get());
        testOneDeps(random, DepsTest.Deps.merge(deps), 200);
    }

    @Test
    public void testWith()
    {
        testWith(seed(), 1000, 3, 50, 10, 4, 100, 10, 200, 1000, 10);
    }

    private static void testWith(long seed, int uniqueTxnIdsRange, int epochRange, int realRange, int logicalRange, int nodeRange,
                                 int uniqueKeysRange, int emptyKeysRange, int keyRange, int totalCountRange, int mergeCountRange)
    {
        Random random = random(seed);
        Supplier<Deps> supplier = supplier(random, uniqueTxnIdsRange, epochRange, realRange, logicalRange, nodeRange,
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
            Assertions.assertSame(deps.test, deps.test.without(ignore -> false));
            // all match
            Assertions.assertSame(accord.primitives.Deps.NONE, deps.test.without(ignore -> true));

            // remove specific TxnId
            for (TxnId txnId : deps.test.txnIds())
            {
                accord.primitives.Deps without = deps.test.without(i -> i.equals(txnId));
                // Was the TxnId removed?
                List<TxnId> expectedTxnId = new ArrayList<>(deps.test.txnIds());
                expectedTxnId.remove(txnId);
                Assertions.assertEquals(expectedTxnId, without.txnIds());

                Assertions.assertEquals(Keys.EMPTY, without.someKeys(txnId));
                // all other keys are fine?
                for (TxnId other : deps.test.txnIds())
                {
                    if (other == txnId)
                        continue;
                    Assertions.assertEquals(deps.test.someKeys(other), without.someKeys(other), "someKeys(" + other + ")");
                }

                // check each key
                for (Key key : deps.test.keys())
                {
                    List<TxnId> expected = get(deps.test, key);
                    expected.remove(txnId);

                    Assertions.assertEquals(expected, get(without, key), () -> "TxnId " + txnId + " is expected to be removed for key " + key);
                }
            }
        });
    }

    private static List<TxnId> get(accord.primitives.Deps deps, Key key)
    {
        List<TxnId> ids = new ArrayList<>();
        deps.forEach(key, ids::add);
        return ids;
    }

    @Test
    public void testIterator()
    {
        qt().forAll(Deps::generate).check(deps -> {
            try (OrderedBuilder builder = accord.primitives.Deps.orderedBuilder(true))
            {
                for (Map.Entry<Key, TxnId> e : deps.test)
                    builder.add(e.getKey(), e.getValue());
                Assertions.assertEquals(deps.test, builder.build());
            }
        });
    }

    @Test
    public void testForEachOnUniqueEndInclusive()
    {
        qt().forAll(Gen.of(Deps::generate).filter(d -> d.test.keys().size() >= 2)).check(deps -> {
            Keys keys = deps.test.keys();
            Key start = keys.get(0);
            Key end = keys.get(keys.size() - 1);
            if (start.equals(end))
                throw new AssertionError(start + " == " + end);

            TreeSet<TxnId> seen = new TreeSet<>();
            deps.test.forEachOn(KeyRanges.of(KeyRange.range(start, end, false, true)), ignore -> true, txnId -> {
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
            Keys keys = deps.test.keys();
            Key start = keys.get(0);
            Key end = keys.get(keys.size() - 1);

            TreeSet<TxnId> seen = new TreeSet<>();
            deps.test.forEachOn(KeyRanges.of(KeyRange.range(start, end, true, false)), ignore -> true, txnId -> {
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
            Keys keys = deps.test.keys();
            Key start = IntHashKey.forHash(Integer.MIN_VALUE);
            Key end = keys.get(0);

            TreeSet<TxnId> seen = new TreeSet<>();
            deps.test.forEachOn(KeyRanges.of(KeyRange.range(start, end, true, false)), ignore -> true, txnId -> {
                if (!seen.add(txnId))
                    throw new AssertionError("Seen " + txnId + " multiple times");
            });
            Assertions.assertEquals(Collections.emptySet(), seen);
        });
    }

    @Test
    public void testMergeFull()
    {
        qt().forAll(lists(Deps::generate).ofSizeBetween(0, 20)).check(DepsTest::testMergedProperty);
    }

    @Test
    public void testMergeFullConcurrent()
    {
        // pure=false due to shared buffer pool
        Runnable test = () -> qt().withPure(false).forAll(lists(Deps::generate).ofSizeBetween(0, 20)).check(DepsTest::testMergedProperty);
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

    //TODO test "with" where TxnId and Keys are the same, but Key -> [TxnId] does not match

    private static Keys keys(List<Deps> list)
    {
        return list.stream().map(d -> d.test.keys()).reduce(Keys.EMPTY, Keys::union);
    }

    private static void testMergedProperty(List<Deps> list)
    {
        Deps expected = Deps.merge(list);
        expected.testSimpleEquality();

        // slightly redundant due to Deps.merge using this method... it is here for completeness
        Assertions.assertEquals(expected.test, accord.primitives.Deps.merge(list, a -> a.test));
        Assertions.assertEquals(expected.test, list.stream().map(a -> a.test).reduce(accord.primitives.Deps.NONE, accord.primitives.Deps::with));
    }

    @Test
    public void orderedBuilder()
    {
        qt().forAll(Deps::generate, Gens.random()).check((deps, random) -> {
            OrderedBuilder builder = accord.primitives.Deps.orderedBuilder(false);
            for (Key key : deps.canonical.keySet())
            {
                builder.nextKey(key);
                List<TxnId> ids = new ArrayList<>(deps.canonical.get(key));
                Collections.shuffle(ids, random);
                ids.forEach(builder::add);
            }

            Assertions.assertEquals(deps.test, builder.build());
        });
    }

    static class Deps
    {
        final Map<Key, Set<TxnId>> canonical;
        final accord.primitives.Deps test;

        Deps(Map<Key, Set<TxnId>> canonical, accord.primitives.Deps test)
        {
            this.canonical = canonical;
            this.test = test;
        }

        static Deps generate(Gen.Random random)
        {
            int epochRange = 3;
            int realRange = 50;
            int logicalRange = 10;
            double uniqueTxnIdsPercentage = 0.66D;
            int uniqueTxnIds = random.nextPositive((int) ((realRange * logicalRange * epochRange) * uniqueTxnIdsPercentage));

            int nodeRange = random.nextPositive(4);
            int uniqueKeys = random.nextInt(2, 200);
            int emptyKeys = random.nextInt(0, 10);
            int keyRange = random.nextInt(uniqueKeys + emptyKeys, 400);
            int totalCount = random.nextPositive(1000);
            Deps deps = generate(random, uniqueTxnIds, epochRange, realRange, logicalRange, nodeRange, uniqueKeys, emptyKeys, keyRange, totalCount);
            deps.testSimpleEquality();
            return deps;
        }

        static Deps generate(Random random, int uniqueTxnIds, int epochRange, int realRange, int logicalRange, int nodeRange,
                             int uniqueKeys, int emptyKeys, int keyRange, int totalCount)
        {
            // populateKeys is a subset of keys
            Keys populateKeys, keys;
            {
                TreeSet<Key> tmp = new TreeSet<>();
                while (tmp.size() < uniqueKeys)
                    tmp.add(IntHashKey.key(random.nextInt(keyRange)));
                populateKeys = new Keys(tmp);
                while (tmp.size() < uniqueKeys + emptyKeys)
                    tmp.add(IntHashKey.key(random.nextInt(keyRange)));
                keys = new Keys(tmp);
            }

            List<TxnId> txnIds; {
                TreeSet<TxnId> tmp = new TreeSet<>();
                while (tmp.size() < uniqueTxnIds)
                    tmp.add(new TxnId(random.nextInt(epochRange), random.nextInt(realRange), random.nextInt(logicalRange), new Id(random.nextInt(nodeRange))));
                txnIds = new ArrayList<>(tmp);
            }

            Map<Key, Set<TxnId>> canonical = new TreeMap<>();
            for (int i = 0 ; i < totalCount ; ++i)
            {
                Key key = populateKeys.get(random.nextInt(uniqueKeys));
                TxnId txnId = txnIds.get(random.nextInt(uniqueTxnIds));
                canonical.computeIfAbsent(key, ignore -> new TreeSet<>()).add(txnId);
            }

            try (OrderedBuilder builder = accord.primitives.Deps.orderedBuilder(false))
            {
                canonical.forEach((key, ids) -> {
                    builder.nextKey(key);
                    ids.forEach(builder::add);
                });

                accord.primitives.Deps test = builder.build();
                return new Deps(canonical, test);
            }
        }

        Deps select(KeyRanges ranges)
        {
            Map<Key, Set<TxnId>> canonical = new TreeMap<>();
            for (Map.Entry<Key, Set<TxnId>> e : this.canonical.entrySet())
            {
                if (ranges.contains(e.getKey()))
                    canonical.put(e.getKey(), e.getValue());
            }

            return new Deps(canonical, test.slice(ranges));
        }

        Deps with(Deps that)
        {
            Map<Key, Set<TxnId>> canonical = new TreeMap<>();
            for (Map.Entry<Key, Set<TxnId>> e : this.canonical.entrySet())
                canonical.computeIfAbsent(e.getKey(), ignore -> new TreeSet<>()).addAll(e.getValue());
            for (Map.Entry<Key, Set<TxnId>> e : that.canonical.entrySet())
                canonical.computeIfAbsent(e.getKey(), ignore -> new TreeSet<>()).addAll(e.getValue());

            return new Deps(canonical, test.with(that.test));
        }

        void testSimpleEquality()
        {
            Assertions.assertArrayEquals(canonical.keySet().toArray(new Key[0]), test.keys().stream().toArray(Key[]::new));
            for (Map.Entry<Key, Set<TxnId>> e : canonical.entrySet())
            {
                List<TxnId> canonical = new ArrayList<>(e.getValue());
                List<TxnId> test = new ArrayList<>();
                this.test.forEach(e.getKey(), test::add);
                Assertions.assertEquals(canonical, test);
            }

            TreeMap<TxnId, List<Key>> canonicalInverted = invertCanonical();
            Assertions.assertArrayEquals(toArray(canonicalInverted.keySet(), TxnId[]::new),
                                         IntStream.range(0, test.txnIdCount()).mapToObj(test::txnId).toArray(TxnId[]::new));
            for (Map.Entry<TxnId, List<Key>> e : canonicalInverted.entrySet())
            {
                Assertions.assertArrayEquals(toArray(e.getValue(), Key[]::new),
                                             test.someKeys(e.getKey()).stream().toArray(Key[]::new));
            }

            StringBuilder builder = new StringBuilder();
            builder.append("{");
            for (Map.Entry<Key, Set<TxnId>> e : canonical.entrySet())
            {
                if (builder.length() > 1)
                    builder.append(", ");
                builder.append(e.getKey());
                builder.append(":");
                builder.append(e.getValue());
            }
            builder.append("}");
            Assertions.assertEquals(builder.toString(), test.toSimpleString());
        }

        TreeMap<TxnId, List<Key>> invertCanonical()
        {
            TreeMap<TxnId, List<Key>> result = new TreeMap<>();
            for (Map.Entry<Key, Set<TxnId>> e : canonical.entrySet())
            {
                e.getValue().forEach(txnId -> result.computeIfAbsent(txnId, ignore -> new ArrayList<>())
                                                    .add(e.getKey()));

            }
            return result;
        }

        static Deps merge(List<Deps> deps)
        {
            Map<Key, Set<TxnId>> canonical = new TreeMap<>();
            for (Deps that : deps)
            {
                for (Map.Entry<Key, Set<TxnId>> e : that.canonical.entrySet())
                    canonical.computeIfAbsent(e.getKey(), ignore -> new TreeSet<>()).addAll(e.getValue());
            }

            return new Deps(canonical, accord.primitives.Deps.merge(deps, d -> d.test));
        }
    }

    private static KeyRanges randomKeyRanges(Random random, int countRange, int valueRange)
    {
        int count = countRange == 1 ? 1 : 1 + random.nextInt(countRange - 1);
        IntHashKey[] keys;
        {
            TreeSet<IntHashKey> tmp = new TreeSet<>();
            while (tmp.size() < count * 2)
            {
                IntHashKey key = random.nextFloat() < 0.1f ? IntHashKey.forHash(random.nextInt())
                                                           : IntHashKey.key(random.nextInt(valueRange));
                tmp.add(key);
            }
            keys = toArray(tmp, IntHashKey[]::new);
        }
        KeyRange[] ranges = new KeyRange[count];
        for (int i = 0 ; i < count ; i++)
            ranges[i] = IntHashKey.range(keys[i * 2], keys[i * 2 + 1]);
        return KeyRanges.ofSortedAndDeoverlapped(ranges);
    }

    private static void testOneRandom(long seed, int uniqueTxnIds, int epochRange, int realRange, int logicalRange, int nodeRange,
                                      int uniqueKeys, int emptyKeys, int keyRange, int totalCountRange)
    {
        Random random = random(seed);
        int totalCount = 1 + random.nextInt(totalCountRange - 1);
        testOneDeps(random,
                    DepsTest.Deps.generate(random, uniqueTxnIds, epochRange, realRange, logicalRange, nodeRange, uniqueKeys, emptyKeys, keyRange, totalCount),
                    keyRange);
    }

    private static Supplier<Deps> supplier(Random random, int uniqueTxnIdsRange, int epochRange, int realRange, int logicalRange, int nodeRange,
                                           int uniqueKeysRange, int emptyKeysRange, int keyRange, int totalCountRange)
    {
        return () -> {
            if (random.nextInt(100) == 0)
                return new Deps(new TreeMap<>(), accord.primitives.Deps.NONE);

            int uniqueTxnIds = 1 + random.nextInt(uniqueTxnIdsRange - 1);
            int uniqueKeys = 1 + random.nextInt(uniqueKeysRange - 1);
            int emptyKeys = 1 + random.nextInt(emptyKeysRange - 1);
            int totalCount = random.nextInt(Math.min(totalCountRange, uniqueKeys * uniqueTxnIds));
            return DepsTest.Deps.generate(random, uniqueTxnIds,
                                                  epochRange, realRange, logicalRange, nodeRange,
                                                  uniqueKeys, emptyKeys, keyRange, totalCount);
        };
    }

    private static void testOneDeps(Random random, Deps deps, int keyRange)
    {
        deps.testSimpleEquality();
        {
            Deps nestedSelect = null;
            // generate some random KeyRanges, and slice using them
            for (int i = 0, count = 1 + random.nextInt(4); i < count ; ++i)
            {
                KeyRanges ranges = randomKeyRanges(random, 1 + random.nextInt(5), keyRange);

                {   // test forEach(key, txnId)
                    List<Entry> canonical = new ArrayList<>();
                    for (Key key : deps.canonical.keySet())
                    {
                        if (ranges.contains(key))
                            deps.canonical.get(key).forEach(txnId -> canonical.add(new Entry(key, txnId)));
                    }
                    deps.test.forEachOn(ranges, ignore -> true, new BiConsumer<Key, TxnId>()
                    {
                        int i = 0;
                        @Override
                        public void accept(Key key, TxnId txnId)
                        {
                            Entry entry = canonical.get(i);
                            Assertions.assertEquals(entry.getKey(), key);
                            Assertions.assertEquals(entry.getValue(), txnId);
                            ++i;
                        }
                    });
                }

                {   // test forEach(txnId)
                    Set<TxnId> canonical = new TreeSet<>();
                    List<TxnId> test = new ArrayList<>();
                    for (Key key : deps.canonical.keySet())
                    {
                        if (ranges.contains(key))
                            canonical.addAll(deps.canonical.get(key));
                    }
                    deps.test.forEachOn(ranges, ignore -> true, txnId -> test.add(txnId));
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

    private static Random random(long seed)
    {
        logger.info("Seed {}", seed);
        Random random = new Random();
        random.setSeed(seed);
        return random;
    }

    public static void main(String[] args)
    {
        for (long seed = 0 ; seed < 10000 ; ++seed)
            testMerge(seed, 100, 3, 50, 10, 4, 4, 2, 100, 10, 4);

        for (long seed = 0 ; seed < 10000 ; ++seed)
            testMerge(seed, 1000, 3, 50, 10, 4, 20, 5, 200, 100, 4);
    }

}
