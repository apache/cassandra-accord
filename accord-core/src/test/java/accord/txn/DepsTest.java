package accord.txn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.impl.IntHashKey;
import accord.local.Node.Id;
import accord.primitives.Deps.Entry;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

// TODO (now): test Keys with no contents
// TODO (now): test without
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

    static class Deps
    {
        final Map<Key, Set<TxnId>> canonical;
        final accord.primitives.Deps test;

        Deps(Map<Key, Set<TxnId>> canonical, accord.primitives.Deps test)
        {
            this.canonical = canonical;
            this.test = test;
        }

        static Deps generate(Random random, int uniqueTxnIds, int epochRange, int realRange, int logicalRange, int nodeRange,
                             int uniqueKeys, int emptyKeys, int keyRange, int totalCount)
        {
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

            accord.primitives.Deps.Builder builder = accord.primitives.Deps.builder(keys);
            Map<Key, Set<TxnId>> canonical = new TreeMap<>();
            for (int i = 0 ; i < totalCount ; ++i)
            {
                Key key = populateKeys.get(random.nextInt(uniqueKeys));
                TxnId txnId = txnIds.get(random.nextInt(uniqueTxnIds));
                canonical.computeIfAbsent(key, ignore -> new TreeSet<>()).add(txnId);
                builder.add(key, txnId);
            }

            return new Deps(canonical, builder.build());
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
            Assertions.assertArrayEquals(canonicalInverted.keySet().toArray(TxnId[]::new),
                                         IntStream.range(0, test.txnIdCount()).mapToObj(test::txnId).toArray(TxnId[]::new));
            for (Map.Entry<TxnId, List<Key>> e : canonicalInverted.entrySet())
            {
                Assertions.assertArrayEquals(e.getValue().toArray(Key[]::new),
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
            Assertions.assertEquals(builder.toString(), test.toString());
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

        static Deps merge(Keys keys, List<Deps> deps)
        {
            Map<Key, Set<TxnId>> canonical = new TreeMap<>();
            for (Deps that : deps)
            {
                for (Map.Entry<Key, Set<TxnId>> e : that.canonical.entrySet())
                    canonical.computeIfAbsent(e.getKey(), ignore -> new TreeSet<>()).addAll(e.getValue());
            }

            return new Deps(canonical, accord.primitives.Deps.merge(keys, deps, d -> d.test));
        }

        static Deps merge(List<Deps> deps)
        {
            TreeSet<Key> keys = new TreeSet<>();
            for (Deps d : deps)
                keys.addAll(d.canonical.keySet());
            return merge(new Keys(keys), deps);
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
            keys = tmp.toArray(IntHashKey[]::new);
        }
        KeyRange[] ranges = new KeyRange[count];
        for (int i = 0 ; i < count ; i++)
            ranges[i] = IntHashKey.range(keys[i * 2], keys[i * 2 + 1]);
        return new KeyRanges(ranges);
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
