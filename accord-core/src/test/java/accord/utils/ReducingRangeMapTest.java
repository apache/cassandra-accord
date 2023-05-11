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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;

import static accord.utils.ReducingRangeMap.trim;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;

// TODO (desired): test start inclusive ranges
public class ReducingRangeMapTest
{
    static final Logger logger = LoggerFactory.getLogger(ReducingRangeMapTest.class);
    static final ReducingRangeMap<Timestamp> EMPTY = new ReducingRangeMap<>(Timestamp.NONE);
    static final RoutingKey MINIMUM_EXCL = new IntKey.Routing(MIN_VALUE);
    static final RoutingKey MAXIMUM_EXCL = new IntKey.Routing(MAX_VALUE);
    private static RoutingKey rk(int t)
    {
        return new IntKey.Routing(t);
    }
    private static RoutingKey rk(Random random)
    {
        int rk = random.nextInt();
        if (random.nextBoolean()) rk = -rk;
        if (rk == MAX_VALUE) --rk;
        return new IntKey.Routing(rk);
    }

    private static Timestamp none()
    {
        return Timestamp.NONE;
    }

    private static Timestamp ts(int b)
    {
        return Timestamp.fromValues(1, b, 0, new Node.Id(1));
    }

    private static Range r(RoutingKey l, RoutingKey r)
    {
        return new Range.EndInclusive(l, r);
    }

    private static RoutingKey incr(RoutingKey rk)
    {
        return new IntKey.Routing(((IntKey.Routing)rk).key + 1);
    }

    private static RoutingKey decr(RoutingKey rk)
    {
        return new IntKey.Routing(((IntKey.Routing)rk).key - 1);
    }

    private static Range r(int l, int r)
    {
        return r(rk(l), rk(r));
    }

    private static Pair<RoutingKey, Timestamp> pt(int t, int b)
    {
        return Pair.create(rk(t), ts(b));
    }

    private static Pair<RoutingKey, Timestamp> pt(int t, Timestamp b)
    {
        return Pair.create(rk(t), b);
    }

    private static Pair<RoutingKey, Timestamp> pt(RoutingKey t, int b)
    {
        return Pair.create(t, ts(b));
    }

    private static ReducingRangeMap<Timestamp> h(Pair<RoutingKey, Timestamp>... points)
    {
        int length = points.length + (points[points.length - 1].left == null ? 0 : 1);
        RoutingKey[] routingKeys = new RoutingKey[length - 1];
        Timestamp[] timestamps = new Timestamp[length];
        for (int i = 0 ; i < length - 1 ; ++i)
        {
            routingKeys[i] = points[i].left;
            timestamps[i] = points[i].right;
        }
        timestamps[length - 1] = length == points.length ? points[length - 1].right : none();
        return new ReducingRangeMap<>(true, routingKeys, timestamps);
    }

    static
    {
        assert rk(100).equals(rk(100));
        assert ts(111).equals(ts(111));
    }

    private static class Builder
    {
        ReducingRangeMap<Timestamp> history = EMPTY;

        Builder add(Timestamp timestamp, Range... ranges)
        {
            history = ReducingRangeMap.add(history, Ranges.of(ranges), timestamp);
            return this;
        }

        Builder clear()
        {
            history = EMPTY;
            return this;
        }
    }

    static Builder builder()
    {
        return new Builder();
    }

    @Test
    public void testAdd()
    {
        Builder builder = builder();
        Assertions.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 5)),
                            builder.add(ts(5), r(10, 20), r(30, 40)).history);

        Assertions.assertEquals(none(), builder.history.get(rk(0)));
        Assertions.assertEquals(none(), builder.history.get(rk(10)));
        Assertions.assertEquals(ts(5), builder.history.get(rk(11)));
        Assertions.assertEquals(ts(5), builder.history.get(rk(20)));
        Assertions.assertEquals(none(), builder.history.get(rk(21)));

        builder.clear();
        Assertions.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 6)),
                            builder.add(ts(5), r(10, 20)).add(ts(6), r(30, 40)).history);
        builder.clear();
        Assertions.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, 6), pt(40, 5)),
                            builder.add(ts(5), r(10, 40)).add(ts(6), r(20, 30)).history);

        builder.clear();
        Assertions.assertEquals(h(pt(10, none()), pt(20, 6), pt(30, 5)),
                            builder.add(ts(6), r(10, 20)).add(ts(5), r(15, 30)).history);

        builder.clear();
        Assertions.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, 6)),
                            builder.add(ts(5), r(10, 25)).add(ts(6), r(20, 30)).history);
    }

    @Test
    public void testTrim()
    {
        Assertions.assertEquals(h(pt(10, none()), pt(20, 5), pt(30, none()), pt(40, 5), pt(50, none()), pt(60, 5)),
                            trim(h(pt(0, none()), pt(70, 5)), Ranges.of(r(10, 20), r(30, 40), r(50, 60)), Timestamp::max));

        Assertions.assertEquals(h(pt(10, none()), pt(20, 5)),
                            trim(h(pt(0, none()), pt(20, 5)), Ranges.of(r(10, 30)), Timestamp::max));

        Assertions.assertEquals(h(pt(10, none()), pt(20, 5)),
                            trim(h(pt(10, none()), pt(30, 5)), Ranges.of(r(0, 20)), Timestamp::max));
    }
//
//    @Test
//    public void testFullRange()
//    {
//        // test full range is collapsed
//        Builder builder = builder();
//        Assertions.assertEquals(h(pt(null, 5)),
//                            builder.add(b(5), r(MIN_RoutingKey, MIN_RoutingKey)).history);
//
//        Assertions.assertEquals(b(5), builder.history.get(MIN_RoutingKey));
//        Assertions.assertEquals(b(5), builder.history.get(t(0)));
//    }

    private static RoutingKey[] tks(int ... tks)
    {
        return IntStream.of(tks).mapToObj(ReducingRangeMapTest::rk).toArray(RoutingKey[]::new);
    }

    @Test
    public void testRandomTrims() throws ExecutionException, InterruptedException
    {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<ListenableFuture<Void>> results = new ArrayList<>();
        int count = 1000;
        for (int numberOfAdditions : new int[] { 1, 10, 100 })
        {
            for (float maxCoveragePerRange : new float[] { 0.01f, 0.1f, 0.5f })
            {
                for (float chanceOfMinRoutingKey : new float[] { 0.01f, 0.1f })
                {
                    results.addAll(testRandomTrims(executor, count, numberOfAdditions, 3, maxCoveragePerRange, chanceOfMinRoutingKey));
                }
            }
        }
        Futures.allAsList(results).get();
        executor.shutdown();
    }

    private List<ListenableFuture<Void>> testRandomTrims(ExecutorService executor, int tests, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinRoutingKey)
    {
        return ThreadLocalRandom.current()
                .longs(tests)
                .mapToObj(seed -> {

                    SettableFuture<Void> promise = SettableFuture.create();
                    executor.execute(() -> {
                        try
                        {
                            testRandomTrims(seed, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
                            promise.set(null);
                        }
                        catch (Throwable t)
                        {
                            promise.setException(t);
                        }
                    });
                    return promise;
                })
                .collect(Collectors.toList());
    }

    private void testRandomTrims(long seed, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinRoutingKey)
    {
        Random random = new Random(seed);
        logger.info("Seed {} ({}, {}, {}, {})", seed, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
        ReducingRangeMap<Timestamp> history = RandomMap.build(random, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
        // generate a random list of ranges that cover the whole ring
        int[] routingKeys = random.ints(16).map(i -> i == MIN_VALUE ? i + 1 : i).distinct().toArray();
        if (random.nextBoolean())
            routingKeys[0] = MIN_VALUE + 1;
        Arrays.sort(routingKeys);
        List<List<Range>> ranges = IntStream.range(0, routingKeys.length <= 3 ? 1 : 1 + random.nextInt((routingKeys.length - 1) / 2))
                .mapToObj(ignore -> new ArrayList<Range>())
                .collect(Collectors.toList());

        ranges.get(random.nextInt(ranges.size())).add(r(MINIMUM_EXCL, rk(routingKeys[0])));
        for (int i = 1 ; i < routingKeys.length ; ++i)
            ranges.get(random.nextInt(ranges.size())).add(r(routingKeys[i - 1], routingKeys[i]));
        ranges.get(random.nextInt(ranges.size())).add(r(rk(routingKeys[routingKeys.length - 1]), MAXIMUM_EXCL));
        // TODO: this was a wrap-around range
//        ranges.get(random.nextInt(ranges.size())).add(r(routingKeys[routingKeys.length - 1], routingKeys[0]));

        List<ReducingRangeMap<Timestamp>> splits = new ArrayList<>();
        for (List<Range> rs : ranges)
        {
            ReducingRangeMap<Timestamp> trimmed = trim(history, Ranges.of(rs.toArray(new Range[0])), Timestamp::max);
            splits.add(trimmed);
            if (rs.isEmpty())
                continue;

            Range prev = rs.get(rs.size() - 1);
            for (Range range : rs)
            {
                if (prev.end().equals(range.start()))
                {
                    Assertions.assertEquals(history.get(decr(range.start())), trimmed.get(decr(range.start())));
                    Assertions.assertEquals(history.get(range.start()), trimmed.get(range.start()));
                }
                else
                {
                    Assertions.assertEquals(none(), trimmed.get(range.start()));
                    Assertions.assertEquals(none(), trimmed.get(incr(prev.end())));
                }
                Assertions.assertEquals(history.get(incr(range.start())), trimmed.get(incr(range.start())));
                if (!incr(range.start()).equals(range.end()))
                    Assertions.assertEquals(history.get(decr(range.end())), trimmed.get(decr(range.end())));

                Assertions.assertEquals(history.get(range.end()), trimmed.get(range.end()));
                prev = range;
            }
        }

        ReducingRangeMap<Timestamp> merged = EMPTY;
        for (ReducingRangeMap<Timestamp> split : splits)
            merged = ReducingRangeMap.merge(merged, split, Timestamp::max);

        Assertions.assertEquals(history, merged);
    }

    @Test
    public void testOne()
    {
        testRandomAdds(-4183621399247163772L, 3, 100, 3, 0.010000f, 0.010000f);
    }

    @Test
    public void testRandomAdds() throws ExecutionException, InterruptedException
    {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<ListenableFuture<Void>> results = new ArrayList<>();
        int count = 1000;
        for (int numberOfAdditions : new int[] { 1, 10, 100 })
        {
            for (float maxCoveragePerRange : new float[] { 0.01f, 0.1f, 0.5f })
            {
                for (float chanceOfMinRoutingKey : new float[] { 0.01f, 0.1f })
                {
                    results.addAll(testRandomAdds(executor, count, 3, numberOfAdditions, 3, maxCoveragePerRange, chanceOfMinRoutingKey));
                }
            }
        }
        Futures.allAsList(results).get();
        executor.shutdown();
    }

    private List<ListenableFuture<Void>> testRandomAdds(ExecutorService executor, int tests, int numberOfMerges, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinRoutingKey)
    {
        return ThreadLocalRandom.current()
                .longs(tests)
                .mapToObj(seed -> {
                    SettableFuture<Void> promise = SettableFuture.create();
                    executor.execute(() -> {
                        try
                        {
                            testRandomAdds(seed, numberOfMerges, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
                            promise.set(null);
                        }
                        catch (Throwable t)
                        {
                            promise.setException(t);
                        }
                    });
                    return promise;
                })
                .collect(Collectors.toList());
    }

    private void testRandomAdds(long seed, int numberOfMerges, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinRoutingKey)
    {
        Random random = new Random(seed);
        String id = String.format("%d, %d, %d, %d, %f, %f", seed, numberOfMerges, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
        logger.info(id);
        List<RandomWithCanonical> merge = new ArrayList<>();
        while (numberOfMerges-- > 0)
        {
            RandomWithCanonical build = new RandomWithCanonical();
            build.addRandom(random, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
            merge.add(build);
        }

        RandomWithCanonical check = new RandomWithCanonical();
        for (RandomWithCanonical add : merge)
            check = check.merge(add);
//        check.serdeser();

        check.validate(random, id);
    }

    static class RandomMap
    {
        ReducingRangeMap<Timestamp> test = new ReducingRangeMap<>(Timestamp.NONE);

        void add(Ranges ranges, Timestamp timestamp)
        {
            test = ReducingRangeMap.add(test, ranges, timestamp);
        }

        void merge(RandomMap other)
        {
            test = ReducingRangeMap.merge(test, other.test, Timestamp::max);
        }

        void addOneRandom(Random random, int maxRangeCount, float maxCoverage, float minChance)
        {
            int count = maxRangeCount == 1 ? 1 : 1 + random.nextInt(maxRangeCount - 1);
            Timestamp timestamp = ts(random.nextInt(MAX_VALUE));
            List<Range> ranges = new ArrayList<>();
            while (count-- > 0)
            {
                int length = (int) (2 * random.nextDouble() * maxCoverage * MAX_VALUE);
                if (length == 0) length = 1;
                Range range;
                if (random.nextFloat() <= minChance)
                {
                    if (random.nextBoolean()) range = r(MIN_VALUE + 1, MIN_VALUE + 1 + length);
                    else range = r(MAX_VALUE - length - 1, MAX_VALUE - 1);
                }
                else
                {
                    int start = random.nextInt(MAX_VALUE - length - 1);
                    range = r(start, start + length);
                }
                ranges.add(range);
            }
            add(Ranges.of(ranges.toArray(new Range[0])), timestamp);
        }

        void addRandom(Random random, int count, int maxNumberOfRangesPerAddition, float maxCoveragePerAddition, float minRoutingKeyChance)
        {
            while (count-- > 0)
                addOneRandom(random, maxNumberOfRangesPerAddition, maxCoveragePerAddition, minRoutingKeyChance);
        }

        static ReducingRangeMap<Timestamp> build(Random random, int count, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinRoutingKey)
        {
            RandomMap result = new RandomMap();
            result.addRandom(random, count, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
            return result.test;
        }
    }

    static class RandomWithCanonical extends RandomMap
    {
        // confusingly, we use lower bounds here since we copied over from C*
        NavigableMap<RoutingKey, Timestamp> canonical = new TreeMap<>();
        {
            canonical.put(MINIMUM_EXCL, none());
            canonical.put(MAXIMUM_EXCL, none());
        }

        Timestamp get(RoutingKey rk)
        {
            return canonical.ceilingEntry(rk).getValue();
        }

        RandomWithCanonical merge(RandomWithCanonical other)
        {
            RandomWithCanonical result = new RandomWithCanonical();
            result.test = ReducingRangeMap.merge(test, other.test, Timestamp::max);
            result.canonical = new TreeMap<>();
            result.canonical.putAll(canonical);
            RoutingKey prev = null;
            for (Map.Entry<RoutingKey, Timestamp> entry : other.canonical.entrySet())
            {
                if (prev != null) result.addCanonical(r(prev, entry.getKey()), entry.getValue());
                prev = entry.getKey();
            }
            return result;
        }

//        void serdeser()
//        {
//            ReduceRangeMap<RoutingKey, Timestamp> tmp = ReduceRangeMap.fromTupleBufferList(test.toTupleBufferList());
//            Assertions.assertEquals(test, tmp);
//            test = tmp;
//        }

        @Override
        void add(Ranges addRanges, Timestamp timestamp)
        {
            super.add(addRanges, timestamp);
            for (Range range : addRanges)
                addCanonical(range, timestamp);
        }

        void addCanonical(Range range, Timestamp timestamp)
        {
            canonical.put(range.start(), canonical.ceilingEntry(range.start()).getValue());
            canonical.put(range.end(), canonical.ceilingEntry(range.end()).getValue());

            canonical.subMap(range.start(), false, range.end(), true)
                    .entrySet().forEach(e -> e.setValue(Timestamp.max(e.getValue(), timestamp)));
        }

        void validate(Random random, String id)
        {
            for (RoutingKey rk : canonical.keySet())
            {
                Assertions.assertEquals(get(decr(rk)), test.get(decr(rk)), id);
                Assertions.assertEquals(get(rk), test.get(rk), id);
                Assertions.assertEquals(get(incr(rk)), test.get(incr(rk)), id);
            }

            // check some random
            {
                int remaining = 1000;
                while (remaining-- > 0)
                {
                    RoutingKey routingKey = rk(random);
                    Assertions.assertEquals(get(routingKey), test.get(routingKey), id);
                }
            }

            // validate foldl
            {
                int remaining = 100;
                while (remaining-- > 0)
                {
                    int count = 1 + random.nextInt(20);
                    RoutingKeys keys;
                    Ranges ranges;
                    {
                        RoutingKey[] tmp = new RoutingKey[count];
                        for (int i = 0 ; i < tmp.length ; ++i)
                            tmp[i] = rk(random);
                        keys = RoutingKeys.of(tmp);
                        Range[] tmp2 = new Range[(keys.size() + 1) / 2];
                        int i = 0, c = 0;
                        if (keys.size() % 2 == 1 && random.nextBoolean())
                            tmp2[c++] = r(MINIMUM_EXCL, keys.get(i++));
                        while (i + 1 < keys.size())
                        {
                            tmp2[c++] = r(keys.get(i), keys.get(i+1));
                            i += 2;
                        }
                        if (i < keys.size())
                            tmp2[c++] = r(keys.get(i++), MAXIMUM_EXCL);
                        ranges = Ranges.of(tmp2);
                    }

                    List<Timestamp> foldl = test.foldl(keys, (timestamp, timestamps) -> {
                            if (timestamps.isEmpty() || !timestamps.get(timestamps.size() - 1).equals(timestamp))
                                timestamps.add(timestamp);
                            return timestamps;
                        }, new ArrayList<>(), ignore -> false);

                    List<Timestamp> canonFoldl = new ArrayList<>();
                    for (RoutingKey key : keys)
                    {
                        Timestamp next = get(key);
                        if (canonFoldl.isEmpty() || !canonFoldl.get(canonFoldl.size() - 1).equals(next))
                            canonFoldl.add(next);
                    }
                    Assertions.assertEquals(canonFoldl, foldl, id);

                    foldl = test.foldl(ranges, (timestamp, timestamps) -> {
                        if (timestamps.isEmpty() || !timestamps.get(timestamps.size() - 1).equals(timestamp))
                            timestamps.add(timestamp);
                        return timestamps;
                    }, new ArrayList<>(), ignore -> false);

                    canonFoldl.clear();
                    for (Range range : ranges)
                    {
                        RoutingKey start = canonical.higherKey(range.start());
                        RoutingKey end = canonical.ceilingKey(range.end());
                        for (Timestamp next : canonical.subMap(start, true, end, true).values())
                        {
                            if (canonFoldl.isEmpty() || !canonFoldl.get(canonFoldl.size() - 1).equals(next))
                                canonFoldl.add(next);
                        }
                    }
                    Assertions.assertEquals(canonFoldl, foldl, id);
                }
            }
        }
    }
}
