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

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.utils.BTreeReducingRangeMap.RawBuilder;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import javax.annotation.Nonnull;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.Integer.MIN_VALUE;

// TODO (desired): test start inclusive ranges
public class BTreeReducingRangeMapTest
{
    static final BTreeReducingRangeMap<Timestamp> EMPTY = new BTreeReducingRangeMap<>();
    static final RoutingKey MINIMUM_EXCL = new IntKey.Routing(MIN_VALUE);
    static final RoutingKey MAXIMUM_EXCL = new IntKey.Routing(MAX_VALUE);
    static boolean END_INCLUSIVE = false;

    private static RoutingKey rk(int t)
    {
        return new IntKey.Routing(t);
    }
    private static RoutingKey rk(Random random)
    {
        int rk = random.nextInt();
        if (random.nextBoolean()) rk = -rk;
        if (rk == MAX_VALUE) --rk;
        if (rk == MIN_VALUE) ++rk;
        return new IntKey.Routing(rk);
    }

    private static Timestamp none()
    {
        return null;
    }

    private static Timestamp ts(int b)
    {
        return Timestamp.fromValues(1, b, 0, new Node.Id(1));
    }

    private static Range r(RoutingKey l, RoutingKey r)
    {
        return END_INCLUSIVE ? new Range.EndInclusive(l, r) : new Range.StartInclusive(l, r);
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

    private static BTreeReducingRangeMap<Timestamp> h(Pair<RoutingKey, Timestamp>... points)
    {
        Invariants.checkState(points[0].right == none());
        int length = points.length;
        RawBuilder<Timestamp, BTreeReducingRangeMap<Timestamp>> builder = new RawBuilder<>(true, length - 1);
        for (int i = 1 ; i < length ; ++i)
            builder.append(points[i - 1].left, points[i].right);
        builder.append(points[length - 1].left);
        return builder.build(BTreeReducingRangeMap::new);
    }

    static
    {
        assert rk(100).equals(rk(100));
        assert ts(111).equals(ts(111));
    }

    private static class Builder
    {
        BTreeReducingRangeMap<Timestamp> history = EMPTY;

        Builder add(Timestamp timestamp, Range... ranges)
        {
            history = BTreeReducingRangeMap.add(history, Ranges.of(ranges), timestamp);
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
    public void testOne()
    {
        testRandomAdds(8532037884171168001L, 3, 1, 3, 0.100000f, 0.100000f);
    }

    @Test
    public void testRandomAdds() throws ExecutionException, InterruptedException
    {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<ListenableFuture<Void>> results = new ArrayList<>();
        int count = 100000;
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
        String id = String.format("%d, %d, %d, %d, %f, %f", seed, numberOfMerges, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
        try
        {
            Random random = new Random(seed);
            List<RandomWithCanonical> merge = new ArrayList<>();
            while (numberOfMerges-- > 0)
            {
                RandomWithCanonical build = new RandomWithCanonical();
                build.addRandom(random, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
                build.validate(random, id);
                merge.add(build);
            }

            RandomWithCanonical check = new RandomWithCanonical();
            for (RandomWithCanonical add : merge)
                check = check.merge(random, add);
    //        check.serdeser();

            check.validate(random, id);
        }
        catch (Throwable t)
        {
            if (!(t instanceof AssertionFailedError))
                throw new RuntimeException(id, t);
        }
    }

    static class RandomMap
    {
        BTreeReducingRangeMap<Timestamp> test = new BTreeReducingRangeMap<>();

        void add(Ranges ranges, Timestamp timestamp)
        {
            test = BTreeReducingRangeMap.add(test, ranges, timestamp);
        }

        void merge(RandomMap other)
        {
            test = BTreeReducingRangeMap.merge(test, other.test, Timestamp::max);
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


        static BTreeReducingRangeMap<Timestamp> build(Random random, int count, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinRoutingKey)
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

        RandomWithCanonical merge(Random random, RandomWithCanonical other)
        {
            RandomWithCanonical result = new RandomWithCanonical();
            result.test = random.nextBoolean()
                          ? BTreeReducingRangeMap.merge(test, other.test, Timestamp::max)
                          : BTreeReducingIntervalMap.mergeIntervals(test, other.test, IntervalBuilder::new);
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

        static class IntervalBuilder extends BTreeReducingIntervalMap.AbstractIntervalBuilder<RoutingKey, Timestamp, BTreeReducingRangeMap<Timestamp>>
        {
            protected IntervalBuilder(boolean inclusiveEnds, int capacity)
            {
                super(inclusiveEnds, capacity);
            }

            @Override
            protected Timestamp slice(RoutingKey start, RoutingKey end, Timestamp value)
            {
                return value;
            }

            @Override
            protected Timestamp reduce(Timestamp a, Timestamp b)
            {
                return Timestamp.max(a, b);
            }

            @Override
            protected Timestamp tryMergeEqual(@Nonnull Timestamp a, Timestamp b)
            {
                return a;
            }

            @Override
            protected BTreeReducingRangeMap<Timestamp> buildInternal(Object[] tree)
            {
                return new BTreeReducingRangeMap<>(inclusiveEnds, tree);
            }
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

        @Override
        void addOneRandom(Random random, int maxRangeCount, float maxCoverage, float minChance)
        {
            super.addOneRandom(random, maxRangeCount, maxCoverage, minChance);
//            validate(new Random(), "");
        }

        void addCanonical(Range range, Timestamp timestamp)
        {
            canonical.put(range.start(), canonical.ceilingEntry(range.start()).getValue());
            canonical.put(range.end(), canonical.ceilingEntry(range.end()).getValue());

            canonical.subMap(range.start(), !END_INCLUSIVE, range.end(), END_INCLUSIVE)
                    .entrySet().forEach(e -> e.setValue(Timestamp.nonNullOrMax(e.getValue(), timestamp)));
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
                        if (next == null)
                            continue;
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
                        RoutingKey start = END_INCLUSIVE ? canonical.higherKey(range.start()) : canonical.ceilingKey(range.start());
                        RoutingKey end = END_INCLUSIVE ? canonical.ceilingKey(range.end()) : canonical.higherKey(range.end());
                        for (Timestamp next : canonical.subMap(start, true, end, true).values())
                        {
                            if (next == null)
                                continue;

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
