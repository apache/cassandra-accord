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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;

public class ReducingRangeMapTest
{
    static int MAX_VALUE = Integer.MAX_VALUE; // for debugging convenience, set to human-readable number
    static int MIN_VALUE = Integer.MIN_VALUE;
    static final RoutingKey MIN_EXCL = new IntKey.Routing(MIN_VALUE);
    static final RoutingKey MAX_EXCL = new IntKey.Routing(MAX_VALUE);

    // Set from test params
    static boolean TEST_END_INCLUSIVE = false;
    static TestWith TEST_WITH = TestWith.BTreeReducingRangeMap;

    enum TestWith
    {
        ReducingRangeMap,
        ReducingIntervalMap,
        BTreeReducingRangeMap
    }

    private static RoutingKey rk(int t)
    {
        return new IntKey.Routing(t);
    }

    private static RoutingKey rk(Random random)
    {
        int rk = random.nextInt(MAX_VALUE);
        if (rk == MAX_VALUE) --rk;
        if (rk == MIN_VALUE) ++rk;
        return new IntKey.Routing(rk);
    }

    private static Timestamp ts(int b)
    {
        return Timestamp.fromValues(1, b, 0, new Node.Id(1));
    }

    private static Range r(RoutingKey l, RoutingKey r)
    {
        return TEST_END_INCLUSIVE ? new Range.EndInclusive(l, r) : new Range.StartInclusive(l, r);
    }

    private static RoutingKey incr(RoutingKey rk)
    {
        return new IntKey.Routing(((IntKey.Routing) rk).key + 1);
    }

    private static RoutingKey decr(RoutingKey rk)
    {
        return new IntKey.Routing(((IntKey.Routing) rk).key - 1);
    }

    private static Range r(int l, int r)
    {
        return r(rk(l), rk(r));
    }

    static
    {
        Invariants.require(rk(100).equals(rk(100)));
        Invariants.require(ts(111).equals(ts(111)));
    }

    @Test
    public void testOne()
    {
        testRandomAdds(8532037884171168001L, 3, 100, 3, 0.100000f, 0.100000f);
    }

    @ParameterizedTest
    @CsvSource({
    "false,ReducingRangeMap",
    "false,ReducingIntervalMap",
    "true,ReducingRangeMap",
    "true,ReducingIntervalMap",
    // TODO: enable BTreeReducingRangeMap tests after fixing reported issues
//    "false,BTreeReducingRangeMap",
//    "true,BTreeReducingRangeMap"
    })
    public void testRandomAddsParametrized(boolean endInclusive, TestWith testWith) throws ExecutionException, InterruptedException
    {
        boolean oldEndInclusive = TEST_END_INCLUSIVE;
        TestWith oldTestWith = TEST_WITH;

        try
        {
            TEST_END_INCLUSIVE = endInclusive;
            TEST_WITH = testWith;

            ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            int count = 1_000;
            for (int numberOfAdditions : new int[]{ 1, 10, 100 })
            {
                for (float maxCoveragePerRange : new float[]{ 0.01f, 0.1f })
                {
                    for (float chanceOfMinRoutingKey : new float[]{ 0.01f, 0.1f })
                    {
                        List<ListenableFuture<Void>> results = new ArrayList<>();
                        results.addAll(testRandomAdds(executor, count, 3, numberOfAdditions, 3, maxCoveragePerRange, chanceOfMinRoutingKey));
                        Futures.allAsList(results).get();
                    }
                }
            }
            executor.shutdown();
        }
        finally
        {
            // Restore original values
            TEST_END_INCLUSIVE = oldEndInclusive;
            TEST_WITH = oldTestWith;
        }
    }

    @ParameterizedTest
    @CsvSource({
    "false,ReducingRangeMap",
    "false,ReducingIntervalMap",
    "true,ReducingRangeMap",
    "true,ReducingIntervalMap"
    })
    public void testOneParametrized(boolean endInclusive, TestWith testWith)
    {
        boolean oldEndInclusive = TEST_END_INCLUSIVE;
        TestWith oldTestWith = TEST_WITH;

        try
        {
            TEST_END_INCLUSIVE = endInclusive;
            TEST_WITH = testWith;

            testRandomAdds(8532037884171168001L, 3, 10, 3, 0.100000f, 0.100000f);
        }
        finally
        {
            // Restore original values
            TEST_END_INCLUSIVE = oldEndInclusive;
            TEST_WITH = oldTestWith;
        }
    }

    AtomicInteger counter = new AtomicInteger();

    private List<ListenableFuture<Void>> testRandomAdds(ExecutorService executor, int tests, int numberOfMerges, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinRoutingKey)
    {
        AtomicBoolean failed = new AtomicBoolean(false);
        return ThreadLocalRandom.current()
                                .longs(tests)
                                .mapToObj(seed -> {
                                    SettableFuture<Void> promise = SettableFuture.create();
                                    executor.execute(() -> {
                                        if (failed.get()) // skip
                                        {
                                            promise.set(null);
                                            return;
                                        }

                                        try
                                        {
                                            int idx = counter.getAndIncrement();
                                            if (idx > 0 && idx % 1000 == 0)
                                                System.out.println("Ops done: " + idx);
                                            testRandomAdds(seed, numberOfMerges, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);
                                            promise.set(null);
                                        }
                                        catch (Throwable t)
                                        {
                                            promise.setException(t);
                                            failed.set(true);
                                        }
                                    });
                                    return promise;
                                })
                                .collect(Collectors.toList());
    }

    private void testRandomAdds(long seed, int numberOfMerges, int numberOfAdditions, int maxNumberOfRangesPerAddition, float maxCoveragePerRange, float chanceOfMinRoutingKey)
    {
        String id = String.format("%dl, %d, %d, %d, %ff, %ff", seed, numberOfMerges, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange, chanceOfMinRoutingKey);

        Random random = new Random(seed);
        List<Model> merge = new ArrayList<>();
        while (numberOfMerges-- > 0)
        {
            Model build = new Model();
            build.addRandom(random, numberOfAdditions, maxNumberOfRangesPerAddition, maxCoveragePerRange);
            build.validate(random, id);
            merge.add(build);
        }

        Model check = new Model();
        for (Model add : merge)
        {
            check = check.merge(add);
            check.validate(random, id);
        }
    }

    static class Model
    {
        SystemUnderTest sut;

        Model()
        {
            switch (TEST_WITH)
            {
                case ReducingRangeMap:
                    sut = new ReducingRangeMapSUT();
                    break;
                case ReducingIntervalMap:
                    sut = new ReducingIntervalMapSUT();
                    break;
                case BTreeReducingRangeMap:
                    sut = new BTreeReducingRangeMapSUT();
                    break;
            }
        }

        static class Entry implements Comparable<Entry>
        {
            final Range range;
            final Timestamp timestamp;

            Entry(Range range, Timestamp timestamp)
            {
                this.range = range;
                this.timestamp = timestamp;
            }

            @Override
            public int compareTo(Entry o)
            {
                return range.compareTo(o.range);
            }

            @Override
            public String toString()
            {
                return String.format("(%s: %s)", range, timestamp);
            }
        }

        List<Entry> list = new ArrayList<>();

        Timestamp get(RoutingKey rk)
        {
            Invariants.require(sorted);
            int idxStart = minIdx(list, rk);

            Timestamp ts = null;
            for (int i = idxStart; i < list.size(); i++)
            {
                Entry e = list.get(i);
                if (!e.range.contains(rk))
                    continue;

                // Later ranges can not include routing key
                if (e.range.start().compareTo(rk) > 0)
                    break;

                if (ts == null)
                    ts = e.timestamp;
                else
                    ts = Timestamp.max(ts, e.timestamp);
            }
            return ts;
        }

        // TODO (desired): this can be further optimized
        Model merge(Model other)
        {
            Model result = new Model();
            result.sut = sut.merge(other.sut);
            result.list.addAll(list);
            result.list.addAll(other.list);
            return result;
        }

        void add(Ranges addRanges, Timestamp timestamp)
        {
            sorted = false;
            sut.add(addRanges, timestamp);
            for (Range range : addRanges)
                addCanonical(range, timestamp);
        }

        void addOneRandom(Random random, int maxRangeCount, float maxCoverage)
        {
            int count = maxRangeCount == 1 ? 1 : 1 + random.nextInt(maxRangeCount - 1);
            Timestamp timestamp = ts(random.nextInt(MAX_VALUE));
            List<Range> ranges = new ArrayList<>();
            while (count-- > 0)
            {
                int length = (int) (2 * random.nextDouble() * maxCoverage * MAX_VALUE);
                if (length == 0) length = 1;
                int start = random.nextInt(MAX_VALUE - length - 1);
                Range range = r(start, start + length);
                ranges.add(range);
            }
            add(Ranges.of(ranges.toArray(new Range[0])), timestamp);
        }

        void addRandom(Random random, int count, int maxNumberOfRangesPerAddition, float maxCoveragePerAddition)
        {
            while (count-- > 0)
                addOneRandom(random, maxNumberOfRangesPerAddition, maxCoveragePerAddition);
        }

        private boolean sorted = false;
        void addCanonical(Range range, Timestamp timestamp)
        {
            list.add(new Entry(range, timestamp));
            sorted = false;
        }

        void validate(Random random, String id)
        {
            if (!sorted)
            {
                list.sort(Entry::compareTo);
                sorted = true;
            }
            List<Entry> deoverlapped = deoverlap(list);

            for (Entry entry : list)
            {
                for (RoutingKey rk : new RoutingKey[]{ entry.range.start(), entry.range.end() })
                {
                    Assertions.assertEquals(get(decr(rk)), sut.get(decr(rk)), id);
                    Assertions.assertEquals(get(rk), sut.get(rk), id);
                    Assertions.assertEquals(get(incr(rk)), sut.get(incr(rk)), id);
                }
            }

            { // check some random
                int remaining = 1000;
                while (remaining-- > 0)
                {
                    RoutingKey routingKey = rk(random);
                    if (get(routingKey) != sut.get(routingKey))
                        get(routingKey);
                    Assertions.assertEquals(get(routingKey), sut.get(routingKey), id);
                }
            }

            { // validate foldl
                int remaining = 100;
                while (remaining-- > 0)
                {
                    int count = 1 + random.nextInt(20);
                    RoutingKeys keys;
                    Ranges ranges; // Ranges to check foldl against
                    {
                        RoutingKey[] tmp = new RoutingKey[count];
                        for (int i = 0; i < tmp.length; ++i)
                            tmp[i] = rk(random);
                        keys = RoutingKeys.of(tmp);
                        Range[] tmp2 = new Range[(keys.size() + 1) / 2];
                        int i = 0, c = 0;
                        if (keys.size() % 2 == 1 && random.nextBoolean())
                            tmp2[c++] = r(MIN_EXCL, keys.get(i++));
                        while (i + 1 < keys.size())
                        {
                            tmp2[c++] = r(keys.get(i), keys.get(i + 1));
                            i += 2;
                        }
                        if (i < keys.size())
                            tmp2[c++] = r(keys.get(i++), MAX_EXCL);
                        ranges = Ranges.of(tmp2);
                    }

                    { // Test foldl over keys
                        List<Timestamp> foldl = sut.foldl(keys, (timestamp, timestamps) -> {
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
                    }

                    { // Test foldl over individual ranges
                        for (Range range : ranges)
                        {
                            List<Timestamp> model = new ArrayList<>();
                            List<Timestamp> impl = sut.foldl(Ranges.of(range), (timestamp, timestamps) -> {
                                if (timestamps.isEmpty() || !timestamps.get(timestamps.size() - 1).equals(timestamp))
                                    timestamps.add(timestamp);
                                return timestamps;
                            }, new ArrayList<>(), ignore -> false);

                            int idxStart = minIdx(list, range.start());

                            // First, find all overlapping ranges
                            for (int i = idxStart; i < deoverlapped.size(); i++)
                            {
                                Entry entry = deoverlapped.get(i);
                                if (entry.range.compareIntersecting(range) != 0)
                                    continue;

                                if (model.isEmpty() || !model.get(model.size() - 1).equals(entry.timestamp))
                                    model.add(entry.timestamp);

                                // No further ranges can be found
                                if (range.start().compareTo(entry.range.end()) > 0)
                                    break;
                            }
                            if (!model.equals(impl))
                                deoverlap(list);
                            Assertions.assertEquals(model, impl, id);
                        }
                    }
                }
            }
        }

        private static List<Entry> deoverlap(List<Entry> entries)
        {
            while (true)
            {
                boolean changed = false;
                List<Entry> newEntries = new ArrayList<>();
                for (int i = 0; i < entries.size(); i++)
                {
                    Entry current = entries.get(i);
                    if (i < entries.size() - 1)
                    {
                        Entry candidate = entries.get(i + 1);
                        // If current overlaps with next, deoverlap them into up to 3 ranges
                        if (current.range.compareIntersecting(candidate.range) == 0)
                        {
                            newEntries.addAll(deoverlap(current, candidate));
                            i++;
                            changed = true;
                            continue;
                        }
                    }
                    newEntries.add(current);
                }

                if (changed)
                {
                    newEntries.sort(Entry::compareTo);
                    entries = newEntries;
                    continue;
                }
                return entries;
            }
        }

        private static List<Entry> deoverlap(Entry e1, Entry e2)
        {
            List<Entry> result = new ArrayList<>();

            RoutingKey e1Start = e1.range.start();
            RoutingKey e1End = e1.range.end();
            RoutingKey e2Start = e2.range.start();
            RoutingKey e2End = e2.range.end();

            boolean e1StartInclusive = e1.range.startInclusive();
            boolean e2StartInclusive = e2.range.startInclusive();

            // Find intersection bounds
            RoutingKey intStart = e1Start.compareTo(e2Start) >= 0 ? e1Start : e2Start;
            RoutingKey intEnd = e1End.compareTo(e2End) <= 0 ? e1End : e2End;

            Timestamp maxTimestamp = Timestamp.max(e1.timestamp, e2.timestamp);

            // Add left part of e1 if it extends before intersection
            if (e1Start.compareTo(intStart) < 0)
            {
                Range leftPart = e1StartInclusive
                                 ? new Range.StartInclusive(e1Start, intStart)
                                 : new Range.EndInclusive(e1Start, intStart);
                result.add(new Entry(leftPart, e1.timestamp));
            }

            // Add left part of e2 if it extends before intersection
            if (e2Start.compareTo(intStart) < 0)
            {
                Range leftPart = e2StartInclusive
                                 ? new Range.StartInclusive(e2Start, intStart)
                                 : new Range.EndInclusive(e2Start, intStart);
                result.add(new Entry(leftPart, e2.timestamp));
            }

            // Add intersection part
            Range intersectionRange = e1StartInclusive
                                      ? new Range.StartInclusive(intStart, intEnd)
                                      : new Range.EndInclusive(intStart, intEnd);
            result.add(new Entry(intersectionRange, maxTimestamp));

            // Add right part of e1 if it extends after intersection
            if (intEnd.compareTo(e1End) < 0)
            {
                Range rightPart = e1StartInclusive
                                  ? new Range.StartInclusive(intEnd, e1End)
                                  : new Range.EndInclusive(intEnd, e1End);
                result.add(new Entry(rightPart, e1.timestamp));
            }

            // Add right part of e2 if it extends after intersection
            if (intEnd.compareTo(e2End) < 0)
            {
                Range rightPart = e2StartInclusive
                                  ? new Range.StartInclusive(intEnd, e2End)
                                  : new Range.EndInclusive(intEnd, e2End);
                result.add(new Entry(rightPart, e2.timestamp));
            }

            return result;
        }


        private static int minIdx(List<Model.Entry> list, RoutingKey rk)
        {
            int idxStart = SortedArrays.binarySearch(list, 0, list.size(), rk, (RoutingKey searched, Model.Entry existing) -> {
                return searched.compareTo(existing.range.start()) > 0 ? 0 : -1;
            }, SortedArrays.Search.CEIL);
            if (idxStart < 0) idxStart = -1 - idxStart;
            return idxStart;
        }
    }

    @Test
    public void inclusiveBoundsTest()
    {
        Timestamp ts1 = ts(100);
        ReducingRangeMap<Timestamp> map;

        {
            map = new ReducingRangeMap<>();
            Range endInclusiveRange = new Range.EndInclusive(rk(10), rk(20));
            map = ReducingRangeMap.add(map, Ranges.of(endInclusiveRange), ts1);
            Assertions.assertEquals(ts1, map.get(rk(20)), "End of end-inclusive range should be included");
            Assertions.assertEquals(null, map.get(rk(10)), "Start of end-inclusive range should _not_ be included");
        }

        {
            map = new ReducingRangeMap<>();
            Range startInclusiveRange = new Range.StartInclusive(rk(10), rk(20));
            map = ReducingRangeMap.add(map, Ranges.of(startInclusiveRange), ts1);
            Assertions.assertEquals(ts1, map.get(rk(10)), "Start of end-inclusive range should be included");
            Assertions.assertEquals(null, map.get(rk(20)), "End of end-inclusive range should _not_ be included");
        }
    }

    interface SystemUnderTest
    {
        void add(Ranges ranges, Timestamp timestamp);
        Timestamp get(RoutingKey key);
        SystemUnderTest merge(SystemUnderTest other);
        <V2> V2 foldl(RoutingKeys keys, BiFunction<Timestamp, V2, V2> fold, V2 accumulator, Predicate<V2> terminate);
        <V2> V2 foldl(Ranges ranges, BiFunction<Timestamp, V2, V2> fold, V2 accumulator, Predicate<V2> terminate);
    }

    static class ReducingRangeMapSUT implements SystemUnderTest
    {
        ReducingRangeMap<Timestamp> sut = new ReducingRangeMap<>();

        @Override
        public void add(Ranges ranges, Timestamp timestamp)
        {
            sut = ReducingRangeMap.add(sut, ranges, timestamp);
        }

        @Override
        public Timestamp get(RoutingKey key)
        {
            return sut.get(key);
        }

        @Override
        public SystemUnderTest merge(SystemUnderTest other)
        {
            ReducingRangeMapSUT result = new ReducingRangeMapSUT();
            result.sut = ReducingRangeMap.merge(this.sut, ((ReducingRangeMapSUT) other).sut, Timestamp::max);
            return result;
        }

        @Override
        public <V2> V2 foldl(RoutingKeys keys, BiFunction<Timestamp, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
        {
            return sut.foldl(keys, fold, accumulator, terminate);
        }

        @Override
        public <V2> V2 foldl(Ranges ranges, BiFunction<Timestamp, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
        {
            return sut.foldl(ranges, fold, accumulator, terminate);
        }
    }

    static class ReducingIntervalMapSUT implements SystemUnderTest
    {
        ReducingRangeMap<Timestamp> sut = new ReducingRangeMap<>();

        @Override
        public void add(Ranges ranges, Timestamp timestamp)
        {
            sut = ReducingRangeMap.add(sut, ranges, timestamp);
        }

        @Override
        public Timestamp get(RoutingKey key)
        {
            return sut.get(key);
        }

        @Override
        public SystemUnderTest merge(SystemUnderTest other)
        {
            ReducingIntervalMapSUT result = new ReducingIntervalMapSUT();
            result.sut = ReducingIntervalMap.mergeIntervals(this.sut, ((ReducingIntervalMapSUT) other).sut, IntervalBuilder::new);
            return result;
        }

        @Override
        public <V2> V2 foldl(RoutingKeys keys, BiFunction<Timestamp, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
        {
            return sut.foldl(keys, fold, accumulator, terminate);
        }

        @Override
        public <V2> V2 foldl(Ranges ranges, BiFunction<Timestamp, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
        {
            return sut.foldl(ranges, fold, accumulator, terminate);
        }

        static class IntervalBuilder extends ReducingIntervalMap.AbstractIntervalBuilder<RoutingKey, Timestamp, ReducingRangeMap<Timestamp>>
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
            protected Timestamp tryMergeEqual(Timestamp a, Timestamp b)
            {
                return a.equals(b) ? a : null;
            }

            @Override
            protected ReducingRangeMap<Timestamp> buildInternal()
            {
                return new ReducingRangeMap<>(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Timestamp[0]));
            }
        }
    }

    static class BTreeReducingRangeMapSUT implements SystemUnderTest
    {
        BTreeReducingRangeMap<Timestamp> sut = new BTreeReducingRangeMap<>();

        @Override
        public void add(Ranges ranges, Timestamp timestamp)
        {
            sut = BTreeReducingRangeMap.add(sut, ranges, timestamp);
        }

        @Override
        public Timestamp get(RoutingKey key)
        {
            return sut.get(key);
        }

        @Override
        public SystemUnderTest merge(SystemUnderTest other)
        {
            BTreeReducingRangeMapSUT sut = new BTreeReducingRangeMapSUT();
            //TODO (required): enable second path after fixing issues with B-Tree Range map
            if (true)
                sut.sut = BTreeReducingRangeMap.merge(this.sut, ((BTreeReducingRangeMapSUT) other).sut, Timestamp::mergeMax);
            else
                sut.sut = BTreeReducingRangeMap.mergeIntervals(this.sut, ((BTreeReducingRangeMapSUT) other).sut, IntervalBuilder::new);
            return sut;
        }

        @Override
        public <V2> V2 foldl(RoutingKeys keys, BiFunction<Timestamp, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
        {
            return sut.foldl(keys, fold, accumulator, terminate);
        }

        @Override
        public <V2> V2 foldl(Ranges ranges, BiFunction<Timestamp, V2, V2> fold, V2 accumulator, Predicate<V2> terminate)
        {
            return sut.foldl(ranges, fold, accumulator, terminate);
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
                return Timestamp.mergeMax(a, b);
            }

            @Override
            protected Timestamp tryMergeEqual(@Nonnull Timestamp a, Timestamp b)
            {
                return a.equals(b) ? a : null;
            }

            @Override
            protected BTreeReducingRangeMap<Timestamp> buildInternal(Object[] tree)
            {
                return new BTreeReducingRangeMap<>(inclusiveEnds, tree);
            }
        }
    }
}