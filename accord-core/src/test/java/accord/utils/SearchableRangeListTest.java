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
import java.util.Comparator;
import java.util.List;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.Test;

import accord.impl.IntKey;
import accord.primitives.Range;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

class SearchableRangeListTest
{
    @Test
    public void fullWorld()
    {
        int numRanges = 1000;
        List<Range> ranges = new ArrayList<>(numRanges);
        for (int i = 0; i < numRanges; i++)
            ranges.add(IntKey.range(i, i + 1));

        SearchableRangeList list = SearchableRangeList.build(ranges.toArray(new Range[0]));
        class Counter { int value;}
        BiConsumer<Integer, Integer> test = (rangeStart, rangeEnd) -> {
            Counter counter = new Counter();
            list.forEach(IntKey.range(rangeStart, rangeEnd), (a, b, c, d, e) -> {
                counter.value++;
            }, (a, b, c, d, start, end) -> {
                counter.value += (end - start + 1);
            }, 0, 0, 0, 0, 0);
            Assertions.assertThat(counter.value).isEqualTo(rangeEnd - rangeStart + 1);
        };
        for (int i = 0; i < numRanges; i++)
            test.accept(i, numRanges);
        for (int i = 0; i < numRanges; i++)
            test.accept(0, numRanges - i);
    }

    @Test
    public void random()
    {
        qt().check(rs -> {
            int numRanges = rs.nextInt(1000, 10000);
            List<Range> ranges = new ArrayList<>(numRanges);
            for (int i = 0; i < numRanges; i++)
            {
                int start = rs.nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE - 1000);
                int offset = rs.nextInt(1, 1000);
                ranges.add(IntKey.range(start, start + offset));
            }
            ranges.sort(Comparator.comparing(Range::start));

            SearchableRangeList list = SearchableRangeList.build(ranges.toArray(new Range[0]));
            for (int i = 0; i < 1000; i++)
            {
                Range range;
                int selection = rs.nextInt(0, 3);
                switch (selection)
                {
                    case 0:
                        range = rs.pick(ranges);
                        break;
                    case 1:
                        int rangeStart = rs.nextInt(Integer.MIN_VALUE, Integer.MAX_VALUE - 1000);
                        int offset = rs.nextInt(1, 1000);
                        range = IntKey.range(rangeStart, rangeStart + offset);
                        break;
                    case 2:
                        int start = rs.nextInt(0, ranges.size());
                        int end = start + rs.nextInt(0, (ranges.size() - start));
                        range = IntKey.range(((IntKey) ranges.get(start).start()).key, ((IntKey) ranges.get(end).end()).key);
                        break;
                    default:
                        throw new IllegalStateException("Unhandled value");
                }
                List<Range> expected = new ArrayList<>();
                for (Range r : ranges)
                {
                    if (range.compareIntersecting(r) == 0)
                        expected.add(r);
                }
                List<Range> actual = new ArrayList<>(expected.size());
                list.forEach(range, (a, b, c, d, idx) -> {
                    actual.add(list.ranges[idx]);
                }, (a, b, c, d, start, end) -> {
                    for (int j = start; j < end; j++)
                        actual.add(list.ranges[j]);
                }, 0, 0, 0, 0, 0);

                Assertions.assertThat(actual).isEqualTo(expected);
            }
        });
    }
}