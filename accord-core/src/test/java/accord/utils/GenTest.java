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

import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

public class GenTest {
    @Test
    public void randomNextInt()
    {
        qt().forAll(Gens.random()).check(r -> {
            int value = r.nextInt(1, 10);
            if (value < 1)
                throw new AssertionError(value + " is less than 1");
            if (value >= 10)
                throw new AssertionError(value + " is >= " + 10);
        });
    }

    @Test
    public void listUnique()
    {
        qt().withExamples(1000).forAll(Gens.lists(Gens.longs().all()).unique().ofSize(1000)).check(list -> {
            SortedSet<Long> unique = new TreeSet<>(list);
            Assertions.assertEquals(unique.size(), list.size(), () -> {
                Collections.sort(list);
                return "Expected " + list + " to be equal to " + unique + " but had different sizes";
            });
        });
    }

    @Test
    public void arrayUnique()
    {
        qt().withExamples(1000).forAll(Gens.arrays(Long.class, Gens.longs().all()).unique().ofSize(1000)).check(array -> {
            List<Long> list = Arrays.asList(array);
            SortedSet<Long> unique = new TreeSet<>(list);
            Assertions.assertEquals(unique.size(), list.size(), () -> {
                Collections.sort(list);
                return "Expected " + list + " to be equal to " + unique + " but had different sizes";
            });
        });
    }

    @Test
    public void intArrayUnique()
    {
        qt().withExamples(1000).forAll(Gens.arrays(Gens.ints().all()).unique().ofSize(1000)).check(array -> {
            List<Integer> list = new ArrayList<>(array.length);
            IntStream.of(array).forEach(list::add);
            SortedSet<Integer> unique = new TreeSet<>(list);
            Assertions.assertEquals(unique.size(), list.size(), () -> {
                Collections.sort(list);
                return "Expected " + list + " to be equal to " + unique + " but had different sizes";
            });
        });
    }

    @Test
    public void longArrayUnique()
    {
        qt().withExamples(1000).forAll(Gens.arrays(Gens.longs().all()).unique().ofSize(1000)).check(array -> {
            List<Long> list = new ArrayList<>(array.length);
            LongStream.of(array).forEach(list::add);
            SortedSet<Long> unique = new TreeSet<>(list);
            Assertions.assertEquals(unique.size(), list.size(), () -> {
                Collections.sort(list);
                return "Expected " + list + " to be equal to " + unique + " but had different sizes";
            });
        });
    }

    enum PickWeight {A, B, C}

    @Test
    public void pickWeight()
    {
        int samples = 1000;
        Gen<PickWeight> enums = Gens.enums().allWithWeights(PickWeight.class, 81, 10, 1);
        Gen<Map<PickWeight, Integer>> gen = rs -> {
            Map<PickWeight, Integer> counts = new EnumMap<>(PickWeight.class);
            for (int i = 0; i < samples; i++)
                counts.compute(enums.next(rs), (ignore, accum) -> accum == null ? 1 : accum + 1);
            return counts;
        };
        qt().forAll(gen).check(counts -> {
            // expected 810
            assertThat(counts.get(PickWeight.A)).isGreaterThan(counts.get(PickWeight.B));
            // expected 100
            assertThat(counts.get(PickWeight.B))
                    .isBetween(50, 200);

            if (counts.containsKey(PickWeight.C))
            {
                assertThat(counts.get(PickWeight.B))
                        .isGreaterThan(counts.get(PickWeight.C));

                // expected 10
                assertThat(counts.get(PickWeight.C))
                        .isBetween(1, 60);
            }
        });
    }

    @Test
    public void runs()
    {
        double ratio = 0.0625;
        int samples = 1000;
        Gen<Runs> gen = Gens.lists(Gens.bools().biasedRepeatingRuns(ratio, 15)).ofSize(samples).map(Runs::new);
        qt().forAll(gen).check(runs -> {
            assertThat(IntStream.of(runs.runs).filter(i -> i > 5).toArray()).isNotEmpty();
            assertThat(runs.counts.get(true) / 1000.0).isBetween(ratio * .5, 0.1);
        });
    }

    @Test
    public void zipf()
    {
        final int samples = 1_000;
        int maxRetries = 100;
        qt().check(rs -> {
            int size = rs.nextInt(2, 100);
            int[] array = IntStream.range(0, size).toArray();
            int[] counts = new int[array.length];
            Gen.IntGen gen = Gens.pickZipf(array);
            outter: for (int retries = 0; retries < maxRetries; retries++)
            {
                boolean lastAttempt = retries == maxRetries - 1;
                for (int i = 0; i < samples; i++)
                    counts[Arrays.binarySearch(array, gen.nextInt(rs))]++;
                int baseline = counts[0];
                String histogram = histogram(array, counts);
                for (int i = 1; i < counts.length; i++)
                {
                    int count = counts[i];
                    int expected = baseline / (i + 1);
                    double wiggleRoom = .3d;
                    double min = expected * (1d - wiggleRoom);
                    double max = expected * (1d + wiggleRoom);
                    if (lastAttempt)
                    {
                        org.assertj.core.api.Assertions.assertThat(count).describedAs("Expected %dth element to be 1/%d of %d;\n%s", (i + 1), (i + 1), baseline, histogram).isBetween((int) Math.ceil(min), (int) Math.floor(max));
                    }
                    else
                    {
                        if (count < min || count > max)
                            continue outter; // add more samples...
                    }
                }
                break;
            }
        });
    }

    private static String histogram(int[] array, int[] counts)
    {
        long total = IntStream.of(counts).sum();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < array.length; i++)
        {
            int value = array[i];
            int count = counts[i];
            int p = (int) ((count / (double) total) * 100d);
            sb.append(value).append(" (").append(count).append(")").append(": ");
            for (int j = 0; j < p; j++)
                sb.append('*');
            sb.append('\n');
        }
        return sb.toString();
    }

    private static class Runs
    {
        private final Map<Boolean, Long> counts;
        private final int[] runs;

        Runs(List<Boolean> samples)
        {
            this.counts = samples.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            IntArrayList runs = new IntArrayList();
            int run = -1;
            for (boolean b : samples)
            {
                if (b)
                {
                    run = run == -1 ? 1 : run + 1;
                }
                else if (run != -1)
                {
                    runs.add(run);
                    run = -1;
                }
            }
            this.runs = runs.toIntArray();
        }
    }
}
