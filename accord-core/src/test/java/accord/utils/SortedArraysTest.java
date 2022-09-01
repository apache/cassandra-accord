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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static accord.utils.Property.qt;

class SortedArraysTest
{
    @Test
    public void testRemapperSequential()
    {
        qt().forAll(remapperSequentialSubset(sortedUniqueIntegerArray(1))).check(p -> {
            Integer[] src = p.src;
            Integer[] trg = p.trg;
            // use false so null is never returned
            int[] result = SortedArrays.remapToSuperset(src, src.length, trg, trg.length, int[]::new);
            for (int i = 0; i < result.length; i++)
                assertRemapperProperty(i, src, trg, result);
        });
    }

    @Test
    public void testRemapperParial()
    {
        qt().forAll(remappedPartialSubset(sortedUniqueIntegerArray(0))).check(p -> {
            Integer[] src = p.src;
            Integer[] trg = p.trg;
            // use false so null is never returned
            int[] result = SortedArrays.remapToSuperset(src, src.length, trg, trg.length, int[]::new);
            if (src.length == trg.length)
            {
                Assertions.assertNull(result, "should be null since src == trg");
                return;
            }
            for (int i = 0; i < result.length; i++)
                assertRemapperProperty(i, src, trg, result);
        });
    }

    private static <T> void assertRemapperProperty(int i, T[] src, T[] trg, int[] result)
    {
        if (!Objects.equals(src[i], trg[result[i]]))
        {
            StringBuilder sb = new StringBuilder();
            sb.append("i = ").append(i).append('\n');
            sb.append(src[i]).append(" != ").append(trg[result[i]]).append('\n');
            sb.append("src:\n").append(Arrays.toString(src)).append('\n');
            sb.append("trg:\n").append(Arrays.toString(trg)).append('\n');
            String str = Arrays.toString(result);
            sb.append("result:\n").append(str).append('\n');
            for (int j = 0, idx = indexOfNth(str, ",", i); j <= idx; j++)
                sb.append(' ');
            sb.append('^');
            throw new AssertionError(sb.toString());
        }
    }

    @Test
    public void testSearch()
    {
        qt().forAll(Gens.arrays(Gens.ints().all()).unique().ofSizeBetween(1, 100)).check(array -> {
            Arrays.sort(array);
            Integer[] jint = IntStream.of(array).mapToObj(Integer::valueOf).toArray(Integer[]::new);
            for (int i = 0; i < array.length; i++)
            {
                int find = array[i];
                int expectedOnToExclusion = -(i + 1);

                Assertions.assertEquals(i, SortedArrays.exponentialSearch(array, 0, array.length, find));
                Assertions.assertEquals(expectedOnToExclusion, SortedArrays.exponentialSearch(array, 0, i, find));

                for (SortedArrays.Search search : SortedArrays.Search.values())
                {
                    Assertions.assertEquals(i, SortedArrays.exponentialSearch(jint, 0, array.length, find, Integer::compare, search));
                    Assertions.assertEquals(expectedOnToExclusion, SortedArrays.exponentialSearch(jint, 0, i, find, Integer::compare, search));

                    Assertions.assertEquals(i, SortedArrays.binarySearch(jint, 0, array.length, find, Integer::compare, search));
                    Assertions.assertEquals(expectedOnToExclusion, SortedArrays.binarySearch(jint, 0, i, find, Integer::compare, search));
                }

                // uses Search.FAST
                long lr = SortedArrays.findNextIntersection(jint, i, jint, i);
                int left = (int) (lr >>> 32);
                int right = (int) lr;
                Assertions.assertEquals(left, right);
                Assertions.assertEquals(left, i);

                // uses Search.CEIL
                lr = SortedArrays.findNextIntersectionWithMultipleMatches(jint, i, jint, i, Integer::compare, Integer::compare);
                left = (int) (lr >>> 32);
                right = (int) lr;
                Assertions.assertEquals(left, right);
                Assertions.assertEquals(left, i);
            }
        });
    }

    @Test
    public void testNextIntersection()
    {
        Gen<Integer[]> gen = Gens.arrays(Integer.class, Gens.ints().between(0, 100))
                .unique()
                .ofSizeBetween(0, 10)
                .map(a -> {
                    Arrays.sort(a);
                    return a;
                });
        qt().forAll(gen, gen).check((a, b) -> {
            // find all intersections
            List<Long> expected = new ArrayList<>();
            for (int i = 0; i < a.length; i++)
            {
                for (int j = 0; j < b.length; j++)
                {
                    if (a[i].equals(b[j]))
                    {
                        long ab = ((long)i << 32) | j;
                        expected.add(ab);
                    }
                }
            }
            List<Long> seen = new ArrayList<>(expected.size());
            int ai = 0, bi = 0;
            while (ai < a.length && bi < b.length)
            {
                long ab = SortedArrays.findNextIntersection(a, ai, b, bi, Integer::compare);
                if (ab == -1)
                    return; // no more matches...
                seen.add(ab);
                ai = (int) (ab >>> 32);
                bi = (int) ab;
                // since data is unique just "consume" both pointers so can find the next one
                ai++;
                bi++;
            }
            assertArrayEquals(expected.toArray(), seen.toArray());
        });
    }

    @Test
    public void testLinearUnion()
    {
        Gen<Integer[]> gen = sortedUniqueIntegerArray(0);
        qt().forAll(gen, gen).check((a, b) -> {
            Set<Integer> seen = new HashSet<>();
            Stream.of(a).forEach(seen::add);
            Stream.of(b).forEach(seen::add);
            Integer[] expected = seen.toArray(Integer[]::new);
            Arrays.sort(expected);

            assertArrayEquals(expected, SortedArrays.linearUnion(a, b, Integer[]::new));
            assertArrayEquals(expected, SortedArrays.linearUnion(b, a, Integer[]::new));
        });
    }

    @Test
    public void testLinearIntersection()
    {
        Gen<Integer[]> gen = sortedUniqueIntegerArray(0);
        qt().forAll(gen, gen).check((a, b) -> {
            Set<Integer> left = new HashSet<>(Arrays.asList(a));
            Set<Integer> right = new HashSet<>(Arrays.asList(b));
            Set<Integer> intersection = Sets.intersection(left, right);
            Integer[] expected = intersection.toArray(Integer[]::new);
            Arrays.sort(expected);

            assertArrayEquals(expected, SortedArrays.linearIntersection(a, b, Integer[]::new));
            assertArrayEquals(expected, SortedArrays.linearIntersection(b, a, Integer[]::new));
        });
    }

    @Test
    public void testLinearIntersectionWithSubset()
    {
        class P
        {
            final Integer[] full, subset;

            P(Integer[] full, Integer[] subset) {
                this.full = full;
                this.subset = subset;
            }
        }
        Gen<P> gen = Gens.arrays(Integer.class, Gens.ints().all())
                .unique()
                .ofSizeBetween(2, 100)
                .map(a -> {
                    Arrays.sort(a);
                    return a;
                }).map((r, full) -> {
                    int to = r.nextInt(1, full.length);
                    int offset = r.nextInt(0, to);
                    return new P(full, Arrays.copyOfRange(full, offset, to));
                });
        qt().forAll(gen).check(p -> {
            Integer[] expected = p.subset;
            // use assertEquals to detect expected pointer-equals actual
            // this is to make sure the optimization to detect perfect subsets is the path used for the return
            Assertions.assertEquals(expected, SortedArrays.linearIntersection(p.full, p.subset, Integer[]::new));
            Assertions.assertEquals(expected, SortedArrays.linearIntersection(p.subset, p.full, Integer[]::new));
        });
    }

    @Test
    public void testLinearDifference()
    {
        Gen<Integer[]> gen = sortedUniqueIntegerArray(0);
        qt().forAll(gen, gen).check((a, b) -> {
            Set<Integer> left = new HashSet<>(Arrays.asList(a));
            Set<Integer> right = new HashSet<>(Arrays.asList(b));

            {
                Set<Integer> difference = Sets.difference(left, right);
                Integer[] expected = difference.toArray(Integer[]::new);
                Arrays.sort(expected);
                assertArrayEquals(expected, SortedArrays.linearDifference(a, b, Integer[]::new));
            }
            {
                Set<Integer> difference = Sets.difference(right, left);
                Integer[] expected = difference.toArray(Integer[]::new);
                Arrays.sort(expected);
                assertArrayEquals(expected, SortedArrays.linearDifference(b, a, Integer[]::new));
            }
        });
    }

    @Test
    public void testInsert()
    {
        Gen<Integer[]> gen = Gens.arrays(Integer.class, Gens.ints().all()).ofSizeBetween(0, 42).map(a -> {
            Arrays.sort(a);
            return a;
        });
        qt().forAll(gen, Gens.random()).check((array, random) -> {
            // insert w/e already exists
            Set<Integer> uniq = new HashSet<>();
            for (int i = 0; i < array.length; i++)
            {
                Integer value = array[i];
                Assertions.assertEquals(array, SortedArrays.insert(array, value, Integer[]::new));
                uniq.add(value);
            }

            // insert something not present
            int numToAdd = random.nextInt(1, 10);
            List<Integer> expected = new ArrayList<>(array.length + numToAdd);
            expected.addAll(Arrays.asList(array));
            for (int i = 0; i < numToAdd; i++)
            {
                int value;
                while (!uniq.add((value = random.nextInt()))) {}
                Integer[] updated = SortedArrays.insert(array, value, Integer[]::new);
                Assertions.assertNotEquals(array, updated);
                expected.add(value);
                array = updated;
            }
            Collections.sort(expected);
            assertArrayEquals(expected.toArray(new Integer[0]), array);
        });
    }

    private static int indexOfNth(String original, String search, int n)
    {
        String str = original;
        for (int i = 0; i < n; i++)
        {
            int idx = str.indexOf(search);
            if (idx < 0)
                throw new AssertionError("Unable to find next " + search);
            str = str.substring(idx + 1);
        }
        return original.length() - str.length();
    }

    private static <T extends Comparable<T>> Gen<Pair<T>> remapperSequentialSubset(Gen<T[]> gen)
    {
        return random -> {
            T[] target = remapperArray(gen.next(random));
            int to = random.nextInt(0, target.length);
            int offset = to == 0 ? 0 : random.nextInt(0, to);
            T[] src = Arrays.copyOfRange(target, offset, to);
            return new Pair<>(src, target);
        };
    }

    private static <T extends Comparable<T>> Gen<Pair<T>> remappedPartialSubset(Gen<T[]> gen)
    {
        return random -> {
            T[] target = remapperArray(gen.next(random));
            T[] src = target.length == 0 ? target :  Stream.of(target).filter(i -> random.nextBoolean()).toArray(size -> (T[]) Array.newInstance(target[0].getClass(), size));
            return new Pair<>(src, target);
        };
    }

    private static <T extends Comparable<T>> T[] remapperArray(T[] target) {
        // remapper requires data to be sorted and unique, so make sure this is true
        Arrays.sort(target);
        for (int i = 1; i < target.length; i++)
            Preconditions.checkArgument(!target[i - 1].equals(target[i]));
        return target;
    }

    private static Gen<Integer[]> sortedUniqueIntegerArray(int minSize) {
        return Gens.arrays(Integer.class, Gens.ints().all())
                .unique()
                .ofSizeBetween(minSize, 100)
                .map(a -> {
                    Arrays.sort(a);
                    return a;
                });
    }

    private static void assertArrayEquals(Object[] expected, Object[] actual)
    {
        Assertions.assertArrayEquals(expected, actual, () -> {
            // default error msg is not as useful as it could be, and since this class always works with sorted data
            // attempt to find first miss-match and return a useful log
            StringBuilder sb = new StringBuilder();
            sb.append("Expected: ").append(Arrays.toString(expected)).append('\n');
            sb.append("Actual:   ").append(Arrays.toString(actual)).append('\n');
            int length = Math.min(expected.length, actual.length);
            if (length != 0)
            {
                for (int i = 0; i < length; i++)
                {
                    Object l = expected[i];
                    Object r = actual[i];
                    if (!Objects.equals(l, r))
                        sb.append("Difference detected at index ").append(i).append("; left=").append(l).append(", right=").append(r).append('\n');
                }
            }
            return sb.toString();
        });
    }

    private static class Pair<T>
    {
        private final T[] src;
        private final T[] trg;

        private Pair(T[] src, T[] trg) {
            this.src = src;
            this.trg = trg;
        }

        @Override
        public String toString() {
            return "Pair{" +
                    "src=" + Arrays.toString(src) +
                    ", trg=" + Arrays.toString(trg) +
                    '}';
        }
    }
}