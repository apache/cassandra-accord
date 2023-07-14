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
import java.util.BitSet;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import static accord.utils.Property.qt;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleBitSetTest
{
    private static final int NOT_FOUND = Integer.MAX_VALUE;

    private static class Check
    {
        final SimpleBitSet test;
        final BitSet canon;
        final int size;

        private Check(SimpleBitSet test, BitSet canon, int size)
        {
            this.test = test;
            this.canon = canon;
            this.size = size;
        }

        void check(RandomSource random)
        {
            assertEquals(canon.cardinality(), test.getSetBitCount());
            assertEquals(canon.nextSetBit(0), test.firstSetBit());
            assertEquals(normaliseNotFound(canon.nextSetBit(0)), test.firstSetBit(NOT_FOUND));
            assertEquals(canon.previousSetBit(size), test.lastSetBit());
            assertEquals(normaliseNotFound(canon.previousSetBit(size)), test.lastSetBit(NOT_FOUND));

            forIndices(random, i -> assertEquals(canon.nextSetBit(i), test.nextSetBit(i)));
            forIndices(random, i -> assertEquals(normaliseBefore(canon.nextSetBit(0), i), test.firstSetBitBefore(i)));
            forIndices(random, i -> assertEquals(normaliseNotFoundBefore(canon.nextSetBit(0), i), test.firstSetBitBefore(i, NOT_FOUND)));
            forRanges(random, (i, j) -> assertEquals(normaliseBefore(canon.nextSetBit(i), j), test.nextSetBitBefore(i, j)));
            forRanges(random, (i, j) -> assertEquals(normaliseNotFoundBefore(canon.nextSetBit(i), j), test.nextSetBitBefore(i, j, NOT_FOUND)));

            forIndices(random, i -> assertEquals(i == 0 ? -1 : canon.previousSetBit(i - 1), test.prevSetBit(i)));
            forIndices(random, i -> assertEquals(normaliseNotBefore(size == 0 ? -1 : canon.previousSetBit(size - 1), i), test.lastSetBitNotBefore(i)));
            forIndices(random, i -> assertEquals(normaliseNotFoundNotBefore(size == 0 ? -1 : canon.previousSetBit(size - 1), i), test.lastSetBitNotBefore(i, NOT_FOUND)));
            forRanges(random, (i, j) -> assertEquals(normaliseNotBefore(j == 0 ? -1 : canon.previousSetBit(j - 1), i), test.prevSetBitNotBefore(j, i)));
            forRanges(random, (i, j) -> assertEquals(normaliseNotFoundNotBefore(j == 0 ? -1 : canon.previousSetBit(j - 1), i), test.prevSetBitNotBefore(j, i, NOT_FOUND)));

            List<Integer> canonCollect = new ArrayList<>(), testCollect = new ArrayList<>();
            canon.stream().forEachOrdered(canonCollect::add);
            test.forEach(testCollect, List::add);
            assertEquals(canonCollect, testCollect);

            canonCollect = Lists.reverse(canonCollect);
            testCollect.clear();
            test.reverseForEach(testCollect, List::add);
            assertEquals(canonCollect, testCollect);
        }

        void forIndices(RandomSource random, IntConsumer consumer)
        {
            for (int c = 0 ; c < 100 ; ++c)
            {
                int i = random.nextInt(size + 1);
                consumer.accept(i);
            }
        }

        void forRanges(RandomSource random, BiConsumer<Integer, Integer> consumer)
        {
            for (int c = 0 ; c < 100 ; ++c)
            {
                int i = random.nextInt(size + 1);
                int j = random.nextInt(size + 1);
                if (i > j) { int t = i; i = j; j = t; }
                consumer.accept(i, j);
            }
        }

        static Check generate(RandomSource random, int maxSize, int modCount, int runLength, float runChance, float clearChance)
        {
            int size = random.nextInt(maxSize);
            runLength = Math.min(size, runLength);
            BitSet canon = new BitSet(size);
            SimpleBitSet test = new SimpleBitSet(size);
            if (size > 0)
            {
                while (modCount-- > 0)
                {
                    boolean set = random.nextFloat() >= clearChance;
                    boolean run = runLength > 1 && random.nextFloat() < runChance;
                    if (run)
                    {
                        int i = random.nextInt(size);
                        int j = random.nextInt(size);
                        if (j < i) { int t = i; i = j; j = t; }
                        j = Math.min(i + 1 + random.nextInt(runLength - 1), j);

                        if (set)
                        {
                            canon.set(i, j);
                            test.setRange(i, j);
                        }
                        else
                        {
                            canon.clear(i, j);
                            while (i < j)
                                test.unset(i++);
                        }
                    }
                    else
                    {
                        int i = random.nextInt(size);
                        if (set)
                        {
                            assertEquals(!canon.get(i), test.set(i));
                            canon.set(i);
                        }
                        else
                        {
                            assertEquals(canon.get(i), test.unset(i));
                            canon.clear(i);
                        }
                    }
                    assertEquals(canon.cardinality(), test.getSetBitCount());
                }
            }
            return new Check(test, canon, size);
        }
    }

    @Test
    public void testRandomBitSets()
    {
        qt().withExamples(100000).forAll(Gens.random()).check(SimpleBitSetTest::testRandomBitSet);
    }
    
    private static void testRandomBitSet(RandomSource random)
    {
        Check.generate(random, 1000, 1 + random.nextInt(99), random.nextInt(100), random.nextFloat() * 0.1f, random.nextFloat() * 0.5f)
             .check(random);
    }

    static int normaliseNotFound(int i)
    {
        return i == -1 ? NOT_FOUND : i;
    }

    static int normaliseNotBefore(int i, int notBefore)
    {
        return i < notBefore ? -1 : i;
    }

    static int normaliseNotFoundNotBefore(int i, int notBefore)
    {
        return i < notBefore ? NOT_FOUND : i;
    }

    static int normaliseBefore(int i, int before)
    {
        return i >= before ? -1 : i;
    }

    static int normaliseNotFoundBefore(int i, int before)
    {
        return i >= before || i == -1 ? NOT_FOUND : i;
    }

}
