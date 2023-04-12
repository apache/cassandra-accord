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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;


public class Gens {
    private Gens() {
    }

    public static <T> Gen<T> constant(T constant)
    {
        return ignore -> constant;
    }

    public static <T> Gen<T> constant(Supplier<T> constant)
    {
        return ignore -> constant.get();
    }

    public static <T> Gen<T> pick(T... ts)
    {
        return pick(Arrays.asList(ts));
    }

    public static <T> Gen<T> pick(List<T> ts)
    {
        Gen.IntGen offset = ints().between(0, ts.size() - 1);
        return rs -> ts.get(offset.nextInt(rs));
    }

    public static Gen<char[]> charArray(Gen.IntGen sizes, char[] domain)
    {
        return charArray(sizes, domain, (a, b) -> true);
    }

    public interface IntCharBiPredicate
    {
        boolean test(int a, char b);
    }

    public static Gen<char[]> charArray(Gen.IntGen sizes, char[] domain, IntCharBiPredicate fn)
    {
        Gen.IntGen indexGen = ints().between(0, domain.length - 1);
        return rs -> {
            int size = sizes.nextInt(rs);
            char[] is = new char[size];
            for (int i = 0; i != size; i++)
            {
                char c;
                do
                {
                    c = domain[indexGen.nextInt(rs)];
                }
                while (!fn.test(i, c));
                is[i] = c;
            }
            return is;
        };
    }

    public static Gen<RandomSource> random() {
        return r -> r;
    }

    public static BooleanDSL bools()
    {
        return new BooleanDSL();
    }

    public static IntDSL ints()
    {
        return new IntDSL();
    }

    public static LongDSL longs() {
        return new LongDSL();
    }

    public static <T> ListDSL<T> lists(Gen<T> fn) {
        return new ListDSL<>(fn);
    }

    public static <T> ArrayDSL<T> arrays(Class<T> type, Gen<T> fn) {
        return new ArrayDSL<>(type, fn);
    }

    public static IntArrayDSL arrays(Gen.IntGen fn) {
        return new IntArrayDSL(fn);
    }

    public static LongArrayDSL arrays(Gen.LongGen fn) {
        return new LongArrayDSL(fn);
    }

    public static EnumDSL enums()
    {
        return new EnumDSL();
    }

    public static StringDSL strings()
    {
        return new StringDSL();
    }

    public static class BooleanDSL
    {
        public Gen<Boolean> all()
        {
            return RandomSource::nextBoolean;
        }
    }

    public static class IntDSL
    {
        public Gen.IntGen of(int value)
        {
            return r -> value;
        }

        public Gen.IntGen all()
        {
            return RandomSource::nextInt;
        }

        public Gen.IntGen between(int min, int max)
        {
            Invariants.checkArgument(max >= min, "max (%d) < min (%d)", max, min);
            if (min == max)
                return of(min);
            // since bounds is exclusive, if max == max_value unable to do +1 to include... so will return a gen
            // that does not include
            if (max == Integer.MAX_VALUE)
                return r -> r.nextInt(min, max);
            return r -> r.nextInt(min, max + 1);
        }
    }

    public static class LongDSL {
        public Gen.LongGen of(long value)
        {
            return r -> value;
        }

        public Gen.LongGen all() {
            return RandomSource::nextLong;
        }

        public Gen.LongGen between(long min, long max) {
            Invariants.checkArgument(max >= min);
            if (min == max)
                return of(min);
            // since bounds is exclusive, if max == max_value unable to do +1 to include... so will return a gen
            // that does not include
            if (max == Long.MAX_VALUE)
                return r -> r.nextLong(min, max);
            return r -> r.nextLong(min, max + 1);
        }
    }

    public static class EnumDSL
    {
        public <T extends Enum<T>> Gen<T> all(Class<T> klass)
        {
            return pick(klass.getEnumConstants());
        }
    }
    
    public static class StringDSL
    {
        public Gen<String> of(Gen.IntGen sizes, char[] domain)
        {
            // note, map is overloaded so String::new is ambugious to javac, so need a lambda here
            return charArray(sizes, domain).map(c -> new String(c));
        }

        public SizeBuilder<String> of(char[] domain)
        {
            return new SizeBuilder<>(sizes -> of(sizes, domain));
        }

        public Gen<String> of(Gen.IntGen sizes, char[] domain, IntCharBiPredicate fn)
        {
            // note, map is overloaded so String::new is ambugious to javac, so need a lambda here
            return charArray(sizes, domain, fn).map(c -> new String(c));
        }

        public SizeBuilder<String> of(char[] domain, IntCharBiPredicate fn)
        {
            return new SizeBuilder<>(sizes -> of(sizes, domain, fn));
        }

        public Gen<String> all(Gen.IntGen sizes)
        {
            return betweenCodePoints(sizes, Character.MIN_CODE_POINT, Character.MAX_CODE_POINT);
        }

        public SizeBuilder<String> all()
        {
            return new SizeBuilder<>(this::all);
        }

        public Gen<String> ascii(Gen.IntGen sizes)
        {
            return betweenCodePoints(sizes, 0, 127);
        }

        public SizeBuilder<String> ascii()
        {
            return new SizeBuilder<>(this::ascii);
        }

        public Gen<String> asciiVisible(Gen.IntGen sizes)
        {
            return betweenCodePoints(sizes, 33, 127);
        }

        public SizeBuilder<String> asciiVisible()
        {
            return new SizeBuilder<>(this::asciiVisible);
        }

        public Gen<String> betweenCodePoints(Gen.IntGen sizes, int min, int max)
        {
            Gen.IntGen codePointGen = ints().between(min, max).filter(Character::isDefined);
            return rs -> {
                int[] array = new int[sizes.nextInt(rs)];
                for (int i = 0; i < array.length; i++)
                    array[i] = codePointGen.nextInt(rs);
                return new String(array, 0, array.length);
            };
        }

        public SizeBuilder<String> betweenCodePoints(int min, int max)
        {
            return new SizeBuilder<>(sizes -> betweenCodePoints(sizes, min, max));
        }
    }

    public static class SizeBuilder<T>
    {
        private final Function<Gen.IntGen, Gen<T>> fn;

        public SizeBuilder(Function<Gen.IntGen, Gen<T>> fn)
        {
            this.fn = fn;
        }

        public Gen<T> ofLength(int fixed)
        {
            return ofLengthBetween(fixed, fixed);
        }

        public Gen<T> ofLengthBetween(int min, int max)
        {
            return fn.apply(ints().between(min, max));
        }
    }

    public static class ListDSL<T> implements BaseSequenceDSL<ListDSL<T>, List<T>> {
        private final Gen<T> fn;

        public ListDSL(Gen<T> fn) {
            this.fn = Objects.requireNonNull(fn);
        }

        @Override
        public ListDSL<T> unique()
        {
            return new ListDSL<>(new GenReset<>(fn));
        }

        @Override
        public Gen<List<T>> ofSizeBetween(int minSize, int maxSize) {
            Gen.IntGen sizeGen = ints().between(minSize, maxSize);
            return r ->
            {
                Reset.tryReset(fn);
                int size = sizeGen.nextInt(r);
                List<T> list = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                    list.add(fn.next(r));
                return list;
            };
        }
    }

    public static class ArrayDSL<T> implements BaseSequenceDSL<ArrayDSL<T>, T[]> {
        private final Class<T> type;
        private final Gen<T> fn;

        public ArrayDSL(Class<T> type, Gen<T> fn) {
            this.type = Objects.requireNonNull(type);
            this.fn = Objects.requireNonNull(fn);
        }

        @Override
        public ArrayDSL<T> unique()
        {
            return new ArrayDSL<>(type, new GenReset<>(fn));
        }

        @Override
        public Gen<T[]> ofSizeBetween(int minSize, int maxSize) {
            Gen.IntGen sizeGen = ints().between(minSize, maxSize);
            return r ->
            {
                Reset.tryReset(fn);
                int size = sizeGen.nextInt(r);
                T[] list = (T[]) Array.newInstance(type, size);
                for (int i = 0; i < size; i++)
                    list[i] = fn.next(r);
                return list;
            };
        }
    }

    public static class IntArrayDSL implements BaseSequenceDSL<IntArrayDSL, int[]> {
        private final Gen.IntGen fn;

        public IntArrayDSL(Gen.IntGen fn) {
            this.fn = Objects.requireNonNull(fn);
        }

        @Override
        public IntArrayDSL unique()
        {
            return new IntArrayDSL(new IntGenReset(fn));
        }

        @Override
        public Gen<int[]> ofSizeBetween(int minSize, int maxSize) {
            Gen.IntGen sizeGen = ints().between(minSize, maxSize);
            return r ->
            {
                int size = sizeGen.nextInt(r);
                int[] list = new int[size];
                for (int i = 0; i < size; i++)
                    list[i] = fn.nextInt(r);
                return list;
            };
        }
    }

    public static class LongArrayDSL implements BaseSequenceDSL<LongArrayDSL, long[]> {
        private final Gen.LongGen fn;

        public LongArrayDSL(Gen.LongGen fn) {
            this.fn = Objects.requireNonNull(fn);
        }

        @Override
        public LongArrayDSL unique()
        {
            return new LongArrayDSL(new LongGenReset(fn));
        }

        @Override
        public Gen<long[]> ofSizeBetween(int minSize, int maxSize) {
            Gen.IntGen sizeGen = ints().between(minSize, maxSize);
            return r ->
            {
                int size = sizeGen.nextInt(r);
                long[] list = new long[size];
                for (int i = 0; i < size; i++)
                    list[i] = fn.nextLong(r);
                return list;
            };
        }
    }

    public interface BaseSequenceDSL<A extends BaseSequenceDSL<A, B>, B>
    {
        A unique();

        Gen<B> ofSizeBetween(int min, int max);

        default Gen<B> ofSize(int size) {
            return ofSizeBetween(size, size);
        }
    }

    private interface Reset {
        static void tryReset(Object o)
        {
            if (o instanceof Reset)
                ((Reset) o).reset();
        }

        void reset();
    }

    private static class GenReset<T> implements Gen<T>, Reset
    {
        private final Set<T> seen = new HashSet<>();
        private final Gen<T> fn;

        private GenReset(Gen<T> fn)
        {
            this.fn = fn;
        }

        @Override
        public T next(RandomSource random)
        {
            T value;
            while (!seen.add((value = fn.next(random)))) {}
            return value;
        }

        @Override
        public void reset()
        {
            seen.clear();
        }
    }

    private static class IntGenReset implements Gen.IntGen, Reset
    {
        private final GenReset<Integer> base;

        private IntGenReset(Gen.IntGen fn)
        {
            this.base = new GenReset<>(fn);
        }
        @Override
        public int nextInt(RandomSource random) {
            return base.next(random);
        }

        @Override
        public void reset() {
            base.reset();
        }
    }

    private static class LongGenReset implements Gen.LongGen, Reset
    {
        private final GenReset<Long> base;

        private LongGenReset(Gen.LongGen fn)
        {
            this.base = new GenReset<>(fn);
        }
        @Override
        public long nextLong(RandomSource random) {
            return base.next(random);
        }

        @Override
        public void reset() {
            base.reset();
        }
    }
}
