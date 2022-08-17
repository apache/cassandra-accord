package accord.utils;

import com.google.common.base.Preconditions;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class Gens {
    private Gens() {
    }

    public static Gen<Gen.Random> random() {
        return r -> r;
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

    public static class IntDSL
    {
        public Gen.IntGen of(int value)
        {
            return r -> value;
        }

        public Gen.IntGen all()
        {
            return Gen.Random::nextInt;
        }

        public Gen.IntGen between(int min, int max)
        {
            Preconditions.checkArgument(max >= min);
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
            return Gen.Random::nextLong;
        }

        public Gen.LongGen between(long min, long max) {
            Preconditions.checkArgument(max >= min);
            if (min == max)
                return of(min);
            // since bounds is exclusive, if max == max_value unable to do +1 to include... so will return a gen
            // that does not include
            if (max == Long.MAX_VALUE)
                return r -> r.nextLong(min, max);
            return r -> r.nextLong(min, max + 1);
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
        public T next(Random random)
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
        public int nextInt(Random random) {
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
        public long nextLong(Random random) {
            return base.next(random);
        }

        @Override
        public void reset() {
            base.reset();
        }
    }
}
