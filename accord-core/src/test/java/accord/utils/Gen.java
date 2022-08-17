package accord.utils;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

public interface Gen<A> {
    /**
     * For cases where method handles isn't able to detect the proper type, this method acts as a cast
     * to inform the compiler of the desired type.
     */
    static <A> Gen<A> of(Gen<A> fn)
    {
        return fn;
    }

    A next(Random random);

    default <B> Gen<B> map(Function<A, B> fn)
    {
        return r -> fn.apply(this.next(r));
    }

    default <B> Gen<B> map(BiFunction<Random, A, B> fn)
    {
        return r -> fn.apply(r, this.next(r));
    }

    default Gen<A> filter(Predicate<A> fn)
    {
        Gen<A> self = this;
        return r -> {
            A value;
            do {
                value = self.next(r);
            }
            while (!fn.test(value));
            return value;
        };
    }

    class Random extends java.util.Random
    {
        public Random(long seed) {
            super(seed);
        }

        public int nextInt(int origin, int bound)
        {
            if (origin >= bound)
                throw new IllegalArgumentException("bound (" + bound + ") must be greater than origin (" + origin + ")");
            int r = nextInt();
            if (origin < bound) {
                int n = bound - origin, m = n - 1;
                if ((n & m) == 0)
                    r = (r & m) + origin;
                else if (n > 0) {
                    for (int u = r >>> 1;
                         u + m - (r = u % n) < 0;
                         u = nextInt() >>> 1)
                        ;
                    r += origin;
                }
                else {
                    while (r < origin || r >= bound)
                        r = nextInt();
                }
            }
            return r;
        }

        public int allPositive()
        {
            return nextInt(1, Integer.MAX_VALUE);
        }

        public int nextPositive(int upper)
        {
            return nextInt(1, upper);
        }

        public long nextLong(long bound)
        {
            return nextLong(0, bound);
        }

        public long nextLong(long origin, long bound)
        {
            if (origin >= bound)
                throw new IllegalArgumentException("bound must be greater than origin");
            long r = nextLong();
            long n = bound - origin, m = n - 1;
            if ((n & m) == 0L)  // power of two
                r = (r & m) + origin;
            else if (n > 0L) {  // reject over-represented candidates
                for (long u = r >>> 1;            // ensure nonnegative
                     u + m - (r = u % n) < 0L;    // rejection check
                     u = nextLong() >>> 1) // retry
                    ;
                r += origin;
            }
            else {              // range not representable as long
                while (r < origin || r >= bound)
                    r = nextLong();
            }
            return r;
        }
    }

    interface IntGen extends Gen<Integer>
    {
        int nextInt(Random random);

        @Override
        default Integer next(Random random)
        {
            return nextInt(random);
        }
    }

    interface LongGen extends Gen<Long>
    {
        long nextLong(Random random);

        @Override
        default Long next(Random random)
        {
            return nextLong(random);
        }
    }
}
