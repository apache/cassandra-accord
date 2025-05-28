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

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.IntSupplier;
import java.util.function.IntUnaryOperator;
import java.util.function.LongPredicate;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A generator (gen for short) interface that produces values using a {@link RandomSource}.
 * <p>
 * This interface provides a way to generate random values of type {@code A} and transform them
 * using various functional operators like {@link #map(Function)}, {@link #filter(Predicate)}, and {@link #flatMap(Function)}.
 * <p>
 * Specialized versions exist for primitive types ({@link IntGen}, {@link LongGen}) to avoid boxing/unboxing overhead.
 *
 * @param <A> The type of values this generator produces
 */
@SuppressWarnings("unused")
public interface Gen<A>
{
    /**
     * For cases where method handles isn't able to detect the proper type, this method acts as a cast
     * to inform the compiler of the desired type.
     */
    static <A> Gen<A> of(Gen<A> fn)
    {
        return fn;
    }

    /**
     * Core method of the {@link Gen} interface that produces a value of type {@code A} using the provided {@link RandomSource}.
     * <p>
     * The semantics of this method are implementation-specific. Some important considerations:
     * <ul>
     * <li>Some implementations may return null values</li>
     * <li>Implementations may return duplicate or identical values across calls</li>
     * <li>Values may contain or be backed by shared mutable state</li>
     * </ul>
     * <p>
     * Due to these implementation-specific behaviors, callers should avoid storing references to returned
     * values for extended periods unless the specific generator's safety guarantees are known.
     *
     * @param random The random source to use for generating values
     * @return A generated value of type {@code A}
     */
    A next(RandomSource random);

    default <B> Gen<B> map(Function<? super A, ? extends B> fn)
    {
        return r -> fn.apply(this.next(r));
    }

    default <B> Gen<B> map(BiFunction<RandomSource, ? super A, ? extends B> fn)
    {
        return r -> fn.apply(r, this.next(r));
    }

    default IntGen mapToInt(ToIntFunction<A> fn)
    {
        return r -> fn.applyAsInt(next(r));
    }

    default LongGen mapToLong(ToLongFunction<A> fn)
    {
        return r -> fn.applyAsLong(next(r));
    }

    default <B> Gen<B> flatMap(Function<? super A, Gen<? extends B>> mapper)
    {
        return rs -> mapper.apply(this.next(rs)).next(rs);
    }

    default <B> Gen<B> flatMap(BiFunction<RandomSource, ? super A, Gen<? extends B>> mapper)
    {
        return rs -> mapper.apply(rs, this.next(rs)).next(rs);
    }

    /**
     * Creates a new generator that only produces values matching the provided predicate.
     * <p>
     * <strong>Warning:</strong> This method is unbounded and will loop indefinitely if the predicate
     * never returns true for any generated value. Use with caution, especially with rare conditions.
     * <p>
     * For a bounded alternative that will stop after a specified number of attempts,
     * see {@link #filter(int, Object, Predicate)}.
     *
     * @param fn the predicate function that values must satisfy
     * @return a new generator that only produces values matching the predicate
     */
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

    /**
     * Creates a new bounded generator that only produces values matching the provided predicate.
     * <p>
     * Unlike the unbounded {@link #filter(Predicate)}, this method will make at most {@code maxAttempts}
     * attempts to generate a value that satisfies the predicate. If no matching value is found within
     * the specified number of attempts, it returns the provided {@code defaultValue} instead.
     *
     * @param maxAttempts the maximum number of generation attempts before falling back to the default value
     * @param defaultValue the value to return if no matching value is found within maxAttempts
     * @param fn the predicate function that values must satisfy
     * @return a new bounded generator that produces values matching the predicate or the default value
     * @throws IllegalArgumentException if maxAttempts is not positive
     */
    default Gen<A> filter(int maxAttempts, A defaultValue, Predicate<A> fn)
    {
        Invariants.requireArgument(maxAttempts > 0, "Max attempts must be positive; given %d", maxAttempts);
        Gen<A> self = this;
        return r -> {
            for (int i = 0; i < maxAttempts; i++)
            {
                A v = self.next(r);
                if (fn.test(v))
                    return v;

            }
            return defaultValue;
        };
    }

    default Supplier<A> asSupplier(RandomSource rs)
    {
        return () -> next(rs);
    }

    default Stream<A> asStream(RandomSource rs)
    {
        return Stream.generate(() -> next(rs));
    }

    interface Int2IntMapFunction
    {
        int applyAsInt(RandomSource rs, int value);
    }

    interface Int2LongMapFunction
    {
        long applyAsLong(RandomSource rs, int value);
    }

    interface Long2LongMapFunction
    {
        long applyAsLong(RandomSource rs, long value);
    }

    interface IntGen extends Gen<Integer>
    {
        int nextInt(RandomSource random);

        @Override
        default Integer next(RandomSource random)
        {
            return nextInt(random);
        }

        default IntGen mapAsInt(IntUnaryOperator fn)
        {
            return r -> fn.applyAsInt(nextInt(r));
        }

        default IntGen mapAsInt(Int2IntMapFunction fn)
        {
            return r -> fn.applyAsInt(r, nextInt(r));
        }

        default LongGen mapAsLong(Int2LongMapFunction fn)
        {
            return r -> fn.applyAsLong(r, nextInt(r));
        }

        default Gen.IntGen filterAsInt(IntPredicate fn)
        {
            return rs -> {
                int value;
                do
                {
                    value = nextInt(rs);
                }
                while (!fn.test(value));
                return value;
            };
        }

        @Override
        default Gen.IntGen filter(Predicate<Integer> fn)
        {
            return filterAsInt(fn::test);
        }

        default IntSupplier asIntSupplier(RandomSource rs)
        {
            return () -> nextInt(rs);
        }

        default IntStream asIntStream(RandomSource rs)
        {
            return IntStream.generate(() -> nextInt(rs));
        }
    }

    interface LongGen extends Gen<Long>
    {
        long nextLong(RandomSource random);

        @Override
        default Long next(RandomSource random)
        {
            return nextLong(random);
        }

        default LongGen mapAsLong(LongUnaryOperator fn)
        {
            return r -> fn.applyAsLong(nextLong(r));
        }

        default LongGen mapAsLong(Long2LongMapFunction fn)
        {
            return r -> fn.applyAsLong(r, nextLong(r));
        }

        default Gen.LongGen filterAsLong(LongPredicate fn)
        {
            return rs -> {
                long value;
                do
                {
                    value = nextLong(rs);
                }
                while (!fn.test(value));
                return value;
            };
        }

        @Override
        default Gen.LongGen filter(Predicate<Long> fn)
        {
            return filterAsLong(fn::test);
        }

        default LongSupplier asLongSupplier(RandomSource rs)
        {
            return () -> nextLong(rs);
        }

        default LongStream asLongStream(RandomSource rs)
        {
            return LongStream.generate(() -> nextLong(rs));
        }
    }
}
