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

import java.util.Iterator;
import java.util.function.Function;
import java.util.function.IntFunction;

public class TinyEnumSet<E extends Enum<E>>
{
    protected final int bitset;

    public TinyEnumSet(Enum<E> ... values)
    {
        this.bitset = encode(values);
    }

    public TinyEnumSet(Enum<E> value)
    {
        this.bitset = encode(value);
    }

    public TinyEnumSet()
    {
        this.bitset = 0;
    }

    public TinyEnumSet(int bitset)
    {
        this.bitset = bitset;
    }

    public static <E extends Enum<E>> TinyEnumSet<E> allOf(Class<E> clazz)
    {
        int count = clazz.getEnumConstants().length;
        return new TinyEnumSet<>(-1 >>> (32 - count));
    }

    public static <E extends Enum<E>> TinyEnumSet<E> of()
    {
        return new TinyEnumSet<>(0);
    }

    public static <E extends Enum<E>> TinyEnumSet<E> of(Enum<E> value)
    {
        return new TinyEnumSet<>(encode(value));
    }

    public static <E extends Enum<E>> TinyEnumSet<E> of(Enum<E> ... values)
    {
        return new TinyEnumSet<>(encode(values));
    }

    public static <E extends Enum<E>> int encode(Enum<E> v1, Enum<E> v2)
    {
        return encode(v1) | encode(v2);
    }
    public static <E extends Enum<E>> int encode(Enum<E> ... values)
    {
        int bitset = 0;
        for (Enum<E> v : values)
        {
            Invariants.requireArgument(v.ordinal() < 32);
            bitset |= encode(v);
        }
        return bitset;
    }

    public static <E extends Enum<E>> int encode(Enum<E> value)
    {
        return 1 << value.ordinal();
    }

    public static <E extends Enum<E>> boolean contains(int bitset, E value)
    {
        return contains(bitset, value.ordinal());
    }

    public static boolean contains(int bitset, int ordinal)
    {
        return 0 != (bitset & (1 << ordinal));
    }

    public boolean isEmpty()
    {
        return bitset == 0;
    }

    public int size()
    {
        return Integer.bitCount(bitset);
    }

    public boolean contains(E value)
    {
        return contains(bitset, value.ordinal());
    }

    public boolean contains(int ordinal)
    {
        return contains(bitset, ordinal);
    }

    public TinyEnumSet<E> or(TinyEnumSet<E> or)
    {
        return or(this, or, TinyEnumSet::new);
    }

    public TinyEnumSet<E> and(TinyEnumSet<E> and)
    {
        return and(this, and, TinyEnumSet::new);
    }

    public TinyEnumSet<E> not(TinyEnumSet<E> not)
    {
        return not(this, not, TinyEnumSet::new);
    }

    public TinyEnumSet<E> with(E flag)
    {
        return new TinyEnumSet<>(bitset | encode(flag));
    }

    public static <S extends TinyEnumSet<?>> S or(S a, S b, IntFunction<S> constructor)
    {
        int newBitset = a.bitset | b.bitset;
        return newBitset == a.bitset ? a : newBitset == b.bitset ? b : constructor.apply(newBitset);
    }

    public static <S extends TinyEnumSet<?>> S and(S a, S b, IntFunction<S> constructor)
    {
        int newBitset = a.bitset & b.bitset;
        return newBitset == a.bitset ? a : newBitset == b.bitset ? b : constructor.apply(newBitset);
    }

    public static <S extends TinyEnumSet<?>> S not(S a, S b, IntFunction<S> constructor)
    {
        int newBitset = a.bitset & ~b.bitset;
        return newBitset == a.bitset ? a : newBitset == b.bitset ? b : constructor.apply(newBitset);
    }

    public boolean test(E kind)
    {
        return testOrdinal(kind.ordinal());
    }

    public boolean testOrdinal(int ordinal)
    {
        return 0 != (bitset & (1 << ordinal));
    }

    public boolean equals(Object that)
    {
        return that != null && that.getClass() == TinyEnumSet.class && ((TinyEnumSet<?>) that).bitset == bitset;
    }

    public int bitset()
    {
        return bitset;
    }

    protected String toString(IntFunction<E> universe)
    {
        return toString(bitset, universe);
    }

    public static <E extends Enum<E>> String toString(int bits, IntFunction<E> universe)
    {
        return toString(bits, universe, Enum::toString);
    }

    public static <E extends Enum<E>> String toString(int bits, IntFunction<E> universe, Function<E, String> print)
    {
        if (bits == 0)
            return "{}";

        StringBuilder out = new StringBuilder();
        out.append('{');
        append(bits, universe, print, out);
        out.append('}');
        return out.toString();
    }

    public static <E extends Enum<E>> void append(int bits, IntFunction<E> universe, Function<E, String> print, StringBuilder out)
    {
        boolean comma = false;
        while (bits != 0)
        {
            if (comma)
                out.append(',');
            int i = Integer.numberOfTrailingZeros(bits);
            out.append(print.apply(universe.apply(i)));
            bits ^= 1 << i;
            comma = true;
        }
    }

    public Iterable<E> iterable(IntFunction<E> lookup)
    {
        return () -> new Iterator<>()
        {
            int remaining = bitset;
            @Override
            public boolean hasNext()
            {
                return remaining != 0;
            }

            @Override
            public E next()
            {
                E next = lookup.apply(Integer.numberOfTrailingZeros(remaining));
                remaining ^= Integer.lowestOneBit(remaining);
                return next;
            }
        };
    }
}

