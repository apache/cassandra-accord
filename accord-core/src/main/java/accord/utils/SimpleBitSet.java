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

import java.util.Arrays;

import static java.lang.Long.highestOneBit;
import static java.lang.Long.lowestOneBit;
import static java.lang.Long.numberOfTrailingZeros;

public class SimpleBitSet
{
    public static class SerializationSupport
    {
        public static long[] getArray(SimpleBitSet bs)
        {
            return bs.bits;
        }

        public static SimpleBitSet construct(long[] bits)
        {
            return new SimpleBitSet(bits);
        }
    }

    final long[] bits;
    int count;

    public SimpleBitSet(int size)
    {
        bits = new long[(size + 63)/64];
    }

    public SimpleBitSet(int size, boolean set)
    {
        this(size);
        if (set)
        {
            Arrays.fill(bits, 0, size / 64, -1L);
            if ((size & 63) != 0)
                bits[indexOf(size - 1)] = -1L >>> (64 - (size & 63));
            count = size;
        }
    }

    public SimpleBitSet(SimpleBitSet copy)
    {
        this(copy, false);
    }

    public SimpleBitSet(SimpleBitSet copy, boolean share)
    {
        bits = share ? copy.bits : copy.bits.clone();
        count = copy.count;
    }

    SimpleBitSet(long[] bits)
    {
        this.bits = bits;
        for (long v : bits)
            count += Long.bitCount(v);
    }

    public boolean set(int i)
    {
        int index = indexOf(i);
        long bit = bit(i);
        if (0 != (bits[index] & bit))
            return false;
        bits[index] |= bit;
        ++count;
        return true;
    }

    public void setRange(int from, int to)
    {
        Invariants.checkArgument(from <= to, "from > to (%s > %s)", from, to);
        if (from == to)
            return;

        int fromIndex = from >>> 6;
        int toIndex = (to + 63) >>> 6;
        if (fromIndex + 1 == toIndex)
        {
            long addBits = (-1L >>> (64 - (to & 63))) & (-1L << (from & 63));
            orBitsAtIndex(fromIndex,  addBits);
        }
        else if (count == 0)
        {
            bits[toIndex - 1] = -1L >>> (64 - (to & 63));
            for (int i = fromIndex + 1, maxi = toIndex - 1; i < maxi ; ++i)
                bits[i] = -1L;
            bits[fromIndex] = -1L << (from & 63);
            count = to - from;
        }
        else
        {
            orBitsAtIndex(fromIndex, -1L << (from & 63));
            for (int i = fromIndex + 1, maxi = toIndex - 1; i < maxi ; ++i)
            {
                count += 64 - Long.bitCount(bits[i]);
                bits[i] = -1L;
            }
            orBitsAtIndex(toIndex - 1, -1L >>> (64 - (to & 63)));
        }
    }

    public int getSetBitCount(int from, int to)
    {
        Invariants.checkArgument(from <= to, "from > to (%s > %s)", from, to);
        if (from == to || this.count == 0)
            return 0;

        int fromIndex = from >>> 6;
        int toIndex = to >>> 6;
        if (fromIndex == toIndex)
        {
            long mask = (-1L >>> (64 - (to & 63))) & (-1L << (from & 63));
            return Long.bitCount(bits[fromIndex] & mask);
        }
        else
        {
            int count = Long.bitCount(bits[fromIndex] & (-1L << (from & 63)));
            for (int i = fromIndex + 1; i < toIndex ; ++i)
                count += Long.bitCount(bits[i]);
            if ((to & 63) != 0)
                count += Long.bitCount(bits[toIndex] & -1L >>> (64 - (to & 63)));
            return count;
        }
    }

    private void orBitsAtIndex(int index, long setBits)
    {
        long prevBits = bits[index];
        long nextBits = setBits | prevBits;
        bits[index] = nextBits;
        count += Long.bitCount(nextBits) - Long.bitCount(prevBits);
    }

    public boolean unset(int i)
    {
        int index = indexOf(i);
        long bit = bit(i);
        if (0 == (bits[index] & bit))
            return false;
        bits[index] &= ~bit;
        --count;
        return true;
    }

    public final boolean get(int i)
    {
        int index = indexOf(i);
        long bit = bit(i);
        return 0 != (bits[index] & bit);
    }

    public final int size()
    {
        return bits.length * 64;
    }

    public final int getSetBitCount()
    {
        return count;
    }

    public final boolean isEmpty()
    {
        return count == 0;
    }

    public final int lastSetBit()
    {
        return prevSetBit(size(), -1);
    }

    public final int lastSetBit(int ifNotFound)
    {
        return prevSetBit(size(), ifNotFound);
    }

    public final int prevSetBit(int i)
    {
        return prevSetBit(i, -1);
    }

    public final int prevSetBit(int i, int ifNotFound)
    {
        return prevSetBitInternal(i, 0, ifNotFound);
    }

    public final int lastSetBitNotBefore(int inclBound)
    {
        return prevSetBitNotBefore(size(), inclBound, -1);
    }

    public final int lastSetBitNotBefore(int inclBound, int ifNotFound)
    {
        return prevSetBitNotBefore(size(), inclBound, ifNotFound);
    }

    public final int prevSetBitNotBefore(int i, int inclBound)
    {
        return prevSetBitNotBefore(i, inclBound, -1);
    }

    public final int prevSetBitNotBefore(int i, int inclBound, int ifNotFound)
    {
        int result = prevSetBitInternal(i, lowerLimitOf(inclBound), ifNotFound);
        return result >= inclBound ? result : ifNotFound;
    }

    private int prevSetBitInternal(int i, int inclIndexBound, int ifNotFound)
    {
        Invariants.checkArgument(i >= 0);
        Invariants.checkArgument(i <= size());
        if (count == 0 || i == 0)
            return ifNotFound;

        int index = i >>> 6;
        long bits;
        {
            int imod64 = i & 63;
            if (imod64 == 0)
            {
                bits = this.bits[--index];
            }
            else
            {
                long bit = 1L << imod64;
                bits = this.bits[index] & (bit - 1);
            }
        }

        while (true)
        {
            if (bits != 0)
                return index * 64 + numberOfTrailingZeros(highestOneBit(bits));

            if (--index < inclIndexBound)
                return ifNotFound;

            bits = this.bits[index];
        }
    }

    public final int firstSetBit()
    {
        return nextSetBit(0, -1);
    }

    public final int firstSetBit(int ifNotFound)
    {
        return nextSetBit(0, ifNotFound);
    }

    public final int nextSetBit(int i)
    {
        return nextSetBit(i, -1);
    }

    public final int nextSetBit(int i, int ifNotFound)
    {
        return nextSetBitInternal(i, bits.length, ifNotFound);
    }

    public final int firstSetBitBefore(int exclBound)
    {
        return nextSetBitBefore(0, exclBound, -1);
    }

    public final int firstSetBitBefore(int exclBound, int ifNotFound)
    {
        return nextSetBitBefore(0, exclBound, ifNotFound);
    }

    public final int nextSetBitBefore(int i, int exclBound)
    {
        return nextSetBitBefore(i, exclBound, -1);
    }

    public final int nextSetBitBefore(int i, int exclBound, int ifNotFound)
    {
        int result = nextSetBitInternal(i, upperIndexLimitOf(exclBound), ifNotFound);
        return result < exclBound ? result : ifNotFound;
    }

    private int nextSetBitInternal(int i, int exclIndexBound, int ifNotFound)
    {
        Invariants.checkArgument(i >= 0);
        Invariants.checkArgument(i <= size());

        if (count == 0)
            return ifNotFound;

        int index = i >>> 6;
        if (index == this.bits.length)
            return ifNotFound;

        long bits = this.bits[index] & bitsEqualOrGreater(i);
        while (true)
        {
            if (bits != 0)
                return index * 64 + numberOfTrailingZeros(lowestOneBit(bits));

            if (++index >= exclIndexBound)
                return ifNotFound;

            bits = this.bits[index];
        }
    }

    public final <P1> void forEach(P1 p1, IndexedConsumer<P1> forEach)
    {
        forEach(forEach, p1, IndexedConsumer::accept);
    }

    public final <P1, P2> void forEach(P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        forEach(forEach, p1, p2, IndexedBiConsumer::accept);
    }

    public final <P1, P2, P3> void forEach(P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
    {
        forEach(forEach, p1, p2, p3, IndexedTriConsumer::accept);
    }

    // the bitset is permitted to mutate as we iterate
    public final <P1, P2, P3, P4> void forEach(P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
    {
        for (int i = 0 ; i < bits.length && count > 0 ; ++i)
        {
            long mask = -1L;
            long register;
            while ((register = (bits[i] & mask)) != 0)
            {
                int bitIndex = numberOfTrailingZeros(register);
                mask = (-1L << bitIndex) << 1;
                forEach.accept(p1, p2, p3, p4, i * 64 + bitIndex);
            }
        }
    }

    public final <P1> void reverseForEach(P1 p1, IndexedConsumer<P1> forEach)
    {
        reverseForEach(forEach, p1, IndexedConsumer::accept);
    }

    public final <P1, P2> void reverseForEach(P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        reverseForEach(forEach, p1, p2, IndexedBiConsumer::accept);
    }

    public final <P1, P2, P3> void reverseForEach(P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
    {
        reverseForEach(forEach, p1, p2, p3, IndexedTriConsumer::accept);
    }

    // the bitset is permitted to mutate as we iterate
    public final <P1, P2, P3, P4> void reverseForEach(P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
    {
        for (int i = bits.length - 1; i >= 0 && count > 0 ; --i)
        {
            long mask = -1L;
            long register;
            while ((register = (bits[i] & mask)) != 0)
            {
                int bitIndex = 63 - Long.numberOfLeadingZeros(register);
                mask = (1L << bitIndex) - 1;
                forEach.accept(p1, p2, p3, p4, i * 64 + bitIndex);
            }
        }
    }

    public final <P1, P2, P3, P4> void reverseForEach(int from, int to, P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
    {
        int fromIndex = from >>> 6;
        int toIndex = to >>> 6;

        if (fromIndex == toIndex)
        {
            if (from == to)
                return;

            long mask = (-1L >>> (64 - (to & 63))) & (-1L << (from & 63));
            reverseForEach(fromIndex, mask, p1, p2, p3, p4, forEach);
        }
        else
        {
            reverseForEach(fromIndex, -1L << (from & 63), p1, p2, p3, p4, forEach);
            for (int i = fromIndex + 1; i < toIndex; ++i)
                reverseForEach(fromIndex, -1L, p1, p2, p3, p4, forEach);
            reverseForEach(fromIndex, -1L >>> (64 - (to & 63)), p1, p2, p3, p4, forEach);
        }
    }

    private <P1, P2, P3, P4> void reverseForEach(int index, long mask, P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
    {
        long register;
        while ((register = (bits[index] & mask)) != 0)
        {
            int bitIndex = 63 - Long.numberOfLeadingZeros(register);
            mask = (1L << bitIndex) - 1;
            forEach.accept(p1, p2, p3, p4, index * 64 + bitIndex);
        }
    }

    private int indexOf(int i)
    {
        int index = i >>> 6;
        if (index >= bits.length)
            throw new IndexOutOfBoundsException(String.format("%d >= %d", index, bits.length));
        return index;
    }

    private int lowerLimitOf(int i)
    {
        return i >>> 6;
    }

    private int upperIndexLimitOf(int i)
    {
        int index = (i + 63) >>> 6;
        return Math.min(index, bits.length);
    }

    private static long bit(int i)
    {
        return 1L << (i & 63);
    }

    private static long bitsEqualOrGreater(int i)
    {
        int imod64 = i & 63;
        long bit = 1L << imod64;
        return -bit;
    }

    @Override
    public boolean equals(Object other)
    {
        return other instanceof SimpleBitSet && this.equals((SimpleBitSet) other);
    }

    public boolean equals(SimpleBitSet other)
    {
        return Arrays.equals(this.bits, other.bits);
    }

    public void clear()
    {
        count = 0;
        Arrays.fill(bits, 0L);
    }
}
