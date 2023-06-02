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
        bits = copy.bits.clone();
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
        if (to <= from)
        {
            Invariants.checkArgument(to >= from, "to < from (%s < %s)", to, from);
            return;
        }

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

    private void orBitsAtIndex(int index, long setBits)
    {
        long prevBits = bits[index];
        bits[index] = setBits | prevBits;
        count += Long.bitCount(setBits) - Long.bitCount(prevBits);
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

    public boolean get(int i)
    {
        int index = indexOf(i);
        long bit = bit(i);
        return 0 != (bits[index] & bit);
    }

    public int size()
    {
        return bits.length * 64;
    }

    public int setBitCount()
    {
        return count;
    }

    public boolean isEmpty()
    {
        return count == 0;
    }

    public int prevSetBit(int i)
    {
        if (count == 0)
            return -1;

        int index = indexOf(i);
        long bits = this.bits[index] & bitsEqualOrLesser(i);
        while (true)
        {
            if (bits != 0)
                return index * 64 + numberOfTrailingZeros(highestOneBit(bits));

            if (--index < 0)
                return -1;

            bits = this.bits[index];
        }
    }

    public int prevSetBitNotBefore(int i, int inclBound, int ifNotFound)
    {
        if (count == 0)
            return ifNotFound;

        int index = indexOf(i);
        int inclIndexBound = lowerLimitOf(inclBound);
        long bits = this.bits[index] & bitsEqualOrLesser(i);
        while (true)
        {
            if (bits != 0)
            {
                int result = index * 64 + numberOfTrailingZeros(highestOneBit(bits));
                return result > inclBound ? result : ifNotFound;
            }

            if (--index < inclIndexBound)
                return -1;

            bits = this.bits[index];
        }
    }

    public int nextSetBit(int i, int ifNotFound)
    {
        if (count == 0)
            return ifNotFound;

        int index = indexOf(i);
        long bits = this.bits[index] & bitsEqualOrGreater(i);
        while (true)
        {
            if (bits != 0)
                return index * 64 + numberOfTrailingZeros(lowestOneBit(bits));

            if (++index >= this.bits.length)
                return ifNotFound;

            bits = this.bits[index];
        }
    }

    public int nextSetBitBefore(int i, int exclBound, int ifNotFound)
    {
        if (count == 0)
            return ifNotFound;

        int index = indexOf(i);
        int exclIndexBound = upperLimitOf(exclBound);
        long bits = this.bits[index] & bitsEqualOrGreater(i);
        while (true)
        {
            if (bits != 0)
            {
                int result = index * 64 + numberOfTrailingZeros(lowestOneBit(bits));
                return result < exclBound ? result : ifNotFound;
            }

            if (++index >= exclIndexBound)
                return ifNotFound;

            bits = this.bits[index];
        }
    }

    public <P1> void forEach(P1 p1, IndexedConsumer<P1> forEach)
    {
        forEach(forEach, p1, IndexedConsumer::accept);
    }

    public <P1, P2> void forEach(P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        forEach(forEach, p1, p2, IndexedBiConsumer::accept);
    }

    public <P1, P2, P3> void forEach(P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
    {
        forEach(forEach, p1, p2, p3, IndexedTriConsumer::accept);
    }

    // the bitset is permitted to mutate as we iterate
    public <P1, P2, P3, P4> void forEach(P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
    {
        int i = 0;
        while (i < bits.length && count > 0)
        {
            long mask = -1L;
            long register;
            while ((register = (bits[i] & mask)) != 0)
            {
                int bitIndex = numberOfTrailingZeros(register);
                mask = (-1L << bitIndex) << 1;
                forEach.accept(p1, p2, p3, p4, i * 64 + bitIndex);
            }
            ++i;
        }
    }

    public <P1> void reverseForEach(IndexedConsumer<P1> forEach, P1 p1)
    {
        reverseForEach(forEach, p1, IndexedConsumer::accept);
    }

    public <P1, P2> void reverseForEach(P1 p1, P2 p2, IndexedBiConsumer<P1, P2> forEach)
    {
        reverseForEach(forEach, p1, p2, IndexedBiConsumer::accept);
    }

    public <P1, P2, P3> void reverseForEach(P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
    {
        reverseForEach(forEach, p1, p2, p3, IndexedTriConsumer::accept);
    }

    // the bitset is permitted to mutate as we iterate
    public <P1, P2, P3, P4> void reverseForEach(P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
    {
        int i = bits.length - 1;
        while (i >= 0 && count > 0)
        {
            long mask = -1L;
            long register;
            while ((register = (bits[i] & mask)) != 0)
            {
                int bitIndex = 63 - Long.numberOfLeadingZeros(register);
                mask = (1L << bitIndex) - 1;
                forEach.accept(p1, p2, p3, p4, i * 64 + bitIndex);
            }
            --i;
        }
    }

    private int indexOf(int i)
    {
        int index = i >>> 6;
        if (index >= bits.length)
            throw new IndexOutOfBoundsException();
        return index;
    }

    private int lowerLimitOf(int i)
    {
        return i >>> 6;
    }

    private int upperLimitOf(int i)
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

    private static long bitsEqualOrLesser(int i)
    {
        int imod64 = i & 63;
        long bit = 1L << imod64;
        return bit + bit - 1;
    }
}
