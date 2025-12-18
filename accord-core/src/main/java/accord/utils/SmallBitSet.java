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

import static accord.utils.LargeBitSet.bitsEqualOrGreater;
import static java.lang.Long.numberOfTrailingZeros;

public class SmallBitSet implements SimpleBitSet
{
    private long bits;

    public SmallBitSet()
    {
    }

    public SmallBitSet(long bits)
    {
        this.bits = bits;
    }

    public long bits()
    {
        return bits;
    }

    private static long bit(int i)
    {
        validateInclusive(i);
        return 1L << i;
    }

    private static void validateInclusive(int i)
    {
        if (i >>> 6 > 0) // i >= 64 || i < 0
            throw new IndexOutOfBoundsException("Unable to access bit " + i + "; must be between 0 and 63");
    }

    private static void validateExclusive(int i)
    {
        if (i >= 65 || i < 0)
            throw new IndexOutOfBoundsException("Unable to access bit " + i + "; must be between 0 and 64");
    }

    @Override
    public boolean set(int i)
    {
        long bit = bit(i);
        boolean result = 0 == (bits & bit);
        bits |= bit;
        return result;
    }

    @Override
    public void setRange(int fromInclusive, int toExclusive)
    {
        validateInclusive(fromInclusive);
        validateExclusive(toExclusive);
        Invariants.requireArgument(fromInclusive <= toExclusive, "from > to (%s > %s)", fromInclusive, toExclusive);
        if (fromInclusive == toExclusive)
            return;

        bits |= (-1L >>> (64 - (toExclusive & 63))) & (-1L << (fromInclusive & 63));
    }

    @Override
    public boolean get(int i)
    {
        long bit = bit(i);
        return 0 != (bits & bit);
    }

    @Override
    public boolean unset(int i)
    {
        long bit = bit(i);
        boolean result = 0 != (bits & bit);
        bits &= ~bit;
        return result;
    }

    @Override
    public void clear()
    {
        bits = 0;
    }

    @Override
    public boolean isEmpty()
    {
        return bits == 0;
    }

    @Override
    public int nextSetBit(int fromIndex)
    {
        if (fromIndex >= 64)
        {
            validateExclusive(fromIndex);
            return -1;
        }
        long bits = this.bits & bitsEqualOrGreater(fromIndex);
        if (bits == 0)
            return -1;
        return numberOfTrailingZeros(bits);
    }

    @Override
    public int getSetBitCount()
    {
        return Long.bitCount(bits);
    }

    @Override
    public boolean equals(Object that)
    {
        return that instanceof SmallBitSet && this.bits == ((SmallBitSet) that).bits;
    }
}
