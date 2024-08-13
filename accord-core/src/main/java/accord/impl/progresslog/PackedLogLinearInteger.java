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

package accord.impl.progresslog;

import accord.utils.Invariants;

/**
 * A simple encoding scheme for very lossy storage of an integer, where the inaccuracy is proportional to the log scale 
 * of the value being stored. That is, a number is truncated to its nearest highest M bits, with the highest bit position
 * being stored as an integer of log2(N) bits, and the remaining bits being stored directly - only that they may be
 * incremented by 1, if the remaining bits of the original integer represent more than half of the value of the lowest
 * stored bit (i.e. we round to the nearest, rather than truncating).
 */
public class PackedLogLinearInteger
{
    public static int validateLowBits(int numberOfLowBits, int numberOfBits)
    {
        Invariants.checkArgument(numberOfBits <= 30, "%s bits can store the raw input without packing", numberOfBits);
        int numberOfHighBits = numberOfBits - numberOfLowBits;
        Invariants.checkArgument(numberOfHighBits <= 5, numberOfHighBits + " bits is too many to represent the maximum bit position");
        Invariants.checkArgument(numberOfHighBits > 0, numberOfHighBits + " bits cannot be zero; this produces a simple linear integer");
        return numberOfLowBits;
    }

    public static int encode(int value, int numberOfLowBits, int numberOfBits)
    {
        Invariants.checkState(value >= 0);
        if (value < 1 << numberOfLowBits)
            return value;

        int highestSetBit = Integer.highestOneBit(value);
        int highBits = Math.max(0, 1 + (Integer.numberOfTrailingZeros(highestSetBit) & 31) - numberOfLowBits);
        value ^= highestSetBit;
        // include one additional bit in lowBits for rounding, and discount highest bit
        if (highBits >= 2) value >>>= highBits - 2;
        else value <<= 2 - highBits;
        // exclude the rounding bit when packing
        int result = (value >>> 1) | (highBits << numberOfLowBits);
        // then increment by any rounding bit, permitting this to overflow and increment our highBits
        result += value & 1;
        // in case of overflow, saturate our high and low bits
        if (result >= 1 << numberOfBits)
            return -1 >>> (32 - numberOfBits);
        return result;
    }

    public static int decode(int encoded, int lowBits)
    {
        int bitShift = encoded >>> lowBits;
        if (bitShift == 0)
            return encoded;

        --bitShift;
        int highestBit = 1 << lowBits;
        int bits = highestBit | (encoded & (highestBit - 1));
        return bits << bitShift;
    }
}
