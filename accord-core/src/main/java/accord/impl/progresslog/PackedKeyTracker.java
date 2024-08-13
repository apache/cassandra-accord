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

import accord.local.Node;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

/**
 * Using a predefined number of bits, store state allowing us to iterate through participating keys in groups
 * small enough we can maintain a bitset of the keys that have met our desired criteria.
 *
 * This permits us to register remote callbacks with minimal state on the sender and recipient and track a transaction's
 * progress towards whatever local or distributed state we need to achieve.
 *
 * We split the bits dynamically between a bitSet and a roundIndex.
 * The bitSet has a number of bits equal to the roundSize, and the roundIndex has enough bits
 * to encode the total number of rounds we need of the given size.
 * <p>
 * In the typical case we have one round with enough bits for all keys.
 * At the other extreme we have many rounds each fetching a single key because we have too many keys
 * to represent in the number of bits available.
 */
public class PackedKeyTracker
{
    /**
     * Calculate a packed round size for a given number of bits.
     */
    static int roundSize(int routeSize, int totalBits)
    {
        if (routeSize < totalBits)
            return routeSize;

        return Math.toIntExact((1L << (totalBits - 1)) / routeSize);
    }

    /**
     * Generate a bitSet of those keys we would have requested from the responding replica for the callbackId.
     * These bits can be unset from the state's bitSet, as their criteria have been fulfilled on the remote replica.
     */
    static int roundCallbackBitSet(DefaultProgressLog instance, TxnId txnId, Node.Id from, Route<?> route, int callbackId, int roundIndex, int roundSize)
    {
        if (callbackId != roundIndex)
        {
            Invariants.checkState(callbackId / roundSize < roundIndex);
            return 0; // stale callback for earlier round
        }

        int start = roundSize * roundIndex;
        int end = Math.min((roundSize + 1) * roundIndex, route.size());

        Ranges sliceRanges = instance.node().topology().globalForEpoch(txnId.epoch()).rangesForNode(from);
        Route<?> ready = route.slice(start, end)
                              .slice(sliceRanges);

        return computeBitSet(route, ready, roundIndex, roundSize);
    }

    /**
     * Read the packed bitSet for a round of the provided size
     */
    static int bitSet(long encodedState, int roundSize, int shift)
    {
        return (int) ((encodedState >>> shift) & ((1L << roundSize) - 1));
    }

    /**
     * Set the packed round bitSet for a round of the provided size
     */
    static long setBitSet(long encodedState, int bitSet, int roundSize, int shift)
    {
        long mask = (1L << roundSize) - 1;
        Invariants.checkArgument(Integer.highestOneBit(bitSet) <= mask);
        encodedState &= ~(mask << shift);
        encodedState |= (long) bitSet << shift;
        return encodedState;
    }

    static long initialiseBitSet(long encodedState, Route<?> route, Unseekables<?> intersecting, int roundIndex, int roundSize, int shift)
    {
        int bitSet = computeBitSet(route, intersecting, roundIndex, roundSize);
        return setBitSet(encodedState, bitSet, roundSize, shift);
    }

    static int computeBitSet(Route<?> route, Unseekables<?> intersecting, int roundIndex, int roundSize)
    {
        int bitSet = 0, start = roundSize * roundIndex, ai = start, bi = 0;
        for (int i = 0; i < intersecting.size(); ++i)
        {
            long abi = route.findNextIntersection(ai, (Routables) intersecting, bi);
            if (abi < 0)
                break;

            ai = (int) abi;
            bi = (int) (abi >>> 32);
            bitSet |= 1 << (ai - start);
        }
        return bitSet;
    }

    /**
     * Read the packed round index for rounds of the provided size
     */
    static int roundIndex(long encodedState, int roundSize, int shift, long mask)
    {
        return (int) (((encodedState >>> shift) & mask) >>> roundSize);
    }

    /**
     * Set the packed round index for a round of the provided size
     */
    static long setRoundIndexAndClearBitSet(long encodedState, int newIndex, int roundSize, int shift, long mask)
    {
        Invariants.checkArgument(roundSize > 0);
        int counterBits = shift - roundSize;
        Invariants.checkState(newIndex < (1L << counterBits));
        int counterShift = shift + roundSize;
        encodedState &= ~(mask << shift);
        encodedState |= (long) newIndex << counterShift;
        return encodedState;
    }

    /**
     * Set the packed round index for a round of the provided size
     */
    static long setMaxRoundIndexAndClearBitSet(long encodedState, int roundSize, int shift, long mask)
    {
        Invariants.checkArgument(roundSize > 0);
        int counterBits = shift - roundSize;
        long maxIndex = (1L << counterBits) - 1;
        int counterShift = shift + roundSize;
        encodedState &= ~(mask << shift);
        encodedState |= maxIndex << counterShift;
        return encodedState;
    }

    static long clearRoundState(long encodedState, int shift, long mask)
    {
        return encodedState & ~(mask << shift);
    }

}
