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

package accord.local.cfk;

import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.api.RoutingKey;
import accord.local.Node.Id;
import accord.local.RedundantBefore.QuickBounds;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.local.cfk.CommandsForKey.InternalStatus;
import accord.local.cfk.CommandsForKey.TxnInfoExtra;
import accord.local.cfk.CommandsForKey.Unmanaged;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.BitUtils;
import accord.utils.Invariants;
import accord.utils.VIntCoding;

import static accord.local.cfk.CommandsForKey.NO_BOUNDS_INFO;
import static accord.local.cfk.CommandsForKey.NO_PENDING_UNMANAGED;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.SingleKey;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithoutDeps;
import static accord.primitives.TxnId.MediumPath.TrackStable;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.ArrayBuffers.cachedAny;
import static accord.utils.ArrayBuffers.cachedInts;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.BitUtils.flushBits;
import static accord.utils.BitUtils.numberOfBitsToRepresent;
import static accord.utils.BitUtils.readLeastSignificantBytes;
import static accord.utils.BitUtils.writeLeastSignificantBytes;
import static accord.utils.BitUtils.writeMostSignificantBytes;
import static accord.utils.VIntCoding.sizeOfUnsignedVInt;
import static accord.utils.VIntCoding.decodeZigZag64;
import static accord.utils.VIntCoding.encodeZigZag64;
import static accord.utils.VIntCoding.sizeOfVInt;

public class Serialize
{
    private static final int TIMESTAMP_BASE_SIZE = 16;
    private static final InternalStatus[] DECODE_STATUS = InternalStatus.values();

    private static final int HAS_MISSING_DEPS_HEADER_BIT = 0x1;
    private static final int HAS_MISSING_DEPS_HEADER_BIT_SHIFT = 0;
    private static final int HAS_EXECUTE_AT_HEADER_BIT = 0x2;
    private static final int HAS_EXECUTE_AT_HEADER_BIT_SHIFT = 1;
    private static final int HAS_BALLOT_HEADER_BIT = 0x4;
    private static final int HAS_BALLOT_HEADER_BIT_SHIFT = 2;
    private static final int HAS_STATUS_OVERRIDES_HEADER_BIT = 0x8;
    private static final int HAS_STATUS_OVERRIDES_HEADER_BIT_SHIFT = 3;
    private static final int HAS_MISSING_DEPS_FLAGS_HEADER_BIT = 0x10;
    private static final int COMMAND_HEADER_BIT_FLAGS_MASK = 0x1f;
    private static final int HAS_BOOTSTRAPPED_AT_HEADER_BIT = 0x20;
    private static final int HAS_BOUNDS_FLAGS_HEADER_BIT = 0x40;
    private static final int HAS_MAX_HLC_HEADER_BIT = 0x80;


    /**
     * We read/write a fixed number of intial bytes for each command, with an initial flexible number of flag bits
     * and the remainder interpreted as the HLC/epoch/node.
     *
     * The preamble encodes:
     *  vint32: number of commands
     *  vint32: number of unique node Ids
     *  [unique node ids]
     *  two flag bytes:
     *   bit 0 is set if there are any missing ids;
     *   bit 1 is set if there are any executeAt specified
     *   bit 2 is set if there are any ballots specified
     *   bit 3 is set if there are any queries with override flags
     *   bit 4 is set if there are any queries with missing ids that have non-identity flags set
     *   bits 6-7 number of header bytes to read for each command
     *   bits 8-9: level 0 extra hlc bytes to read
     *   bits 10-11: level 1 extra hlc bytes to read (+ 1 + level 0)
     *   bits 12-13: level 2 extra hlc bytes to read (+ 1 + level 1)
     *   bits 14-15: level 3 extra hlc bytes to read (+ 1 + level 2)
     *
     * In order, for each command, we consume:
     * 3 bits for the InternalStatus of the command
     * 1 optional bit: if the status is not APPLIED_DURABLE for encoding if the transaction is durably committed
     * 1 optional bit: if any command has override flags; 2 bits more to read if this bit is set
     * 1 optional bit: if the status encodes an executeAt, indicating if the executeAt is not the TxnId
     * 1 optional bit: if the status encodes any dependencies and there are non-zero missing ids, indicating if there are any missing for this command
     * 3 bits encoding the position of the TxnId flags in our flagHistory, or the number of bytes to read if not present
     * 1 bit encoding if the epoch has changed
     * 2 optional bits: if the prior bit is set, indicating how many bits should be read for the epoch increment: 0=none (increment by 1); 1=4, 2=8, 3=32
     * 4 option bits: if prior bits=01, epoch delta
     * N node id bits (where 2^N unique node ids in the CFK)
     * 2 bits indicating how many more payload bytes should be read, with mapping written in header
     * all remaining bits are interpreted as a delta from the prior HLC
     *
     * if txnId kind flag bits represent 0 or 1, read 1 or 2 bytes respectively for the TxnId flag;
     * if epoch increment flag is 2 or 3, read additional 1 or 4 bytes for epoch delta
     * if executeAt is expected, read vint32 for epoch, vint32 for delta from txnId hlc, and ceil(N/8) bytes for node id
     *
     * After writing all transactions, we then write out the missing txnid collections. This is written at the end
     * so that on deserialization we have already read all of the TxnId. This also permits more efficient serialization,
     * as we can encode a single bit stream with the optimal number of bits.
     * TODO (desired): we could prefix this collection with the subset of TxnId that are actually missing from any other
     *   deps, so as to shrink this collection much further.
     * TODO (desired): offer filtering option that does not need to reconstruct objects/info, reusing prior encoding decisions
     * TODO (desired): accept new redundantBefore on load to avoid deserializing stale data
     * TODO (desired): determine timestamp resolution as a factor of 10
     */
    public static ByteBuffer toBytesWithoutKey(CommandsForKey cfk)
    {
        return toBytesWithoutKey(0, cfk);
    }

    public static ByteBuffer toBytesWithoutKey(int prefixBytes, CommandsForKey cfk)
    {
        Invariants.requireArgument(!cfk.isLoadingPruned());
        return unsafeToBytesWithoutKey(prefixBytes, cfk);
    }

    private static ByteBuffer unsafeToBytesWithoutKey(int prefixBytes, CommandsForKey cfk)
    {
        Invariants.require(!cfk.isLoadingPruned());

        int commandCount = cfk.size();
        if (commandCount == 0)
        {
            if (!cfk.hasMaxUniqueHlc())
                return ByteBuffer.allocate(prefixBytes + 1);

            int size = 1 + VIntCoding.sizeOfUnsignedVInt(cfk.maxUniqueHlc);
            ByteBuffer result = ByteBuffer.allocate(prefixBytes + size);
            result.position(prefixBytes);
            VIntCoding.writeUnsignedVInt32(1, result);
            VIntCoding.writeUnsignedVInt(cfk.maxUniqueHlc, result);
            Invariants.require(!result.hasRemaining());
            result.flip();
            return result;
        }

        int[] nodeIds = cachedInts().getInts(Math.min(64, Math.max(4, commandCount)));
        try
        {
            // first compute the unique Node Ids and some basic characteristics of the data, such as
            // whether we have any missing transactions to encode, any executeAt that are not equal to their TxnId
            // and whether there are any non-standard flag bits to encode
            int nodeIdCount, missingIdCount = 0, missingIdWithFlagsCount = 0, executeAtCount = 0, ballotCount = 0, overrideCount = 0;
            int bitsPerExecuteAtEpoch = 0, bitsPerExecuteAtFlags = 0, bitsPerExecuteAtHlc = 1, bitsPerMissingIdFlags = 0; // to permit us to use full 64 bits and encode in 5 bits we force at least one hlc bit
            {
                nodeIds[0] = cfk.redundantBefore().node.id;
                nodeIdCount = 1;
                {
                    TxnId readyAt = cfk.readyAt();
                    if (readyAt != null)
                        nodeIds[nodeIdCount++] = readyAt.node.id;
                }
                for (int i = 0 ; i < commandCount ; ++i)
                {
                    if (nodeIdCount + 3 >= nodeIds.length)
                    {
                        nodeIdCount = compact(nodeIds, nodeIdCount);
                        if (nodeIdCount > nodeIds.length/2 || nodeIdCount + 3 >= nodeIds.length)
                            nodeIds = cachedInts().resize(nodeIds, nodeIds.length, Math.max(nodeIdCount + 4, nodeIds.length * 2));
                    }

                    TxnInfo txn = cfk.get(i);
                    overrideCount += txn.statusOverrides();
                    nodeIds[nodeIdCount++] = txn.node.id;

                    if (txn.executeAt != txn)
                    {
                        Invariants.require(txn.hasExecuteAt());
                        nodeIds[nodeIdCount++] = txn.executeAt.node.id;
                        bitsPerExecuteAtEpoch = Math.max(bitsPerExecuteAtEpoch, numberOfBitsToRepresent(txn.executeAt.epoch() - txn.epoch()));
                        bitsPerExecuteAtHlc = Math.max(bitsPerExecuteAtHlc, numberOfBitsToRepresent(txn.executeAt.hlc() - txn.hlc()));
                        bitsPerExecuteAtFlags = Math.max(bitsPerExecuteAtFlags, numberOfBitsToRepresent(txn.executeAt.flags()));
                        executeAtCount += 1;
                    }

                    if (txn.getClass() == TxnInfoExtra.class)
                    {
                        TxnInfoExtra extra = (TxnInfoExtra) txn;
                        TxnId[] missing = extra.missing;
                        if (missing.length > 0)
                        {
                            Invariants.require(txn.hasDeps());
                            missingIdCount += missing.length;
                            for (TxnId txnId : missing)
                            {
                                int nonIdentityFlags = txnId.nonIdentityFlags();
                                if (nonIdentityFlags != 0)
                                {
                                    ++missingIdWithFlagsCount;
                                    bitsPerMissingIdFlags = Math.max(bitsPerMissingIdFlags, numberOfBitsToRepresent(nonIdentityFlags));
                                }
                            }
                        }
                        Invariants.require(extra.missing.length == 0 || txn.hasDeps());
                        if (extra.ballot != Ballot.ZERO)
                        {
                            Invariants.require(txn.hasBallot());
                            nodeIds[nodeIdCount++] = extra.ballot.node.id;
                            ballotCount += 1;
                        }
                    }
                }

                for (Unmanaged unmanaged : cfk.unmanageds)
                {
                    if (nodeIdCount + 2 >= nodeIds.length)
                    {
                        nodeIdCount = compact(nodeIds, nodeIdCount);
                        if (nodeIdCount > nodeIds.length / 2 || nodeIdCount + 2 >= nodeIds.length)
                            nodeIds = cachedInts().resize(nodeIds, nodeIds.length, nodeIds.length * 2);
                    }

                    nodeIds[nodeIdCount++] = unmanaged.txnId.node.id;
                    nodeIds[nodeIdCount++] = unmanaged.waitingUntil.node.id;
                }

                nodeIdCount = compact(nodeIds, nodeIdCount);
                Invariants.require(nodeIdCount > 0);
            }

            // We can now use this information to calculate the fixed header size, compute the amount
            // of additional space we'll need to store the TxnId and its basic info
            int bitsPerNodeId = numberOfBitsToRepresent(nodeIdCount);
            int minHeaderBits = 10 + bitsPerNodeId + (overrideCount > 0 ? 1 : 0);
            int headerFlags = (executeAtCount > 0 ? HAS_EXECUTE_AT_HEADER_BIT : 0)
                            | (missingIdCount > 0 ? HAS_MISSING_DEPS_HEADER_BIT : 0)
                            | (ballotCount > 0    ? HAS_BALLOT_HEADER_BIT : 0);

            int maxHeaderBits = minHeaderBits;
            int totalBytes = 0;

            int prunedBeforeIndex = cfk.prunedBefore().equals(TxnId.NONE) ? -1 : cfk.indexOf(cfk.prunedBefore());

            long flagHistory = EMPTY_FLAG_HISTORY;
            long prevEpoch = cfk.redundantBefore().epoch();
            long prevHlc = cfk.redundantBefore().hlc();
            int[] bytesHistogram = cachedInts().getInts(12);
            Arrays.fill(bytesHistogram, 0);
            for (int i = 0 ; i < commandCount ; ++i)
            {
                int headerBits = minHeaderBits;
                int payloadBits = 0;

                TxnInfo txn = cfk.get(i);
                {
                    long epoch = txn.epoch();
                    Invariants.require(epoch >= prevEpoch);
                    long epochDelta = epoch - prevEpoch;
                    long hlc = txn.hlc();
                    long hlcDelta = hlc - prevHlc;

                    if (epochDelta > 0)
                    {
                        if (hlcDelta < 0)
                            hlcDelta = -1 - hlcDelta;

                        headerBits += 3;
                        if (epochDelta > 1)
                        {
                            if (epochDelta <= 0xf) headerBits += 4;
                            else if (epochDelta <= 0xff) totalBytes += 1;
                            else { totalBytes += 4; Invariants.require(epochDelta <= 0xffffffffL); }
                        }
                    }

                    payloadBits += numberOfBitsToRepresent(hlcDelta);
                    prevEpoch = epoch;
                    prevHlc = hlc;
                }

                int flags = txn.flags();
                int encodedFlagBits = encodedFlagBits(flags, flagHistory);
                flagHistory = updateFlagHistory(flags, encodedFlagBits, flagHistory);
                if (encodedFlagBits < 2)
                    totalBytes += 1 + encodedFlagBits;

                if (txn.hasExecuteAt())
                    headerBits += (headerFlags >>> HAS_EXECUTE_AT_HEADER_BIT_SHIFT) & 1;
                if (txn.hasDeps())
                    headerBits += (headerFlags >>> HAS_MISSING_DEPS_HEADER_BIT_SHIFT) & 1;
                if (txn.hasBallot())
                    headerBits += (headerFlags >>> HAS_BALLOT_HEADER_BIT_SHIFT) & 1;
                if (txn.compareTo(InternalStatus.APPLIED_DURABLE) < 0)
                    ++headerBits;

                maxHeaderBits = Math.max(headerBits, maxHeaderBits);
                int basicBytes = (headerBits + payloadBits + 7)/8;
                bytesHistogram[basicBytes]++;
            }

            int minBasicBytes = -1, maxBasicBytes = 0;
            for (int i = 0 ; i < bytesHistogram.length ; ++i)
            {
                if (bytesHistogram[i] == 0) continue;
                if (minBasicBytes == -1) minBasicBytes = i;
                maxBasicBytes = i;
            }
            for (int i = minBasicBytes + 1 ; i <= maxBasicBytes ; ++i)
                bytesHistogram[i] += bytesHistogram[i-1];

            int globalFlags =   (missingIdCount          > 0  ? HAS_MISSING_DEPS_HEADER_BIT       : 0)
                              | (missingIdWithFlagsCount > 0  ? HAS_MISSING_DEPS_FLAGS_HEADER_BIT : 0)
                              | (executeAtCount          > 0  ? HAS_EXECUTE_AT_HEADER_BIT         : 0)
                              | (ballotCount             > 0  ? HAS_BALLOT_HEADER_BIT             : 0)
                              | (overrideCount           > 0  ? HAS_STATUS_OVERRIDES_HEADER_BIT   : 0)
                              | (cfk.readyAt() != null        ? HAS_BOOTSTRAPPED_AT_HEADER_BIT    : 0)
                              | (hasBoundsFlags(cfk)          ? HAS_BOUNDS_FLAGS_HEADER_BIT       : 0)
                              | (cfk.hasMaxUniqueHlc()        ? HAS_MAX_HLC_HEADER_BIT            : 0)
            ;

            int headerBytes = (maxHeaderBits+7)/8;
            globalFlags |= Invariants.requireArgument(headerBytes - 1, headerBytes <= 4) << 14;

            int hlcBytesLookup;
            {   // 2bits per size, first value may be zero and remainder may be increments of 1-4;
                // only need to be able to encode a distribution of approx. 8 bytes at most, so
                // pick lowest number we need first, then next lowest as 25th %ile while ensuring value of 1-4;
                // then pick highest number we need, ensuring at least 2 greater than second (leaving room for third)
                // then pick third number as 75th %ile, but at least 1 less than highest, and one more than second
                // finally, ensure third then second are distributed so that there is no more than a gap of 4 between them and the next
                int l0 = Math.max(0, Math.min(3, minBasicBytes - headerBytes));
                int l1 = Arrays.binarySearch(bytesHistogram, minBasicBytes, maxBasicBytes, commandCount/4);
                l1 = Math.max(l0+1, Math.min(l0+4, (l1 < 0 ? -1 - l1 : l1) - headerBytes));
                int l3 = Math.max(l1+2, maxBasicBytes - headerBytes);
                int l2 = Arrays.binarySearch(bytesHistogram, minBasicBytes, maxBasicBytes,(3*commandCount)/4);
                l2 = Math.max(l1+1, Math.min(l3-1, (l2 < 0 ? -1 -l2 : l2) - headerBytes));
                while (l3-l2 > 4) ++l2;
                while (l2-l1 > 4) ++l1;
                hlcBytesLookup = setHlcBytes(l0, l1, l2, l3);
                globalFlags |= (l0 | ((l1-(1+l0))<<2) | ((l2-(1+l1))<<4) | ((l3-(1+l2))<<6)) << 16;
            }
            int hlcFlagLookup = hlcBytesLookupToHlcFlagLookup(hlcBytesLookup);

            totalBytes += bytesHistogram[minBasicBytes] * (headerBytes + getHlcBytes(hlcBytesLookup, getHlcFlag(hlcFlagLookup, minBasicBytes - headerBytes)));
            for (int i = minBasicBytes + 1 ; i <= maxBasicBytes ; ++i)
                totalBytes += (bytesHistogram[i] - bytesHistogram[i-1]) * (headerBytes + getHlcBytes(hlcBytesLookup, getHlcFlag(hlcFlagLookup, i - headerBytes)));
            totalBytes += sizeOfUnsignedVInt(commandCount + 1);
            totalBytes += sizeOfUnsignedVInt(nodeIdCount);
            totalBytes += sizeOfUnsignedVInt(nodeIds[0]);
            for (int i = 1 ; i < nodeIdCount ; ++i)
                totalBytes += sizeOfUnsignedVInt(nodeIds[i] - nodeIds[i - 1]);
            totalBytes += 3;

            cachedInts().forceDiscard(bytesHistogram);

            {
                Timestamp redundantBefore = cfk.redundantBefore();
                TxnId readyAt = cfk.readyAt();
                prevEpoch = redundantBefore.epoch();
                prevHlc = redundantBefore.hlc();
                {
                    QuickBounds bounds = cfk.bounds();
                    long start = bounds.startEpoch;
                    long end = bounds.endEpoch;
                    totalBytes += sizeOfUnsignedVInt(start * 2 + (end == Long.MAX_VALUE ? 0 : 1));
                    if (end != Long.MAX_VALUE)
                        totalBytes += sizeOfUnsignedVInt(end - start);
                    totalBytes += sizeOfUnsignedVInt(prevEpoch - start);
                }
                totalBytes += sizeOfUnsignedVInt(prevHlc);
                if (0 != (globalFlags & HAS_BOUNDS_FLAGS_HEADER_BIT))
                    totalBytes += sizeOfUnsignedVInt(redundantBefore.flags());
                totalBytes += sizeOfUnsignedVInt(Arrays.binarySearch(nodeIds, 0, nodeIdCount, redundantBefore.node.id));
                if (readyAt != null)
                {
                    totalBytes += sizeOfUnsignedVInt(readyAt.epoch() - prevEpoch);
                    totalBytes += sizeOfVInt(readyAt.hlc() - prevHlc);
                    if (0 != (globalFlags & HAS_BOUNDS_FLAGS_HEADER_BIT))
                        totalBytes += sizeOfUnsignedVInt(readyAt.flags());
                    totalBytes += sizeOfUnsignedVInt(Arrays.binarySearch(nodeIds, 0, nodeIdCount, readyAt.node.id));
                }
                if (0 != (globalFlags & HAS_MAX_HLC_HEADER_BIT))
                    totalBytes += sizeOfVInt(cfk.maxUniqueHlc - prevHlc);
            }
            totalBytes += sizeOfUnsignedVInt(prunedBeforeIndex + 1);

            int bitsPerBallotEpoch = 0, bitsPerBallotHlc = 1, bitsPerBallotFlags = 0;
            if ((missingIdCount | executeAtCount | ballotCount) > 0)
            {
                if (ballotCount > 0)
                {
                    Ballot prevBallot = null;
                    for (int i = 0 ; i < commandCount ; ++i)
                    {
                        TxnInfo txn = cfk.get(i);
                        if (txn.getClass() != TxnInfoExtra.class) continue;
                        if (!txn.hasBallot()) continue;
                        TxnInfoExtra extra = (TxnInfoExtra) txn;
                        if (extra.ballot == Ballot.ZERO) continue;
                        if (prevBallot != null)
                        {
                            bitsPerBallotEpoch = Math.max(bitsPerBallotEpoch, numberOfBitsToRepresent(encodeZigZag64(extra.ballot.epoch() - prevBallot.epoch())));
                            bitsPerBallotHlc = Math.max(bitsPerBallotHlc, numberOfBitsToRepresent(encodeZigZag64(extra.ballot.hlc() - prevBallot.hlc())));
                            bitsPerBallotFlags = Math.max(bitsPerBallotFlags, numberOfBitsToRepresent(extra.ballot.flags()));
                        }
                        prevBallot = extra.ballot;
                    }
                    totalBytes += 2; // encode bit widths
                }

                if (executeAtCount > 0)
                    totalBytes += 2; // encode bit widths

                // account for encoding missing id stream
                int missingIdBits = (missingIdWithFlagsCount > 0 ? 1 : 0) // bit to encode whether there are extra flags
                                    + 1 // last element bit
                                    + numberOfBitsToRepresent(commandCount);
                int executeAtBits = bitsPerNodeId
                                    + bitsPerExecuteAtEpoch
                                    + bitsPerExecuteAtHlc
                                    + bitsPerExecuteAtFlags;
                int ballotBits = bitsPerNodeId
                                 + bitsPerBallotEpoch
                                 + bitsPerBallotHlc
                                 + bitsPerBallotFlags;
                totalBytes += (missingIdBits * missingIdCount
                               + bitsPerMissingIdFlags * missingIdWithFlagsCount + (missingIdWithFlagsCount > 0 ? 4 : 0)
                               + executeAtBits * executeAtCount
                               + (ballotCount > 0 ? ballotBits * (ballotCount - 1) + bitsPerNodeId + 128 : 0)
                               + 7)/8;
            }

            // count unmanaged bytes
            int unmanagedPendingCommitCount = 0;
            {
                int bytesPerNodeId = (bitsPerNodeId + 7) / 8;
                for (int i = 0 ; i < cfk.unmanagedCount() ; ++i)
                {
                    Unmanaged unmanaged = cfk.getUnmanaged(i);
                    if (unmanaged.pending == Unmanaged.Pending.COMMIT)
                        ++unmanagedPendingCommitCount;
                    totalBytes += 2 * (TIMESTAMP_BASE_SIZE + bytesPerNodeId);
                }
            }
            totalBytes += sizeOfUnsignedVInt(unmanagedPendingCommitCount);
            totalBytes += sizeOfUnsignedVInt(cfk.unmanagedCount() - unmanagedPendingCommitCount);

            ByteBuffer out = ByteBuffer.allocate(prefixBytes + totalBytes);
            out.position(prefixBytes);
            VIntCoding.writeUnsignedVInt32(commandCount + 1, out);
            VIntCoding.writeUnsignedVInt32(nodeIdCount, out);
            VIntCoding.writeUnsignedVInt32(nodeIds[0], out);
            for (int i = 1 ; i < nodeIdCount ; ++i) // TODO (desired): can encode more efficiently as a stream of N bit integers
                VIntCoding.writeUnsignedVInt32(nodeIds[i] - nodeIds[i-1], out);
            writeLeastSignificantBytes(globalFlags, 3, out);

            {
                TxnId redundantBefore = cfk.redundantBefore();
                Timestamp readyAt = cfk.readyAt();
                QuickBounds bounds = cfk.bounds();
                long start = bounds.startEpoch;
                long end = bounds.endEpoch;
                VIntCoding.writeUnsignedVInt((start << 1) | (end != Long.MAX_VALUE ? 1 : 0), out);
                if (end != Long.MAX_VALUE)
                    VIntCoding.writeUnsignedVInt(end - start, out);
                VIntCoding.writeUnsignedVInt(prevEpoch - start, out);
                VIntCoding.writeUnsignedVInt(prevHlc, out);
                if (0 != (globalFlags & HAS_BOUNDS_FLAGS_HEADER_BIT))
                    VIntCoding.writeUnsignedVInt32(redundantBefore.flags(), out);
                VIntCoding.writeUnsignedVInt32(Arrays.binarySearch(nodeIds, 0, nodeIdCount, redundantBefore.node.id), out);
                if (readyAt != null)
                {
                    VIntCoding.writeUnsignedVInt(readyAt.epoch() - prevEpoch, out);
                    VIntCoding.writeVInt(readyAt.hlc() - prevHlc, out);
                    if (0 != (globalFlags & HAS_BOUNDS_FLAGS_HEADER_BIT))
                        VIntCoding.writeUnsignedVInt(readyAt.flags(), out);
                    VIntCoding.writeUnsignedVInt(Arrays.binarySearch(nodeIds, 0, nodeIdCount, readyAt.node.id), out);
                }
                if (0 != (globalFlags & HAS_MAX_HLC_HEADER_BIT))
                    VIntCoding.writeVInt(cfk.maxUniqueHlc - prevHlc, out);
            }
            VIntCoding.writeUnsignedVInt32(prunedBeforeIndex + 1, out);

            flagHistory = EMPTY_FLAG_HISTORY;
            int flagsPlus = globalFlags & COMMAND_HEADER_BIT_FLAGS_MASK;
            // TODO (desired): check this loop compiles correctly to only branch on epoch case, for binarySearch and flushing
            for (int i = 0 ; i < commandCount ; ++i)
            {
                TxnInfo txn = cfk.get(i);
                long bits = txn.status().ordinal();
                int bitIndex = 4;

                // TODO (desired): these are all single bit flags, so we could do explicit shift and mask to produce this value
                int statusHasExecuteAt = txn.hasExecuteAt() ? 1 : 0;
                int statusHasDeps = txn.hasDeps() ? 1 : 0;
                int statusHasBallot = txn.hasBallot() ? 1 : 0;

                int statusOverrides = txn.statusOverrides();
                bits |= ((long)statusOverrides) << bitIndex;
                bitIndex += 1 & (flagsPlus >>> HAS_STATUS_OVERRIDES_HEADER_BIT_SHIFT);

                long hasExecuteAt = txn.executeAt != txn ? 1 : 0;
                Invariants.require(hasExecuteAt <= statusHasExecuteAt);
                bits |= hasExecuteAt << bitIndex;
                bitIndex += statusHasExecuteAt & (flagsPlus >>> HAS_EXECUTE_AT_HEADER_BIT_SHIFT);

                long hasMissingIds = txn.getClass() == TxnInfoExtra.class && ((TxnInfoExtra)txn).missing != NO_TXNIDS ? 1 : 0;
                bits |= hasMissingIds << bitIndex;
                bitIndex += statusHasDeps & (flagsPlus >>> HAS_MISSING_DEPS_HEADER_BIT_SHIFT);

                long hasBallot = txn.getClass() == TxnInfoExtra.class && ((TxnInfoExtra)txn).ballot != Ballot.ZERO ? 1 : 0;
                bits |= hasBallot << bitIndex;
                bitIndex += statusHasBallot & (flagsPlus >>> HAS_BALLOT_HEADER_BIT_SHIFT);

                if (txn.compareTo(InternalStatus.APPLIED_DURABLE) < 0)
                {
                    bits |= (long)txn.durablyCommittedBit() << bitIndex;
                    bitIndex += 1;
                }

                int encodedFlagBits;
                {
                    int flags = txn.flags();
                    encodedFlagBits = encodedFlagBits(flags, flagHistory);
                    flagHistory = updateFlagHistory(flags, encodedFlagBits, flagHistory);
                    bits |= (long)encodedFlagBits << bitIndex;
                    bitIndex += 3;
                }

                long hlcBits;
                int extraEpochDeltaBytes = 0;
                {
                    long epoch = txn.epoch();
                    long delta = epoch - prevEpoch;
                    long hlc = txn.hlc();
                    hlcBits = hlc - prevHlc;
                    if (delta == 0)
                    {
                        bitIndex++;
                    }
                    else
                    {
                        bits |= 1L << bitIndex++;
                        if (hlcBits < 0)
                        {
                            hlcBits = -1 - hlcBits;
                            bits |= 1L << bitIndex;
                        }
                        bitIndex++;
                        if (delta > 1)
                        {
                            if (delta <= 0xf)
                            {
                                bits |= 1L << bitIndex;
                                bits |= delta << (bitIndex + 2);
                                bitIndex += 4;
                            }
                            else
                            {
                                bits |= (delta <= 0xff ? 2L : 3L) << bitIndex;
                                extraEpochDeltaBytes = Ints.checkedCast(delta);
                            }
                        }
                        bitIndex += 2;
                    }
                    prevEpoch = epoch;
                    prevHlc = hlc;
                }

                bits |= ((long)Arrays.binarySearch(nodeIds, 0, nodeIdCount, txn.node.id)) << bitIndex;
                bitIndex += bitsPerNodeId;

                bits |= hlcBits << (bitIndex + 2);
                hlcBits >>>= 8*headerBytes - (bitIndex + 2);
                int hlcFlag = getHlcFlag(hlcFlagLookup, (7 + numberOfBitsToRepresent(hlcBits))/8);
                bits |= ((long)hlcFlag) << bitIndex;

                writeLeastSignificantBytes(bits, headerBytes, out);
                writeLeastSignificantBytes(hlcBits, getHlcBytes(hlcBytesLookup, hlcFlag), out);

                if (encodedFlagBits < 2)
                {
                    int flags = txn.flags();
                    if (encodedFlagBits == 0) out.put((byte)flags);
                    else out.putShort((short)flags);
                }

                if (extraEpochDeltaBytes > 0)
                {
                    if (extraEpochDeltaBytes <= 0xff) out.put((byte)extraEpochDeltaBytes);
                    else out.putInt(extraEpochDeltaBytes);
                }
            }

            VIntCoding.writeUnsignedVInt32(unmanagedPendingCommitCount, out);
            VIntCoding.writeUnsignedVInt32(cfk.unmanagedCount() - unmanagedPendingCommitCount, out);
            Unmanaged.Pending pending = unmanagedPendingCommitCount == 0 ? Unmanaged.Pending.APPLY : Unmanaged.Pending.COMMIT;
            {
                int bytesPerNodeId = (bitsPerNodeId + 7) / 8;
                for (int i = 0 ; i < cfk.unmanagedCount() ; ++i)
                {
                    Unmanaged unmanaged = cfk.getUnmanaged(i);
                    Invariants.require(unmanaged.pending == pending);

                    writeTxnId(unmanaged.txnId, out, nodeIds, nodeIdCount, bytesPerNodeId);
                    writeTimestamp(unmanaged.waitingUntil, out, nodeIds, nodeIdCount, bytesPerNodeId);
                    if (--unmanagedPendingCommitCount == 0) pending = Unmanaged.Pending.APPLY;
                }
            }

            if ((executeAtCount | missingIdCount | ballotCount) > 0)
            {
                int bitsPerCommandId =  numberOfBitsToRepresent(commandCount);
                int bitsPerMissingId = 1 + bitsPerCommandId;
                int bitsPerExecuteAt = bitsPerExecuteAtEpoch + bitsPerExecuteAtHlc + bitsPerExecuteAtFlags + bitsPerNodeId;
                if (bitsPerBallotFlags == 15) bitsPerBallotFlags = 16;
                int bitsPerBallot = bitsPerBallotEpoch + bitsPerBallotHlc + bitsPerBallotFlags + bitsPerNodeId;
                Invariants.require(bitsPerExecuteAtEpoch < 64);
                Invariants.require(bitsPerExecuteAtHlc <= 64);
                Invariants.require(bitsPerExecuteAtFlags <= 16);
                if (0 != (globalFlags & HAS_EXECUTE_AT_HEADER_BIT)) // we encode both 15 and 16 bits for flag length as 15 to fit in a short
                    out.putShort((short) ((bitsPerExecuteAtEpoch << 10) | ((bitsPerExecuteAtHlc-1) << 4) | (Math.min(15, bitsPerExecuteAtFlags))));
                if (0 != (globalFlags & HAS_BALLOT_HEADER_BIT)) // we encode both 15 and 16 bits for flag length as 15 to fit in a short
                    out.putShort((short) ((bitsPerBallotEpoch << 10) | ((bitsPerBallotHlc-1) << 4) | (Math.min(15, bitsPerBallotFlags))));
                long buffer = 0L;
                int bufferCount = 0;

                if (0 != (globalFlags & HAS_MISSING_DEPS_FLAGS_HEADER_BIT))
                {
                    Invariants.require(bitsPerMissingIdFlags < 16);
                    buffer = flushBits(buffer, bufferCount, bitsPerMissingIdFlags, 4, out);
                    bufferCount = (bufferCount + 4) & 63;
                    ++bitsPerMissingId;
                }

                Ballot prevBallot = null;
                for (int i = 0 ; i < commandCount ; ++i)
                {
                    TxnInfo txn = cfk.get(i);
                    if (txn.executeAt != txn)
                    {
                        Timestamp executeAt = txn.executeAt;
                        int nodeIdx = Arrays.binarySearch(nodeIds, 0, nodeIdCount, executeAt.node.id);
                        if (bitsPerExecuteAt <= 64)
                        {
                            Invariants.require(executeAt.epoch() >= txn.epoch());
                            long executeAtBits = executeAt.epoch() - txn.epoch();
                            int offset = bitsPerExecuteAtEpoch;
                            executeAtBits |= (executeAt.hlc() - txn.hlc()) << offset ;
                            offset += bitsPerExecuteAtHlc;
                            executeAtBits |= ((long)executeAt.flags()) << offset;
                            offset += bitsPerExecuteAtFlags;
                            executeAtBits |= ((long)nodeIdx) << offset;
                            buffer = flushBits(buffer, bufferCount, executeAtBits, bitsPerExecuteAt, out);
                            bufferCount = (bufferCount + bitsPerExecuteAt) & 63;
                        }
                        else
                        {
                            buffer = flushBits(buffer, bufferCount, executeAt.epoch() - txn.epoch(), bitsPerExecuteAtEpoch, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtEpoch) & 63;
                            buffer = flushBits(buffer, bufferCount, executeAt.hlc() - txn.hlc(), bitsPerExecuteAtHlc, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtHlc) & 63;
                            buffer = flushBits(buffer, bufferCount, executeAt.flags(), bitsPerExecuteAtFlags, out);
                            bufferCount = (bufferCount + bitsPerExecuteAtFlags) & 63;
                            buffer = flushBits(buffer, bufferCount, nodeIdx, bitsPerNodeId, out);
                            bufferCount = (bufferCount + bitsPerNodeId) & 63;
                        }
                    }

                    if (txn.getClass() == TxnInfoExtra.class)
                    {
                        TxnInfoExtra extra = (TxnInfoExtra) txn;

                        TxnId[] missing = extra.missing;
                        if (missing.length > 0)
                        {
                            for (int j = 0 ; j < missing.length ; ++j)
                            {
                                TxnId txnId = missing[j];
                                int missingId = cfk.indexOf(txnId);
                                int nonIdentityFlags = txnId.nonIdentityFlags();
                                missingId |= (nonIdentityFlags != 0 ? 1 : 0) << (bitsPerCommandId + 1);
                                missingId |= (j == missing.length - 1 ? 1 : 0) << bitsPerCommandId;
                                buffer = flushBits(buffer, bufferCount, missingId, bitsPerMissingId, out);
                                bufferCount = (bufferCount + bitsPerMissingId) & 63;
                                if (nonIdentityFlags != 0)
                                {
                                    buffer = flushBits(buffer, bufferCount, nonIdentityFlags, bitsPerMissingIdFlags, out);
                                    bufferCount = (bufferCount + bitsPerMissingIdFlags) & 63;
                                }
                            }
                        }

                        Ballot ballot = extra.ballot;
                        if (ballot != Ballot.ZERO)
                        {
                            int nodeIdx = Arrays.binarySearch(nodeIds, 0, nodeIdCount, ballot.node.id);
                            if (prevBallot == null)
                            {
                                buffer = flushBits(buffer, bufferCount, ballot.msb, 64, out);
                                buffer = flushBits(buffer, bufferCount, ballot.lsb, 64, out);
                                buffer = flushBits(buffer, bufferCount, nodeIdx, bitsPerNodeId, out);
                                bufferCount = (bufferCount + bitsPerNodeId) & 63;
                            }
                            else if (bitsPerBallot <= 64)
                            {
                                long ballotBits = encodeZigZag64(ballot.epoch() - prevBallot.epoch());
                                int offset = bitsPerBallotEpoch;
                                ballotBits |= encodeZigZag64(ballot.hlc() - prevBallot.hlc()) << offset ;
                                offset += bitsPerBallotHlc;
                                ballotBits |= ((long)ballot.flags()) << offset;
                                offset += bitsPerBallotFlags;
                                ballotBits |= ((long)nodeIdx) << offset;
                                buffer = flushBits(buffer, bufferCount, ballotBits, bitsPerBallot, out);
                                bufferCount = (bufferCount + bitsPerBallot) & 63;
                            }
                            else
                            {
                                buffer = flushBits(buffer, bufferCount, encodeZigZag64(ballot.epoch() - prevBallot.epoch()), bitsPerBallotEpoch, out);
                                bufferCount = (bufferCount + bitsPerBallotEpoch) & 63;
                                buffer = flushBits(buffer, bufferCount, encodeZigZag64(ballot.hlc() - prevBallot.hlc()), bitsPerBallotHlc, out);
                                bufferCount = (bufferCount + bitsPerBallotHlc) & 63;
                                buffer = flushBits(buffer, bufferCount, ballot.flags(), bitsPerBallotFlags, out);
                                bufferCount = (bufferCount + bitsPerBallotFlags) & 63;
                                buffer = flushBits(buffer, bufferCount, nodeIdx, bitsPerNodeId, out);
                                bufferCount = (bufferCount + bitsPerNodeId) & 63;
                            }
                            prevBallot = ballot;
                        }
                    }
                }

                writeMostSignificantBytes(buffer, (bufferCount + 7)/8, out);
            }

            Invariants.require(!out.hasRemaining());
            out.flip();
            return out;
        }
        finally
        {
            cachedInts().forceDiscard(nodeIds);
        }
    }

    public static CommandsForKey fromBytes(RoutingKey key, ByteBuffer in)
    {
        return fromBytes(key, in, true);
    }

    public static CommandsForKey fromBytes(RoutingKey key, ByteBuffer in, boolean duplicate)
    {
        if (!in.hasRemaining())
            return null;

        if (duplicate)
            in = in.duplicate();

        int commandCount = VIntCoding.readUnsignedVInt32(in) - 1;
        if (commandCount <= 0)
        {
            if (commandCount == -1)
                return new CommandsForKey(key);

            long maxUniqueHlc = VIntCoding.readUnsignedVInt(in);
            return new CommandsForKey(key).updateUniqueHlc(maxUniqueHlc);
        }

        TxnId[] txnIds = cachedTxnIds().get(commandCount);
        int[] commandFlags = cachedInts().getInts(commandCount);
        TxnInfo[] txns = new TxnInfo[commandCount];
        int nodeIdCount = VIntCoding.readUnsignedVInt32(in);
        int bitsPerNodeId = numberOfBitsToRepresent(nodeIdCount);
        long nodeIdMask = (1L << bitsPerNodeId) - 1;
        Object[] nodeIds = cachedAny().get(nodeIdCount);
        {
            int prev = VIntCoding.readUnsignedVInt32(in);
            nodeIds[0] = new Id(prev);
            for (int i = 1 ; i < nodeIdCount ; ++i)
                nodeIds[i] = new Id(prev += VIntCoding.readUnsignedVInt32(in));
        }

        int globalFlags = (int) readLeastSignificantBytes(3, in);
        int headerByteCount, hlcBytesLookup;
        {
            headerByteCount = 1 + ((globalFlags >>> 14) & 0x3);
            hlcBytesLookup = setHlcByteDeltas((globalFlags >>> 16) & 0x3, (globalFlags >>> 18) & 0x3, (globalFlags >>> 20) & 0x3, (globalFlags >>> 22) & 0x3);
        }

        long flagHistory = EMPTY_FLAG_HISTORY;
        QuickBounds bounds;
        long prevEpoch, prevHlc, maxUniqueHlc = 0;
        {
            long startEpoch, endEpoch;
            {
                startEpoch = VIntCoding.readUnsignedVInt(in);
                boolean hasEndEpoch = (startEpoch & 1) == 1;
                startEpoch /= 2;
                if (hasEndEpoch) endEpoch = startEpoch + VIntCoding.readUnsignedVInt(in);
                else endEpoch = Long.MAX_VALUE;
            }

            bounds = NO_BOUNDS_INFO.withEpochs(startEpoch, endEpoch);
            prevEpoch = startEpoch + VIntCoding.readUnsignedVInt(in);
            prevHlc = VIntCoding.readUnsignedVInt(in);
            {
                int flags = ((globalFlags & HAS_BOUNDS_FLAGS_HEADER_BIT) == 0) ? RX_FLAGS : VIntCoding.readUnsignedVInt32(in);
                Id node = (Id)nodeIds[VIntCoding.readUnsignedVInt32(in)];
                bounds = bounds.withShardAppliedHlcBoundBeforeAtLeast(TxnId.fromValues(prevEpoch, prevHlc, flags, node));
            }
            if (0 != (globalFlags & HAS_BOOTSTRAPPED_AT_HEADER_BIT))
            {
                long epoch = prevEpoch + VIntCoding.readUnsignedVInt(in);
                long hlc = prevHlc + VIntCoding.readVInt(in);
                int flags = ((globalFlags & HAS_BOUNDS_FLAGS_HEADER_BIT) == 0) ? RX_FLAGS : VIntCoding.readUnsignedVInt32(in);
                Id node = (Id)nodeIds[VIntCoding.readUnsignedVInt32(in)];
                bounds = bounds.withReadyAtLeast(TxnId.fromValues(epoch, hlc, flags, node));
            }
            if (0 != (globalFlags & HAS_MAX_HLC_HEADER_BIT))
                maxUniqueHlc = bounds.cleanCfkBefore().hlc() + VIntCoding.readVInt(in);
        }
        int prunedBeforeIndex = VIntCoding.readUnsignedVInt32(in) - 1;

        for (int i = 0 ; i < commandCount ; ++i)
        {
            long header = readLeastSignificantBytes(headerByteCount, in);
            header |= 1L << (8 * headerByteCount); // marker so we know where to shift-left most-significant bytes to
            int commandDecodeFlags = (int)(header & 0xF);
            InternalStatus status = DECODE_STATUS[commandDecodeFlags];
            header >>>= 4;
            commandDecodeFlags <<= 6;

            {
                int overrideMask = (globalFlags >>> HAS_STATUS_OVERRIDES_HEADER_BIT_SHIFT) & 0x1;
                int statusOverrides = (int) (header & overrideMask);
                header >>>= overrideMask;
                commandDecodeFlags |= statusOverrides << 3;

                int statusFlags = status.flags ^ statusOverrides;
                int hasExecuteAt = (statusFlags >>> 1) & (globalFlags >>> HAS_EXECUTE_AT_HEADER_BIT_SHIFT);
                int hasMissingDeps = (statusFlags & 0x1) & (globalFlags >>> HAS_MISSING_DEPS_HEADER_BIT_SHIFT);
                commandDecodeFlags |= ((int)header & hasExecuteAt) << 1;
                header >>>= hasExecuteAt;
                commandDecodeFlags |= ((int)header & hasMissingDeps);
                header >>>= hasMissingDeps;
                int ballotMask = status.hasBallot ? ((globalFlags >>> HAS_BALLOT_HEADER_BIT_SHIFT) & 0x1) : 0;
                commandDecodeFlags |= ((int)header & ballotMask) << 2;
                header >>>= ballotMask;
                if (status.compareTo(InternalStatus.APPLIED_DURABLE) < 0)
                {
                    commandDecodeFlags |= ((int)header & 1) << 4;
                    header >>>= 1;
                }
                commandFlags[i] = commandDecodeFlags;
            }

            int encodedFlags = (int)header & 7;
            header >>>= 3;

            boolean hlcIsNegative = false;
            long epoch = prevEpoch;
            int readEpochBytes = 0;
            {
                boolean hasEpochDelta = (header & 1) == 1;
                header >>>= 1;
                if (hasEpochDelta)
                {
                    hlcIsNegative = (header & 1) == 1;
                    header >>>= 1;

                    int epochFlag = ((int)header & 0x3);
                    header >>>= 2;
                    switch (epochFlag)
                    {
                        default: throw new AssertionError("Unexpected value not 0-3");
                        case 0: ++epoch; break;
                        case 1: epoch += (header & 0xf); header >>>= 4; break;
                        case 2: readEpochBytes = 1; break;
                        case 3: readEpochBytes = 4; break;
                    }
                }
            }

            Id node = (Id)nodeIds[(int)(header & nodeIdMask)];
            header >>>= bitsPerNodeId;

            int readHlcBytes = getHlcBytes(hlcBytesLookup, (int)(header & 0x3));
            header >>>= 2;

            long hlc = header;
            {
                long highestBit = Long.highestOneBit(hlc);
                hlc ^= highestBit;
                int hlcShift = Long.numberOfTrailingZeros(highestBit);
                hlc |= readLeastSignificantBytes(readHlcBytes, in) << hlcShift;
            }
            if (hlcIsNegative)
                hlc = -1-hlc;
            hlc += prevHlc;

            int decodedFlags;
            switch (encodedFlags)
            {
                case 0: decodedFlags = in.get() & 0xff; break;
                case 1: decodedFlags = in.getShort() & 0xffff; break;
                default: decodedFlags = selectFlagHistory(encodedFlags, flagHistory);
            }
            if (readEpochBytes > 0)
                epoch += readEpochBytes == 1 ? (in.get() & 0xff) : in.getInt();

            TxnId txnId = TxnId.fromValues(epoch, hlc, decodedFlags, node);
            txnIds[i] = txnId;
            flagHistory = updateFlagHistory(decodedFlags, encodedFlags, flagHistory);

            prevEpoch = epoch;
            prevHlc = hlc;
        }

        int unmanagedPendingCommitCount = VIntCoding.readUnsignedVInt32(in);
        int unmanagedCount = unmanagedPendingCommitCount + VIntCoding.readUnsignedVInt32(in);
        Unmanaged[] unmanageds;
        if (unmanagedCount == 0)
        {
            unmanageds = NO_PENDING_UNMANAGED;
        }
        else
        {
            unmanageds = new Unmanaged[unmanagedCount];
            Unmanaged.Pending pending = unmanagedPendingCommitCount == 0 ? Unmanaged.Pending.APPLY : Unmanaged.Pending.COMMIT;
            int bytesPerNodeId = (bitsPerNodeId + 7) / 8;
            for (int i = 0 ; i < unmanagedCount ; ++i)
            {
                TxnId txnId = readTxnId(in, nodeIds, bytesPerNodeId);
                Timestamp waitingUntil = readTimestamp(in, nodeIds, bytesPerNodeId);
                unmanageds[i] = new Unmanaged(pending, txnId, waitingUntil);
                if (--unmanagedPendingCommitCount == 0) pending = Unmanaged.Pending.APPLY;
            }
        }

        if (((globalFlags & HAS_EXECUTE_AT_HEADER_BIT) | (globalFlags & HAS_MISSING_DEPS_HEADER_BIT) | (globalFlags & HAS_BALLOT_HEADER_BIT)) != 0)
        {
            TxnId[] missingIdBuffer = cachedTxnIds().get(8);
            int missingIdCount = 0, maxIdBufferCount = 0;
            int bitsPerTxnId = numberOfBitsToRepresent(commandCount);
            int txnIdMask = (1 << bitsPerTxnId) - 1;
            int bitsPerMissingId = bitsPerTxnId + 1;

            int decodeExecuteAtBits = (globalFlags & HAS_EXECUTE_AT_HEADER_BIT) != 0 ? in.getShort() & 0xffff : 0;
            int bitsPerExecuteAtEpoch = decodeExecuteAtBits >>> 10;
            int bitsPerExecuteAtHlc = 1 + ((decodeExecuteAtBits >>> 4) & 0x3f);
            int bitsPerExecuteAtFlags = decodeExecuteAtBits & 0xf;
            if (bitsPerExecuteAtFlags == 15) bitsPerExecuteAtFlags = 16;
            int bitsPerExecuteAt = bitsPerExecuteAtEpoch + bitsPerExecuteAtHlc + bitsPerExecuteAtFlags + bitsPerNodeId;

            long executeAtEpochMask = bitsPerExecuteAtEpoch == 0 ? 0 : (-1L >>> (64 - bitsPerExecuteAtEpoch));
            long executeAtHlcMask = (-1L >>> (64 - bitsPerExecuteAtHlc));
            long executeAtFlagsMask = bitsPerExecuteAtFlags == 0 ? 0 : (-1L >>> (64 - bitsPerExecuteAtFlags));

            int decodeBallotBits = (globalFlags & HAS_BALLOT_HEADER_BIT) != 0 ? in.getShort() & 0xffff : 0;
            int bitsPerBallotEpoch = decodeBallotBits >>> 10;
            int bitsPerBallotHlc = 1 + ((decodeBallotBits >>> 4) & 0x3f);
            int bitsPerBallotFlags = decodeBallotBits & 0xf;
            if (bitsPerBallotFlags == 15) bitsPerBallotFlags = 16;
            int bitsPerBallot = bitsPerBallotEpoch + bitsPerBallotHlc + bitsPerBallotFlags + bitsPerNodeId;

            long ballotEpochMask = bitsPerBallotEpoch == 0 ? 0 : (-1L >>> (64 - bitsPerBallotEpoch));
            long ballotHlcMask = (-1L >>> (64 - bitsPerBallotHlc));
            long ballotFlagsMask = bitsPerBallotFlags == 0 ? 0 : (-1L >>> (64 - bitsPerBallotFlags));

            Ballot prevBallot = null;
            final BitUtils.BitReader reader = new BitUtils.BitReader();
            int bitsPerMissingIdFlags = 0;
            if (0 != (globalFlags & HAS_MISSING_DEPS_FLAGS_HEADER_BIT))
            {
                bitsPerMissingIdFlags = (int) reader.read(4, in);
                ++bitsPerMissingId;
            }

            for (int i = 0 ; i < commandCount ; ++i)
            {
                TxnId txnId = txnIds[i];
                int commandDecodeFlags = commandFlags[i];
                Timestamp executeAt = txnId;
                if ((commandDecodeFlags & HAS_EXECUTE_AT_HEADER_BIT) != 0)
                {
                    long epoch, hlc;
                    int flags;
                    Id id;
                    if (bitsPerExecuteAt <= 64)
                    {
                        long executeAtBits = reader.read(bitsPerExecuteAt, in);
                        epoch = txnId.epoch() + (executeAtBits & executeAtEpochMask);
                        executeAtBits >>>= bitsPerExecuteAtEpoch;
                        hlc = txnId.hlc() + (executeAtBits & executeAtHlcMask);
                        executeAtBits >>>= bitsPerExecuteAtHlc;
                        flags = (int)(executeAtBits & executeAtFlagsMask);
                        executeAtBits >>>= bitsPerExecuteAtFlags;
                        id = (Id)nodeIds[(int)(executeAtBits & nodeIdMask)];
                    }
                    else
                    {
                        epoch = txnId.epoch() + reader.read(bitsPerExecuteAtEpoch, in);
                        hlc = txnId.hlc() + reader.read(bitsPerExecuteAtHlc, in);
                        flags = (int) reader.read(bitsPerExecuteAtFlags, in);
                        id = (Id)nodeIds[(int)(reader.read(bitsPerNodeId, in))];
                    }
                    executeAt = Timestamp.fromValues(epoch, hlc, flags, id);
                }

                TxnId[] missing = NO_TXNIDS;
                if ((commandDecodeFlags & HAS_MISSING_DEPS_HEADER_BIT) != 0)
                {
                    int prev = -1;
                    while (true)
                    {
                        if (missingIdCount == missingIdBuffer.length)
                            missingIdBuffer = cachedTxnIds().resize(missingIdBuffer, missingIdCount, missingIdCount * 2);

                        int next = (int) reader.read(bitsPerMissingId, in);
                        Invariants.require(next > prev);
                        int controlBits = next >>> Integer.bitCount(txnIdMask);
                        next &= txnIdMask;
                        missingIdBuffer[missingIdCount++] = txnIds[next];
                        if (controlBits != 0)
                        {
                            // either finished this array or have non-standard flags
                            if (controlBits > 1)
                            {
                                int nonIdentityFlags = (int) reader.read(bitsPerMissingIdFlags, in);
                                missingIdBuffer[missingIdCount - 1] = missingIdBuffer[missingIdCount - 1].addFlags(nonIdentityFlags << TxnId.NON_IDENTITY_FLAGS_SHIFT);
                                controlBits ^= 2;
                            }
                            if (controlBits != 0)
                                break; // finished this array
                        }
                        prev = next;
                    }

                    missing = Arrays.copyOf(missingIdBuffer, missingIdCount);
                    maxIdBufferCount = missingIdCount;
                    missingIdCount = 0;
                }

                Ballot ballot = Ballot.ZERO;
                if ((commandDecodeFlags & HAS_BALLOT_HEADER_BIT) != 0)
                {
                    if (prevBallot == null)
                    {
                        long msb = reader.read(64, in);
                        long lsb = reader.read(64, in);
                        Id id = (Id)nodeIds[(int)(reader.read(bitsPerNodeId, in))];
                        ballot = Ballot.fromBits(msb, lsb, id);
                    }
                    else
                    {
                        long epoch, hlc;
                        int flags;
                        Id id;
                        if (bitsPerBallot <= 64)
                        {
                            long ballotBits = reader.read(bitsPerBallot, in);
                            epoch = prevBallot.epoch() + decodeZigZag64(ballotBits & ballotEpochMask);
                            ballotBits >>>= bitsPerBallotEpoch;
                            hlc = prevBallot.hlc() + decodeZigZag64(ballotBits & ballotHlcMask);
                            ballotBits >>>= bitsPerBallotHlc;
                            flags = (int)(ballotBits & ballotFlagsMask);
                            ballotBits >>>= bitsPerBallotFlags;
                            id = (Id)nodeIds[(int)(ballotBits & nodeIdMask)];
                        }
                        else
                        {
                            epoch = prevBallot.epoch() + decodeZigZag64(reader.read(bitsPerBallotEpoch, in));
                            hlc = prevBallot.hlc() + decodeZigZag64(reader.read(bitsPerBallotHlc, in));
                            flags = (int) reader.read(bitsPerBallotFlags, in);
                            id = (Id)nodeIds[(int)(reader.read(bitsPerNodeId, in))];
                        }
                        ballot = Ballot.fromValues(epoch, hlc, flags, id);
                    }

                    prevBallot = ballot;
                }

                int statusIndex = commandDecodeFlags >>> 6;
                InternalStatus status = DECODE_STATUS[statusIndex];
                int statusOverrides = (commandDecodeFlags >>> 3) & 0x1;
                boolean durablyCommitted = 0 != (commandDecodeFlags & 0x10);
                txns[i] = create(bounds, txnId, status, statusOverrides, durablyCommitted, executeAt, missing, ballot);
            }

            cachedTxnIds().forceDiscard(missingIdBuffer, maxIdBufferCount);
        }
        else
        {
            for (int i = 0 ; i < commandCount ; ++i)
            {
                int commandDecodeFlags = commandFlags[i];
                int statusIndex = commandDecodeFlags >>> 6;
                InternalStatus status = DECODE_STATUS[statusIndex];
                int statusOverrides = (commandDecodeFlags >>> 3) & 0x1;
                boolean durablyCommitted = 0 != (commandDecodeFlags & 0x10);
                txns[i] = create(bounds, txnIds[i], status, statusOverrides, durablyCommitted, txnIds[i], NO_TXNIDS, Ballot.ZERO);
            }
        }
        cachedTxnIds().forceDiscard(txnIds, commandCount);
        cachedAny().forceDiscard(nodeIds, nodeIdCount);
        return CommandsForKey.SerializerSupport.create(key, txns, maxUniqueHlc, unmanageds, prunedBeforeIndex == -1 ? TxnId.NONE : txns[prunedBeforeIndex], bounds, false);
    }

    private static TxnInfo create(QuickBounds bounds, @Nonnull TxnId txnId, InternalStatus status, int statusOverrides, boolean durablyCommitted, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing, @Nonnull Ballot ballot)
    {
        boolean mayExecute = status.isCommittedToExecute() ? CommandsForKey.executes(bounds, txnId, executeAt)
                                                           : CommandsForKey.mayExecute(bounds, txnId);
        return TxnInfo.create(txnId, status, mayExecute, statusOverrides, durablyCommitted, executeAt, missing, ballot);
    }

    private static int getHlcBytes(int lookup, int index)
    {
        return (lookup >>> (index * 4)) & 0xf;
    }

    private static int setHlcBytes(int value1, int value2, int value3, int value4)
    {
        return value1 | (value2 << 4) | (value3 << 8) | (value4 << 12);
    }

    private static int setHlcByteDeltas(int value1, int value2, int value3, int value4)
    {
        value2 += 1 + value1;
        value3 += 1 + value2;
        value4 += 1 + value3;
        return setHlcBytes(value1, value2, value3, value4);
    }

    private static int getHlcFlag(int flagsLookup, int bytes)
    {
        return (flagsLookup >>> (bytes * 2)) & 0x3;
    }

    private static boolean hasBoundsFlags(CommandsForKey cfk)
    {
        Timestamp readyAt = cfk.readyAt();
        Timestamp redundantBefore = cfk.redundantBefore();
        return (readyAt != null && readyAt.flags() != RX_FLAGS) || (redundantBefore.flags() != RX_FLAGS);
    }

    private static final int RX_FLAGS = new TxnId(0, 0, TrackStable.bit(), ExclusiveSyncPoint, Range, Id.NONE).flags();

    private static int hlcBytesLookupToHlcFlagLookup(int bytesLookup)
    {
        int flagsLookup = 0;
        int flagIndex = 0;
        for (int bytesIndex = 0 ; bytesIndex < 4 ; bytesIndex++)
        {
            int flagLimit = getHlcBytes(bytesLookup, bytesIndex);
            while (flagIndex <= flagLimit)
                flagsLookup |= bytesIndex << (2 * flagIndex++);
        }
        return flagsLookup;
    }

    private static int compact(int[] buffer, int usedSize)
    {
        Arrays.sort(buffer, 0, usedSize);
        int count = 0;
        int j = 0;
        while (j < usedSize)
        {
            int prev;
            buffer[count++] = prev = buffer[j];
            while (++j < usedSize && buffer[j] == prev) {}
        }
        return count;
    }

    // a static initial flag history that may capture some of the more common flags
    // TODO (desired): let the implementation define this, or encode a compressed set of flags in an initial bit mask
    static final long EMPTY_FLAG_HISTORY;
    static
    {
        long empty = 0L;
        empty |=        new TxnId(0, 0, TrackStable.bit() | PrivilegedCoordinatorWithoutDeps.bits, Read, Key, SingleKey, Id.NONE).flags();
        empty |= (long) new TxnId(0, 0, TrackStable.bit(), Read, Key, Id.NONE).flags() << 9;
        empty |= (long) new TxnId(0, 0, TrackStable.bit() | PrivilegedCoordinatorWithoutDeps.bits, Write, Key, SingleKey, Id.NONE).flags() << 18;
        empty |= (long) new TxnId(0, 0, TrackStable.bit(), Write, Key, SingleKey, Id.NONE).flags() << 27;
        empty |= (long) new TxnId(0, 0, TrackStable.bit(), Write, Key, Id.NONE).flags() << 36;
        empty |= (long) new TxnId(0, 0, TrackStable.bit(), ExclusiveSyncPoint, Range, Id.NONE).flags() << 45;
        EMPTY_FLAG_HISTORY = empty;
    }

    private static final long MATCH_MASK1 = replicateMatchBits(0x0ff);
    private static final long MATCH_INCR  = replicateMatchBits(0x001);
    private static final long MATCH_MASK2 = replicateMatchBits(0x100);
    private static final long MAP_FLAG_BITS = 2L  | (3L << 9) | (4L << 18) | (5L << 27) | (6L << 36) | (7L << 45);

    // copy the provided 8 bits into bit positions 9, 18, 27, 36 and 45
    private static long replicateMatchBits(long flags)
    {
        flags = flags | (flags << 9) | (flags << 18); // duplicate the flag values so they occur at both position 0 and 9
        return flags | (flags << 27);    // duplicate the flag values so they occur at both position 0, 9, 18, 27, 36, 45
    }

    // OR all 8 bits at each bit position 0, 9, 18, 27, 36 and 45 into position 0
    private static long collapseMatchBits(long flags)
    {
        flags = flags | (flags >>> 9) | (flags >>> 18);
        return flags | (flags >>> 27);
    }

    /**
     * The bits we write to the output to encode the input flags.
     * If the flags are equal to any of the flagHistory entries, we encode the relevant index (plus 2);
     * otherwise we encode how many bytes (1 or 2) we must read (as 0 or 1).
     *
     * 0 => write 1 byte flags
     * 1 => write 2 byte flags
     * 2 to 6 => index into the flagHistory for a recently encoded 1-byte flag
     */
    @VisibleForTesting
    static int encodedFlagBits(int flags, long flagHistory)
    {
        if (flags > 0xff)
            return 1;

        long flagMask = replicateMatchBits(flags);      // duplicate the flag values so they occur at both position 0, 9, 18, 27, 36, 45
        long matches = ~flagHistory ^ flagMask;         // set 0xff if either history position is equal to the current flags value
        matches &= MATCH_MASK1;                         // clear any unrelated bits, so we can use integer overflow
        matches += MATCH_INCR;                          // increment by 1 - if 0xff was set ONLY, this will overflow to the next higher bit
        matches &= MATCH_MASK2;                         // clear all but these overflow bits (if any)
        matches -= matches >>> 8;                       // subtract the overflow bit from itself shifted down by 8 - this will set 0xff in the correct position
        matches &= MAP_FLAG_BITS;                       // apply all potential masks simultaneously to the relevant decodeBits positions - only one is set (assuming no duplicate flag history entry)
        return (int) collapseMatchBits(matches) & 0xff; // collapse all 6 possible match positions into an answer (only one will be non-zero)
    }

    // update the flag history register, moving any decoded value into the most recent position and shifting all
    // values that were previously more recent back by one
    static long updateFlagHistory(int flags, int encoded, long flagHistory)
    {
        switch (encoded)
        {
            case 0:
            case 7: // we're evicting least recently used
                flagHistory <<= 9;
                flagHistory |= flags;
            case 2: // have re-used most recent value
                return flagHistory;
            default:
                long mask = -1L << flagHistoryShift(encoded + 1);
                flagHistory = (flagHistory & mask) | ((flagHistory << 9) & ~mask);
                flagHistory |= flags;
                return flagHistory;
        }
    }

    static int selectFlagHistory(int encoded, long flagHistory)
    {
        return (int) ((flagHistory >>> flagHistoryShift(encoded)) & 0xff);
    }

    private static int flagHistoryShift(int encodedFlagBits)
    {
        int pos = encodedFlagBits - 2;
        return (pos * 8) + pos;
    }

    private static Timestamp readTimestamp(ByteBuffer in, Object[] ids, int idBytes)
    {
        // TODO (desired): smarter encoding, separating into epoch,hlc,flag parts to be encoded separately
        //   OR encode Unmanaged as a list using same logic as we do for managed transactions
        long msb = in.getLong();
        long lsb = in.getLong();
        Id id = (Id)ids[(int) readLeastSignificantBytes(idBytes, in)];
        return Timestamp.fromBits(msb, lsb, id);
    }

    private static TxnId readTxnId(ByteBuffer in, Object[] ids, int idBytes)
    {
        long msb = in.getLong();
        long lsb = in.getLong();
        Id id = (Id)ids[(int) readLeastSignificantBytes(idBytes, in)];
        return TxnId.fromBits(msb, lsb, id);
    }

    private static void writeTimestamp(Timestamp timestamp, ByteBuffer out, int[] ids, int idCount, int idBytes)
    {
        // TODO (desired): smarter encoding, separating into epoch,hlc,flag parts to be encoded separately
        //   OR encode Unmanaged as a list using same logic as we do for managed transactions
        out.putLong(timestamp.msb);
        out.putLong(timestamp.lsb);
        int i = Arrays.binarySearch(ids, 0, idCount, timestamp.node.id);
        Invariants.require(i >= 0 && i < idCount);
        writeLeastSignificantBytes(i, idBytes, out);
    }

    private static void writeTxnId(TxnId txnId, ByteBuffer out, int[] ids, int idCount, int idBytes)
    {
        writeTimestamp(txnId, out, ids, idCount, idBytes);
    }

}
