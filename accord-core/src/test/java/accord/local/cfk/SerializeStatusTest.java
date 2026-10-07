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
import java.util.Random;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.impl.IntKey;
import accord.local.Node;
import accord.local.cfk.CommandsForKey.InternalStatus;
import accord.local.cfk.CommandsForKey.TxnInfo;
import accord.primitives.Ballot;
import accord.primitives.TxnId;
import accord.utils.BitUtils;
import accord.utils.VIntCoding;

import static accord.local.cfk.CommandsForKey.NO_BOUNDS_INFO;

/**
 * This file was authored by LLM
 * InternalStatus is serialized by id, using 4 bits unless some id requires 5 bits, in which case an extended header is used.
 */
public class SerializeStatusTest
{
    private static final InternalStatus[] ALL = Stream.of(InternalStatus.values()).filter(s -> s != InternalStatus.PRUNED).toArray(InternalStatus[]::new);
    private static final InternalStatus[] NOT_EXTENDED = Stream.of(ALL).filter(s -> s.id < 16).toArray(InternalStatus[]::new);

    private static boolean isExtended(ByteBuffer bytes)
    {
        bytes = bytes.duplicate();
        VIntCoding.readUnsignedVInt32(bytes); // command count
        int nodeIdCount = VIntCoding.readUnsignedVInt32(bytes);
        for (int i = 0 ; i < nodeIdCount ; ++i)
            VIntCoding.readUnsignedVInt32(bytes);
        int globalFlags = (int) BitUtils.readLeastSignificantBytes(3, bytes);
        return (globalFlags & 0x100) != 0;
    }

    @Test
    public void test()
    {
        Assertions.assertTrue(NOT_EXTENDED.length < ALL.length);
        Random rnd = new Random();
        long seed = rnd.nextLong();
        rnd.setSeed(seed);
        for (int round = 0 ; round < 10000 ; ++round)
        {
            InternalStatus[] statuses = rnd.nextBoolean() ? ALL : NOT_EXTENDED;
            int n = 1 + rnd.nextInt(20);
            TreeSet<TxnId> ids = new TreeSet<>();
            while (ids.size() < n)
                ids.add(TxnId.fromValues(1 + rnd.nextInt(3), 1 + rnd.nextInt(100000), rnd.nextBoolean() ? 0 : 2, new Node.Id(1 + rnd.nextInt(5))));

            TxnInfo[] info = new TxnInfo[n];
            boolean expectExtended = false;
            int i = 0;
            for (TxnId id : ids)
            {
                InternalStatus status = statuses[rnd.nextInt(statuses.length)];
                expectExtended |= status.id >= 16;
                info[i++] = TxnInfo.create(id, status, CommandsForKey.mayExecute(NO_BOUNDS_INFO, id), id, TxnId.NO_TXNIDS, Ballot.ZERO);
            }

            CommandsForKey expected = CommandsForKey.SerializerSupport.create(IntKey.routing(1), info, 0, CommandsForKey.NO_PENDING_UNMANAGED, TxnId.NONE, NO_BOUNDS_INFO, false);
            ByteBuffer bytes = Serialize.toBytesWithoutKey(expected);
            Assertions.assertEquals(expectExtended, isExtended(bytes), "seed " + seed);
            CommandsForKey roundTrip = Serialize.fromBytes(expected.key(), bytes);
            Assertions.assertEquals(expected, roundTrip, "seed " + seed);
            for (TxnInfo txn : info)
                Assertions.assertEquals(txn.status(), roundTrip.get(txn).status(), "seed " + seed);
        }
    }
}
