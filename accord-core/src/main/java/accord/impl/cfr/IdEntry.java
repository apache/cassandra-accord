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

package accord.impl.cfr;

import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.BitUtils;
import accord.utils.Invariants;

public abstract class IdEntry extends TxnId
{
    public static class SerializerSupport
    {
        public static IdSingleEntry create(TxnId txnId, int serialized, Range range)
        {
            IdSingleEntry result = new IdSingleEntry(txnId, range);
            result.encoded = deserialize(serialized);
            return result;
        }

        public static IdMultiEntry create(TxnId txnId, int serialized, Ranges ranges)
        {
            IdMultiEntry result = new IdMultiEntry(txnId, ranges);
            result.encoded = deserialize(serialized);
            return result;
        }
    }

    static final int SAVE_STATUS_SHIFT = Status.Durability.ENCODING_BITS;
    static final int SAVE_STATUS_BITS = BitUtils.numberOfBitsToRepresent(SaveStatus.values().length - 1);
    static final int SAVE_STATUS_MASK = (1 << SAVE_STATUS_BITS) - 1;
    static final int EXECUTE_AT_BIT = 1 << (SAVE_STATUS_BITS + SAVE_STATUS_SHIFT);

    static final int SERIALIZED_EXECUTE_AT_BIT = 1 << 11;
    static final int SERIALIZED_EXECUTE_AT_BIT_OFFSET = Integer.numberOfTrailingZeros(EXECUTE_AT_BIT) - Integer.numberOfTrailingZeros(SERIALIZED_EXECUTE_AT_BIT);
    static final int SERIALIZED_ID_LOW_BITS_MASK = (SERIALIZED_EXECUTE_AT_BIT >>> SAVE_STATUS_SHIFT) - 1;
    static final int SERIALIZED_ID_HIGH_BITS_SHIFT = Status.Durability.ENCODING_BITS + 1;

    static final int[] SAVE_STATUS_ORDINAL_TO_ID;
    static final int[] SAVE_STATUS_ID_TO_ORDINAL;
    static
    {
        SaveStatus[] saveStatuses = SaveStatus.values();
        int[] idToOrdinal = new int[saveStatuses.length];
        int[] ordinalToId = new int[saveStatuses.length];
        for (int i = 0 ; i < saveStatuses.length ; i++)
        {
            SaveStatus saveStatus = saveStatuses[i];
            ordinalToId[i] = saveStatus.id;
            idToOrdinal[saveStatus.id] = i;
        }
        SAVE_STATUS_ORDINAL_TO_ID = ordinalToId;
        SAVE_STATUS_ID_TO_ORDINAL = idToOrdinal;
    }

    // backwards compatible format, handling new save statuses by remapping ordinals and shifting executeAt bit
    static int serialize(int encoded)
    {
        int durability = encoded & Status.Durability.ENCODING_MASK;
        int serializedExecuteAtBit = (encoded & EXECUTE_AT_BIT) >>> SERIALIZED_EXECUTE_AT_BIT_OFFSET;
        int serializedId = SAVE_STATUS_ORDINAL_TO_ID[saveStatusOrdinal(encoded)];
        return durability
               | (serializedId & SERIALIZED_ID_LOW_BITS_MASK) << SAVE_STATUS_SHIFT
               | serializedExecuteAtBit
               | ((serializedId & ~SERIALIZED_ID_LOW_BITS_MASK) << SERIALIZED_ID_HIGH_BITS_SHIFT);
    }

    static int deserialize(int serialized)
    {
        int durability = serialized & Status.Durability.ENCODING_MASK;
        int id = ((serialized >>> SAVE_STATUS_SHIFT) & SERIALIZED_ID_LOW_BITS_MASK)
                 | (serialized >>> SERIALIZED_ID_HIGH_BITS_SHIFT) & ~SERIALIZED_ID_LOW_BITS_MASK;
        int executeAtBit = (serialized & SERIALIZED_EXECUTE_AT_BIT) << SERIALIZED_EXECUTE_AT_BIT_OFFSET;
        int ordinal = SAVE_STATUS_ID_TO_ORDINAL[id];
        return durability
               | ordinal << SAVE_STATUS_SHIFT
               | executeAtBit;
    }

    int encoded;

    TxnId plainTxnId()
    {
        return new TxnId(this);
    }

    Status.Durability durability()
    {
        return Status.Durability.forEncoded(encoded & Status.Durability.ENCODING_MASK);
    }

    SaveStatus saveStatus()
    {
        return SaveStatus.forOrdinal(saveStatusOrdinal());
    }

    int saveStatusOrdinal()
    {
        return saveStatusOrdinal(encoded);
    }

    Timestamp maybeExecuteAt()
    {
        if ((encoded & EXECUTE_AT_BIT) != 0)
            return plainTxnId();
        return null;
    }

    public IdEntry(TxnId txnId)
    {
        super(txnId);
    }

    abstract IdEntry copy();

    abstract Ranges ranges();

    public boolean update(SaveStatus saveStatus, Status.Durability durability, Timestamp executeAt)
    {
        int newEncoded = (saveStatus.ordinal() << SAVE_STATUS_SHIFT) | durability.encoded() | (super.equals(executeAt) ? EXECUTE_AT_BIT : 0);
        if (encoded == newEncoded)
            return false;
        encoded = newEncoded;
        Invariants.require(durability() == durability);
        Invariants.require(saveStatus() == saveStatus);
        return true;
    }

    public final int serialize()
    {
        return serialize(encoded);
    }

    static int saveStatusOrdinal(int encoded)
    {
        return (encoded >>> SAVE_STATUS_SHIFT) & SAVE_STATUS_MASK;
    }
}
