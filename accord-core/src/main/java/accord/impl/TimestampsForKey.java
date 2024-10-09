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

package accord.impl;

import java.util.Objects;

import accord.api.RoutingKey;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

public class TimestampsForKey
{
    public static final long NO_LAST_EXECUTED_HLC = Long.MIN_VALUE;

    // TODO (required): this isn't safe - we won't reproduce the same timestamps on replay
    private static volatile boolean replay;

    public static void unsafeSetReplay(boolean newReplay)
    {
        replay = newReplay;
    }

    public static class SerializerSupport
    {
        public static TimestampsForKey create(RoutingKey key,
                                                  Timestamp lastExecutedTimestamp,
                                                  long lastExecutedHlc,
                                                  TxnId lastWriteId,
                                                  Timestamp lastWriteTimestamp)
        {
            return new TimestampsForKey(key, lastExecutedTimestamp, lastExecutedHlc, lastWriteId, lastWriteTimestamp);
        }
    }

    private final RoutingKey key;
    private final Timestamp lastExecutedTimestamp;
    // TODO (desired): we have leaked C* implementation details here
    private final long rawLastExecutedHlc;
    private final TxnId lastWriteId;
    private final Timestamp lastWriteTimestamp;

    public TimestampsForKey(RoutingKey key, Timestamp lastExecutedTimestamp, long rawLastExecutedHlc, TxnId lastWriteId, Timestamp lastWriteTimestamp)
    {
        this.key = key;
        this.lastExecutedTimestamp = lastExecutedTimestamp;
        this.rawLastExecutedHlc = rawLastExecutedHlc;
        this.lastWriteId = lastWriteId;
        this.lastWriteTimestamp = lastWriteTimestamp;
    }

    public TimestampsForKey(RoutingKey key)
    {
        this.key = key;
        this.lastExecutedTimestamp = Timestamp.NONE;
        this.rawLastExecutedHlc = 0;
        this.lastWriteId = TxnId.NONE;
        this.lastWriteTimestamp = Timestamp.NONE;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimestampsForKey that = (TimestampsForKey) o;
        return rawLastExecutedHlc == that.rawLastExecutedHlc && Objects.equals(key, that.key) && Objects.equals(lastExecutedTimestamp, that.lastExecutedTimestamp) && Objects.equals(lastWriteTimestamp, that.lastWriteTimestamp);
    }

    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public RoutingKey key()
    {
        return key;
    }

    public Timestamp lastExecutedTimestamp()
    {
        return lastExecutedTimestamp;
    }

    public long lastExecutedHlc()
    {
        return rawLastExecutedHlc == NO_LAST_EXECUTED_HLC ? lastExecutedTimestamp.hlc() : rawLastExecutedHlc;
    }

    public long rawLastExecutedHlc()
    {
        return rawLastExecutedHlc;
    }

    public TxnId lastWriteId()
    {
        return lastWriteId;
    }

    public Timestamp lastWriteTimestamp()
    {
        return lastWriteTimestamp;
    }

    public TimestampsForKey withoutRedundant(TxnId redundantBefore)
    {
        if (lastWriteId.compareTo(redundantBefore) < 0)
            return new TimestampsForKey(key);
        return this;
    }

    public boolean validateExecuteAtTime(Timestamp executeAt, boolean isForWriteTxn)
    {
        if (executeAt.compareTo(lastWriteTimestamp) < 0)
        {
            if (replay) return false;
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWriteTimestamp));
        }

        int cmp = executeAt.compareTo(lastExecutedTimestamp);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return true;

        if (replay) return false;
        else if (cmp < 0) throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecutedTimestamp));
        else throw new IllegalArgumentException(String.format("%s is greater than the most recent executed timestamp, cfk should be updated", executeAt, lastExecutedTimestamp));
    }

    public long hlcFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        if (!validateExecuteAtTime(executeAt, isForWriteTxn))
            return executeAt.hlc();
        return rawLastExecutedHlc == NO_LAST_EXECUTED_HLC ? lastExecutedTimestamp.hlc() : rawLastExecutedHlc;
    }

    public String toString()
    {
        return "TimestampsForKey@" + System.identityHashCode(this) + '{' +
               "key=" + key +
               ", lastExecutedTimestamp=" + lastExecutedTimestamp +
               ", rawLastExecutedHlc=" + rawLastExecutedHlc +
               ", lastWriteTimestamp=" + lastWriteTimestamp +
               '}';
    }
}
