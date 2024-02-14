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

import accord.api.Key;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

public class TimestampsForKey
{
    public static final long NO_LAST_EXECUTED_HLC = Long.MIN_VALUE;

    public static class SerializerSupport
    {
        public static TimestampsForKey create(Key key,
                                                  Timestamp lastExecutedTimestamp,
                                                  long lastExecutedHlc,
                                                  Timestamp lastWriteTimestamp)
        {
            return new TimestampsForKey(key, lastExecutedTimestamp, lastExecutedHlc, lastWriteTimestamp);
        }
    }

    private final Key key;
    private final Timestamp lastExecutedTimestamp;
    // TODO (desired): we have leaked C* implementation details here
    private final long rawLastExecutedHlc;
    private final Timestamp lastWriteTimestamp;

    public TimestampsForKey(Key key, Timestamp lastExecutedTimestamp, long rawLastExecutedHlc, Timestamp lastWriteTimestamp)
    {
        this.key = key;
        this.lastExecutedTimestamp = lastExecutedTimestamp;
        this.rawLastExecutedHlc = rawLastExecutedHlc;
        this.lastWriteTimestamp = lastWriteTimestamp;
    }

    public TimestampsForKey(Key key)
    {
        this.key = key;
        this.lastExecutedTimestamp = Timestamp.NONE;
        this.rawLastExecutedHlc = 0;
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

    public Key key()
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

    public Timestamp lastWriteTimestamp()
    {
        return lastWriteTimestamp;
    }

    public TimestampsForKey withoutRedundant(TxnId redundantBefore)
    {
        return new TimestampsForKey(key,
                                    lastExecutedTimestamp.compareTo(redundantBefore) < 0 ? Timestamp.NONE : lastExecutedTimestamp,
                                    rawLastExecutedHlc < redundantBefore.hlc() ? NO_LAST_EXECUTED_HLC : rawLastExecutedHlc,
                                    lastWriteTimestamp.compareTo(redundantBefore) < 0 ? Timestamp.NONE : lastWriteTimestamp);
    }

    public void validateExecuteAtTime(Timestamp executeAt, boolean isForWriteTxn)
    {
        if (executeAt.compareTo(lastWriteTimestamp) < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWriteTimestamp));

        int cmp = executeAt.compareTo(lastExecutedTimestamp);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return;
        if (cmp < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecutedTimestamp));
        else
            throw new IllegalArgumentException(String.format("%s is greater than the most recent executed timestamp, cfk should be updated", executeAt, lastExecutedTimestamp));
    }

    public long hlcFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        validateExecuteAtTime(executeAt, isForWriteTxn);
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
