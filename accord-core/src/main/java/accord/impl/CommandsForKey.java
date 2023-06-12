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

import accord.api.Key;
import accord.impl.CommandTimeseries.CommandLoader;
import accord.local.*;
import accord.primitives.*;
import com.google.common.collect.ImmutableSortedMap;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static accord.local.Status.PreAccepted;
import static accord.local.Status.PreCommitted;

public class CommandsForKey implements CommandTimeseriesHolder
{
    public static class SerializerSupport
    {
        public static Listener listener(Key key)
        {
            return new Listener(key);
        }

        public static  <D> CommandsForKey create(Key key, Timestamp max,
                                                 Timestamp lastExecutedTimestamp, long lastExecutedMicros, Timestamp lastWriteTimestamp,
                                                 CommandLoader<D> loader,
                                                 ImmutableSortedMap<Timestamp, D> byId,
                                                 ImmutableSortedMap<Timestamp, D> byExecuteAt)
        {
            return new CommandsForKey(key, max, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp, loader, byId, byExecuteAt);
        }
    }

    public static class Listener implements Command.DurableAndIdempotentListener
    {
        protected final Key listenerKey;

        public Listener(Key listenerKey)
        {
            this.listenerKey = listenerKey;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
            return listenerKey.equals(that.listenerKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerKey);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerKey + '}';
        }

        public Key key()
        {
            return listenerKey;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            SafeCommandsForKey cfk = ((AbstractSafeCommandStore) safeStore).commandsForKey(listenerKey);
            cfk.listenerUpdate(safeCommand.current());
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(caller, Keys.of(listenerKey));
        }
    }

    // TODO (now): add validation that anything inserted into *committedBy* has everything prior in its dependencies
    private final Key key;
    private final Timestamp max;
    private final Timestamp lastExecutedTimestamp;
    private final long lastExecutedMicros;
    private final Timestamp lastWriteTimestamp;
    private final CommandTimeseries<?> byId;
    // TODO (expected): we probably do not need to separately maintain byExecuteAt - probably better to just filter byId
    private final CommandTimeseries<?> byExecuteAt;

    <D> CommandsForKey(Key key, Timestamp max,
                       Timestamp lastExecutedTimestamp,
                       long lastExecutedMicros,
                       Timestamp lastWriteTimestamp,
                       CommandTimeseries<D> byId,
                       CommandTimeseries<D> byExecuteAt)
    {
        this.key = key;
        this.max = max;
        this.lastExecutedTimestamp = lastExecutedTimestamp;
        this.lastExecutedMicros = lastExecutedMicros;
        this.lastWriteTimestamp = lastWriteTimestamp;
        this.byId = byId;
        this.byExecuteAt = byExecuteAt;
    }

    <D> CommandsForKey(Key key, Timestamp max,
                       Timestamp lastExecutedTimestamp,
                       long lastExecutedMicros,
                       Timestamp lastWriteTimestamp,
                       CommandLoader<D> loader,
                       ImmutableSortedMap<Timestamp, D> committedById,
                       ImmutableSortedMap<Timestamp, D> committedByExecuteAt)
    {
        this(key, max, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp,
             new CommandTimeseries<>(key, loader, committedById),
             new CommandTimeseries<>(key, loader, committedByExecuteAt));
    }

    public <D> CommandsForKey(Key key, CommandLoader<D> loader)
    {
        this.key = key;
        this.max = Timestamp.NONE;
        this.lastExecutedTimestamp = Timestamp.NONE;
        this.lastExecutedMicros = 0;
        this.lastWriteTimestamp = Timestamp.NONE;
        this.byId = new CommandTimeseries<>(key, loader);
        this.byExecuteAt = new CommandTimeseries<>(key, loader);
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return lastExecutedMicros == that.lastExecutedMicros
                && key.equals(that.key)
                && Objects.equals(max, that.max)
                && Objects.equals(lastExecutedTimestamp, that.lastExecutedTimestamp)
                && Objects.equals(lastWriteTimestamp, that.lastWriteTimestamp)
                && byId.equals(that.byId)
                && byExecuteAt.equals(that.byExecuteAt);
    }

    @Override
    public final int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    public final Command.DurableAndIdempotentListener asListener()
    {
        return new Listener(key());
    }

    public Key key()
    {
        return key;
    }

    @Override
    public Timestamp max()
    {
        return max;
    }

    public Timestamp lastExecutedTimestamp()
    {
        return lastExecutedTimestamp;
    }

    public long lastExecutedMicros()
    {
        return lastExecutedMicros;
    }

    public Timestamp lastWriteTimestamp()
    {
        return lastWriteTimestamp;
    }

    @Override
    public CommandTimeseries<?> byId()
    {
        return byId;
    }

    @Override
    public CommandTimeseries<?> byExecuteAt()
    {
        return byExecuteAt;
    }

    public boolean hasRedundant(TxnId redundantBefore)
    {
        return byId.minTimestamp().compareTo(redundantBefore) < 0;
    }

    public CommandsForKey withoutRedundant(TxnId redundantBefore)
    {
        Timestamp removeExecuteAt = byId.maxExecuteAtBefore(redundantBefore);

        return new CommandsForKey(key, max, lastExecutedTimestamp, lastExecutedMicros, lastWriteTimestamp,
                                  (CommandTimeseries)byId.beginUpdate().removeBefore(redundantBefore).build(),
                                  (CommandTimeseries)byExecuteAt.beginUpdate().removeBefore(removeExecuteAt).build()
                                 );
    }

    public void forWitnessed(Timestamp minTs, Timestamp maxTs, Consumer<TxnId> consumer)
    {
        byId.between(minTs, maxTs, status -> status.hasBeen(PreAccepted)).forEach(consumer);
        byExecuteAt.between(minTs, maxTs, status -> status.hasBeen(PreCommitted)).forEach(consumer);
    }

    private static long getTimestampMicros(Timestamp timestamp)
    {
        return timestamp.hlc();
    }

    private void validateExecuteAtTime(Timestamp executeAt, boolean isForWriteTxn)
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

    public int nowInSecondsFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        validateExecuteAtTime(executeAt, isForWriteTxn);
        // we use the executeAt time instead of the monotonic database timestamp to prevent uneven
        // ttl expiration in extreme cases, ie 1M+ writes/second to a key causing timestamps to overflow
        // into the next second on some keys and not others.
        return Math.toIntExact(TimeUnit.MICROSECONDS.toSeconds(getTimestampMicros(lastExecutedTimestamp)));
    }

    public long timestampMicrosFor(Timestamp executeAt, boolean isForWriteTxn)
    {
        validateExecuteAtTime(executeAt, isForWriteTxn);
        return lastExecutedMicros;
    }
}
