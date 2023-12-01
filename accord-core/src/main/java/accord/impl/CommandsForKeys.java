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

import accord.api.VisibleForImplementation;
import accord.local.Command;
import accord.local.CommandStore;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.local.Command.NotDefined.uninitialised;

public class CommandsForKeys
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKeys.class);

    private CommandsForKeys() {}

    @VisibleForTesting
    @VisibleForImplementation
    public static TimestampsForKey updateMax(AbstractSafeCommandStore<?,?,?,?> safeStore, RoutableKey key, Timestamp timestamp)
    {
        SafeTimestampsForKey tfk = safeStore.timestampsForKey(key);
        return tfk.updateMax(timestamp);
    }

    public static Command.DurableAndIdempotentListener registerCommand(SafeTimestampsForKey timestamps, SafeCommandsForKey.Update<?,?> update, Command command)
    {

        update.common().commands().add(command.txnId(), command);

        timestamps.updateMax(command.executeAt());

        return new CommandsForKey.Listener(timestamps.key());
    }

    public static Command.DurableAndIdempotentListener registerCommand(AbstractSafeCommandStore<?,?,?,?> safeStore, RoutableKey key, Command command)
    {
        return registerCommand(safeStore.timestampsForKey(key), safeStore.getOrCreateCommandsForKeyUpdate(key), command);
    }

    public static void registerNotWitnessed(AbstractSafeCommandStore<?,?,?,?> safeStore, RoutableKey key, TxnId txnId)
    {
        SafeTimestampsForKey tfk = safeStore.timestampsForKey(key);
        SafeCommandsForKey cfk = safeStore.depsCommandsForKey(key);

        // FIXME: should be the recovery commands
        if (cfk.current().commands().commands.containsKey(txnId))
            return;

        tfk.updateMax(txnId);
        SafeCommandsForKey.Update<?,?> update = safeStore.getOrCreateCommandsForKeyUpdate(key);

        update.common().commands().add(txnId, uninitialised(txnId));
    }

    public static void listenerUpdate(AbstractSafeCommandStore<?,?,?,?> safeStore, RoutableKey listenerKey, Command command)
    {
        if (logger.isTraceEnabled())
            logger.trace("[{}]: updating as listener in response to change on {} with status {} ({})",
                         listenerKey, command.txnId(), command.status(), command);

        SafeTimestampsForKey tfk = safeStore.timestampsForKey(listenerKey);
        SafeCommandsForKey.Update<?,?> update = safeStore.getOrCreateCommandsForKeyUpdate(listenerKey);

        // add/remove the command on every listener update to avoid
        // special denormalization handling in Cassandra
        switch (command.status())
        {
            default: throw new AssertionError();
            case PreAccepted:
            case NotDefined:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Applied:
            case PreApplied:
            case Committed:
            case ReadyToExecute:
                update.common().commands().add(command.txnId(), command);
                break;
            case Invalidated:
                update.common().commands().remove(command.txnId());
            case Truncated:
                break;
        }

        tfk.updateMax(command.executeAt());
    }

    public static TimestampsForKey updateLastExecutionTimestamps(CommandStore commandStore, SafeTimestampsForKey tfk, Timestamp executeAt, boolean isForWriteTxn)
    {
        TimestampsForKey current = tfk.current();

        Timestamp lastWrite = current.lastWriteTimestamp();

        if (executeAt.compareTo(lastWrite) < 0)
        {
            if (!commandStore.safeToReadAt(executeAt).contains(tfk.key().toUnseekable()))
                return current;
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWrite));
        }

        Timestamp lastExecuted = current.lastExecutedTimestamp();
        int cmp = executeAt.compareTo(lastExecuted);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return current;

        if (cmp < 0)
        {
            if (!commandStore.safeToReadAt(executeAt).contains(tfk.key().toUnseekable()))
                return current;
            throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecuted));
        }

        long micros = executeAt.hlc();
        long lastMicros = current.lastExecutedHlc();

        Timestamp lastExecutedTimestamp = executeAt;
        long lastExecutedHlc = micros > lastMicros ? Long.MIN_VALUE : lastMicros + 1;
        Timestamp lastWriteTimestamp = isForWriteTxn ? executeAt : current.lastWriteTimestamp();

        return tfk.updateLastExecutionTimestamps(lastExecutedTimestamp, lastExecutedHlc, lastWriteTimestamp);
    }

    @VisibleForImplementation
    public static <D> TimestampsForKey updateLastExecutionTimestamps(AbstractSafeCommandStore<?,?,?,?> safeStore, RoutableKey key, Timestamp executeAt, boolean isForWriteTxn)
    {
        return updateLastExecutionTimestamps(safeStore.commandStore(), safeStore.timestampsForKey(key), executeAt, isForWriteTxn);
    }
}
