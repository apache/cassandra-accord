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
import accord.api.VisibleForImplementation;
import accord.impl.CommandTimeseries.CommandLoader;
import accord.local.Command;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static accord.local.Command.NotDefined.uninitialised;

public abstract class SafeCommandsForKey implements SafeState<CommandsForKey>
{
    private static final Logger logger = LoggerFactory.getLogger(SafeCommandsForKey.class);

    private final Key key;

    public SafeCommandsForKey(Key key)
    {
        this.key = key;
    }

    protected abstract void set(CommandsForKey update);

    public Key key()
    {
        return key;
    }

    private CommandsForKey update(CommandsForKey update)
    {
        set(update);
        return update;
    }

    public CommandsForKey initialize(CommandLoader<?> loader)
    {
        return update(new CommandsForKey(key, loader));
    }

    @VisibleForTesting
    @VisibleForImplementation
    public static Timestamp updateMax(CommandsForKey cfk, Timestamp timestamp)
    {
        Invariants.checkArgument(cfk != null || timestamp != null);
        if (cfk == null)
            return timestamp;
        if (timestamp == null)
            return cfk.max();
        return Timestamp.max(cfk.max(), timestamp);
    }

    @VisibleForTesting
    @VisibleForImplementation
    public <D> CommandsForKey updateMax(Timestamp timestamp)
    {
        CommandsForKey current = current();
        return update(new CommandsForKey(current.key(),
                                         updateMax(current, timestamp),
                                         current.lastExecutedTimestamp(),
                                         current.lastExecutedMicros(),
                                         current.lastWriteTimestamp(),
                                         (CommandTimeseries<D>) current().byId(),
                                         (CommandTimeseries<D>) current().byExecuteAt()));
    }

    public <D> CommandsForKey register(Command command)
    {
        CommandsForKey current = current();
        CommandTimeseries.Update<D> byId = (CommandTimeseries.Update<D>) current().byId().beginUpdate();
        CommandTimeseries.Update<D> byExecuteAt = (CommandTimeseries.Update<D>) current().byExecuteAt().beginUpdate();
        return update(new CommandsForKey(current.key(),
                                         updateMax(current, command.executeAt()),
                                         current.lastExecutedTimestamp(),
                                         current.lastExecutedMicros(),
                                         current.lastWriteTimestamp(),
                                         byId.add(command.txnId(), command).build(),
                                         byExecuteAt.add(command.txnId(), command).build() ));
    }

    public <D> CommandsForKey registerNotWitnessed(TxnId txnId)
    {
        CommandsForKey current = current();
        if (current.byId().commands.containsKey(txnId))
            return current;

        CommandTimeseries.Update<D> byId = (CommandTimeseries.Update<D>) current().byId().beginUpdate();
        CommandTimeseries.Update<D> byExecuteAt = (CommandTimeseries.Update<D>) current().byExecuteAt().beginUpdate();
        return update(new CommandsForKey(current.key(),
                                         updateMax(current, txnId),
                                         current.lastExecutedTimestamp(),
                                         current.lastExecutedMicros(),
                                         current.lastWriteTimestamp(),
                                         byId.add(txnId, uninitialised(txnId)).build(),
                                         byExecuteAt.add(txnId, uninitialised(txnId)).build()));
    }

    public <D> CommandsForKey listenerUpdate(Command command)
    {
        if (logger.isTraceEnabled())
            logger.trace("[{}]: updating as listener in response to change on {} with status {} ({})",
                         key(), command.txnId(), command.status(), command);

        CommandsForKey current = current();
        CommandTimeseries.Update<D> byId = (CommandTimeseries.Update<D>) current().byId().beginUpdate();
        CommandTimeseries.Update<D> byExecuteAt = (CommandTimeseries.Update<D>) current().byExecuteAt().beginUpdate();

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
                byId.add(command.txnId(), command);
                byExecuteAt.add(command.txnId(), command);
                break;
            case Applied:
            case Applying:
            case PreApplied:
            case Committed:
            case ReadyToExecute:
                byId.add(command.txnId(), command);
                byExecuteAt.remove(command.txnId());
                byExecuteAt.add(command.executeAt(), command);
                break;
            case Invalidated:
                byId.remove(command.txnId());
                byExecuteAt.remove(command.txnId());
            case Truncated:
                break;
        }

        return update(new CommandsForKey(current.key(),
                                         updateMax(current, command.executeAt()),
                                         current.lastExecutedTimestamp(),
                                         current.lastExecutedMicros(),
                                         current.lastWriteTimestamp(),
                                         byId.build(),
                                         byExecuteAt.build()));
    }

    @VisibleForImplementation
    public <D> CommandsForKey updateLastExecutionTimestamps(Timestamp executeAt, boolean isForWriteTxn)
    {
        CommandsForKey current = current();

        Timestamp lastWrite = current.lastWriteTimestamp();

        if (executeAt.compareTo(lastWrite) < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent write timestamp %s", executeAt, lastWrite));

        Timestamp lastExecuted = current.lastExecutedTimestamp();
        int cmp = executeAt.compareTo(lastExecuted);
        // execute can be in the past if it's for a read and after the most recent write
        if (cmp == 0 || (!isForWriteTxn && cmp < 0))
            return current;
        if (cmp < 0)
            throw new IllegalArgumentException(String.format("%s is less than the most recent executed timestamp %s", executeAt, lastExecuted));

        long micros = executeAt.hlc();
        long lastMicros = current.lastExecutedMicros();

        Timestamp lastExecutedTimestamp = executeAt;
        long lastExecutedMicros = Math.max(micros, lastMicros + 1);
        Timestamp lastWriteTimestamp = isForWriteTxn ? executeAt : current.lastWriteTimestamp();

        return update(new CommandsForKey(current.key(),
                                         current.max(),
                                         lastExecutedTimestamp,
                                         lastExecutedMicros,
                                         lastWriteTimestamp,
                                         (CommandTimeseries<D>) current.byId(),
                                         (CommandTimeseries<D>) current.byExecuteAt()));
    }
}
