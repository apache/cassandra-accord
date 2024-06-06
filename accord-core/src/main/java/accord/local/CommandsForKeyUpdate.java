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

package accord.local;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.local.CommandsForKey.TxnInfo;
import accord.primitives.Keys;
import accord.primitives.Seekables;
import accord.primitives.TxnId;

import static accord.local.CommandsForKey.InternalStatus.PREACCEPTED_OR_ACCEPTED_INVALIDATE;
import static accord.local.KeyHistory.COMMANDS;
import static accord.local.SaveStatus.LocalExecution.WaitingToExecute;

public abstract class CommandsForKeyUpdate
{
    @VisibleForTesting
    public abstract CommandsForKey cfk();
    abstract ExtraNotify notifier();
    abstract void notify(SafeCommandStore safeStore, @Nullable CommandsForKey prevCfk, @Nullable Command command, NotifySink notifySink);

    void notify(SafeCommandStore safeStore, @Nullable CommandsForKey prevCfk, @Nullable Command command)
    {
        notify(safeStore, prevCfk, command, DefaultNotifySink.INSTANCE);
    }
}

interface NotifySink
{
    void notWaiting(SafeCommandStore safeStore, TxnId txnId, Key key);
    void notWaiting(SafeCommandStore safeStore, SafeCommand safeCommand, Key key);
    void waitingOnCommit(SafeCommandStore safeStore, TxnInfo uncommitted, Key key);
}

class DefaultNotifySink implements NotifySink
{
    static final DefaultNotifySink INSTANCE = new DefaultNotifySink();

    @Override
    public void notWaiting(SafeCommandStore safeStore, TxnId txnId, Key key)
    {
        SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
        if (safeCommand != null) notWaiting(safeStore, safeCommand, key);
        else
        {
            PreLoadContext context = PreLoadContext.contextFor(txnId);
            safeStore.commandStore().execute(context, safeStore0 -> notWaiting(safeStore0, safeStore0.get(txnId), key))
                     .begin(safeStore.agent());
        }
    }

    @Override
    public void notWaiting(SafeCommandStore safeStore, SafeCommand safeCommand, Key key)
    {
        Commands.removeWaitingOnKeyAndMaybeExecute(safeStore, safeCommand, key);
    }

    @Override
    public void waitingOnCommit(SafeCommandStore safeStore, TxnInfo uncommitted, Key key)
    {
        TxnId txnId = uncommitted.plainTxnId();
        if (uncommitted.status.compareTo(PREACCEPTED_OR_ACCEPTED_INVALIDATE) < 0)
        {
            Keys keys = Keys.of(key);
            PreLoadContext context = PreLoadContext.contextFor(txnId, keys);
            if (safeStore.canExecuteWith(context)) doNotifyMaybeInvalidWaitingOnCommit(safeStore, txnId, key, keys);
            else safeStore.commandStore().execute(PreLoadContext.contextFor(txnId, keys, COMMANDS), safeStore0 -> doNotifyMaybeInvalidWaitingOnCommit(safeStore0, txnId, key, keys)).begin(safeStore.agent());
        }
        else
        {
            safeStore.progressLog().waiting(txnId, WaitingToExecute, null, Keys.of(key).toParticipants());
        }
    }

    private void doNotifyMaybeInvalidWaitingOnCommit(SafeCommandStore safeStore, TxnId txnId, Key key, Keys keys)
    {
        SafeCommand safeCommand = safeStore.get(txnId);
        safeCommand.initialise();
        Command command = safeCommand.current();
        Seekables<?, ?> keysOrRanges = command.keysOrRanges();
        if (keysOrRanges == null || !keysOrRanges.contains(key))
        {
            CommonAttributes.Mutable attrs = command.mutable();
            if (command.additionalKeysOrRanges() == null) attrs.additionalKeysOrRanges(keys);
            else attrs.additionalKeysOrRanges(keys.with((Keys)command.additionalKeysOrRanges()));
            safeCommand.update(safeStore, command.updateAttributes(attrs));
        }
        if (command.hasBeen(Status.Committed))
        {
            // if we're committed but not invalidated, that means EITHER we have raced with a commit+
            // OR we adopted as a dependency a
            safeStore.get(key).update(safeStore, safeCommand.current());
        }
        else
        {
            // TODO (desired): we could complicate our state machine to replicate PreCommitted here, so we can simply wait for ReadyToExclude
            safeStore.progressLog().waiting(txnId, WaitingToExecute, null, keys.toParticipants());
        }
    }
}

abstract class ExtraNotify
{
    final ExtraNotify prev;

    protected ExtraNotify(ExtraNotify prev)
    {
        this.prev = prev;
    }

    void notify(SafeCommandStore safeStore, Key key, NotifySink notifySink)
    {
        doNotify(safeStore, key, notifySink);
        if (prev != null)
            prev.notify(safeStore, key, notifySink);
    }

    abstract void doNotify(SafeCommandStore safeStore, Key key, NotifySink notifySink);
}

class CommandsForKeyUpdateWithNotifier extends CommandsForKeyUpdate
{
    final CommandsForKey cfk;
    final ExtraNotify extraNotify;

    CommandsForKeyUpdateWithNotifier(CommandsForKey cfk, ExtraNotify extraNotify)
    {
        this.cfk = cfk;
        this.extraNotify = extraNotify;
    }

    @Override
    public CommandsForKey cfk()
    {
        return cfk;
    }

    @Override
    ExtraNotify notifier()
    {
        return extraNotify;
    }

    @Override
    void notify(SafeCommandStore safeStore, @Nullable CommandsForKey prevCfk, @Nullable Command command, NotifySink notifySink)
    {
        cfk.notify(safeStore, prevCfk, command, notifySink);
        extraNotify.notify(safeStore, cfk.key(), notifySink);
    }
}
