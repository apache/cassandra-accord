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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.local.Command.Truncated;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;

import static accord.local.StoreParticipants.Filter.LOAD;

public abstract class SafeCommand
{
    private final TxnId txnId;

    public SafeCommand(TxnId txnId)
    {
        this.txnId = txnId;
    }

    public abstract Command current();
    public abstract void invalidate();
    public abstract boolean invalidated();

    public boolean isUnset()
    {
        return current() == null;
    }

    protected abstract void set(Command command);

    public TxnId txnId()
    {
        return txnId;
    }

    public <C extends Command> C update(SafeCommandStore safeStore, C update)
    {
        return update(safeStore, update, false);
    }

    public <C extends Command> C update(SafeCommandStore safeStore, C update, boolean force)
    {
        Command prev = current();
        if (prev == update && !force)
            return update;

        Invariants.require(prev == null || update == SaveStatus.max(update, prev, Command::saveStatus, Command::acceptedOrCommitted) || update.saveStatus() == SaveStatus.Erased || update.saveStatus() == SaveStatus.Vestigial);
        set(update);
        safeStore.progressLog().update(safeStore, txnId, prev, update, force);
        safeStore.update(prev, update, force);
        return update;
    }

    public <C extends Command> C incidentalUpdate(C update)
    {
        if (current() == update)
            return update;

        set(update);
        return update;
    }

    public Command.Committed updateWaitingOn(SafeCommandStore safeStore, Command.WaitingOn.Update waitingOn)
    {
        return update(safeStore, Command.updateWaitingOn(current().asCommitted(), waitingOn));
    }

    public Command updateParticipants(SafeCommandStore safeStore, StoreParticipants participants)
    {
        Command prev = current();
        if (prev.participants() == participants)
            return prev;

        participants = participants.filter(LOAD, safeStore, prev.txnId(), prev.executeAtIfKnown());
        return update(safeStore, prev.updateParticipants(participants));
    }

    public Command.PreAccepted preaccept(SafeCommandStore safeStore, SaveStatus saveStatus, StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps)
    {
        return update(safeStore, Command.preaccept(current(), saveStatus, participants, promised, executeAt, partialTxn, partialDeps));
    }

    public Command.Accepted markDefined(SafeCommandStore safeStore, StoreParticipants participants, Ballot promised, PartialTxn partialTxn)
    {
        return update(safeStore, Command.markDefined(current(), participants, promised, partialTxn));
    }

    public Command updatePromised(Ballot promised)
    {
        return incidentalUpdate(current().updatePromised(promised));
    }

    public Command.Accepted accept(SafeCommandStore safeStore, SaveStatus status, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted)
    {
        return update(safeStore, Command.accept(current(), status, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted));
    }

    public Command notAccept(SafeCommandStore safeStore, Status status, Ballot ballot)
    {
        return update(safeStore, Command.notAccept(status, current(), ballot));
    }

    public Command.Committed commit(SafeCommandStore safeStore, @Nonnull StoreParticipants participants, Ballot ballot, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps)
    {
        return update(safeStore, Command.commit(current(), participants, ballot, executeAt, partialTxn, partialDeps));
    }

    public Command.Committed stable(SafeCommandStore safeStore, @Nonnull StoreParticipants participants, Ballot ballot, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Command.WaitingOn waitingOn)
    {
        return update(safeStore, Command.stable(current(), participants, ballot, executeAt, partialTxn, partialDeps, waitingOn));
    }

    public Truncated commitInvalidated(SafeCommandStore safeStore)
    {
        Command current = current();
        if (current.hasBeen(Status.Truncated))
            return (Truncated) current;

        return update(safeStore, Truncated.invalidated(current));
    }

    public Command precommit(SafeCommandStore safeStore, Timestamp executeAt, Ballot promisedAtLeast)
    {
        return update(safeStore, Command.precommit(current(), executeAt, promisedAtLeast));
    }

    public Command.Committed readyToExecute(SafeCommandStore safeStore)
    {
        return update(safeStore, Command.readyToExecute(current().asCommitted()));
    }

    public Command.Executed preapplied(SafeCommandStore safeStore, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return update(safeStore, Command.preapplied(current(), participants, promised, executeAt, partialTxn, partialDeps, waitingOn, writes, result));
    }

    public Command.Executed applying(SafeCommandStore safeStore)
    {
        return update(safeStore, Command.applying(current().asExecuted()));
    }

    public Command.Executed applying(SafeCommandStore safeStore, @Nonnull StoreParticipants participants, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return update(safeStore, Command.applying(current(), participants, executeAt, partialTxn, partialDeps, waitingOn, writes, result));
    }

    public Command.Executed applied(SafeCommandStore safeStore)
    {
        return applied(safeStore, false);
    }

    public Command.Executed applied(SafeCommandStore safeStore, boolean forceUpdate)
    {
        return update(safeStore, Command.applied(current().asExecuted()), forceUpdate);
    }

    public Command.Executed applied(SafeCommandStore safeStore, @Nonnull StoreParticipants participants, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return update(safeStore, Command.applied(current(), participants, executeAt, partialTxn, partialDeps, waitingOn, writes, result));
    }

    public Command.NotDefined uninitialised()
    {
        Invariants.requireArgument(current() == null);
        return incidentalUpdate(Command.NotDefined.uninitialised(txnId));
    }

    public Command initialise()
    {
        Command current = current();
        if (!current.saveStatus().isUninitialised())
            return current;
        return incidentalUpdate(Command.NotDefined.notDefined(current, current.promised()));
    }

    public static @Nullable Participants<?> maxParticipants(@Nullable SafeCommand safeCommand)
    {
        if (safeCommand == null)
            return null;
        Command command = safeCommand.current();
        if (command == null)
            return null;
        return command.maxParticipants();
    }
}
