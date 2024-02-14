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

import accord.api.Result;
import accord.local.Command.TransientListener;
import accord.local.Command.Truncated;
import accord.primitives.Ballot;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;

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
    public abstract void addListener(Command.TransientListener listener);
    public abstract boolean removeListener(Command.TransientListener listener);
    public abstract Listeners<Command.TransientListener> transientListeners();

    public boolean isEmpty()
    {
        return current() == null;
    }

    protected abstract void set(Command command);

    public TxnId txnId()
    {
        return txnId;
    }

    // TODO (expected): it isn't ideal to pass keysOrRanges for the special case of Accept. We can either:
    //   1 - remove the special case that permits accept without the definition
    //   2 - store some pseudo transaction with only the keys
    //   3 - just come up with something a bit neater
    <C extends Command> C update(SafeCommandStore safeStore, @Nullable Seekables<?, ?> keysOrRanges, C update)
    {
        Command prev = current();
        set(update);
        safeStore.update(prev, update, keysOrRanges);
        return update;
    }

     private <C extends Command> C incidentalUpdate(C update)
    {
        set(update);
        return update;
    }

    public Command addListener(Command.DurableAndIdempotentListener listener)
    {
        return incidentalUpdate(Command.addListener(current(), listener));
    }

    public void addAndInvokeListener(SafeCommandStore safeStore, TransientListener listener)
    {
        addListener(listener);
        listener.onChange(safeStore, this);
    }

    public Command removeListener(Command.DurableAndIdempotentListener listener)
    {
        Command current = current();
        if (!current.durableListeners().contains(listener))
            return current;
        return incidentalUpdate(Command.removeListener(current(), listener));
    }

    public Command.Committed updateWaitingOn(Command.WaitingOn.Update waitingOn)
    {
        return incidentalUpdate(Command.updateWaitingOn(current().asCommitted(), waitingOn));
    }

    public Command updateAttributes(CommonAttributes attrs)
    {
        return incidentalUpdate(current().updateAttributes(attrs));
    }

    public Command.PreAccepted preaccept(SafeCommandStore safeStore, CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return update(safeStore, null, Command.preaccept(current(), attrs, executeAt, ballot));
    }

    public Command.Accepted markDefined(SafeCommandStore safeStore, CommonAttributes attributes, Ballot promised)
    {
        return update(safeStore, null, Command.markDefined(current(), attributes, promised));
    }

    public Command updatePromised(Ballot promised)
    {
        return incidentalUpdate(current().updatePromised(promised));
    }

    public Command.Accepted accept(SafeCommandStore safeStore, Seekables<?, ?> keysOrRanges, CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return update(safeStore, keysOrRanges, Command.accept(current(), attrs, executeAt, ballot));
    }

    public Command.Accepted acceptInvalidated(SafeCommandStore safeStore, Ballot ballot)
    {
        return update(safeStore, null, Command.acceptInvalidated(current(), ballot));
    }

    public Command.Committed commit(SafeCommandStore safeStore, CommonAttributes attrs, Ballot ballot, Timestamp executeAt)
    {
        return update(safeStore, null, Command.commit(current(), attrs, ballot, executeAt));
    }

    public Command.Committed stable(SafeCommandStore safeStore, CommonAttributes attrs, Ballot ballot, Timestamp executeAt, Command.WaitingOn waitingOn)
    {
        return update(safeStore, null, Command.stable(current(), attrs, ballot, executeAt, waitingOn));
    }

    public Truncated commitInvalidated(SafeCommandStore safeStore)
    {
        Command current = current();
        if (current.hasBeen(Status.Truncated))
            return (Truncated) current;

        return update(safeStore, null, Truncated.invalidated(current));
    }

    public Command precommit(SafeCommandStore safeStore, CommonAttributes attrs, Timestamp executeAt)
    {
        return update(safeStore, null, Command.precommit(attrs, current(), executeAt));
    }

    public Command.Committed readyToExecute(SafeCommandStore safeStore)
    {
        return update(safeStore, null, Command.readyToExecute(current().asCommitted()));
    }

    public Command.Executed preapplied(SafeCommandStore safeStore, CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return update(safeStore, null, Command.preapplied(current(), attrs, executeAt, waitingOn, writes, result));
    }

    public Command.Executed applying(SafeCommandStore safeStore)
    {
        return update(safeStore, null, Command.applying(current().asExecuted()));
    }

    public Command.Executed applied(SafeCommandStore safeStore)
    {
        return update(safeStore, null, Command.applied(current().asExecuted()));
    }

    public Command.NotDefined uninitialised()
    {
        Invariants.checkArgument(current() == null);
        return incidentalUpdate(Command.NotDefined.uninitialised(txnId));
    }

    public Command initialise()
    {
        Command current = current();
        if (!current.saveStatus().isUninitialised())
            return current;
        return incidentalUpdate(Command.NotDefined.notDefined(current, current.promised()));
    }
}
