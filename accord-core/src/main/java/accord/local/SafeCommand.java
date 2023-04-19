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

import accord.api.Result;
import accord.primitives.Ballot;
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

    public boolean isEmpty()
    {
        return current() == null;
    }

    protected abstract void set(Command command);

    public TxnId txnId()
    {
        return txnId;
    }

    private <C extends Command> C update(C update)
    {
        set(update);
        return update;
    }

    public Command addListener(CommandListener listener)
    {
        return update(Command.addListener(current(), listener));
    }

    public Command addAndInvokeListener(SafeCommandStore safeStore, CommandListener listener)
    {
        Command command = update(Command.addListener(current(), listener));
        listener.onChange(safeStore, this);
        return command;
    }

    public Command removeListener(CommandListener listener)
    {
        return update(Command.removeListener(current(), listener));
    }

    public Command.Committed updateWaitingOn(Command.WaitingOn.Update waitingOn)
    {
        return update(Command.updateWaitingOn(current().asCommitted(), waitingOn));
    }

    public Command updateAttributes(CommonAttributes attrs)
    {
        return update(current().updateAttributes(attrs));
    }

    public Command.PreAccepted preaccept(CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return update(Command.preaccept(current(), attrs, executeAt, ballot));
    }

    public Command.Accepted markDefined(CommonAttributes attributes, Ballot promised)
    {
        return update(Command.markDefined(current(), attributes, promised));
    }

    public Command updatePromised(Ballot promised)
    {
        return update(current().updatePromised(promised));
    }

    public Command.Accepted accept(CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return update(Command.accept(current(), attrs, executeAt, ballot));
    }

    public Command.Accepted acceptInvalidated(Ballot ballot)
    {
        return update(Command.acceptInvalidated(current(), ballot));
    }

    public Command.Committed commit(CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn)
    {
        return update(Command.commit(current(), attrs, executeAt, waitingOn));
    }

    public Command precommit(Timestamp executeAt)
    {
        return update(Command.precommit(current(), executeAt));
    }

    public Command.Committed commitInvalidated(CommonAttributes attrs, Timestamp executeAt)
    {
        return update(Command.commitInvalidated(current(), attrs, executeAt));
    }

    public Command.Committed readyToExecute()
    {
        return update(Command.readyToExecute(current().asCommitted()));
    }

    public Command.Executed preapplied(CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return update(Command.preapplied(current(), attrs, executeAt, waitingOn, writes, result));
    }

    public Command.Executed applied()
    {
        return update(Command.applied(current().asExecuted()));
    }

    public Command.NotWitnessed notWitnessed()
    {
        Invariants.checkArgument(current() == null);
        return update(Command.NotWitnessed.notWitnessed(txnId));
    }
}
