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

package accord.messages;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.local.SaveStatus.LocalExecution.WaitingToExecute;
import static accord.local.Status.Committed;
import static accord.messages.ReadData.ReadNack.NotCommitted;
import static accord.messages.ReadData.ReadNack.Redundant;
import static accord.utils.MapReduceConsume.forEach;

public abstract class AbstractExecute extends ReadData implements Command.TransientListener, EpochSupplier
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractExecute.class);

    class ObsoleteTracker implements Command.TransientListener
    {
        @Override
        public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            switch (actionForStatus(safeCommand.current().status()))
            {
                case OBSOLETE:
                    obsoleteAndSend();
                    safeCommand.removeListener(this);
            }
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return AbstractExecute.this.listenerPreLoadContext(caller);
        }
    }

    private enum State { PENDING, RETURNED, OBSOLETE }

    public final long executeAtEpoch;
    final ObsoleteTracker obsoleteTracker = new ObsoleteTracker();
    private transient State state = State.PENDING; // TODO (low priority, semantics): respond with the Executed result we have stored?

    public AbstractExecute(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, Timestamp executeAt)
    {
        super(to, topologies, txnId, readScope);
        this.executeAtEpoch = executeAt.epoch();
    }

    public AbstractExecute(TxnId txnId, Participants<?> readScope, long waitForEpoch, long executeAtEpoch)
    {
        super(txnId, readScope, waitForEpoch);
        this.executeAtEpoch = executeAtEpoch;
    }

    @Override
    public ReadType kind()
    {
        return ReadType.readTxnData;
    }

    @Override
    protected long executeAtEpoch()
    {
        return executeAtEpoch;
    }

    @Override
    public long epoch()
    {
        return executeAtEpoch;
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(txnId, caller, keys());
    }

    protected enum Action { WAIT, EXECUTE, OBSOLETE }

    protected abstract boolean canExecutePreApplied();

    /*
     * Reading data definitely requires respecting obsoletion since it invalidates the read
     * but writing data doesn't always make it necessary to fail the transaction due to preemption.
     * At worst we do some duplicate work, and ignoring obsoletion means we don't have to fail the transaction at the
     * original coordinator.
     */
    protected boolean executeIfObsoleted()
    {
        return false;
    }

    // TODO (review): Is this too liberal in allowing old things to execute?
    // would it be better to let things fail if coordinators compete?
    Action maybeObsoleteOrExecute(Action action, Status status)
    {
        if (action == Action.OBSOLETE && executeIfObsoleted())
            // Just because it isn't obsolete doesn't mean it is ready to execute
            return status.hasBeen(Status.ReadyToExecute) ? Action.EXECUTE : Action.WAIT;
        return action;
    }

    protected Action actionForStatus(Status status)
    {
        switch (status)
        {
            default: throw new AssertionError();
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Committed:
                return Action.WAIT;

            case PreApplied:
                return canExecutePreApplied() ? Action.EXECUTE : maybeObsoleteOrExecute(Action.OBSOLETE, status);

            case ReadyToExecute:
                return Action.EXECUTE;

            case Applied:
                return maybeObsoleteOrExecute(Action.OBSOLETE, status);
            case Invalidated:
            case Truncated:
                return Action.OBSOLETE;
        }
    }

    @Override
    public synchronized void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     this, command.txnId(), command.status(), command);

        switch (actionForStatus(command.status()))
        {
            default: throw new AssertionError();
            case WAIT:
                return;
            case OBSOLETE:
                obsoleteAndSend();
                return;
            case EXECUTE:
        }

        if (safeCommand.removeListener(this))
            maybeRead(safeStore, safeCommand);
    }

    @Override
    public synchronized ReadNack apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, this, readScope);
        return apply(safeStore, safeCommand);
    }

    protected synchronized ReadNack apply(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (state != State.PENDING)
            return null;

        Command command = safeCommand.current();
        Status status = command.status();

        logger.trace("{}: setting up read with status {} on {}", txnId, status, safeStore);
        switch (actionForStatus(status))
        {
            default: throw new AssertionError();
            case WAIT:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                safeCommand.addListener(this);

                safeStore.progressLog().waiting(safeCommand, WaitingToExecute, null, readScope);
                if (status == Committed) return null;
                else return NotCommitted;
            case OBSOLETE:
                state = State.OBSOLETE;
                return Redundant;
            case EXECUTE:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                maybeRead(safeStore, safeCommand);
                return null;
        }
    }

    synchronized void obsoleteAndSend()
    {
        if (state == State.PENDING)
        {
            state = State.OBSOLETE;
            node.reply(replyTo, replyContext, Redundant, null);
        }
    }

    void maybeRead(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        switch (state)
        {
            case PENDING:
                Command command = safeCommand.current();
                logger.trace("{}: executing read", command.txnId());
                safeCommand.addListener(obsoleteTracker);
                read(safeStore, command.executeAt(), command.partialTxn());
                break;
            case OBSOLETE:
                // nothing to see here
                break;
            case RETURNED:
                throw new IllegalStateException("ReadOk was sent, yet ack called again");
            default:
                throw new AssertionError("Unknown state: " + state);
        }
    }

    @Override
    protected synchronized void readComplete(CommandStore commandStore, @Nullable Data result, @Nullable Ranges unavailable)
    {
        // TODO (expected): lots of undesirable costs associated with the obsoletion tracker
//        commandStore.execute(contextFor(txnId), safeStore -> safeStore.command(txnId).removeListener(obsoleteTracker));
        super.readComplete(commandStore, result, unavailable);
    }

    @Override
    protected void reply(@Nullable Ranges unavailable, @Nullable Data data, @Nullable Throwable fail)
    {
        switch (state)
        {
            case RETURNED:
                throw new IllegalStateException("ReadOk was sent, yet ack called again");
            case OBSOLETE:
                logger.debug("After the read completed for txn {}, the result was marked obsolete", txnId);
                break;
            case PENDING:
                state = State.RETURNED;
                node.reply(replyTo, replyContext, fail == null ? constructReadOk(unavailable, data) : null, fail);
                break;
            default:
                throw new AssertionError("Unknown state: " + state);
        }
    }

    protected ReadOk constructReadOk(Ranges unavailable, Data data)
    {
        return new ReadOk(unavailable, data);
    }

    private void removeListener(SafeCommandStore safeStore, TxnId txnId)
    {
        SafeCommand safeCommand = safeStore.ifInitialised(txnId);
        safeCommand.removeListener(this);
    }

    @Override
    protected void cancel()
    {
        node.commandStores().mapReduceConsume(this, waitingOn.stream(), forEach(in -> removeListener(in, txnId), node.agent()));
    }

    @Override
    public String toString()
    {
        return "ReadTxnData{" +
               "txnId:" + txnId +
               '}';
    }
}
