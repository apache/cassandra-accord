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

// TODO (required, efficiency): dedup - can currently have infinite pending reads that will be executed independently
public class ReadTxnData extends ReadData implements Command.TransientListener, EpochSupplier
{
    private static final Logger logger = LoggerFactory.getLogger(ReadTxnData.class);

    public static class SerializerSupport
    {
        public static ReadTxnData create(TxnId txnId, Participants<?> scope, long executeAtEpoch, long waitForEpoch)
        {
            return new ReadTxnData(txnId, scope, executeAtEpoch, waitForEpoch);
        }
    }

    class ObsoleteTracker implements Command.TransientListener
    {
        @Override
        public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            switch (safeCommand.current().status())
            {
                case PreApplied:
                case Applied:
                case Invalidated:
                case Truncated:
                    obsoleteAndSend();
                    safeCommand.removeListener(this);
            }
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return ReadTxnData.this.listenerPreLoadContext(caller);
        }
    }

    private enum State { PENDING, RETURNED, OBSOLETE }

    public final long executeAtEpoch;
    final ObsoleteTracker obsoleteTracker = new ObsoleteTracker();
    private transient State state = State.PENDING; // TODO (low priority, semantics): respond with the Executed result we have stored?

    public ReadTxnData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, Timestamp executeAt)
    {
        super(to, topologies, txnId, readScope);
        this.executeAtEpoch = executeAt.epoch();
    }

    protected ReadTxnData(TxnId txnId, Participants<?> readScope, long executeAtEpoch, long waitForEpoch)
    {
        super(txnId, readScope, waitForEpoch);
        this.executeAtEpoch = executeAtEpoch;
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

    @Override
    public synchronized void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                this, command.txnId(), command.status(), command);
        switch (command.status())
        {
            default: throw new AssertionError();
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Committed:
                return;

            case PreApplied:
            case Applied:
            case Invalidated:
            case Truncated:
                obsoleteAndSend();
                return;
            case ReadyToExecute:
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
        switch (status) {
            default:
                throw new AssertionError();
            case Committed:
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                safeCommand.addListener(this);

                safeStore.progressLog().waiting(safeCommand, WaitingToExecute, null, readScope);
                if (status == Committed) return null;
                else return NotCommitted;

            case PreApplied:
            case Applied:
            case Invalidated:
            case Truncated:
                state = State.OBSOLETE;
                return Redundant;

            case ReadyToExecute:
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
            node.reply(replyTo, replyContext, Redundant);
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
        // TODO (expected): we should unregister our listener, but this is quite costly today
        super.readComplete(commandStore, result, unavailable);
    }

    @Override
    protected void reply(@Nullable Ranges unavailable, @Nullable Data data)
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
                node.reply(replyTo, replyContext, new ReadOk(unavailable, data));
                break;
            default:
                throw new AssertionError("Unknown state: " + state);
        }
    }

    private void removeListener(SafeCommandStore safeStore, TxnId txnId)
    {
        safeStore.get(txnId, this, readScope).removeListener(this);
    }

    @Override
    protected void cancel()
    {
        node.commandStores().mapReduceConsume(this, waitingOn.stream(), forEach(in -> removeListener(in, txnId), node.agent()));
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               '}';
    }
}
