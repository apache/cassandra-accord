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
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus.LocalExecution;
import accord.local.Status;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.local.SaveStatus.LocalExecution.WaitingToExecute;
import static accord.local.Status.Committed;
import static accord.messages.ReadData.ReadNack.Invalid;
import static accord.messages.ReadData.ReadNack.NotCommitted;
import static accord.messages.ReadData.ReadNack.Redundant;
import static accord.utils.MapReduceConsume.forEach;

// TODO (required, efficiency): dedup - can currently have infinite pending reads that will be executed independently
public class WaitUntilApplied extends ReadData implements Command.TransientListener, EpochSupplier
{
    private static final Logger logger = LoggerFactory.getLogger(WaitUntilApplied.class);

    public static class SerializerSupport
    {
        public static WaitUntilApplied create(TxnId txnId, Participants<?> scope, Timestamp executeAt, long waitForEpoch)
        {
            return new WaitUntilApplied(txnId, scope, executeAt, waitForEpoch);
        }
    }

    public final Timestamp executeAt;
    private boolean isInvalid;

    public WaitUntilApplied(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, Timestamp executeAt)
    {
        super(to, topologies, txnId, readScope);
        this.executeAt = executeAt;
    }

    protected WaitUntilApplied(TxnId txnId, Participants<?> readScope, Timestamp executeAt, long waitForEpoch)
    {
        super(txnId, readScope, waitForEpoch);
        this.executeAt = executeAt;
    }

    @Override
    protected long executeAtEpoch()
    {
        return executeAt.epoch();
    }

    @Override
    public long epoch()
    {
        return executeAt.epoch();
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(txnId, caller);
    }

    @Override
    public synchronized void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                this, command.txnId(), command.status(), command);
        switch (command.status())
        {
            default: throw new AssertionError("Unknown status: " + command.status());
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Committed:
            case ReadyToExecute:
            case PreApplied:
                return;

            case Invalidated:
                sendInvalidated();
                return;

            case Truncated:
                // implies durable and applied
                sendTruncated();
                return;

            case Applied:
        }

        if (safeCommand.removeListener(this))
            maybeApplied(safeStore, safeCommand);
    }

    @Override
    protected void process()
    {
        super.process();
    }

    @Override
    public synchronized ReadNack apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, this, readScope);
        return apply(safeStore, safeCommand);
    }

    private ReadNack apply(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (isInvalid)
            return null;

        Command command = safeCommand.current();
        Status status = command.status();

        logger.trace("{}: setting up read with status {} on {}", txnId, status, safeStore);
        switch (status) {
            default:
                throw new AssertionError("Unknown status: " + status);
            case Committed:
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case ReadyToExecute:
            case PreApplied:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                safeCommand.addListener(this);

                if (status.compareTo(Committed) >= 0)
                {
                    safeStore.progressLog().waiting(safeCommand, LocalExecution.Applied, null, readScope);
                    return null;
                }
                else
                {
                    safeStore.progressLog().waiting(safeCommand, WaitingToExecute, null, readScope);
                    return NotCommitted;
                }

            case Invalidated:
                isInvalid = true;
                return Invalid;

            case Applied:
            case Truncated:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                maybeApplied(safeStore, safeCommand);
                return null;
        }
    }

    synchronized void sendInvalidated()
    {
        if (isInvalid)
            return;

        isInvalid = true;
        node.reply(replyTo, replyContext, Invalid);
    }

    synchronized void sendTruncated()
    {
        if (isInvalid)
            return;

        isInvalid = true;
        node.reply(replyTo, replyContext, Redundant);
    }

    void maybeApplied(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (isInvalid)
            return;

        Ranges unavailable = safeStore.ranges().unsafeToReadAt(executeAt);
        readComplete(safeStore.commandStore(), null, unavailable);
    }

    @Override
    protected void reply(@Nullable Ranges unavailable, @Nullable Data data)
    {
        if (isInvalid)
            return;

        node.reply(replyTo, replyContext, new ReadOk(unavailable, data));
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
        return MessageType.WAIT_ON_APPLY_REQ;
    }

    @Override
    public String toString()
    {
        return "WaitForApply{" +
               "txnId:" + txnId +
               '}';
    }
}
