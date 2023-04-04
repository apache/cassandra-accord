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

import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.local.Command;
import accord.local.CommandListener;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.primitives.Keys;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import static accord.local.Status.Committed;
import static accord.messages.MessageType.READ_RSP;
import static accord.messages.ReadData.ReadNack.NotCommitted;
import static accord.messages.ReadData.ReadNack.Redundant;
import static accord.messages.TxnRequest.computeScope;
import static accord.messages.TxnRequest.computeWaitForEpoch;
import static accord.messages.TxnRequest.latestRelevantEpochIndex;
import static accord.utils.MapReduceConsume.forEach;

// TODO (required, efficiency): dedup - can currently have infinite pending reads that will be executed independently
public class ReadData extends AbstractEpochRequest<ReadData.ReadNack> implements CommandListener
{
    private static final Logger logger = LoggerFactory.getLogger(ReadData.class);

    public static class SerializerSupport
    {
        public static ReadData create(TxnId txnId, Seekables<?, ?> scope, long executeAtEpoch, long waitForEpoch)
        {
            return new ReadData(txnId, scope, executeAtEpoch, waitForEpoch);
        }
    }

    public final long executeAtEpoch;
    public final Seekables<?, ?> readScope; // TODO (low priority, efficiency): this should be RoutingKeys, as we have the Keys locally, but for simplicity we use this to implement keys()
    private final long waitForEpoch;
    private Data data;
    private transient boolean isObsolete; // TODO (low priority, semantics): respond with the Executed result we have stored?
    private transient BitSet waitingOn;
    private transient int waitingOnCount;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Seekables<?, ?> readScope, Timestamp executeAt)
    {
        super(txnId);
        this.executeAtEpoch = executeAt.epoch();
        int startIndex = latestRelevantEpochIndex(to, topologies, readScope);
        this.readScope = computeScope(to, topologies, (Seekables)readScope, startIndex, Seekables::slice, Seekables::with);
        this.waitForEpoch = computeWaitForEpoch(to, topologies, startIndex);
    }

    ReadData(TxnId txnId, Seekables<?, ?> readScope, long executeAtEpoch, long waitForEpoch)
    {
        super(txnId);
        this.executeAtEpoch = executeAtEpoch;
        this.readScope = readScope;
        this.waitForEpoch = waitForEpoch;
    }

    @Override
    public long waitForEpoch()
    {
        return waitForEpoch;
    }

    @Override
    protected void process()
    {
        waitingOn = new BitSet();
        node.mapReduceConsumeLocal(this, readScope, executeAtEpoch, executeAtEpoch, this);
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        Set<TxnId> ids = new HashSet<>();
        ids.add(txnId);
        ids.add(caller);
        return PreLoadContext.contextFor(ids, keys());
    }

    @Override
    public boolean isTransient()
    {
        return true;
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
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
            case Committed:
                return;

            case PreApplied:
            case Applied:
            case Invalidated:
                obsolete();
            case ReadyToExecute:
        }

        command = safeCommand.removeListener(this);

        if (!isObsolete)
            read(safeStore, command.asCommitted());
    }

    @Override
    public synchronized ReadNack apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.command(txnId);
        Status status = safeCommand.current().status();
        logger.trace("{}: setting up read with status {} on {}", txnId, status, safeStore);
        switch (status) {
            default:
                throw new AssertionError();
            case Committed:
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
            case PreCommitted:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;

                safeCommand.addListener(this);

                if (status == Committed)
                    return null;

                safeStore.progressLog().waiting(txnId, Committed.minKnown, readScope.toUnseekables());
                return NotCommitted;

            case ReadyToExecute:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                if (!isObsolete)
                    read(safeStore, safeCommand.current().asCommitted());
                return null;

            case PreApplied:
            case Applied:
            case Invalidated:
                isObsolete = true;
                return Redundant;
        }
    }

    @Override
    public ReadNack reduce(ReadNack r1, ReadNack r2)
    {
        return r1 == null || r2 == null
                ? r1 == null ? r2 : r1
                : r1.compareTo(r2) >= 0 ? r1 : r2;
    }

    private void removeListener(SafeCommandStore safeStore, TxnId txnId)
    {
        safeStore.command(txnId).removeListener(this);
    }

    @Override
    public synchronized void accept(ReadNack reply, Throwable failure)
    {
        if (reply != null)
        {
            node.reply(replyTo, replyContext, reply);
        }
        else if (failure != null)
        {
            // TODO (expected, testing): test
            node.reply(replyTo, replyContext, ReadNack.Error);
            data = null;
            // TODO (expected, exceptions): probably a better way to handle this, as might not be uncaught
            node.agent().onUncaughtException(failure);
            node.commandStores().mapReduceConsume(this, waitingOn.stream(), forEach(in -> removeListener(in, txnId), node.agent()));
        }
        else
        {
            ack();
        }
    }

    private void ack()
    {
        // wait for -1 to ensure the setup phase has also completed. Setup calls ack in its callback
        // and prevents races where we respond before dispatching all the required reads (if the reads are
        // completing faster than the reads can be setup on all required shards)
        if (-1 == --waitingOnCount)
        {
            // It is possible that the status changed between the read call and the result, double check that the status
            // has not changed and is safe to return the read
            node.mapReduceConsumeLocal(this, readScope, executeAtEpoch, executeAtEpoch, new MapReduceConsume<SafeCommandStore, Status>()
            {
                @Override
                public void accept(Status status, Throwable failure)
                {
                    if (failure != null)
                    {
                        ReadData.this.accept(null, failure);
                    }
                    else
                    {
                        switch (status)
                        {
                            case PreApplied:
                            case Applied:
                            case Invalidated:
                                logger.debug("After the read completed for txn {}, the status was changed to {}; marking the read as Redundant", txnId, status);
                                isObsolete = true;
                                node.reply(replyTo, replyContext, Redundant);
                                break;
                            case ReadyToExecute:
                                node.reply(replyTo, replyContext, new ReadOk(data));
                                break;
                            default:
                                throw new IllegalStateException("Unexpected status detected: " + status);
                        }
                    }
                }

                @Override
                public Status apply(SafeCommandStore safeStore)
                {
                    return safeStore.command(txnId).current().status();
                }

                @Override
                public Status reduce(Status o1, Status o2)
                {
                    int rc = o1.phase.compareTo(o2.phase);
                    if (rc > 0) return o1;
                    if (rc < 0) return o2;
                    rc = o1.compareTo(o2);
                    if (rc > 0) return o1;
                    if (rc < 0) return o2;
                    return o1;
                }
            });
        }
    }

    private synchronized void readComplete(CommandStore commandStore, Data result)
    {
        Invariants.checkState(waitingOn.get(commandStore.id()), "Waiting on does not contain store %d; waitingOn=%s", commandStore.id(), waitingOn);
        logger.trace("{}: read completed on {}", txnId, commandStore);
        if (result != null)
            data = data == null ? result : data.merge(result);

        waitingOn.clear(commandStore.id());
        ack();
    }

    private void read(SafeCommandStore safeStore, Command.Committed command)
    {
        CommandStore unsafeStore = safeStore.commandStore();
        logger.trace("{}: executing read", command.txnId());
        command.read(safeStore).begin((next, throwable) -> {
            if (throwable != null)
            {
                // TODO (expected, exceptions): should send exception to client, and consistency handle/propagate locally
                logger.trace("{}: read failed for {}: {}", txnId, unsafeStore, throwable);
                node.reply(replyTo, replyContext, ReadNack.Error);
            }
            else
                readComplete(unsafeStore, next);
        });
    }

    void obsolete()
    {
        if (!isObsolete)
        {
            isObsolete = true;
            node.reply(replyTo, replyContext, Redundant);
        }
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId);
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return Keys.EMPTY;
    }

    @Override
    public MessageType type()
    {
        return MessageType.READ_REQ;
    }

    public interface ReadReply extends Reply
    {
        boolean isOk();
    }

    public enum ReadNack implements ReadReply
    {
        Invalid, NotCommitted, Redundant, Error;

        @Override
        public String toString()
        {
            return "Read" + name();
        }

        @Override
        public MessageType type()
        {
            return READ_RSP;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public boolean isFinal()
        {
            return this != NotCommitted;
        }
    }

    public static class ReadOk implements ReadReply
    {
        public final @Nullable Data data;

        public ReadOk(@Nullable Data data)
        {
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + '}';
        }

        @Override
        public MessageType type()
        {
            return READ_RSP;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               '}';
    }
}
