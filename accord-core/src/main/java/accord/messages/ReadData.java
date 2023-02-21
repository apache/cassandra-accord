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

import accord.primitives.*;

import accord.local.*;
import accord.api.Data;
import accord.topology.Topologies;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static accord.local.Status.Committed;
import static accord.messages.MessageType.READ_RSP;
import static accord.messages.ReadData.ReadNack.NotCommitted;
import static accord.messages.ReadData.ReadNack.Redundant;
import static accord.messages.TxnRequest.*;
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
    public synchronized void onChange(SafeCommandStore safeStore, Command command)
    {
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

        command.removeListener(this);
        if (!isObsolete)
            read(safeStore, command);
    }

    @Override
    public synchronized ReadNack apply(SafeCommandStore safeStore)
    {
        Command command = safeStore.command(txnId);
        Status status = command.status();
        logger.trace("{}: setting up read with status {} on {}", txnId, status, safeStore);
        switch (command.status()) {
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
                command.addListener(this);

                if (status == Committed)
                    return null;

                safeStore.progressLog().waiting(txnId, Committed.minKnown, readScope.toUnseekables());
                return NotCommitted;

            case ReadyToExecute:
                waitingOn.set(safeStore.commandStore().id());
                ++waitingOnCount;
                if (!isObsolete)
                    read(safeStore, command);
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
            node.commandStores().mapReduceConsume(this, waitingOn.stream(), forEach(in -> in.command(txnId).removeListener(this), node.agent()));
        }
        else
        {
            ack();
        }
    }

    private void ack()
    {
        if (-1 == --waitingOnCount)
            node.reply(replyTo, replyContext, new ReadOk(data));
    }

    private synchronized void readComplete(CommandStore commandStore, Data result)
    {
        Preconditions.checkState(waitingOn.get(commandStore.id()));
        logger.trace("{}: read completed on {}", txnId, commandStore);
        if (result != null)
            data = data == null ? result : data.merge(result);

        waitingOn.clear(commandStore.id());
        ack();
    }

    private void read(SafeCommandStore safeStore, Command command)
    {
        logger.trace("{}: executing read", command.txnId());
        CommandStore unsafeStore = safeStore.commandStore();
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
