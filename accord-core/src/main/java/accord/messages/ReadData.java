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
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.LocalListeners;
import accord.api.RequestTimeouts;
import accord.api.RequestTimeouts.RegisteredTimeout;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasStableDeps;
import static accord.messages.MessageType.READ_RSP;
import static accord.messages.ReadData.CommitOrReadNack.Insufficient;
import static accord.messages.ReadData.CommitOrReadNack.Redundant;
import static accord.messages.TxnRequest.latestRelevantEpochIndex;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class ReadData extends AbstractEpochRequest<ReadData.CommitOrReadNack> implements LocalListeners.ComplexListener, RequestTimeouts.Timeout
{
    private static final Logger logger = LoggerFactory.getLogger(ReadData.class);

    private enum State { PENDING, RETURNED, OBSOLETE }
    protected enum Action { WAIT, EXECUTE, OBSOLETE }

    public enum ReadType
    {
        readTxnData(0),
        readDataWithoutTimestamp(1),
        waitUntilApplied(2),
        applyThenWaitUntilApplied(3);

        public final byte val;

        ReadType(int val)
        {
            this.val = (byte) val;
        }

        public static ReadType valueOf(int val)
        {
            switch (val)
            {
                case 0:
                    return readTxnData;
                case 1:
                    return readDataWithoutTimestamp;
                case 2:
                    return waitUntilApplied;
                case 3:
                    return applyThenWaitUntilApplied;
                default:
                    throw new IllegalArgumentException("Unrecognized ReadType value " + val);
            }
        }
    }

    protected static class ExecuteOn
    {
        final SaveStatus min, max;

        public ExecuteOn(SaveStatus min, SaveStatus max)
        {
            this.min = min;
            this.max = max;
        }
    }

    // TODO (desired, cleanup): should this be a Route?
    public final Participants<?> readScope;
    public final long executeAtEpoch;
    private transient State state = State.PENDING; // TODO (low priority, semantics): respond with the Executed result we have stored?

    private Data data;
    transient BitSet waitingOn, reading;
    transient int waitingOnCount;
    transient Ranges unavailable;
    Int2ObjectHashMap<LocalListeners.Registered> registrations = new Int2ObjectHashMap<>();
    RegisteredTimeout timeout;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, long executeAtEpoch)
    {
        super(txnId);
        int startIndex = latestRelevantEpochIndex(to, topologies, readScope);
        this.readScope = TxnRequest.computeScope(to, topologies, readScope, startIndex, Participants::slice, Participants::with);
        this.executeAtEpoch = executeAtEpoch;
    }

    protected ReadData(TxnId txnId, Participants<?> readScope, long executeAtEpoch)
    {
        super(txnId);
        this.readScope = readScope;
        this.executeAtEpoch = executeAtEpoch;
    }

    protected abstract ExecuteOn executeOn();
    abstract public ReadType kind();

    @Override
    public long waitForEpoch()
    {
        return executeAtEpoch;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    protected void process()
    {
        waitingOn = new BitSet();
        reading = new BitSet();
        node.mapReduceConsumeLocal(this, readScope, executeAtEpoch, executeAtEpoch, this);
    }

    @Override
    public CommitOrReadNack reduce(CommitOrReadNack r1, CommitOrReadNack r2)
    {
        return r1 == null || r2 == null
               ? r1 == null ? r2 : r1
               : r1.compareTo(r2) >= 0 ? r1 : r2;
    }


    @Override
    public synchronized CommitOrReadNack apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.execute(safeStore, readScope, txnId, executeAtEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        return apply(safeStore, safeCommand, participants);
    }

    protected synchronized CommitOrReadNack apply(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
    {
        if (state != State.PENDING)
            return null;

        Command command = safeCommand.current();
        SaveStatus status = command.saveStatus();
        int storeId = safeStore.commandStore().id();

        logger.trace("{}: setting up read with status {} on {}", txnId, status, safeStore);
        switch (actionForStatus(status))
        {
            default: throw new AssertionError();
            case WAIT:
                registrations.put(storeId, safeStore.register(txnId, this));
                waitingOn.set(storeId);
                ++waitingOnCount;

                int c = status.compareTo(SaveStatus.Stable);
                if (c < 0) safeStore.progressLog().waiting(HasStableDeps, safeStore, safeCommand, participants.route(), participants.owns());
                else if (c > 0 && status.compareTo(executeOn().min) >= 0 && status.compareTo(SaveStatus.PreApplied) < 0) safeStore.progressLog().waiting(CanApply, safeStore, safeCommand, null, readScope);
                return status.compareTo(SaveStatus.Stable) >= 0 ? null : Insufficient;

            case OBSOLETE:
                state = State.OBSOLETE;
                return Redundant;

            case EXECUTE:
                registrations.put(storeId, safeStore.register(txnId, this));
                waitingOn.set(storeId);
                ++waitingOnCount;
                reading.set(storeId);
                read(safeStore, safeCommand.current());
                return null;
        }
    }

    protected final Action actionForStatus(SaveStatus status)
    {
        ExecuteOn executeOn = executeOn();
        if (status.compareTo(executeOn.min) < 0) return Action.WAIT;
        if (status.compareTo(executeOn.max) > 0) return Action.OBSOLETE;
        return Action.EXECUTE;
    }

    @Override
    public synchronized boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     this, command.txnId(), command.status(), command);

        int storeId = safeStore.commandStore().id();
        if (state != State.PENDING)
            return false;

        switch (actionForStatus(command.saveStatus()))
        {
            default: throw new AssertionError("Unhandled Action: " + actionForStatus(command.saveStatus()));
            case WAIT:
                return true;

            case OBSOLETE:
                onFailure(Redundant, null);
                return false;

            case EXECUTE:
                if (!reading.get(storeId))
                {
                    if (!waitingOn.get(storeId))
                    {
                        waitingOn.set(storeId);
                        ++waitingOnCount;
                    }
                    reading.set(storeId);
                    logger.trace("{}: executing read", command.txnId());
                    read(safeStore, command);
                }
                return true;
        }
    }

    @Override
    public synchronized void accept(CommitOrReadNack reply, Throwable failure)
    {
        // Unless failed always ack to indicate setup has completed otherwise the counter never gets to -1
        if ((reply == null || !reply.isFinal()) && failure == null)
        {
            onOneSuccess(-1, null);
            if (state == State.PENDING)
            {
                // Time out reads to avoid doing extra work for requests that will have responses ignored
                long replyTimeout = node.agent().replyTimeout(replyContext, MILLISECONDS);
                timeout = node.requestTimeouts().register(this, replyTimeout, MILLISECONDS);
            }
            if (reply != null)
                node.reply(replyTo, replyContext, reply, null);
        }
        else
        {
            onFailure(reply, failure);
        }
    }

    protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Ranges unavailable)
    {
        return txn.read(safeStore, executeAt, unavailable);
    }

    static Ranges unavailable(SafeCommandStore safeStore, Command command)
    {
        Timestamp executeAt = command.executesAtLeast();
        // TODO (required): for awaitsOnlyDeps commands, if we cannot infer an actual executeAtLeast we should confirm no situation where txnId is not an adequately conservative value for unavailable/unsafeToRead
        return safeStore.ranges().unsafeToReadAt(executeAt);
    }

    void read(SafeCommandStore safeStore, Command command)
    {
        // TODO (required): do we need to check unavailable again on completion, or throughout execution?
        //    e.g. if we are marked stale and begin processing later commands
        Ranges unavailable = unavailable(safeStore, command);
        CommandStore unsafeStore = safeStore.commandStore();
        beginRead(safeStore, command.executeAt(), command.partialTxn(), unavailable).begin((next, throwable) -> {
            if (throwable != null)
            {
                logger.trace("{}: read failed for {}: {}", txnId, unsafeStore, throwable);
                onFailure(null, throwable);
            }
            else
            {
                readComplete(unsafeStore, next, unavailable);
            }
        });
    }

    protected synchronized void readComplete(CommandStore commandStore, @Nullable Data result, @Nullable Ranges unavailable)
    {
        if (state == State.OBSOLETE)
            return;

        logger.trace("{}: read completed on {}", txnId, commandStore);
        if (result != null)
            data = data == null ? result : data.merge(result);

        int storeId = commandStore.id();
        registrations.remove(storeId).cancel();
        onOneSuccess(storeId, unavailable);
    }

    protected void onOneSuccess(int storeId, @Nullable Ranges newUnavailable)
    {
        if (storeId >= 0)
        {
            Invariants.checkState(waitingOn.get(storeId), "Txn %s's reading not contain store %d; waitingOn=%s", txnId, storeId, waitingOn);
            waitingOn.clear(storeId);
        }

        if (newUnavailable != null && !newUnavailable.isEmpty())
        {
            newUnavailable = newUnavailable.intersecting(readScope, Minimal);
            if (unavailable == null) unavailable = newUnavailable;
            else unavailable = newUnavailable.with(unavailable);
        }

        // wait for -1 to ensure the setup phase has also completed. Setup calls ack in its callback
        // and prevents races where we respond before dispatching all the required reads (if the reads are
        // completing faster than the reads can be setup on all required shards)
        if (-1 == --waitingOnCount)
            onAllSuccess(this.unavailable, data, null);
    }

    protected void onAllSuccess(@Nullable Ranges unavailable, @Nullable Data data, @Nullable Throwable fail)
    {
        switch (state)
        {
            case RETURNED:
                throw illegalState("ReadOk was sent, yet ack called again");

            case OBSOLETE:
                logger.debug("After the read completed for txn {}, the result was marked obsolete", txnId);
                if (fail != null)
                    node.agent().onUncaughtException(fail);
                break;

            case PENDING:
                state = State.RETURNED;
                node.reply(replyTo, replyContext, fail == null ? constructReadOk(unavailable, data) : null, fail);
                clear();
                break;

            default:
                throw new AssertionError("Unknown state: " + state);
        }
    }

    void cancel()
    {
        state = State.OBSOLETE;
        clear();
    }

    void clear()
    {
        if (timeout != null)
            timeout.cancel();
        registrations.forEach((i, r) -> r.cancel());
        waitingOn.clear();
        reading.clear();
        data = null;
        unavailable = null;
        timeout = null;
    }

    synchronized public void timeout()
    {
        timeout = null;
        cancel();
    }

    public int stripe()
    {
        return txnId.hashCode();
    }

    synchronized void onFailure(CommitOrReadNack failReply, Throwable throwable)
    {
        cancel();
        if (throwable != null)
        {
            node.reply(replyTo, replyContext, null, throwable);
            node.agent().onUncaughtException(throwable);
        }
        else
        {
            node.reply(replyTo, replyContext, failReply, null);
        }
    }

    protected ReadOk constructReadOk(Ranges unavailable, Data data)
    {
        return new ReadOk(unavailable, data);
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               '}';
    }

    public interface ReadReply extends Reply
    {
        boolean isOk();
    }

    public enum CommitOrReadNack implements ReadData.ReadReply
    {
        /**
         * The read is for a point in the past
         */
        Invalid("CommitInvalid"),

        /**
         * The commit has been rejected due to stale ballot.
         */
        Rejected("CommitRejected"),
        /**
         * Either not committed, or not stable
         */
        Insufficient("CommitInsufficient"),
        Redundant("CommitOrReadRedundant");

        final String fullname;

        CommitOrReadNack(String fullname)
        {
            this.fullname = fullname;
        }

        @Override
        public String toString()
        {
            return fullname;
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
            return this != Insufficient;
        }
    }

    public static class ReadOk implements ReadData.ReadReply
    {
        /**
         * if the replica we contacted was unable to fully answer the query, due to bootstrapping some portion,
         * this is set to the ranges that were unavailable
         *
         * TODO (required): narrow to only the *intersecting* ranges that are unavailable, or do so on the recipient
         */
        public final @Nullable Ranges unavailable;

        public final @Nullable Data data;

        public ReadOk(@Nullable Ranges unavailable, @Nullable Data data)
        {
            this.unavailable = unavailable;
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data
                   + (unavailable == null ? "" : ", unavailable:" + unavailable) + '}';
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

    public static class ReadOkWithFutureEpoch extends ReadOk
    {
        public final long futureEpoch;
        public ReadOkWithFutureEpoch(@Nullable Ranges unavailable, @Nullable Data data, long futureEpoch)
        {
            super(unavailable, data);
            this.futureEpoch = futureEpoch;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + (unavailable == null ? "" : ", unavailable:" + unavailable) + ", futureEpoch=" + futureEpoch + '}';
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
}
