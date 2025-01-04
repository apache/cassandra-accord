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
import accord.api.LocalListeners;
import accord.api.LocalListeners.Registered;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.PreLoadContext;
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
import accord.utils.MapReduceConsume;
import accord.utils.async.AsyncChain;
import accord.utils.async.Cancellable;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasStableDeps;
import static accord.messages.MessageType.READ_RSP;
import static accord.messages.ReadData.CommitOrReadNack.Insufficient;
import static accord.messages.ReadData.CommitOrReadNack.Redundant;
import static accord.messages.TxnRequest.latestRelevantEpochIndex;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

// TODO (expected): if one shard timesout waiting to reply, but another shard produces a reply, return a partial response (or response with suitably populated unavailable)
//   this means timing out a little earlier so the reply has time to arrive (but this should anyway be the case)
public abstract class ReadData implements PreLoadContext, Request, MapReduceConsume<SafeCommandStore, ReadData.CommitOrReadNack>, LocalListeners.ComplexListener, Timeouts.Timeout
{
    private static final Logger logger = LoggerFactory.getLogger(ReadData.class);

    private enum State { PENDING, PENDING_OBSOLETE, RETURNED, OBSOLETE }
    protected enum StoreAction { WAIT, EXECUTE, OBSOLETE }

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
    public final TxnId txnId;
    public final Participants<?> readScope;
    public final long executeAtEpoch;
    private transient State state = State.PENDING; // TODO (low priority, semantics): respond with the Executed result we have stored?

    transient Timestamp executeAt;
    private Data data;
    transient IntHashSet waitingOn, reading;
    transient int waitingOnCount;
    transient Ranges unavailable;
    transient Int2ObjectHashMap<LocalListeners.Registered> listeners = new Int2ObjectHashMap<>();
    transient Cancellable cancel;
    transient RegisteredTimeout timeout;

    protected transient Node node;
    protected transient Node.Id replyTo;
    protected transient ReplyContext replyContext;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> readScope, long executeAtEpoch)
    {
        this.txnId = txnId;
        int startIndex = latestRelevantEpochIndex(to, topologies, readScope);
        this.readScope = TxnRequest.computeScope(to, topologies, readScope, startIndex, Participants::slice, Participants::with);
        this.executeAtEpoch = executeAtEpoch;
    }

    protected ReadData(TxnId txnId, Participants<?> readScope, long executeAtEpoch)
    {
        this.txnId = txnId;
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

    protected long minEpoch()
    {
        return executeAtEpoch;
    }

    @Override
    public final void process(Node on, Node.Id replyTo, ReplyContext replyContext)
    {
        this.node = on;
        this.replyTo = replyTo;
        this.replyContext = replyContext;
        waitingOn = new IntHashSet();
        reading = new IntHashSet();
        Cancellable cancel = node.mapReduceConsumeLocal(this, readScope, minEpoch(), executeAtEpoch, this);
        long expiresAt = node.agent().expiresAt(replyContext, MICROSECONDS);
        RegisteredTimeout timeout = expiresAt <= 0 ? null : node.timeouts().registerWithDelay(this, expiresAt, MICROSECONDS);
        synchronized (this)
        {
            switch (state)
            {
                case PENDING:
                    this.cancel = cancel;
                    this.timeout = timeout;
                    return;
                case PENDING_OBSOLETE:
                case OBSOLETE:
                    timeout = null;
                case RETURNED:
                    cancel = null;
            }
        }
        if (cancel != null) cancel.cancel();
        if (timeout != null) timeout.cancel();
    }

    @Override
    public CommitOrReadNack reduce(CommitOrReadNack r1, CommitOrReadNack r2)
    {
        return r1 == null || r2 == null
               ? r1 == null ? r2 : r1
               : r1.compareTo(r2) >= 0 ? r1 : r2;
    }

    @Override
    public CommitOrReadNack apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.execute(safeStore, readScope, txnId, minEpoch(), executeAtEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        return apply(safeStore, safeCommand, participants);
    }

    protected CommitOrReadNack apply(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
    {
        synchronized (this)
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
                    listeners.put(storeId, safeStore.register(txnId, this));
                    waitingOn.add(storeId);
                    ++waitingOnCount;

                    int c = status.compareTo(SaveStatus.Stable);
                    if (c < 0) safeStore.progressLog().waiting(HasStableDeps, safeStore, safeCommand, null, null, participants);
                    else if (c > 0 && status.compareTo(executeOn().min) >= 0 && status.compareTo(SaveStatus.PreApplied) < 0) safeStore.progressLog().waiting(CanApply, safeStore, safeCommand, null, readScope, null);
                    return status.compareTo(SaveStatus.Stable) >= 0 ? null : Insufficient;

                case OBSOLETE:
                    state = State.PENDING_OBSOLETE;
                    return Redundant;

                case EXECUTE:
                    listeners.put(storeId, safeStore.register(txnId, this));
                    waitingOn.add(storeId);
                    ++waitingOnCount;
                    reading.add(storeId);
            }
        }

        read(safeStore, safeCommand.current());
        return null;
    }

    protected final StoreAction actionForStatus(SaveStatus status)
    {
        ExecuteOn executeOn = executeOn();
        if (status.compareTo(executeOn.min) < 0) return StoreAction.WAIT;
        if (status.compareTo(executeOn.max) > 0) return StoreAction.OBSOLETE;
        return StoreAction.EXECUTE;
    }

    @Override
    public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     this, command.txnId(), command.status(), command);

        boolean execute;
        synchronized (this)
        {
            int storeId = safeStore.commandStore().id();
            if (state != State.PENDING)
                return false;

            switch (actionForStatus(command.saveStatus()))
            {
                default: throw new AssertionError("Unhandled Action: " + actionForStatus(command.saveStatus()));
                case WAIT:
                    return true;

                case OBSOLETE:
                    execute = false;
                    break;

                case EXECUTE:
                    if (!reading.add(storeId))
                        return true;

                    if (waitingOn.add(storeId))
                        ++waitingOnCount;
                    execute = true;
            }
        }

        if (execute)
        {
            logger.trace("{}: executing read", command.txnId());
            read(safeStore, command);
            return true;
        }
        else
        {
            onFailure(Redundant, null);
            return false;
        }
    }

    @Override
    public void accept(CommitOrReadNack reply, Throwable failure)
    {
        // Unless failed always ack to indicate setup has completed otherwise the counter never gets to -1
        if ((reply == null || !reply.isFinal()) && failure == null)
        {
            onOneSuccess(-1, null, true);
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
        if (this.executeAt == null) this.executeAt = executeAt;
        else if (txnId.awaitsOnlyDeps()) this.executeAt = Timestamp.max(this.executeAt, executeAt);
        else Invariants.checkState(executeAt.equals(this.executeAt));
        return txn.read(safeStore, executeAt, unavailable);
    }

    static Ranges unavailable(SafeCommandStore safeStore, Command command)
    {
        Timestamp executeAt = command.executesAtLeast();
        if (executeAt == null) executeAt = command.executeAtOrTxnId();
        // TODO (required): for awaitsOnlyDeps commands, if we cannot infer an actual executeAtLeast we should confirm no situation where txnId is not an adequately conservative value for unavailable/unsafeToRead
        return safeStore.unsafeToReadAt(executeAt);
    }

    protected void read(SafeCommandStore safeStore, Command command)
    {
        // TODO (required): do we need to check unavailable again on completion, or throughout execution?
        //    e.g. if we are marked stale and begin processing later commands
        Ranges unavailable = unavailable(safeStore, command);
        CommandStore unsafeStore = safeStore.commandStore();
        beginRead(safeStore, command.executeAt(), command.partialTxn(), unavailable).begin((next, throwable) -> {
            if (throwable != null)
            {
                logger.trace("{}: read failed for {}", txnId, unsafeStore, throwable);
                onFailure(null, throwable);
            }
            else
            {
                readComplete(unsafeStore, next, unavailable);
            }
        });
    }

    protected void readComplete(CommandStore commandStore, @Nullable Data result, @Nullable Ranges unavailable)
    {
        Registered cancelSelf;
        Cancellable clear;
        synchronized(this)
        {
            if (state != State.PENDING)
                return;

            logger.trace("{}: read completed on {}", txnId, commandStore);
            if (result != null)
                data = data == null ? result : data.merge(result);

            int storeId = commandStore.id();
            cancelSelf = listeners.remove(storeId);
            clear = onOneSuccessInternal(storeId, unavailable, false);
        }
        cancelSelf.cancel();
        cleanup(clear);
    }

    protected void onOneSuccess(int storeId, @Nullable Ranges newUnavailable, boolean registration)
    {
        cleanup(onOneSuccessInternal(storeId, newUnavailable, registration));
    }

    protected synchronized Cancellable onOneSuccessInternal(int storeId, @Nullable Ranges newUnavailable, boolean registration)
    {
        if (storeId >= 0)
        {
            boolean removed = waitingOn.remove(storeId);
            Invariants.checkState(removed, "Txn %s's reading not contain store %d; waitingOn=%s", txnId, storeId, waitingOn);
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
        if (--waitingOnCount >= 0)
            return null;

        switch (state)
        {
            default:
                throw new AssertionError("Unknown state: " + state);

            case RETURNED:
                throw illegalState("ReadOk was sent, yet ack called again");

            case PENDING_OBSOLETE:
            case OBSOLETE:
                logger.debug("Before the read completed successfully for txn {}, the result was marked obsolete", txnId);
                return null;

            case PENDING:
                state = State.RETURNED;
                node.reply(replyTo, replyContext, constructReadOk(unavailable, data), null);
                return clearUnsafe();
        }
    }

    protected boolean cancel()
    {
        Cancellable clear;
        synchronized (this)
        {
            if (state.compareTo(State.PENDING_OBSOLETE) > 0)
                return false;

            state = State.OBSOLETE;
            clear = clearUnsafe();
        }
        cleanup(clear);
        return true;
    }

    @Nullable Cancellable clearUnsafe()
    {
        Invariants.checkState(state != State.PENDING);
        RegisteredTimeout cancelTimeout = timeout;
        Int2ObjectHashMap<LocalListeners.Registered> cancelListeners = listeners;
        timeout = null;
        listeners = null;
        waitingOn.clear();
        reading.clear();
        data = null;
        unavailable = null;

        if (cancelListeners == null && cancelTimeout == null)
            return null;

        return () -> {
            if (cancelTimeout != null)
                cancelTimeout.cancel();
            if (cancelListeners != null)
                cancelListeners.forEach((i, r) -> r.cancel());
        };
    }

    private static void cleanup(@Nullable Cancellable cancel)
    {
        if (cancel != null)
            cancel.cancel();
    }

    public void timeout()
    {
        timeout = null;
        cancel();
    }

    public int stripe()
    {
        return txnId.hashCode();
    }

    void onFailure(CommitOrReadNack failReply, Throwable throwable)
    {
        if (!cancel())
            return;

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
        if (data != null) data.validateReply(txnId, executeAt);
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
        Invalid("CommitInvalid", true),

        /**
         * The commit has been rejected due to stale ballot.
         */
        Rejected("CommitRejected", true),

        /**
         * Either not committed, or not stable
         */
        Insufficient("CommitInsufficient", false),

        /**
         * PreApplied successfully, but the request is blocking so waiting to reply
         */
        Waiting("ApplyWaiting", false),

        Redundant("CommitOrReadRedundant", true);

        final String fullname;
        final boolean isFinal;

        CommitOrReadNack(String fullname, boolean isFinal)
        {
            this.fullname = fullname;
            this.isFinal = isFinal;
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
            return isFinal;
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
