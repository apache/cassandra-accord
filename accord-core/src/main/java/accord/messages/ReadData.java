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

import java.util.NavigableMap;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.LocalListeners;
import accord.api.LocalListeners.Registered;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Command;
import accord.local.Command.Committed;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.Cancellable;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasStableDeps;
import static accord.api.ProtocolModifiers.Toggles.dataStoreDetectsFutureReads;
import static accord.api.ProtocolModifiers.Toggles.fastReadsMayBypassCommandsForKey;
import static accord.api.ProtocolModifiers.Toggles.fastReadsMayBypassSafeStore;
import static accord.coordinate.ExecuteFlag.HAS_UNIQUE_HLC;
import static accord.coordinate.ExecuteFlag.READY_TO_EXECUTE;
import static accord.messages.MessageType.StandardMessage.READ_RSP;
import static accord.messages.ReadData.CommitOrReadNack.Insufficient;
import static accord.messages.ReadData.CommitOrReadNack.Redundant;
import static accord.messages.TxnRequest.latestRelevantEpochIndex;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.nonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

// TODO (required): (v1.1) if one shard times out waiting to reply, but another shard produces a reply, return a partial response (or response with suitably populated unavailable)
//   this means timing out a little earlier so the reply has time to arrive (but this should anyway be the case).
//   This ensures a multi-key transaction interacting with replicas that have different stale shards on a replica can make progress
public abstract class ReadData implements PreLoadContext, Request, MapReduceConsume<SafeCommandStore, ReadData.CommitOrReadNack>, LocalListeners.ComplexListener, Timeouts.Timeout
{
    private static final Logger logger = LoggerFactory.getLogger(ReadData.class);

    public enum Flag { FAST_READ }
    private enum State { PENDING, PENDING_OBSOLETE, RETURNED, OBSOLETE }
    protected enum StoreAction { WAIT, EXECUTE, OBSOLETE }

    public enum ReadType
    {
        readTxnData(0),
        readEphemeral(1),
        waitUntilApplied(2),
        applyThenWaitUntilApplied(3),
        stableThenRead(4);

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
                    return readEphemeral;
                case 2:
                    return waitUntilApplied;
                case 3:
                    return applyThenWaitUntilApplied;
                case 4:
                    return stableThenRead;
                default:
                    throw new IllegalArgumentException("Unrecognized ReadType value " + val);
            }
        }
    }

    public static class ExecuteOn
    {
        final SaveStatus min, max;

        public ExecuteOn(SaveStatus min, SaveStatus max)
        {
            this.min = min;
            this.max = max;
        }
    }

    public final TxnId txnId;
    public final Participants<?> scope;
    public final long executeAtEpoch;
    public final ExecuteFlags flags;
    final boolean requiresListenersDuringExecution;
    protected @Nullable PartialTxn partialTxn;
    protected @Nullable Timestamp executeAt;

    private transient State state = State.PENDING; // TODO (low priority, semantics): respond with the Executed result we have stored?

    private Data data;
    private long uniqueHlc;
    private long stamp;
    transient IntHashSet waitingOn, reading;
    transient int waitingOnCount;
    transient Ranges unavailable;
    transient Int2ObjectHashMap<LocalListeners.Registered> listeners = new Int2ObjectHashMap<>();
    transient Cancellable cancel;
    transient RegisteredTimeout timeout;

    protected transient Node node;
    protected transient Node.Id replyTo;
    protected transient ReplyContext replyContext;

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> scope, @Nullable Txn txn, @Nullable Timestamp executeAt, long executeAtEpoch)
    {
        this(to, topologies, txnId, scope, txn, executeAt, executeAtEpoch, ExecuteFlags.none());
    }

    public ReadData(Node.Id to, Topologies topologies, TxnId txnId, Participants<?> scope, @Nullable Txn txn, @Nullable Timestamp executeAt, long executeAtEpoch, ExecuteFlags flags)
    {
        this.txnId = txnId;
        this.flags = flags;
        int startIndex = latestRelevantEpochIndex(to, topologies, scope);
        this.scope = TxnRequest.computeScope(to, topologies, scope, startIndex, Participants::slice, Participants::with);
        this.partialTxn = txn == null ? null : txn.intersecting(scope, true);
        this.executeAt = executeAt;
        this.executeAtEpoch = executeAtEpoch;
        this.requiresListenersDuringExecution = requiresListenersDuringExecution(txnId, flags);
    }

    protected ReadData(TxnId txnId, Participants<?> scope, @Nullable PartialTxn partialTxn, @Nullable Timestamp executeAt, long executeAtEpoch)
    {
        this(txnId, scope, partialTxn, executeAt, executeAtEpoch, ExecuteFlags.none());
    }

    protected ReadData(TxnId txnId, Participants<?> scope, @Nullable PartialTxn partialTxn, @Nullable Timestamp executeAt, long executeAtEpoch, ExecuteFlags flags)
    {
        this.txnId = txnId;
        this.scope = scope;
        this.partialTxn = partialTxn;
        this.executeAt = executeAt;
        this.executeAtEpoch = executeAtEpoch;
        this.flags = flags;
        this.requiresListenersDuringExecution = requiresListenersDuringExecution(txnId, flags);
    }

    private static boolean requiresListenersDuringExecution(TxnId txnId, ExecuteFlags flags)
    {
        return !txnId.is(EphemeralRead) && (!dataStoreDetectsFutureReads() || !flags.contains(HAS_UNIQUE_HLC));
    }

    protected abstract ExecuteOn executeOn();
    abstract public ReadType kind();

    public final @Nullable PartialTxn partialTxn()
    {
        return partialTxn;
    }

    public final @Nullable Timestamp executeAt()
    {
        return executeAt;
    }

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
        this.replyTo = replyTo;
        this.replyContext = replyContext;
        long expiresAt = on.agent().expiresAt(replyContext, MICROSECONDS);
        process(on, expiresAt);
    }

    // TODO (expected): register slowAt
    public void process(Node on, long expiresAt)
    {
        this.node = on;
        waitingOn = new IntHashSet();
        reading = new IntHashSet();
        Cancellable cancel = submit();
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

    protected Cancellable submit()
    {
        stamp = node.currentStamp();

        if (flags.contains(READY_TO_EXECUTE) && fastReadsMayBypassSafeStore(txnId) && partialTxn != null && executeAt != null && (txnId.is(EphemeralRead) || flags.contains(HAS_UNIQUE_HLC)))
            return node.commandStores().mapReduceConsume(scope, minEpoch(), executeAtEpoch, this::applyFastRead, this, this);

        return node.mapReduceConsumeLocal(this, scope, minEpoch(), executeAtEpoch, this);
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
        StoreParticipants participants = StoreParticipants.execute(safeStore, scope, txnId, minEpoch(), executeAtEpoch);
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

            if (logger.isTraceEnabled())
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
                    else if (c > 0 && status.compareTo(executeOn().min) >= 0 && status.compareTo(SaveStatus.PreApplied) < 0) safeStore.progressLog().waiting(CanApply, safeStore, safeCommand, null, scope, null);
                    return status.compareTo(SaveStatus.Stable) >= 0 ? null : Insufficient;

                case OBSOLETE:
                    state = State.PENDING_OBSOLETE;
                    return Redundant;

                case EXECUTE:
                    if (requiresListenersDuringExecution)
                        listeners.put(storeId, safeStore.register(txnId, this));
                    waitingOn.add(storeId);
                    ++waitingOnCount;
                    reading.add(storeId);
            }
        }

        read(safeStore, safeCommand.current());
        return null;
    }

    private AsyncChain<CommitOrReadNack> applyFastRead(CommandStore unsafeStore)
    {
        Invariants.require(partialTxn != null);
        Invariants.require(executeAt != null);

        synchronized (this)
        {
            if (state != State.PENDING)
                return null;

            int storeId = unsafeStore.id();
            CommandStores.RangesForEpoch ranges = unsafeStore.unsafeGetRangesForEpoch();
            Ranges unavailable = unavailable(unsafeStore);

            Participants<?> read = scope.slice(ranges.allAt(executeAtEpoch), Minimal)
                                        .without(unavailable);
            if (read.isEmpty())
            {
                updateUnavailable(unavailable);
                return null;
            }

            waitingOn.add(storeId);
            ++waitingOnCount;
            reading.add(storeId);

            return partialTxn.readDirect(unsafeStore, executeAt, read)
                             .invoke(readCallback(unsafeStore, unavailable))
                             .map(i -> null);
        }
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
        int storeId = safeStore.commandStore().id();
        Command command = safeCommand.current();

        if (logger.isTraceEnabled())
            logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                         this, command.txnId(), command.status(), command);

        boolean execute;
        synchronized (this)
        {
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

                    if (!requiresListenersDuringExecution)
                        listeners.remove(storeId);

                    execute = true;
            }
        }

        if (execute)
        {
            logger.trace("{}: executing read", command.txnId());
            read(safeStore, command);
            return requiresListenersDuringExecution;
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
        partialTxn = null;
        // Unless failed always ack to indicate setup has completed otherwise the counter never gets to -1
        if ((reply == null || !reply.isFinal()) && failure == null)
        {
            onOneSuccess(-1, null);
            if (reply != null)
                reply(reply, null);
        }
        else
        {
            onFailure(reply, failure);
        }
    }

    protected AsyncChain<Data> beginRead(SafeCommandStore safeStore, Timestamp executeAt, PartialTxn txn, Participants<?> execute)
    {
        return txn.read(safeStore, executeAt, execute);
    }

    static Ranges unavailable(SafeCommandStore safeStore, Command command)
    {
        return unavailable(command.txnId(), command.executeAtIfKnown(), command.route(), safeStore.ranges(), safeStore.commandStore().unsafeGetSafeToRead());
    }

    Ranges unavailable(CommandStore unsafeStore)
    {
        Invariants.require(executeAt != null);
        return unavailable(txnId, executeAt, scope, unsafeStore.unsafeGetRangesForEpoch(), unsafeStore.unsafeGetSafeToRead());
    }

    static Ranges unavailable(TxnId txnId, Timestamp executeAt, Participants<?> scope, CommandStores.RangesForEpoch ranges, NavigableMap<Timestamp, Ranges> safeToReadAt)
    {
        // note: syncpoints and ephemeral reads simply consume the latest information (whatever it is),
        //  which is represented by the latest possible safeToRead entry (which is only updated on successful bootstrap)
        //  We DO NOT use executesAtLeast here, this is used only to compute retryInFutureEpoch which reports ranges
        //  we aren't able to serve information newer than executesAtLeast for.
        //  executeAtLeast can yield false answers for safeToRead even for single keys by picking a time
        if (executeAt == null)
        {
            Invariants.require(txnId.awaitsOnlyDeps());
            executeAt = txnId;
        }
        Timestamp readAt = txnId.awaitsOnlyDeps() ? Timestamp.MAX : executeAt;
        Ranges safeToRead = safeToReadAt.lowerEntry(readAt).getValue();
        Ranges reads = ranges.allAt(executeAt);
        Ranges unsafeToRead = reads.without(safeToRead);
        if (unsafeToRead.size() > scope.size())
            unsafeToRead = unsafeToRead.intersecting(scope, Minimal);
        return unsafeToRead;
    }

    protected void read(SafeCommandStore safeStore, Command command)
    {
        // TODO (required): do we need to check unavailable again on completion, or throughout execution?
        //    e.g. if we are marked stale and begin processing later commands
        Ranges unavailable = unavailable(safeStore, command);
        if (txnId.is(Write))
        {
            Committed committed = command.asCommitted();
            long uniqueHlc = committed.waitingOn().minUniqueHlc();
            if (committed.executeAt().hasDistinctHlcAndUniqueHlc())
                uniqueHlc = committed.executeAt().uniqueHlc();

            if (uniqueHlc > 0)
            {
                synchronized (this)
                {
                    this.uniqueHlc = Math.max(this.uniqueHlc, uniqueHlc);
                }
            }
        }

        CommandStore unsafeStore = safeStore.commandStore();
        // note: we DO NOT use stillExecutes() here, as we do not know what the remote replica requires
        StoreParticipants participants = command.participants();
        Participants<?> executes = nonNull(participants.executes());
        if (executes != participants.stillExecutes() && !txnId.isSystemTxn())
        {
            // if stillExecutes has been filtered, we may have also filtered
            PartialTxn txn = command.partialTxn();
            Participants<?> covers = txn == null ? executes.slice(0, 0) : txn.keys().toParticipants();
            Participants<?> missing = executes.without(covers).without(unavailable);
            if (!missing.isEmpty())
                unavailable = unavailable.with(missing.toRanges());
        }
        executes = executes.without(unavailable);

        Timestamp executeAt = command.executeAt();
        if (this.executeAt == null) this.executeAt = executeAt;
        else if (txnId.awaitsOnlyDeps()) this.executeAt = Timestamp.max(this.executeAt, executeAt);
        else Invariants.require(executeAt.equals(this.executeAt));

        if (executes.isEmpty()) readComplete(unsafeStore, null, unavailable);
        else beginRead(safeStore, executeAt, command.partialTxn(), executes)
             .begin(readCallback(unsafeStore, unavailable));
    }

    private BiConsumer<Data, Throwable> readCallback(CommandStore unsafeStore, @Nullable Ranges unavailable)
    {
        return (success, fail) -> {
            if (fail != null)
            {
                if (logger.isTraceEnabled())
                    logger.trace("{}: read failed for {}", txnId, unsafeStore, fail);
                onFailure(null, fail);
            }
            else if (success == null)
            {
                onFailure(Redundant, null);
            }
            else
            {
                readComplete(unsafeStore, success, unavailable);
            }
        };
    }

    protected void readComplete(CommandStore commandStore, @Nullable Data result, @Nullable Ranges unavailable)
    {
        if (stamp != node.currentStamp())
        {
            Ranges newUnavailable = unavailable(commandStore);
            if (newUnavailable != null)
            {
                Ranges discard;
                if (unavailable == null) unavailable = discard = newUnavailable;
                else
                {
                    unavailable = unavailable.with(newUnavailable);
                    discard = newUnavailable.without(unavailable);
                }
                if (result != null)
                    result = result.without(discard);
            }
        }

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
            clear = onOneSuccessInternal(storeId, unavailable);
        }
        if (cancelSelf != null)
            cancelSelf.cancel();
        cleanup(clear);
    }

    protected void onOneSuccess(int storeId, @Nullable Ranges newUnavailable)
    {
        cleanup(onOneSuccessInternal(storeId, newUnavailable));
    }

    private void updateUnavailable(@Nullable Ranges newUnavailable)
    {
        if (newUnavailable != null && !newUnavailable.isEmpty())
        {
            newUnavailable = newUnavailable.intersecting(scope, Minimal);
            if (unavailable == null) unavailable = newUnavailable;
            else unavailable = newUnavailable.with(unavailable);
        }
    }

    protected synchronized Cancellable onOneSuccessInternal(int storeId, @Nullable Ranges newUnavailable)
    {
        if (storeId >= 0)
        {
            boolean removed = waitingOn.remove(storeId);
            Invariants.require(removed, "%s not waiting on store %d; waitingOn=%s", txnId, storeId, waitingOn);
        }

        updateUnavailable(newUnavailable);

        // wait for -1 to ensure the setup phase has also completed. Setup calls ack in its callback
        // and prevents races where we respond before dispatching all the required reads (if the reads are
        // completing faster than the reads can be setup on all required shards)
        if (--waitingOnCount >= 0)
            return null;

        switch (state)
        {
            default: throw new UnhandledEnum(state);
            case RETURNED:
                throw illegalState("ReadOk was sent, yet ack called again");

            case PENDING_OBSOLETE:
            case OBSOLETE:
                logger.debug("Before the read completed successfully for txn {}, the result was marked obsolete", txnId);
                return null;

            case PENDING:
                state = State.RETURNED;
                reply(unavailable, data, uniqueHlc);
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
        Invariants.require(state != State.PENDING);
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

    protected void onFailure(CommitOrReadNack failReply, Throwable throwable)
    {
        if (!cancel())
            return;

        if (throwable != null)
        {
            reply(null, throwable);
            node.agent().onUncaughtException(throwable);
        }
        else
        {
            reply(failReply, null);
        }
    }

    @Override
    public Unseekables<?> keys()
    {
        if (flags.contains(READY_TO_EXECUTE) && fastReadsMayBypassCommandsForKey(txnId))
            return RoutingKeys.EMPTY;
        return scope;
    }

    protected void reply(Ranges unavailable, Data data, long uniqueHlc)
    {
        if (data != null && !data.validateReply(txnId, executeAt, !requiresListenersDuringExecution)) reply(Redundant, null);
        else reply(new ReadOk(unavailable, data, uniqueHlc), null);
    }

    protected void reply(ReadReply reply, Throwable fail)
    {
        node.reply(replyTo, replyContext, reply, fail);
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
         * Either not committed, or not stable
         */
        Insufficient("CommitInsufficient", false),

        /**
         * Committed/PreApplied successfully, but the request is blocking so waiting to reply
         */
        Waiting("ReadOrApplyWaiting", false),

        Redundant("CommitOrReadRedundant", true),

        /**
         * The commit has been rejected due to stale ballot.
         */
        Rejected("CommitRejected", true),
        ;

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
         */
        public final @Nullable Ranges unavailable;
        public final @Nullable Data data;
        public final long uniqueHlc;

        protected ReadOk(@Nullable Ranges unavailable, @Nullable Data data)
        {
            this(unavailable, data, 0);
        }

        public ReadOk(@Nullable Ranges unavailable, @Nullable Data data, long uniqueHlc)
        {
            this.unavailable = unavailable;
            this.data = data;
            this.uniqueHlc = uniqueHlc;
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
            return "ReadOk{" + data
                   + (unavailable == null ? "" : ", unavailable:" + unavailable)
                   + (uniqueHlc == 0 ? "" : ", hlc:" + uniqueHlc)
                   + (futureEpoch == 0 ? "" : ", futureEpoch=" + futureEpoch)
                   + '}';
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
