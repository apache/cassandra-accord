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

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;

import javax.annotation.Nullable;

import accord.api.LocalListeners;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.RemoteListeners;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Ballot;
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Status.Durability;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import static accord.messages.Await.Until.CommittedOrNotFastPathCommit;
import static accord.messages.MessageType.StandardMessage.ASYNC_AWAIT_COMPLETE_REQ;
import static accord.messages.MessageType.StandardMessage.AWAIT_REQ;
import static accord.messages.MessageType.StandardMessage.AWAIT_RSP;
import static accord.messages.RouteRequest.computeScope;
import static accord.primitives.Status.Durability.HasDecisionOrOutcome.None;
import static accord.primitives.Status.Durability.HasDecisionOrOutcome.DurablyStable;
import static accord.primitives.Status.Durability.HasDecisionOrOutcome.DurablyPreApplied;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

/**
 * Contact a replica to perform a synchronous or asynchronous wait on some condition for a transaction + some keys.
 * If the callbackId is less than zero, the wait condition is met synchronously - that is, a reply is not forthcoming
 * until the wait condition is met. A non-negative callbackId will register this callbackId if the wait condition is not
 * met, and return the set of keys where the wait condition *was* met.
 *
 * TODO (desired): return an OK message indicating we're waiting synchronously
 */
public class Await extends AbstractRequest<Participants<?>, Void> implements Request, LocalListeners.ComplexListener, Timeouts.Timeout
{
    public static class SerializerSupport
    {
        public static Await create(TxnId txnId, Participants<?> scope, Until until, boolean notifyProgressLog, long minAwaitEpoch, long maxAwaitEpoch, int callbackId)
        {
            return new Await(txnId, scope, until, minAwaitEpoch, maxAwaitEpoch, callbackId, notifyProgressLog);
        }
    }

    public enum Until
    {
        NotBlocked(SaveStatus.NotDefined, None),

        /**
         * Wait for the transaction to decide its executeAt (or else decide to be invalidated).
         *
         * This also waits for a FullRoute to be known.
         */
        HasDecidedExecuteAt(SaveStatus.PreCommitted, None),

        /**
         * Wait for the transaction to be Committed, or guaranteed not to fast-path commit.
         * This essentially means that the coordinator can reply as soon as the promised ballot is non-zero,
         * but any other replica must wait until Committed.
         *
         * This is used for transactions that may have used the coordinator optimisation.
         * TODO (expected): why is additionallyUnblockedBy not set?
         */
        CommittedOrNotFastPathCommit(SaveStatus.Committed, None, (self, command) -> command.txnId().hasPrivilegedCoordinator()
                                                                                    && command.txnId().node.equals(self)
                                                                                    && !command.promised().equals(Ballot.ZERO)
        ),

        /**
         * Wait for the transaction to be Committed.
         *
         * Note that we only set Committed during coordination, we do not propagate Committed directly between replicas,
         * so for local progress it only makes sense to request HasStableDeps.
         *
         * This BlockedUntil is useful for remote listeners performing recovery that are waiting for transactions in
         * the Accept phase that need to reach Committed to advance the recovery machine.
         */
        HasCommittedDeps(SaveStatus.Committed, None),

        /**
         * Wait for the transaction's dependencies to stabilise. This provides enough information
         * to locally execute a transaction (if all the dependencies have applied).
         */
        HasStableDeps(SaveStatus.Stable, None),

        /**
         * Wait for the transaction's dependencies to stabilise. This provides enough information
         * to locally execute a transaction (if all the dependencies have applied).
         */
        HasStableDepsAtAllShards(SaveStatus.Stable, DurablyStable),

        /**
         * Wait for all shards to be ReadyToExecute so that a recovery coordinator may make progress
         */
        CanCoordinateExecution(SaveStatus.ReadyToExecute, None),

        /**
         * Wait for the transaction to have enough information to apply.
         * It does not need to be ready to apply yet.
         */
        CanApply(SaveStatus.PreApplied, None),

        /**
         * Wait for the transaction to have enough information to apply.
         * It does not need to be ready to apply yet.
         */
        CanApplyAtAllShards(SaveStatus.PreApplied, DurablyPreApplied),

        /**
         * Wait for the transaction to be applied.
         */
        IsApplied(SaveStatus.Applied, None);

        private static final Until[] lookupByOrdinal = values();
        public final SaveStatus requiredSaveStatus;
        public final Durability.HasDecisionOrOutcome requiredDurability;
        public final @Nullable BiPredicate<Node.Id, Command> additionallyUnblockedBy;

        Until(SaveStatus requiredSaveStatus, Durability.HasDecisionOrOutcome requiredDurability)
        {
            this(requiredSaveStatus, requiredDurability, null);
        }

        Until(SaveStatus requiredSaveStatus, Durability.HasDecisionOrOutcome requiredDurability, @Nullable BiPredicate<Node.Id, Command> additionallyUnblockedBy)
        {
            this.requiredSaveStatus = requiredSaveStatus;
            this.requiredDurability = requiredDurability;
            this.additionallyUnblockedBy = additionallyUnblockedBy;
        }

        public static Until forOrdinal(int ordinal)
        {
            if (ordinal < 0 || ordinal > lookupByOrdinal.length)
                throw new IndexOutOfBoundsException(ordinal);
            return lookupByOrdinal[ordinal];
        }

        public BlockedUntil toBlockedUntil()
        {
            switch (this)
            {
                default: throw new UnhandledEnum(this);
                case NotBlocked:
                    return BlockedUntil.NotBlocked;
                case HasDecidedExecuteAt:
                    return BlockedUntil.HasDecidedExecuteAt;
                case CommittedOrNotFastPathCommit:
                case HasCommittedDeps:
                case CanCoordinateExecution:
                case HasStableDeps:
                    return BlockedUntil.HasStableDeps;
                case CanApply:
                case IsApplied:
                    return BlockedUntil.CanApply;
            }
        }
    }

    public final Until until;
    public final long minAwaitEpoch, maxAwaitEpoch;
    public final int callbackId; // < 0 means synchronous await
    public final boolean notifyProgressLog;

    private transient volatile RemoteListeners.Registration asyncRegistration;
    private static final AtomicReferenceFieldUpdater<Await, RemoteListeners.Registration> registrationUpdater = AtomicReferenceFieldUpdater.newUpdater(Await.class, RemoteListeners.Registration.class, "asyncRegistration");

    private transient volatile int synchronouslyWaitingOn;
    private static final AtomicIntegerFieldUpdater<Await> synchronouslyWaitingOnUpdater = AtomicIntegerFieldUpdater.newUpdater(Await.class, "synchronouslyWaitingOn");

    private transient volatile Collection<LocalListeners.Registered> syncRegistrations;
    private static final AtomicReferenceFieldUpdater<Await, Collection<LocalListeners.Registered>> syncRegistrationsUpdater = AtomicReferenceFieldUpdater.newUpdater(Await.class, (Class)Collection.class, "syncRegistrations");
    private transient RegisteredTimeout timeout;

    // we use exactly -1 to make serialization easy (can increment by 1 and store a non-negative integer)
    private static final int SYNCHRONOUS_CALLBACKID = -1;
    protected boolean unavailable;

    public Await(Id to, Topologies topologies, TxnId txnId, Participants<?> participants, Until until, int callbackId, boolean notifyProgressLog)
    {
        super(txnId, computeScope(to, topologies, participants));
        this.callbackId = callbackId;
        this.notifyProgressLog = notifyProgressLog;
        this.until = until;
        this.maxAwaitEpoch = topologies.currentEpoch();
        this.minAwaitEpoch = topologies.oldestEpoch();
        Invariants.require(minAwaitEpoch >= txnId.epoch());
    }

    public Await(Id to, Topologies topologies, TxnId txnId, Participants<?> participants, Until until, boolean notifyProgressLog)
    {
        this(to, topologies, txnId, participants, until, SYNCHRONOUS_CALLBACKID, notifyProgressLog);
    }

    public Await(TxnId txnId, Participants<?> scope, Until until, long minAwaitEpoch, long maxAwaitEpoch, int callbackId, boolean notifyProgressLog)
    {
        super(txnId, scope);
        this.until = until;
        this.minAwaitEpoch = minAwaitEpoch;
        this.maxAwaitEpoch = maxAwaitEpoch;
        this.callbackId = callbackId;
        this.notifyProgressLog = notifyProgressLog;
        Invariants.require(minAwaitEpoch >= txnId.epoch());
    }

    @Override
    public Cancellable process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        this.node = node;
        this.replyTo = replyToNode;
        this.replyContext = replyContext;
        // TODO (expected): integrate with cancellation
        return node.commandStores().mapReduceConsume(minAwaitEpoch, maxAwaitEpoch, this);
    }

    @Override
    public Void applyInternal(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, scope, minAwaitEpoch, txnId, maxAwaitEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();
        Invariants.require(minAwaitEpoch >= txnId.epoch());
        if (command.saveStatus().compareTo(until.requiredSaveStatus) >= 0
            || (until == CommittedOrNotFastPathCommit && node.id().equals(txnId.node) && !command.promised().equals(Ballot.ZERO)))
        {
            onNotWaiting(safeStore, safeCommand);
            return null;
        }

        participants = Commands.supplementParticipants(safeStore, safeCommand, participants).participants();
        if (participants.stillTouches().isEmpty())
        {
            // stillTouches removes only shard redundant participants, so we know any consensus decision
            // we might participate for e.g. a pre-bootstrap range is also something we cannot usefully answer
            onUnavailable();
            return null;
        }

        if (callbackId >= 0)
        {
            RemoteListeners.Registration registered = asyncRegistration;
            if (registered == null)
            {
                registered = node.remoteListeners().register(txnId, until.requiredSaveStatus, until.requiredDurability, replyTo, callbackId);
                if (!registrationUpdater.compareAndSet(this, null, registered))
                    registered = asyncRegistration;
            }
            registered.add(safeStore, safeCommand);
        }
        else
        {
            if (syncRegistrations == null)
                syncRegistrationsUpdater.compareAndSet(this, null, new ConcurrentLinkedQueue<>());
            syncRegistrations.add(safeStore.register(txnId, this));
            synchronouslyWaitingOnUpdater.incrementAndGet(this);
        }

        if (notifyProgressLog)
            safeStore.progressLog().waiting(until.toBlockedUntil(), safeStore, safeCommand, null, null, participants);
        return null;
    }

    @Override
    public Void reduce(Void o1, Void o2)
    {
        return null;
    }

    @Override
    public void accept(Void result, Throwable failure)
    {
        if (failure != null)
        {
            cancel();
            reply(null, failure);
        }
        else if (callbackId >= 0)
        {
            RemoteListeners.Registration asyncRegistration = this.asyncRegistration;
            AwaitOk reply = unavailable ? AwaitOk.Unavailable : asyncRegistration == null || 0 == asyncRegistration.done() ? AwaitOk.Ready : AwaitOk.NotReady;
            reply(reply, null);
        }
        else
        {
            int waitingOn = synchronouslyWaitingOnUpdater.decrementAndGet(this);
            if (waitingOn >= 0)
            {
                long expiresAtMicros = replyContext.expiresAt(MICROSECONDS);
                if (expiresAtMicros > 0)
                {
                    timeout = node.timeouts().registerAt(this, expiresAtMicros, MICROSECONDS);
                    if (-1 == synchronouslyWaitingOn)
                        timeout.cancel(); // we could leave a dangling timeout in this rare race condition
                }
            }
            else
            {
                onSynchronousAwaitComplete();
            }
        }
    }

    protected void reply(AwaitOk reply, Throwable failure)
    {
        node.reply(replyTo, replyContext, reply, failure, tracing());
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

    void cancel()
    {
        RegisteredTimeout cancelTimeout = timeout;
        Collection<LocalListeners.Registered> cancelRegistrations = syncRegistrations;
        if (cancelTimeout != null)
            cancelTimeout.cancel();
        if (cancelRegistrations != null)
            cancelRegistrations.forEach(LocalListeners.Registered::cancel);
        timeout = null;
        syncRegistrations = null;
        asyncRegistration = null;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public String reason()
    {
        return "AwaitComplete{" + until + ',' + txnId + '}';
    }

    @Override
    public MessageType type()
    {
        return AWAIT_REQ;
    }

    public enum AwaitOk implements Reply
    {
        NotReady, Ready, Unavailable;

        @Override
        public MessageType type()
        {
            return AWAIT_RSP;
        }
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch();
    }

    @Override
    public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        if (!checkOneSynchronousAwait(safeCommand))
            return true;

        onNotWaiting(safeStore, safeCommand);
        if (-1 == synchronouslyWaitingOnUpdater.decrementAndGet(this))
        {
            onSynchronousAwaitComplete();
            if (timeout != null)
                timeout.cancel();
            syncRegistrations = null;
        }

        return false;
    }

    protected boolean checkOneSynchronousAwait(SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
        SaveStatus saveStatus = command.saveStatus();
        return saveStatus.compareTo(until.requiredSaveStatus) >= 0
               || (until.additionallyUnblockedBy != null && until.additionallyUnblockedBy.test(node.id(), command));
    }

    protected void onUnavailable()
    {
        unavailable = true;
    }

    protected void onNotWaiting(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
    }

    protected void onSynchronousAwaitComplete()
    {
        node.reply(replyTo, replyContext, unavailable ? AwaitOk.Unavailable : AwaitOk.Ready, null, tracing());
    }

    public static class AsyncAwaitComplete extends RouteRequest<Reply>
    {
        public final SaveStatus newStatus;
        public final int callbackId;

        public AsyncAwaitComplete(TxnId txnId, Route<?> route, SaveStatus newStatus, int callbackId)
        {
            super(txnId, route, txnId.epoch());
            this.newStatus = newStatus;
            this.callbackId = callbackId;
        }

        @Override
        public MessageType type()
        {
            return ASYNC_AWAIT_COMPLETE_REQ;
        }

        @Nullable
        @Override
        protected Cancellable submit()
        {
            return node.commandStores().mapReduceConsume(txnId.epoch(), Long.MAX_VALUE, this);
        }

        @Override
        protected void acceptInternal(Reply reply, Throwable failure)
        {
        }

        @Override
        public Reply reduce(Reply o1, Reply o2)
        {
            return null;
        }

        @Override
        public Reply applyInternal(SafeCommandStore safeStore)
        {
            SafeCommand safeCommand = safeStore.unsafeTryGet(txnId);
            if (safeCommand == null || safeCommand.current().saveStatus() == SaveStatus.Uninitialised)
                return null;

            Commands.updateRoute(safeStore, safeCommand, scope);
            safeStore.progressLog().remoteCallback(safeStore, safeCommand, newStatus, callbackId, node.id());
            return null;
        }

        @Override
        public String toString()
        {
            return "AsyncAwaitComplete{" +
                   "txnId=" + txnId +
                   ", route=" + scope +
                   ", newStatus=" + newStatus +
                   ", callbackId=" + callbackId +
                   '}';
        }
    }

    @Override
    public String toString()
    {
        return "Await{" +
               "txnId=" + txnId +
               ", scope=" + scope +
               ", until=" + until +
               ", minAwaitEpoch=" + minAwaitEpoch +
               ", maxAwaitEpoch=" + maxAwaitEpoch +
               ", callbackId=" + callbackId +
               ", notifyProgressLog=" + notifyProgressLog +
               '}';
    }
}
