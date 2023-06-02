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

import accord.api.Data;
import accord.api.ProgressLog.ProgressShard;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.ImmutableBitSet;
import accord.utils.IndexedQuadConsumer;
import accord.utils.Invariants;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.utils.SimpleBitSet;
import accord.utils.async.AsyncChain;

import com.google.common.collect.ImmutableSortedSet;

import java.util.*;

import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.local.Listeners.Immutable.EMPTY;
import static accord.local.SaveStatus.Uninitialised;
import static accord.local.Status.Durability.DurableOrInvalidated;
import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.NotDurable;
import static accord.utils.SortedArrays.forEachIntersection;
import static accord.utils.Utils.*;
import static java.lang.String.format;

public abstract class Command implements CommonAttributes
{
    interface Listener
    {
        void onChange(SafeCommandStore safeStore, SafeCommand safeCommand);

        /**
         * Scope needed to run onChange
         */
        PreLoadContext listenerPreLoadContext(TxnId caller);
    }

    public interface DurableAndIdempotentListener extends Listener
    {
    }

    public interface TransientListener extends Listener
    {
    }

    public static class ProxyListener implements DurableAndIdempotentListener
    {
        protected final TxnId listenerId;

        public ProxyListener(TxnId listenerId)
        {
            this.listenerId = listenerId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProxyListener that = (ProxyListener) o;
            return listenerId.equals(that.listenerId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(listenerId);
        }

        @Override
        public String toString()
        {
            return "ListenerProxy{" + listenerId + '}';
        }

        public TxnId txnId()
        {
            return listenerId;
        }

        @Override
        public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            Commands.listenerUpdate(safeStore, safeStore.get(listenerId), safeCommand);
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(listenerId, caller, Keys.EMPTY);
        }
    }

    static PreLoadContext contextForCommand(Command command)
    {
        Invariants.checkState(command.hasBeen(Status.PreAccepted) && command.partialTxn() != null);
        return command instanceof PreLoadContext ? (PreLoadContext) command : PreLoadContext.contextFor(command.txnId(), command.partialTxn().keys());
    }

    private static Status.Durability durability(Status.Durability durability, SaveStatus status)
    {
        if (status.compareTo(SaveStatus.PreApplied) >= 0 && durability == NotDurable)
            return Local; // not necessary anywhere, but helps for logical consistency
        return durability;
    }

    @VisibleForImplementation
    public static class SerializerSupport
    {
        public static NotDefined notDefined(CommonAttributes attributes, Ballot promised)
        {
            return NotDefined.notDefined(attributes, promised);
        }

        public static PreAccepted preaccepted(CommonAttributes common, Timestamp executeAt, Ballot promised)
        {
            return PreAccepted.preAccepted(common, executeAt, promised);
        }

        public static Accepted accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            return Accepted.accepted(common, status, executeAt, promised, accepted);
        }

        public static Committed committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            return Committed.committed(common, status, executeAt, promised, accepted, waitingOn);
        }

        public static Executed executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            return Executed.executed(common, status, executeAt, promised, accepted, waitingOn, writes, result);
        }
    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> expected, Class<?> actual)
    {
        if (actual != expected)
        {
            throw new IllegalStateException(format("Cannot instantiate %s for status %s. %s expected",
                                                   actual.getSimpleName(), status, expected.getSimpleName()));
        }
        return status;
    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> klass)
    {
        switch (status.status)
        {
            case NotDefined:
                return validateCommandClass(status, NotDefined.class, klass);
            case PreAccepted:
                return validateCommandClass(status, PreAccepted.class, klass);
            case AcceptedInvalidate:
            case Accepted:
            case PreCommitted:
                return validateCommandClass(status, Accepted.class, klass);
            case Committed:
            case ReadyToExecute:
                return validateCommandClass(status, Committed.class, klass);
            case PreApplied:
            case Applying:
            case Applied:
                return validateCommandClass(status, Executed.class, klass);
            case Invalidated:
            case Truncated:
                return validateCommandClass(status, Truncated.class, klass);
            default:
                throw new IllegalStateException("Unhandled status " + status);
        }
    }

    private abstract static class AbstractCommand extends Command
    {
        private final TxnId txnId;
        private final SaveStatus status;
        private final Status.Durability durability;
        private final Route<?> route;
        private final ProgressShard progressShard;
        private final Ballot promised;
        private final Listeners.Immutable listeners;

        private AbstractCommand(TxnId txnId, SaveStatus status, Status.Durability durability, Route<?> route, ProgressShard progressShard, Ballot promised, Listeners.Immutable listeners)
        {
            this.txnId = txnId;
            this.status = validateCommandClass(status, getClass());
            this.durability = durability;
            this.route = route;
            this.progressShard = progressShard;
            this.promised = promised;
            this.listeners = listeners;
        }

        private AbstractCommand(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            this.txnId = common.txnId();
            this.status = validateCommandClass(status, getClass());
            this.durability = common.durability();
            this.route = common.route();
            this.progressShard = common.progressShard();
            this.promised = promised;
            this.listeners = common.durableListeners();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Command command = (Command) o;
            return txnId.equals(command.txnId())
                    && status == command.saveStatus()
                    && durability == command.durability()
                    && Objects.equals(route, command.route())
                    && Objects.equals(progressShard, command.progressShard())
                    && Objects.equals(promised, command.promised())
                    && listeners.equals(command.durableListeners());
        }

        @Override
        public String toString()
        {
            return "Command@" + System.identityHashCode(this) + '{' + txnId + ':' + status + '}';
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        @Override
        public final Route<?> route()
        {
            return route;
        }

        @Override
        public final @Nonnull ProgressShard progressShard()
        {
            return progressShard;
        }

        @Override
        public Ballot promised()
        {
            return promised;
        }

        @Override
        public Status.Durability durability()
        {
            return Command.durability(durability, saveStatus());
        }

        @Override
        public Listeners.Immutable durableListeners()
        {
            if (listeners == null)
                return EMPTY;
            return listeners;
        }

        @Override
        public final SaveStatus saveStatus()
        {
            return status;
        }
    }

    @Override
    public final int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * We require that this is a FullRoute for all states where isDefinitionKnown().
     * In some cases, the home shard will contain an arbitrary slice of the Route where !isDefinitionKnown(),
     * i.e. when a non-home shard informs the home shards of a transaction to ensure forward progress.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     *
     * TODO (expected): audit uses; do not assume null means it is a complete route for the shard
     */
    @Override
    public abstract Route<?> route();

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     */
    public RoutingKey homeKey()
    {
        Route<?> route = route();
        return route == null ? null : route.homeKey();
    }

    @Override
    public abstract ProgressShard progressShard();
    @Override
    public abstract TxnId txnId();
    public abstract Ballot promised();
    @Override
    public abstract Status.Durability durability();
    @Override
    public abstract Listeners.Immutable durableListeners();
    public abstract SaveStatus saveStatus();

    static boolean isSameClass(Command command, Class<? extends Command> klass)
    {
        return command.getClass() == klass;
    }

    private static void checkNewBallot(Ballot current, Ballot next, String name)
    {
        if (next.compareTo(current) < 0)
            throw new IllegalArgumentException(format("Cannot update %s ballot from %s to %s. New ballot is less than current", name, current, next));
    }

    private static void checkPromised(Command command, Ballot ballot)
    {
        checkNewBallot(command.promised(), ballot, "promised");
    }

    private static void checkAccepted(Command command, Ballot ballot)
    {
        checkNewBallot(command.accepted(), ballot, "accepted");
    }

    private static void checkSameClass(Command command, Class<? extends Command> klass, String errorMsg)
    {
        if (!isSameClass(command, klass))
            throw new IllegalArgumentException(errorMsg + format(" expected %s got %s", klass.getSimpleName(), command.getClass().getSimpleName()));
    }

    // TODO (low priority, progress): callers should try to consult the local progress shard (if any) to obtain the full set of keys owned locally
    public Route<?> maxRoute()
    {
        Route<?> route = route();
        if (route == null)
            return null;

        return route.withHomeKey();
    }

    public PreLoadContext contextForSelf()
    {
        return contextForCommand(this);
    }

    public abstract Timestamp executeAt();
    public abstract Ballot accepted();
    @Override
    public abstract PartialTxn partialTxn();
    @Override
    public abstract @Nullable PartialDeps partialDeps();

    public final Status status()
    {
        return saveStatus().status;
    }

    public final Status.Known known()
    {
        return saveStatus().known;
    }

    public final boolean hasBeen(Status status)
    {
        return status().compareTo(status) >= 0;
    }

    public boolean has(Status.Known known)
    {
        return known.isSatisfiedBy(saveStatus().known);
    }

    public boolean has(Status.Definition definition)
    {
        return known().definition.compareTo(definition) >= 0;
    }

    public boolean has(Status.Outcome outcome)
    {
        return known().outcome.compareTo(outcome) >= 0;
    }

    public boolean is(Status status)
    {
        return status() == status;
    }

    public final ProxyListener asListener()
    {
        return new ProxyListener(txnId());
    }

    public final boolean isDefined()
    {
        boolean result = status().hasBeen(Status.PreAccepted);
        Invariants.checkState(result == (this instanceof PreAccepted));
        return result;
    }

    public final PreAccepted asWitnessed()
    {
        return Invariants.cast(this, PreAccepted.class);
    }

    public final boolean isAccepted()
    {
        boolean result = status().hasBeen(Status.AcceptedInvalidate);
        Invariants.checkState(result == (this instanceof Accepted));
        return result;
    }

    public final Accepted asAccepted()
    {
        return Invariants.cast(this, Accepted.class);
    }

    public final boolean isCommitted()
    {
        boolean result = status().hasBeen(Status.Committed);
        Invariants.checkState(result == (this instanceof Committed));
        return result;
    }

    public final Committed asCommitted()
    {
        return Invariants.cast(this, Committed.class);
    }

    public final boolean isExecuted()
    {
        boolean result = hasBeen(Status.PreApplied) && !hasBeen(Status.Truncated);
        Invariants.checkState(result == (this instanceof Executed));
        return result;
    }

    public final Executed asExecuted()
    {
        return Invariants.cast(this, Executed.class);
    }

    public abstract Command updateAttributes(CommonAttributes attrs, Ballot promised);

    public final Command updateAttributes(CommonAttributes attrs)
    {
        return updateAttributes(attrs, promised());
    }

    public final Command updatePromised(Ballot promised)
    {
        return updateAttributes(this, promised);
    }

    public static final class NotDefined extends AbstractCommand
    {
        NotDefined(TxnId txnId, SaveStatus status, Status.Durability durability, Route<?> route, ProgressShard progressShard, Ballot promised, Listeners.Immutable listeners)
        {
            super(txnId, status, durability, route, progressShard, promised, listeners);
        }

        NotDefined(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            super(common, status, promised);
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new NotDefined(attrs, initialise(saveStatus()), promised);
        }

        public static NotDefined notDefined(CommonAttributes common, Ballot promised)
        {
            return new NotDefined(common, SaveStatus.NotDefined, promised);
        }

        public static NotDefined uninitialised(TxnId txnId)
        {
            return new NotDefined(txnId, Uninitialised, NotDurable, null, Unsure, Ballot.ZERO, null);
        }

        @Override
        public Timestamp executeAt()
        {
            return null;
        }

        @Override
        public Ballot promised()
        {
            // we can be preacceptedInvalidated, so can be promised even if not witnessed
            return super.promised;
        }

        @Override
        public Ballot accepted()
        {
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return null;
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            return null;
        }

        private static SaveStatus initialise(SaveStatus saveStatus)
        {
            return saveStatus == Uninitialised ? SaveStatus.NotDefined : saveStatus;
        }
    }

    public static final class Truncated extends AbstractCommand
    {
        final Timestamp executeAt;
        public Truncated(TxnId txnId, SaveStatus saveStatus, Route<?> route, ProgressShard progressShard, Timestamp executeAt, Listeners.Immutable listeners)
        {
            super(txnId, saveStatus, DurableOrInvalidated, route, progressShard, Ballot.MAX, listeners);
            this.executeAt = executeAt;
        }

        public static Truncated truncated(Command command)
        {
            return new Truncated(command.txnId(), SaveStatus.Truncated, command.route(), command.progressShard(), command.known().executeAt.hasDecidedExecuteAt() ? command.executeAt() : null, EMPTY);
        }

        public static Truncated truncated(Command command, Route<?> route, ProgressShard progressShard, Timestamp executeAt)
        {
            return new Truncated(command.txnId(), SaveStatus.Truncated, route, progressShard, executeAt, EMPTY);
        }

        public static Truncated invalidated(Command command)
        {
            Invariants.checkState(!command.hasBeen(Status.PreCommitted));
            return new Truncated(command.txnId(), SaveStatus.Invalidated, null, command.progressShard(), Timestamp.NONE, command.durableListeners());
        }

        @Override
        public Timestamp executeAt()
        {
            return executeAt;
        }

        @Override
        public Ballot accepted()
        {
            return Ballot.MAX;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return null;
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            return null;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            // TODO (now): invoke listeners precisely once when we adopt this state, then we can simply return `this`
            return new Truncated(txnId(), saveStatus(), attrs.route(), attrs.progressShard(), executeAt, attrs.durableListeners());
        }
    }

    public static class PreAccepted extends AbstractCommand
    {
        private final Timestamp executeAt;
        private final PartialTxn partialTxn;
        private final @Nullable PartialDeps partialDeps;

        private PreAccepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised)
        {
            super(common, status, promised);
            this.executeAt = executeAt;
            this.partialTxn = common.partialTxn();
            this.partialDeps = common.partialDeps();
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new PreAccepted(attrs, saveStatus(), executeAt(), promised);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            PreAccepted that = (PreAccepted) o;
            return executeAt.equals(that.executeAt)
                    && Objects.equals(partialTxn, that.partialTxn)
                    && Objects.equals(partialDeps, that.partialDeps);
        }

        public static PreAccepted preAccepted(CommonAttributes common, Timestamp executeAt, Ballot promised)
        {
            return new PreAccepted(common, SaveStatus.PreAccepted, executeAt, promised);
        }
        public static PreAccepted preAccepted(PreAccepted command, CommonAttributes common, Ballot promised)
        {
            checkPromised(command, promised);
            checkSameClass(command, PreAccepted.class, "Cannot update");
            Invariants.checkArgument(command.getClass() == PreAccepted.class);
            return preAccepted(common, command.executeAt(), promised);
        }

        @Override
        public Timestamp executeAt()
        {
            return executeAt;
        }

        @Override
        public Ballot accepted()
        {
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return partialTxn;
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            return partialDeps;
        }
    }

    public static class Accepted extends PreAccepted
    {
        private final Ballot accepted;

        Accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            super(common, status, executeAt, promised);
            this.accepted = accepted;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new Accepted(attrs, saveStatus(), executeAt(), promised, accepted());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Accepted that = (Accepted) o;
            return Objects.equals(accepted, that.accepted);
        }

        static Accepted accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            return new Accepted(common, status, executeAt, promised, accepted);
        }
        static Accepted accepted(Accepted command, CommonAttributes common, SaveStatus status, Ballot promised)
        {
            checkPromised(command, promised);
            checkSameClass(command, Accepted.class, "Cannot update");
            return new Accepted(common, status, command.executeAt(), promised, command.accepted());
        }
        static Accepted accepted(Accepted command, CommonAttributes common, Ballot promised)
        {
            return accepted(command, common, command.saveStatus(), promised);
        }

        @Override
        public Ballot accepted()
        {
            return accepted;
        }
    }

    public static class Committed extends Accepted
    {
        public final WaitingOn waitingOn;
        private Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            super(common, status, executeAt, promised, accepted);
            this.waitingOn = waitingOn;
            Invariants.checkState(waitingOn.deps.equals(common.partialDeps()));
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new Committed(attrs, saveStatus(), executeAt(), promised, accepted(), waitingOn());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Committed committed = (Committed) o;
            return Objects.equals(waitingOn, committed.waitingOn);
        }

        private static Committed committed(Committed command, CommonAttributes common, Ballot promised, SaveStatus status, WaitingOn waitingOn)
        {
            checkPromised(command, promised);
            checkSameClass(command, Committed.class, "Cannot update");
            return new Committed(common, status, command.executeAt(), promised, command.accepted(), waitingOn);
        }

        static Committed committed(Committed command, CommonAttributes common, Ballot promised)
        {
            return committed(command, common, promised, command.saveStatus(), command.waitingOn());
        }

        static Committed committed(Committed command, CommonAttributes common, SaveStatus status)
        {
            return committed(command, common, command.promised(), status, command.waitingOn());
        }

        static Committed committed(Committed command, CommonAttributes common, WaitingOn waitingOn)
        {
            return committed(command, common, command.promised(), command.saveStatus(), waitingOn);
        }

        static Committed committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            return new Committed(common, status, executeAt, promised, accepted, waitingOn);
        }

        public AsyncChain<Data> read(SafeCommandStore safeStore)
        {
            return partialTxn().read(safeStore, executeAt());
        }

        public WaitingOn waitingOn()
        {
            return waitingOn;
        }

        public boolean isWaitingOnCommit()
        {
            return waitingOn.isWaitingOnCommit();
        }

        public boolean isWaitingOnApply()
        {
            return waitingOn.isWaitingOnApply();
        }

        public boolean isWaitingOnDependency()
        {
            return isWaitingOnCommit() || isWaitingOnApply();
        }
    }

    public static class Executed extends Committed
    {
        private final Writes writes;
        private final Result result;

        public Executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            super(common, status, executeAt, promised, accepted, waitingOn);
            this.writes = writes;
            this.result = result;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new Executed(attrs, saveStatus(), executeAt(), promised, accepted(), waitingOn(), writes, result);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Executed executed = (Executed) o;
            return Objects.equals(writes, executed.writes)
                    && Objects.equals(result, executed.result);
        }

        public static Executed executed(Executed command, CommonAttributes common, SaveStatus status, Ballot promised, WaitingOn waitingOn)
        {
            checkSameClass(command, Executed.class, "Cannot update");
            return new Executed(common, status, command.executeAt(), promised, command.accepted(), waitingOn, command.writes(), command.result());
        }

        public static Executed executed(Executed command, CommonAttributes common, SaveStatus status)
        {
            return executed(command, common, status, command.promised(), command.waitingOn());
        }

        public static Executed executed(Executed command, CommonAttributes common, WaitingOn waitingOn)
        {
            return executed(command, common, command.saveStatus(), command.promised(), waitingOn);
        }

        public static Executed executed(Executed command, CommonAttributes common, Ballot promised)
        {
            return executed(command, common, command.saveStatus(), promised, command.waitingOn());
        }

        public static Executed executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            return new Executed(common, status, executeAt, promised, accepted, waitingOn, writes, result);
        }

        public Writes writes()
        {
            return writes;
        }

        public Result result()
        {
            return result;
        }
    }

    public static class WaitingOn
    {
        public static final WaitingOn EMPTY = new WaitingOn(Deps.NONE, ImmutableBitSet.EMPTY, ImmutableBitSet.EMPTY, ImmutableBitSet.EMPTY);

        public final Deps deps;
        // note that transactions default to waitingOnCommit, so presence in the set does not mean the transaction is uncommitted
        public final ImmutableBitSet waitingOnCommit, waitingOnApply, appliedOrInvalidated;

        public WaitingOn(Deps deps)
        {
            this.deps = deps;
            this.waitingOnCommit = new ImmutableBitSet(deps.txnIdCount(), true);
            this.waitingOnApply = new ImmutableBitSet(deps.txnIdCount(), false);
            this.appliedOrInvalidated = new ImmutableBitSet(deps.txnIdCount(), false);
        }

        public WaitingOn(Deps deps, ImmutableBitSet waitingOnCommit, ImmutableBitSet waitingOnApply, ImmutableBitSet appliedOrInvalidated)
        {
            this.deps = deps;
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
            this.appliedOrInvalidated = appliedOrInvalidated;
        }

        public boolean isWaitingOnCommit()
        {
            return !waitingOnCommit.isEmpty();
        }

        public boolean isWaitingOnApply()
        {
            return !waitingOnApply.isEmpty();
        }

        public boolean isWaitingOn(TxnId txnId)
        {
            int index = deps.indexOf(txnId);
            return index >= 0 && (waitingOnCommit.get(index) || waitingOnApply.get(index));
        }

        public TxnId nextWaitingOnCommit()
        {
            int i = waitingOnCommit.prevSetBit(waitingOnCommit.size() - 1);
            return i < 0 ? null : deps.txnId(i);
        }

        public TxnId nextWaitingOnApply()
        {
            int i = waitingOnApply.prevSetBit(waitingOnApply.size() - 1);
            return i < 0 ? null : deps.txnId(i);
        }

        public TxnId nextWaitingOn()
        {
            TxnId next = nextWaitingOnApply();
            return next != null ? next : nextWaitingOnCommit();
        }

        public boolean isAppliedOrInvalidatedRangeIdx(int i)
        {
            return appliedOrInvalidated.get(i + deps.keyDeps.txnIdCount());
        }

        public TxnId minWaitingOnTxnId()
        {
            return minWaitingOnTxnId(deps, waitingOnCommit, waitingOnApply);
        }

        static TxnId minWaitingOnTxnId(Deps deps, SimpleBitSet waitingOnCommit, SimpleBitSet waitingOnApply)
        {
            int keyDepsCount = deps.keyDeps.txnIdCount();
            int minWaitingOnKeys = Math.min(waitingOnCommit.nextSetBitBefore(0, keyDepsCount, Integer.MAX_VALUE), waitingOnApply.nextSetBitBefore(0, keyDepsCount, Integer.MAX_VALUE));
            int minWaitingOnRanges = Math.min(waitingOnCommit.nextSetBit(keyDepsCount, Integer.MAX_VALUE), waitingOnApply.nextSetBit(keyDepsCount, Integer.MAX_VALUE));
            return TxnId.nonNullOrMin(minWaitingOnKeys == Integer.MAX_VALUE ? null : deps.txnId(minWaitingOnKeys),
                                      minWaitingOnRanges == Integer.MAX_VALUE ? null : deps.txnId(minWaitingOnRanges));
        }

        static TxnId minWaitingOn(Deps deps, SimpleBitSet waitingOn)
        {
            int keyDepsCount = deps.keyDeps.txnIdCount();
            int minWaitingOnKeys = waitingOn.nextSetBitBefore(0, keyDepsCount, -1);
            int minWaitingOnRanges = waitingOn.nextSetBit(keyDepsCount, -1);
            return TxnId.nonNullOrMin(minWaitingOnKeys < 0 ? null : deps.keyDeps.txnId(minWaitingOnKeys),
                                      minWaitingOnRanges < 0 ? null : deps.rangeDeps.txnId(minWaitingOnRanges - keyDepsCount));
        }

        static TxnId maxWaitingOn(Deps deps, SimpleBitSet waitingOn)
        {
            int keyDepsCount = deps.keyDeps.txnIdCount();
            int maxWaitingOnRanges = waitingOn.prevSetBitNotBefore(waitingOn.size() - 1, keyDepsCount, -1);
            int maxWaitingOnKeys = waitingOn.prevSetBit(keyDepsCount);
            return TxnId.nonNullOrMax(maxWaitingOnKeys < 0 ? null : deps.keyDeps.txnId(maxWaitingOnKeys),
                                      maxWaitingOnRanges < 0 ? null : deps.rangeDeps.txnId(maxWaitingOnRanges - keyDepsCount));
        }

        public ImmutableSortedSet<TxnId> computeWaitingOnCommit()
        {
            return computeWaitingOnCommit(deps, waitingOnCommit);
        }

        public ImmutableSortedSet<TxnId> computeWaitingOnApply()
        {
            return computeWaitingOnApply(deps, waitingOnCommit, waitingOnApply);
        }

        private static ImmutableSortedSet<TxnId> computeWaitingOnCommit(Deps deps, SimpleBitSet waitingOnCommit)
        {
            ImmutableSortedSet.Builder<TxnId> builder = new ImmutableSortedSet.Builder<>(TxnId::compareTo);
            waitingOnCommit.forEach(builder, deps, (b, d, i) -> b.add(d.txnId(i)));
            return builder.build();
        }

        private static ImmutableSortedSet<TxnId> computeWaitingOnApply(Deps deps, SimpleBitSet waitingOnCommit, SimpleBitSet waitingOnApply)
        {
            ImmutableSortedSet.Builder<TxnId> builder = new ImmutableSortedSet.Builder<>(TxnId::compareTo);
            waitingOnApply.forEach(builder, deps, waitingOnCommit, (b, d, s, i) -> {
                if (!s.get(i))
                    b.add(d.txnId(i));
            });
            return builder.build();
        }

        private static String toString(Deps deps, SimpleBitSet waitingOnCommit, SimpleBitSet waitingOnApply)
        {
            return "onApply=" + computeWaitingOnApply(deps, waitingOnCommit, waitingOnApply).descendingSet() + ", onCommit=" + computeWaitingOnCommit(deps, waitingOnCommit).descendingSet();
        }

        public String toString()
        {
            return toString(deps, waitingOnCommit, waitingOnApply);
        }

        public static class Update
        {
            final Deps deps;
            private SimpleBitSet waitingOnCommit, waitingOnApply, appliedOrInvalidated;

            public Update(WaitingOn waitingOn)
            {
                this.deps = waitingOn.deps;
                this.waitingOnCommit = waitingOn.waitingOnCommit;
                this.waitingOnApply = waitingOn.waitingOnApply;
                this.appliedOrInvalidated = waitingOn.appliedOrInvalidated;
            }

            public Update(Committed committed)
            {
                this(committed.waitingOn);
            }

            public Update(Unseekables<?, ?> participants, Deps deps)
            {
                this.deps = deps;
                this.waitingOnCommit = new SimpleBitSet(deps.txnIdCount(), false);
                this.waitingOnCommit.setRange(0, deps.keyDeps.txnIdCount());
                if (!participants.containsAll(deps.keyDeps.keys()))
                {
                    // TODO (now): we don't need to wait on these as we have lost ownership of them locally
                    System.out.println();
                }
                deps.rangeDeps.forEach(participants, this, (u, i) -> {
                    u.waitingOnCommit.set(u.deps.keyDeps.txnIdCount() + i);
                });
                this.waitingOnApply = new SimpleBitSet(deps.txnIdCount(), false);
                this.appliedOrInvalidated = new SimpleBitSet(deps.txnIdCount(), false);
            }

            public boolean hasChanges()
            {
                return !(waitingOnCommit instanceof ImmutableBitSet)
                       || !(waitingOnApply instanceof ImmutableBitSet)
                       || !(appliedOrInvalidated instanceof ImmutableBitSet);
            }

            public boolean removeWaitingOnCommit(TxnId txnId)
            {
                int index = deps.indexOf(txnId);
                if (!waitingOnCommit.get(index))
                    return false;

                waitingOnCommit = ensureMutable(waitingOnCommit);
                waitingOnCommit.unset(index);
                return true;
            }

            public boolean addWaitingOnApply(TxnId txnId)
            {
                int index = deps.indexOf(txnId);
                if (waitingOnApply.get(index))
                    return false;

                waitingOnApply = ensureMutable(waitingOnApply);
                waitingOnApply.set(index);
                return true;
            }

            public boolean removeWaitingOnApply(TxnId txnId)
            {
                int index = deps.indexOf(txnId);
                if (!waitingOnApply.get(index))
                    return false;

                waitingOnApply = ensureMutable(waitingOnApply);
                waitingOnApply.unset(index);
                return true;
            }

            public boolean removeWaitingOn(TxnId txnId)
            {
                return removeWaitingOnCommit(txnId) || removeWaitingOnApply(txnId);
            }

            public boolean removeInvalidatedOrTruncated(TxnId txnId)
            {
                int index = this.deps.indexOf(txnId);
                return setAppliedOrInvalidated(index);
            }

            public boolean isEmpty()
            {
                return waitingOnApply.isEmpty() && waitingOnCommit.isEmpty();
            }

            public boolean removeApplied(TxnId txnId, WaitingOn propagate)
            {
                int index = this.deps.indexOf(txnId);
                if (!setAppliedOrInvalidated(index))
                    return false;

                if (!propagate.appliedOrInvalidated.isEmpty())
                {
                    forEachIntersection(propagate.deps.keyDeps.txnIds(), deps.keyDeps.txnIds(),
                                        (from, to, ignore, i1, i2) -> {
                                            if (from.get(i1))
                                                to.setAppliedOrInvalidated(i2);
                                        }, propagate.appliedOrInvalidated, this, null);

                    forEachIntersection(propagate.deps.rangeDeps.txnIds(), deps.rangeDeps.txnIds(),
                                        (from, to, ignore, i1, i2) -> {
                                            if (from.isAppliedOrInvalidatedRangeIdx(i1))
                                                to.setAppliedOrInvalidatedRangeIdx(i2);
                                        }, propagate, this, null);
                }

                return true;
            }

            public TxnId minWaitingOnTxnId()
            {
                return WaitingOn.minWaitingOnTxnId(deps, waitingOnCommit, waitingOnApply);
            }

            boolean setAppliedOrInvalidatedRangeIdx(int i)
            {
                return setAppliedOrInvalidated(i + deps.keyDeps.txnIdCount());
            }

            boolean setAppliedOrInvalidated(int i)
            {
                if (appliedOrInvalidated.get(i))
                    return false;

                if (waitingOnCommit.get(i))
                {
                    waitingOnCommit = ensureMutable(waitingOnCommit);
                    waitingOnCommit.unset(i);
                }
                else if (waitingOnApply.get(i))
                {
                    waitingOnApply = ensureMutable(waitingOnApply);
                    waitingOnApply.unset(i);
                }
                else
                {
                    return false;
                }

                appliedOrInvalidated = ensureMutable(appliedOrInvalidated);
                appliedOrInvalidated.set(i);

                return true;
            }

            public <P1, P2, P3, P4> void forEachWaitingOnCommit(P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
            {
                waitingOnCommit.reverseForEach(p1, p2, p3, p4, forEach);
            }

            public <P1, P2, P3, P4> void forEachWaitingOnApply(P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
            {
                waitingOnApply.reverseForEach(p1, p2, p3, p4, forEach);
            }

            public WaitingOn build()
            {
                return new WaitingOn(deps, ensureImmutable(waitingOnCommit), ensureImmutable(waitingOnApply), ensureImmutable(appliedOrInvalidated));
            }

            @Override
            public String toString()
            {
                return WaitingOn.toString(deps, waitingOnCommit, waitingOnApply);
            }
        }
    }

    static Command addListener(Command command, DurableAndIdempotentListener listener)
    {
        CommonAttributes attrs = command.mutable().addListener(listener);
        return command.updateAttributes(attrs);
    }

    static Command removeListener(Command command, Listener listener)
    {
        CommonAttributes attrs = command.mutable().removeListener(listener);
        return command.updateAttributes(attrs);
    }

    static Command.Committed updateWaitingOn(Committed command, WaitingOn.Update waitingOn)
    {
        if (!waitingOn.hasChanges())
            return command;

        return command instanceof Command.Executed ?
                Command.Executed.executed(command.asExecuted(), command, waitingOn.build()) :
                Command.Committed.committed(command, command, waitingOn.build());
    }

    static Command.PreAccepted preaccept(Command command, CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        if (command.status() == Status.NotDefined)
        {
            return Command.PreAccepted.preAccepted(attrs, executeAt, ballot);
        }
        else if (command.status() == Status.AcceptedInvalidate && command.executeAt() == null)
        {
            Command.Accepted accepted = command.asAccepted();
            return Command.Accepted.accepted(attrs, accepted.saveStatus(), executeAt, ballot, accepted.accepted());
        }
        else
        {
            Invariants.checkState(command.status() == Status.Accepted);
            return (Command.PreAccepted) command.updateAttributes(attrs, ballot);
        }
    }

    static Command.Accepted markDefined(Command command, CommonAttributes attributes, Ballot promised)
    {
        if (Command.isSameClass(command, Command.Accepted.class))
            return Command.Accepted.accepted(command.asAccepted(), attributes, SaveStatus.enrich(command.saveStatus(), Status.Known.DefinitionOnly), promised);
        return (Command.Accepted) command.updateAttributes(attributes, promised);
    }

    static Command.Accepted accept(Command command, CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return new Command.Accepted(attrs, SaveStatus.get(Status.Accepted, command.known()), executeAt, ballot, ballot);
    }

    static Command.Accepted acceptInvalidated(Command command, Ballot ballot)
    {
        Timestamp executeAt = command.isDefined() ? command.asWitnessed().executeAt() : null;
        return new Command.Accepted(command, SaveStatus.get(Status.AcceptedInvalidate, command.known()), executeAt, ballot, ballot);
    }

    static Command.Committed commit(Command command, CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn)
    {
        return Command.Committed.committed(attrs, SaveStatus.get(Status.Committed, command.known()), executeAt, command.promised(), command.accepted(), waitingOn);
    }

    static Command precommit(Command command, Timestamp executeAt)
    {
        return new Command.Accepted(command, SaveStatus.get(Status.PreCommitted, command.known()), executeAt, command.promised(), command.accepted());
    }

    static Command.Committed readyToExecute(Command.Committed command)
    {
        return Command.Committed.committed(command, command, SaveStatus.ReadyToExecute);
    }

    static Command.Executed preapplied(Command command, CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return Command.Executed.executed(attrs, SaveStatus.get(Status.PreApplied, command.known()), executeAt, command.promised(), command.accepted(), waitingOn, writes, result);
    }

    static Command.Executed applying(Command.Executed command)
    {
        return Command.Executed.executed(command, command, SaveStatus.get(Status.Applying, command.known()));
    }

    static Command.Executed applied(Command.Executed command)
    {
        return Command.Executed.executed(command, command, SaveStatus.get(Status.Applied, command.known()));
    }
}
