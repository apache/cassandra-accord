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

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;

import accord.api.Read;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.UnresolvedData;
import accord.api.VisibleForImplementation;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.Utils;
import accord.utils.async.AsyncChain;

import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.NotDurable;
import static accord.local.Status.Known.DefinitionOnly;
import static accord.utils.Utils.ensureSortedImmutable;
import static accord.utils.Utils.ensureSortedMutable;
import static java.lang.String.format;

public abstract class Command implements CommonAttributes
{
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
        public static NotWitnessed notWitnessed(CommonAttributes attributes, Ballot promised)
        {
            return NotWitnessed.notWitnessed(attributes, promised);
        }

        public static PreAccepted preaccepted(CommonAttributes common, Timestamp executeAt, Ballot promised)
        {
            return PreAccepted.preAccepted(common, executeAt, promised);
        }

        public static Accepted accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            return Accepted.accepted(common, status, executeAt, promised, accepted);
        }

        public static Committed committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            return Committed.committed(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
        }

        public static Executed executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            return Executed.executed(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply, writes, result);
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
            case NotWitnessed:
                return validateCommandClass(status, NotWitnessed.class, klass);
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
            case Applied:
            case Invalidated:
                return validateCommandClass(status, Executed.class, klass);
            default:
                throw new IllegalStateException("Unhandled status " + status);
        }
    }

    public static class Listener implements CommandListener
    {
        protected final TxnId listenerId;

        public Listener(TxnId listenerId)
        {
            this.listenerId = listenerId;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Listener that = (Listener) o;
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
            Commands.listenerUpdate(safeStore, safeStore.command(listenerId), safeCommand);
        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(Utils.listOf(listenerId, caller), Keys.EMPTY);
        }
    }

    private abstract static class AbstractCommand extends Command
    {
        private final TxnId txnId;
        private final SaveStatus status;
        private final Status.Durability durability;
        private final RoutingKey homeKey;
        private final RoutingKey progressKey;
        private final Route<?> route;
        private final Ballot promised;
        private final Listeners.Immutable listeners;

        private AbstractCommand(TxnId txnId, SaveStatus status, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Route<?> route, Ballot promised, Listeners.Immutable listeners)
        {
            this.txnId = txnId;
            this.status = validateCommandClass(status, getClass());
            this.durability = durability;
            this.homeKey = homeKey;
            this.progressKey = progressKey;
            this.route = route;
            this.promised = promised;
            this.listeners = listeners;
        }

        private AbstractCommand(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            this.txnId = common.txnId();
            this.status = validateCommandClass(status, getClass());
            this.durability = common.durability();
            this.homeKey = common.homeKey();
            this.progressKey = common.progressKey();
            this.route = common.route();
            this.promised = promised;
            this.listeners = common.listeners();
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
                    && Objects.equals(homeKey, command.homeKey())
                    && Objects.equals(progressKey, command.progressKey())
                    && Objects.equals(route, command.route())
                    && Objects.equals(promised, command.promised())
                    && listeners.equals(command.listeners());
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
        public final RoutingKey homeKey()
        {
            return homeKey;
        }

        @Override
        public final RoutingKey progressKey()
        {
            return progressKey;
        }

        @Override
        public final Route<?> route()
        {
            return route;
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
        public Listeners.Immutable listeners()
        {
            if (listeners == null)
                return Listeners.Immutable.EMPTY;
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
     * If this is the home shard, we require that this is a Route for all states &gt; NotWitnessed;
     * otherwise for the local progress shard this is ordinarily a PartialRoute, and for other shards this is not set,
     * so that there is only one copy per node that can be consulted to construct the full set of involved keys.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     */
    public abstract Route<?> route();

    /**
     * A key nominated to be the primary shard within this node for managing progress of the command.
     * It is nominated only as of txnId.epoch, and may be null (indicating that this node does not monitor
     * the progress of this command).
     *
     * Preferentially, this is homeKey on nodes that replicate it, and otherwise any key that is replicated, as of txnId.epoch
     */
    public abstract RoutingKey progressKey();

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     *
     * TODO (expected, efficiency): we probably do not want to save this on its own, as we probably want to
     *  minimize IO interactions and discrete registers, so will likely reference commit log entries directly
     *  At which point we may impose a requirement that only a Route can be saved, not a homeKey on its own.
     *  Once this restriction is imposed, we no longer need to pass around Routable.Domain with TxnId.
     */
    public abstract RoutingKey homeKey();
    public abstract TxnId txnId();
    public abstract Ballot promised();
    public abstract Status.Durability durability();
    public abstract Listeners.Immutable listeners();
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
    public final Route<?> someRoute()
    {
        if (route() != null)
            return route();

        if (homeKey() != null)
            return PartialRoute.empty(txnId().domain(), homeKey());

        return null;
    }

    public Unseekables<?, ?> maxUnseekables()
    {
        Route<?> route = someRoute();
        if (route == null)
            return null;

        return route.toMaximalUnseekables();
    }

    public PreLoadContext contextForSelf()
    {
        return contextForCommand(this);
    }

    public abstract Timestamp executeAt();
    public abstract Ballot accepted();
    public abstract PartialTxn partialTxn();
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

    public final CommandListener asListener()
    {
        return new Listener(txnId());
    }

    public final boolean isWitnessed()
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
        boolean result = status().hasBeen(Status.PreApplied);
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

    public static final class NotWitnessed extends AbstractCommand
    {
        NotWitnessed(TxnId txnId, SaveStatus status, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Route<?> route, Ballot promised, Listeners.Immutable listeners)
        {
            super(txnId, status, durability, homeKey, progressKey, route, promised, listeners);
        }

        NotWitnessed(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            super(common, status, promised);
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new NotWitnessed(attrs, saveStatus(), promised);
        }

        public static NotWitnessed notWitnessed(CommonAttributes common, Ballot promised)
        {
            return new NotWitnessed(common, SaveStatus.NotWitnessed, promised);
        }

        public static NotWitnessed notWitnessed(TxnId txnId)
        {
            return new NotWitnessed(txnId, SaveStatus.NotWitnessed, NotDurable, null, null, null, Ballot.ZERO, null);
        }

        public static NotWitnessed notWitnessed(NotWitnessed command, CommonAttributes common, Ballot promised)
        {
            checkSameClass(command, NotWitnessed.class, "Cannot update");
            Invariants.checkArgument(command.txnId().equals(common.txnId()));
            return new NotWitnessed(common, command.saveStatus(), promised);
        }

        @Override
        public Timestamp executeAt()
        {
            return null;
        }

        @Override
        public Ballot promised()
        {
            return Ballot.ZERO;
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
        private final ImmutableSortedSet<TxnId> waitingOnCommit;
        private final ImmutableSortedMap<Timestamp, TxnId> waitingOnApply;

        private Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            super(common, status, executeAt, promised, accepted);
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
        }

        private Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            this(common, status, executeAt, promised, accepted, waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new Committed(attrs, saveStatus(), executeAt(), promised, accepted(), waitingOnCommit(), waitingOnApply());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Committed committed = (Committed) o;
            return Objects.equals(waitingOnCommit, committed.waitingOnCommit)
                    && Objects.equals(waitingOnApply, committed.waitingOnApply);
        }

        private static Committed committed(Committed command, CommonAttributes common, Ballot promised, SaveStatus status, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            checkPromised(command, promised);
            checkSameClass(command, Committed.class, "Cannot update");
            return new Committed(common, status, command.executeAt(), promised, command.accepted(), waitingOnCommit, waitingOnApply);
        }

        static Committed committed(Committed command, CommonAttributes common, Ballot promised)
        {
            return committed(command, common, promised, command.saveStatus(), command.waitingOnCommit(), command.waitingOnApply());
        }

        static Committed committed(Committed command, CommonAttributes common, SaveStatus status)
        {
            return committed(command, common, command.promised(), status, command.waitingOnCommit(), command.waitingOnApply());
        }

        static Committed committed(Committed command, CommonAttributes common, WaitingOn waitingOn)
        {
            return committed(command, common, command.promised(), command.saveStatus(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        static Committed committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            return new Committed(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
        }

        public AsyncChain<UnresolvedData> read(SafeCommandStore safeStore, @Nullable RoutingKeys dataReadKeys, @Nullable Read followupRead)
        {
            return partialTxn().read(safeStore, dataReadKeys, followupRead, this);
        }

        public WaitingOn waitingOn()
        {
            return new WaitingOn(waitingOnCommit, waitingOnApply);
        }

        public ImmutableSortedSet<TxnId> waitingOnCommit()
        {
            return waitingOnCommit;
        }

        public boolean isWaitingOnCommit()
        {
            return waitingOnCommit != null && !waitingOnCommit.isEmpty();
        }

        public TxnId firstWaitingOnCommit()
        {
            return isWaitingOnCommit() ? waitingOnCommit.first() : null;
        }

        public ImmutableSortedMap<Timestamp, TxnId> waitingOnApply()
        {
            return waitingOnApply;
        }

        public boolean isWaitingOnApply()
        {
            return waitingOnApply != null && !waitingOnApply.isEmpty();
        }

        public TxnId firstWaitingOnApply()
        {
            return isWaitingOnApply() ? waitingOnApply.firstEntry().getValue() : null;
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

        public Executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            super(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply);
            this.writes = writes;
            this.result = result;
        }

        public Executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            super(common, status, executeAt, promised, accepted, waitingOn);
            this.writes = writes;
            this.result = result;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new Executed(attrs, saveStatus(), executeAt(), promised, accepted(), waitingOnCommit(), waitingOnApply(), writes, result);
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

        public static Executed executed(Executed command, CommonAttributes common, SaveStatus status, Ballot promised, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            checkSameClass(command, Executed.class, "Cannot update");
            return new Executed(common, status, command.executeAt(), promised, command.accepted(), waitingOnCommit, waitingOnApply, command.writes(), command.result());
        }

        public static Executed executed(Executed command, CommonAttributes common, SaveStatus status)
        {
            return executed(command, common, status, command.promised(), command.waitingOnCommit(), command.waitingOnApply());
        }

        public static Executed executed(Executed command, CommonAttributes common, WaitingOn waitingOn)
        {
            return executed(command, common, command.saveStatus(), command.promised(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
        }

        public static Executed executed(Executed command, CommonAttributes common, Ballot promised)
        {
            return executed(command, common, command.saveStatus(), promised, command.waitingOnCommit(), command.waitingOnApply());
        }

        public static Executed executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply, Writes writes, Result result)
        {
            return new Executed(common, status, executeAt, promised, accepted, waitingOnCommit, waitingOnApply, writes, result);
        }

        public static Executed executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            return new Executed(common, status, executeAt, promised, accepted, waitingOn.waitingOnCommit, waitingOn.waitingOnApply, writes, result);
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
        public static final WaitingOn EMPTY = new WaitingOn(ImmutableSortedSet.of(), ImmutableSortedMap.of());
        public final ImmutableSortedSet<TxnId> waitingOnCommit;
        public final ImmutableSortedMap<Timestamp, TxnId> waitingOnApply;

        public WaitingOn(ImmutableSortedSet<TxnId> waitingOnCommit, ImmutableSortedMap<Timestamp, TxnId> waitingOnApply)
        {
            this.waitingOnCommit = waitingOnCommit;
            this.waitingOnApply = waitingOnApply;
        }

        public static class Update
        {
            private boolean hasChanges = false;
            private NavigableSet<TxnId> waitingOnCommit;
            private NavigableMap<Timestamp, TxnId> waitingOnApply;

            public Update()
            {

            }

            public Update(WaitingOn waitingOn)
            {
                this.waitingOnCommit = waitingOn.waitingOnCommit;
                this.waitingOnApply = waitingOn.waitingOnApply;
            }

            public Update(Committed committed)
            {
                this.waitingOnCommit = committed.waitingOnCommit();
                this.waitingOnApply = committed.waitingOnApply();
            }

            public boolean hasChanges()
            {
                return hasChanges;
            }

            public void addWaitingOnCommit(TxnId txnId)
            {
                waitingOnCommit = ensureSortedMutable(waitingOnCommit);
                waitingOnCommit.add(txnId);
                hasChanges = true;
            }

            public void removeWaitingOnCommit(TxnId txnId)
            {
                if (waitingOnApply == null)
                    return;
                waitingOnCommit = ensureSortedMutable(waitingOnCommit);
                waitingOnCommit.remove(txnId);
                hasChanges = true;
            }

            public void addWaitingOnApply(TxnId txnId, Timestamp executeAt)
            {
                waitingOnApply = ensureSortedMutable(waitingOnApply);
                waitingOnApply.put(executeAt, txnId);
                hasChanges = true;
            }

            public void removeWaitingOnApply(TxnId txnId, Timestamp executeAt)
            {
                if (waitingOnApply == null)
                    return;
                waitingOnApply = ensureSortedMutable(waitingOnApply);
                waitingOnApply.remove(executeAt);
                hasChanges = true;
            }

            public void removeWaitingOn(TxnId txnId, Timestamp executeAt)
            {
                removeWaitingOnCommit(txnId);
                removeWaitingOnApply(txnId, executeAt);
                hasChanges = true;
            }

            public WaitingOn build()
            {
                if ((waitingOnCommit == null || waitingOnCommit.isEmpty()) && (waitingOnApply == null || waitingOnApply.isEmpty()))
                    return EMPTY;
                return new WaitingOn(ensureSortedImmutable(waitingOnCommit), ensureSortedImmutable(waitingOnApply));
            }
        }
    }

    static Command addListener(Command command, CommandListener listener)
    {
        CommonAttributes attrs = command.mutable().addListener(listener);
        return command.updateAttributes(attrs);
    }

    static Command removeListener(Command command, CommandListener listener)
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
        if (command.status() == Status.NotWitnessed)
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
            return Command.Accepted.accepted(command.asAccepted(), attributes, SaveStatus.enrich(command.saveStatus(), DefinitionOnly), promised);
        return (Command.Accepted) command.updateAttributes(attributes, promised);
    }

    static Command.Accepted accept(Command command, CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return new Command.Accepted(attrs, SaveStatus.get(Status.Accepted, command.known()), executeAt, ballot, ballot);
    }

    static Command.Accepted acceptInvalidated(Command command, Ballot ballot)
    {
        Timestamp executeAt = command.isWitnessed() ? command.asWitnessed().executeAt() : null;
        return new Command.Accepted(command, SaveStatus.get(Status.AcceptedInvalidate, command.known()), executeAt, ballot, ballot);
    }

    static Command.Committed commit(Command command, CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn)
    {
        return Command.Committed.committed(attrs, SaveStatus.get(Status.Committed, command.known()), executeAt, command.promised(), command.accepted(), waitingOn.waitingOnCommit, waitingOn.waitingOnApply);
    }

    static Command precommit(Command command, Timestamp executeAt)
    {
        return new Command.Accepted(command, SaveStatus.get(Status.PreCommitted, command.known()), executeAt, command.promised(), command.accepted());
    }

    static Command.Committed commitInvalidated(Command command, CommonAttributes attrs, Timestamp executeAt)
    {
        return Command.Executed.executed(attrs, SaveStatus.get(Status.Invalidated, command.known()), executeAt, command.promised(), command.accepted(), Command.WaitingOn.EMPTY, null, null);
    }

    static Command.Committed readyToExecute(Command.Committed command)
    {
        return Command.Committed.committed(command, command, SaveStatus.ReadyToExecute);
    }

    static Command.Executed preapplied(Command command, CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return Command.Executed.executed(attrs, SaveStatus.get(Status.PreApplied, command.known()), executeAt, command.promised(), command.accepted(), waitingOn, writes, result);
    }

    static Command.Executed applied(Command.Executed command)
    {
        return Command.Executed.executed(command, command, SaveStatus.get(Status.Applied, command.known()));
    }
}
