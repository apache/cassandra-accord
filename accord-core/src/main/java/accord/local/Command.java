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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import accord.api.Key;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.local.Status.Durability;
import accord.local.Status.Known;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.ImmutableBitSet;
import accord.utils.IndexedQuadConsumer;
import accord.utils.IndexedTriConsumer;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;

import javax.annotation.Nullable;

import static accord.local.Command.AbstractCommand.validate;
import static accord.local.Listeners.Immutable.EMPTY;
import static accord.local.SaveStatus.Uninitialised;
import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.NotDurable;
import static accord.local.Status.Durability.ShardUniversal;
import static accord.local.Status.Durability.UniversalOrInvalidated;
import static accord.local.Status.Invalidated;
import static accord.local.Status.KnownExecuteAt.ExecuteAtKnown;
import static accord.local.Status.Stable;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Invariants.illegalState;
import static accord.utils.SortedArrays.forEachIntersection;
import static accord.utils.Utils.ensureImmutable;
import static accord.utils.Utils.ensureMutable;
import static java.lang.String.format;

public abstract class Command implements CommonAttributes
{
    public interface Listener
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

    private static Durability durability(Durability durability, SaveStatus status)
    {
        if (status.compareTo(SaveStatus.PreApplied) >= 0 && !status.hasBeen(Invalidated) && durability == NotDurable)
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

        public static Truncated invalidated(TxnId txnId, Listeners.Immutable durableListeners)
        {
            return Truncated.invalidated(txnId, durableListeners);
        }

        public static Truncated truncatedApply(CommonAttributes common, SaveStatus saveStatus, Timestamp executeAt, Writes writes, Result result)
        {
            return Truncated.truncatedApply(common, saveStatus, executeAt, writes, result);
        }

        public static Truncated truncatedApply(CommonAttributes common, SaveStatus saveStatus, Timestamp executeAt, Writes writes, Result result, Timestamp executesAtLeast)
        {
            return Truncated.truncatedApply(common, saveStatus, executeAt, writes, result, executesAtLeast);
        }
    }

    private static SaveStatus validateCommandClass(SaveStatus status, Class<?> expected, Class<?> actual)
    {
        if (actual != expected)
        {
            throw illegalState(format("Cannot instantiate %s for status %s. %s expected",
                                                   actual.getSimpleName(), status, expected.getSimpleName()));
        }
        return status;
    }

    private static SaveStatus validateCommandClass(TxnId txnId, SaveStatus status, Class<?> klass)
    {
        switch (status)
        {
            case Uninitialised:
            case NotDefined:
                return validateCommandClass(status, NotDefined.class, klass);
            case PreAccepted:
                return validateCommandClass(status, PreAccepted.class, klass);
            case Accepted:
            case AcceptedWithDefinition:
            case AcceptedInvalidate:
            case AcceptedInvalidateWithDefinition:
            case PreCommitted:
            case PreCommittedWithAcceptedDeps:
            case PreCommittedWithDefinition:
            case PreCommittedWithDefinitionAndAcceptedDeps:
                return validateCommandClass(status, Accepted.class, klass);
            case Committed:
            case ReadyToExecute:
            case Stable:
                return validateCommandClass(status, Committed.class, klass);
            case PreApplied:
            case Applying:
            case Applied:
                return validateCommandClass(status, Executed.class, klass);
            case TruncatedApply:
            case TruncatedApplyWithDeps:
            case TruncatedApplyWithOutcome:
                if (txnId.kind().awaitsOnlyDeps())
                    return validateCommandClass(status, TruncatedAwaitsOnlyDeps.class, klass);
            case Erased:
            case ErasedOrInvalidated:
            case Invalidated:
                return validateCommandClass(status, Truncated.class, klass);
            default:
                throw illegalState("Unhandled status " + status);
        }
    }

    abstract static class AbstractCommand extends Command
    {
        private final TxnId txnId;
        private final SaveStatus status;
        private final Durability durability;
        private final @Nullable Route<?> route;
        // TODO (expected): more consistent handling of keys for transactions that only *may* intersect a command store, as have both additionalKeysOrRanges AND forLocalEpoch to separately handle the problem
        private final @Nullable Seekables<?, ?> additionalKeysOrRanges;
        private final Ballot promised;
        private final Listeners.Immutable listeners;

        private AbstractCommand(TxnId txnId, SaveStatus status, Durability durability, @Nullable Route<?> route, @Nullable Seekables<?, ?> additionalKeysOrRanges, Ballot promised, Listeners.Immutable listeners)
        {
            this.txnId = txnId;
            this.status = validateCommandClass(txnId, status, getClass());
            this.durability = durability;
            this.route = route;
            this.additionalKeysOrRanges = additionalKeysOrRanges;
            this.promised = promised;
            this.listeners = listeners;
        }

        private AbstractCommand(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            this.txnId = common.txnId();
            this.status = validateCommandClass(txnId, status, getClass());
            this.durability = common.durability();
            this.route = common.route();
            this.additionalKeysOrRanges = common.additionalKeysOrRanges();
            this.promised = promised;
            this.listeners = common.durableListeners();
        }


        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Command command = (Command) o;
            return txnId().equals(command.txnId())
                    && saveStatus() == command.saveStatus()
                    && durability() == command.durability()
                    && Objects.equals(route(), command.route())
                    && Objects.equals(additionalKeysOrRanges, command.additionalKeysOrRanges())
                    && Objects.equals(promised(), command.promised())
                    && Objects.equals(durableListeners(), command.durableListeners());
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
        public Ballot promised()
        {
            return promised;
        }

        @Override
        public Durability durability()
        {
            return Command.durability(durability, saveStatus());
        }

        @Override
        public Listeners.Immutable durableListeners()
        {
            return listeners == null ? EMPTY : listeners;
        }

        @Override
        public final SaveStatus saveStatus()
        {
            return status;
        }

        @Override
        public @Nullable Seekables<?, ?> additionalKeysOrRanges()
        {
            return additionalKeysOrRanges;
        }

        public static <T extends AbstractCommand> T validate(T validate)
        {
            Known known = validate.known();
            switch (known.route)
            {
                default: throw new AssertionError("Unhandled KnownRoute: " + known.route);
                case Maybe: break;
                case Full: Invariants.checkState(Route.isFullRoute(validate.route())); break;
                case Covering: Invariants.checkState(Route.isRoute(validate.route())); break;
            }
            {
                PartialTxn partialTxn = validate.partialTxn();
                switch (known.definition)
                {
                    default: throw new AssertionError("Unhandled Definition: " + known.definition);
                    case DefinitionErased:
                    case DefinitionUnknown:
                    case NoOp:
                        Invariants.checkState(partialTxn == null, "partialTxn is defined");
                        break;
                    case DefinitionKnown:
                        Invariants.checkState(partialTxn != null, "partialTxn is null");
                        break;
                }
            }
            {
                Timestamp executeAt = validate.executeAt();
                switch (known.executeAt)
                {
                    default: throw new AssertionError("Unhandled KnownExecuteAt: " + known.executeAt);
                    case ExecuteAtErased:
                    case ExecuteAtUnknown:
                        break;
                    case ExecuteAtProposed:
                    case ExecuteAtKnown:
                        Invariants.checkState(executeAt != null && executeAt.compareTo(validate.txnId()) >= 0);
                        break;
                    case NoExecuteAt:
                        Invariants.checkState(executeAt.equals(Timestamp.NONE));
                        break;
                }
            }
            {
                PartialDeps deps = validate.partialDeps();
                switch (known.deps)
                {
                    default: throw new AssertionError("Unhandled KnownDeps: " + known.deps);
                    case DepsUnknown:
                    case DepsErased:
                    case NoDeps:
                        Invariants.checkState(deps == null);
                        break;
                    case DepsProposed:
                    case DepsCommitted:
                    case DepsKnown:
                        Invariants.checkState(deps != null);
                        break;
                }
            }
            {
                Writes writes = validate.writes();
                Result result = validate.result();
                switch (known.outcome)
                {
                    default: throw new AssertionError("Unhandled Outcome: " + known.outcome);
                    case Apply:
                        Invariants.checkState(writes != null, "Writes is null");
                        Invariants.checkState(result != null, "Result is null");
                        break;
                    case Invalidated:
                        Invariants.checkState(validate.durability().isMaybeInvalidated());
                    case Unknown:
                        Invariants.checkState(validate.durability() != Local);
                    case Erased:
                    case WasApply:
                        Invariants.checkState(writes == null, "Writes exist");
                        Invariants.checkState(result == null, "Results exist");
                        break;
                }
            }
            switch (validate.saveStatus().execution)
            {
                case NotReady:
                case CleaningUp:
                    break;
                case ReadyToExclude:
                    Invariants.checkState(validate.saveStatus() != SaveStatus.Committed || validate.asCommitted().waitingOn == null);
                    break;
                case WaitingToExecute:
                case ReadyToExecute:
                case Applied:
                case Applying:
                case WaitingToApply:
                    Invariants.checkState(validate.asCommitted().waitingOn != null);
                    break;
            }
            return validate;
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
     * TODO (required): audit uses; do not assume non-null means it is a complete route for the shard;
     *    preferably introduce two variations so callers can declare whether they need the full shard's route
     *    or any route will do
     */
    @Override
    @Nullable
    public abstract Route<?> route();

    /**
     * The command may have an incomplete route when this is false
     */
    public boolean hasFullRoute()
    {
        return route() != null && route().kind().isFullRoute();
    }

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     */
    @Nullable
    public RoutingKey homeKey()
    {
        Route<?> route = route();
        return route == null ? null : route.homeKey();
    }

    @Override
    public abstract TxnId txnId();
    public abstract Ballot promised();
    @Override
    public abstract Durability durability();
    @Override
    public abstract Listeners.Immutable<DurableAndIdempotentListener> durableListeners();
    public abstract SaveStatus saveStatus();

    /**
     * Only meaningful when txnId.kind().awaitsOnlyDeps()
     */
    public Timestamp executesAtLeast() { return executeAt(); }

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
        checkNewBallot(command.acceptedOrCommitted(), ballot, "accepted");
    }

    private static void checkSameClass(Command command, Class<? extends Command> klass, String errorMsg)
    {
        if (!isSameClass(command, klass))
            throw illegalArgument(errorMsg + format(" expected %s got %s", klass.getSimpleName(), command.getClass().getSimpleName()));
    }

    public abstract Timestamp executeAt();
    public abstract Ballot acceptedOrCommitted();

    @Override
    public abstract PartialTxn partialTxn();
    public abstract Seekables<?, ?> keysOrRanges();

    @Override
    public abstract @Nullable PartialDeps partialDeps();

    public @Nullable Writes writes() { return null; }
    public @Nullable Result result() { return null; }

    public final Timestamp executeAtIfKnownElseTxnId()
    {
        if (known().executeAt == ExecuteAtKnown)
            return executeAt();
        return txnId();
    }

    public final Timestamp executeAtIfKnown()
    {
        return executeAtIfKnown(null);
    }

    public final Timestamp executeAtIfKnown(Timestamp orElse)
    {
        if (known().executeAt == ExecuteAtKnown)
            return executeAt();
        return orElse;
    }

    public final boolean executesInFutureEpoch()
    {
        return known().executeAt == ExecuteAtKnown && executeAt().epoch() > txnId().epoch();
    }

    public final Timestamp executeAtOrTxnId()
    {
        Timestamp executeAt = executeAt();
        return executeAt == null || executeAt.equals(Timestamp.NONE) ? txnId() : executeAt;
    }

    public final Timestamp executeAtIfKnownOrTxnId()
    {
        Timestamp executeAt = executeAtIfKnown();
        return executeAt == null || executeAt.equals(Timestamp.NONE) ? txnId() : executeAt;
    }

    public final Status status()
    {
        return saveStatus().status;
    }

    public final Known known()
    {
        return saveStatus().known;
    }

    public final boolean hasBeen(Status status)
    {
        return status().compareTo(status) >= 0;
    }

    public boolean has(Known known)
    {
        return known.isSatisfiedBy(saveStatus().known);
    }

    public boolean isAtLeast(SaveStatus.LocalExecution execution)
    {
        return saveStatus().execution.compareTo(execution) >= 0;
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
        if (status().hasBeen(Status.Truncated))
            return false;
        return status().hasBeen(Status.PreAccepted);
    }

    public final PreAccepted asWitnessed()
    {
        return Invariants.cast(this, PreAccepted.class);
    }

    public final boolean isAccepted()
    {
        return status().hasBeen(Status.AcceptedInvalidate);
    }

    public final Accepted asAccepted()
    {
        return Invariants.cast(this, Accepted.class);
    }

    public final boolean isCommitted()
    {
        SaveStatus saveStatus = saveStatus();
        return saveStatus.hasBeen(Status.Committed) && !saveStatus.hasBeen(Invalidated);
    }

    public final boolean isStable()
    {
        SaveStatus saveStatus = saveStatus();
        return isStable(saveStatus);
    }

    public static boolean isStable(SaveStatus saveStatus)
    {
        return saveStatus.hasBeen(Status.Stable) && !saveStatus.hasBeen(Invalidated);
    }

    public final Committed asCommitted()
    {
        return Invariants.cast(this, Committed.class);
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
        NotDefined(TxnId txnId, SaveStatus status, Durability durability, @Nullable Route<?> route, @Nullable Seekables<?, ?> additionalKeysOrRanges, Ballot promised, Listeners.Immutable listeners)
        {
            super(txnId, status, durability, route, additionalKeysOrRanges, promised, listeners);
        }

        NotDefined(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            super(common, status, promised);
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return validate(new NotDefined(attrs, initialise(saveStatus()), promised));
        }

        public static NotDefined notDefined(CommonAttributes common, Ballot promised)
        {
            return validate(new NotDefined(common, SaveStatus.NotDefined, promised));
        }

        public static NotDefined uninitialised(TxnId txnId)
        {
            return validate(new NotDefined(txnId, Uninitialised, NotDurable, null, null, Ballot.ZERO, null));
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
        public Ballot acceptedOrCommitted()
        {
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return null;
        }

        @Override
        public Seekables<?, ?> keysOrRanges()
        {
            return additionalKeysOrRanges();
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

    public static class Truncated extends AbstractCommand
    {
        @Nullable final Timestamp executeAt;
        @Nullable final Writes writes;
        @Nullable final Result result;

        public Truncated(CommonAttributes commonAttributes, SaveStatus saveStatus, @Nullable Timestamp executeAt, @Nullable Writes writes, @Nullable Result result)
        {
            super(commonAttributes, saveStatus, Ballot.MAX);
            this.executeAt = executeAt;
            this.writes = writes;
            this.result = result;
        }

        public Truncated(TxnId txnId, SaveStatus saveStatus, Durability durability, @Nullable Route<?> route, @Nullable Timestamp executeAt, Listeners.Immutable listeners, @Nullable Writes writes, @Nullable Result result)
        {
            super(txnId, saveStatus, durability, route, null, Ballot.MAX, listeners);
            this.executeAt = executeAt;
            this.writes = writes;
            this.result = result;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Truncated that = (Truncated) o;
            return Objects.equals(executeAt, that.executeAt)
                && Objects.equals(writes, that.writes)
                && Objects.equals(result, that.result);
        }

        public static Truncated erased(Command command)
        {
            Durability durability = Durability.mergeAtLeast(command.durability(), UniversalOrInvalidated);
            return erased(command.txnId(), durability, command.route());
        }

        public static Truncated erasedOrInvalidated(TxnId txnId, Status.Durability durability, Route<?> route)
        {
            return validate(new Truncated(txnId, SaveStatus.ErasedOrInvalidated, durability, route, null, EMPTY, null, null));
        }

        public static Truncated erased(TxnId txnId, Status.Durability durability, Route<?> route)
        {
            return validate(new Truncated(txnId, SaveStatus.Erased, durability, route, null, EMPTY, null, null));
        }

        public static Truncated truncatedApply(Command command)
        {
            return truncatedApply(command, null);
        }

        public static Truncated truncatedApply(Command command, @Nullable FullRoute<?> route)
        {
            Invariants.checkArgument(command.known().executeAt.isDecidedAndKnownToExecute());
            if (route == null) route = Route.castToNonNullFullRoute(command.route());
            Durability durability = Durability.mergeAtLeast(command.durability(), ShardUniversal);
            if (command.txnId().kind().awaitsOnlyDeps())
            {
                Timestamp executesAtLeast = command.hasBeen(Stable) ? command.executesAtLeast() : null;
                return validate(new TruncatedAwaitsOnlyDeps(command.txnId(), SaveStatus.TruncatedApply, durability, route, command.executeAt(), EMPTY, null, null, executesAtLeast));
            }
            return validate(new Truncated(command.txnId(), SaveStatus.TruncatedApply, durability, route, command.executeAt(), EMPTY, null, null));
        }

        public static Truncated truncatedApplyWithOutcome(Executed command)
        {
            Durability durability = Durability.mergeAtLeast(command.durability(), ShardUniversal);
            if (command.txnId().kind().awaitsOnlyDeps())
                return validate(new TruncatedAwaitsOnlyDeps(command.txnId(), SaveStatus.TruncatedApplyWithOutcome, durability, command.route(), command.executeAt(), EMPTY, command.writes, command.result, command.executesAtLeast()));
            return validate(new Truncated(command.txnId(), SaveStatus.TruncatedApplyWithOutcome, durability, command.route(), command.executeAt(), EMPTY, command.writes, command.result));
        }

        public static Truncated truncatedApply(CommonAttributes common, SaveStatus saveStatus, Timestamp executeAt, Writes writes, Result result)
        {
            Invariants.checkArgument(!common.txnId().kind().awaitsOnlyDeps());
            Durability durability = checkTruncatedApplyInvariants(common, saveStatus, executeAt);
            return validate(new Truncated(common.txnId(), saveStatus, durability, common.route(), executeAt, EMPTY, writes, result));
        }

        public static Truncated truncatedApply(CommonAttributes common, SaveStatus saveStatus, Timestamp executeAt, Writes writes, Result result, @Nullable Timestamp dependencyExecutesAt)
        {
            if (!common.txnId().kind().awaitsOnlyDeps())
            {
                Invariants.checkState(dependencyExecutesAt == null);
                return truncatedApply(common, saveStatus, executeAt, writes, result);
            }
            Durability durability = checkTruncatedApplyInvariants(common, saveStatus, executeAt);
            return validate(new TruncatedAwaitsOnlyDeps(common.txnId(), saveStatus, durability, common.route(), executeAt, EMPTY, writes, result, dependencyExecutesAt));
        }

        private static Durability checkTruncatedApplyInvariants(CommonAttributes common, SaveStatus saveStatus, Timestamp executeAt)
        {
            Invariants.checkArgument(executeAt != null);
            Invariants.checkArgument(saveStatus == SaveStatus.TruncatedApply || saveStatus == SaveStatus.TruncatedApplyWithDeps || saveStatus == SaveStatus.TruncatedApplyWithOutcome);
            return Durability.mergeAtLeast(common.durability(), ShardUniversal);
        }

        public static Truncated invalidated(Command command)
        {
            Invariants.checkState(!command.hasBeen(Status.PreCommitted));
            // TODO (now): we shouldn't need to propagate these
            return invalidated(command.txnId(), command.durableListeners());
        }

        public static Truncated invalidated(TxnId txnId, Listeners.Immutable durableListeners)
        {
            // TODO (expected): migrate to using null for executeAt when invalidated
            // TODO (expected): is UniversalOrInvalidated correct here? Should we have a lower implication pure Invalidated?
            return validate(new Truncated(txnId, SaveStatus.Invalidated, UniversalOrInvalidated, null, Timestamp.NONE, durableListeners, null, null));
        }

        @Override
        public Timestamp executeAt()
        {
            return executeAt;
        }

        @Override
        public @Nullable Writes writes()
        {
            return writes;
        }

        @Override
        public @Nullable Result result()
        {
            return result;
        }

        @Override
        public Ballot acceptedOrCommitted()
        {
            return Ballot.MAX;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return null;
        }

        @Override
        public Seekables<?, ?> keysOrRanges()
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
            return validate(new Truncated(txnId(), saveStatus(), attrs.durability(), attrs.route(), executeAt, attrs.durableListeners(), writes, result));
        }
    }

    public static class TruncatedAwaitsOnlyDeps extends Truncated
    {
        /**
         * TODO (desired): Ideally we would not store this differently than we do for earlier states (where we encode in WaitingOn), but we also
         *  don't want to waste the space and complexity budget in earlier phases. Consider how to improve.
         */
        @Nullable final Timestamp executesAtLeast;

        public TruncatedAwaitsOnlyDeps(CommonAttributes commonAttributes, SaveStatus saveStatus, @Nullable Timestamp executeAt, @Nullable Writes writes, @Nullable Result result, @Nullable Timestamp executesAtLeast)
        {
            super(commonAttributes, saveStatus, executeAt, writes, result);
            this.executesAtLeast = executesAtLeast;
        }

        public TruncatedAwaitsOnlyDeps(TxnId txnId, SaveStatus saveStatus, Durability durability, @Nullable Route<?> route, @Nullable Timestamp executeAt, Listeners.Immutable listeners, @Nullable Writes writes, @Nullable Result result, @Nullable Timestamp executesAtLeast)
        {
            super(txnId, saveStatus, durability, route, executeAt, listeners, writes, result);
            this.executesAtLeast = executesAtLeast;
        }

        public Timestamp executesAtLeast()
        {
            return executesAtLeast;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!super.equals(o)) return false;
            return Objects.equals(executesAtLeast, ((TruncatedAwaitsOnlyDeps)o).executesAtLeast);
        }
    }

    public static class PreAccepted extends AbstractCommand
    {
        private final @Nullable Timestamp executeAt;
        private final PartialTxn partialTxn;
        private final @Nullable PartialDeps partialDeps;

        private PreAccepted(CommonAttributes common, SaveStatus status, Ballot promised, Timestamp executeAt)
        {
            this(common, status, promised, executeAt, common.partialTxn(), common.partialDeps());
        }

        private PreAccepted(CommonAttributes common, SaveStatus status, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps)
        {
            super(common, status, promised);
            this.executeAt = executeAt;
            this.partialTxn = partialTxn;
            this.partialDeps = partialDeps;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new PreAccepted(attrs, saveStatus(), promised, executeAt());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            PreAccepted that = (PreAccepted) o;
            return Objects.equals(executeAt, that.executeAt)
                    && Objects.equals(partialTxn, that.partialTxn)
                    && Objects.equals(partialDeps, that.partialDeps);
        }

        public static PreAccepted preAccepted(CommonAttributes common, Timestamp executeAt, Ballot promised)
        {
            return validate(new PreAccepted(common, SaveStatus.PreAccepted, promised, executeAt));
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
        public Ballot acceptedOrCommitted()
        {
            return Ballot.ZERO;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return partialTxn;
        }

        @Override
        public Seekables<?, ?> keysOrRanges()
        {
            return partialTxn == null ? additionalKeysOrRanges()
                                      : additionalKeysOrRanges() == null
                                        ? partialTxn.keys() : ((Seekables)partialTxn.keys()).with(additionalKeysOrRanges());
        }

        @Override
        public @Nullable PartialDeps partialDeps()
        {
            return partialDeps;
        }
    }

    public static class Accepted extends PreAccepted
    {
        private final Ballot acceptedOrCommitted;

        Accepted(CommonAttributes common, SaveStatus status, Ballot promised, Timestamp executeAt, Ballot acceptedOrCommitted)
        {
            super(common, status, promised, executeAt);
            this.acceptedOrCommitted = acceptedOrCommitted;
        }

        Accepted(CommonAttributes common, SaveStatus status, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted)
        {
            super(common, status, promised, executeAt, partialTxn, partialDeps);
            this.acceptedOrCommitted = acceptedOrCommitted;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return validate(new Accepted(attrs, saveStatus(), promised, executeAt(), acceptedOrCommitted()));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Accepted that = (Accepted) o;
            return Objects.equals(acceptedOrCommitted, that.acceptedOrCommitted);
        }

        public static Accepted accepted(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted)
        {
            return validate(new Accepted(common, status, promised, executeAt, accepted));
        }

        static Accepted accepted(Accepted command, CommonAttributes common, SaveStatus status, Ballot promised)
        {
            checkPromised(command, promised);
            checkSameClass(command, Accepted.class, "Cannot update");
            return validate(new Accepted(common, status, promised, command.executeAt(), command.acceptedOrCommitted()));
        }

        static Accepted accepted(Accepted command, CommonAttributes common, Ballot promised)
        {
            return accepted(command, common, command.saveStatus(), promised);
        }

        @Override
        public Ballot acceptedOrCommitted()
        {
            return acceptedOrCommitted;
        }
    }

    public static class Committed extends Accepted
    {
        public final WaitingOn waitingOn;

        private Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            super(common, status, promised, executeAt, accepted);
            this.waitingOn = waitingOn;
            Invariants.checkState(common.route().kind().isFullRoute(), "Expected a full route but given %s", common.route().kind());
        }

        @Override
        public Timestamp executesAtLeast()
        {
            if (!txnId().kind().awaitsOnlyDeps()) return executeAt();
            if (status().hasBeen(Stable)) return waitingOn.executeAtLeast(executeAt());
            return null;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return validate(new Committed(attrs, saveStatus(), executeAt(), promised, acceptedOrCommitted(), waitingOn()));
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
            return validate(new Committed(common, status, command.executeAt(), promised, command.acceptedOrCommitted(), waitingOn));
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

        public static Committed committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            return validate(new Committed(common, status, executeAt, promised, accepted, waitingOn));
        }

        public WaitingOn waitingOn()
        {
            return waitingOn;
        }

        public boolean isWaitingOnDependency()
        {
            return waitingOn.isWaiting();
        }
    }

    public static class Executed extends Committed
    {
        private final Writes writes;
        private final Result result;

        public Executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            super(common, status, executeAt, promised, accepted, waitingOn);
            Invariants.checkState(txnId().kind() != Txn.Kind.Write || writes != null);
            this.writes = writes;
            this.result = result;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return validate(new Executed(attrs, saveStatus(), executeAt(), promised, acceptedOrCommitted(), waitingOn(), writes, result));
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
            return validate(new Executed(common, status, command.executeAt(), promised, command.acceptedOrCommitted(), waitingOn, command.writes(), command.result()));
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
            return validate(new Executed(common, status, executeAt, promised, accepted, waitingOn, writes, result));
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
        public static final WaitingOn EMPTY = new WaitingOn(Keys.EMPTY, RangeDeps.NONE, KeyDeps.NONE, ImmutableBitSet.EMPTY, ImmutableBitSet.EMPTY);

        public final Keys keys;
        public final RangeDeps directRangeDeps;
        public final KeyDeps directKeyDeps;
        // waitingOn ONLY encodes both txnIds and keys
        public final ImmutableBitSet waitingOn;
        public final @Nullable ImmutableBitSet appliedOrInvalidated;

        public WaitingOn(WaitingOn copy)
        {
            this(copy.keys, copy.directRangeDeps, copy.directKeyDeps, copy.waitingOn, copy.appliedOrInvalidated);
        }

        public WaitingOn(Keys keys, RangeDeps directRangeDeps, KeyDeps directKeyDeps, ImmutableBitSet waitingOn, ImmutableBitSet appliedOrInvalidated)
        {
            this.keys = keys;
            this.directRangeDeps = directRangeDeps;
            this.directKeyDeps = directKeyDeps;
            this.waitingOn = waitingOn;
            this.appliedOrInvalidated = appliedOrInvalidated;
        }

        public int txnIdCount()
        {
            return directRangeDeps.txnIdCount() + directKeyDeps.txnIdCount();
        }

        TxnId txnId(int i)
        {
            int ki = i - directRangeDeps.txnIdCount();
            if (ki < 0)
                return directRangeDeps.txnId(i);

            if (ki < directKeyDeps.txnIdCount())
                return directKeyDeps.txnId(ki);

            throw new IndexOutOfBoundsException(i + " >= " + txnIdCount());
        }

        int indexOf(TxnId txnId)
        {
            if (txnId.domain() == Range)
                return directRangeDeps.indexOf(txnId);

            if (!CommandsForKey.managesExecution(txnId))
            {
                int i = directKeyDeps.indexOf(txnId);
                int offset = directRangeDeps.txnIdCount();
                return i < 0 ? i - offset : i + offset;
            }

            throw new IllegalArgumentException("WaitingOn does not track this kind of TxnId: " + txnId);
        }

        public Timestamp executeAtLeast()
        {
            return null;
        }

        public Timestamp executeAtLeast(Timestamp ifNull)
        {
            return ifNull;
        }

        public static WaitingOn none(Deps deps)
        {
            ImmutableBitSet empty = new ImmutableBitSet(deps.txnIdCount() + deps.keyDeps.keys().size());
            return new WaitingOn(deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps, empty, empty);
        }

        public boolean isWaiting()
        {
            return !waitingOn.isEmpty();
        }

        public boolean isWaitingOnKey()
        {
            return waitingOn.lastSetBit() >= txnIdCount();
        }

        public boolean isWaitingOnKey(int keyIndex)
        {
            return waitingOn.get(txnIdCount() + keyIndex);
        }

        public boolean isWaitingOnCommand()
        {
            return waitingOn.firstSetBit() < txnIdCount();
        }

        public boolean isWaitingOn(TxnId txnId)
        {
            int index = indexOf(txnId);
            return index >= 0 && waitingOn.get(index);
        }

        public TxnId nextWaitingOn()
        {
            int i = nextWaitingOnIndex();
            return i < 0 ? null : txnId(i);
        }

        private int nextWaitingOnIndex()
        {
            int directRangeTxnIdCount = directRangeDeps.txnIdCount();
            int nextWaitingOnDirectRangeIndex = waitingOn.prevSetBit(directRangeTxnIdCount);
            if (directKeyDeps == KeyDeps.NONE)
                return nextWaitingOnDirectRangeIndex;

            int directKeyTxnIdCount = directKeyDeps.txnIdCount();
            int txnIdCount = directKeyTxnIdCount + directRangeTxnIdCount;
            int nextWaitingOnDirectKeyIndex = waitingOn.prevSetBitNotBefore(txnIdCount, directRangeTxnIdCount);
            if (nextWaitingOnDirectKeyIndex < 0)
                return nextWaitingOnDirectRangeIndex;
            if (nextWaitingOnDirectRangeIndex < 0)
                return nextWaitingOnDirectKeyIndex;
            int c = directRangeDeps.txnId(nextWaitingOnDirectRangeIndex).compareTo(directKeyDeps.txnId(nextWaitingOnDirectKeyIndex - directRangeDeps.txnIdCount()));
            return c > 0 ? nextWaitingOnDirectRangeIndex : nextWaitingOnDirectKeyIndex;
        }

        @Override
        public String toString()
        {
            List<TxnId> waitingOnTxnIds = new ArrayList<>();
            List<Key> waitingOnKeys = new ArrayList<>();
            waitingOn.reverseForEach(waitingOnTxnIds, waitingOnKeys, this, keys, (outIds, outKeys, self, ks, i) -> {
                if (i < self.txnIdCount()) outIds.add(self.txnId(i));
                else outKeys.add(ks.get(i - self.txnIdCount()));
            });
            Collections.reverse(waitingOnKeys);
            return "keys=" + waitingOnKeys + ", txnIds=" + waitingOnTxnIds;
        }

        @Override
        public boolean equals(Object other)
        {
            return other.getClass() == WaitingOn.class && this.equals((WaitingOn) other);
        }

        boolean equals(WaitingOn other)
        {
            return this.keys.equals(other.keys)
                && directKeyDeps.equals(other.directKeyDeps)
                && directRangeDeps.equals(other.directRangeDeps)
                && this.waitingOn.equals(other.waitingOn)
                && Objects.equals(this.appliedOrInvalidated, other.appliedOrInvalidated);
        }

        public static class Update
        {
            final Keys keys;
            final RangeDeps directRangeDeps;
            final KeyDeps directKeyDeps;
            private SimpleBitSet waitingOn;
            private @Nullable SimpleBitSet appliedOrInvalidated;
            private Timestamp executeAtLeast;

            public Update(WaitingOn waitingOn)
            {
                this.keys = waitingOn.keys;
                this.directRangeDeps = waitingOn.directRangeDeps;
                this.directKeyDeps = waitingOn.directKeyDeps;
                this.waitingOn = waitingOn.waitingOn;
                this.appliedOrInvalidated = waitingOn.appliedOrInvalidated;
                if (waitingOn.getClass() == WaitingOnWithExecuteAt.class)
                    executeAtLeast = ((WaitingOnWithExecuteAt) waitingOn).executeAtLeast;
            }

            public Update(Committed committed)
            {
                this(committed.waitingOn);
            }

            private Update(TxnId txnId, Keys keys, RangeDeps directRangeDeps, KeyDeps directKeyDeps)
            {
                this.keys = keys;
                this.directRangeDeps = directRangeDeps;
                this.directKeyDeps = directKeyDeps;
                this.waitingOn = new SimpleBitSet(txnIdCount() + keys.size(), false);
                this.appliedOrInvalidated = CommandsForKey.managesExecution(txnId) ? null : new SimpleBitSet(txnIdCount(), false);
            }

            public static Update initialise(TxnId txnId, Route route, Ranges ranges, Deps deps)
            {
                Unseekables<?> executionParticipants = route.participants().slice(ranges, Minimal);
                Update update = new Update(txnId, deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps);
                deps.rangeDeps.forEach(executionParticipants, update, Update::initialise);
                deps.directKeyDeps.forEach(ranges, 0, deps.directKeyDeps.txnIdCount(), update, deps.rangeDeps, (upd, rdeps, index) -> upd.initialise(index + rdeps.txnIdCount()));
                deps.keyDeps.keys().forEach(ranges, (upd, key, index) -> upd.initialise(index + upd.txnIdCount()), update);
                return update;
            }

            private void initialise(int i)
            {
                waitingOn.set(i);
            }

            public boolean hasChanges()
            {
                return !(waitingOn instanceof ImmutableBitSet)
                       || (appliedOrInvalidated != null && !(appliedOrInvalidated instanceof ImmutableBitSet));
            }

            public TxnId txnId(int i)
            {
                int ki = i - directRangeDeps.txnIdCount();
                if (ki < 0)
                    return directRangeDeps.txnId(i);
                if (ki < directKeyDeps.txnIdCount())
                    return directKeyDeps.txnId(ki);
                throw new IndexOutOfBoundsException(i + " >= " + txnIdCount());
            }

            public int indexOf(TxnId txnId)
            {
                if (txnId.domain() == Range)
                    return directRangeDeps.indexOf(txnId);
                if (!CommandsForKey.managesExecution(txnId))
                    return directRangeDeps.txnIdCount() + directKeyDeps.indexOf(txnId);
                throw new IllegalArgumentException("WaitingOn does not track this kind of TxnId: " + txnId);
            }

            int txnIdCount()
            {
                return directRangeDeps.txnIdCount() + directKeyDeps.txnIdCount();
            }

            public boolean removeWaitingOn(TxnId txnId)
            {
                int index = indexOf(txnId);
                if (!waitingOn.get(index))
                    return false;

                waitingOn = ensureMutable(waitingOn);
                waitingOn.unset(index);
                return true;
            }

            public boolean isWaitingOn(TxnId txnId)
            {
                int index = indexOf(txnId);
                return index >= 0 && waitingOn.get(index);
            }

            public boolean hasUpdatedDirectDependency(WaitingOn prev)
            {
                int i = prev.nextWaitingOnIndex();
                return i >= 0 && !waitingOn.get(i);
            }

            public boolean removeWaitingOn(Key key)
            {
                int index = keys.indexOf(key);
                // we can be a member of a CFK we aren't waiting on, if we Accept in a later epoch using a key we don't own once Committed to an earlier eopch
                // TODO (expected): simply remove ourselves from any CFK we aren't waiting on
                if (index < 0) return false;

                index += txnIdCount();
                if (!waitingOn.get(index))
                    return false;

                waitingOn = ensureMutable(waitingOn);
                waitingOn.unset(index);
                return true;
            }

            public boolean isWaitingOn(Key key)
            {
                int index = keys.indexOf(key) + txnIdCount();
                return index >= 0 && waitingOn.get(index);
            }

            public boolean isWaiting()
            {
                return !waitingOn.isEmpty();
            }

            public TxnId minWaitingOnTxnId()
            {
                int index = minWaitingOnTxnIdx();
                return index < 0 ? null : txnId(index);
            }

            public int minWaitingOnTxnIdx()
            {
                int directRangeTxnIdCount = directRangeDeps.txnIdCount();
                int minWaitingOnDirectRangeIndex = waitingOn.nextSetBitBefore(0, directRangeTxnIdCount);
                if (directKeyDeps == KeyDeps.NONE)
                    return minWaitingOnDirectRangeIndex;

                int directKeyTxnIdCount = directKeyDeps.txnIdCount();
                int txnIdCount = directKeyTxnIdCount + directRangeTxnIdCount;
                int minWaitingOnDirectKeyIndex = waitingOn.nextSetBitBefore(directRangeTxnIdCount, txnIdCount);
                if (minWaitingOnDirectKeyIndex < 0)
                    return minWaitingOnDirectRangeIndex;
                if (minWaitingOnDirectRangeIndex < 0)
                    return minWaitingOnDirectKeyIndex;
                int c = directRangeDeps.txnId(minWaitingOnDirectRangeIndex).compareTo(directKeyDeps.txnId(minWaitingOnDirectKeyIndex - directRangeDeps.txnIdCount()));
                return c < 0 ? minWaitingOnDirectRangeIndex : minWaitingOnDirectKeyIndex;
            }

            public boolean isWaitingOnDirectRangeTxnIdx(int idx)
            {
                return waitingOn.get(idx);
            }

            public boolean isWaitingOnDirectKeyTxnIdx(int idx)
            {
                return waitingOn.get(idx + directRangeDeps.txnIdCount());
            }

            public void updateExecuteAtLeast(Timestamp executeAtLeast)
            {
                this.executeAtLeast = Timestamp.nonNullOrMax(executeAtLeast, this.executeAtLeast);
            }

            boolean removeWaitingOnDirectRangeTxnId(int i)
            {
                return removeWaitingOn(i);
            }

            boolean removeWaitingOnDirectKeyTxnId(int i)
            {
                return removeWaitingOn(i + directRangeDeps.txnIdCount());
            }

            boolean removeWaitingOnKey(int i)
            {
                return removeWaitingOn(txnIdCount() + i);
            }

            private boolean removeWaitingOn(int i)
            {
                if (waitingOn.get(i))
                {
                    waitingOn = ensureMutable(waitingOn);
                    waitingOn.unset(i);
                    return true;
                }
                else
                {
                    return false;
                }
            }

            /**
             * Warning: DO NOT invoke this when you really mean removeWaitingOn.
             * This propagates the applied/invalidated status to dependent transactions, which may
             * adopt a different set of dependency relations on this transaction. If the transaction
             * is e.g. partially stale, pre-bootstrap etc this could lead to an erroneous propagation
             * unless the transaction is truly (and fully) applied or invalidated locally.
             */
            public boolean setAppliedOrInvalidated(TxnId txnId)
            {
                int index = indexOf(txnId);
                return setAppliedOrInvalidated(index);
            }

            private boolean setAppliedOrInvalidatedDirectRangeTxn(int i)
            {
                return setAppliedOrInvalidated(i);
            }

            private boolean setAppliedOrInvalidatedDirectKeyTxn(int i)
            {
                return setAppliedOrInvalidated(i + directRangeDeps.txnIdCount());
            }

            private boolean setAppliedOrInvalidated(int i)
            {
                if (appliedOrInvalidated == null)
                    return removeWaitingOn(i);

                if (appliedOrInvalidated.get(i))
                    return false;

                if (!removeWaitingOn(i))
                    return false;

                appliedOrInvalidated = ensureMutable(appliedOrInvalidated);
                appliedOrInvalidated.set(i);
                return true;
            }

            public boolean setAppliedAndPropagate(TxnId txnId, WaitingOn propagate)
            {
                int index = indexOf(txnId);
                if (!setAppliedOrInvalidated(index))
                    return false;

                if (propagate.appliedOrInvalidated != null && !propagate.appliedOrInvalidated.isEmpty())
                {
                    forEachIntersection(propagate.directRangeDeps.txnIds(), directRangeDeps.txnIds(),
                                        (from, to, ignore, i1, i2) -> {
                                            if (from.get(i1))
                                                to.setAppliedOrInvalidatedDirectRangeTxn(i2);
                                        }, propagate.appliedOrInvalidated, this, null);

                    if (propagate.directKeyDeps != KeyDeps.NONE)
                    {
                        forEachIntersection(propagate.directKeyDeps.txnIds(), directKeyDeps.txnIds(),
                                            (from, to, ignore, i1, i2) -> {
                                                if (from.get(i1))
                                                    to.setAppliedOrInvalidatedDirectKeyTxn(i2);
                                            }, propagate.appliedOrInvalidated, this, null);
                    }
                }

                return true;
            }

            public <P1, P2, P3, P4> void forEachWaitingOnId(P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
            {
                waitingOn.reverseForEach(0, txnIdCount(), p1, p2, p3, p4, forEach);
            }

            public <P1, P2, P3, P4> void forEachWaitingOnKey(P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
            {
                waitingOn.reverseForEach(txnIdCount(), keys.size(), p1, p2, p3, this, (pp1, pp2, pp3, pp4, i) -> forEach.accept(pp1, pp2, pp3, i - pp4.txnIdCount()));
            }

            public WaitingOn build()
            {
                WaitingOn result = new WaitingOn(keys, directRangeDeps, directKeyDeps, ensureImmutable(waitingOn), ensureImmutable(appliedOrInvalidated));
                if (executeAtLeast == null)
                    return result;
                return new WaitingOnWithExecuteAt(result, executeAtLeast);
            }

            @Override
            public String toString()
            {
                List<TxnId> waitingOnTxnIds = new ArrayList<>();
                List<Key> waitingOnKeys = new ArrayList<>();
                waitingOn.reverseForEach(waitingOnTxnIds, waitingOnKeys, this, keys, (outIds, outKeys, self, ks, i) -> {
                    if (i < self.txnIdCount()) outIds.add(self.txnId(i));
                    else outKeys.add(ks.get(i - self.txnIdCount()));
                });
                Collections.reverse(waitingOnKeys);
                return "keys=" + waitingOnKeys + ", txnIds=" + waitingOnTxnIds;
            }
        }
    }

    public static final class WaitingOnWithExecuteAt extends WaitingOn
    {
        public final Timestamp executeAtLeast;
        public WaitingOnWithExecuteAt(WaitingOn waitingOn, Timestamp executeAtLeast)
        {
            super(waitingOn);
            this.executeAtLeast = executeAtLeast;
        }

        @Override
        public Timestamp executeAtLeast()
        {
            return executeAtLeast;
        }

        @Override
        public Timestamp executeAtLeast(Timestamp ifNull)
        {
            return executeAtLeast != null ? executeAtLeast : ifNull;
        }

        @Override
        public boolean equals(Object other)
        {
            return other.getClass() == WaitingOnWithExecuteAt.class && this.equals((WaitingOnWithExecuteAt) other);
        }

        boolean equals(WaitingOnWithExecuteAt other)
        {
            return super.equals(other) && executeAtLeast == other.executeAtLeast;
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
            // TODO (now): reconsider this special-casing
            Command.Accepted accepted = command.asAccepted();
            return Command.Accepted.accepted(attrs, SaveStatus.enrich(accepted.saveStatus(), SaveStatus.PreAccepted.known), executeAt, ballot, accepted.acceptedOrCommitted());
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
            return Command.Accepted.accepted(command.asAccepted(), attributes, SaveStatus.enrich(command.saveStatus(), Known.DefinitionOnly), promised);
        return (Command.Accepted) command.updateAttributes(attributes, promised);
    }

    static Command.Accepted accept(Command command, CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return validate(new Command.Accepted(attrs, SaveStatus.get(Status.Accepted, command.known()), ballot, executeAt, ballot));
    }

    static Command.Accepted acceptInvalidated(Command command, Ballot ballot)
    {
        SaveStatus saveStatus = SaveStatus.get(Status.AcceptedInvalidate, command.known());
        // TODO (desired): This should be NotDefined, but AcceptedInvalidated is represented by Command.Accepted because there's no acceptedOrCommitted register in NotDefined
        return validate(new Command.Accepted(command, saveStatus, ballot, command.executeAt(), command.partialTxn(), null, ballot));
    }

    static Command.Committed commit(Command command, CommonAttributes attrs, Ballot ballot, Timestamp executeAt)
    {
        return validate(Command.Committed.committed(attrs, SaveStatus.get(Status.Committed, command.known()), executeAt, Ballot.max(command.promised(), ballot), ballot, null));
    }

    static Command.Committed stable(Command command, CommonAttributes attrs, Ballot ballot, Timestamp executeAt, Command.WaitingOn waitingOn)
    {
        return validate(Command.Committed.committed(attrs, SaveStatus.Stable, executeAt, Ballot.max(command.promised(), ballot), ballot, waitingOn));
    }

    static Command precommit(CommonAttributes attrs, Command command, Timestamp executeAt)
    {
        SaveStatus saveStatus = SaveStatus.get(Status.PreCommitted, command.known());
        return validate(new Command.Accepted(attrs, saveStatus, command.promised(), executeAt, command.acceptedOrCommitted()));
    }

    static Command.Committed readyToExecute(Command.Committed command)
    {
        return Command.Committed.committed(command, command, SaveStatus.ReadyToExecute);
    }

    static Command.Executed preapplied(Command command, CommonAttributes attrs, Timestamp executeAt, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return Command.Executed.executed(attrs, SaveStatus.get(Status.PreApplied, command.known()), executeAt, command.promised(), command.acceptedOrCommitted(), waitingOn, writes, result);
    }

    static Command.Executed applying(Command.Executed command)
    {
        return Command.Executed.executed(command, command, SaveStatus.Applying);
    }

    static Command.Executed applied(Command.Executed command)
    {
        return Command.Executed.executed(command, command, SaveStatus.Applied);
    }
}
