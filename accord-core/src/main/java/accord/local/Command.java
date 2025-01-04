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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.Known;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
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

import com.google.common.annotations.VisibleForTesting;

import static accord.local.Command.AbstractCommand.validate;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtKnown;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice;
import static accord.primitives.SaveStatus.AcceptedInvalidate;
import static accord.primitives.SaveStatus.ErasedOrVestigial;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Status.Durability.Local;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Status.Durability.ShardUniversal;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.Stable;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.Paranoia.NONE;
import static accord.utils.Invariants.ParanoiaCostFactor.LOW;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Invariants.illegalState;
import static accord.utils.SortedArrays.forEachIntersection;
import static accord.utils.Utils.ensureImmutable;
import static accord.utils.Utils.ensureMutable;
import static java.lang.String.format;

public abstract class Command implements CommonAttributes
{
    private static Durability durability(Durability durability, SaveStatus status)
    {
        if (durability == NotDurable && status.compareTo(SaveStatus.PreApplied) >= 0 && status.compareTo(ErasedOrVestigial) < 0)
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

        public static AcceptedInvalidateWithoutDefinition acceptedInvalidateWithoutDefinition(CommonAttributes common, Ballot promised, Ballot accepted)
        {
            return AcceptedInvalidateWithoutDefinition.acceptedInvalidate(common, promised, accepted);
        }

        public static Committed committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            return Committed.committed(common, status, executeAt, promised, accepted, waitingOn);
        }

        public static Executed executed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn, Writes writes, Result result)
        {
            return Executed.executed(common, status, executeAt, promised, accepted, waitingOn, writes, result);
        }

        public static Truncated invalidated(TxnId txnId, StoreParticipants participants)
        {
            return Truncated.invalidated(txnId, participants);
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
            case AcceptedInvalidate:
                return validateCommandClass(status, AcceptedInvalidateWithoutDefinition.class, klass);
            case Accepted:
            case AcceptedWithDefinition:
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
                if (txnId.awaitsOnlyDeps())
                    return validateCommandClass(status, TruncatedAwaitsOnlyDeps.class, klass);
            case Erased:
            case ErasedOrVestigial:
            case Invalidated:
                return validateCommandClass(status, Truncated.class, klass);
            default:
                throw illegalState("Unhandled status " + status);
        }
    }

    public abstract static class AbstractCommand extends Command
    {
        private final TxnId txnId;
        private final SaveStatus status;
        private final Durability durability;
        private final @Nonnull StoreParticipants participants;
        private final Ballot promised;

        private AbstractCommand(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised)
        {
            this.txnId = txnId;
            this.status = validateCommandClass(txnId, status, getClass());
            this.durability = Invariants.nonNull(durability);
            this.participants = Invariants.nonNull(participants);
            this.promised = Invariants.nonNull(promised);
        }

        private AbstractCommand(CommonAttributes common, SaveStatus status, Ballot promised)
        {
            this.txnId = common.txnId();
            this.status = validateCommandClass(txnId, status, getClass());
            this.durability = Invariants.nonNull(common.durability());
            this.participants = Invariants.nonNull(common.participants());
            this.promised = Invariants.nonNull(promised);
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
                    && Objects.equals(participants(), command.participants())
                    && Objects.equals(promised(), command.promised());
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
        public final StoreParticipants participants()
        {
            return participants;
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
        public final SaveStatus saveStatus()
        {
            return status;
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
                        Invariants.checkState(partialTxn == null, "partialTxn is defined %s", validate);
                        break;
                    case DefinitionKnown:
                        Invariants.checkState(partialTxn != null || validate.participants().owns().isEmpty(), "partialTxn is null");
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
                        Invariants.checkState(executeAt == null || executeAt.compareTo(validate.txnId()) >= 0);
                        break;
                    case ExecuteAtKnown:
                    case ExecuteAtProposed:
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
                        Invariants.checkState(writes != null, "Writes is null %s", validate);
                        Invariants.checkState(result != null, "Result is null %s", validate);
                        break;
                    case Invalidated:
                        Invariants.checkState(validate.durability().isMaybeInvalidated(), "%s is not invalidated", validate.durability());
                    case Unknown:
                        Invariants.checkState(validate.durability() != Local);
                    case Erased:
                    case WasApply:
                        Invariants.checkState(writes == null, "Writes exist for %s", validate);
                        Invariants.checkState(result == null, "Results exist %s", validate);
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
                    Invariants.checkState(validate.participants().executes() != null);
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
    public final Route<?> route() { return participants().route(); }

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
    public abstract StoreParticipants participants();

    public Participants<?> maxContactable()
    {
        Route<?> route = route();
        if (route != null) return route.withHomeKey();
        else return participants().hasTouched();
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

    public boolean is(Status status)
    {
        return status() == status;
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
        return saveStatus.hasBeen(Status.Committed) && !saveStatus.is(Invalidated);
    }

    public final boolean isStable()
    {
        SaveStatus saveStatus = saveStatus();
        return isStable(saveStatus);
    }

    public static boolean isStable(SaveStatus saveStatus)
    {
        return saveStatus.hasBeen(Status.Stable) && !saveStatus.is(Invalidated);
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
    public abstract Command updateParticipants(StoreParticipants participants);

    public static class NotDefined extends AbstractCommand
    {
        NotDefined(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised)
        {
            super(txnId, status, durability, participants, promised);
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

        @Override
        public Command updateParticipants(StoreParticipants participants)
        {
            return new NotDefined(txnId(), SaveStatus.NotDefined, durability(), participants, promised());
        }

        public static NotDefined notDefined(CommonAttributes common, Ballot promised)
        {
            return validate(new NotDefined(common, SaveStatus.NotDefined, promised));
        }

        public static NotDefined uninitialised(TxnId txnId)
        {
            return validate(new NotDefined(txnId, Uninitialised, NotDurable, StoreParticipants.empty(txnId), Ballot.ZERO));
        }

        @Override
        public Timestamp executeAt()
        {
            return null;
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
            // TODO (expected): is Ballot.MAX helpful or harmful here?
            //  We have to special-case partial truncation anyway as we can infinite loop if we think there's a superseding recovery.
            super(commonAttributes, saveStatus, Ballot.MAX);
            this.executeAt = executeAt;
            this.writes = writes;
            this.result = result;
        }

        public Truncated(TxnId txnId, SaveStatus saveStatus, Durability durability, @Nonnull StoreParticipants participants, @Nullable Timestamp executeAt, @Nullable Writes writes, @Nullable Result result)
        {
            super(txnId, saveStatus, durability, participants, Ballot.MAX);
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
            return erased(command.txnId(), durability, command.participants());
        }

        public static Truncated erased(TxnId txnId, Status.Durability durability, StoreParticipants participants)
        {
            return validate(new Truncated(txnId, SaveStatus.Erased, durability, participants, null, null, null));
        }

        public static Truncated erasedOrVestigial(Command command)
        {
            return erasedOrVestigial(command.txnId(), command.participants());
        }

        public static Truncated erasedOrVestigial(TxnId txnId, StoreParticipants participants)
        {
            return validate(new Truncated(txnId, SaveStatus.ErasedOrVestigial, NotDurable, participants, null, null, null));
        }

        public static Truncated truncatedApply(Command command)
        {
            return truncatedApply(command, command.participants());
        }

        public static Truncated truncatedApply(Command command, @Nonnull StoreParticipants participants)
        {
            Invariants.checkArgument(command.known().executeAt.isDecidedAndKnownToExecute());
            Durability durability = Durability.mergeAtLeast(command.durability(), ShardUniversal);
            if (command.txnId().awaitsOnlyDeps())
            {
                Timestamp executesAtLeast = command.hasBeen(Stable) ? command.executesAtLeast() : null;
                return validate(new TruncatedAwaitsOnlyDeps(command.txnId(), SaveStatus.TruncatedApply, durability, participants, command.executeAt(), null, null, executesAtLeast));
            }
            return validate(new Truncated(command.txnId(), SaveStatus.TruncatedApply, durability, participants, command.executeAt(), null, null));
        }

        public static Truncated truncatedApplyWithOutcome(Executed command)
        {
            Durability durability = Durability.mergeAtLeast(command.durability(), ShardUniversal);
            if (command.txnId().awaitsOnlyDeps())
                return validate(new TruncatedAwaitsOnlyDeps(command.txnId(), SaveStatus.TruncatedApplyWithOutcome, durability, command.participants(), command.executeAt(), command.writes, command.result, command.executesAtLeast()));
            return validate(new Truncated(command.txnId(), SaveStatus.TruncatedApplyWithOutcome, durability, command.participants(), command.executeAt(), command.writes, command.result));
        }

        public static Truncated truncatedApply(CommonAttributes common, SaveStatus saveStatus, Timestamp executeAt, Writes writes, Result result)
        {
            Invariants.checkArgument(!common.txnId().awaitsOnlyDeps());
            Durability durability = checkTruncatedApplyInvariants(common, saveStatus, executeAt);
            return validate(new Truncated(common.txnId(), saveStatus, durability, common.participants(), executeAt, writes, result));
        }

        public static Truncated truncatedApply(CommonAttributes common, SaveStatus saveStatus, Timestamp executeAt, Writes writes, Result result, @Nullable Timestamp dependencyExecutesAt)
        {
            if (!common.txnId().awaitsOnlyDeps())
            {
                Invariants.checkState(dependencyExecutesAt == null);
                return truncatedApply(common, saveStatus, executeAt, writes, result);
            }
            Durability durability = checkTruncatedApplyInvariants(common, saveStatus, executeAt);
            return validate(new TruncatedAwaitsOnlyDeps(common.txnId(), saveStatus, durability, common.participants(), executeAt, writes, result, dependencyExecutesAt));
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
            return invalidated(command.txnId(), command.participants());
        }

        public static Truncated invalidated(TxnId txnId, StoreParticipants participants)
        {
            // TODO (expected): migrate to using null for executeAt when invalidated
            // TODO (expected): is UniversalOrInvalidated correct here? Should we have a lower implication pure Invalidated?
            return validate(new Truncated(txnId, SaveStatus.Invalidated, UniversalOrInvalidated, participants, Timestamp.NONE, null, null));
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
        public @Nullable PartialDeps partialDeps()
        {
            return null;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return validate(new Truncated(txnId(), saveStatus(), attrs.durability(), attrs.participants(), executeAt, writes, result));
        }

        @Override
        public Command updateParticipants(StoreParticipants participants)
        {
            return validate(new Truncated(txnId(), saveStatus(), durability(), participants, executeAt, writes, result));
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

        public TruncatedAwaitsOnlyDeps(TxnId txnId, SaveStatus saveStatus, Durability durability, StoreParticipants participants, @Nullable Timestamp executeAt, @Nullable Writes writes, @Nullable Result result, @Nullable Timestamp executesAtLeast)
        {
            super(txnId, saveStatus, durability, participants, executeAt, writes, result);
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

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return validate(new TruncatedAwaitsOnlyDeps(txnId(), saveStatus(), attrs.durability(), attrs.participants(), executeAt, writes, result, executesAtLeast));
        }

        @Override
        public Command updateParticipants(StoreParticipants participants)
        {
            return validate(new TruncatedAwaitsOnlyDeps(txnId(), saveStatus(), durability(), participants, executeAt, writes, result, executesAtLeast));
        }
    }

    public static class PreAccepted extends AbstractCommand
    {
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

        private PreAccepted(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps)
        {
            super(txnId, status, durability, participants, promised);
            this.executeAt = executeAt;
            this.partialTxn = partialTxn;
            this.partialDeps = partialDeps;
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
        public @Nullable PartialDeps partialDeps()
        {
            return partialDeps;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new PreAccepted(attrs, saveStatus(), promised, executeAt());
        }

        @Override
        public Command updateParticipants(StoreParticipants participants)
        {
            return new PreAccepted(txnId(), saveStatus(), durability(), participants, promised(), executeAt, partialTxn, partialDeps);
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
    }

    public static class AcceptedInvalidateWithoutDefinition extends NotDefined
    {
        public static AcceptedInvalidateWithoutDefinition acceptedInvalidate(CommonAttributes common, Ballot promised, Ballot accepted)
        {
            return new AcceptedInvalidateWithoutDefinition(common, promised, accepted);
        }

        private final Ballot accepted;

        private AcceptedInvalidateWithoutDefinition(CommonAttributes common, Ballot promised, Ballot accepted)
        {
            super(common, SaveStatus.AcceptedInvalidate, promised);
            this.accepted = accepted;
        }

        private AcceptedInvalidateWithoutDefinition(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Ballot accepted)
        {
            super(txnId, status, durability, participants, promised);
            this.accepted = accepted;
        }

        @Override
        public Ballot acceptedOrCommitted()
        {
            return accepted;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return new AcceptedInvalidateWithoutDefinition(attrs, promised, accepted);
        }

        @Override
        public Command updateParticipants(StoreParticipants participants)
        {
            return new AcceptedInvalidateWithoutDefinition(txnId(), saveStatus(), durability(), participants, promised(), accepted);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            AcceptedInvalidateWithoutDefinition that = (AcceptedInvalidateWithoutDefinition) o;
            return Objects.equals(accepted, that.accepted);
        }
    }

    public static class Accepted extends PreAccepted
    {
        private final Ballot acceptedOrCommitted;

        Accepted(CommonAttributes common, SaveStatus status, Ballot promised, Timestamp executeAt, Ballot acceptedOrCommitted)
        {
            super(common, status, promised, executeAt);
            this.acceptedOrCommitted = acceptedOrCommitted;
            validateDeps(participants(), partialDeps());
        }

        Accepted(CommonAttributes common, SaveStatus status, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted)
        {
            super(common, status, promised, executeAt, partialTxn, partialDeps);
            this.acceptedOrCommitted = acceptedOrCommitted;
            validateDeps(participants(), partialDeps);
        }

        Accepted(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted)
        {
            super(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps);
            this.acceptedOrCommitted = acceptedOrCommitted;
            validateDeps(participants(), partialDeps);
        }

        static void validateDeps(StoreParticipants participants, PartialDeps partialDeps)
        {
            if (Invariants.isParanoid() && Invariants.testParanoia(LINEAR, NONE, LOW))
            {
                Invariants.checkState(partialDeps == null
                    || (participants.touches() == participants.stillTouches() && participants.touches().equals(partialDeps.covering))
                    || (partialDeps.covers(participants.stillTouches()) && participants.touches().containsAll(partialDeps.covering))
                );
            }
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return validate(new Accepted(attrs, saveStatus(), promised, executeAt(), acceptedOrCommitted()));
        }

        @Override
        public Command updateParticipants(StoreParticipants participants)
        {
            return new Accepted(txnId(), saveStatus(), durability(), participants, promised(), executeAt(), partialTxn(), partialDeps(), acceptedOrCommitted);
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

        Committed(CommonAttributes common, SaveStatus status, Timestamp executeAt, Ballot promised, Ballot accepted, WaitingOn waitingOn)
        {
            super(common, status, promised, executeAt, accepted);
            this.waitingOn = waitingOn;
            Invariants.nonNull(common.participants());
            Invariants.nonNull(common.route());
            Invariants.checkState(common.route().kind().isFullRoute(), "Expected a full route but given %s", common.route().kind());
        }

        Committed(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted, WaitingOn waitingOn)
        {
            super(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted);
            this.waitingOn = waitingOn;
        }

        @Override
        public Timestamp executesAtLeast()
        {
            if (!txnId().awaitsOnlyDeps()) return executeAt();
            if (status().hasBeen(Stable)) return waitingOn.executeAtLeast(executeAt());
            return null;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return validate(new Committed(attrs, saveStatus(), executeAt(), promised, acceptedOrCommitted(), waitingOn()));
        }

        @Override
        public Command updateParticipants(StoreParticipants participants)
        {
            Invariants.checkState(partialDeps().covers(participants.stillTouches()));
            return new Committed(txnId(), saveStatus(), durability(), participants, promised(), executeAt(), partialTxn(), partialDeps(), acceptedOrCommitted(), waitingOn);
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

        public static Committed committed(Committed command, CommonAttributes common, WaitingOn waitingOn)
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

        private Executed(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted, WaitingOn waitingOn, Writes writes, Result result)
        {
            super(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted, waitingOn);
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
        public Command updateParticipants(StoreParticipants participants)
        {
            return new Executed(txnId(), saveStatus(), durability(), participants, promised(), executeAt(), partialTxn(), partialDeps(), acceptedOrCommitted(), waitingOn, writes, result);
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

    public static class Minimal
    {
        public final TxnId txnId;
        public final SaveStatus saveStatus;
        public final StoreParticipants participants;
        public final Status.Durability durability;
        public final Timestamp executeAt;
        public final Writes writes;

        public Minimal(TxnId txnId, SaveStatus saveStatus, StoreParticipants participants, Status.Durability durability, Timestamp executeAt, Writes writes)
        {
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.participants = participants;
            this.durability = durability;
            this.executeAt = executeAt;
            this.writes = writes;
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            Minimal minimal = (Minimal) object;
            return Objects.equals(txnId, minimal.txnId) && saveStatus == minimal.saveStatus && Objects.equals(participants, minimal.participants) && durability == minimal.durability && Objects.equals(executeAt, minimal.executeAt) && Objects.equals(writes, minimal.writes);
        }

        @Override
        public final int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class WaitingOn
    {
        private static final WaitingOn EMPTY_FOR_KEY = new WaitingOn(RoutingKeys.EMPTY, RangeDeps.NONE, KeyDeps.NONE, ImmutableBitSet.EMPTY, null);
        private static final WaitingOn EMPTY_FOR_RANGE = new WaitingOn(RoutingKeys.EMPTY, RangeDeps.NONE, KeyDeps.NONE, ImmutableBitSet.EMPTY, ImmutableBitSet.EMPTY);

        public static WaitingOn empty(Routable.Domain domain)
        {
            if (domain == Range)
                return EMPTY_FOR_RANGE;
            return EMPTY_FOR_KEY;
        }

        public final RoutingKeys keys;
        public final RangeDeps directRangeDeps;
        public final KeyDeps directKeyDeps;
        // waitingOn ONLY encodes both txnIds and keys
        public final ImmutableBitSet waitingOn;
        public final @Nullable ImmutableBitSet appliedOrInvalidated;

        public WaitingOn(WaitingOn copy)
        {
            this(copy.keys, copy.directRangeDeps, copy.directKeyDeps, copy.waitingOn, copy.appliedOrInvalidated);
        }

        public WaitingOn(RoutingKeys keys, RangeDeps directRangeDeps, KeyDeps directKeyDeps, ImmutableBitSet waitingOn, ImmutableBitSet appliedOrInvalidated)
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

        public TxnId txnId(int i)
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

        public static WaitingOn none(Routable.Domain domain, Deps deps)
        {
            return new WaitingOn(deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps,
                                 new ImmutableBitSet(deps.directKeyDeps.txnIdCount() + deps.keyDeps.keys().size()),
                                 domain == Range ? new ImmutableBitSet(deps.rangeDeps.txnIdCount()) : null);
        }

        public boolean isWaiting()
        {
            return !waitingOn.isEmpty();
        }

        public boolean isWaitingOnKey()
        {
            return waitingOn.lastSetBit() >= txnIdCount();
        }

        public RoutingKey lastWaitingOnKey()
        {
            int keyIndex = waitingOn.lastSetBit() - txnIdCount();
            if (keyIndex < 0) return null;
            return keys.get(keyIndex);
        }

        public boolean isWaitingOnKey(int keyIndex)
        {
            Invariants.checkIndex(keyIndex, keys.size());
            return waitingOn.get(txnIdCount() + keyIndex);
        }

        public RoutingKeys waitingOnKeys()
        {
            int offset = txnIdCount();
            int count = waitingOn.getSetBitCount(offset, waitingOn.size());
            if (count == 0)
                return RoutingKeys.EMPTY;
            if (count == keys.size())
                return keys;
            RoutingKey[] selected = new RoutingKey[count];
            int c = 0;
            for (int i = waitingOn.nextSetBit(offset, -1) ; i >= 0 ; i = waitingOn.nextSetBit(i + 1, -1))
                selected[c++] = keys.get(i - offset);
            return RoutingKeys.ofSortedUnique(selected);
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
            List<RoutingKey> waitingOnKeys = new ArrayList<>();
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
            final RoutingKeys keys;
            final RangeDeps directRangeDeps;
            final KeyDeps directKeyDeps;
            private SimpleBitSet waitingOn;
            private @Nullable SimpleBitSet appliedOrInvalidated;
            private Timestamp executeAtLeast;
            private boolean executeAtLeastUpdated;

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

            private Update(TxnId txnId, RoutingKeys keys, RangeDeps directRangeDeps, KeyDeps directKeyDeps)
            {
                this.keys = keys;
                this.directRangeDeps = directRangeDeps;
                this.directKeyDeps = directKeyDeps;
                this.waitingOn = new SimpleBitSet(txnIdCount() + keys.size(), false);
                this.appliedOrInvalidated = txnId.is(Key) ? null : new SimpleBitSet(txnIdCount(), false);
            }

            public static Update initialise(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, StoreParticipants participants, Deps deps)
            {
                Update update = new Update(txnId, deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps);
                if (txnId.is(Txn.Kind.ExclusiveSyncPoint))
                {
                    CommandStores.RangesForEpoch rangesForEpoch = safeStore.ranges();
                    long prevEpoch = Long.MAX_VALUE;
                    for (int i = rangesForEpoch.ranges.length - 1 ; i >= 0 ; --i)
                    {
                        long maxEpoch = prevEpoch;
                        long epoch = rangesForEpoch.epochs[i];
                        Ranges ranges = rangesForEpoch.ranges[i];
                        ranges = safeStore.redundantBefore().removePreBootstrap(txnId, ranges);
                        if (!ranges.isEmpty())
                        {
                            deps.rangeDeps.forEach(participants.stillExecutes().slice(ranges, Slice.Minimal), update, (upd, idx) -> {
                                TxnId id = upd.txnId(idx);
                                // because we use RX as RedundantBefore bounds, we must not let an RX on a closing range
                                // get ahead of one that isn't closed but has overlapping transactions (else we may erroneously treat as redundant)
                                if (id.epoch() >= epoch && (id.is(Txn.Kind.ExclusiveSyncPoint) || id.epoch() < maxEpoch))
                                    update.initialise(idx);
                            });
                            int lbound = deps.rangeDeps.txnIdCount();
                            deps.directKeyDeps.forEach(ranges, 0, deps.directKeyDeps.txnIdCount(), update, deps.rangeDeps, (upd, rdeps, idx) -> {
                                TxnId id = upd.txnId(idx + lbound);
                                if (id.epoch() >= epoch && id.epoch() < maxEpoch)
                                    update.initialise(idx + lbound);
                            });// TODO (expected): do same loop as above for keys, but mark the key if we find any matching id
                            deps.keyDeps.keys().forEach(ranges, (upd, key, index) -> upd.initialise(index + upd.txnIdCount()), update);
                        }
                        prevEpoch = epoch;
                    }
                    return update;
                }
                else
                {
                    Ranges executeRanges = participants.executeRanges(safeStore, txnId, executeAt);
                    // TODO (expected): refactor this to operate only on participants, not ranges
                    deps.rangeDeps.forEach(participants.stillExecutes(), update, Update::initialise);
                    deps.directKeyDeps.forEach(executeRanges, 0, deps.directKeyDeps.txnIdCount(), update, deps.rangeDeps, (upd, rdeps, index) -> upd.initialise(index + rdeps.txnIdCount()));
                    deps.keyDeps.keys().forEach(executeRanges, (upd, key, index) -> upd.initialise(index + upd.txnIdCount()), update);
                    return update;
                }
            }

            @VisibleForTesting
            public static Update unsafeInitialise(TxnId txnId, Ranges executeRanges, Route<?> route, Deps deps)
            {
                Unseekables<?> executionParticipants = route.slice(executeRanges, Slice.Minimal);
                Update update = new Update(txnId, deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps);
                // TODO (expected): refactor this to operate only on participants, not ranges
                deps.rangeDeps.forEach(executionParticipants, update, Update::initialise);
                deps.directKeyDeps.forEach(executeRanges, 0, deps.directKeyDeps.txnIdCount(), update, deps.rangeDeps, (upd, rdeps, index) -> upd.initialise(index + rdeps.txnIdCount()));
                deps.keyDeps.keys().forEach(executeRanges, (upd, key, index) -> upd.initialise(index + upd.txnIdCount()), update);
                return update;
            }

            private void initialise(int i)
            {
                waitingOn.set(i);
            }

            public boolean hasChanges()
            {
                return !(waitingOn instanceof ImmutableBitSet)
                       || (appliedOrInvalidated != null && !(appliedOrInvalidated instanceof ImmutableBitSet))
                       || executeAtLeastUpdated;
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
                throw illegalArgument("WaitingOn does not track this kind of TxnId: " + txnId);
            }

            public int txnIdCount()
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

            public boolean removeWaitingOn(RoutingKey key)
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

            public boolean isWaitingOn(RoutingKey key)
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
                Invariants.checkIndex(idx, directRangeDeps.txnIdCount());
                return waitingOn.get(idx);
            }

            public boolean isWaitingOnDirectKeyTxnIdx(int idx)
            {
                Invariants.checkIndex(idx, directKeyDeps.txnIdCount());
                return waitingOn.get(idx + directRangeDeps.txnIdCount());
            }

            public void updateExecuteAtLeast(TxnId txnId, Timestamp executeAtLeast)
            {
                Invariants.checkState(executeAtLeast.compareTo(txnId) > 0);
                if (this.executeAtLeast == null || this.executeAtLeast.compareTo(executeAtLeast) < 0)
                {
                    this.executeAtLeast = executeAtLeast;
                    this.executeAtLeastUpdated = true;
                }
            }

            boolean removeWaitingOnDirectRangeTxnId(int i)
            {
                Invariants.checkIndex(i, directRangeDeps.txnIdCount());
                return removeWaitingOn(i);
            }

            boolean removeWaitingOnDirectKeyTxnId(int i)
            {
                Invariants.checkIndex(i, directKeyDeps.txnIdCount());
                return removeWaitingOn(i + directRangeDeps.txnIdCount());
            }

            boolean removeWaitingOnKey(int i)
            {
                Invariants.checkIndex(i, keys.size());
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
                Invariants.checkIndex(i, directRangeDeps.txnIdCount());
                return setAppliedOrInvalidated(i);
            }

            private boolean setAppliedOrInvalidatedDirectKeyTxn(int i)
            {
                Invariants.checkIndex(i, directKeyDeps.txnIdCount());
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
                List<RoutingKey> waitingOnKeys = new ArrayList<>();
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
            return super.equals(other) && executeAtLeast.equals(other.executeAtLeast);
        }
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
            // TODO (desired): reconsider this special-casing
            return Command.Accepted.accepted(attrs, SaveStatus.enrich(command.saveStatus(), SaveStatus.PreAccepted.known), executeAt, ballot, command.acceptedOrCommitted());
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
            return Command.Accepted.accepted(command.asAccepted(), attributes, SaveStatus.withDefinition(command.saveStatus()), promised);
        return (Command.Accepted) command.updateAttributes(attributes, promised);
    }

    static Command.Accepted accept(Command command, CommonAttributes attrs, Timestamp executeAt, Ballot ballot)
    {
        return validate(new Command.Accepted(attrs, SaveStatus.get(Status.Accepted, command.known()), ballot, executeAt, ballot));
    }

    static Command acceptInvalidated(Command command, Ballot ballot)
    {
        SaveStatus saveStatus = SaveStatus.get(Status.AcceptedInvalidate, command.known());
        if (saveStatus == AcceptedInvalidate)
            return validate(new Command.AcceptedInvalidateWithoutDefinition(command, ballot, ballot));

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
