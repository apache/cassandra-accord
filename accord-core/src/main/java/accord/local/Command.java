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
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Known;
import accord.primitives.Known.Outcome;
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
import accord.primitives.Status.Phase;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.ImmutableBitSet;
import accord.utils.IndexedQuadConsumer;
import accord.utils.IndexedTriConsumer;
import accord.utils.Invariants;
import accord.utils.LargeBitSet;
import accord.utils.UnhandledEnum;

import com.google.common.annotations.VisibleForTesting;

import static accord.local.Command.Committed.committed;
import static accord.local.Command.Executed.executed;
import static accord.local.Command.NotAcceptedWithoutDefinition.notAccepted;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice;
import static accord.primitives.SaveStatus.AcceptedInvalidate;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.TruncatedUnapplied;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Status.Durability.ShardUniversal;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.Paranoia.NONE;
import static accord.utils.Invariants.ParanoiaCostFactor.LOW;
import static accord.utils.Invariants.illegalArgument;
import static accord.utils.Invariants.illegalState;
import static accord.utils.SortedArrays.forEachIntersection;
import static accord.utils.Utils.ensureImmutable;
import static accord.utils.Utils.ensureMutable;
import static java.lang.String.format;

public abstract class Command implements ICommand
{
    public static class Minimal
    {
        public final TxnId txnId;
        public final SaveStatus saveStatus;
        public final StoreParticipants participants;
        public final Status.Durability durability;
        public final Timestamp executeAt;

        public Minimal(TxnId txnId, SaveStatus saveStatus, StoreParticipants participants, Status.Durability durability, Timestamp executeAt)
        {
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.participants = participants;
            this.durability = durability;
            this.executeAt = executeAt;
        }

        @Override
        public boolean equals(Object object)
        {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;
            Minimal minimal = (Minimal) object;
            return Objects.equals(txnId, minimal.txnId) && saveStatus == minimal.saveStatus && Objects.equals(participants, minimal.participants) && durability == minimal.durability && Objects.equals(executeAt, minimal.executeAt);
        }

        @Override
        public final int hashCode()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static abstract class MinimalWithDeps extends Minimal
    {
        public MinimalWithDeps(TxnId txnId, SaveStatus saveStatus, StoreParticipants participants, Durability durability, Timestamp executeAt)
        {
            super(txnId, saveStatus, participants, durability, executeAt);
        }

        abstract public PartialDeps partialDeps();
    }

    public static class MinimalWithConcreteDeps extends MinimalWithDeps
    {
        final PartialDeps partialDeps;
        public MinimalWithConcreteDeps(TxnId txnId, SaveStatus saveStatus, StoreParticipants participants, Durability durability, Timestamp executeAt, PartialDeps partialDeps)
        {
            super(txnId, saveStatus, participants, durability, executeAt);
            this.partialDeps = partialDeps;
        }

        public PartialDeps partialDeps()
        {
            return partialDeps;
        }
    }

    private final TxnId txnId;
    private final SaveStatus saveStatus;
    private final Durability durability;
    private final @Nonnull StoreParticipants participants;
    private final Ballot promised;
    private final Timestamp executeAt;
    private final @Nullable PartialTxn partialTxn;
    private final @Nullable PartialDeps partialDeps;
    private final @Nonnull Ballot acceptedOrCommitted;

    protected Command(TxnId txnId, SaveStatus saveStatus, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, @Nullable PartialTxn partialTxn, @Nullable PartialDeps partialDeps, Ballot acceptedOrCommitted)
    {
        this.txnId = txnId;
        this.saveStatus = validateCommandClass(txnId, saveStatus, getClass());
        this.durability = Invariants.nonNull(durability);
        this.participants = Invariants.nonNull(participants);
        this.promised = Invariants.nonNull(promised);
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.executeAt = executeAt;
        this.acceptedOrCommitted = Invariants.nonNull(acceptedOrCommitted);
        Invariants.require(partialTxn == null || txnId.isSystemTxn() || participants.owns().containsAll(partialTxn.keys()));
        Invariants.require(partialTxn == null || txnId.isSystemTxn() || participants.owns().containsAll(partialTxn.read().keys()));
    }

    protected Command(TxnId txnId, SaveStatus saveStatus, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, @Nullable PartialTxn partialTxn, @Nullable PartialDeps partialDeps, Ballot acceptedOrCommitted, boolean unsafeInitialisation)
    {
        Invariants.require(unsafeInitialisation);
        this.txnId = txnId;
        this.saveStatus = saveStatus;
        this.durability = durability;
        this.participants = participants;
        this.promised = promised;
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.executeAt = executeAt;
        this.acceptedOrCommitted = acceptedOrCommitted;
    }

    protected Command(ICommand copy, SaveStatus saveStatus)
    {
        this.txnId = copy.txnId();
        this.saveStatus = validateCommandClass(txnId, saveStatus, getClass());
        this.durability = Invariants.nonNull(copy.durability());
        this.participants = Invariants.nonNull(copy.participants());
        this.promised = copy.promised();
        this.partialTxn = copy.partialTxn();
        this.partialDeps = copy.partialDeps();
        this.executeAt = copy.executeAt();
        this.acceptedOrCommitted = copy.acceptedOrCommitted();
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
               && Objects.equals(acceptedOrCommitted(), command.acceptedOrCommitted())
               && Objects.equals(promised(), command.promised());
    }

    @Override
    public String toString()
    {
        return "Command@" + System.identityHashCode(this) + '{' + txnId + ':' + saveStatus + '}';
    }

    @Override
    public final TxnId txnId()
    {
        return txnId;
    }

    @Override
    public final StoreParticipants participants()
    {
        return participants;
    }

    @Override
    public final Ballot promised()
    {
        return promised;
    }

    @Override
    public final Durability durability()
    {
        return durability;
    }

    public final SaveStatus saveStatus()
    {
        return saveStatus;
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
    @Nullable
    public final Route<?> route() { return participants().route(); }

    public Participants<?> maxParticipants()
    {
        return participants.max();
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
    public final Timestamp executeAt() { return executeAt; }

    @Override
    public final Ballot acceptedOrCommitted() { return acceptedOrCommitted; }

    @Override
    public final @Nullable PartialTxn partialTxn() { return partialTxn; }

    @Override
    public final @Nullable PartialDeps partialDeps() { return partialDeps; }

    public @Nullable Writes writes() { return null; }
    public @Nullable Result result() { return null; }
    public @Nullable WaitingOn waitingOn() { return null; }

    /**
     * Only meaningful when txnId.kind().awaitsOnlyDeps()
     */
    public @Nullable Timestamp executesAtLeast()
    {
        return executesAtLeast(null);
    }

    public @Nullable Timestamp executesAtLeast(@Nullable Timestamp orElse)
    {
        WaitingOn waitingOn = waitingOn();
        if (waitingOn == null)
            return orElse;
        return waitingOn.executeAtLeast();
    }

    public final Timestamp executeAtIfKnownElseTxnId()
    {
        return executeAtIfKnown(txnId);
    }

    public final Timestamp executeAtIfKnown()
    {
        return executeAtIfKnown(null);
    }

    public final Timestamp executeAtIfKnown(Timestamp orElse)
    {
        if (known().isExecuteAtKnown())
            return executeAt();
        return orElse;
    }

    public final Timestamp applyAtIfKnown()
    {
        return applyAtIfKnown(null);
    }

    public final Timestamp applyAtIfKnown(Timestamp orElse)
    {
        if (known().is(ApplyAtKnown))
            return executeAt();
        return orElse;
    }

    public final Timestamp executeAtOrTxnId()
    {
        Timestamp executeAt = executeAt();
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

    public boolean is(Status status)
    {
        return status() == status;
    }

    public final Accepted asAccepted()
    {
        return Invariants.cast(this, Accepted.class);
    }
    public final Committed asCommitted()
    {
        return Invariants.cast(this, Committed.class);
    }
    public final Executed asExecuted()
    {
        return Invariants.cast(this, Executed.class);
    }

    public final Command updatePromised(Ballot promised) { return updateAttributes(participants(), promised, durability()); }
    public final Command updateParticipants(StoreParticipants participants) { return updateAttributes(participants, promised(), durability()); }
    public final Command updateDurability(Durability durability) { return updateAttributes(participants(), promised(), durability); }
    public abstract Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability);

    public static class NotDefined extends Command
    {
        public static NotDefined notDefined(ICommand copy)
        {
            return validate(notDefined(copy, copy.promised()));
        }

        public static NotDefined notDefined(ICommand copy, Ballot promised)
        {
            return validate(new NotDefined(copy.txnId(), SaveStatus.NotDefined, copy.durability(), copy.participants(), promised));
        }

        public static NotDefined notDefined(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised)
        {
            return validate(new NotDefined(txnId, status, durability, participants, promised));
        }

        public static NotDefined uninitialised(TxnId txnId)
        {
            return validate(new NotDefined(txnId, Uninitialised, NotDurable, StoreParticipants.empty(txnId), Ballot.ZERO));
        }

        NotDefined(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised)
        {
            super(txnId, status, durability, participants, promised, null, null, null, Ballot.ZERO);
        }

        @Override
        public Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability)
        {
            return validate(new NotDefined(txnId(), initialise(saveStatus()), durability, participants, promised));
        }

        private static SaveStatus initialise(SaveStatus saveStatus)
        {
            return saveStatus == Uninitialised ? SaveStatus.NotDefined : saveStatus;
        }
    }

    public static class Truncated extends Command
    {
        public static Truncated erased(TxnId txnId)
        {
            return validate(new Truncated(txnId, SaveStatus.Erased, UniversalOrInvalidated, StoreParticipants.empty(txnId), null, null, null, null));
        }

        public static Truncated vestigial(Command command)
        {
            return vestigial(command.txnId(), command.participants());
        }

        public static Truncated vestigial(Command command, StoreParticipants participants)
        {
            return vestigial(command.txnId(), participants);
        }

        public static Truncated vestigial(TxnId txnId, StoreParticipants participants)
        {
            return validate(new Truncated(txnId, SaveStatus.Vestigial, NotDurable, participants, null, null, null, null));
        }

        public static Truncated truncated(Command command)
        {
            return truncated(command, command.participants());
        }

        public static Truncated truncated(Command command, @Nonnull StoreParticipants participants)
        {
            Invariants.requireArgument(command.known().isExecuteAtKnown());
            // TODO (expected): centralise this translation so applied consistently
            SaveStatus newSaveStatus = command.known().is(ApplyAtKnown) ? TruncatedApply : TruncatedUnapplied;
            return truncated(command, participants, newSaveStatus);
        }

        public static Truncated truncated(TxnId txnId, SaveStatus saveStatus, Durability durability, StoreParticipants participants, @Nullable Timestamp executeAt, @Nullable PartialDeps partialDeps, @Nullable Writes writes, Result result)
        {
            Invariants.requireArgument(!txnId.awaitsOnlyDeps());
            durability = checkTruncatedApplyInvariants(durability, saveStatus, executeAt);
            return validate(new Truncated(txnId, saveStatus, durability, participants, executeAt, partialDeps, writes, result));
        }

        public static Truncated truncated(Command command, StoreParticipants participants, SaveStatus newSaveStatus)
        {
            Timestamp executesAtLeast = command.executesAtLeast();
            PartialDeps partialDeps = newSaveStatus.known.is(DepsKnown) ? command.partialDeps : null;
            Writes writes = null;
            Result result = null;
            if (newSaveStatus.known.is(Outcome.Apply))
            {
                writes = command.writes();
                result = command.result();
            }
            return truncated(command.txnId(), newSaveStatus, command.durability(), participants, command.executeAt, partialDeps, writes, result, executesAtLeast);
        }

        public static Truncated truncated(ICommand common, SaveStatus saveStatus, @Nullable Timestamp executeAt, @Nullable PartialDeps partialDeps, @Nullable Writes writes, @Nullable Result result, @Nullable Timestamp executesAtLeast)
        {
            return truncated(common.txnId(), saveStatus, common.durability(), common.participants(), executeAt, partialDeps, writes, result, executesAtLeast);
        }

        public static Truncated truncated(TxnId txnId, SaveStatus saveStatus, Durability durability, StoreParticipants participants, Timestamp executeAt, PartialDeps partialDeps, Writes writes, Result result, @Nullable Timestamp executesAtLeast)
        {
            durability = checkTruncatedApplyInvariants(durability, saveStatus, executeAt);
            if (!txnId.awaitsOnlyDeps())
            {
                Invariants.require(executesAtLeast == null);
                return truncated(txnId, saveStatus, durability, participants, executeAt, partialDeps, writes, result);
            }
            return validate(new TruncatedAwaitsOnlyDeps(txnId, saveStatus, durability, participants, executeAt, partialDeps, writes, result, executesAtLeast));
        }

        private static Durability checkTruncatedApplyInvariants(Durability durability, SaveStatus saveStatus, Timestamp executeAt)
        {
            Invariants.requireArgument(executeAt != null);
            Invariants.requireArgument(saveStatus.status == Status.Truncated);
            return durability.mergeMax(ShardUniversal);
        }

        public static Truncated invalidated(Command command)
        {
            return invalidated(command, command.participants());
        }

        public static Truncated invalidated(Command command, StoreParticipants participants)
        {
            Invariants.require(!command.hasBeen(Status.PreCommitted));
            return invalidated(command.txnId(), participants);
        }

        public static Truncated invalidated(TxnId txnId, StoreParticipants participants)
        {
            // NOTE: we *must* save participants here so that on replay we properly repopulate CommandsForKey
            // (otherwise we may see this as a transitive transaction, but never mark it invalidated)
            return validate(new Truncated(txnId, SaveStatus.Invalidated, UniversalOrInvalidated, participants, Timestamp.NONE, null, null, null));
        }

        @Nullable final Writes writes;
        @Nullable final Result result;

        public Truncated(ICommand copy, SaveStatus saveStatus)
        {
            super(copy, saveStatus);
            this.writes = copy.writes();
            this.result = copy.result();
        }

        public Truncated(TxnId txnId, SaveStatus saveStatus, Durability durability, @Nonnull StoreParticipants participants, @Nullable Timestamp executeAt, @Nullable PartialDeps partialDeps, @Nullable Writes writes, @Nullable Result result)
        {
            super(txnId, saveStatus, durability, participants, Ballot.MAX, executeAt, null, partialDeps, Ballot.MAX);
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
            return Objects.equals(writes, that.writes)
                && Objects.equals(result, that.result);
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
        public Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability)
        {
            return validate(new Truncated(txnId(), saveStatus(), durability, participants, executeAt(), partialDeps(), writes, result));
        }
    }

    public static class TruncatedAwaitsOnlyDeps extends Truncated
    {
        /**
         * TODO (desired): Ideally we would not store this differently than we do for earlier states (where we encode in WaitingOn), but we also
         *  don't want to waste the space and complexity budget in earlier phases. Consider how to improve.
         */
        @Nullable final Timestamp executesAtLeast;

        public TruncatedAwaitsOnlyDeps(ICommand cmd, SaveStatus saveStatus, Timestamp executesAtLeast)
        {
            super(cmd, saveStatus);
            this.executesAtLeast = executesAtLeast;
        }

        public TruncatedAwaitsOnlyDeps(TxnId txnId, SaveStatus saveStatus, Durability durability, StoreParticipants participants, @Nullable Timestamp executeAt, @Nullable PartialDeps partialDeps, @Nullable Writes writes, @Nullable Result result, @Nullable Timestamp executesAtLeast)
        {
            super(txnId, saveStatus, durability, participants, executeAt, partialDeps, writes, result);
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
        public Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability)
        {
            return validate(new TruncatedAwaitsOnlyDeps(txnId(), saveStatus(), durability, participants, executeAt(), partialDeps(), writes, result, executesAtLeast));
        }
    }

    public static class PreAccepted extends Command
    {
        public static PreAccepted preaccepted(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps)
        {
            return validate(new PreAccepted(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps));
        }

        public static PreAccepted preaccepted(ICommand common, SaveStatus newSaveStatus)
        {
            return validate(new PreAccepted(common, newSaveStatus));
        }

        private PreAccepted(ICommand copy, SaveStatus status)
        {
            super(copy, status);
        }

        private PreAccepted(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps)
        {
            super(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, Ballot.ZERO);
        }

        private PreAccepted(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted)
        {
            super(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted);
        }

        @Override
        public Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability)
        {
            return new PreAccepted(txnId(), saveStatus(), durability, participants, promised, executeAt(), partialTxn(), partialDeps());
        }
    }

    public static class NotAcceptedWithoutDefinition extends Command
    {
        public static NotAcceptedWithoutDefinition notAccepted(ICommand copy, SaveStatus saveStatus)
        {
            return new NotAcceptedWithoutDefinition(copy, saveStatus);
        }

        public static NotAcceptedWithoutDefinition notAccepted(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Ballot accepted, PartialDeps partialDeps)
        {
            return new NotAcceptedWithoutDefinition(txnId, status, durability, participants, promised, accepted, partialDeps);
        }

        public static NotAcceptedWithoutDefinition acceptedInvalidate(ICommand copy)
        {
            return new NotAcceptedWithoutDefinition(copy, SaveStatus.AcceptedInvalidate);
        }

        private NotAcceptedWithoutDefinition(ICommand copy, SaveStatus saveStatus)
        {
            super(copy, saveStatus);
        }

        private NotAcceptedWithoutDefinition(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Ballot accepted, @Nullable PartialDeps partialDeps)
        {
            super(txnId, status, durability, participants, promised, null, null, partialDeps, accepted);
        }

        @Override
        public Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability)
        {
            return new NotAcceptedWithoutDefinition(txnId(), saveStatus(), durability, participants, promised, acceptedOrCommitted(), partialDeps());
        }
    }

    public static class Accepted extends PreAccepted
    {
        public static Accepted accepted(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted)
        {
            return validate(new Accepted(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted));
        }

        public static Accepted accepted(ICommand common, SaveStatus status)
        {
            return validate(new Accepted(common, status));
        }

        Accepted(ICommand copy, SaveStatus status)
        {
            super(copy, status);
            validateDeps(participants(), partialDeps());
        }

        Accepted(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted)
        {
            super(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted);
            validateDeps(participants(), partialDeps);
        }

        static void validateDeps(StoreParticipants participants, PartialDeps partialDeps)
        {
            if (Invariants.isParanoid() && Invariants.testParanoia(LINEAR, NONE, LOW))
            {
                Invariants.require(partialDeps == null
                                   || (participants.touches() == participants.stillTouches() && participants.touches().equals(partialDeps.covering))
                                   || (partialDeps.covers(participants.stillTouches()) && participants.touches().containsAll(partialDeps.covering))
                );
            }
        }

        @Override
        public Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability)
        {
            return validate(new Accepted(txnId(), saveStatus(), durability, participants, promised, executeAt(), partialTxn(), partialDeps(), acceptedOrCommitted()));
        }
    }

    public static class Committed extends Accepted
    {
        public static Committed committed(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted, WaitingOn waitingOn)
        {
            return validate(new Committed(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted, waitingOn));
        }

        public static Committed committed(ICommand copy, SaveStatus status)
        {
            return validate(new Committed(copy, status));
        }

        public static Committed committed(ICommand copy, SaveStatus status, WaitingOn overrideWaitingOn)
        {
            Invariants.require(status == (overrideWaitingOn.isWaiting() ? SaveStatus.Stable : ReadyToExecute));
            return validate(new Committed(copy, status, overrideWaitingOn));
        }

        public final WaitingOn waitingOn;

        Committed(ICommand copy, SaveStatus status)
        {
            this(copy, status, copy.waitingOn());
        }

        Committed(ICommand copy, SaveStatus status, WaitingOn overrideWaitingOn)
        {
            super(copy, status);
            this.waitingOn = overrideWaitingOn;
            Invariants.nonNull(copy.participants());
            Invariants.require(Route.isFullRoute(copy.participants().route()), "Expected a full route but given %s", copy.participants().route());
        }

        Committed(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted, WaitingOn waitingOn)
        {
            super(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted);
            this.waitingOn = waitingOn;
        }

        @Override
        public Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability)
        {
            Invariants.require(partialDeps().covers(participants.stillTouches()));
            return validate(new Committed(txnId(), saveStatus(), durability, participants, promised, executeAt(), partialTxn(), partialDeps(), acceptedOrCommitted(), waitingOn));
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

        public final WaitingOn waitingOn()
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
        public static Executed executed(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted, WaitingOn waitingOn, Writes writes, Result result)
        {
            return validate(new Executed(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted, waitingOn, writes, result));
        }

        public static Executed executed(ICommand common, SaveStatus status)
        {
            return validate(new Executed(common, status));
        }

        public static Executed executed(ICommand common, SaveStatus status, WaitingOn waitingOn)
        {
            return validate(new Executed(common, status, waitingOn));
        }

        private final Writes writes;
        private final Result result;

        private Executed(ICommand copy, SaveStatus status)
        {
            this(copy, status, copy.waitingOn());
        }

        private Executed(ICommand copy, SaveStatus status, WaitingOn overrideWaitingOn)
        {
            super(copy, status, overrideWaitingOn);
            this.writes = copy.writes();
            this.result = copy.result();
            validateWrites(txnId(), writes);
        }

        private Executed(TxnId txnId, SaveStatus status, Durability durability, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted, WaitingOn waitingOn, Writes writes, Result result)
        {
            super(txnId, status, durability, participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted, waitingOn);
            validateWrites(txnId, writes);
            this.writes = writes;
            this.result = result;
        }

        static void validateWrites(TxnId txnId, Writes writes)
        {
            Invariants.require(txnId.kind() != Write || writes != null, (writes == null ? "Expected writes for %s" : "Unexpected writes for %s"), txnId);
        }

        @Override
        public Command updateAttributes(StoreParticipants participants, Ballot promised, Durability durability)
        {
            Invariants.require(partialDeps().covers(participants.stillTouches()));
            return validate(new Executed(txnId(), saveStatus(), durability, participants, promised, executeAt(), partialTxn(), partialDeps(), acceptedOrCommitted(), waitingOn, writes, result));
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
        private static final WaitingOn EMPTY_FOR_KEY = new WaitingOn(RoutingKeys.EMPTY, RangeDeps.NONE, ImmutableBitSet.EMPTY, null);
        private static final WaitingOn EMPTY_FOR_RANGE = new WaitingOn(RoutingKeys.EMPTY, RangeDeps.NONE, ImmutableBitSet.EMPTY, ImmutableBitSet.EMPTY);

        public static WaitingOn empty(Routable.Domain domain)
        {
            if (domain == Range)
                return EMPTY_FOR_RANGE;
            return EMPTY_FOR_KEY;
        }

        public final RoutingKeys keys;
        public final RangeDeps directRangeDeps;
        // waitingOn ONLY encodes both txnIds and keys
        public final ImmutableBitSet waitingOn;
        public final @Nullable ImmutableBitSet appliedOrInvalidated;

        public WaitingOn(WaitingOn copy)
        {
            this(copy.keys, copy.directRangeDeps, copy.waitingOn, copy.appliedOrInvalidated);
        }

        public WaitingOn(RoutingKeys keys, RangeDeps directRangeDeps, ImmutableBitSet waitingOn, ImmutableBitSet appliedOrInvalidated)
        {
            this.keys = keys;
            this.directRangeDeps = directRangeDeps;
            this.waitingOn = waitingOn;
            this.appliedOrInvalidated = appliedOrInvalidated;
        }

        public int txnIdCount()
        {
            return directRangeDeps.txnIdCount();
        }

        public TxnId txnId(int i)
        {
            int ki = i - directRangeDeps.txnIdCount();
            if (ki < 0)
                return directRangeDeps.txnId(i);

            throw new IndexOutOfBoundsException(i + " >= " + txnIdCount());
        }

        int indexOf(TxnId txnId)
        {
            if (txnId.domain() == Range)
                return directRangeDeps.indexOf(txnId);

            throw illegalArgument("WaitingOn does not track this kind of TxnId: %s", txnId);
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
            return new WaitingOn(deps.keyDeps.keys(), deps.rangeDeps,
                                 new ImmutableBitSet(deps.keyDeps.keys().size() + deps.rangeDeps.txnIdCount()),
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
            Invariants.requireIndex(keyIndex, keys.size());
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
            return waitingOn.prevSetBit(directRangeTxnIdCount);
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

        public boolean equalBitSets(WaitingOn other)
        {
            return this.waitingOn.equals(other.waitingOn) && Objects.equals(this.appliedOrInvalidated, other.appliedOrInvalidated);
        }

        boolean equals(WaitingOn other)
        {
            return this.keys.equals(other.keys)
                && directRangeDeps.equals(other.directRangeDeps)
                && this.waitingOn.equals(other.waitingOn)
                && Objects.equals(this.appliedOrInvalidated, other.appliedOrInvalidated);
        }

        public static class Initialise extends Update implements ICommand
        {
            final TxnId txnId;
            final StoreParticipants participants;
            final Timestamp executeAt;
            final PartialDeps deps;

            private Initialise(TxnId txnId, StoreParticipants participants, Timestamp executeAt, PartialDeps deps)
            {
                super(txnId, deps.keyDeps.keys(), deps.rangeDeps);
                this.txnId = txnId;
                this.participants = participants;
                this.executeAt = executeAt;
                this.deps = deps;
            }

            @Override public TxnId txnId() { return txnId; }
            @Override public Status.Durability durability() { return null; }
            @Override public StoreParticipants participants() { return participants; }
            @Override public Ballot promised() { return null; }
            @Override public PartialTxn partialTxn() { return null; }
            @Override public PartialDeps partialDeps() { return deps; }
            @Override public Timestamp executeAt() { return executeAt; }
            @Override public Ballot acceptedOrCommitted() { return null; }
            @Override public WaitingOn waitingOn() { return null; }
            @Override public Writes writes() { return null; }
            @Override public Result result() { return null; }
        }

        public static class Update
        {
            final RoutingKeys keys;
            final RangeDeps directRangeDeps;
            private LargeBitSet waitingOn;
            private @Nullable LargeBitSet appliedOrInvalidated;
            private Timestamp executeAtLeast;
            private long uniqueHlc;
            private boolean executeAtLeastOrUniqueHlcUpdated;

            public Update(WaitingOn waitingOn)
            {
                this.keys = waitingOn.keys;
                this.directRangeDeps = waitingOn.directRangeDeps;
                this.waitingOn = waitingOn.waitingOn;
                this.appliedOrInvalidated = waitingOn.appliedOrInvalidated;
                if (waitingOn.getClass() == WaitingOnWithExecuteAt.class)
                    executeAtLeast = ((WaitingOnWithExecuteAt) waitingOn).executeAtLeast;
                if (waitingOn.getClass() == WaitingOnWithMinUniqueHlc.class)
                    uniqueHlc = ((WaitingOnWithMinUniqueHlc) waitingOn).uniqueHlc;
            }

            public Update(Committed committed)
            {
                this(committed.waitingOn);
            }

            private Update(TxnId txnId, RoutingKeys keys, RangeDeps directRangeDeps)
            {
                this.keys = keys;
                this.directRangeDeps = directRangeDeps;
                this.waitingOn = new LargeBitSet(txnIdCount() + keys.size(), false);
                this.appliedOrInvalidated = txnId.is(Key) ? null : new LargeBitSet(txnIdCount(), false);
            }

            public static Initialise initialise(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, StoreParticipants participants, PartialDeps deps)
            {
                Initialise initialise = new Initialise(txnId, participants, executeAt, deps);
                Participants<?> stillWaitsOn = participants.stillWaitsOn();
                // TODO (expected): refactor this to operate only on participants, not ranges
                if (stillWaitsOn.domain() == Range) deps.keyDeps.keys().forEach((AbstractRanges)stillWaitsOn, (upd, key, index) -> upd.initialise(index + upd.txnIdCount()), initialise);
                else deps.keyDeps.keys().forEach((AbstractUnseekableKeys)stillWaitsOn, (upd, key, index) -> upd.initialise(index + upd.txnIdCount()), initialise);
                deps.rangeDeps.forEach(stillWaitsOn, initialise, Update::initialise);
                if (txnId.isSyncPoint())
                {
                    // sync points wait on ranges we have lost but not closed, but the sync point may have dependencies
                    // that were started after we lost the ranges; we do not need to wait for these, so remove them
                    CommandStores.RangesForEpoch ranges = safeStore.ranges();
                    RangeDeps rDeps = deps.rangeDeps;
                    int txnIdx = rDeps.txnIdCount() - 1;
                    int latest = ranges.floorIndex(txnId.epoch());
                    Ranges all = ranges.all();
                    for (int i = latest ; txnIdx >= 0 && i >= 1 ; i--)
                    {
                        long epoch = ranges.epochAtIndex(i);
                        Ranges removed = all.without(ranges.rangesAtIndex(i));
                        while (txnIdx >= 0 && rDeps.txnId(txnIdx).epoch() >= epoch)
                        {
                            if (!rDeps.txnId(txnIdx).isSyncPoint() && initialise.isWaitingOnDirectRangeTxnIdx(txnIdx))
                            {
                                Ranges rs = rDeps.ranges(txnIdx).slice(all, Slice.Minimal);
                                if (removed.containsAll(rs))
                                    initialise.removeWaitingOnDirectRangeTxnId(txnIdx);
                            }
                            --txnIdx;
                        }
                    }
                }
                return initialise;
            }

            @VisibleForTesting
            public static Update unsafeInitialise(TxnId txnId, Ranges executeRanges, Route<?> route, Deps deps)
            {
                Unseekables<?> executionParticipants = route.slice(executeRanges, Slice.Minimal);
                Update update = new Update(txnId, deps.keyDeps.keys(), deps.rangeDeps);
                // TODO (expected): refactor this to operate only on participants, not ranges
                deps.rangeDeps.forEach(executionParticipants, update, Update::initialise);
                deps.keyDeps.keys().forEach(executeRanges, (upd, key, index) -> upd.initialise(index + upd.txnIdCount()), update);
                return update;
            }

            void initialise(int i)
            {
                waitingOn.set(i);
            }

            public boolean hasChanges()
            {
                return !(waitingOn instanceof ImmutableBitSet)
                       || (appliedOrInvalidated != null && !(appliedOrInvalidated instanceof ImmutableBitSet))
                       || executeAtLeastOrUniqueHlcUpdated;
            }

            public TxnId txnId(int i)
            {
                int ki = i - directRangeDeps.txnIdCount();
                if (ki < 0)
                    return directRangeDeps.txnId(i);
                throw new IndexOutOfBoundsException(i + " >= " + txnIdCount());
            }

            public int indexOf(TxnId txnId)
            {
                if (txnId.domain() == Range)
                    return directRangeDeps.indexOf(txnId);
                throw illegalArgument("WaitingOn does not track this kind of TxnId: " + txnId);
            }

            public int txnIdCount()
            {
                return directRangeDeps.txnIdCount();
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
                return waitingOn.nextSetBitBefore(0, directRangeTxnIdCount);
            }

            public boolean isWaitingOnDirectRangeTxnIdx(int idx)
            {
                Invariants.requireIndex(idx, directRangeDeps.txnIdCount());
                return waitingOn.get(idx);
            }

            public void updateExecuteAtLeast(TxnId txnId, Timestamp executeAtLeast)
            {
                Invariants.require(txnId.awaitsOnlyDeps());
                Invariants.require(uniqueHlc == 0);
                Invariants.require(executeAtLeast.compareTo(txnId) > 0);
                if (this.executeAtLeast == null || this.executeAtLeast.compareTo(executeAtLeast) < 0)
                {
                    this.executeAtLeast = executeAtLeast;
                    this.executeAtLeastOrUniqueHlcUpdated = true;
                }
            }

            public void updateUniqueHlc(Timestamp executeAt, long uniqueHlc)
            {
                Invariants.require(executeAtLeast == null);
                if (uniqueHlc > executeAt.hlc() && uniqueHlc > this.uniqueHlc)
                {
                    this.uniqueHlc = uniqueHlc;
                    this.executeAtLeastOrUniqueHlcUpdated = true;
                }
            }

            boolean removeWaitingOnDirectRangeTxnId(int i)
            {
                Invariants.requireIndex(i, directRangeDeps.txnIdCount());
                return removeWaitingOn(i);
            }

            boolean removeWaitingOnKey(int i)
            {
                Invariants.requireIndex(i, keys.size());
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
                Invariants.requireIndex(i, directRangeDeps.txnIdCount());
                return setAppliedOrInvalidated(i);
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
                    forEachIntersection(propagate.directRangeDeps.txnIdsWithFlags(), directRangeDeps.txnIdsWithFlags(),
                                        (from, to, ignore, i1, i2) -> {
                                            if (from.get(i1))
                                                to.setAppliedOrInvalidatedDirectRangeTxn(i2);
                                        }, propagate.appliedOrInvalidated, this, null);
                }

                return true;
            }

            public <P1, P2, P3, P4> void forEachWaitingOnId(P1 p1, P2 p2, P3 p3, P4 p4, IndexedQuadConsumer<P1, P2, P3, P4> forEach)
            {
                waitingOn.reverseForEach(0, txnIdCount(), p1, p2, p3, p4, forEach);
            }

            public <P1, P2, P3> void forEachWaitingOnKey(P1 p1, P2 p2, P3 p3, IndexedTriConsumer<P1, P2, P3> forEach)
            {
                waitingOn.reverseForEach(txnIdCount(), txnIdCount() + keys.size(), p1, p2, p3, this, (pp1, pp2, pp3, pp4, i) -> forEach.accept(pp1, pp2, pp3, i - pp4.txnIdCount()));
            }

            public WaitingOn build()
            {
                WaitingOn result = new WaitingOn(keys, directRangeDeps, ensureImmutable(waitingOn), ensureImmutable(appliedOrInvalidated));
                if (executeAtLeast == null && uniqueHlc == 0)
                    return result;
                if (executeAtLeast == null)
                    return new WaitingOnWithMinUniqueHlc(result, uniqueHlc);
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

        /**
         * Note: we don't guarantee to maintain this once an actual uniqueHlc is known.
         */
        public long minUniqueHlc()
        {
            return 0;
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

    public static final class WaitingOnWithMinUniqueHlc extends WaitingOn
    {
        public final long uniqueHlc;
        public WaitingOnWithMinUniqueHlc(WaitingOn waitingOn, long uniqueHlc)
        {
            super(waitingOn);
            this.uniqueHlc = uniqueHlc;
        }

        @Override
        public long minUniqueHlc()
        {
            return uniqueHlc;
        }

        @Override
        public boolean equals(Object other)
        {
            return other.getClass() == WaitingOnWithMinUniqueHlc.class && this.equals((WaitingOnWithMinUniqueHlc) other);
        }

        boolean equals(WaitingOnWithMinUniqueHlc other)
        {
            return super.equals(other) && uniqueHlc == other.uniqueHlc;
        }
    }

    static Command.Committed updateWaitingOn(Committed command, WaitingOn.Update waitingOn)
    {
        if (!waitingOn.hasChanges())
            return command;

        return command instanceof Command.Executed ?
               executed(command, command.saveStatus(), waitingOn.build()) :
               committed(command, waitingOn.isWaiting() ? SaveStatus.Stable : ReadyToExecute, waitingOn.build());
    }

    static Command.PreAccepted preaccept(Command command, SaveStatus saveStatus, StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps)
    {
        if (command.status() == Status.NotDefined)
        {
            return Command.PreAccepted.preaccepted(command.txnId(), saveStatus, command.durability(), participants, promised, executeAt, partialTxn, partialDeps);
        }
        else if (command.executeAt() == null && command.status().phase == Phase.Accept)
        {
            SaveStatus prevSaveStatus = command.saveStatus();
            Invariants.require(prevSaveStatus == AcceptedInvalidate);
            // TODO (desired): reconsider this special-casing
            return Command.Accepted.accepted(command.txnId(), SaveStatus.enrich(prevSaveStatus, SaveStatus.PreAccepted.known), command.durability(), participants, promised, executeAt, partialTxn, partialDeps, command.acceptedOrCommitted());
        }
        else
        {
            Invariants.require(command.status() == Status.AcceptedMedium);
            return (Command.PreAccepted) command.updatePromised(promised);
        }
    }

    static Command.Accepted markDefined(Command command, StoreParticipants newParticipants, Ballot promised, PartialTxn partialTxn)
    {
        Invariants.require(!(command instanceof Committed));
        return Command.Accepted.accepted(command.txnId(), SaveStatus.withDefinition(command.saveStatus()), command.durability(),
                                         newParticipants, promised, command.executeAt(), partialTxn, command.partialDeps(),
                                         command.acceptedOrCommitted());
    }

    static Command.Accepted accept(Command current, SaveStatus saveStatus, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Ballot acceptedOrCommitted)
    {
        Invariants.require(saveStatus.status == Status.AcceptedSlow || saveStatus.status == Status.AcceptedMedium || saveStatus.status == Status.PreCommitted);
        return validate(new Command.Accepted(current.txnId(), saveStatus, current.durability(), participants, promised, executeAt, partialTxn, partialDeps, acceptedOrCommitted));
    }

    static Command notAccept(Status newStatus, Command copy, Ballot ballot)
    {
        Invariants.requireArgument(newStatus == Status.AcceptedInvalidate);

        SaveStatus saveStatus = SaveStatus.get(newStatus, copy.known());

        PartialDeps partialDeps = copy.partialDeps();
        if (saveStatus.known.is(DepsUnknown))
            partialDeps = null;

        if (!saveStatus.known.isDefinitionKnown())
            return validate(notAccepted(copy.txnId, saveStatus, copy.durability, copy.participants, ballot, ballot, partialDeps));

        return validate(new Command.Accepted(copy.txnId, saveStatus, copy.durability, copy.participants, ballot, copy.executeAt(), copy.partialTxn(), partialDeps, ballot));
    }

    static Command.Committed commit(Command command, @Nonnull StoreParticipants participants, Ballot ballot, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps)
    {
        return validate(committed(command.txnId(), SaveStatus.get(Status.Committed, command.known()), command.durability(), participants, ballot, executeAt, partialTxn, partialDeps, ballot, null));
    }

    static Command.Committed stable(Command command, @Nonnull StoreParticipants participants, Ballot ballot, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, WaitingOn waitingOn)
    {
        return validate(committed(command.txnId(), SaveStatus.Stable, command.durability(), participants,
                                  Ballot.max(ballot, command.promised()), executeAt, partialTxn, partialDeps,
                                  Ballot.max(ballot, command.acceptedOrCommitted()), waitingOn));
    }

    static Command precommit(Command command, Timestamp executeAt, Ballot promisedAtLeast)
    {
        SaveStatus saveStatus = SaveStatus.get(Status.PreCommitted, command.known());
        PartialDeps partialDeps = command.partialDeps();
        if (!saveStatus.known.deps().hasProposedOrDecidedDeps() && command.partialDeps() != null)
            partialDeps = null;
        return validate(new Command.Accepted(command.txnId, saveStatus, command.durability, command.participants, Ballot.max(command.promised, promisedAtLeast), executeAt, command.partialTxn, partialDeps, command.acceptedOrCommitted));
    }

    static Command.Committed readyToExecute(Command.Committed command)
    {
        return committed(command, SaveStatus.ReadyToExecute);
    }

    static Command.Executed preapplied(Command command, @Nonnull StoreParticipants participants, Ballot promised, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return executed(command.txnId(), SaveStatus.get(Status.PreApplied, command.known()), command.durability(), participants, promised, executeAt, partialTxn, partialDeps, command.acceptedOrCommitted(), waitingOn, writes, result);
    }

    static Command.Executed applying(Command command, @Nonnull StoreParticipants participants, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return executed(command.txnId(), SaveStatus.Applying, command.durability(), participants, command.promised(), executeAt, partialTxn, partialDeps, command.acceptedOrCommitted(), waitingOn, writes, result);
    }

    static Command.Executed applying(Command.Executed command)
    {
        return executed(command, SaveStatus.Applying);
    }

    static Command.Executed applied(Command command, @Nonnull StoreParticipants participants, Timestamp executeAt, PartialTxn partialTxn, PartialDeps partialDeps, Command.WaitingOn waitingOn, Writes writes, Result result)
    {
        return executed(command.txnId(), SaveStatus.Applied, command.durability(), participants, command.promised(), executeAt, partialTxn, partialDeps, command.acceptedOrCommitted(), waitingOn, writes, result);
    }

    static Command.Executed applied(Command.Executed command)
    {
        return executed(command, SaveStatus.Applied);
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
            case PreAcceptedWithDeps:
            case PreAcceptedWithVote:
                return validateCommandClass(status, PreAccepted.class, klass);
            case AcceptedInvalidate:
                return validateCommandClass(status, NotAcceptedWithoutDefinition.class, klass);
            case AcceptedInvalidateWithDefinition:
            case AcceptedMedium:
            case AcceptedMediumWithDefinition:
            case AcceptedMediumWithDefAndVote:
            case AcceptedSlow:
            case AcceptedSlowWithDefinition:
            case AcceptedSlowWithDefAndVote:
            case PreCommitted:
            case PreCommittedWithDeps:
            case PreCommittedWithFixedDeps:
            case PreCommittedWithDefinition:
            case PreCommittedWithDefAndDeps:
            case PreCommittedWithDefAndFixedDeps:
                return validateCommandClass(status, Accepted.class, klass);
            case Committed:
            case ReadyToExecute:
            case Stable:
                return validateCommandClass(status, Committed.class, klass);
            case PreApplied:
            case Applying:
            case Applied:
                return validateCommandClass(status, Executed.class, klass);
            case TruncatedUnapplied:
            case TruncatedApply:
            case TruncatedApplyWithOutcome:
                if (txnId.awaitsOnlyDeps())
                    return validateCommandClass(status, TruncatedAwaitsOnlyDeps.class, klass);
            case Erased:
            case Vestigial:
            case Invalidated:
                return validateCommandClass(status, Truncated.class, klass);
            default:
                throw illegalState("Unhandled status " + status);
        }
    }

    public static <T extends Command> T validate(T validate)
    {
        TxnId txnId = validate.txnId();
        Invariants.require(txnId.hasOnlyIdentityFlags(), "%s has non-identity flags", txnId);

        SaveStatus saveStatus = validate.saveStatus();
        if (saveStatus == Erased)
            return validate;

        StoreParticipants participants = validate.participants();
        Invariants.require(!participants.hasTouched().isEmpty() || saveStatus == Uninitialised, "%s(%s): hasTouched is empty. %s", txnId, saveStatus, participants);
        Known known = validate.known();

        {
            Route<?> route = validate.route();
            switch (known.route())
            {
                default: throw new UnhandledEnum(known.route());
                case MaybeRoute: break;
                case FullRoute: Invariants.require(Route.isFullRoute(route), "%s(%s): missing FullRoute; have %s", txnId, saveStatus, route); break;
                case CoveringRoute: Invariants.require(Route.isRoute(route), "%s(%s): missing Route", txnId, saveStatus); break;
            }
        }

        {
            PartialTxn partialTxn = validate.partialTxn();
            switch (known.definition())
            {
                default: throw new UnhandledEnum(known.definition());
                case DefinitionErased:
                case DefinitionUnknown:
                case NoOp:
                    Invariants.require(partialTxn == null, "partialTxn is defined %s", validate);
                    break;
                case DefinitionKnown:
                    Invariants.require(partialTxn != null || validate.participants().stillOwns().isEmpty(), "partialTxn is null");
                    break;
            }
        }
        {
            Timestamp executeAt = validate.executeAt();
            switch (known.executeAt())
            {
                default: throw new UnhandledEnum(known.executeAt());
                case ExecuteAtErased:
                case ExecuteAtUnknown:
                    Invariants.require(executeAt == null || executeAt.compareTo(validate.txnId()) >= 0);
                    break;
                case ExecuteAtKnown:
                case ApplyAtKnown:
                    // TODO (expected): enable this invariant; requires rethinking how we update StoreParticipants on PreCommitted
                    //     which must be done carefully as we cannot mess with touches()/Deps as the relationship there must be maintained
                    //     for state machine correctness
                case ExecuteAtProposed:
                    Invariants.require(executeAt != null);
                    int c =  executeAt.compareTo(validate.txnId());
                    Invariants.require(c > 0 || (c == 0 && executeAt.getClass() != Timestamp.class));
                    break;
                case NoExecuteAt:
                    Invariants.require(executeAt.equals(Timestamp.NONE));
                    break;
            }
        }
        {
            PartialDeps deps = validate.partialDeps();
            switch (known.deps())
            {
                default: throw new UnhandledEnum(known.deps());
                case DepsUnknown:
                case DepsErased:
                case NoDeps:
                    Invariants.require(deps == null, "Deps should have been null, but were %s", deps);
                    break;
                case DepsFromCoordinator:
                case DepsProposedFixed:
                case DepsProposed:
                case DepsCommitted:
                case DepsKnown:
                    Invariants.require(deps != null);
                    break;
            }
        }
        {
            Writes writes = validate.writes();
            Result result = validate.result();
            switch (known.outcome())
            {
                default: throw new UnhandledEnum(known.outcome());
                case Apply:
                    if (validate.txnId().is(Write)) Invariants.require(writes != null, "Writes is null %s", validate);
                    else Invariants.require(writes == null, "Writes is not-null %s", validate);
                    Invariants.require(result != null, "Result is null %s", validate);
                    break;
                case Abort:
                    Invariants.require(validate.durability().isMaybeInvalidated(), "%s is not invalidated", validate.durability());
                case Unknown:
                case Erased:
                case WasApply:
                    Invariants.require(writes == null, "Writes exist for %s", validate);
                    Invariants.require(result == null, "Results exist %s %s", validate, result);
                    break;
            }
        }
        switch (saveStatus.execution)
        {
            case NotReady:
            case CleaningUp:
                break;
            case ReadyToExclude:
                Invariants.require(saveStatus != SaveStatus.Committed || validate.asCommitted().waitingOn == null);
                break;
            case WaitingToExecute:
            case ReadyToExecute:
            case Applied:
            case Applying:
            case WaitingToApply:
                Invariants.require(validate.participants().executes() != null);
                Invariants.require(validate.asCommitted().waitingOn != null);
                break;
        }
        return validate;
    }

    private static boolean isSameClass(Command command, Class<? extends Command> klass)
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
}
