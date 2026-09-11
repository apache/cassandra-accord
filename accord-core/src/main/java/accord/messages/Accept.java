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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Commands.AcceptOutcome;
import accord.local.DepsCalculator;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import static accord.api.ProtocolModifiers.filterDuplicateDependenciesFromAcceptReply;
import static accord.api.ProtocolModifiers.syncPointsTrackUnstableMediumPathDependencies;
import static accord.local.Commands.AcceptOutcome.Redundant;
import static accord.local.Commands.AcceptOutcome.RejectedBallot;
import static accord.local.Commands.AcceptOutcome.Success;
import static accord.local.LoadKeys.SYNC;
import static accord.messages.MessageType.StandardMessage.ACCEPT_REQ;
import static accord.messages.MessageType.StandardMessage.ACCEPT_RSP;
import static accord.messages.MessageType.StandardMessage.NOT_ACCEPT_REQ;
import static accord.primitives.Known.KnownDeps.DepsKnown;

// TODO (low priority, efficiency): use different objects for send and receive, so can be more efficient
//                                  (e.g. serialize without slicing, and without unnecessary fields)
public class Accept extends RouteRequest.WithUnsynced<Accept.AcceptReply>
{
    public static class SerializerSupport
    {
        public static Accept create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Kind kind, Ballot ballot, Timestamp executeAt, PartialDeps partialDeps, int acceptFlags)
        {
            return new Accept(txnId, scope, waitForEpoch, minEpoch, kind, ballot, executeAt, partialDeps, acceptFlags);
        }
    }

    public enum Kind { SLOW, MEDIUM }

    public enum AcceptFlags
    {
        IS_PARTIAL,

        /**
         * SyncPoints don't need to compute any deps in the Accept phase,
         * as any dependencies they did not witness in the preaccept phase cannot reach consensus
         */
        NO_CALCULATE_DEPS,

        /**
         * See {@link accord.api.ProtocolModifiers#filterDuplicateDependenciesFromAcceptReply()}.
         */
        NO_FILTER_DEPS;

        public static int encode(TxnId txnId, boolean isPartial)
        {
            return (isPartial ? TinyEnumSet.encode(IS_PARTIAL) : 0)
                   | (txnId.isSyncPoint() && !syncPointsTrackUnstableMediumPathDependencies() ? TinyEnumSet.encode(NO_CALCULATE_DEPS) : 0)
                   | (filterDuplicateDependenciesFromAcceptReply() ? 0 : TinyEnumSet.encode(NO_FILTER_DEPS));
        }

        public static boolean isPartial(int encoded)
        {
            return TinyEnumSet.contains(encoded, IS_PARTIAL);
        }

        public static boolean filterDeps(int encoded)
        {
            return !TinyEnumSet.contains(encoded, NO_FILTER_DEPS);
        }

        public static boolean calculateDeps(int encoded)
        {
            return !TinyEnumSet.contains(encoded, NO_CALCULATE_DEPS);
        }
    }

    public final Kind kind;
    public final Ballot ballot;
    public final Timestamp executeAt;
    private PartialDeps partialDeps;
    public final int acceptFlags;

    public PartialDeps partialDeps() { return partialDeps; }

    public Accept(Id to, Topologies topologies, Kind kind, Ballot ballot, TxnId txnId, FullRoute<?> route, Timestamp executeAt, Deps deps, int acceptFlags)
    {
        super(to, topologies, txnId, route);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialDeps = deps.intersecting(scope);
        this.acceptFlags = acceptFlags;
    }

    private Accept(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Kind kind, Ballot ballot, Timestamp executeAt, PartialDeps partialDeps, int acceptFlags)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialDeps = partialDeps;
        this.acceptFlags = acceptFlags;
    }

    @Override
    protected boolean abort(Refuse.MinMax refuses)
    {
        return refuses.max != Refuse.NONE;
    }
    
    @Override
    public AcceptReply applyInternal(SafeCommandStore safeStore)
    {
        PartialDeps partialDeps = this.partialDeps;
        if (ifDoneExpectCancelled()) // check cancellation after reading nullable fields
            return null; // we can't throw an exception here else we override any non-exceptional reply informing the reason

        StoreParticipants participants = StoreParticipants.update(safeStore, scope, minEpoch, txnId, txnId.epoch(), executeAt.epoch());
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        AcceptOutcome outcome = Commands.accept(safeStore, safeCommand, participants, txnId, kind, ballot, scope, executeAt, partialDeps);
        switch (outcome)
        {
            default: throw new UnhandledEnum(outcome);
            case Redundant:
            case Truncated:
            {
                Command command = safeCommand.current();

                boolean notOwner = participants.owns().isEmpty();
                Participants<?> hasDeps = null;
                Deps deps = null;

                if (command.known().is(DepsKnown) && (isPartialAccept() || notOwner))
                {
                    deps = command.partialDeps().asFullUnsafe();
                    hasDeps = command.participants().stillTouches();
                }

                Ballot superseding = command.promised();
                if (superseding.compareTo(ballot) <= 0)
                    superseding = null;

                boolean calculateDeps = isPartialAccept() && calculateDeps();
                if (command.saveStatus() == SaveStatus.Vestigial)
                {
                    superseding = null;
                    outcome = Success;
                    calculateDeps = calculateDeps();
                }

                if (calculateDeps)
                {
                    Participants<?> calculate = participants.touches();
                    if (hasDeps != null)
                        calculate = calculate.without(hasDeps);

                    if (!calculate.isEmpty())
                    {
                        Deps calculatedDeps = DepsCalculator.calculateDeps(safeStore, txnId, calculate, minEpoch, executeAt, true);
                        if (calculatedDeps == null)
                            return AcceptReply.inThePast(ballot, participants, command);

                        deps = deps == null ? calculatedDeps : calculatedDeps.with(deps);
                    }
                    hasDeps = participants.touches();
                }

                Participants<?> successful = isPartialAccept() ? hasDeps : null;
                if (notOwner && (outcome == Redundant || (hasDeps != null && hasDeps.containsAll(participants.touches()))))
                    outcome = Success;

                if (deps != null && filterDeps())
                    deps = deps.without(partialDeps);
                return new AcceptReply(outcome, superseding, successful, deps, command.executeAtIfKnown());
            }

            case RejectedBallot:
                return new AcceptReply(safeCommand.current().promised());

            case Retired:
                // if we're Retired, participants.owns() is empty, so we're just fetching deps
                // TODO (desired): optimise deps calculation; for some keys we only need to return the last RX
            case Success:
                ExecuteFlags flags;
                Deps deps;
                if (calculateDeps())
                {
                    try (DepsCalculator calculator = new DepsCalculator(executeAt))
                    {
                        deps = calculator.calculate(safeStore, txnId, participants, minEpoch, executeAt, true);
                        if (deps == null)
                            return AcceptReply.inThePast(ballot, participants, safeCommand.current());
                        flags = calculator.executeFlags(txnId);
                    }

                    Invariants.require(deps.maxTxnId(txnId).epoch() <= executeAt.epoch());
                    if (filterDeps())
                        deps = deps.without(partialDeps);
                }
                else
                {
                    flags = ExecuteFlags.none();
                    deps = Deps.NONE;
                }

                Participants<?> successful = isPartialAccept() ? participants.touches() : null;
                return new AcceptReply(successful, deps, flags);
        }
    }

    private boolean isPartialAccept()
    {
        return AcceptFlags.isPartial(acceptFlags);
    }

    private boolean calculateDeps()
    {
        return AcceptFlags.calculateDeps(acceptFlags);
    }

    private boolean filterDeps()
    {
        return AcceptFlags.filterDeps(acceptFlags);
    }

    @Override
    public AcceptReply reduce(AcceptReply r1, AcceptReply r2)
    {
        return AcceptReply.reduce(r1, r2);
    }

    @Override
    public Cancellable submit()
    {
        return node.commandStores().mapReduceConsume(minEpoch, executeAt.epoch(), this);
    }

    @Override
    protected void acceptInternal(AcceptReply reply, Throwable failure)
    {
        // finished processing, null out large objects
        partialDeps = null;
        super.acceptInternal(reply, failure);
    }

    @Override
    public LoadKeys loadKeys()
    {
        return SYNC;
    }

    @Override
    public LoadKeysFor loadKeysFor()
    {
        return calculateDeps() ? LoadKeysFor.READ_WRITE : LoadKeysFor.WRITE;
    }

    public ExecutionKind executionKind()
    {
        return ExecutionKind.ACCEPT;
    }

    @Override
    public MessageType type()
    {
        return ACCEPT_REQ;
    }

    public String toString() {
        return "Accept{" +
                "kind: " + kind +
                ", ballot: " + ballot +
                ", txnId: " + txnId +
                ", executeAt: " + executeAt +
                ", deps: " + partialDeps +
                '}';
    }

    public static final class AcceptReply implements Reply
    {
        public static final AcceptReply SUCCESS = new AcceptReply(Success);

        public final AcceptOutcome outcome;
        public final Ballot supersededBy;
        public final @Nullable Participants<?> successful;
        public final @Nullable Deps deps;
        public final @Nullable Timestamp committedExecuteAt;
        public final ExecuteFlags flags;

        private AcceptReply(AcceptOutcome outcome)
        {
            this.outcome = outcome;
            this.supersededBy = null;
            this.successful = null;
            this.deps = null;
            this.committedExecuteAt = null;
            this.flags = ExecuteFlags.none();
        }

        public AcceptReply(Ballot supersededBy)
        {
            this.outcome = RejectedBallot;
            this.supersededBy = supersededBy;
            this.successful = null;
            this.deps = null;
            this.committedExecuteAt = null;
            this.flags = ExecuteFlags.none();
        }

        public AcceptReply(@Nullable Participants<?> successful, @Nonnull Deps deps)
        {
            this(successful, deps, ExecuteFlags.none());
        }

        public AcceptReply(@Nullable Participants<?> successful, @Nonnull Deps deps, ExecuteFlags flags)
        {
            this.outcome = Success;
            this.supersededBy = null;
            this.successful = successful;
            this.deps = deps;
            this.committedExecuteAt = null;
            this.flags = flags;
        }

        public AcceptReply(AcceptOutcome outcome, Ballot supersededBy, @Nullable Timestamp committedExecuteAt)
        {
            this.outcome = outcome;
            this.supersededBy = supersededBy;
            this.successful = null;
            this.deps = null;
            this.committedExecuteAt = committedExecuteAt;
            this.flags = ExecuteFlags.none();
        }

        public AcceptReply(AcceptOutcome outcome, Ballot supersededBy, @Nullable Participants<?> successful, @Nullable Deps deps,  @Nullable Timestamp committedExecuteAt)
        {
            this(outcome, supersededBy, successful, deps, committedExecuteAt, ExecuteFlags.none());
        }

        public AcceptReply(AcceptOutcome outcome, Ballot supersededBy, @Nullable Participants<?> successful, @Nullable Deps deps,  @Nullable Timestamp committedExecuteAt, ExecuteFlags flags)
        {
            this.outcome = outcome;
            this.supersededBy = supersededBy;
            this.successful = successful;
            this.deps = deps;
            this.committedExecuteAt = committedExecuteAt;
            this.flags = flags;
        }

        static AcceptReply redundant(AcceptOutcome outcome, Ballot ballot, Command command)
        {
            Ballot superseding = command.promised();
            if (superseding.compareTo(ballot) <= 0)
                superseding = null;

            return new AcceptReply(outcome, superseding, command.executeAtIfKnown());
        }

        static AcceptReply inThePast(Ballot ballot, StoreParticipants participants, Command command)
        {
            if (participants.owns().isEmpty() && (command.is(Status.Truncated) || command.promised().compareTo(ballot) <= 0))
                return new AcceptReply(participants.owns(), Deps.NONE);

            return redundant(Redundant, ballot, command);
        }

        public static AcceptReply reduce(AcceptReply r1, AcceptReply r2)
        {
            if (r1 == null || r2 == null)
                return r1 != null ? r1 : r2;
            AcceptOutcome o1 = r1.outcome(), o2 = r2.outcome();
            AcceptOutcome o = o1.compareTo(o2) >= 0 ? o1 : o2;
            Deps deps = r1.deps == null ? r2.deps : r2.deps == null ? r1.deps : r1.deps.with(r2.deps);
            Participants<?> successful = Participants.merge(r1.successful, (Participants)r2.successful);
            Ballot supersededBy = Ballot.nonNullOrMax(r1.supersededBy, r2.supersededBy);
            Timestamp committedExecuteAt = r1.committedExecuteAt != null ? r1.committedExecuteAt : r2.committedExecuteAt;
            if (o == r1.outcome && successful == r1.successful && deps == r1.deps && supersededBy == r1.supersededBy && committedExecuteAt == r1.committedExecuteAt)
                return r1;
            if (o == r2.outcome && successful == r2.successful && deps == r2.deps && supersededBy == r2.supersededBy && committedExecuteAt == r2.committedExecuteAt)
                return r2;
            return new AcceptReply(o, supersededBy, successful, deps, committedExecuteAt);
        }

        @Override
        public MessageType type()
        {
            return ACCEPT_RSP;
        }

        public boolean isOk()
        {
            return outcome == Success;
        }

        public AcceptOutcome outcome()
        {
            return outcome;
        }

        @Override
        public String toString()
        {
            switch (outcome)
            {
                default: throw new AssertionError();
                case Success:
                    return "AcceptOk{deps=" + deps + '}';
                case Redundant:
                    return "AcceptRedundant(" + supersededBy + ',' + committedExecuteAt + ")";
                case RejectedBallot:
                    return "AcceptNack(" + supersededBy + ")";
            }
        }
    }

    @Override
    public Timestamp executeAt()
    {
        return executeAt;
    }

    public static class NotAccept extends ParticipantsRequest<Participants<?>, AcceptReply>
    {
        public final Status status;
        public final Ballot ballot;

        public NotAccept(Status status, Ballot ballot, TxnId txnId, Participants<?> participants)
        {
            super(txnId, participants, txnId.epoch());
            this.status = status;
            this.ballot = ballot;
        }

        @Override
        public Cancellable submit()
        {
            return node.commandStores().mapReduceConsume(txnId.epoch(), txnId.epoch(), this);
        }

        @Override
        public AcceptReply applyInternal(SafeCommandStore safeStore)
        {
            StoreParticipants participants = StoreParticipants.notAccept(safeStore, scope, txnId);
            SafeCommand safeCommand = safeStore.get(txnId, participants);
            AcceptOutcome outcome = Commands.notAccept(safeStore, safeCommand, status, ballot);
            switch (outcome)
            {
                default: throw new IllegalArgumentException("Unknown status: " + outcome);
                case Redundant:
                case Truncated:
                    // TODO (required): consider more fully how to safely handle (and add dedicated testing of)
                    //    all the various combinations of states we might encounter when some shard or replica of a shard has truncated
                    if (safeCommand.current().saveStatus() != SaveStatus.Vestigial)
                        return AcceptReply.redundant(outcome, ballot, safeCommand.current());
                case Retired:
                case Success:
                    return AcceptReply.SUCCESS;
                case RejectedBallot:
                    return new AcceptReply(safeCommand.current().promised());
            }
        }

        @Override
        public AcceptReply reduce(AcceptReply r1, AcceptReply r2)
        {
            return AcceptReply.reduce(r1, r2);
        }

        @Override
        public MessageType type()
        {
            return NOT_ACCEPT_REQ;
        }

        @Override
        public String toString()
        {
            return "NotAccept{kind: " + status + ", ballot:" + ballot + ", txnId:" + txnId + ", key:" + scope + '}';
        }

        @Override
        public String reason()
        {
            return status.toString();
        }
    }
}
