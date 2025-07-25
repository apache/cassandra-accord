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
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.Commands.AcceptOutcome;
import accord.local.DepsCalculator;
import accord.local.KeyHistory;
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
import accord.utils.IndexedTriFold;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import static accord.api.ProtocolModifiers.Toggles.filterDuplicateDependenciesFromAcceptReply;
import static accord.local.Commands.AcceptOutcome.Redundant;
import static accord.local.Commands.AcceptOutcome.RejectedBallot;
import static accord.local.Commands.AcceptOutcome.Success;
import static accord.local.KeyHistory.SYNC;
import static accord.messages.MessageType.StandardMessage.ACCEPT_REQ;
import static accord.messages.MessageType.StandardMessage.ACCEPT_RSP;
import static accord.messages.MessageType.StandardMessage.NOT_ACCEPT_REQ;
import static accord.primitives.Known.KnownDeps.DepsKnown;

// TODO (low priority, efficiency): use different objects for send and receive, so can be more efficient
//                                  (e.g. serialize without slicing, and without unnecessary fields)
public class Accept extends TxnRequest.WithUnsynced<Accept.AcceptReply>
{
    public static class SerializerSupport
    {
        public static Accept create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Kind kind, Ballot ballot, Timestamp executeAt, PartialDeps partialDeps, boolean isPartialAccept)
        {
            return new Accept(txnId, scope, waitForEpoch, minEpoch, kind, ballot, executeAt, partialDeps, isPartialAccept);
        }
    }

    public enum Kind { SLOW, MEDIUM }

    public final Kind kind;
    public final Ballot ballot;
    public final Timestamp executeAt;
    public final PartialDeps partialDeps;
    public final boolean isPartialAccept;

    public Accept(Id to, Topologies topologies, Kind kind, Ballot ballot, TxnId txnId, FullRoute<?> route, Timestamp executeAt, Deps deps, boolean isPartialAccept)
    {
        super(to, topologies, txnId, route);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialDeps = deps.intersecting(scope);
        this.isPartialAccept = isPartialAccept;
    }

    private Accept(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Kind kind, Ballot ballot, Timestamp executeAt, PartialDeps partialDeps, boolean isPartialAccept)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.kind = kind;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialDeps = partialDeps;
        this.isPartialAccept = isPartialAccept;
    }

    @Override
    public AcceptReply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, scope, minEpoch, txnId, txnId.epoch(), executeAt.epoch());
        if (!safeStore.safeToCoordinate(txnId, participants.touches()))
            throw new CommandStore.NotReadyException();

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

                if (command.known().is(DepsKnown) && (isPartialAccept || notOwner))
                {
                    deps = command.partialDeps().asFullUnsafe();
                    hasDeps = command.participants().stillTouches();
                }

                Ballot superseding = command.promised();
                if (superseding.compareTo(ballot) <= 0)
                    superseding = null;

                boolean calculateDeps = isPartialAccept;
                if (command.saveStatus() == SaveStatus.Vestigial)
                {
                    superseding = null;
                    outcome = Success;
                    calculateDeps = true;
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

                Participants<?> successful = isPartialAccept ? hasDeps : null;
                if (notOwner && (outcome == Redundant || (hasDeps != null && hasDeps.containsAll(participants.touches()))))
                    outcome = Success;
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
                try (DepsCalculator calculator = new DepsCalculator())
                {
                    deps = calculator.calculate(safeStore, txnId, participants, minEpoch, executeAt, true);
                    if (deps == null)
                        return AcceptReply.inThePast(ballot, participants, safeCommand.current());
                    flags = calculator.executeFlags(txnId);
                }

                Invariants.require(deps.maxTxnId(txnId).epoch() <= executeAt.epoch());
                if (filterDuplicateDependenciesFromAcceptReply())
                    deps = deps.without(this.partialDeps);

                Participants<?> successful = isPartialAccept ? participants.touches() : null;
                return new AcceptReply(successful, deps, flags);
        }
    }

    @Override
    public AcceptReply reduce(AcceptReply r1, AcceptReply r2)
    {
        return AcceptReply.reduce(r1, r2);
    }

    @Override
    public Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, executeAt.epoch(), this);
    }

    @Override
    public KeyHistory keyHistory()
    {
        return SYNC;
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

    public static class NotAccept extends AbstractRequest<AcceptReply>
    {
        public final Status status;
        public final Ballot ballot;
        public final Participants<?> participants;

        public NotAccept(Status status, Ballot ballot, TxnId txnId, Participants<?> participants)
        {
            super(txnId);
            this.status = status;
            this.ballot = ballot;
            this.participants = participants;
        }

        @Override
        public Cancellable submit()
        {
            return node.mapReduceConsumeLocal(this, participants, txnId.epoch(), txnId.epoch(), this);
        }

        @Override
        public AcceptReply apply(SafeCommandStore safeStore)
        {
            StoreParticipants participants = StoreParticipants.notAccept(safeStore, this.participants, txnId);
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
            return "NotAccept{kind: " + status + ", ballot:" + ballot + ", txnId:" + txnId + ", key:" + participants + '}';
        }

        @Override
        public long waitForEpoch()
        {
            return txnId.epoch();
        }
    }
}
