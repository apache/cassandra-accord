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

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Commands.AcceptOutcome;
import accord.local.KeyHistory;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.Cancellable;

import static accord.local.Commands.AcceptOutcome.Redundant;
import static accord.local.Commands.AcceptOutcome.RejectedBallot;
import static accord.local.Commands.AcceptOutcome.Success;
import static accord.local.Commands.AcceptOutcome.Truncated;

// TODO (low priority, efficiency): use different objects for send and receive, so can be more efficient
//                                  (e.g. serialize without slicing, and without unnecessary fields)
public class Accept extends TxnRequest.WithUnsynced<Accept.AcceptReply>
{
    public static class SerializerSupport
    {
        public static Accept create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Ballot ballot, Timestamp executeAt, PartialDeps partialDeps)
        {
            return new Accept(txnId, scope, waitForEpoch, minEpoch, ballot, executeAt, partialDeps);
        }
    }

    public final Ballot ballot;
    public final Timestamp executeAt;
    public final PartialDeps partialDeps;

    public Accept(Id to, Topologies topologies, Ballot ballot, TxnId txnId, FullRoute<?> route, Timestamp executeAt, Deps deps)
    {
        super(to, topologies, txnId, route);
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialDeps = deps.intersecting(scope);
    }

    private Accept(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, Ballot ballot, Timestamp executeAt, PartialDeps partialDeps)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.partialDeps = partialDeps;
    }

    @Override
    public AcceptReply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, scope, minEpoch, txnId, txnId.epoch(), executeAt.epoch());
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        switch (Commands.accept(safeStore, safeCommand, participants, txnId, ballot, scope, executeAt, partialDeps))
        {
            default: throw new IllegalStateException();
            case Truncated:
                return AcceptReply.TRUNCATED;
            case Redundant:
                return AcceptReply.redundant(ballot, safeCommand.current());
            case RejectedBallot:
                return new AcceptReply(safeCommand.current().promised());
            case Success:
                // TODO (desirable, efficiency): we don't need to calculate deps if executeAt == txnId
                // TODO (desirable, efficiency): only return delta of sent and calculated deps
                Deps deps = calculateDeps(safeStore, participants);
                Invariants.checkState(deps.maxTxnId(txnId).epoch() <= executeAt.epoch());
                return new AcceptReply(calculateDeps(safeStore, participants));
        }
    }

    private Deps calculateDeps(SafeCommandStore safeStore, StoreParticipants participants)
    {
        return PreAccept.calculateDeps(safeStore, txnId, participants, EpochSupplier.constant(minEpoch), executeAt);
    }

    @Override
    public AcceptReply reduce(AcceptReply ok1, AcceptReply ok2)
    {
        if (!ok1.isOk() || !ok2.isOk())
            return ok1.outcome().compareTo(ok2.outcome()) >= 0 ? ok1 : ok2;

        Deps deps = ok1.deps.with(ok2.deps);
        if (deps == ok1.deps) return ok1;
        if (deps == ok2.deps) return ok2;
        return new AcceptReply(deps);
    }

    @Override
    public Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, minEpoch, executeAt.epoch(), this);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Unseekables<?> keys()
    {
        return scope;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.SYNC;
    }

    @Override
    public MessageType type()
    {
        return MessageType.ACCEPT_REQ;
    }

    public String toString() {
        return "Accept{" +
                "ballot: " + ballot +
                ", txnId: " + txnId +
                ", executeAt: " + executeAt +
                ", deps: " + partialDeps +
                '}';
    }

    public static final class AcceptReply implements Reply
    {
        public static final AcceptReply ACCEPT_INVALIDATE = new AcceptReply(Success);
        public static final AcceptReply TRUNCATED = new AcceptReply(Truncated);

        public final AcceptOutcome outcome;
        public final Ballot supersededBy;
        // TODO (expected): only send back deps that weren't in those we received
        public final @Nullable Deps deps;
        public final @Nullable Timestamp committedExecuteAt;

        private AcceptReply(AcceptOutcome outcome)
        {
            this.outcome = outcome;
            this.supersededBy = null;
            this.deps = null;
            this.committedExecuteAt = null;
        }

        public AcceptReply(Ballot supersededBy)
        {
            this.outcome = RejectedBallot;
            this.supersededBy = supersededBy;
            this.deps = null;
            this.committedExecuteAt = null;
        }

        public AcceptReply(@Nonnull Deps deps)
        {
            this.outcome = Success;
            this.supersededBy = null;
            this.deps = deps;
            this.committedExecuteAt = null;
        }

        public AcceptReply(Ballot supersededBy, @Nullable Timestamp committedExecuteAt)
        {
            this.outcome = Redundant;
            this.supersededBy = supersededBy;
            this.deps = null;
            this.committedExecuteAt = committedExecuteAt;
        }

        static AcceptReply redundant(Ballot ballot, Command command)
        {
            Ballot superseding = command.promised();
            if (superseding.compareTo(ballot) <= 0)
                superseding = null;
            return new AcceptReply(superseding, command.executeAtIfKnown());
        }

        @Override
        public MessageType type()
        {
            return MessageType.ACCEPT_RSP;
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

    public static class Invalidate extends AbstractRequest<AcceptReply>
    {
        public final Ballot ballot;
        // should not be a non-participating home key
        public final RoutingKey someKey;

        public Invalidate(Ballot ballot, TxnId txnId, RoutingKey someKey)
        {
            super(txnId);
            this.ballot = ballot;
            this.someKey = someKey;
        }

        @Override
        public Cancellable submit()
        {
            return node.mapReduceConsumeLocal(this, someKey, txnId.epoch(), this);
        }

        @Override
        public AcceptReply apply(SafeCommandStore safeStore)
        {
            StoreParticipants participants = StoreParticipants.invalidate(safeStore, Participants.singleton(txnId.domain(), someKey), txnId);
            SafeCommand safeCommand = safeStore.get(txnId, participants);
            AcceptOutcome outcome = Commands.acceptInvalidate(safeStore, safeCommand, ballot);
            switch (outcome)
            {
                default: throw new IllegalArgumentException("Unknown status: " + outcome);
                case Truncated:
                    return AcceptReply.TRUNCATED;
                case Redundant:
                    return AcceptReply.redundant(ballot, safeCommand.current());
                case Success:
                    return AcceptReply.ACCEPT_INVALIDATE;
                case RejectedBallot:
                    return new AcceptReply(safeCommand.current().promised());
            }
        }

        @Override
        public MessageType type()
        {
            return MessageType.ACCEPT_INVALIDATE_REQ;
        }

        @Override
        public String toString()
        {
            return "AcceptInvalidate{ballot:" + ballot + ", txnId:" + txnId + ", key:" + someKey + '}';
        }

        @Override
        public long waitForEpoch()
        {
            return txnId.epoch();
        }
    }
}
