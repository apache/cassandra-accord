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

import java.util.Objects;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Command;
import accord.local.Commands;
import accord.local.DepsCalculator;
import accord.local.LoadKeys;
import accord.local.LoadKeysFor;
import accord.local.Node.Id;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.PartialDeps;
import accord.local.StoreParticipants;
import accord.messages.RouteRequest.WithUnsynced;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import static accord.messages.MessageType.StandardMessage.PRE_ACCEPT_REQ;
import static accord.messages.MessageType.StandardMessage.PRE_ACCEPT_RSP;
import static accord.primitives.Timestamp.Flag.REJECTED;

public class PreAccept extends WithUnsynced<PreAccept.PreAcceptReply>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(PreAccept.class);

    public static class SerializerSupport
    {
        public static PreAccept create(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, long maxEpoch, PartialTxn partialTxn, @Nullable PartialDeps deps, boolean hasCoordinatorVote, @Nullable FullRoute<?> fullRoute)
        {
            return new PreAccept(txnId, scope, waitForEpoch, minEpoch, maxEpoch, partialTxn, deps, hasCoordinatorVote, fullRoute);
        }
    }

    // TODO (desired): we send partialTxn pieces to epochs that no longer own the range and that won't save it; stop
    //  (depends in part on moving to RoutingKey for CommandsForKey and Deps)
    public final PartialTxn partialTxn;
    public final @Nullable PartialDeps partialDeps;
    public final FullRoute<?> route;
    public final long acceptEpoch;
    public final boolean hasCoordinatorVote;

    public PreAccept(Id to, Topologies topologies, TxnId txnId, Txn txn, @Nullable Deps deps, boolean hasCoordinatorVote, FullRoute<?> route)
    {
        super(to, topologies, txnId, route);
        // TODO (desired): only includeQuery if route.contains(route.homeKey()); this affects state eviction and is low priority given size in C*
        this.partialTxn = txn.intersecting(scope, true);
        this.acceptEpoch = topologies.currentEpoch();
        this.partialDeps = deps == null ? null : deps.intersecting(scope);
        this.route = route;
        this.hasCoordinatorVote = hasCoordinatorVote;
    }

    PreAccept(TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch, long acceptEpoch, PartialTxn partialTxn, @Nullable PartialDeps partialDeps, boolean hasCoordinatorVote, @Nullable FullRoute<?> fullRoute)
    {
        super(txnId, scope, waitForEpoch, minEpoch);
        this.partialTxn = partialTxn;
        this.acceptEpoch = acceptEpoch;
        this.partialDeps = partialDeps;
        this.route = fullRoute;
        this.hasCoordinatorVote = hasCoordinatorVote;
    }

    @Override
    public LoadKeys loadKeys()
    {
        return LoadKeys.SYNC;
    }

    @Override
    public LoadKeysFor loadKeysFor()
    {
        return LoadKeysFor.READ_WRITE;
    }

    @Override
    public ExecutionKind executionKind()
    {
        return ExecutionKind.PREACCEPT;
    }

    protected boolean abort(Refuse.MinMax refuses)
    {
        return refuses.max != Refuse.NONE;
    }

    @Override
    protected Cancellable submit()
    {
        return node.commandStores().mapReduceConsume(minEpoch, acceptEpoch, this);
    }

    @Override
    public PreAcceptReply applyInternal(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.update(safeStore, route, minEpoch, txnId, acceptEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Commands.AcceptOutcome outcome = Commands.preaccept(safeStore, safeCommand, participants, txnId, partialTxn, partialDeps, hasCoordinatorVote);
        Command command = safeCommand.current();
        switch (outcome)
        {
            default: throw new UnhandledEnum(outcome);
            case Redundant:
                // we might hit 'Redundant' if we have to contact later epochs and partially re-contact a node we already contacted
            case Success:
                // Either messages are being delivered out of order or the coordinator was interrupted by some recovery coordinator
                if (command.status().compareTo(Status.PreAccepted) > 0)
                    return PreAcceptNack.INSTANCE;

                // TODO (expected): special case Deps.NONE here - e.g. as null, or otherwise we should prevent their merge with any others reply
                //   (to avoid edge cases similar to the one where we used thus vote for earlier epochs, but achieved a valid fast quorum without this vote in latest epoch)
                if (command.executeAt().is(REJECTED) && !participants.owns().isEmpty()) // if our vote is required we don't need to compute deps
                    return new PreAcceptOk(txnId, command.executeAt(), Deps.NONE, ExecuteFlags.none());

            case Retired:
                Timestamp executeAt;
                ExecuteFlags flags;
                Deps deps;
                try (DepsCalculator calculator = new DepsCalculator(txnId))
                {
                    deps = calculator.calculate(safeStore, txnId, participants, minEpoch, txnId, true);
                    if (deps == null)
                        return PreAcceptNack.INSTANCE;
                    flags = calculator.executeFlags(txnId);
                    executeAt = calculator.executeAt(safeCommand, node);
                }

                // NOTE: we CANNOT test whether we adopt a future dependency here because it might be that this command
                // is guaranteed to not reach agreement, but that this replica is unaware of that fact and has pruned
                // all preceding transactions. In which case we may be able to adopt a future dependency but won't propose it.
                // We do however prohibit later epochs as dependencies as we cannot handle those effectively
                // when back-filling for execution of the transaction.
                Invariants.require(deps.maxTxnId(txnId).epoch() <= txnId.epoch());
                return new PreAcceptOk(txnId, executeAt, deps, flags);

            case Truncated:
            case RejectedBallot:
                return PreAcceptNack.INSTANCE;
        }
    }

    @Override
    public PreAcceptReply reduce(PreAcceptReply r1, PreAcceptReply r2)
    {
        return PreAcceptReply.reduce(r1, r2);
    }

    @Override
    public MessageType type()
    {
        return PRE_ACCEPT_REQ;
    }

    public static abstract class PreAcceptReply implements Reply
    {
        @Override
        public MessageType type()
        {
            return PRE_ACCEPT_RSP;
        }

        public abstract boolean isOk();

        public static PreAcceptReply reduce(PreAcceptReply r1, PreAcceptReply r2)
        {
            if (!r1.isOk()) return r1;
            if (!r2.isOk()) return r2;
            PreAcceptOk ok1 = (PreAcceptOk) r1;
            PreAcceptOk ok2 = (PreAcceptOk) r2;
            PreAcceptOk okMax = ok1.witnessedAt.compareTo(ok2.witnessedAt) >= 0 ? ok1 : ok2;
            PreAcceptOk okMin = okMax == ok1 ? ok2 : ok1;

            Timestamp witnessedAt = Timestamp.mergeMaxAndFlags(okMax.witnessedAt, okMin.witnessedAt);
            Deps deps = ok1.deps.with(ok2.deps);
            ExecuteFlags flags = ok1.flags.and(ok2.flags);

            if (deps == okMax.deps && witnessedAt == okMax.witnessedAt && okMax.flags == flags)
                return okMax;
            return new PreAcceptOk(ok1.txnId, witnessedAt, deps, flags);
        }
    }

    public static class PreAcceptOk extends PreAcceptReply
    {
        public final TxnId txnId;
        public final Timestamp witnessedAt;
        public final Deps deps;
        public final ExecuteFlags flags;

        public PreAcceptOk(TxnId txnId, Timestamp witnessedAt, Deps deps, ExecuteFlags flags)
        {
            this.txnId = txnId;
            this.witnessedAt = witnessedAt;
            this.deps = deps;
            this.flags = flags;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PreAcceptOk that = (PreAcceptOk) o;
            return witnessedAt.equals(that.witnessedAt) && deps.equals(that.deps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(witnessedAt, deps);
        }

        @Override
        public String toString()
        {
            return "PreAcceptOk{" +
                    "txnId:" + txnId +
                    ", witnessedAt:" + witnessedAt +
                    ", deps:" + deps +
                    '}';
        }
    }

    public static class PreAcceptNack extends PreAcceptReply
    {
        public static final PreAcceptNack INSTANCE = new PreAcceptNack();

        private PreAcceptNack() {}

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "PreAcceptNack{}";
        }
    }

    @Override
    public String toString()
    {
        return "PreAccept{" +
               "txnId:" + txnId +
               ", txn:" + partialTxn +
               ", scope:" + scope +
               '}';
    }
}
