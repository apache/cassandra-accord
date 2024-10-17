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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.VisibleForImplementation;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.TxnId;

import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;
import static accord.local.RedundantStatus.LIVE;
import static accord.local.RedundantStatus.NOT_OWNED;
import static accord.local.RedundantStatus.SHARD_REDUNDANT;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.ErasedOrVestigial;
import static accord.primitives.SaveStatus.Invalidated;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Durability.Majority;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.utils.Invariants.illegalState;

/**
 * Logic related to whether metadata about transactions is safe to discard given currently available information.
 * The data may not be completely discarded if parts of it will still be necessary.
 */
public enum Cleanup
{
    NO(Uninitialised),
    // we don't know if the command has been applied or invalidated as we have incomplete information
    // so erase what information we don't need in future to decide this
    // TODO (required): tighten up semantics here (and maybe infer more aggressively)
    EXPUNGE_PARTIAL(TruncatedApplyWithOutcome),
    TRUNCATE_WITH_OUTCOME(TruncatedApplyWithOutcome),
    TRUNCATE(TruncatedApply),
    INVALIDATE(Invalidated),
    VESTIGIAL(ErasedOrVestigial),
    ERASE(Erased),
    // we can stop storing the record entirely
    EXPUNGE(Erased);

    private static final Cleanup[] VALUES = values();

    public final SaveStatus appliesIfNot;

    Cleanup(SaveStatus appliesIfNot)
    {
        this.appliesIfNot = appliesIfNot;
    }

    public final Cleanup filter(SaveStatus saveStatus)
    {
        return saveStatus.compareTo(appliesIfNot) >= 0 ? NO : this;
    }

    public static Cleanup shouldCleanup(SafeCommandStore safeStore, Command command)
    {
        return shouldCleanup(safeStore, command, command.participants());
    }

    public static Cleanup shouldCleanup(SafeCommandStore safeStore, Command command, @Nonnull StoreParticipants participants)
    {
        return shouldCleanup(safeStore.agent(), command.txnId(), command.saveStatus(), command.durability(), participants,
                             safeStore.redundantBefore(), safeStore.durableBefore());
    }

    public static Cleanup shouldCleanup(Agent agent, Command command, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return shouldCleanup(agent, command.txnId(), command.saveStatus(), command.durability(), command.participants(),
                             redundantBefore, durableBefore);
    }

    public static Cleanup shouldCleanup(Agent agent, TxnId txnId, SaveStatus status, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return shouldCleanupInternal(agent, txnId, status, durability, participants, redundantBefore, durableBefore).filter(status);
    }

    // TODO (required): simulate compaction of log records in burn test
    @VisibleForImplementation
    public static Cleanup shouldCleanupPartial(Agent agent, TxnId txnId, SaveStatus status, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return shouldCleanupPartialInternal(agent, txnId, status, durability, participants, redundantBefore, durableBefore).filter(status);
    }

    private static Cleanup shouldCleanupInternal(Agent agent, TxnId txnId, SaveStatus saveStatus, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (txnId.kind() == EphemeralRead)
            return NO; // TODO (required): clean-up based on timeout

        if (expunge(txnId, saveStatus, durableBefore, redundantBefore))
            return EXPUNGE;

        if (saveStatus == Uninitialised)
        {
            if (!redundantBefore.isAnyOnAnyEpoch(txnId, participants.touches, SHARD_REDUNDANT))
                return NO;

            // participants.touches() means e.g. we used to or will own the participant, but shard redundant means all
            // owners of the command have applied it - if we aren't guaranteed to know it and we don't then it is
            // "vestigial" i.e. represents some attempt to coordinate the command against us (e.g. failed propose or calculateDeps)
            Cleanup cleanup = VESTIGIAL;
            if (redundantBefore.isAnyOnCoordinationEpoch(txnId, participants.owns, SHARD_REDUNDANT))
            {
                // owns means the key is known to interact with us is shard redundant then we
                cleanup = ERASE;
                if (redundantBefore.isAnyOnCoordinationEpoch(txnId, participants.owns, LIVE))
                    cleanup = INVALIDATE;
            }
            return cleanup;
        }

        if (!participants.hasFullRoute())
        {
            if (!saveStatus.hasBeen(PreCommitted) && redundantBefore.isAnyOnCoordinationEpoch(txnId, participants.owns, SHARD_REDUNDANT))
                return Cleanup.INVALIDATE;

            return Cleanup.NO;
        }

        return cleanupWithFullRoute(agent, false, participants, txnId, saveStatus, durability, redundantBefore, durableBefore);
    }

    private static Cleanup shouldCleanupPartialInternal(Agent agent, TxnId txnId, SaveStatus status, @Nullable Durability durability, @Nullable StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (txnId.kind() == EphemeralRead)
            return NO; // TODO (required): clean-up based on timeout

        if (expunge(txnId, status, durableBefore, redundantBefore))
            return EXPUNGE;

        if (participants == null)
            return NO;

        if (!participants.hasFullRoute())
        {
            if (!redundantBefore.isAnyOnCoordinationEpoch(txnId, participants.owns, SHARD_REDUNDANT))
                return NO;

            // we only need to keep the outcome if we have it; otherwise we can expunge
            switch (status)
            {
                case TruncatedApply:
                case TruncatedApplyWithOutcome:
                case Invalidated:
                    return NO;
                case PreApplied:
                case Applied:
                case Applying:
                    return TRUNCATE_WITH_OUTCOME;
                default:
                    return EXPUNGE_PARTIAL;
            }
        }

        return cleanupWithFullRoute(agent, true, participants, txnId, status, durability, redundantBefore, durableBefore);
    }

    private static Cleanup cleanupWithFullRoute(Agent agent, boolean isPartial, StoreParticipants participants, TxnId txnId, SaveStatus saveStatus, Durability durability, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        // We first check if the command is redundant locally, i.e. whether it has been applied to all non-faulty replicas of the local shard
        // If not, we don't want to truncate its state else we may make catching up for these other replicas much harder
        FullRoute<?> route = Route.castToFullRoute(participants.route);
        RedundantStatus redundant = redundantBefore.status(txnId, route);
        if (redundant == NOT_OWNED)
            illegalState("Command " + txnId + " that is being loaded is not owned by this shard on route " + route);

        switch (redundant)
        {
            default: throw new AssertionError();
            case LIVE:
            case PARTIALLY_PRE_BOOTSTRAP_OR_STALE:
            case PRE_BOOTSTRAP_OR_STALE:
            case REDUNDANT_PRE_BOOTSTRAP_OR_STALE:
            case LOCALLY_REDUNDANT:
                return Cleanup.NO;
            case SHARD_REDUNDANT:
                if (!isPartial && saveStatus.hasBeen(PreCommitted) && !saveStatus.hasBeen(Applied) && redundantBefore.preBootstrapOrStale(txnId, participants.owns) != FULLY)
                {
                    agent.onViolation(String.format("Loading SHARD_REDUNDANT command %s with status %s (that should have been Applied). Expected to be witnessed and executed by %s.", txnId, saveStatus, redundantBefore.max(participants.route, e -> e.shardAppliedOrInvalidatedBefore)));
                    return TRUNCATE;
                }
            case WAS_OWNED:
        }

        if (!isPartial && !saveStatus.hasBeen(PreCommitted))
            return INVALIDATE;

        Durability min = durableBefore.min(txnId, participants.route);
        switch (min)
        {
            default:
            case Local:
                throw new AssertionError("Unexpected durability: " + min);

            case NotDurable:
                if (durability == Majority)
                    return Cleanup.TRUNCATE;
                return Cleanup.TRUNCATE_WITH_OUTCOME;

            case MajorityOrInvalidated:
            case Majority:
                return Cleanup.TRUNCATE;

            case UniversalOrInvalidated:
            case Universal:
                // TODO (expected): can we EXPUNGE here?
                return Cleanup.ERASE;
        }
    }

    private static boolean expunge(TxnId txnId, SaveStatus saveStatus, DurableBefore durableBefore, RedundantBefore redundantBefore)
    {
        if (durableBefore.min(txnId) != UniversalOrInvalidated)
            return false;

        if (saveStatus == Invalidated)
            return true;

        // TODO (required): we should perhaps weaken this to separately account whether remotely and locally redundant?
        //  i.e., if we know that the shard is remotely durable and we know we don't need it locally (e.g. due to bootstrap)
        //  then we can safely erase. Revisit as part of rationalising RedundantBefore registers.
        return redundantBefore.shardStatus(txnId) == SHARD_REDUNDANT;
    }

    public static Cleanup forOrdinal(int ordinal)
    {
        return VALUES[ordinal];
    }
}
