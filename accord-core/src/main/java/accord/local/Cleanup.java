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

import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

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
    TRUNCATE_WITH_OUTCOME(TruncatedApplyWithOutcome),
    TRUNCATE(TruncatedApply),
    INVALIDATE(Invalidated),
    VESTIGIAL(ErasedOrVestigial),
    ERASE(Erased);

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
        return shouldCleanup(safeStore.commandStore(), command, command.participants());
    }

    public static Cleanup shouldCleanup(SafeCommandStore safeStore, Command command, @Nonnull StoreParticipants participants)
    {
        return shouldCleanup(safeStore.commandStore(), command, participants);
    }

    public static Cleanup shouldCleanup(CommandStore commandStore, Command command, @Nonnull StoreParticipants participants)
    {
        return shouldCleanup(command.txnId(), command.saveStatus(), command.durability(), participants,
                               commandStore.redundantBefore(), commandStore.durableBefore());
    }

    public static Cleanup shouldCleanup(TxnId txnId, SaveStatus status, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return shouldCleanup(txnId, status, durability, participants, redundantBefore, durableBefore, true);
    }

    public static Cleanup shouldCleanup(TxnId txnId, SaveStatus status, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore, boolean enforceInvariants)
    {
        return shouldCleanupInternal(txnId, status, durability, participants, redundantBefore, durableBefore, enforceInvariants).filter(status);
    }

    private static Cleanup shouldCleanupInternal(TxnId txnId, SaveStatus status, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore, boolean enforceInvariants)
    {
        if (txnId.kind() == EphemeralRead)
            return NO; // TODO (required): clean-up based on timeout

        if (status == Uninitialised)
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

        if (!status.hasBeen(PreCommitted) && redundantBefore.isAnyOnCoordinationEpoch(txnId, participants.owns, SHARD_REDUNDANT))
            return Cleanup.INVALIDATE;

        if (!participants.hasFullRoute())
        {
            if (status == Invalidated && durableBefore.min(txnId) == UniversalOrInvalidated)
                return Cleanup.ERASE;

            return Cleanup.NO;
        }

        // We first check if the command is redundant locally, i.e. whether it has been applied to all non-faulty replicas of the local shard
        // If not, we don't want to truncate its state else we may make catching up for these other replicas much harder
        RedundantStatus redundant = redundantBefore.status(txnId, participants.route);
        if (redundant == NOT_OWNED)
            illegalState("Command " + txnId + " that is being loaded is not owned by this shard on route " + participants.route);

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
                if (enforceInvariants && status.hasBeen(PreCommitted) && !status.hasBeen(Applied) && redundantBefore.preBootstrapOrStale(txnId, participants.owns) != FULLY)
                    illegalState("Loading redundant command that has been PreCommitted but not Applied.");
            case WAS_OWNED:
        }

        if (!status.hasBeen(PreCommitted))
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
                return Cleanup.ERASE;
        }
    }
}
