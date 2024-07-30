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

import accord.local.Status.Durability;
import accord.primitives.EpochSupplier;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;

import static accord.local.RedundantBefore.NO_UPPER_BOUND;
import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;
import static accord.local.RedundantStatus.NOT_OWNED;
import static accord.local.SaveStatus.Erased;
import static accord.local.SaveStatus.Invalidated;
import static accord.local.SaveStatus.TruncatedApply;
import static accord.local.SaveStatus.TruncatedApplyWithOutcome;
import static accord.local.SaveStatus.Uninitialised;
import static accord.local.Status.Applied;
import static accord.local.Status.Durability.Majority;
import static accord.local.Status.Durability.UniversalOrInvalidated;
import static accord.local.Status.PreCommitted;
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
    ERASE(Erased);

    public final SaveStatus appliesIfNot;

    Cleanup(SaveStatus appliesIfNot)
    {
        this.appliesIfNot = appliesIfNot;
    }

    /**
     * Durability has been achieved for the specific keys associated with this txnId that makes its metadata safe to purge
     */
    public static boolean isSafeToCleanup(DurableBefore durableBefore, TxnId txnId, Unseekables<?> participants)
    {
        return durableBefore.min(txnId, participants) == UniversalOrInvalidated;
    }

    public static Cleanup shouldCleanup(SafeCommandStore safeStore, Command command, EpochSupplier toEpoch, Unseekables<?> maybeFullRoute)
    {
        return shouldCleanup(safeStore.commandStore(), command, toEpoch, maybeFullRoute);
    }

    public static Cleanup shouldCleanup(CommandStore commandStore, Command command, EpochSupplier toEpoch, Unseekables<?> maybeFullRoute)
    {
        return shouldCleanup(commandStore, command, toEpoch, maybeFullRoute, true);
    }

    public static Cleanup shouldCleanup(CommandStore commandStore, Command command, EpochSupplier toEpoch, Unseekables<?> maybeFullRoute, boolean enforceInvariants)
    {
        if (command.saveStatus() == Erased)
            return Cleanup.NO; // once erased we no longer have executeAt, and may consider that a transaction is owned by us that should not be (as its txnId is an earlier epoch than we adopted a range)

        Route<?> route = command.route();
        if (!Route.isFullRoute(route))
        {
            if (command.known().hasFullRoute()) illegalState(command.saveStatus() + " should include full route, but only found " + route);
            else if (Route.isFullRoute(maybeFullRoute)) route = Route.castToFullRoute(maybeFullRoute);
            else return Cleanup.NO;
        }

        Timestamp executeAt = command.executeAt();
        if (toEpoch == null || (executeAt != null && executeAt.epoch() > toEpoch.epoch()))
            toEpoch = executeAt;

        return shouldCleanup(command.txnId(), command.status(), command.durability(), toEpoch, route,
                             commandStore.redundantBefore(), commandStore.durableBefore(), enforceInvariants);
    }

    public static Cleanup shouldCleanup(TxnId txnId, Status status, Durability durability, EpochSupplier toEpoch, Route<?> route, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return shouldCleanup(txnId, status, durability, toEpoch, route, redundantBefore, durableBefore, true);
    }

    public static Cleanup shouldCleanup(TxnId txnId, Status status, Durability durability, EpochSupplier toEpoch, Route<?> route, RedundantBefore redundantBefore, DurableBefore durableBefore, boolean enforceInvariants)
    {
        if (txnId.kind() == EphemeralRead)
            return Cleanup.NO; // TODO (required): clean-up based on timeout

        if (durableBefore.min(txnId) == UniversalOrInvalidated)
        {
            if (status.hasBeen(PreCommitted) && !status.hasBeen(Applied)) // TODO (expected): either stale or pre-bootstrap
                illegalState("Loading universally-durable command that has been PreCommitted but not Applied");
            return Cleanup.ERASE;
        }

        if (!Route.isFullRoute(route))
            return Cleanup.NO;

        // TODO (required): should we apply additionalKeysOrRanges() to calculations here?
        // TODO (required): enrich with additional epochs we know the command applies to
        // We first check if the command is redundant locally, i.e. whether it has been applied to all non-faulty replicas of the local shard
        // If not, we don't want to truncate its state else we may make catching up for these other replicas much harder
        RedundantStatus redundant = redundantBefore.status(txnId, toEpoch, route.participants());
        if (redundant == NOT_OWNED)
        {
            // ONLY upgrade to looking to future if NOT_OWNED, as if we're owned we can safely manage the lifecycle whatever epochs it participated in
            // however, if this is a vestigial preaccept/accept in an epoch after the execution epoch we may not clean up
            redundant = redundantBefore.status(txnId, NO_UPPER_BOUND, route.participants());
            if (redundant == NOT_OWNED)
            {
                // TODO (expected): improve handling of epoch bounds
                //      - we can impose additional validations here IF we receive an epoch upper bound
                //      - we should be more robust to the presence/absence of executeAt
                //      - be cognisant of future epochs that participated only for PreAccept/Accept, but where txn was not committed to execute in the epoch (this is why we provide null toEpoch here)
                illegalState("Command %s that is being loaded is not owned by this shard on route %s. Redundant before: %s", txnId, route, redundantBefore);
            }
        }
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
                if (enforceInvariants && status.hasBeen(PreCommitted) && !status.hasBeen(Applied) && redundantBefore.preBootstrapOrStale(txnId, toEpoch, route.participants()) != FULLY)
                    illegalState("Loading redundant command that has been PreCommitted but not Applied.");
        }

        if (!status.hasBeen(PreCommitted))
            return INVALIDATE;

        Durability min = durableBefore.min(txnId, route);
        switch (min)
        {
            default:
            case Local:
                throw new AssertionError("Unexpected durability: " + min);

            case NotDurable:
                // TODO (required): disambiguate INVALIDATE and TRUNCATE_WITH_OUTCOME, and impose stronger invariants on former
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
