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

import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;

import static accord.api.ProtocolModifiers.dataStoreRequiresUniqueHlcs;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.Input.PARTIAL;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DEFUNCT;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOG_INCOMPLETE;
import static accord.local.RedundantStatus.Property.NOT_OWNED;
import static accord.local.RedundantStatus.Property.LOG_UNAVAILABLE;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.RedundantStatus.Property.TRUNCATE_BEFORE;
import static accord.local.RedundantStatus.Property.UNREADY;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.KnownRoute.CoveringRoute;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.Invalidated;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.SaveStatus.Vestigial;
import static accord.primitives.Status.Durability.HasOutcome.None;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.Any;

/**
 * Logic related to whether metadata about transactions is safe to discard given currently available information.
 * The data may not be completely discarded if parts of it will still be necessary.
 */
public enum Cleanup
{
    NO(Uninitialised),
    TRUNCATE_WITH_OUTCOME(TruncatedApplyWithOutcome),
    TRUNCATE(TruncatedApply),
    VESTIGIAL(Vestigial),
    INVALIDATE(Invalidated),
    ERASE(Erased),
    // we can stop storing the (inspected portion of the) record entirely
    EXPUNGE(Erased);

    private static final Cleanup[] VALUES = values();

    public final SaveStatus newStatus;

    Cleanup(SaveStatus newStatus)
    {
        this.newStatus = newStatus;
    }

    public final Cleanup atLeast(Cleanup that)
    {
        return compareTo(that) >= 0 ? this : that;
    }

    public final boolean appliesTo(SaveStatus saveStatus)
    {
        if (saveStatus == null)
            return this != NO;

        switch (this)
        {
            case EXPUNGE: return true;
            case ERASE: return saveStatus != Erased;
            default: return saveStatus.compareTo(newStatus) < 0;
        }
    }

    public final Cleanup filter(SaveStatus saveStatus)
    {
        return appliesTo(saveStatus) ? this : NO;
    }

    public enum Input { PARTIAL, FULL }

    public static Cleanup shouldCleanup(Input input, SafeCommandStore safeStore, Command command)
    {
        return shouldCleanup(input, safeStore, command, command.participants());
    }

    public static Cleanup shouldCleanup(Input input, SafeCommandStore safeStore, Command command, @Nonnull StoreParticipants participants)
    {
        return shouldCleanup(input, command.txnId(), command.executeAt(), command.saveStatus(), command.durability(), participants,
                             safeStore.redundantBefore(), safeStore.durableBefore());
    }

    public static Cleanup shouldCleanup(Input input, Command command, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        return shouldCleanup(input, command.txnId(), command.executeAt(), command.saveStatus(), command.durability(), command.participants(),
                             redundantBefore, durableBefore);
    }

    public static Cleanup shouldCleanup(Input input, TxnId txnId, Timestamp executeAt, SaveStatus status, Durability durability, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        Cleanup cleanup = shouldCleanupInternal(input, txnId, executeAt, status, durability, participants, redundantBefore, durableBefore);
        return cleanup.filter(status);
    }

    /**
     * Decide if (and how) we can cleanup the transaction in question.
     * </p>
     * We can ERASE data once we are certain no other replicas require our information.
     * We can EXPUNGE data once we can reliably and safely EXPUNGE any partial record.
     * To achieve the latter, we use only global summary information and the TxnId -
     * and if present any applyAt.
     *
     * [If implementations require unique HLCs they must guarantee to save any applyAt alongside
     * a Route that can be used to report the applyAt to any participating keys on restart.]
     *
     * </p>
     * A transaction may be truncated as soon as it is durable locally and all shard(s)
     * the CommandStore participates in, but must retain the transaction Outcome until
     * it is durable at a majority of replicas on all shards.
     * This permits other shards to contact us for recovery information.
     *
     * Note importantly that we cannot safely truncate commands that are pre-bootstrap that have
     * not yet been applied, as we may be a member of the quorum that coordinated the command, even
     * if we have not bootstrapped the range.
     * TODO (expected): this requirement could be restricted to the home shard.
     * </p>
     * If we know a transaction is invalidated, but don't know its FullRoute,
     * we pessimistically assume the whole cluster may need to see its outcome
     * TODO (expected): we should be able to rely on replicas to infer Invalidated from an Erased record
     */
    private static Cleanup shouldCleanupInternal(Input input, TxnId txnId, @Nullable Timestamp executeAt, @Nullable SaveStatus saveStatus, Durability durability, @Nullable StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (txnId.kind() == EphemeralRead)
            return NO;

        if (expunge(txnId, executeAt, saveStatus, participants, redundantBefore, durableBefore))
            return expunge(txnId);

        if (saveStatus == null || participants == null)
            return NO;

        if (participants.hasFullRoute())
            return cleanupWithFullRoute(input, participants, txnId, executeAt, saveStatus, durability, redundantBefore, durableBefore);
        return cleanupWithoutFullRoute(input, txnId, saveStatus, participants, redundantBefore);
    }

    private static Cleanup cleanupWithFullRoute(Input input, StoreParticipants participants, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        // We first check if the command is redundant locally, i.e. whether it has been applied to all non-faulty replicas of the local shard
        // If not, we don't want to truncate its state else we may make catching up for these other replicas much harder
        FullRoute<?> route = Route.castToFullRoute(participants.route());
        // we must not use executeAt here if input == PARTIAL because with partial compaction we might be merging the combination of some old executeAt with a later partial status
        RedundantStatus redundant = redundantBefore.status(txnId, input == FULL && saveStatus.known.is(ApplyAtKnown) ? executeAt : null, route);
        Invariants.require(redundant.none(NOT_OWNED), "Command %s that is being loaded is not owned by this shard on route %s", txnId, route);

        if (redundant.none(LOCALLY_REDUNDANT))
            return NO;

        // we don't currently expunge any partial log based on faults -
        // we leave this to the normal process to ensure we don't either
        // 1) lose information we want to retain or 2) imply something we don't want to imply (e.g. TRUNCATED and ERASED imply global durability)
        if (input == FULL && hasLogFault(redundant, saveStatus))
            throw logFault(txnId, participants, redundant);

        Cleanup min = cleanupIfUndecidedWithFullRoute(input, txnId, saveStatus, redundant);
        if (!redundant.all(TRUNCATE_BEFORE))
        {
            // TODO (expected): see if we can improve our invariants so we can remove this special-case
            if (input != PARTIAL && redundant.all(SHARD_APPLIED) && redundant.all(LOCALLY_DEFUNCT) && !redundant.any(LOCALLY_APPLIED))
                return truncate(txnId, min);
            return min;
        }

        Invariants.paranoid(redundant.all(SHARD_APPLIED));

        if (!redundant.all(LOCALLY_DURABLE_TO_DATA_STORE))
            return truncateWithOutcome(txnId, input, redundant, min, participants);

        HasOutcome test = durability.allShards();
        if (saveStatus.compareTo(Vestigial) >= 0)
        {
            // we can't use durability from an Invalidated record to decide if we can erase/expunge,
            // as we don't know that this has been persisted at all shards.
            // Similarly, vestigial/erased records don't know their etymology, and may derive from Invalidate
            test = None;
        }

        if (test != Universal)
            test = Durability.HasOutcome.max(test, durableBefore.min(txnId, participants.route()));

        if (test != Universal)
            return truncateWithOutcome(txnId, input, redundant, min, participants);

        if (redundant.all(GC_BEFORE))
            return erase(txnId, min);
        return truncate(txnId, min);
    }

    /**
     * Whether a log fault forbids us from using this record.
     *
     * LOG_UNAVAILABLE means we treat the log as unreliable. LOG_INCOMPLETE means we may have lost writes
     * to the log, but what we have is a valid earlier state - so any terminal states (>=Stable) can be used safely
     *
     * Since LOG_UNAVAILABLE and LOG_INCOMPLETE may be updated for different ranges at different times,
     * we require that a record is FULLY defunct before throwing a faulty log exception (not fully covered by
     * the fault, as it might be the record is partially not owned). Logically, we care that the record
     * is fully lost|faulty - a record that is partially faulty and partially live is considered fully intact.
     *
     * This distinction may not matter, since for a faulty log we must mark the whole log as unreliable below the bounds
     * at which it is unsafe, and coordinations are entirely refused that overlap those ranges until the bounds have been
     * decided - whereas to simply catchup a subrange we do not mark the log as unreliable.
     * However, it is the simplest formulation and logically sound.
     */
    private static boolean hasLogFault(RedundantStatus redundant, SaveStatus saveStatus)
    {
        return redundant.any(UNREADY) // efficiency - includes both unavailable sources to short-circuit test
               && redundant.all(LOCALLY_DEFUNCT)  // includes e.g. lost ownership, so that we ignore records we have totally missing from the log via either mechanism
               && (redundant.any(LOG_UNAVAILABLE)
                   || (redundant.all(LOG_INCOMPLETE) && saveStatus.compareTo(Stable) < 0));
    }

    private static Cleanup cleanupWithoutFullRoute(Input input, TxnId txnId, SaveStatus saveStatus, StoreParticipants participants, RedundantBefore redundantBefore)
    {
        // TODO (expected): consider if we can truncate more aggressively partial records, although we cannot infer anything from the fact they're undecided
        if (input == PARTIAL)
            return NO;

        // TODO (expected): should check touches only for requests that assemble dependency responses
        RedundantStatus touchesStatus = redundantBefore.status(txnId, null, participants.touches());
        if (hasLogFault(touchesStatus, saveStatus))
            throw logFault(txnId, participants, touchesStatus);

        if (saveStatus.hasBeen(Truncated) || saveStatus == Uninitialised)
            return NO;

        Invariants.require(!saveStatus.hasBeen(PreCommitted));
        boolean isCovering = saveStatus.known.route() == CoveringRoute || txnId.compareTo(redundantBefore.minShardAndLocallyAppliedBefore()) <= 0;

        if (isCovering && txnId.isSyncPoint() && participants.owns().isEmpty() && touchesStatus.all(SHARD_APPLIED, LOCALLY_REDUNDANT))
        {
            // TODO (required): derive and document the reason for this logic (presumably related to how sync points interact with ranges we used to own)
            return vestigial(txnId);
        }

        RedundantStatus ownStatus = redundantBefore.status(txnId, null, participants.owns());
        if (hasLogFault(ownStatus, saveStatus))
            throw logFault(txnId, participants, ownStatus);
        return cleanupUndecided(txnId, ownStatus, isCovering);
    }

    /**
     * We have nothing left to answer for and may not trust what we hold, so a FULL read refuses.  Note that
     * this is only reachable for FULL: {@code cleanupWithoutFullRoute} returns NO for PARTIAL before either
     * of its call sites, and {@code cleanupWithFullRoute} tests {@code input == FULL} explicitly - a log
     * fault never rewrites the log.
     */
    private static LogFaultException logFault(TxnId txnId, StoreParticipants participants, RedundantStatus redundant)
    {
        return new LogFaultException(txnId + ": " + participants + ' ' + redundant);
    }

    private static Cleanup cleanupIfUndecidedWithFullRoute(Input input, TxnId txnId, SaveStatus saveStatus, RedundantStatus redundant)
    {
        if (input == PARTIAL || saveStatus.hasBeen(PreCommitted))
            return NO;

        return cleanupUndecided(txnId, redundant, true);
    }

    private static Cleanup cleanupUndecided(TxnId txnId, RedundantStatus ownStatus, boolean isCoveringRoute)
    {
        if (ownStatus.any(LOCALLY_APPLIED))
            return invalidate(txnId);

        // TODO (desired): safe to use QUORUM_APPLIED, LOCALLY_REDUNDANT?
        // TODO (formalise): can we guarantee we will always eventually obtain a covering route if others are garbage collecting?
        if (isCoveringRoute && ownStatus.all(SHARD_APPLIED, LOCALLY_REDUNDANT))
            return vestigial(txnId);

        return NO;
    }

    private static boolean expunge(TxnId txnId, @Nullable Timestamp executeAt, @Nullable SaveStatus saveStatus, @Nullable StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        // TODO (required): improve expungeability of data when we know all participating shards are durable,
        //  by e.g. emitting a special erase record that retains the participants, permitting us to expunge everything else independently
        // since we cannot guarantee to witness participants for all records, we must use the global durableBefore bounds
        if (txnId.is(Any) && !durableBefore.min(txnId).isDurable())
            return false;

        TxnId minGcBefore = redundantBefore.minGcBefore();
        if (minGcBefore.compareTo(txnId) <= 0)
            return false;

        if (!dataStoreRequiresUniqueHlcs() || !txnId.is(Write)) return true;
        if (saveStatus == null || !saveStatus.known.is(ApplyAtKnown)) return true;
        // note, it is safe to use ApplyAtKnown even with PARTIAL input here, because we are only discarding information,
        // and we can safely discard any stale executeAt
        if (executeAt == null) return true;

        long minGcHlcBefore = redundantBefore.minGcHlcBefore();
        if (executeAt.uniqueHlc() < minGcHlcBefore) return true;
        if (participants == null)
            return true;
        Participants<?> waitsOn = participants.waitsOn();
        return waitsOn == null || waitsOn.isEmpty();
    }

    public static Cleanup forOrdinal(int ordinal)
    {
        return VALUES[ordinal];
    }

    // convenient for debugging
    private static Cleanup invalidate(TxnId txnId)
    {
        return INVALIDATE;
    }

    private static Cleanup truncateWithOutcome(TxnId txnId, Input input, RedundantStatus status, Cleanup atLeast, StoreParticipants participants)
    {
        if (atLeast.compareTo(TRUNCATE_WITH_OUTCOME) >= 0)
            return atLeast;
        if (input == PARTIAL)
            return TRUNCATE_WITH_OUTCOME;
        if (status.all(LOCALLY_DEFUNCT))
            return TRUNCATE;
        // TODO (expected): tighten constraints when stillExecutes is null (this means undecided; should be handled elsewhere but confirm)
        Participants<?> stillExecutes = participants.stillExecutes();
        if (stillExecutes != null && stillExecutes.isEmpty())
            return TRUNCATE;
        return TRUNCATE_WITH_OUTCOME;
    }

    private static Cleanup truncate(TxnId txnId, Cleanup atLeast)
    {
        return atLeast.compareTo(TRUNCATE) > 0 ? atLeast : TRUNCATE;
    }

    private static Cleanup vestigial(TxnId txnId)
    {
        return VESTIGIAL;
    }

    private static Cleanup erase(TxnId txnId, Cleanup atLeast)
    {
        return atLeast != EXPUNGE ? ERASE : EXPUNGE;
    }

    private static Cleanup expunge(TxnId txnId)
    {
        return EXPUNGE;
    }
}
