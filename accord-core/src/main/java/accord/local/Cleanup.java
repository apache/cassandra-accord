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
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.Toggles.requiresUniqueHlcs;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.Input.PARTIAL;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DEFUNCT;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.NOT_OWNED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.RedundantStatus.Property.TRUNCATE_BEFORE;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.KnownRoute.CoveringRoute;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.Invalidated;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.TruncatedApplyWithOutcome;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.SaveStatus.Vestigial;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
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
        return cleanupWithoutFullRoute(input, txnId, saveStatus, participants, redundantBefore, durableBefore);
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

        Cleanup min = cleanupIfUndecidedWithFullRoute(input, txnId, saveStatus, redundant);
        if (!redundant.all(TRUNCATE_BEFORE))
        {
            // TODO (expected): see if we can improve our invariants so we can remove this special-case
            if (input != PARTIAL && redundant.all(SHARD_APPLIED) && redundant.all(LOCALLY_DEFUNCT) && !redundant.any(LOCALLY_APPLIED))
                return truncate(txnId, min);
            return min;
        }

        Invariants.paranoid(redundant.all(SHARD_APPLIED));

        if (saveStatus.compareTo(Vestigial) >= 0)
        {
            // we can't use durability from an Invalidated record to decide if we can erase/expunge,
            // as we don't know that this has been persisted at all shards.
            // Similarly, vestigial/erased records don't know their etymology, and may derive from Invalidate
            durability = NotDurable;
        }

        Durability test;
        if (durability.compareTo(UniversalOrInvalidated) >= 0) test = durability;
        else test = Durability.max(durability, durableBefore.min(txnId, participants.route()));

        switch (test)
        {
            default: throw new UnhandledEnum(durability);
            case Local:
            case NotDurable:
            case ShardUniversal:
                // TODO (required): consider how we guarantee not to break recovery of other shards if a majority on this shard are PRE_BOOTSTRAP
                //   (if the condition is false and we fall through to removing Outcome)
                if (input != FULL || participants.doesStillExecute())
                    return min.atLeast(truncateWithOutcome(txnId, min));

            case MajorityOrInvalidated:
            case Majority:
                return min.atLeast(truncate(txnId, min));

            case UniversalOrInvalidated:
            case Universal:
                if (redundant.all(GC_BEFORE))
                    return erase(txnId, min);
                return truncate(txnId, min);
        }
    }

    private static Cleanup cleanupWithoutFullRoute(Input input, TxnId txnId, SaveStatus saveStatus, StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        // TODO (expected): consider if we can truncate more aggressively partial records, although we cannot infer anything from the fact they're undecided
        if (input == PARTIAL || saveStatus.hasBeen(Truncated) || saveStatus == Uninitialised)
            return NO;

        Invariants.require(!saveStatus.hasBeen(PreCommitted));
        boolean isCovering = saveStatus.known.route() == CoveringRoute || txnId.compareTo(redundantBefore.minShardAndLocallyAppliedBefore()) <= 0;

        if (isCovering && txnId.isSyncPoint() && participants.owns().isEmpty())
        {
            RedundantStatus redundant = redundantBefore.status(txnId, null, participants.touches());
            if (redundant.all(SHARD_APPLIED, LOCALLY_REDUNDANT))
                return vestigial(txnId);
        }

        RedundantStatus ownStatus = redundantBefore.status(txnId, null, participants.owns());
        return cleanupUndecided(txnId, ownStatus, isCovering);
    }

    private static Cleanup cleanupIfUndecidedWithFullRoute(Input input, TxnId txnId, SaveStatus saveStatus, RedundantStatus redundantStatus)
    {
        if (input == PARTIAL || saveStatus.hasBeen(PreCommitted))
            return NO;

        return cleanupUndecided(txnId, redundantStatus, true);
    }

    private static Cleanup cleanupUndecided(TxnId txnId, RedundantStatus ownStatus, boolean isCoveringRoute)
    {
        if (ownStatus.any(LOCALLY_APPLIED))
            return invalidate(txnId);

        // TODO (desired): safe to use MAJORITY_APPLIED, LOCALLY_REDUNDANT?
        // TODO (required): can we guarantee we will always eventually obtain a covering route if others are garbage collecting?
        if (isCoveringRoute && ownStatus.all(SHARD_APPLIED, LOCALLY_REDUNDANT))
            return vestigial(txnId);

        return NO;
    }

    private static boolean expunge(TxnId txnId, @Nullable Timestamp executeAt, @Nullable SaveStatus saveStatus, @Nullable StoreParticipants participants, RedundantBefore redundantBefore, DurableBefore durableBefore)
    {
        if (txnId.is(Any) && durableBefore.min(txnId) != UniversalOrInvalidated)
            return false;

        // since we cannot guarantee to witness participants for all records, we must use the global durableBefore bounds
        TxnId minGcBefore = redundantBefore.minGcBefore();
        if (minGcBefore.compareTo(txnId) <= 0)
            return false;

        if (!requiresUniqueHlcs() || !txnId.is(Write)) return true;
        if (saveStatus == null || !saveStatus.known.is(ApplyAtKnown)) return true;
        // note, it is safe to use ApplyAtKnown even with PARTIAL input here, because we are only discarding information,
        // and we can safely discard any stale executeAt
        if (executeAt == null) return true;
        if (minGcBefore.is(HLC_BOUND) && executeAt.uniqueHlc() < minGcBefore.hlc()) return true;
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

    private static Cleanup truncateWithOutcome(TxnId txnId, Cleanup atLeast)
    {
        return atLeast.compareTo(TRUNCATE_WITH_OUTCOME) > 0 ? atLeast : TRUNCATE_WITH_OUTCOME;
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
