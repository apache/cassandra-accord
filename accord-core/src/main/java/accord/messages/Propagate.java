/*
 * Licensed to the Apache Software ation (ASF) under one
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

import accord.api.ProtocolModifiers;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Tracing;
import accord.coordinate.FetchData.FetchResult;
import accord.coordinate.Infer.InvalidIf;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.CommandStores.StoreSelector;
import accord.local.Commands;
import accord.local.Node;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.RedundantStatus;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Known.Outcome;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Known;
import accord.local.StoreParticipants;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.primitives.KnownMap;
import accord.primitives.WithQuorum;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import static accord.coordinate.Infer.InvalidIf.IfUncommitted;
import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;
import static accord.local.Cleanup.ERASE;
import static accord.local.Cleanup.VESTIGIAL;
import static accord.local.Commands.purge;
import static accord.local.RedundantStatus.Property.LOCALLY_DEFUNCT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.StoreParticipants.Filter.UPDATE;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.KnownRoute.FullRoute;
import static accord.primitives.Known.Nothing;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.NotDefined;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.WithQuorum.HasQuorum;
import static accord.utils.Invariants.illegalState;

// TODO (required): detect propagate loops where we don't manage to update anything but should
public class Propagate extends MapReduceConsumeCommandStores<Route<?>, Void>
{
    final Node node;
    final TxnId txnId;
    final Known target;
    @Nullable final Known foundAtLeast; // the minimum we found for the keys we queried
    final InvalidIf invalidIf;

    // TODO (desired): remove dependency on these two SaveStatus
    final SaveStatus maxKnowledgeSaveStatus;
    final SaveStatus maxSaveStatus;
    final Ballot promised;
    final Ballot acceptedOrCommitted;
    final Status.Durability durability;
    @Nullable final RoutingKey homeKey;
    final KnownMap known;
    final WithQuorum withQuorum;
    @Nullable final PartialTxn partialTxn;
    @Nullable final PartialDeps stableDeps;
    @Nullable final Timestamp committedExecuteAt;
    @Nullable final Writes writes;
    @Nullable final Result result;
    final BiConsumer<? super FetchResult, Throwable> callback;
    final @Nullable Tracing tracing;

    private transient volatile FetchResult fetchResult;
    private static final AtomicReferenceFieldUpdater<Propagate, FetchResult> fetchResultUpdater = AtomicReferenceFieldUpdater.newUpdater(Propagate.class, FetchResult.class, "fetchResult");

    Propagate(
    Node node, TxnId txnId,
    Route<?> route,
    Known target,
    @Nullable Known foundAtLeast,
    InvalidIf invalidIf,
    SaveStatus maxKnowledgeSaveStatus,
    SaveStatus maxSaveStatus, Ballot promised,
    Ballot acceptedOrCommitted,
    Status.Durability durability,
    @Nullable RoutingKey homeKey,
    KnownMap known, WithQuorum withQuorum,
    @Nullable PartialTxn partialTxn,
    @Nullable PartialDeps stableDeps,
    @Nullable Timestamp committedExecuteAt,
    @Nullable Writes writes,
    @Nullable Result result,
    BiConsumer<? super FetchResult, Throwable> callback,
    @Nullable Tracing tracing)
    {
        super(route);
        this.node = node;
        this.txnId = txnId;
        this.target = target;
        this.foundAtLeast = foundAtLeast;
        this.invalidIf = invalidIf;
        this.maxKnowledgeSaveStatus = maxKnowledgeSaveStatus;
        this.maxSaveStatus = maxSaveStatus;
        this.promised = promised;
        this.acceptedOrCommitted = acceptedOrCommitted;
        this.durability = durability;
        this.homeKey = homeKey;
        this.known = known;
        this.withQuorum = withQuorum;
        this.partialTxn = partialTxn;
        this.stableDeps = stableDeps;
        this.committedExecuteAt = committedExecuteAt;
        this.writes = writes;
        this.result = result;
        this.callback = callback;
        this.tracing = tracing;
    }

    public static void propagate(Node node, TxnId txnId, InvalidIf previouslyKnownToBeInvalidIf, long sourceEpoch, WithQuorum withQuorum, Route<?> queried, Participants<?> contactable, LatentStoreSelector reportTo, @Nullable Known target, CheckStatusOkFull full, BiConsumer<? super FetchResult, Throwable> callback, @Nullable Tracing tracing)
    {
        if (full.maxKnowledgeSaveStatus.status == NotDefined && full.invalidIf == NotKnownToBeInvalid)
        {
            if (tracing != null)
                tracing.trace(null, "Found nothing");
            callback.accept(new FetchResult(Nothing, queried.slice(0, 0), Nothing), null);
            return;
        }

        Invariants.require(sourceEpoch == txnId.epoch() || (full.executeAt != null && sourceEpoch == full.executeAt.epoch()) || full.maxSaveStatus == SaveStatus.Erased || full.maxSaveStatus == SaveStatus.Vestigial);

        // TODO (required): consider and document whether it is safe to infer that we are stale if we have not received responses from all shards we know of
        //  (in principle, we should at least require responses from our own shard, and the home shard if we know it); if we only hear from a remote shard it may have fully Erased
        full = full.finish(queried, contactable, queried.with((Unseekables) contactable), withQuorum, previouslyKnownToBeInvalidIf);
        Route<?> route = Invariants.nonNull(full.route);

        if (tracing != null)
            tracing.trace(null, "Found %s", full.map);

        Timestamp committedExecuteAt = full.executeAtIfKnown();
        Known foundAtLeast = full.map.foldl(queried, (minMax, min) -> Known.nonNullOrMin(min, minMax.minOwned), null);
        Propagate propagate =
            new Propagate(node, txnId, route, target, foundAtLeast, full.invalidIf, full.maxKnowledgeSaveStatus, full.maxSaveStatus, full.maxPromised, full.acceptedOrCommitted, full.durability, full.homeKey, full.map, withQuorum, full.partialTxn, full.stableDeps, committedExecuteAt, full.writes, full.result, callback, tracing);

        long untilEpoch = txnId.epoch();
        if (committedExecuteAt != null)
            untilEpoch = Math.max(untilEpoch, committedExecuteAt.epoch());

        StoreSelector selector = reportTo.refine(txnId, committedExecuteAt, route);
        node.withEpochAtLeast(untilEpoch, null, propagate, () -> node.commandStores().mapReduceConsume(selector, propagate));
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public String reason()
    {
        return "Propagate";
    }

    @Override
    public Void applyInternal(SafeCommandStore safeStore)
    {
        long executeAtEpoch = committedExecuteAt == null ? txnId.epoch() : committedExecuteAt.epoch();
        long lowEpoch = StoreParticipants.computePropagateLowEpoch(safeStore, txnId, scope);
        StoreParticipants participants = StoreParticipants.update(safeStore, scope, lowEpoch, txnId, executeAtEpoch, executeAtEpoch, committedExecuteAt != null);
        // TODO (expected): can we come up with a better more universal pattern for avoiding updating a command we don't intersect with?
        //   ideally integrated with safeStore.get()
        if (participants.owns().isEmpty() && safeStore.ifInitialised(txnId) == null)
        {
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "Uninitialised and not owned; skipping");
            return null;
        }

        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();

        Timestamp executeAtIfKnown = command.executeAtIfKnown(committedExecuteAt);
        if (participants.executes() == null && executeAtIfKnown != null)
        {
            executeAtEpoch = executeAtIfKnown.epoch();
            participants = StoreParticipants.update(safeStore, scope, lowEpoch, txnId, executeAtEpoch, executeAtEpoch, true);
        }

        switch (command.saveStatus().phase)
        {
            // Already know the outcome, waiting on durability so maybe update with new durability information which can also trigger cleanup
            case Persist:
            case Cleanup:
            case Invalidate:
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "Already %s; skipping", command.saveStatus());
                return updateDurability(safeStore, safeCommand, participants);
        }

        participants = participants.supplement(command.participants())
                                   .filter(UPDATE, safeStore, txnId, executeAtIfKnown);

        Known found = known.knownFor(participants.stillOwns(), participants.stillTouches());
        Known currentlyKnown = command.known();
        if (!currentlyKnown.has(FullRoute) && Route.isFullRoute(command.route()))
            currentlyKnown = currentlyKnown.with(FullRoute);

        PartialTxn partialTxn = null;
        if (found.hasDefinition() && !participants.stillOwns().isEmpty())
            partialTxn = this.partialTxn.intersecting(participants.stillOwns(), true).reconstitutePartial(participants.stillOwns());

        PartialDeps stableDeps = null;
        if (found.hasDecidedDeps() && !participants.stillTouches().isEmpty())
            stableDeps = this.stableDeps.intersecting(participants.stillTouches()).reconstitutePartial(participants.stillTouches());

        // TODO (required): hasAnyFullyTruncated could hit edge cases where two replicas are behind and cannot catch up and each participate in the others' result set
        //     should either try to include all peers, or else exclude those that are e.g. pre-bootstrap or otherwise cannot catch up.
        //     Maybe reconsider the logic more holistically, and introduce some strong invariants.
        boolean isShardTruncated = withQuorum == HasQuorum && known.hasAnyFullyTruncated(participants.stillTouches());
        if (isShardTruncated)
        {
            found = tryUpgradeTruncated(safeStore, safeCommand, participants, command, executeAtIfKnown, found);
            if (found == null)
            {
                // TODO (expected): should be ownsOrExecutes()?
                updateFetchResult(Nothing, participants.owns());
                return null;
            }

            if (command.known().is(DepsKnown))
            {
                // keep the deps we already have
                participants = command.participants().supplement(participants);
            }
            else
            {
                Participants<?> depsNeeds = participants.stillTouches();
                if (found.hasDecidedDeps() && stableDeps == null && this.stableDeps != null)
                {
                    Invariants.require(executeAtIfKnown != null);
                    // we don't subtract existing partialDeps, as they cannot be committed deps; we only permit committing deps covering all participating ranges
                    stableDeps = this.stableDeps.intersecting(depsNeeds).reconstitutePartial(depsNeeds);
                }
            }

            Participants<?> needs = participants.stillOwns();
            if (found.isDefinitionKnown() && partialTxn == null && this.partialTxn != null)
            {
                PartialTxn existing = command.partialTxn();
                Participants<?> neededExtra = needs;
                if (existing != null) neededExtra = neededExtra.without(existing.keys().toParticipants());
                partialTxn = this.partialTxn.intersecting(neededExtra, true).reconstitutePartial(neededExtra);
            }
        }

        SaveStatus propagate = found.atLeast(currentlyKnown).propagatesSaveStatus();
        if (propagate.known.isSatisfiedBy(currentlyKnown))
        {
            if (found.is(Outcome.Erased) && participants.stillTouches().isEmpty())
            {
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "No longer interact with this command, and received erased response. Erasing.");
                purge(safeStore, safeCommand, command, participants, ERASE, true);
            }
            else if (invalidIf == IfUncommitted)
            {
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "Marking invalidIfUncommitted");
                safeStore.progressLog().invalidIfUncommitted(txnId);
            }
            else
            {
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "Already know at least as much as peer responses.");
            }

            updateFetchResult(found, participants.owns());
            return updateDurability(safeStore, safeCommand, participants);
        }

        switch (propagate.status)
        {
            default: throw UnhandledEnum.invalid(propagate);
            case NotDefined:
            case Truncated:
                throw illegalState("Status expected to be handled elsewhere: " + propagate);

            case AcceptedMedium:
            case AcceptedSlow:
            case AcceptedInvalidate:
                // we never "propagate" accepted statuses as these are essentially votes,
                // and contribute nothing to our local state machine
                throw illegalState("Invalid states to propagate: " + propagate);

            case Invalidated:
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "Invalidating");
                Commands.commitInvalidate(safeStore, safeCommand, participants.route());
                break;

            case Applied:
            case PreApplied:
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "Applying");
                Invariants.require(committedExecuteAt != null);
                // we must use the remote executeAt, as it might have a uniqueHlc we aren't aware of at commit
                confirm(Commands.apply(safeStore, safeCommand, participants, Ballot.ZERO, txnId, participants.route(), committedExecuteAt, stableDeps, partialTxn, writes, result));
                break;

            case Stable:
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "Committing as stable");
                confirm(Commands.commit(safeStore, safeCommand, participants, Stable, acceptedOrCommitted, txnId, participants.route(), partialTxn, executeAtIfKnown, stableDeps, null));
                break;

            case Committed:
                // TODO (expected): we can propagate Committed as Stable if we have any other Stable result AND a quorum of committedDeps
            case PreCommitted:
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "Pre-committing");
                confirm(Commands.precommit(safeStore, safeCommand, participants, txnId, executeAtIfKnown, promised));
                // TODO (desired): would it be clearer to yield a SaveStatus so we can have PreCommittedWithDefinition
                if (!found.definition().isKnown())
                    break;

            case PreAccepted:
                // only preaccept if we coordinate the transaction
                if (safeStore.ranges().coordinates(txnId).intersects(scope) && Route.isFullRoute(scope))
                {
                    if (tracing != null)
                        tracing.trace(safeStore.commandStore(), "Pre-accepting");
                    Commands.preaccept(safeStore, safeCommand, participants, txnId, partialTxn, null, false);
                }
                break;
        }

        updateFetchResult(found, participants.owns());
        return updateDurability(safeStore, safeCommand, participants);
    }

    @Override
    protected Void refuseInternal(SafeCommandStore safeStore)
    {
        return null;
    }

    private void updateFetchResult(Known found, Participants<?> owns)
    {
        Known achieved = found.propagates();
        found = found.atLeast(foundAtLeast);
        Unseekables<?> achievedTarget = owns;
        if (target != null && !target.isSatisfiedBy(achieved))
            achievedTarget = owns.slice(0, 0);

        while (true)
        {
            FetchResult current = fetchResult;
            FetchResult next = current == null ? new FetchResult(target, achievedTarget, found)
                               : new FetchResult(target, achievedTarget.with((Unseekables)current.achievedTarget), found.reduce(current.foundAtLeast));

            if (fetchResultUpdater.compareAndSet(this, current, next))
                return;
        }
    }

    private FetchResult finaliseFetchResult()
    {
        FetchResult current = fetchResult;
        if (current == null)
            return new FetchResult(target, scope.slice(0, 0), foundAtLeast);

        return current;
    }

    // if can only propagate Truncated, we might be stale; try to upgrade for this command store only, even partially if necessary
    private Known tryUpgradeTruncated(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Command command, Timestamp executeAtIfKnown, Known found)
    {
        // if our peers have truncated this command, then either:
        // 1) we have already applied it locally; 2) the command doesn't apply locally; 3) we are stale; or 4) the command is invalidated
        Invariants.require(!maxKnowledgeSaveStatus.is(Status.Invalidated));

        Participants<?> stillTouches = participants.stillTouches();
        if (stillTouches.isEmpty())
        {
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "No longer participating (stillTouches is empty); using knownForAny: %s", known.knownForAny());
            return known.knownForAny();
        }

        RedundantStatus status = safeStore.redundantBefore().status(txnId, null, stillTouches);
        // try to see if we can safely purge the full command
        if (tryPurge(safeStore, safeCommand, status))
        {
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "Redundant with status %s; purged", status);
            return null;
        }

        // if the command has been truncated globally, then we should expect to apply it
        // if we cannot obtain enough information from a majority to do so then we have been left behind
        Known required = PreApplied.minKnown;
        Known requireExtra = required.subtract(command.known()); // the extra information we need to reach pre-applied

        Participants<?> stillOwnsOrMayExecute = txnId.isSyncPoint() ? participants.stillTouches() : participants.stillOwns();
        Participants<?> notStaleTouches = known.knownFor(Nothing.with(requireExtra.deps()), stillTouches); // the ranges for which we can already successfully achieve this
        Participants<?> notStaleOwnsOrMayExecute = known.knownFor(requireExtra.with(DepsUnknown), stillOwnsOrMayExecute); // the ranges for which we can already successfully achieve this

        // any ranges we execute but cannot achieve the pre-applied status for have been left behind and are stale
        Participants<?> staleTouches = stillTouches.without(notStaleTouches);
        Participants<?> staleOwnsOrMayExecute = stillOwnsOrMayExecute.without(notStaleOwnsOrMayExecute);
        if (staleOwnsOrMayExecute.isEmpty())
        {
            if (staleTouches.isEmpty() && found.is(Outcome.Apply))
            {
                Invariants.require(notStaleTouches.containsAll(stillTouches));
                Invariants.require(notStaleOwnsOrMayExecute.containsAll(stillOwnsOrMayExecute));
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "No longer touches any keys that were found truncated");
                return required;
            }

            if (stillOwnsOrMayExecute.isEmpty() && (!found.is(Outcome.Apply) || known.hasFullyTruncated(staleTouches)))
            {
                if (tracing != null)
                    tracing.trace(safeStore.commandStore(), "No longer owns or executes any keys that were found truncated; marking vestigial");
                Commands.setTruncatedOrVestigial(safeStore, safeCommand, participants);
                return null;
            }
            Invariants.require(!staleTouches.isEmpty());
        }

        Participants<?> stale = staleTouches.with((Participants) staleOwnsOrMayExecute);
        // TODO (expected): could be that two replicas are stale but cannot catch up;
        //  I think this condition is to ensure a full quorum has truncated (so we haven't raced with truncation)
        //  should solve another way
        if (!known.hasFullyTruncated(stale))
        {
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "Has participants %s that could not be fetched. Some responses were not truncated, so we may have raced with completion. Aborting.", stale);
            return null;
        }

        // TODO (expected): trigger a refresh of redundantBefore; should be available on a peer
        // wait until we know the shard is ahead and we are behind
        if (!safeStore.redundantBefore().isShardOnlyApplied(txnId, stale))
        {
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "Has participants %s that could not be fetched, but the shard(s) have not been marked universally durable so we will not mark ourselves stale. Aborting.", stale);
            return null;
        }

        Participants<?> staleOnlyTouches = staleTouches.without(staleOwnsOrMayExecute);
        Invariants.expect(txnId.awaitsPreviouslyOwned() || staleOnlyTouches.isEmpty(), "%s is SHARD_ONLY_APPLIED, so we expect it to have been filtered from StoreParticipants", staleOnlyTouches);
        // TODO (expected): if the above last ditch doesn't work, see if only the stale ranges can't apply and do some shenanigans to apply partially and move on
        if (ProtocolModifiers.Toggles.markStaleIfCannotExecute(txnId))
        {
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "Has participants %s that could not be fetched and the shard(s) have been marked universally durable. We have marked ourselves stale, and will apply the remaining ranges.", stale);

            safeStore.commandStore().markShardStale(safeStore, executeAtIfKnown == null ? txnId : executeAtIfKnown, stale.toRanges(), true);
            if (!stale.containsAll(stillTouches) || !stale.containsAll(stillOwnsOrMayExecute))
                return required;
        }
        else
        {
            if (tracing != null)
                tracing.trace(safeStore.commandStore(), "Has participants %s that could not be fetched and the shard(s) have been marked universally durable. This transaction type is configured not to induce staleness, so erasing.", stale);
        }

        // TODO (expected): we might prefer to adopt Redundant status, and permit ourselves to later accept the result of the execution and/or definition
        Commands.setTruncatedOrVestigial(safeStore, safeCommand, participants);
        return null;
    }

    private boolean tryPurge(SafeCommandStore safeStore, SafeCommand safeCommand, RedundantStatus status)
    {
        if (!status.all(SHARD_APPLIED, LOCALLY_SYNCED))
            return false;

        Cleanup cleanup = status.all(LOCALLY_DEFUNCT) ? VESTIGIAL : ERASE;
        purge(safeStore, safeCommand, cleanup, true, true);
        return true;
    }

    /*
     *  If there is new information about the command being durable and we are in the coordination shard in the coordination epoch then update the durability information and possibly cleanup
     */
    private Void updateDurability(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
    {
        if (!durability.isFastPathDurablyDecided() || homeKey == null)
            return null;

        Commands.setDurability(safeStore, safeCommand, participants, durability, committedExecuteAt);
        return null;
    }

    @Override
    public Void reduce(Void o1, Void o2)
    {
        return null;
    }

    @Override
    public void accept(Void result, Throwable failure)
    {
        if (null != callback)
            callback.accept(failure != null ? null : finaliseFetchResult(), failure);
        if (failure != null && tracing != null)
            tracing.trace(null, "Failed propagate: %s", failure);
    }

    private static void confirm(Commands.CommitOutcome outcome)
    {
        switch (outcome)
        {
            default: throw illegalState("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw illegalState("Should have enough information");
        }
    }

    private static void confirm(Commands.ApplyOutcome outcome)
    {
        switch (outcome)
        {
            default: throw illegalState("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
            case RaceWithRecovery:
                return;
            case InsufficientEpochs:
                // TODO (expected): do we definitely guarantee we have enough here?
                //  but may anyway not be safe to continue if we haven't
            case Insufficient:
                throw illegalState("Should have enough information");
        }
    }

    @Override
    public String toString()
    {
        return "Propagate{txnId: " + txnId +
               ", saveStatus: " + maxKnowledgeSaveStatus +
               ", deps: " + stableDeps +
               ", txn: " + partialTxn +
               ", executeAt: " + committedExecuteAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }

}
