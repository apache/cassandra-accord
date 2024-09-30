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

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.FetchData.FetchResult;
import accord.local.Command;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.RedundantStatus;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
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
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;

import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;
import static accord.local.Cleanup.ERASE;
import static accord.local.Cleanup.VESTIGIAL;
import static accord.local.Commands.purge;
import static accord.local.RedundantStatus.LOCALLY_REDUNDANT;
import static accord.primitives.SaveStatus.Stable;
import static accord.primitives.Status.NotDefined;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;

// TODO (required): detect propagate loops where we don't manage to update anything but should
public class Propagate implements PreLoadContext, MapReduceConsume<SafeCommandStore, Void>
{
    final Node node;
    final TxnId txnId;
    final Route<?> route;
    final Unseekables<?> propagateTo;
    final Known target;

    // TODO (desired): remove dependency on these two SaveStatus
    final SaveStatus maxKnowledgeSaveStatus;
    final SaveStatus maxSaveStatus;
    final Ballot ballot;
    final Status.Durability durability;
    @Nullable final RoutingKey homeKey;
    // this is a WHOLE NODE measure, so if commit epoch has more ranges we do not count as committed if we can only commit in coordination epoch
    final KnownMap known;
    @Nullable final PartialTxn partialTxn;
    @Nullable final PartialDeps stableDeps;
    // TODO (expected): toEpoch may only apply to certain local command stores that have "witnessed the future" - confirm it is fine to use globally or else narrow its scope
    final long lowEpoch, highEpoch;
    @Nullable final Timestamp committedExecuteAt;
    @Nullable final Writes writes;
    @Nullable final Result result;
    final BiConsumer<? super FetchResult, Throwable> callback;

    private transient volatile FetchResult fetchResult;
    private static final AtomicReferenceFieldUpdater<Propagate, FetchResult> fetchResultUpdater = AtomicReferenceFieldUpdater.newUpdater(Propagate.class, FetchResult.class, "fetchResult");

    Propagate(
    Node node, TxnId txnId,
    Route<?> route,
    Unseekables<?> propagateTo, Known target,
    SaveStatus maxKnowledgeSaveStatus,
    SaveStatus maxSaveStatus,
    Ballot ballot,
    Status.Durability durability,
    @Nullable RoutingKey homeKey,
    KnownMap known,
    @Nullable PartialTxn partialTxn,
    @Nullable PartialDeps stableDeps,
    long lowEpoch,
    long highEpoch,
    @Nullable Timestamp committedExecuteAt,
    @Nullable Writes writes,
    @Nullable Result result,
    BiConsumer<? super FetchResult, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.propagateTo = propagateTo;
        this.target = target;
        this.maxKnowledgeSaveStatus = maxKnowledgeSaveStatus;
        this.maxSaveStatus = maxSaveStatus;
        this.ballot = ballot;
        this.durability = durability;
        this.homeKey = homeKey;
        this.known = known;
        this.partialTxn = partialTxn;
        this.stableDeps = stableDeps;
        this.lowEpoch = lowEpoch;
        this.highEpoch = highEpoch;
        this.committedExecuteAt = committedExecuteAt;
        this.writes = writes;
        this.result = result;
        this.callback = callback;
    }

    public static void propagate(Node node, TxnId txnId, long sourceEpoch, WithQuorum withQuorum, Route<?> route, Unseekables<?> propagateTo, @Nullable Known target, CheckStatusOkFull full, BiConsumer<? super FetchResult, Throwable> callback)
    {
        propagate(node, txnId, sourceEpoch, txnId.epoch(), sourceEpoch, withQuorum, route, propagateTo, target, full, callback);
    }

    public static void propagate(Node node, TxnId txnId, long sourceEpoch, long lowEpoch, long highEpoch, WithQuorum withQuorum, Route<?> route, Unseekables<?> propagateTo, @Nullable Known target, CheckStatusOkFull full, BiConsumer<? super FetchResult, Throwable> callback)
    {
        if (full.maxKnowledgeSaveStatus.status == NotDefined && full.invalidIf == NotKnownToBeInvalid)
        {
            callback.accept(new FetchResult(Known.Nothing, propagateTo.slice(0, 0), propagateTo), null);
            return;
        }

        Invariants.checkState(sourceEpoch == txnId.epoch() || (full.executeAt != null && sourceEpoch == full.executeAt.epoch()) || full.maxKnowledgeSaveStatus == SaveStatus.Erased || full.maxKnowledgeSaveStatus == SaveStatus.ErasedOrVestigial);

        route = route.with((Unseekables) propagateTo);
        full = full.finish(route, withQuorum);
        route = Invariants.nonNull(full.route);

        Propagate propagate =
            new Propagate(node, txnId, route, propagateTo, target, full.maxKnowledgeSaveStatus, full.maxSaveStatus, full.acceptedOrCommitted, full.durability, full.homeKey, full.map, full.partialTxn, full.stableDeps, lowEpoch, highEpoch, full.executeAtIfKnown(), full.writes, full.result, callback);

        if (full.executeAt != null && full.executeAt.epoch() > highEpoch)
            highEpoch = full.executeAt.epoch();
        long untilEpoch = full.executeAt == null ? highEpoch : Math.max(highEpoch, full.executeAt.epoch());

        Route<?> finalRoute = route;
        node.withEpoch(highEpoch, propagate, () -> node.mapReduceConsumeLocal(propagate, finalRoute, lowEpoch, untilEpoch, propagate));
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Unseekables<?> keys()
    {
        // TODO (required): confirm this includes Route
        return propagateTo;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.COMMANDS;
    }

    @Override
    public Void apply(SafeCommandStore safeStore)
    {
        long executeAtEpoch = committedExecuteAt == null ? txnId.epoch() : committedExecuteAt.epoch();
        // TODO (required): rework low and high epoch handling; we should replicate coordination quorum intersections here,
        //  and then we can safely compute our participants based on this. right now we're ignoring low/high epoch except for
        //  deciding which commandStores to notify
        StoreParticipants participants = StoreParticipants.update(safeStore, route, txnId.epoch(), txnId, executeAtEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();

        Timestamp executeAtIfKnown = command.executeAtIfKnown(committedExecuteAt);
        if (executeAtIfKnown != null && executeAtIfKnown.epoch() != executeAtEpoch)
        {
            executeAtEpoch = executeAtIfKnown.epoch();
            StoreParticipants newParticipants = participants.supplement(StoreParticipants.update(safeStore, route, txnId.epoch(), txnId, executeAtEpoch));
            if (participants != newParticipants)
            {
                participants = newParticipants;
                if (Commands.maybeCleanup(safeStore, safeCommand, command, participants))
                    command = safeCommand.current();
            }
        }

        switch (command.saveStatus().phase)
        {
            // Already know the outcome, waiting on durability so maybe update with new durability information which can also trigger cleanup
            case Persist: return updateDurability(safeStore, safeCommand, participants);
            case Cleanup: return null;
        }

        participants = participants.supplement(command.participants());
        Known found = known.knownFor(participants.owns(), participants.touches());

        PartialTxn partialTxn = null;
        if (found.hasDefinition())
            partialTxn = this.partialTxn.intersecting(participants.owns(), true).reconstitutePartial(participants.owns());

        PartialDeps stableDeps = null;
        if (found.hasDecidedDeps())
            stableDeps = this.stableDeps.intersecting(participants.touches()).reconstitutePartial(participants.touches());

        boolean isShardTruncated = known.hasTruncated(participants.owns());
        if (isShardTruncated)
        {
            // TODO (required): applyOrUpgradeTruncated assumes we had a node-level achieved, whereas we now compute at CommandStore level; simplify/merge
            // TODO (required): do not markShardStale for reads; in general optimise handling of case where we cannot recover a known no-op transaction
            // TODO (required): permit staleness to be gated by some configuration state
            found = applyOrUpgradeTruncated(safeStore, safeCommand, participants, command, executeAtIfKnown);
            if (found == null)
            {
                updateFetchResult(Known.Nothing, participants.owns());
                return null;
            }

            Participants<?> needed = participants.owns();
            if (found.isDefinitionKnown() && partialTxn == null && this.partialTxn != null)
            {
                PartialTxn existing = command.partialTxn();
                Participants<?> neededExtra = needed;
                if (existing != null) neededExtra = neededExtra.without(existing.keys().toParticipants());
                partialTxn = this.partialTxn.intersecting(neededExtra, true).reconstitutePartial(neededExtra);
            }

            needed = participants.touches();
            if (found.hasDecidedDeps() && stableDeps == null && this.stableDeps != null)
            {
                Invariants.checkState(executeAtIfKnown != null);
                // we don't subtract existing partialDeps, as they cannot be committed deps; we only permit committing deps covering all participating ranges
                stableDeps = this.stableDeps.intersecting(needed).reconstitutePartial(needed);
            }
        }

        SaveStatus propagate = found.atLeast(command.known()).propagatesSaveStatus();
        if (propagate.known.isSatisfiedBy(command.known()))
        {
            updateFetchResult(found, participants.owns());
            return updateDurability(safeStore, safeCommand, participants);
        }

        switch (propagate.status)
        {
            default: throw illegalState("Unexpected status: " + propagate);
            case Truncated: throw illegalState("Status expected to be handled elsewhere: " + propagate);
            case Accepted:
            case AcceptedInvalidate:
                // we never "propagate" accepted statuses as these are essentially votes,
                // and contribute nothing to our local state machine
                throw illegalState("Invalid states to propagate: " + propagate);

            case Invalidated:
                Commands.commitInvalidate(safeStore, safeCommand, route);
                break;

            case Applied:
            case PreApplied:
                Invariants.checkState(executeAtIfKnown != null);
                confirm(Commands.apply(safeStore, safeCommand, participants, txnId, route, executeAtIfKnown, stableDeps, partialTxn, writes, result));
                break;

            case Stable:
                confirm(Commands.commit(safeStore, safeCommand, participants, Stable, ballot, txnId, route, partialTxn, executeAtIfKnown, stableDeps));
                break;

            case Committed:
                // TODO (expected): we can propagate Committed as Stable if we have any other Stable result AND a quorum of committedDeps
            case PreCommitted:
                confirm(Commands.precommit(safeStore, safeCommand, participants, txnId, executeAtIfKnown));
                // TODO (desired): would it be clearer to yield a SaveStatus so we can have PreCommittedWithDefinition
                if (!found.definition.isKnown())
                    break;

            case PreAccepted:
                // only preaccept if we coordinate the transaction
                if (safeStore.ranges().coordinates(txnId).intersects(route) && Route.isFullRoute(route))
                    Commands.preaccept(safeStore, safeCommand, participants, txnId, txnId.epoch(), partialTxn, Route.castToFullRoute(route));
                break;

            case NotDefined:
                break;
        }

        updateFetchResult(found.propagates(), participants.owns());
        return updateDurability(safeStore, safeCommand, participants);
    }

    private void updateFetchResult(Known achieved, Participants<?> owns)
    {
        achieved = achieved.propagates();
        Unseekables<?> achievedTarget = owns;
        Unseekables<?> didNotAchieveTarget = null;
        if (target != null && !target.isSatisfiedBy(achieved))
        {
            achievedTarget = owns.slice(0, 0);
            didNotAchieveTarget = owns;
        }

        FetchResult current = fetchResult;
        while (true)
        {
            FetchResult next = current == null ? new FetchResult(achieved, achievedTarget, didNotAchieveTarget)
                               : new FetchResult(achieved.reduce(current.achieved),
                                                 achievedTarget.with((Unseekables)current.achievedTarget),
                                                 Unseekables.merge(current.didNotAchieveTarget, (Unseekables) didNotAchieveTarget));

            if (fetchResultUpdater.compareAndSet(this, current, next))
                return;
        }
    }

    private FetchResult finaliseFetchResult()
    {
        FetchResult current = fetchResult;
        if (current == null)
            return new FetchResult(Known.Nothing, propagateTo.slice(0, 0), propagateTo);

        Unseekables<?> missed = propagateTo.without(current.achievedTarget);
        if (missed.isEmpty())
            return current;

        return new FetchResult(Known.Nothing, current.achievedTarget, Unseekables.merge(missed, (Unseekables) current.didNotAchieveTarget));
    }

    // if can only propagate Truncated, we might be stale; try to upgrade for this command store only, even partially if necessary
    private Known applyOrUpgradeTruncated(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Command command, Timestamp executeAtIfKnown)
    {
        // if our peers have truncated this command, then either:
        // 1) we have already applied it locally; 2) the command doesn't apply locally; 3) we are stale; or 4) the command is invalidated
        Invariants.checkState(!maxKnowledgeSaveStatus.is(Status.Invalidated));

        if (participants.owns().isEmpty())
            return known.knownForAny();

        RedundantStatus status = safeStore.commandStore().redundantBefore().status(txnId, participants.owns());

        // if our peers have truncated this command, then either:
        // 1) we have already applied it locally; 2) the command doesn't apply locally; 3) we are stale; or 4) the command is invalidated
        if (executeAtIfKnown == null)
        {
            if (status.compareTo(LOCALLY_REDUNDANT) >= 0)
            {
                purge(safeStore, safeCommand, null, ERASE, true);
                return null;
            }

            Ranges ranges = safeStore.ranges().allSince(txnId.epoch());
            ranges = safeStore.commandStore().redundantBefore().everExpectToExecute(txnId, ranges);
            if (!ranges.isEmpty())
            {
                // even though command stores only lose ranges, we still adopt ranges as of some epoch, and re-bootstrap.
                // however, this eventuality should be exceptionally rare
                // we don't even know the execution time, so we cannot possibly proceed besides erasing the command state and marking ourselves stale
                // TODO (required): do not setErased until markShardStale is complete
                safeStore.commandStore().markShardStale(safeStore, txnId, route.slice(ranges, Minimal).toRanges(), false);
            }
            Commands.setErased(safeStore, safeCommand);
            return null;
        }

        if (tryPurge(safeStore, safeCommand, status))
            return null;

        Participants<?> executes = participants.executes(safeStore, txnId, executeAtIfKnown);
        status = safeStore.commandStore().redundantBefore().status(txnId, executes);
        if (tryPurge(safeStore, safeCommand, status))
            return null;

        // compute the ranges we expect to execute - i.e. those we own, and are not stale or pre-bootstrap
        // TODO (required): use StoreParticipants.executes
        Ranges ranges = safeStore.ranges().allAt(executeAtIfKnown.epoch());
        ranges = safeStore.commandStore().redundantBefore().expectToExecute(txnId, executeAtIfKnown, ranges);
        if (ranges.isEmpty() || (executes = executes.slice(ranges, Minimal)).isEmpty())
        {
            // TODO (expected): we might prefer to adopt Redundant status, and permit ourselves to later accept the result of the execution and/or definition
            Commands.setTruncatedApplyOrErasedVestigial(safeStore, safeCommand, participants, executeAtIfKnown);
            return null;
        }

        // if the command has been truncated globally, then we should expect to apply it
        // if we cannot obtain enough information from a majority to do so then we have been left behind
        Known required = PreApplied.minKnown;
        Known requireExtra = required.subtract(command.known()); // the extra information we need to reach pre-applied
        Ranges achieveRanges = known.knownForRanges(requireExtra, ranges); // the ranges for which we can already successfully achieve this

        // any ranges we execute but cannot achieve the pre-applied status for have been left behind and are stale
        Ranges staleRanges = ranges.without(achieveRanges);
        Participants<?> staleParticipants = executes.slice(staleRanges, Minimal);
        staleRanges = staleParticipants.toRanges();

        if (staleRanges.isEmpty())
        {
            Invariants.checkState(achieveRanges.containsAll(executes));
            return required;
        }

        safeStore.commandStore().markShardStale(safeStore, executeAtIfKnown, staleRanges, true);
        if (!staleRanges.containsAll(executes))
            return required;

        // TODO (expected): we might prefer to adopt Redundant status, and permit ourselves to later accept the result of the execution and/or definition
        Commands.setTruncatedApplyOrErasedVestigial(safeStore, safeCommand, participants, executeAtIfKnown);
        return null;
    }

    private boolean tryPurge(SafeCommandStore safeStore, SafeCommand safeCommand, RedundantStatus status)
    {
        switch (status)
        {
            default: throw new AssertionError("Unhandled RedundantStatus: " + status);
            case NOT_OWNED:
            case WAS_OWNED:
            case SHARD_REDUNDANT:
            case LOCALLY_REDUNDANT:
                purge(safeStore, safeCommand, null, ERASE, true);
                return true;
            case PRE_BOOTSTRAP_OR_STALE:
            case REDUNDANT_PRE_BOOTSTRAP_OR_STALE:
                purge(safeStore, safeCommand, null, VESTIGIAL, true);
                return true;
            case PARTIALLY_PRE_BOOTSTRAP_OR_STALE:
            case LIVE:
                return false;
        }
    }

    /*
     *  If there is new information about the command being durable and we are in the coordination shard in the coordination epoch then update the durability information and possibly cleanup
     */
    private Void updateDurability(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
    {
        // TODO (expected): Infer durability status from cleanup/truncation
        if (!durability.isDurable() || homeKey == null)
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
                return;
            case Insufficient: throw illegalState("Should have enough information");
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
