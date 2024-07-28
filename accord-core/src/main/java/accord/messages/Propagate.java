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

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Infer;
import accord.local.Command;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.messages.CheckStatus.FoundKnownMap;
import accord.messages.CheckStatus.WithQuorum;
import accord.primitives.Ballot;
import accord.primitives.EpochSupplier;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import javax.annotation.Nullable;
import java.util.function.BiConsumer;

import static accord.coordinate.Infer.InvalidIfNot.NotKnownToBeInvalid;
import static accord.local.RedundantStatus.LOCALLY_REDUNDANT;
import static accord.local.SaveStatus.Stable;
import static accord.local.SaveStatus.Uninitialised;
import static accord.local.Status.NotDefined;
import static accord.local.Status.Phase.Cleanup;
import static accord.local.Status.PreApplied;
import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;

// TODO (required): detect propagate loops where we don't manage to update anything but should
public class Propagate implements EpochSupplier, LocalRequest<Status.Known>, PreLoadContext, MapReduceConsume<SafeCommandStore, Void>
{
    public static class SerializerSupport
    {
        public static Propagate create(TxnId txnId, Route<?> route, SaveStatus maxKnowledgeSaveStatus, SaveStatus maxSaveStatus, Ballot ballot, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Status.Known achieved, FoundKnownMap known, boolean isTruncated, PartialTxn partialTxn, PartialDeps committedDeps, long toEpoch, Timestamp executeAt, Writes writes, Result result)
        {
            return new Propagate(txnId, route, maxKnowledgeSaveStatus, maxSaveStatus, ballot, durability, homeKey, progressKey, achieved, known, isTruncated, partialTxn, committedDeps, toEpoch, executeAt, writes, result);
        }
    }

    public final TxnId txnId;
    public final Route<?> route;
    // TODO (expected): remove dependency on these two SaveStatus
    public final SaveStatus maxKnowledgeSaveStatus;
    public final SaveStatus maxSaveStatus;
    public final Ballot ballot;
    public final Status.Durability durability;
    @Nullable public final RoutingKey homeKey;
    @Nullable public final RoutingKey progressKey;
    // this is a WHOLE NODE measure, so if commit epoch has more ranges we do not count as committed if we can only commit in coordination epoch
    public final Status.Known achieved;
    public final FoundKnownMap known;
    public final boolean isShardTruncated;
    @Nullable public final PartialTxn partialTxn;
    @Nullable public final PartialDeps stableDeps;
    // TODO (expected): toEpoch may only apply to certain local command stores that have "witnessed the future" - confirm it is fine to use globally or else narrow its scope
    public final long toEpoch;
    @Nullable public final Timestamp committedExecuteAt;
    @Nullable public final Writes writes;
    @Nullable public final Result result;
    protected transient BiConsumer<? super Status.Known, Throwable> callback;

    Propagate(
        TxnId txnId,
        Route<?> route,
        SaveStatus maxKnowledgeSaveStatus,
        SaveStatus maxSaveStatus,
        Ballot ballot,
        Status.Durability durability,
        @Nullable RoutingKey homeKey,
        @Nullable RoutingKey progressKey,
        Status.Known achieved,
        FoundKnownMap known,
        boolean isShardTruncated,
        @Nullable PartialTxn partialTxn,
        @Nullable PartialDeps stableDeps,
        long toEpoch,
        @Nullable Timestamp committedExecuteAt,
        @Nullable Writes writes,
        @Nullable Result result)
    {
        this.txnId = txnId;
        this.route = route;
        this.maxKnowledgeSaveStatus = maxKnowledgeSaveStatus;
        this.maxSaveStatus = maxSaveStatus;
        this.ballot = ballot;
        this.durability = durability;
        this.homeKey = homeKey;
        this.progressKey = progressKey;
        this.achieved = achieved;
        this.known = known;
        this.isShardTruncated = isShardTruncated;
        this.partialTxn = partialTxn;
        this.stableDeps = stableDeps;
        this.toEpoch = toEpoch;
        this.committedExecuteAt = committedExecuteAt;
        this.writes = writes;
        this.result = result;
    }

    @Override
    public void process(Node on, BiConsumer<? super Status.Known, Throwable> callback)
    {
        this.callback = callback;
        on.mapReduceConsumeLocal(this, route, txnId.epoch(), toEpoch, this);
    }

    @SuppressWarnings({"rawtypes"})
    public static void propagate(Node node, TxnId txnId, long sourceEpoch, WithQuorum withQuorum, Route route, @Nullable Status.Known target, CheckStatus.CheckStatusOkFull full, BiConsumer<Status.Known, Throwable> callback)
    {
        propagate(node, txnId, sourceEpoch, sourceEpoch, withQuorum, route, target, full, callback);
    }

    public static void propagate(Node node, TxnId txnId, long sourceEpoch, long toEpoch, WithQuorum withQuorum, Route route, @Nullable Status.Known target, CheckStatus.CheckStatusOkFull full, BiConsumer<Status.Known, Throwable> callback)
    {
        if (full.maxKnowledgeSaveStatus.status == NotDefined && full.maxInvalidIfNot() == NotKnownToBeInvalid)
        {
            callback.accept(Status.Known.Nothing, null);
            return;
        }

        Invariants.checkState(sourceEpoch == txnId.epoch() || (full.executeAt != null && sourceEpoch == full.executeAt.epoch()) || full.maxKnowledgeSaveStatus == SaveStatus.Erased || full.maxKnowledgeSaveStatus == SaveStatus.ErasedOrInvalidOrVestigial);

        full = full.finish(route, withQuorum);
        route = Invariants.nonNull(full.route);

        Ranges sliceRanges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);

        RoutingKey progressKey = node.trySelectProgressKey(txnId, route);

        Route<?> covering = route.slice(sliceRanges, Minimal);
        Status.Known achieved = full.knownFor(covering);
        if (achieved.executeAt.isDecidedAndKnownToExecute() && full.executeAt.epoch() > toEpoch)
        {
            Invariants.checkState(Route.isFullRoute(route));
            Ranges acceptRanges;
            if (!node.topology().hasEpoch(full.executeAt.epoch()) ||
                (!covering.containsAll(route.slice(acceptRanges = node.topology().localRangesForEpochs(txnId.epoch(), full.executeAt.epoch())))))
            {
                // we don't know what the execution epoch requires, so we cannot be sure we can replicate it locally
                // we *could* wait until we have the local epoch before running this
                Status.Outcome outcome = achieved.outcome.propagatesBetweenShards() ? achieved.outcome : Status.Outcome.Unknown;
                achieved = new Status.Known(achieved.route, achieved.definition, achieved.executeAt, Status.KnownDeps.DepsUnknown, outcome);
            }
            else
            {
                // TODO (expected): this should only be the two precise epochs, not the full range of epochs
                sliceRanges = acceptRanges;
                covering = route.slice(sliceRanges, Minimal);
                Status.Known knownForExecution = full.knownFor(covering);
                if ((target != null && target.isSatisfiedBy(knownForExecution)) || achieved.isSatisfiedBy(knownForExecution))
                {
                    achieved = knownForExecution;
                    toEpoch = full.executeAt.epoch();
                }
                else
                {   // TODO (expected): does downgrading this ever block progress?
                    Invariants.checkState(sourceEpoch == txnId.epoch(), "%d != %d", sourceEpoch, txnId.epoch());
                    achieved = new Status.Known(achieved.route, achieved.definition, achieved.executeAt, knownForExecution.deps, knownForExecution.outcome);
                }
            }
        }

        boolean isShardTruncated = withQuorum == HasQuorum && full.isTruncatedResponse(covering);

        PartialTxn partialTxn = full.partialTxn;
        if (achieved.definition.isKnown())
            partialTxn = full.partialTxn.intersecting(route, true).reconstitutePartial(covering);

        PartialDeps stableDeps = full.stableDeps;
        if (achieved.deps.hasDecidedDeps())
            stableDeps = full.stableDeps.intersecting(route).reconstitutePartial(covering);

        Propagate propagate =
            new Propagate(txnId, route, full.maxKnowledgeSaveStatus, full.maxSaveStatus, full.acceptedOrCommitted, full.durability, full.homeKey, progressKey, achieved, full.map, isShardTruncated, partialTxn, stableDeps, toEpoch, full.executeAtIfKnown(), full.writes, full.result);

        node.localRequest(propagate, callback);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        // TODO (required): these may be insufficient; must make update of CommandsForKey async
        if (partialTxn != null)
            return partialTxn.keys();

        if (stableDeps != null)
            return stableDeps.keyDeps.keys();

        return Keys.EMPTY;
    }

    @Override
    public KeyHistory keyHistory()
    {
        return KeyHistory.COMMANDS;
    }

    @Override
    public Void apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, this, route);
        Command command = safeCommand.current();

        PartialTxn partialTxn = achieved.hasDefinition() ? this.partialTxn : null;
        PartialDeps stableDeps = achieved.hasDecidedDeps() ? this.stableDeps : null;
        switch (command.saveStatus().phase)
        {
            // Already know the outcome, waiting on durability so maybe update with new durability information which can also trigger cleanup
            case Persist: return updateDurability(safeStore, safeCommand);
            case Cleanup: return null;
        }

        Timestamp executeAtIfKnown = command.executeAtIfKnown(committedExecuteAt);
        Status.Known achieved = this.achieved;
        if (isShardTruncated)
        {
            // TODO (required): do not markShardStale for reads; in general optimise handling of case where we cannot recover a known no-op transaction
            // TODO (required): permit staleness to be gated by some configuration state
            achieved = applyOrUpgradeTruncated(safeStore, safeCommand, command, executeAtIfKnown);
            if (achieved == null)
                return null;

            Participants<?> needed = route.slice(safeStore.ranges().allBetween(txnId.epoch(), (executeAtIfKnown == null ? txnId : executeAtIfKnown).epoch()));
            if (achieved.isDefinitionKnown() && partialTxn == null && this.partialTxn != null)
            {
                PartialTxn existing = command.partialTxn();
                Participants<?> neededExtra = needed;
                if (existing != null) neededExtra = neededExtra.subtract(existing.keys().toParticipants());
                partialTxn = this.partialTxn.intersecting(neededExtra, true).reconstitutePartial(neededExtra);
            }

            if (achieved.hasDecidedDeps() && stableDeps == null && this.stableDeps != null)
            {
                Invariants.checkState(executeAtIfKnown != null);
                // we don't subtract existing partialDeps, as they cannot be committed deps; we only permit committing deps covering all participating ranges
                stableDeps = this.stableDeps.intersecting(needed).reconstitutePartial(needed);
            }
        }

        Status propagate = achieved.atLeast(command.known()).propagatesStatus();
        if (command.hasBeen(propagate))
        {
            if (maxSaveStatus.phase == Cleanup && durability.isDurableOrInvalidated() && Infer.safeToCleanup(safeStore, command, route, executeAtIfKnown))
                Commands.setTruncatedApplyOrErasedVestigial(safeStore, safeCommand);

            // TODO (expected): maybe stale?
            return updateDurability(safeStore, safeCommand);
        }

        switch (propagate)
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
                if (toEpoch >= executeAtIfKnown.epoch())
                {
                    confirm(Commands.apply(safeStore, safeCommand, txnId, route, progressKey, executeAtIfKnown, stableDeps, partialTxn, writes, result));
                    break;
                }

            case Stable:
                confirm(Commands.commit(safeStore, safeCommand, Stable, ballot, txnId, route, progressKey, partialTxn, executeAtIfKnown, stableDeps));
                break;

            case Committed:
                // TODO (expected): we can propagate Committed as Stable if we have any other Stable result AND a quorum of committedDeps
            case PreCommitted:
                confirm(Commands.precommit(safeStore, safeCommand, txnId, executeAtIfKnown, route));
                // TODO (desired): would it be clearer to yield a SaveStatus so we can have PreCommittedWithDefinition
                if (!achieved.definition.isKnown())
                    break;

            case PreAccepted:
                // only preaccept if we coordinate the transaction
                if (safeStore.ranges().coordinates(txnId).intersects(route) && Route.isFullRoute(route))
                    Commands.preaccept(safeStore, safeCommand, txnId, txnId.epoch(), partialTxn, Route.castToFullRoute(route), progressKey);
                break;

            case NotDefined:
                break;
        }

        return updateDurability(safeStore, safeCommand);
    }

    // if can only propagate Truncated, we might be stale; try to upgrade for this command store only, even partially if necessary
    // note: this is invoked if the command is truncated for ANY local command store - we might
    private Status.Known applyOrUpgradeTruncated(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, Timestamp executeAtIfKnown)
    {
        Invariants.checkState(!maxKnowledgeSaveStatus.is(Status.Invalidated));

        if (Infer.safeToCleanup(safeStore, command, route, executeAtIfKnown))
        {
            // don't create a new Erased record if we're already cleaned up
            Commands.setErased(safeStore, safeCommand);
            return null;
        }

        Ranges ranges = safeStore.ranges().allBetween(txnId.epoch(), (executeAtIfKnown == null ? txnId : executeAtIfKnown).epoch());
        Participants<?> participants = route.participants(ranges, Minimal);
        if (participants.isEmpty())
        {
            // we shouldn't be fetching data for transactions we only coordinate
            // however we might fetch data for ranges we expected to execute (e.g. during accept round) but ended up not executing
            if (command.additionalKeysOrRanges() == null)
            {
                Invariants.checkState(command.saveStatus() == Uninitialised);
                return null;
            }

            // TODO (required): if everyone else has truncated, we might not have enough information on any replicas we contact
            //    in this case we should erase
            return known.knownForAny();
        }

        boolean isTruncatedForLocalRanges = known.hasTruncated(participants);
        if (!isTruncatedForLocalRanges)
        {
            // we're truncated *somewhere* but not locally; whether we have the executeAt is immaterial to this calculus,
            // as we're either ready to go or we're waiting on the coordinating shard to complete this transaction, so pick
            // the maximum we can achieve and return that
            return known.knownFor(participants);
        }

        // if our peers have truncated this command, then either:
        // 1) we have already applied it locally; 2) the command doesn't apply locally; 3) we are stale; or 4) the command is invalidated
        // we're now at least partially stale, but let's first see if we can progress this shard, or we can do so in part
        if (executeAtIfKnown == null)
        {
            // TODO (desired): merge this with the executeAt known path where possible, as a lot of duplication
            if (safeStore.commandStore().redundantBefore().status(txnId, txnId, participants).compareTo(LOCALLY_REDUNDANT) >= 0)
            {
                // invalidated
                Commands.setErased(safeStore, safeCommand);
                return null;
            }

            ranges = safeStore.commandStore().redundantBefore().everExpectToExecute(txnId, ranges);
            if (!ranges.isEmpty())
            {
                // we don't even know the execution time, so we cannot possibly proceed besides erasing the command state and marking ourselves stale
                // TODO (required): we could in principle be stale for future epochs we haven't witnessed yet. Ensure up to date epochs before finalising this application, or else fetch a maximum possible epoch
                safeStore.commandStore().markShardStale(safeStore, txnId, participants.toRanges().slice(ranges, Minimal), false);
            }
            Commands.setErased(safeStore, safeCommand);
            return null;
        }

        if (safeStore.commandStore().redundantBefore().status(txnId, executeAtIfKnown, participants).compareTo(LOCALLY_REDUNDANT) >= 0)
        {
            // invalidated
            Commands.setErased(safeStore, safeCommand);
            return null;
        }

        // compute the ranges we expect to execute - i.e. those we own, and are not stale or pre-bootstrap
        ranges = safeStore.commandStore().redundantBefore().expectToExecute(txnId, executeAtIfKnown, ranges);
        if (ranges.isEmpty())
        {
            // TODO (expected): we might prefer to adopt Redundant status, and permit ourselves to later accept the result of the execution and/or definition
            Commands.setTruncatedApplyOrErasedVestigial(safeStore, safeCommand, executeAtIfKnown, route);
            return null;
        }

        participants = participants.slice(ranges, Minimal);
        if (participants.isEmpty())
        {
            // we only coordinate this transaction, so being unable to retrieve its state does not imply any staleness
            // TODO (now): double check this doesn't stop us coordinating the transaction (it shouldn't, as doesn't imply durability)
            Commands.setTruncatedApplyOrErasedVestigial(safeStore, safeCommand, executeAtIfKnown, route);
            return null;
        }

        // if the command has been truncated globally, then we should expect to apply it
        // if we cannot obtain enough information from a majority to do so then we have been left behind
        Status.Known required = PreApplied.minKnown;
        Status.Known requireExtra = required.subtract(command.known()); // the extra information we need to reach pre-applied
        Ranges achieveRanges = known.knownFor(requireExtra, ranges); // the ranges for which we can already successfully achieve this

        // any ranges we execute but cannot achieve the pre-applied status for have been left behind and are stale
        Ranges staleRanges = ranges.subtract(achieveRanges);
        Participants<?> staleParticipants = participants.slice(staleRanges, Minimal);
        staleRanges = staleParticipants.toRanges();

        if (staleRanges.isEmpty())
        {
            Invariants.checkState(achieveRanges.containsAll(participants));
            return required;
        }

        safeStore.commandStore().markShardStale(safeStore, executeAtIfKnown, staleRanges, true);
        if (!staleRanges.containsAll(participants))
            return required;

        // TODO (expected): we might prefer to adopt Redundant status, and permit ourselves to later accept the result of the execution and/or definition
        Commands.setTruncatedApplyOrErasedVestigial(safeStore, safeCommand, executeAtIfKnown, route);
        return null;
    }

    /*
     *  If there is new information about the command being durable and we are in the coordination shard in the coordination epoch then update the durability information and possibly cleanup
     */
    private Void updateDurability(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        // TODO (expected): Infer durability status from cleanup/truncation
        if (!durability.isDurable() || homeKey == null)
            return null;

        Commands.setDurability(safeStore, safeCommand, durability, route, committedExecuteAt, this);
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
            callback.accept(failure == null ? achieved : null, failure);
    }

    @Override
    public MessageType type()
    {
        // TODO (now): this logic doesn't work now we permit upgrading; need to pick the maximum *possible* status we might propagate
        //     might be better to
        switch (achieved.propagatesStatus())
        {
            case Applied:
            case PreApplied:
                if (toEpoch >= committedExecuteAt.epoch())
                    return MessageType.PROPAGATE_APPLY_MSG;
            case Committed:
            case Stable:
                return MessageType.PROPAGATE_STABLE_MSG;
            case PreCommitted:
                if (!achieved.definition.isKnown())
                    return MessageType.PROPAGATE_OTHER_MSG;
            case PreAccepted:
                return MessageType.PROPAGATE_PRE_ACCEPT_MSG;
            default:
                return MessageType.PROPAGATE_OTHER_MSG;
        }
    }

    @Override
    public long epoch()
    {
        return toEpoch;
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
        return "Propagate{type:" + type() +
               ", txnId: " + txnId +
               ", saveStatus: " + maxKnowledgeSaveStatus +
               ", deps: " + stableDeps +
               ", txn: " + partialTxn +
               ", executeAt: " + committedExecuteAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }

}
