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
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.messages.CheckStatus.FoundKnownMap;
import accord.messages.CheckStatus.WithQuorum;
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

import javax.annotation.Nullable;
import java.util.function.BiConsumer;

import static accord.coordinate.Infer.InvalidIfNot.NotKnownToBeInvalid;
import static accord.local.RedundantStatus.LOCALLY_REDUNDANT;
import static accord.local.Status.NotDefined;
import static accord.local.Status.Phase.Cleanup;
import static accord.local.Status.PreApplied;
import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.primitives.Routables.Slice.Minimal;

public class Propagate implements EpochSupplier, LocalRequest<Status.Known>
{
    public static class SerializerSupport
    {
        public static Propagate create(TxnId txnId, Route<?> route, SaveStatus maxKnowledgeSaveStatus, SaveStatus maxSaveStatus, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Status.Known achieved, FoundKnownMap known, boolean isTruncated, PartialTxn partialTxn, PartialDeps committedDeps, long toEpoch, Timestamp executeAt, Writes writes, Result result)
        {
            return new Propagate(txnId, route, maxKnowledgeSaveStatus, maxSaveStatus, durability, homeKey, progressKey, achieved, known, isTruncated, partialTxn, committedDeps, toEpoch, executeAt, writes, result, null);
        }
    }

    public final TxnId txnId;
    public final Route<?> route;
    // TODO (expected): remove dependency on these two SaveStatus
    public final SaveStatus maxKnowledgeSaveStatus;
    public final SaveStatus maxSaveStatus;
    public final Status.Durability durability;
    @Nullable public final RoutingKey homeKey;
    @Nullable public final RoutingKey progressKey;
    // this is a WHOLE NODE measure, so if commit epoch has more ranges we do not count as committed if we can only commit in coordination epoch
    public final Status.Known achieved;
    public final FoundKnownMap known;
    public final boolean isTruncated;
    @Nullable public final PartialTxn partialTxn;
    @Nullable public final PartialDeps committedDeps;
    public final long toEpoch;
    @Nullable public final Timestamp committedExecuteAt;
    @Nullable public final Writes writes;
    @Nullable public final Result result;

    protected transient BiConsumer<Status.Known, Throwable> callback;

    @Override
    public BiConsumer<Status.Known, Throwable> callback()
    {
        return callback;
    }

    Propagate(
        TxnId txnId,
        Route<?> route,
        SaveStatus maxKnowledgeSaveStatus,
        SaveStatus maxSaveStatus,
        Status.Durability durability,
        @Nullable RoutingKey homeKey,
        @Nullable RoutingKey progressKey,
        Status.Known achieved,
        FoundKnownMap known,
        boolean isTruncated,
        @Nullable PartialTxn partialTxn,
        @Nullable PartialDeps committedDeps,
        long toEpoch,
        @Nullable Timestamp committedExecuteAt,
        @Nullable Writes writes,
        @Nullable Result result,
        BiConsumer<Status.Known, Throwable> callback)
    {
        this.txnId = txnId;
        this.route = route;
        this.maxKnowledgeSaveStatus = maxKnowledgeSaveStatus;
        this.maxSaveStatus = maxSaveStatus;
        this.durability = durability;
        this.homeKey = homeKey;
        this.progressKey = progressKey;
        this.achieved = achieved;
        this.known = known;
        this.isTruncated = isTruncated;
        this.partialTxn = partialTxn;
        this.committedDeps = committedDeps;
        this.toEpoch = toEpoch;
        this.committedExecuteAt = committedExecuteAt;
        this.writes = writes;
        this.result = result;
        this.callback = callback;
    }

    @Override
    public void process(Node on, BiConsumer<Status.Known, Throwable> callback)
    {
        this.callback = callback;
        process(on);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void propagate(Node node, TxnId txnId, long sourceEpoch, WithQuorum withQuorum, Route route, @Nullable Status.Known target, CheckStatus.CheckStatusOkFull full, BiConsumer<Status.Known, Throwable> callback)
    {
        if (full.maxKnowledgeSaveStatus.status == NotDefined && full.maxInvalidIfNot() == NotKnownToBeInvalid)
        {
            callback.accept(Status.Known.Nothing, null);
            return;
        }

        Invariants.checkState(sourceEpoch == txnId.epoch() || (full.executeAt != null && sourceEpoch == full.executeAt.epoch()) || full.maxKnowledgeSaveStatus == SaveStatus.Erased);

        full = full.finish(route, withQuorum);
        route = Invariants.nonNull(full.route);

        // TODO (required): permit individual shards that are behind to catch up by themselves
        long toEpoch = sourceEpoch;
        Ranges sliceRanges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);

        RoutingKey progressKey = node.trySelectProgressKey(txnId, route);

        Ranges covering = route.sliceCovering(sliceRanges, Minimal);
        Participants<?> participatingKeys = route.participants().slice(covering, Minimal);
        Status.Known achieved = full.knownFor(participatingKeys);
        if (achieved.executeAt.isDecidedAndKnownToExecute() && full.executeAt.epoch() > toEpoch)
        {
            Ranges acceptRanges;
            if (!node.topology().hasEpoch(full.executeAt.epoch()) ||
                (!route.covers(acceptRanges = node.topology().localRangesForEpochs(txnId.epoch(), full.executeAt.epoch()))))
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
                covering = route.sliceCovering(sliceRanges, Minimal);
                participatingKeys = route.participants().slice(covering, Minimal);
                Status.Known knownForExecution = full.knownFor(participatingKeys);
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

        boolean isTruncated = withQuorum == HasQuorum && full.isTruncatedResponse(covering);

        PartialTxn partialTxn = full.partialTxn;
        if (achieved.definition.isKnown())
            partialTxn = full.partialTxn.slice(sliceRanges, true).reconstitutePartial(covering);

        PartialDeps committedDeps = full.committedDeps;
        if (achieved.deps.hasDecidedDeps())
            committedDeps = full.committedDeps.slice(sliceRanges).reconstitutePartial(covering);

        Propagate propagate =
            new Propagate(txnId, route, full.maxKnowledgeSaveStatus, full.maxSaveStatus, full.durability, full.homeKey, progressKey, achieved, full.map, isTruncated, partialTxn, committedDeps, toEpoch, full.executeAtIfKnown(), full.writes, full.result, callback);

        node.localRequest(propagate);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        // TODO (now): this is insufficient, as we may applyOrUpgradeTruncated
        // since we don't calculateDeps, we might not need to load CFK to register with it
        if (achieved.definition.isKnown())
            return partialTxn.keys();
        else if (achieved.deps.hasProposedOrDecidedDeps())
            return committedDeps.keyDeps.keys();
        else
            return Keys.EMPTY;
    }

    @Override
    public void preProcess(Node on, Node.Id from, ReplyContext replyContext)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void process(Node on, Node.Id from, ReplyContext replyContext)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void process(Node node)
    {
        node.mapReduceConsumeLocal(this, route, txnId.epoch(), toEpoch, this);
    }

    @Override
    public Void apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, this, route);
        Command command = safeCommand.current();

        PartialTxn partialTxn = achieved.hasDefinition() ? this.partialTxn : null;
        PartialDeps committedDeps = achieved.hasDecidedDeps() ? this.committedDeps : null;
        switch (command.saveStatus().phase)
        {
            // Already know the outcome, waiting on durability so maybe update with new durability information which can also trigger cleanup
            case Persist: return updateDurability(safeStore, safeCommand);
            case Cleanup: return null;
        }

        Timestamp executeAtIfKnown = command.executeAtIfKnown(committedExecuteAt);
        Status.Known achieved = this.achieved;
        if (isTruncated)
        {
            // TODO (required): do not markShardStale for reads; in general optimise handling of case where we cannot recover a known no-op transaction

            achieved = applyOrUpgradeTruncated(safeStore, safeCommand, command, executeAtIfKnown);
            if (achieved == null)
                return null;

            Ranges needed = safeStore.ranges().allBetween(txnId.epoch(), (executeAtIfKnown == null ? txnId : executeAtIfKnown).epoch());
            if (achieved.isDefinitionKnown() && partialTxn == null)
            {
                PartialTxn existing = command.partialTxn();
                Ranges neededForDefinition = existing == null ? needed : needed.subtract(existing.covering());
                partialTxn = this.partialTxn.slice(needed, true).reconstitutePartial(neededForDefinition);
            }

            if (achieved.hasDecidedDeps() && committedDeps == null)
            {
                Invariants.checkState(executeAtIfKnown != null);
                // we don't subtract existing partialDeps, as they cannot be committed deps; we only permit committing deps covering all participating ranges
                committedDeps = this.committedDeps.slice(needed).reconstitutePartial(needed);
            }
        }

        Status propagate = achieved.atLeast(command.known()).propagatesStatus();
        if (command.hasBeen(propagate))
        {
            if (maxSaveStatus.phase == Cleanup && durability.isDurableOrInvalidated() && Infer.safeToCleanup(safeStore, command, route, executeAtIfKnown))
                Commands.setTruncatedApply(safeStore, safeCommand);

            // TODO (expected): maybe stale?
            return updateDurability(safeStore, safeCommand);
        }

        switch (propagate)
        {
            default: throw new IllegalStateException("Unexpected status: " + propagate);
            case Truncated: throw new IllegalStateException("Status expected to be handled elsewhere: " + propagate);
            case Accepted:
            case AcceptedInvalidate:
                // we never "propagate" accepted statuses as these are essentially votes,
                // and contribute nothing to our local state machine
                throw new IllegalStateException("Invalid states to propagate: " + propagate);

            case Invalidated:
                Commands.commitInvalidate(safeStore, safeCommand, route);
                break;

            case Applied:
            case PreApplied:
                Invariants.checkState(executeAtIfKnown != null);
                if (toEpoch >= executeAtIfKnown.epoch())
                {
                    confirm(Commands.apply(safeStore, safeCommand, txnId, route, progressKey, executeAtIfKnown, committedDeps, partialTxn, writes, result));
                    break;
                }

            case Committed:
            case ReadyToExecute:
                confirm(Commands.commit(safeStore, safeCommand, txnId, route, progressKey, partialTxn, executeAtIfKnown, committedDeps));
                break;

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
        Invariants.checkState(!participants.isEmpty()); // we shouldn't be fetching data for transactions we only coordinate
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
            Commands.setTruncatedApply(safeStore, safeCommand, route);
            return null;
        }

        // if the command has been truncated globally, then we should expect to apply it
        // if we cannot obtain enough information from a majority to do so then we have been left behind
        Status.Known required = PreApplied.minKnown;
        Status.Known requireExtra = required.subtract(command.known()); // the extra information we need to reach pre-applied
        Ranges achieveRanges = known.knownFor(requireExtra, ranges); // the ranges for which we can successfully achieve this

        if (participants.isEmpty())
        {
            // we only coordinate this transaction, so being unable to retrieve its state does not imply any staleness
            // TODO (now): double check this doesn't stop us coordinating the transaction (it shouldn't, as doesn't imply durability)
            Commands.setTruncatedApply(safeStore, safeCommand, route);
            return null;
        }

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
        Commands.setTruncatedApply(safeStore, safeCommand, route);
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

        Commands.setDurability(safeStore, safeCommand, durability, route, committedExecuteAt);
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
        switch (achieved.propagatesStatus())
        {
            case Applied:
            case PreApplied:
                if (toEpoch >= committedExecuteAt.epoch())
                    return MessageType.PROPAGATE_APPLY_MSG;
            case Committed:
            case ReadyToExecute:
                return MessageType.PROPAGATE_COMMIT_MSG;
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
            default: throw new IllegalStateException("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }

    private static void confirm(Commands.ApplyOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }

    @Override
    public String toString()
    {
        return "Propagate{type:" + type() +
               ", txnId: " + txnId +
               ", saveStatus: " + maxKnowledgeSaveStatus +
               ", deps: " + committedDeps +
               ", txn: " + partialTxn +
               ", executeAt: " + committedExecuteAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }

}
