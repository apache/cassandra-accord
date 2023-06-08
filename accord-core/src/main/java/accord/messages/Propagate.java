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

import static accord.local.Status.NotDefined;
import static accord.local.Status.Phase.Cleanup;
import static accord.local.Status.PreApplied;
import static accord.primitives.Routables.Slice.Minimal;

public class Propagate implements MapReduceConsume<SafeCommandStore, Void>, EpochSupplier, LocalMessage
{
    public static class SerializerSupport
    {
        public static Propagate create(TxnId txnId, Route<?> route, SaveStatus saveStatus, SaveStatus maxSaveStatus, Status.Durability durability, RoutingKey homeKey, RoutingKey progressKey, Status.Known achieved, PartialTxn partialTxn, PartialDeps partialDeps, long toEpoch, Timestamp executeAt, Writes writes, Result result)
        {
            return new Propagate(txnId, route, saveStatus, maxSaveStatus, durability, homeKey, progressKey, achieved, partialTxn, partialDeps, toEpoch, executeAt, writes, result, null);
        }
    }

    public final TxnId txnId;
    public final Route<?> route;
    public final SaveStatus saveStatus;
    public final SaveStatus maxSaveStatus;
    public final Status.Durability durability;
    @Nullable public final RoutingKey homeKey;
    @Nullable public final RoutingKey progressKey;
    // this is a WHOLE NODE measure, so if commit epoch has more ranges we do not count as committed if we can only commit in coordination epoch
    public final Status.Known achieved;
    @Nullable public final PartialTxn partialTxn;
    @Nullable public final PartialDeps partialDeps;
    public final long toEpoch;
    @Nullable public final Timestamp executeAt;
    @Nullable public final Writes writes;
    @Nullable public final Result result;

    transient final BiConsumer<Status.Known, Throwable> callback;

    Propagate(
        TxnId txnId,
        Route<?> route,
        SaveStatus saveStatus,
        SaveStatus maxSaveStatus,
        Status.Durability durability,
        @Nullable RoutingKey homeKey,
        @Nullable RoutingKey progressKey,
        Status.Known achieved,
        @Nullable PartialTxn partialTxn,
        @Nullable PartialDeps partialDeps,
        long toEpoch,
        @Nullable Timestamp executeAt,
        @Nullable Writes writes,
        @Nullable Result result,
        BiConsumer<Status.Known, Throwable> callback)
    {
        this.txnId = txnId;
        this.route = route;
        this.saveStatus = saveStatus;
        this.maxSaveStatus = maxSaveStatus;
        this.durability = durability;
        this.homeKey = homeKey;
        this.progressKey = progressKey;
        this.achieved = achieved;
        this.partialTxn = partialTxn;
        this.partialDeps = partialDeps;
        this.toEpoch = toEpoch;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
        this.callback = callback;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static void propagate(Node node, TxnId txnId, long sourceEpoch, CheckStatus.WithQuorum withQuorum, Route route, @Nullable Status.Known target, CheckStatus.CheckStatusOkFull full, BiConsumer<Status.Known, Throwable> callback)
    {
        if (full.saveStatus.status == NotDefined && full.invalidIfNotAtLeast == NotDefined)
        {
            callback.accept(Status.Known.Nothing, null);
            return;
        }

        Invariants.checkState(sourceEpoch == txnId.epoch() || (full.executeAt != null && sourceEpoch == full.executeAt.epoch()));
        Route<?> maxRoute = Route.merge(route, full.route);

        // TODO (required): permit individual shards that are behind to catch up by themselves
        long toEpoch = sourceEpoch;
        Ranges sliceRanges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);
        if (!maxRoute.covers(sliceRanges))
        {
            callback.accept(Status.Known.Nothing, null);
            return;
        }

        RoutingKey progressKey = node.trySelectProgressKey(txnId, maxRoute);

        Ranges covering = maxRoute.sliceCovering(sliceRanges, Minimal);
        Participants<?> participatingKeys = maxRoute.participants().slice(covering, Minimal);
        Status.Known achieved = full.sufficientFor(participatingKeys, withQuorum);
        if (achieved.executeAt.hasDecidedExecuteAt() && full.executeAt.epoch() > toEpoch)
        {
            Ranges acceptRanges;
            if (!node.topology().hasEpoch(full.executeAt.epoch()) ||
                    (!maxRoute.covers(acceptRanges = node.topology().localRangesForEpochs(txnId.epoch(), full.executeAt.epoch()))))
            {
                // we don't know what the execution epoch requires, so we cannot be sure we can replicate it locally
                // we *could* wait until we have the local epoch before running this
                Status.Outcome outcome = achieved.outcome.propagatesBetweenShards() ? achieved.outcome : Status.Outcome.Unknown;
                achieved = new Status.Known(achieved.definition, achieved.executeAt, Status.KnownDeps.DepsUnknown, outcome);
            }
            else
            {
                // TODO (expected): this should only be the two precise epochs, not the full range of epochs
                sliceRanges = acceptRanges;
                covering = maxRoute.sliceCovering(sliceRanges, Minimal);
                participatingKeys = maxRoute.participants().slice(covering, Minimal);
                Status.Known knownForExecution = full.sufficientFor(participatingKeys, withQuorum);
                if ((target != null && target.isSatisfiedBy(knownForExecution)) || knownForExecution.isSatisfiedBy(achieved))
                {
                    achieved = knownForExecution;
                    toEpoch = full.executeAt.epoch();
                }
                else
                {
                    Invariants.checkState(sourceEpoch == txnId.epoch(), "%d != %d", sourceEpoch, txnId.epoch());
                    achieved = new Status.Known(achieved.definition, achieved.executeAt, knownForExecution.deps, knownForExecution.outcome);
                }
            }
        }

        PartialTxn partialTxn = null;
        if (achieved.definition.isKnown())
            partialTxn = full.partialTxn.slice(sliceRanges, true).reconstitutePartial(covering);

        PartialDeps partialDeps = null;
        if (achieved.deps.hasDecidedDeps())
            partialDeps = full.committedDeps.slice(sliceRanges).reconstitutePartial(covering);

        Propagate propagate =
            new Propagate(txnId, maxRoute, full.saveStatus, full.maxSaveStatus, full.durability, full.homeKey, progressKey, achieved, partialTxn, partialDeps, toEpoch, full.executeAt, full.writes, full.result, callback);

        node.localMessage(propagate);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        if (achieved.definition.isKnown())
            return partialTxn.keys();
        else if (achieved.deps.hasProposedOrDecidedDeps())
            return partialDeps.keyDeps.keys();
        else
            return Keys.EMPTY;
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
        if (command.saveStatus().phase.compareTo(Status.Phase.Persist) >= 0)
            return null;

        Status propagate = achieved.propagate();
        if (command.hasBeen(propagate))
        {
            if (maxSaveStatus.phase == Cleanup && durability.isDurableOrInvalidated() && Infer.safeToCleanup(safeStore, command, route, executeAt))
                Commands.setTruncatedApply(safeStore, safeCommand);
            return null;
        }

        switch (propagate)
        {
            default: throw new IllegalStateException("Unexpected status: " + propagate);
            case Accepted:
            case AcceptedInvalidate:
                // we never "propagate" accepted statuses as these are essentially votes,
                // and contribute nothing to our local state machine
                throw new IllegalStateException("Invalid states to propagate: " + achieved.propagate());

            case Truncated:
                // if our peers have truncated this command, then either:
                // 1) we have already applied it locally; 2) the command doesn't apply locally; 3) we are stale; or 4) the command is invalidated
                if (command.hasBeen(PreApplied) || command.saveStatus().isUninitialised())
                    break;

                if (Infer.safeToCleanup(safeStore, command, route, executeAt))
                {
                    Commands.setErased(safeStore, safeCommand);
                    break;
                }

                // TODO (required): check if we are stale
                // otherwise we are either stale, or the command didn't reach consensus

            case Invalidated:
                Commands.commitInvalidate(safeStore, safeCommand, route);
                break;

            case Applied:
            case PreApplied:
                Invariants.checkState(executeAt != null);
                if (toEpoch >= executeAt.epoch())
                {
                    confirm(Commands.apply(safeStore, safeCommand, txnId, route, progressKey, executeAt, partialDeps, partialTxn, writes, result));
                    break;
                }

            case Committed:
            case ReadyToExecute:
                confirm(Commands.commit(safeStore, safeCommand, txnId, route, progressKey, partialTxn, executeAt, partialDeps));
                break;

            case PreCommitted:
                Commands.precommit(safeStore, safeCommand, txnId, executeAt, route);
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


        if (!durability.isDurable() || homeKey == null)
            return null;

        if (!safeStore.ranges().coordinates(txnId).contains(homeKey))
            return null;

        Timestamp executeAt = saveStatus.known.executeAt.hasDecidedExecuteAt() ? this.executeAt : null;
        Commands.setDurability(safeStore, safeCommand, durability, route, executeAt);
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
        switch (achieved.propagate())
        {
            case Applied:
            case PreApplied:
                if (toEpoch >= executeAt.epoch())
                    return MessageType.PROPAGATE_APPLY_MSG;
            case Committed:
            case ReadyToExecute:
                return MessageType.PROPAGATE_COMMIT_MSG;
            case PreCommitted:
                if (!achieved.definition.isKnown())
                    return MessageType.PROPAGATE_OTHER_MSG;
            case PreAccepted:
                if (Route.isFullRoute(route))
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
}
