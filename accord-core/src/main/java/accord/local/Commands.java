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

import java.util.Collection;
import java.util.Collections;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.Infer;
import accord.local.Command.WaitingOn;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasDecidedExecuteAt;
import static accord.local.Cleanup.ERASE;
import static accord.local.Cleanup.shouldCleanup;
import static accord.local.Command.Truncated.erased;
import static accord.local.Command.Truncated.invalidated;
import static accord.local.Command.Truncated.truncatedApply;
import static accord.local.Command.Truncated.truncatedApplyWithOutcome;
import static accord.local.KeyHistory.TIMESTAMPS;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.RedundantStatus.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.SaveStatus.Applying;
import static accord.local.SaveStatus.Erased;
import static accord.local.SaveStatus.TruncatedApply;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Durability;
import static accord.local.Status.Invalidated;
import static accord.local.Status.KnownExecuteAt.ExecuteAtKnown;
import static accord.local.Status.KnownRoute.Full;
import static accord.local.Status.NotDefined;
import static accord.local.Status.PreApplied;
import static accord.local.Status.PreCommitted;
import static accord.local.Status.Stable;
import static accord.local.Status.Truncated;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Route.isFullRoute;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Invariants.illegalState;

public class Commands
{
    private static final Logger logger = LoggerFactory.getLogger(Commands.class);

    private Commands()
    {
    }

    public enum AcceptOutcome { Success, Redundant, RejectedBallot, Truncated }

    public static AcceptOutcome preaccept(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, long acceptEpoch, PartialTxn partialTxn, FullRoute<?> route)
    {
        return preacceptOrRecover(safeStore, safeCommand, txnId, acceptEpoch, partialTxn, route, Ballot.ZERO);
    }

    public static AcceptOutcome recover(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, PartialTxn partialTxn, FullRoute<?> route, Ballot ballot)
    {
        // for recovery we only ever propose either the original epoch or an Accept that we witness; otherwise we invalidate
        return preacceptOrRecover(safeStore, safeCommand, txnId, txnId.epoch(), partialTxn, route, ballot);
    }

    private static AcceptOutcome preacceptOrRecover(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, long acceptEpoch, PartialTxn partialTxn, FullRoute<?> route, Ballot ballot)
    {
        final Command command = safeCommand.current();

        if (command.hasBeen(Truncated))
        {
            logger.trace("{}: skipping preaccept - command is truncated", txnId);
            return command.is(Invalidated) ? AcceptOutcome.RejectedBallot : AcceptOutcome.Truncated;
        }

        int compareBallots = command.promised().compareTo(ballot);
        if (compareBallots > 0)
        {
            logger.trace("{}: skipping preaccept - higher ballot witnessed ({})", txnId, command.promised());
            return AcceptOutcome.RejectedBallot;
        }

        if (command.known().definition.isKnown())
        {
            Invariants.checkState(command.status() == Invalidated || command.executeAt() != null);
            logger.trace("{}: skipping preaccept - already known ({})", txnId, command.status());
            // in case of Ballot.ZERO, we must either have a competing recovery coordinator or have late delivery of the
            // preaccept; in the former case we should abandon coordination, and in the latter we have already completed
            safeCommand.updatePromised(ballot);
            return ballot.equals(Ballot.ZERO) ? AcceptOutcome.Redundant : AcceptOutcome.Success;
        }

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Ranges acceptRanges = acceptRanges(safeStore, txnId, acceptEpoch, coordinateRanges);
        Invariants.checkState(!acceptRanges.isEmpty());
        Invariants.checkState(validate(SaveStatus.PreAccepted, command, acceptRanges, route, partialTxn, null, null));

        // FIXME: this should go into a consumer method
        CommonAttributes attrs = set(SaveStatus.PreAccepted, command, command, acceptRanges, ballot, route, partialTxn, null);
        if (command.executeAt() == null)
        {
            // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
            //  - use a global logical clock to issue new timestamps; or
            //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
            // if we are performing recovery (i.e. non-zero ballot), do not permit a fast path decision as we want to
            // invalidate any transactions that were not completed by their initial coordinator
            // TODO (desired): limit preaccept to keys we include, to avoid inflating unnecessary state
            Timestamp executeAt = safeStore.commandStore().preaccept(txnId, attrs.partialTxn().keys(), safeStore, ballot.equals(Ballot.ZERO));
            safeCommand.preaccept(safeStore, attrs, executeAt, ballot);
        }
        else
        {
            // TODO (expected, ?): in the case that we are pre-committed but had not been preaccepted/accepted, should we inform progressLog?
            safeCommand.markDefined(safeStore, attrs, ballot);
        }

        safeStore.notifyListeners(safeCommand, command);
        return AcceptOutcome.Success;
    }

    public static boolean preacceptInvalidate(SafeCommand safeCommand, Ballot ballot)
    {
        Command command = safeCommand.current();

        if (command.hasBeen(Status.Committed))
        {
            if (command.is(Truncated)) logger.trace("{}: skipping preacceptInvalidate - already truncated", command.txnId());
            else if (command.is(Invalidated)) logger.trace("{}: skipping preacceptInvalidate - already invalidated", command.txnId());
            else logger.trace("{}: skipping preacceptInvalidate - already committed", command.txnId());
            return false;
        }

        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping preacceptInvalidate - witnessed higher ballot ({})", command.txnId(), command.promised());
            return false;
        }
        safeCommand.updatePromised(ballot);
        return true;
    }

    public static AcceptOutcome accept(SafeCommandStore safeStore, TxnId txnId, Ballot ballot, Route<?> route, Seekables<?, ?> keysOrRanges, Timestamp executeAt, PartialDeps partialDeps)
    {
        SafeCommand safeCommand = safeStore.get(txnId, executeAt, route);
        final Command command = safeCommand.current();
        if (command.hasBeen(PreCommitted))
        {
            if (command.is(Invalidated))
            {
                logger.trace("{}: skipping accept - command is invalidated", txnId);
                return AcceptOutcome.RejectedBallot;
            }

            if (command.is(Truncated))
            {
                logger.trace("{}: skipping accept - command is truncated", txnId);
                return AcceptOutcome.Truncated;
            }

            logger.trace("{}: skipping accept - already committed ({})", txnId, command.status());
            return AcceptOutcome.Redundant;
        }

        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept - witnessed higher ballot ({} > {})", txnId, command.promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Ranges acceptRanges = acceptRanges(safeStore, txnId, executeAt, coordinateRanges);
        Invariants.checkState(!acceptRanges.isEmpty());

        Invariants.checkState(validate(SaveStatus.Accepted, command, acceptRanges, route, null, partialDeps, null));

        // TODO (desired, clarity/efficiency): we don't need to set the route here, and perhaps we don't even need to
        //  distributed partialDeps at all, since all we gain is not waiting for these transactions to commit during
        //  recovery. We probably don't want to directly persist a Route in any other circumstances, either, to ease persistence.
        CommonAttributes attrs = set(SaveStatus.Accepted, command, command, acceptRanges, ballot, route, null, partialDeps);

        keysOrRanges = keysOrRanges.slice(acceptRanges);
        safeCommand.accept(safeStore, keysOrRanges, attrs, executeAt, ballot);
        safeStore.notifyListeners(safeCommand, command);

        return AcceptOutcome.Success;
    }

    public static AcceptOutcome acceptInvalidate(SafeCommandStore safeStore, SafeCommand safeCommand, Ballot ballot)
    {
        // TODO (expected): save some partial route we can use to determine if it can be GC'd
        final Command command = safeCommand.current();
        if (command.hasBeen(PreCommitted))
        {
            if (command.is(Invalidated))
            {
                logger.trace("{}: skipping accept invalidated - already invalidated ({})", command.txnId(), command.status());
                return AcceptOutcome.Redundant;
            }

            if (command.is(Truncated))
            {
                logger.trace("{}: skipping accept invalidated - already truncated ({})", command.txnId(), command.status());
                return AcceptOutcome.Truncated;
            }

            logger.trace("{}: skipping accept invalidated - already committed ({})", command.txnId(), command.status());
            return AcceptOutcome.RejectedBallot;
        }

        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept invalidated - witnessed higher ballot ({} > {})", command.txnId(), command.promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        logger.trace("{}: accepted invalidated", command.txnId());

        safeCommand.acceptInvalidated(safeStore, ballot);
        safeStore.notifyListeners(safeCommand, command);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Rejected, Redundant, Insufficient }


    // relies on mutual exclusion for each key
    public static CommitOutcome commit(SafeCommandStore safeStore, SafeCommand safeCommand, SaveStatus newStatus, Ballot ballot, TxnId txnId, Route<?> route, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        final Command command = safeCommand.current();
        SaveStatus curStatus = command.saveStatus();

        Invariants.checkArgument(newStatus == SaveStatus.Committed || newStatus == SaveStatus.Stable);
        if (newStatus == SaveStatus.Committed && ballot.compareTo(command.promised()) < 0)
            return CommitOutcome.Rejected;

        if (curStatus.hasBeen(PreCommitted))
        {
            if (!curStatus.is(Truncated))
            {
                if (!executeAt.equals(command.executeAt()) || curStatus.status == Invalidated)
                    safeStore.agent().onInconsistentTimestamp(command, (curStatus.status == Invalidated ? Timestamp.NONE : command.executeAt()), executeAt);
            }

            if (curStatus.compareTo(newStatus) > 0 || curStatus.hasBeen(Stable))
            {
                logger.trace("{}: skipping commit - already newer or stable ({})", txnId, command.status());
                return CommitOutcome.Redundant;
            }

            if (curStatus == SaveStatus.Committed && newStatus == SaveStatus.Committed)
            {
                if (ballot.equals(command.acceptedOrCommitted()))
                    return CommitOutcome.Redundant;

                Invariants.checkState(ballot.compareTo(command.acceptedOrCommitted()) > 0);
            }
        }

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Ranges acceptRanges = acceptRanges(safeStore, txnId, executeAt, coordinateRanges);

        if (!validate(newStatus, command, acceptRanges, route, partialTxn, partialDeps, null))
            return CommitOutcome.Insufficient;

        CommonAttributes attrs = set(newStatus, command, command, acceptRanges, ballot, route, partialTxn, partialDeps);

        logger.trace("{}: committed with executeAt: {}, deps: {}", txnId, executeAt, partialDeps);
        final Command.Committed committed;
        if (newStatus == SaveStatus.Stable)
        {
            WaitingOn waitingOn = initialiseWaitingOn(safeStore, txnId, attrs, executeAt, attrs.route());
            committed = safeCommand.stable(safeStore, attrs, Ballot.max(command.acceptedOrCommitted(), ballot), executeAt, waitingOn);
            safeStore.agent().metricsEventsListener().onStable(committed);
            // TODO (expected, safety): introduce intermediate status to avoid reentry when notifying listeners (which might notify us)
            maybeExecute(safeStore, safeCommand, true, true);
        }
        else
        {
            Invariants.checkArgument(command.acceptedOrCommitted().compareTo(ballot) <= 0);
            committed = safeCommand.commit(safeStore, attrs, ballot, executeAt);
            safeStore.notifyListeners(safeCommand, committed);
            safeStore.agent().metricsEventsListener().onCommitted(committed);
        }

        return CommitOutcome.Success;
    }

    // relies on mutual exclusion for each key
    public static CommitOutcome precommit(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Timestamp executeAt, Route<?> route)
    {
        Invariants.checkState(Route.isFullRoute(route));
        final Command command = safeCommand.current();
        if (command.hasBeen(PreCommitted))
        {
            if (command.is(Truncated))
            {
                logger.trace("{}: skipping commit - already truncated ({})", txnId, command.status());
                return CommitOutcome.Redundant;
            }
            else
            {
                logger.trace("{}: skipping precommit - already committed ({})", txnId, command.status());
                if (executeAt.equals(command.executeAt()) && command.status() != Invalidated)
                    return CommitOutcome.Redundant;

                safeStore.agent().onInconsistentTimestamp(command, (command.status() == Invalidated ? Timestamp.NONE : command.executeAt()), executeAt);
            }
        }

        CommonAttributes attrs = command;
        if (command.route() == null || !Route.isFullRoute(command.route()))
            attrs = updateRoute(command, route);

        safeCommand.precommit(safeStore, attrs, executeAt);
        safeStore.notifyListeners(safeCommand, command);
        logger.trace("{}: precommitted with executeAt: {}", txnId, executeAt);
        return CommitOutcome.Success;
    }

    public static void createBootstrapCompleteMarkerTransaction(SafeCommandStore safeStore, TxnId localSyncId, Seekables<?, ?> keys)
    {
        SafeCommand safeCommand = safeStore.get(localSyncId);
        final Command command = safeCommand.current();
        Invariants.checkState(!command.hasBeen(Committed));
        FullRoute<?> route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));

        Ranges coordinateRanges = coordinateRanges(safeStore, localSyncId);
        // TODO (desired, consider): in the case of sync points, the coordinator is unlikely to be a home shard, do we mind this? should document at least
        Txn emptyTxn = safeStore.agent().emptySystemTxn(localSyncId.kind(), keys);
        PartialDeps none = Deps.NONE.intersecting(route);
        PartialTxn partialTxn = emptyTxn.slice(coordinateRanges, true);
        Invariants.checkState(validate(SaveStatus.Stable, command, coordinateRanges, route, partialTxn, none, null));
        CommonAttributes newAttributes = set(SaveStatus.Stable, command, command, coordinateRanges, Ballot.ZERO, route, partialTxn, none);
        safeCommand.stable(safeStore, newAttributes, Ballot.ZERO, localSyncId, WaitingOn.empty(emptyTxn.keys().domain()));
        safeStore.notifyListeners(safeCommand, command);
    }

    public static void ephemeralRead(SafeCommandStore safeStore, SafeCommand safeCommand, Route<?> route, TxnId txnId, PartialTxn partialTxn, PartialDeps partialDeps, long executeAtEpoch)
    {
        // TODO (expected): introduce in-memory only commands
        Command command = safeCommand.current();
        if (command.hasBeen(Stable))
            return;

        // TODO (required): by creating synthetic TxnId in future epochs we may not be evictable
        //   but for ephemeral reads we want parallel eviction - or preferably no durability - anyway
        txnId = txnId.withEpoch(executeAtEpoch);

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Invariants.checkState(validate(SaveStatus.Stable, command, coordinateRanges, route, partialTxn, partialDeps, null));
        CommonAttributes attrs = set(SaveStatus.Stable, command, command, coordinateRanges, Ballot.ZERO, route, partialTxn, partialDeps);
        safeCommand.stable(safeStore, attrs, Ballot.ZERO, txnId, initialiseWaitingOn(safeStore, txnId, attrs, txnId, route));
        maybeExecute(safeStore, safeCommand, false, true);
    }

    public static void markBootstrapComplete(SafeCommandStore safeStore, TxnId localSyncId, Seekables<?, ?> keys)
    {
        SafeCommand safeCommand = safeStore.get(localSyncId);
        Command.Committed command = safeCommand.current().asCommitted();
        if (command.hasBeen(PreApplied))
            return;

        // NOTE: if this is ever made a non-empty txn this will introduce a potential bug where the txn is registered against CommandsForKeys
        Txn emptyTxn = safeStore.agent().emptySystemTxn(localSyncId.kind(), keys);
        safeCommand.preapplied(safeStore, command, command.executeAt(), command.waitingOn(), emptyTxn.execute(localSyncId, localSyncId, null), emptyTxn.result(localSyncId, localSyncId, null));
        maybeExecute(safeStore, safeCommand, true, false);
    }

    // TODO (expected, ?): commitInvalidate may need to update cfks _if_ possible
    public static void commitInvalidate(SafeCommandStore safeStore, SafeCommand safeCommand, Unseekables<?> scope)
    {
        final Command command = safeCommand.current();
        if (command.hasBeen(PreCommitted))
        {
            if (command.is(Truncated))
            {
                logger.trace("{}: skipping commit invalidated - already truncated ({})", safeCommand.txnId(), command.status());
            }
            else
            {
                logger.trace("{}: skipping commit invalidated - already committed ({})", safeCommand.txnId(), command.status());
                if (!command.is(Invalidated) && !(command.is(Truncated) && command.executeAt().equals(Timestamp.NONE)))
                    safeStore.agent().onInconsistentTimestamp(command, Timestamp.NONE, command.executeAt());
            }
            return;
        }
        else if (command.saveStatus().isUninitialised() && !safeStore.ranges().allAt(command.txnId().epoch()).intersects(scope))
            return; // don't bother propagating the invalidation to future epochs where the replica didn't already witness the command

        safeCommand.commitInvalidated(safeStore);
        safeStore.progressLog().clear(command.txnId());
        logger.trace("{}: committed invalidated", safeCommand.txnId());
        safeStore.notifyListeners(safeCommand, command);
    }

    public enum ApplyOutcome { Success, Redundant, Insufficient }

    public static ApplyOutcome apply(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Route<?> route, Timestamp executeAt, @Nullable PartialDeps partialDeps, @Nullable PartialTxn partialTxn, Writes writes, Result result)
    {
        Command command = safeCommand.current();
        if (command.hasBeen(PreApplied) && executeAt.equals(command.executeAt()))
        {
            logger.trace("{}: skipping apply - already executed ({})", txnId, command.status());
            return ApplyOutcome.Redundant;
        }
        else if (command.hasBeen(PreCommitted) && !executeAt.equals(command.executeAt()))
        {
            if (command.is(Truncated) && command.executeAt() == null)
                return ApplyOutcome.Redundant;
            safeStore.agent().onInconsistentTimestamp(command, command.executeAt(), executeAt);
        }

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Ranges acceptRanges = acceptRanges(safeStore, txnId, executeAt, coordinateRanges);

        if (!validate(SaveStatus.PreApplied, command, acceptRanges, route, partialTxn, partialDeps, safeStore))
            return ApplyOutcome.Insufficient; // TODO (expected, consider): this should probably be an assertion failure if !TrySet

        CommonAttributes attrs = set(SaveStatus.PreApplied, command, command, acceptRanges, null, route, partialTxn, partialDeps);

        WaitingOn waitingOn = !command.hasBeen(Stable) ? initialiseWaitingOn(safeStore, txnId, attrs, executeAt, attrs.route()) : command.asCommitted().waitingOn();

        safeCommand.preapplied(safeStore, attrs, executeAt, waitingOn, writes, result);
        logger.trace("{}: apply, status set to Executed with executeAt: {}, deps: {}", txnId, executeAt, partialDeps);

        // must signal preapplied first, else we may be applied (and have cleared progress log state) already before maybeExecute exits
        maybeExecute(safeStore, safeCommand, true, true);
        safeStore.agent().metricsEventsListener().onExecuted(command);

        return ApplyOutcome.Success;
    }

    public static void listenerUpdate(SafeCommandStore safeStore, SafeCommand safeListener, SafeCommand safeUpdated)
    {
        Command listener = safeListener.current();
        Command updated = safeUpdated.current();
        if (listener.is(NotDefined) || listener.is(Truncated))
        {
            // This listener must be a stale vestige
            // TODO (desired): would be nice to ensure these are deregistered explicitly, but would be costly
            Invariants.checkState(listener.saveStatus().isUninitialised() || listener.is(Truncated), "Listener status expected to be Uninitialised or Truncated, but was %s", listener.saveStatus());
            Invariants.checkState(updated.is(NotDefined) || updated.hasBeen(Truncated) || !updated.asCommitted().waitingOn().isWaitingOn(listener.txnId()), "Updated status expected to be Applied or NotDefined, but was %s", updated);
            return;
        }

        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     listener.txnId(), updated.txnId(), updated.status(), updated);
        switch (updated.status())
        {
            default:
                throw illegalState("Unexpected status: " + updated.status());
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                break;

            case PreCommitted:
            case Committed:
            case Stable:
            case PreApplied:
            case Applied:
            case Invalidated:
            case Truncated:
                updateDependencyAndMaybeExecute(safeStore, safeListener, safeUpdated, true);
                break;
        }
    }

    protected static void postApply(SafeCommandStore safeStore, TxnId txnId)
    {
        logger.trace("{} applied, setting status to Applied and notifying listeners", txnId);
        SafeCommand safeCommand = safeStore.get(txnId);
        final Command original = safeCommand.current();
        safeCommand.applied(safeStore);
        safeStore.notifyListeners(safeCommand, original);
    }

    /**
     * The ranges for which we participate in the consensus decision of when a transaction executes
     */
    private static Ranges coordinateRanges(SafeCommandStore safeStore, TxnId txnId)
    {
        return safeStore.ranges().coordinates(txnId);
    }

    private static Ranges acceptRanges(SafeCommandStore safeStore, TxnId txnId, long untilEpoch, Ranges coordinateRanges)
    {
        return safeStore.ranges().extend(coordinateRanges, txnId.epoch(), untilEpoch);
    }

    private static Ranges acceptRanges(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, Ranges coordinateRanges)
    {
        return safeStore.ranges().extend(coordinateRanges, txnId, executeAt);
    }

    private static Ranges executeRanges(SafeCommandStore safeStore, Timestamp executeAt)
    {
        return safeStore.ranges().allAt(executeAt.epoch());
    }

    /**
     * The ranges for which we participate in the execution of a transaction, excluding those ranges
     * for transactions below a SyncPoint where we adopted the range, and that will be obtained from peers,
     * and therefore we do not want to execute locally
     */
    private static Ranges applyRanges(SafeCommandStore safeStore, Timestamp executeAt)
    {
        return safeStore.ranges().applyRanges(executeAt);
    }

    public static AsyncChain<Void> applyChain(SafeCommandStore safeStore, PreLoadContext context, TxnId txnId)
    {
        Command.Executed command = safeStore.get(txnId).current().asExecuted();
        if (command.hasBeen(Applied))
        {
            return AsyncChains.success(null);
        }
        return apply(safeStore, context, txnId);
    }

    public static AsyncChain<Void> apply(SafeCommandStore safeStore, PreLoadContext context, TxnId txnId)
    {
        Command.Executed command = safeStore.get(txnId).current().asExecuted();
        // TODO (required): make sure we are correctly handling (esp. C* side with validation logic) executing a transaction
        //  that was pre-bootstrap for some range (so redundant and we may have gone ahead of), but had to be executed locally
        //  for another range
        CommandStore unsafeStore = safeStore.commandStore();
        // TODO: avoid allocating a timestamp here
        long t0 = safeStore.time().now();
        return command.writes().apply(safeStore, applyRanges(safeStore, command.executeAt()), command.partialTxn())
               .flatMap(unused -> unsafeStore.submit(context, ss -> {
                   Command cmd = ss.get(txnId).current();
                   if (!cmd.hasBeen(Applied))
                       ss.agent().metricsEventsListener().onApplied(cmd, t0);
                   postApply(ss, txnId);
                   return null;
               }));
    }

    public static AsyncChain<Void> applyFromReplay(SafeCommandStore safeStore, PreLoadContext context, TxnId txnId)
    {
        Command.Executed command = safeStore.get(txnId).current().asExecuted();
        // TODO (required): make sure we are correctly handling (esp. C* side with validation logic) executing a transaction
        //  that was pre-bootstrap for some range (so redundant and we may have gone ahead of), but had to be executed locally
        //  for another range
        CommandStore unsafeStore = safeStore.commandStore();
        long t0 = safeStore.time().now();
        return command.writes().apply(safeStore, applyRanges(safeStore, command.executeAt()), command.partialTxn())
                      .flatMap(unused -> unsafeStore.submit(context, ss -> {
                          Command cmd = ss.get(txnId).current();
                          if (!cmd.hasBeen(Applied))
                              ss.agent().metricsEventsListener().onApplied(cmd, t0);
                          postApply(ss, txnId);
                          return null;
                      }));
    }

    public static void apply(SafeCommandStore safeStore, Command command)
    {
        CommandStore unsafeStore = safeStore.commandStore();
        TxnId txnId = command.txnId();
        // TODO (expected): there is some coupling going on here - concept of TIMESTAMPS only needed if implementation tracks on apply
        PreLoadContext context = contextFor(command.txnId(), command.partialTxn().keys(), TIMESTAMPS);
        // this is sometimes called from a listener update, which will not have the keys in context
        if (safeStore.canExecuteWith(context))
        {
            applyChain(safeStore, context, txnId).begin(safeStore.agent());
        }
        else
        {
            unsafeStore.submit(context, ss -> {
                applyChain(ss, context, txnId).begin(ss.agent());
                return null;
            }).begin(safeStore.agent());
        }
    }

    public static boolean maybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        final Command command = safeCommand.current();
        if (logger.isTraceEnabled())
            logger.trace("{}: Maybe executing with status {}. Will notify listeners on noop: {}", command.txnId(), command.status(), alwaysNotifyListeners);

        if (command.status() != Stable && command.status() != PreApplied)
        {
            if (alwaysNotifyListeners)
                safeStore.notifyListeners(safeCommand, command);
            return false;
        }

        WaitingOn waitingOn = command.asCommitted().waitingOn();
        if (waitingOn.isWaiting())
        {
            if (alwaysNotifyListeners)
                safeStore.notifyListeners(safeCommand, command);

            if (notifyWaitingOn && waitingOn.isWaitingOnCommand())
                new NotifyWaitingOn(safeCommand).accept(safeStore);
            return false;
        }

        // TODO (required): slice our execute ranges based on any pre-bootstrap state
        // FIXME: need to communicate to caller that we didn't execute if we take one of the above paths
        switch (command.status())
        {
            case Stable:
                // TODO (desirable, efficiency): maintain distinct ReadyToRead and ReadyToWrite states
                // TODO (required): we can have dangling transactions in some cases when proposing in a future epoch but
                //   later deciding on an earlier epoch. We should probably turn this into an erased vestigial command,
                //   but we should tighten up our semantics there in general.
                safeCommand.readyToExecute(safeStore);
                logger.trace("{}: set to ReadyToExecute", command.txnId());
                safeStore.notifyListeners(safeCommand, command);
                return true;

            case PreApplied:
                Ranges executeRanges = executeRanges(safeStore, command.executeAt());
                Command.Executed executed = command.asExecuted();
                boolean intersects = executed.writes().keys.intersects(executeRanges);

                if (intersects)
                {
                    // TODO (now): we should set applying within apply to avoid applying multiple times
                    safeCommand.applying(safeStore);
                    safeStore.notifyListeners(safeCommand, command);
                    logger.trace("{}: applying", command.txnId());
                    apply(safeStore, executed);
                    return true;
                }
                else
                {
                    // TODO (desirable, performance): This could be performed immediately upon Committed
                    //      but: if we later support transitive dependency elision this could be dangerous
                    logger.trace("{}: applying no-op", command.txnId());
                    safeCommand.applied(safeStore);
                    if (command.txnId().kind() == ExclusiveSyncPoint)
                    {
                        Ranges ranges = safeStore.ranges().allAt(command.txnId().epoch());
                        ranges = command.route().slice(ranges, Minimal).participants().toRanges();
                        safeStore.commandStore().markExclusiveSyncPointLocallyApplied(safeStore, command.txnId(), ranges);
                    }
                    safeStore.notifyListeners(safeCommand, command);
                    return true;
                }
            default:
                throw illegalState("Unexpected status: " + command.status());
        }
    }

    protected static WaitingOn initialiseWaitingOn(SafeCommandStore safeStore, TxnId waitingId, CommonAttributes waiting, Timestamp waitingExecuteAt, Route<?> route)
    {
        if (waitingId.kind().awaitsOnlyDeps())
            waitingExecuteAt = Timestamp.maxForEpoch(waitingId.epoch());

        Ranges ranges = safeStore.ranges().allAt(waitingExecuteAt);
        PartialDeps deps = waiting.partialDeps();

        WaitingOn.Update update = WaitingOn.Update.initialise(waitingId, route, ranges, deps);
        return updateWaitingOn(safeStore, waiting, waitingExecuteAt, update, route.participants()).build();
    }

    protected static WaitingOn.Update updateWaitingOn(SafeCommandStore safeStore, CommonAttributes waiting, Timestamp executeAt, WaitingOn.Update update, Participants<?> participants)
    {
        CommandStore commandStore = safeStore.commandStore();
        TxnId minWaitingOnTxnId = update.minWaitingOnTxnId();
        if (minWaitingOnTxnId != null && commandStore.hasLocallyRedundantDependencies(update.minWaitingOnTxnId(), executeAt, participants))
            safeStore.commandStore().removeRedundantDependencies(participants, update);

        update.forEachWaitingOnId(safeStore, update, waiting, executeAt, (store, upd, w, exec, i) -> {
            SafeCommand dep = store.ifLoadedAndInitialised(upd.txnId(i));
            if (dep == null || !dep.current().hasBeen(PreCommitted))
                return;
            updateWaitingOn(store, w, exec, upd, dep);
        });

        return update;
    }

    /**
     * @param dependencySafeCommand is either committed truncated, or invalidated
     * @return true iff {@code maybeExecute} might now have a different outcome
     */
    private static boolean updateWaitingOn(SafeCommandStore safeStore, CommonAttributes waiting, Timestamp waitingExecuteAt, WaitingOn.Update waitingOn, SafeCommand dependencySafeCommand)
    {
        TxnId waitingId = waiting.txnId();
        Command dependency = dependencySafeCommand.current();
        Invariants.checkState(dependency.hasBeen(PreCommitted));
        TxnId dependencyId = dependency.txnId();
        if (waitingId.kind().awaitsOnlyDeps() && dependency.known().executeAt == ExecuteAtKnown && dependency.executeAt().compareTo(waitingId) > 0)
            waitingOn.updateExecuteAtLeast(dependency.executeAt());

        if (dependency.hasBeen(Truncated))
        {
            switch (dependency.saveStatus())
            {
                default: throw new AssertionError("Unhandled saveStatus: " + dependency.saveStatus());
                case TruncatedApply:
                case TruncatedApplyWithOutcome:
                case TruncatedApplyWithDeps:
                    Invariants.checkState(dependency.executeAt().compareTo(waitingExecuteAt) < 0 || waitingId.kind().awaitsOnlyDeps() || !dependency.txnId().kind().witnesses(waitingId));
                case ErasedOrInvalidOrVestigial:
                case Erased:
                    logger.trace("{}: {} is truncated. Stop listening and removing from waiting on commit set.", waitingId, dependencyId);
                    break;
                case Invalidated:
                    logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", waitingId, dependencyId);
            }
            return waitingOn.setAppliedOrInvalidated(dependencyId);
        }
        else if (dependency.executeAt().compareTo(waitingExecuteAt) > 0 && !waitingId.kind().awaitsOnlyDeps())
        {
            // dependency cannot be a predecessor if it executes later
            logger.trace("{}: {} executes after us. Removing from waiting on apply set.", waitingId, dependencyId);
            return waitingOn.removeWaitingOn(dependencyId);
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Removing from waiting on apply set.", waitingId, dependencyId);
            return waitingOn.setAppliedAndPropagate(dependencyId, dependency.asCommitted().waitingOn());
        }
        else if (waitingOn.isWaitingOn(dependencyId))
        {
            safeStore.registerListener(dependencySafeCommand, SaveStatus.Applied, Full, waitingId);
            return false;
        }
        else if (safeStore.isFullyPreBootstrapOrStale(dependency, waiting.partialDeps().participants(dependency.txnId())))
        {
            // TODO (expected): erase the dependency or otherwise prevent from executing
            return false;
        }
        else
        {
            throw illegalState("We have a dependency to wait on, but have already finished waiting");
        }
    }

    static void updateDependencyAndMaybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, SafeCommand predecessor, boolean notifyWaitingOn)
    {
        Command.Committed command = safeCommand.current().asCommitted();
        if (command.hasBeen(Applied))
            return;

        WaitingOn.Update waitingOn = new WaitingOn.Update(command);
        if (updateWaitingOn(safeStore, command, command.executeAt(), waitingOn, predecessor))
        {
            safeCommand.updateWaitingOn(waitingOn);
            // don't bother invoking maybeExecute if we weren't already blocked on the updated command
            if (waitingOn.hasUpdatedDirectDependency(command.waitingOn()))
                maybeExecute(safeStore, safeCommand, false, notifyWaitingOn);
            else Invariants.checkState(waitingOn.isWaiting());
        }
        else
        {
            Command pred = predecessor.current();
            if (pred.hasBeen(PreCommitted))
            {
                TxnId nextWaitingOn = command.waitingOn().nextWaitingOn();
                if (nextWaitingOn != null && nextWaitingOn.equals(pred.txnId()) && !pred.hasBeen(PreApplied))
                    safeStore.progressLog().waiting(CanApply, safeStore, predecessor, pred.route(), null);
            }
        }
    }

    public static void removeWaitingOnKeyAndMaybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, Key key)
    {
        if (safeCommand.current().hasBeen(Applied))
            return;

        Command.Committed command = safeCommand.current().asCommitted();

        WaitingOn currentWaitingOn = command.waitingOn;
        int keyIndex = currentWaitingOn.keys.indexOf(key);
        if (keyIndex < 0 || !currentWaitingOn.isWaitingOnKey(keyIndex))
            return;

        WaitingOn.Update waitingOn = new WaitingOn.Update(command);
        waitingOn.removeWaitingOnKey(keyIndex);
        safeCommand.updateWaitingOn(waitingOn);
        if (!waitingOn.isWaiting())
            maybeExecute(safeStore, safeCommand, false, true);
    }

    // TODO (now): document and justify all calls
    public static void setTruncatedApplyOrErasedVestigial(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        setTruncatedApplyOrErasedVestigial(safeStore, safeCommand, null, null);
    }

    public static void setTruncatedApplyOrErasedVestigial(SafeCommandStore safeStore, SafeCommand safeCommand, @Nullable Timestamp executeAt, Route<?> maybeFullRoute)
    {
        Command command = safeCommand.current();
        if (command.saveStatus().compareTo(TruncatedApply) >= 0) return;
        FullRoute<?> route = Route.tryCastToFullRoute(maybeFullRoute);
        if (route == null) route = Route.tryCastToFullRoute(command.route());
        if (executeAt == null) executeAt = command.executeAtIfKnown();
        if (route == null || executeAt == null)
        {
            safeCommand.update(safeStore, Command.Truncated.erasedOrInvalidOrVestigial(command));
            if (route != null && !safeStore.ranges().allAt(command.txnId()).contains(route.homeKey()))
                safeStore.progressLog().clear(command.txnId());
        }
        else
        {
            CommonAttributes attributes = command.mutable().route(route);
            if (!safeCommand.txnId().kind().awaitsOnlyDeps())
            {
                safeCommand.update(safeStore, truncatedApply(attributes, TruncatedApply, executeAt, null, null));
            }
            else if (safeCommand.current().saveStatus().hasBeen(Applied))
            {
                Timestamp executesAtLeast = safeCommand.current().executesAtLeast();
                if (executesAtLeast == null) safeCommand.update(safeStore, erased(command));
                else safeCommand.update(safeStore, truncatedApply(attributes, TruncatedApply, executeAt, null, null, executesAtLeast));
            }
            safeStore.progressLog().clear(command.txnId());
        }
    }

    public static void setErased(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        purge(safeStore, safeCommand, null, null, ERASE, true);
    }

    /**
     * Purge all or part of the metadata for a Commmand
     */
    public static Command purge(SafeCommandStore safeStore, SafeCommand safeCommand, @Nullable Seekables<?, ?> keysOrRanges, @Nullable Unseekables<?> maybeFullRoute, Cleanup cleanup, boolean notifyListeners)
    {
        final Command command = safeCommand.current();

        //   1) a command has been applied; or
        //   2) has been coordinated but *will not* be applied (we just haven't witnessed the invalidation yet); or
        //   3) a command is durably decided and this shard only hosts its home data, so no explicit truncation is necessary to remove it
        // TODO (desired): consider if there are better invariants we can impose for undecided transactions, to verify they aren't later committed (should be detected already, but more is better)
        // note that our invariant here is imperfectly applied to keep the code cleaner: we don't verify that the caller was safe to invoke if we don't already have a route in the command and we're only PreCommitted
        Invariants.checkState(command.hasBeen(Applied) || !command.hasBeen(PreCommitted)
                              || (command.route() == null || Infer.safeToCleanup(safeStore, command, command.route(), command.executeAt()) || safeStore.isFullyPreBootstrapOrStale(command, command.route().participants()))
        , "Command %s could not be truncated", command);

        Command result = purge(command, maybeFullRoute, cleanup);
        safeCommand.update(safeStore, result);
        safeStore.progressLog().clear(safeCommand.txnId());
        if (notifyListeners)
            safeStore.notifyListeners(safeCommand, result);
        return result;
    }

    public static Command purge(Command command, @Nullable Unseekables<?> maybeFullRoute, Cleanup cleanup)
    {
        Command result;
        switch (cleanup)
        {
            default: throw new AssertionError("Unexpected cleanup: " + cleanup);
            case INVALIDATE:
                Invariants.checkArgument(!command.hasBeen(PreCommitted));
                result = invalidated(command);
                break;

            case TRUNCATE_WITH_OUTCOME:
                Invariants.checkArgument(!command.hasBeen(Truncated));
                Invariants.checkState(command.hasBeen(PreApplied));
                result = truncatedApplyWithOutcome(command.asExecuted());
                break;

            case TRUNCATE:
                Invariants.checkState(command.saveStatus().compareTo(TruncatedApply) < 0);
                if (!command.hasBeen(PreCommitted)) result = Command.Truncated.erasedOrInvalidOrVestigial(command);
                else result = truncatedApply(command, Route.tryCastToFullRoute(maybeFullRoute));
                break;

            case ERASE:
                Invariants.checkState(command.saveStatus().compareTo(Erased) < 0);
                result = erased(command);
                break;
        }
        return result;
    }

    public static boolean maybeCleanup(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, EpochSupplier toEpoch, Unseekables<?> maybeFullRoute)
    {
        Cleanup cleanup = shouldCleanup(safeStore, command, toEpoch, maybeFullRoute);
        if (command.saveStatus().compareTo(cleanup.appliesIfNot) >= 0)
            return false;

        purge(safeStore, safeCommand, null, maybeFullRoute, cleanup, true);
        return true;
    }

    // TODO (now): either ignore this message if we don't have a route, or else require FullRoute requiring route, or else require FullRoute
    public static Command setDurability(SafeCommandStore safeStore, SafeCommand safeCommand, Durability durability, @Nullable Route<?> route, @Nullable Timestamp executeAt)
    {
        return setDurability(safeStore, safeCommand, durability, route, executeAt, null);
    }

    public static Command setDurability(SafeCommandStore safeStore, SafeCommand safeCommand, Durability durability, @Nullable Route<?> route, @Nullable Timestamp executeAt, @Nullable EpochSupplier toEpoch)
    {
        final Command command = safeCommand.current();
        if (command.is(Truncated))
            return command;

        if (command.durability().compareTo(durability) >= 0)
            return command;

        CommonAttributes attrs = route == null ? command : updateRoute(command, route);
        if (executeAt != null && command.status().hasBeen(Committed) && !command.executeAt().equals(executeAt))
            safeStore.agent().onInconsistentTimestamp(command, command.asCommitted().executeAt(), executeAt);
        attrs = attrs.mutable().durability(durability);

        Command updated = safeCommand.updateAttributes(safeStore, attrs);
        if (maybeCleanup(safeStore, safeCommand, command, toEpoch, route))
            updated = safeCommand.current();

        safeStore.notifyListeners(safeCommand, command);
        return updated;
    }

    static class NotifyWaitingOn implements PreLoadContext, Consumer<SafeCommandStore>
    {
        final TxnId waitingId;
        TxnId loadDepId;

        public NotifyWaitingOn(SafeCommand root)
        {
            Invariants.checkArgument(root.current().hasBeen(Stable));
            this.waitingId = root.txnId();
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            SafeCommand waitingSafe = safeStore.get(waitingId);
            SafeCommand depSafe = null;
            {
                Command waiting = waitingSafe.current();
                if (waiting.saveStatus().compareTo(Applying) >= 0)
                    return; // nothing to do

                if (loadDepId != null)
                {
                    depSafe = safeStore.ifInitialised(loadDepId);
                    if (depSafe == null)
                    {
                        RedundantStatus redundantStatus = safeStore.commandStore().redundantBefore().status(waitingId, waiting.executeAt(), waiting.partialDeps().participants(loadDepId));
                        switch (redundantStatus)
                        {
                            default: throw new AssertionError("Unexpected redundant status: " + redundantStatus);
                            case NOT_OWNED: throw new AssertionError("Invalid state: waiting for execution of command that is not owned at the execution time");
                            case SHARD_REDUNDANT:
                            case LOCALLY_REDUNDANT:
                            case REDUNDANT_PRE_BOOTSTRAP_OR_STALE:
                            case PRE_BOOTSTRAP_OR_STALE:
                                removeRedundantDependencies(safeStore, waitingSafe, loadDepId);
                                break;
                            case LIVE:
                            case PARTIALLY_PRE_BOOTSTRAP_OR_STALE:
                        }
                    }
                }
            }

            while (true)
            {
                Command waiting = waitingSafe.current();
                if (depSafe == null)
                {
                    WaitingOn waitingOn = waiting.asCommitted().waitingOn();
                    TxnId directlyBlockedOn = waitingOn.nextWaitingOn();
                    if (directlyBlockedOn == null)
                    {
                        if (waitingOn.isWaiting())
                            return; // nothing more we can do; all direct dependencies are notified

                        switch (waiting.saveStatus())
                        {
                            default: throw illegalState("Invalid saveStatus with empty waitingOn: " + waiting.saveStatus());
                            case ReadyToExecute:
                            case Applied:
                            case Applying:
                                return;

                            case Stable:
                            case PreApplied:
                                boolean executed = maybeExecute(safeStore, waitingSafe, true, false);
                                Invariants.checkState(executed);
                                return;
                        }
                    }

                    depSafe = safeStore.ifLoadedAndInitialised(directlyBlockedOn);
                    if (depSafe == null)
                    {
                        loadDepId = directlyBlockedOn;
                        safeStore.commandStore().execute(this, this).begin(safeStore.agent());
                        return;
                    }
                }
                else
                {
                    Command dep = depSafe.current();
                    SaveStatus depStatus = dep.saveStatus();
                    switch (depStatus.known.executeAt)
                    {
                        case ExecuteAtKnown:
                            if (waitingId.kind().awaitsOnlyDeps() || dep.executeAt().compareTo(waiting.executeAt()) < 0)
                                break;

                        case NoExecuteAt:
                            updateDependencyAndMaybeExecute(safeStore, waitingSafe, depSafe, false);
                            Invariants.checkState(!waitingSafe.current().asCommitted().waitingOn().isWaitingOn(dep.txnId()));
                            depSafe = null;
                            continue;
                    }

                    if (!Route.isFullRoute(dep.route()) || depStatus.hasBeen(Truncated))
                    {
                        // TODO (desired): slightly costly to invert a large partialDeps collection
                        Participants<?> participants = waiting.partialDeps().participants(dep.txnId());
                        RedundantStatus redundantStatus = safeStore.commandStore().redundantBefore().status(dep.txnId(), waiting.executeAt(), participants);
                        switch (redundantStatus)
                        {
                            default: throw new AssertionError("Unknown redundant status: " + redundantStatus);
                            case NOT_OWNED: throw new AssertionError("Invalid state: waiting for execution of command that is not owned at the execution time");
                            case LIVE:
                            case PARTIALLY_PRE_BOOTSTRAP_OR_STALE:
                                if (logger.isTraceEnabled()) logger.trace("{} blocked on {} until ReadyToExclude", waitingId, dep.txnId());
                                safeStore.registerListener(depSafe, HasDecidedExecuteAt.minSaveStatus, Full, waitingId);
                                safeStore.progressLog().waiting(HasDecidedExecuteAt, safeStore, depSafe, null, participants);
                                return;

                            case LOCALLY_REDUNDANT:
                            case SHARD_REDUNDANT:
                            case PRE_BOOTSTRAP_OR_STALE:
                            case REDUNDANT_PRE_BOOTSTRAP_OR_STALE:
                                Invariants.checkState(dep.hasBeen(Applied) || !dep.hasBeen(PreCommitted) || redundantStatus == PRE_BOOTSTRAP_OR_STALE);

                                // we've been applied, invalidated, or are no longer relevant
                                removeRedundantDependencies(safeStore, waitingSafe, dep.txnId());
                                depSafe = null;
                                continue;
                        }
                    }

                    switch (depStatus.execution)
                    {
                        default: throw new AssertionError("Unhandled LocalExecution: " + depStatus.execution);
                        case CleaningUp: throw illegalState("Invalid LocalExecution (should already be handled): " + depStatus.execution);

                        case NotReady:
                            safeStore.registerListener(depSafe, HasDecidedExecuteAt.minSaveStatus, Full, waitingId);
                            safeStore.progressLog().waiting(HasDecidedExecuteAt, safeStore, depSafe, dep.route(), null);
                            return;

                        case ReadyToExclude:
                        case WaitingToExecute:
                        case ReadyToExecute:
                            safeStore.progressLog().waiting(CanApply, safeStore, depSafe, dep.route(), null);

                        case Applying:
                            safeStore.registerListener(depSafe, SaveStatus.Applied, Full, waitingId);
                            return;

                        case WaitingToApply:
                            if (dep.asCommitted().isWaitingOnDependency())
                            {
                                safeStore.registerListener(depSafe, SaveStatus.Applied, Full, waitingId);
                                return;
                            }
                            else
                            {
                                maybeExecute(safeStore, depSafe, false, false);
                                switch (depSafe.current().saveStatus())
                                {
                                    default: throw illegalState("Invalid child status after attempt to execute: " + depSafe.current().saveStatus());
                                    case Applying:
                                        safeStore.registerListener(depSafe, SaveStatus.Applied, Full, waitingId);
                                        return;

                                    case Applied:
                                        // fall-through to outer Applied branch
                                }
                            }

                        case Applied:
                            updateDependencyAndMaybeExecute(safeStore, waitingSafe, depSafe, false);
                            depSafe = null;
                    }
                }
            }
        }

        @Override
        public TxnId primaryTxnId()
        {
            return waitingId;
        }

        @Override
        public Collection<TxnId> additionalTxnIds()
        {
            return loadDepId == null ? Collections.emptyList() : Collections.singletonList(loadDepId);
        }
    }

    static Command removeRedundantDependencies(SafeCommandStore safeStore, SafeCommand safeCommand, @Nullable TxnId redundant)
    {
        CommandStore commandStore = safeStore.commandStore();
        Command.Committed current = safeCommand.current().asCommitted();

        WaitingOn.Update update = new WaitingOn.Update(current.waitingOn);
        TxnId minWaitingOnTxnId = update.minWaitingOnTxnId();
        if (minWaitingOnTxnId != null && commandStore.hasLocallyRedundantDependencies(update.minWaitingOnTxnId(), current.executeAt(), current.route().participants()))
            safeStore.commandStore().removeRedundantDependencies(current.route().participants(), update);

        // if we are a range transaction, being redundant for this transaction does not imply we are redundant for all transactions
        if (redundant != null)
            update.removeWaitingOn(redundant);
        return safeCommand.updateWaitingOn(update);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void ensureHomeIsMonitoring(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, Unseekables<?> maybeRoute)
    {
        if (command.known().isDefinitionKnown() || command.hasBeen(Truncated))
            return;

        Route<?> route = Route.tryCastToRoute(maybeRoute);
        if (route == null)
            return;

        safeCommand.updateAttributes(safeStore, command.mutable().route(Route.merge((Route)route, command.route())));
    }

    /**
     * A key nominated to represent the "home" shard - only members of the home shard may be nominated to recover
     * a transaction, to reduce the cluster-wide overhead of ensuring progress. A transaction that has only been
     * witnessed at PreAccept may however trigger a process of ensuring the home shard is durably informed of
     * the transaction.
     *
     * Note that for ProgressLog purposes the "home shard" is the shard as of txnId.epoch.
     * For recovery purposes the "home shard" is as of txnId.epoch until Committed, and executeAt.epoch once Executed
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static CommonAttributes updateRoute(Command command, Route<?> route)
    {
        if (command.route() == null || !Route.isFullRoute(command.route()))
            return command.mutable().route(Route.merge((Route)route, command.route()));

        return command;
    }

    public static Command updateRouteOrParticipants(SafeCommandStore safeStore, SafeCommand safeCommand, Unseekables<?> participants)
    {
        Command current = safeCommand.current();
        if (current.hasBeen(Invalidated))
            return current;

        CommonAttributes updated = current;
        if (Route.isRoute(participants))
        {
            updated = updateRoute(current, Route.castToRoute(participants));
        }
        else
        {
            Route<?> existingRoute = current.route();
            if (existingRoute != null && !existingRoute.containsAll(participants))
                updated = current.mutable().route(existingRoute.with((Unseekables) participants));
        }

        if (current == updated)
            return current;

        return safeCommand.updateAttributes(safeStore, updated);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static CommonAttributes set(SaveStatus newStatus, Command cur, CommonAttributes upd,
                                        Ranges acceptRanges, @Nullable Ballot newAcceptedOrCommittedBallot,
                                        Route<?> route, @Nullable PartialTxn partialTxn, @Nullable PartialDeps partialDeps)
    {
        Status.Known haveKnown = cur.saveStatus().known;
        Status.Known expectKnown = newStatus.known;

        if (Route.isFullRoute(upd.route())) route = upd.route();
        else upd = upd.mutable().route(route = Route.merge(upd.route(), (Route)route));

        Route<?> scope;
        if (partialTxn != null && expectKnown.definition.isKnown())
        {
            scope = route.slice(acceptRanges);
            partialTxn = partialTxn.intersecting(scope, true);
            if (haveKnown.definition.isKnown())
                upd = upd.mutable().partialTxn(upd.partialTxn().with(partialTxn));
            else
                upd = upd.mutable().partialTxn(partialTxn);
        }

        if (partialDeps != null && expectKnown.deps.hasProposedOrDecidedDeps() && (haveKnown.deps != expectKnown.deps || (newAcceptedOrCommittedBallot != null && !newAcceptedOrCommittedBallot.equals(cur.acceptedOrCommitted()))))
        {
            scope = route.slice(acceptRanges);
            upd = upd.mutable().partialDeps(partialDeps.intersecting(scope));
        }

        return upd;
    }

    private static boolean validate(SaveStatus newStatus, Command cur,
                                    Ranges acceptRanges,
                                    Route<?> addRoute,
                                    @Nullable PartialTxn addPartialTxn,
                                    @Nullable PartialDeps newPartialDeps,
                                    @Nullable SafeCommandStore permitStaleMissing)
    {
        Status.Known haveKnown = cur.saveStatus().known;
        Status.Known expectKnown = newStatus.known;

        FullRoute<?> fullRoute;
        if (isFullRoute(cur.route())) fullRoute = Route.castToFullRoute(cur.route());
        else if (isFullRoute(addRoute)) fullRoute = Route.castToFullRoute(addRoute);
        else return expectKnown.route != Full; // we cannot validate further without a full route, so in cases we don't expect a full route we just assume we have received the right information

        Participants<?> acceptScope = null;
        if (expectKnown.definition.isKnown())
        {
            acceptScope = fullRoute.slice(acceptRanges);
            if (haveKnown.definition.isKnown())
            {
                // TODO (desired): avoid converting to participants before subtracting
                Participants<?> extraScope = acceptScope.without(cur.partialTxn().keys().toParticipants());
                if (!containsAll(addPartialTxn, PartialTxn::covers, extraScope, permitStaleMissing))
                    return false;
            }
            else
            {
                if (!containsAll(addPartialTxn, PartialTxn::covers, acceptScope, permitStaleMissing))
                    return false;
            }
        }

        if (haveKnown.deps != expectKnown.deps && expectKnown.deps.hasProposedOrDecidedDeps())
        {
            if (acceptScope == null)
                acceptScope = fullRoute.slice(acceptRanges);

            return containsAll(newPartialDeps, PartialDeps::covers, acceptScope, permitStaleMissing);
        }

        return true;
    }

    private static <V> boolean containsAll(V adding, BiPredicate<V, Participants<?>> covers, Participants<?> required, @Nullable SafeCommandStore permitStaleMissing)
    {
        if (adding == null ? required.isEmpty() : covers.test(adding, required))
            return true;

        if (permitStaleMissing != null)
        {
            // TODO (required, later): in the event we are depending on a stale key for an insert into a non-stale key, we cannot proceed and must mark the new key stale
            //  I think today this is unsupported in practice, but must be addressed before we improve efficiency of result handling
            Ranges staleRanges = permitStaleMissing.commandStore().redundantBefore().staleRanges();
            required = required.without(staleRanges);
            return adding == null ? required.isEmpty() : covers.test(adding, required);
        }

        return false;
    }
}
