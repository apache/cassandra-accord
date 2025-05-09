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

import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.local.Command.WaitingOn;
import accord.local.Command.WaitingOn.Update;
import accord.local.CommandStores.RangesForEpochSupplier;
import accord.local.RedundantBefore.RedundantBeforeSupplier;
import accord.local.cfk.CommandsForKey;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Known;
import accord.primitives.Known.KnownExecuteAt;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasDecidedExecuteAt;
import static accord.api.ProtocolModifiers.Toggles.DependencyElision.IF_DURABLE;
import static accord.api.ProtocolModifiers.Toggles.dependencyElision;
import static accord.api.ProtocolModifiers.Toggles.markStaleIfCannotExecute;
import static accord.local.Cleanup.EXPUNGE;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.NO;
import static accord.local.Cleanup.shouldCleanup;
import static accord.local.Command.Truncated.erased;
import static accord.local.Command.Truncated.invalidated;
import static accord.local.Command.Truncated.truncated;
import static accord.local.Command.Truncated.vestigial;
import static accord.local.Commands.Validated.INSUFFICIENT;
import static accord.local.Commands.Validated.UPDATE_TXN_AND_DEPS_INTERSECT_STABLE;
import static accord.local.Commands.Validated.UPDATE_TXN_IGNORE_DEPS;
import static accord.local.Commands.Validated.UPDATE_TXN_KEEP_DEPS;
import static accord.local.Commands.Validated.UPDATE_TXN_AND_DEPS;
import static accord.local.Commands.Validated.UPDATE_TXN_MERGE_DEPS;
import static accord.local.KeyHistory.INCR;
import static accord.local.KeyHistory.SYNC;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DEFUNCT;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.RedundantStatus.Property.WAS_OWNED;
import static accord.local.StoreParticipants.Filter.LOAD;
import static accord.local.StoreParticipants.Filter.UPDATE;
import static accord.messages.Commit.Kind.StableFastPath;
import static accord.messages.Commit.Kind.StableMediumPath;
import static accord.primitives.Known.KnownDeps.DepsFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsProposedFixed;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.Outcome.Apply;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.LocalExecution.WaitingToExecute;
import static accord.primitives.SaveStatus.PreAccepted;
import static accord.primitives.SaveStatus.PreAcceptedWithDeps;
import static accord.primitives.SaveStatus.PreAcceptedWithVote;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Committed;
import static accord.primitives.Status.Durability;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Known.KnownRoute.FullRoute;
import static accord.primitives.Status.NotDefined;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Route.isFullRoute;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.utils.Invariants.illegalState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class Commands
{
    private static final Logger logger = LoggerFactory.getLogger(Commands.class);

    private Commands()
    {
    }

    public enum AcceptOutcome { Success, Redundant, RejectedBallot, Insufficient, Retired, Truncated }

    public static AcceptOutcome preaccept(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, TxnId txnId, Txn partialTxn, @Nullable Deps partialDeps, boolean hasCoordinatorVote)
    {
        Invariants.require(partialDeps == null || txnId.is(PrivilegedCoordinatorWithDeps));
        Invariants.require(!hasCoordinatorVote || txnId.hasPrivilegedCoordinator());
        SaveStatus newSaveStatus;
        if (partialDeps != null) newSaveStatus = PreAcceptedWithDeps;
        else if (hasCoordinatorVote) newSaveStatus = PreAcceptedWithVote;
        else newSaveStatus = PreAccepted;

        return preacceptOrRecover(safeStore, safeCommand, participants, newSaveStatus, txnId, partialTxn, partialDeps, Ballot.ZERO);
    }

    public static AcceptOutcome recover(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, TxnId txnId, PartialTxn partialTxn, Ballot ballot)
    {
        // for recovery we only ever propose either the original epoch or an Accept that we witness; otherwise we invalidate
        return preacceptOrRecover(safeStore, safeCommand, participants, SaveStatus.PreAccepted, txnId, partialTxn, null, ballot);
    }

    private static AcceptOutcome preacceptOrRecover(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, SaveStatus newSaveStatus, TxnId txnId, Txn txn, @Nullable Deps deps, Ballot ballot)
    {
        final Command command = safeCommand.current();
        if (command.hasBeen(Truncated))
        {
            logger.trace("{}: skipping preaccept - command is truncated", txnId);
            return command.is(Invalidated) ? AcceptOutcome.RejectedBallot : participants.owns().isEmpty()
                                             ? AcceptOutcome.Retired : AcceptOutcome.Truncated;
        }

        int compareBallots = command.promised().compareTo(ballot);
        if (compareBallots > 0)
        {
            logger.trace("{}: skipping preaccept - higher ballot witnessed ({})", txnId, command.promised());
            return AcceptOutcome.RejectedBallot;
        }

        if (command.known().definition().isKnown())
        {
            Invariants.require(command.status() == Invalidated || command.executeAt() != null);
            logger.trace("{}: skipping preaccept - already known ({})", txnId, command.status());
            // in case of Ballot.ZERO, we must either have a competing recovery coordinator or have late delivery of the
            // preaccept; in the former case we should abandon coordination, and in the latter we have already completed
            safeCommand.updatePromised(ballot);
            return ballot.equals(Ballot.ZERO) ? AcceptOutcome.Redundant : AcceptOutcome.Success;
        }

        if (command.known().deps().hasProposedOrDecidedDeps()) participants = command.participants().supplement(participants);
        else participants = participants.filter(UPDATE, safeStore, txnId, null);

        Validated validated = validate(ballot, newSaveStatus, command, participants, participants.route(), txn, deps);
        Invariants.require(validated != INSUFFICIENT);

        if (command.executeAt() == null)
        {
            // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
            //  - use a global logical clock to issue new timestamps; or
            //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
            // if we are performing recovery (i.e. non-zero ballot), do not permit a fast path decision as we want to
            // invalidate any transactions that were not completed by their initial coordinator
            // TODO (desired): limit preaccept to keys we include, to avoid inflating unnecessary state
            Timestamp executeAt = safeStore.commandStore().preaccept(txnId, participants.route(), safeStore, ballot.equals(Ballot.ZERO));
            if (txnId != executeAt || !command.is(NotDefined))
            {
                newSaveStatus = PreAccepted;
                validated = UPDATE_TXN_IGNORE_DEPS;
            }

            PartialTxn partialTxn = prepareTxn(newSaveStatus, participants, command, txn);
            PartialDeps partialDeps = prepareDeps(validated, participants, command, deps);
            participants = prepareParticipants(validated, participants, command);
            safeCommand.preaccept(safeStore, newSaveStatus, participants, ballot, executeAt, partialTxn, partialDeps);
        }
        else
        {
            // TODO (expected): in the case that we are pre-committed but had not been preaccepted/accepted, should we inform progressLog?
            PartialTxn partialTxn = prepareTxn(newSaveStatus, participants, command, txn);
            participants = prepareParticipants(validated, participants, command);
            safeCommand.markDefined(safeStore, participants, ballot, partialTxn);
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

    private static AcceptOutcome maybeRejectAccept(Ballot ballot, Timestamp executeAt, Command command)
    {
        Status status = command.status();
        int compareStatus = status.compareTo(PreCommitted);
        if (compareStatus > 0)
        {
            logger.trace("{}: skipping accept/notaccept - already committed/invalidated ({})", command.txnId(), status);
            return AcceptOutcome.Redundant;
        }

        Ballot promised = command.promised();
        int comparePromised = command.promised().compareTo(ballot);
        if (comparePromised > 0 || (comparePromised == 0 && compareStatus == 0 && command.acceptedOrCommitted().compareTo(ballot) == 0))
        {
            if (logger.isTraceEnabled())
                logger.trace("{}: rejecting accept/notaccept - witnessed higher ballot (({},{}) > {})", command.txnId(), promised, status, ballot);
            return AcceptOutcome.RejectedBallot;
        }

        if (compareStatus == 0 && executeAt != null && !executeAt.equals(command.executeAt()))
        {
            // we have to special-case this because we advance to Stable/Applied without Ballot, so we can propagate PreCommitted without the ballot used to agree it
            if (logger.isTraceEnabled())
                logger.trace("{}: rejecting accept/notaccept - witnessed conflicting committed timestamp: {} != {}", command.txnId(), executeAt, command.executeAt());
            return AcceptOutcome.RejectedBallot;
        }
        return null;
    }

    public static AcceptOutcome accept(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, TxnId txnId, Accept.Kind kind, Ballot ballot, Route<?> route, Timestamp executeAt, PartialDeps deps)
    {
        final Command command = safeCommand.current();
        {
            AcceptOutcome reject = maybeRejectAccept(ballot, executeAt, command);
            if (reject != null)
                return reject;
        }

        SaveStatus newSaveStatus = SaveStatus.get(kind == Accept.Kind.MEDIUM ? Status.AcceptedMedium : Status.AcceptedSlow, command.known());
        participants = participants.filter(UPDATE, safeStore, txnId, null);
        Validated validated = validate(ballot, newSaveStatus, command, participants, route, null, deps);
        Invariants.require(validated != INSUFFICIENT);

        PartialTxn partialTxn = prepareTxn(newSaveStatus, participants, command, null);
        PartialDeps partialDeps = prepareDeps(validated, participants, command, deps);
        participants = prepareParticipants(validated, participants, command);

        safeCommand.accept(safeStore, newSaveStatus, participants, ballot, executeAt, partialTxn, partialDeps, ballot);
        safeStore.notifyListeners(safeCommand, command);

        return AcceptOutcome.Success;
    }

    public static AcceptOutcome notAccept(SafeCommandStore safeStore, SafeCommand safeCommand, Status status, Ballot ballot)
    {
        final Command command = safeCommand.current();
        {
            AcceptOutcome reject = maybeRejectAccept(ballot, null, command);
            if (reject != null)
                return reject;
        }

        logger.trace("{}: not accepted ({})", command.txnId(), status);
        safeCommand.notAccept(safeStore, status, ballot);
        safeStore.notifyListeners(safeCommand, command);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Rejected, Redundant, Insufficient }


    // relies on mutual exclusion for each key
    public static CommitOutcome commit(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, SaveStatus newSaveStatus, Ballot ballot, TxnId txnId, Route<?> route, @Nullable Txn txn, Timestamp executeAt, Deps deps, @Nullable Commit.Kind kind)
    {
        final Command command = safeCommand.current();
        if (kind == StableFastPath && !command.promised().equals(Ballot.ZERO))
            return CommitOutcome.Rejected;

        SaveStatus curStatus = command.saveStatus();
        Invariants.requireArgument(newSaveStatus == SaveStatus.Committed || newSaveStatus == SaveStatus.Stable);
        if (newSaveStatus == SaveStatus.Committed && ballot.compareTo(command.promised()) < 0)
            return curStatus.is(Truncated) || participants.owns().isEmpty()
                   ? CommitOutcome.Redundant : CommitOutcome.Rejected;

        if (curStatus.hasBeen(PreCommitted))
        {
            if (!curStatus.is(Truncated))
            {
                if (!executeAt.equals(command.executeAt()) || curStatus.status == Invalidated)
                    safeStore.agent().onInconsistentTimestamp(command, (curStatus.status == Invalidated ? Timestamp.NONE : command.executeAt()), executeAt);
            }

            if (curStatus.compareTo(newSaveStatus) > 0 || curStatus.hasBeen(Stable))
            {
                logger.trace("{}: skipping commit - already newer or stable ({})", txnId, command.status());
                return CommitOutcome.Redundant;
            }

            if (curStatus == SaveStatus.Committed && newSaveStatus == SaveStatus.Committed)
            {
                if (ballot.equals(command.acceptedOrCommitted()))
                    return CommitOutcome.Redundant;

                Invariants.require(ballot.compareTo(command.acceptedOrCommitted()) > 0);
            }
        }

        participants = participants.filter(UPDATE, safeStore, txnId, executeAt);
        Validated validated = validate(ballot, newSaveStatus, command, participants, route, txn, deps, kind, executeAt);
        if (validated == INSUFFICIENT)
            return CommitOutcome.Insufficient;

        PartialTxn partialTxn = prepareTxn(newSaveStatus, participants, command, txn);
        PartialDeps partialDeps = prepareDeps(validated, participants, command, deps);
        participants = prepareParticipants(validated, participants, command);

        if (logger.isTraceEnabled())
            logger.trace("{}: committed with executeAt: {}, deps: {}", txnId, executeAt, deps);
        final Command.Committed committed;
        if (newSaveStatus == SaveStatus.Stable)
        {
            WaitingOn waitingOn = initialiseWaitingOn(safeStore, txnId, executeAt, participants, partialDeps);
            committed = safeCommand.stable(safeStore, participants, ballot, executeAt, partialTxn, partialDeps, waitingOn);
            safeStore.agent().eventListener().onStable(committed);
            maybeExecute(safeStore, safeCommand, true, true);
        }
        else
        {
            Invariants.requireArgument(command.acceptedOrCommitted().compareTo(ballot) <= 0);
            committed = safeCommand.commit(safeStore, participants, ballot, executeAt, partialTxn, partialDeps);
            safeStore.notifyListeners(safeCommand, committed);
            safeStore.agent().eventListener().onCommitted(committed);
        }

        return CommitOutcome.Success;
    }

    // relies on mutual exclusion for each key
    public static CommitOutcome precommit(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, TxnId txnId, Timestamp executeAt, Ballot promisedAtLeast)
    {
        Invariants.require(Route.isFullRoute(participants.route()));
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

        supplementParticipants(safeStore, safeCommand, participants);
        safeCommand.precommit(safeStore, executeAt, promisedAtLeast);
        safeStore.notifyListeners(safeCommand, command);
        logger.trace("{}: precommitted with executeAt: {}", txnId, executeAt);
        return CommitOutcome.Success;
    }

    public static void ephemeralRead(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Route<?> route, TxnId txnId, PartialTxn txn, PartialDeps deps)
    {
        Command command = safeCommand.current();
        if (command.hasBeen(Stable))
            return;

        // BREAKING CHANGE NOTE: if in future we support a CommandStore adopting additional ranges (rather than only shedding them)
        //                       then we need to revisit how we execute transactions that awaitsOnlyDeps, as they may need additional
        //                       information to execute in the eventual execution epoch (that they didn't know they needed when they were made stable)

        participants = participants.supplement(route);
        participants = participants.filter(UPDATE, safeStore, txnId, null);
        Validated validated = validate(null, SaveStatus.Stable, command, participants, route, txn, deps);
        Invariants.require(validated != INSUFFICIENT);

        PartialTxn partialTxn = prepareTxn(SaveStatus.Stable, participants, command, txn);
        PartialDeps partialDeps = prepareDeps(validated, participants, command, deps);
        participants = prepareParticipants(validated, participants, command);

        safeCommand.stable(safeStore, participants, Ballot.ZERO, txnId, partialTxn, partialDeps, initialiseWaitingOn(safeStore, txnId, txnId, participants, partialDeps));
        maybeExecute(safeStore, safeCommand, false, true);
    }

    public static void eraseEphemeralRead(SafeCommandStore safeStore, TxnId txnId)
    {
        SafeCommand safeCommand = safeStore.unsafeGetNoCleanup(txnId);
        if (safeCommand == null)
            return;

        Command command = safeCommand.current();
        if (command.hasBeen(Truncated))
            return;

        safeCommand.set(erased(command));
    }

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

    public static ApplyOutcome apply(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, TxnId txnId, Route<?> route, Timestamp executeAt, @Nullable Deps deps, @Nullable Txn txn, Writes writes, Result result)
    {
        Command command = safeCommand.current();
        if (command.hasBeen(PreApplied))
        {
            logger.trace("{}: skipping apply - already preapplied ({})", txnId, command.status());
            boolean inconsistent = false;
            Timestamp cur = command.executeAt();
            KnownExecuteAt known = command.known().executeAt();
            switch (known)
            {
                default: throw UnhandledEnum.unknown(known);
                case ExecuteAtProposed: throw UnhandledEnum.invalid(known);
                case ApplyAtKnown: inconsistent = !executeAt.equalsStrict(cur); break;
                case ExecuteAtKnown: inconsistent = !executeAt.equals(cur); break;
                case NoExecuteAt: inconsistent = true;
                case ExecuteAtErased:
                case ExecuteAtUnknown:
            }
            if (inconsistent)
                safeStore.agent().onInconsistentTimestamp(command, command.executeAt(), executeAt);
            return ApplyOutcome.Redundant;
        }
        else if (command.hasBeen(PreCommitted) && !executeAt.equals(command.executeAt()))
        {
            if (command.is(Truncated) && command.executeAt() == null)
                return ApplyOutcome.Redundant;
            safeStore.agent().onInconsistentTimestamp(command, command.executeAt(), executeAt);
        }

        participants = participants.filter(UPDATE, safeStore, txnId, executeAt);
        Validated validated = validate(Ballot.ZERO, SaveStatus.PreApplied, command, participants, route, txn, deps, null, executeAt);
        if (validated == INSUFFICIENT)
            return ApplyOutcome.Insufficient;

        PartialTxn partialTxn = prepareTxn(SaveStatus.PreApplied, participants, command, txn);
        PartialDeps partialDeps = prepareDeps(validated, participants, command, deps);
        participants = prepareParticipants(validated, participants, command);

        WaitingOn waitingOn = !command.hasBeen(Stable) ? initialiseWaitingOn(safeStore, txnId,  executeAt, participants, partialDeps)
                                                       : command.asCommitted().waitingOn();

        safeCommand.preapplied(safeStore, participants, executeAt, partialTxn, partialDeps, waitingOn, writes, result);
        if (logger.isTraceEnabled())
            logger.trace("{}: apply, status set to Executed with executeAt: {}, deps: {}", txnId, executeAt, partialDeps);

        // must signal preapplied first, else we may be applied (and have cleared progress log state) already before maybeExecute exits
        maybeExecute(safeStore, safeCommand, true, true);
        safeStore.agent().eventListener().onExecuted(command);

        return ApplyOutcome.Success;
    }

    public static void listenerUpdate(SafeCommandStore safeStore, SafeCommand safeListener, SafeCommand safeUpdated)
    {
        Command listener = safeListener.current();
        Command updated = safeUpdated.current();
        if (listener.is(NotDefined) || listener.is(Truncated))
        {
            // This listener must be a stale vestige
            Invariants.require(listener.saveStatus().hasBeen(Truncated), "Listener status expected to be Truncated, but was %s", listener.saveStatus());
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                         listener.txnId(), updated.txnId(), updated.status(), updated);

        switch (updated.status())
        {
            default:
                throw illegalState("Unexpected status: " + updated.status());
            case NotDefined:
            case PreAccepted:
            case AcceptedMedium:
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

    protected static void postApply(SafeCommandStore safeStore, TxnId txnId, long t0, boolean forceApply)
    {
        SafeCommand safeCommand = safeStore.get(txnId);
        Command command = safeCommand.current();
        logger.trace("{} applied, setting status to Applied and notifying listeners", command);
        if (command.hasBeen(Applied) && !forceApply)
            return;

        safeCommand.applied(safeStore);
        safeStore.notifyListeners(safeCommand, command);
        if (t0 >= 0) safeStore.agent().eventListener().onApplied(command, t0);
    }

    /**
     * The ranges for which we participate in the execution of a transaction, excluding those ranges
     * for transactions below a SyncPoint where we adopted the range, and that will be obtained from peers,
     * and therefore we do not want to execute locally
     */
    public static Ranges applyRanges(SafeCommandStore safeStore, Timestamp executeAt)
    {
        return safeStore.ranges().allAt(executeAt.epoch());
    }

    public static AsyncChain<Void> applyChain(SafeCommandStore safeStore, Command.Executed command)
    {
        // TODO (required): make sure we are correctly handling (esp. C* side with validation logic) executing a transaction
        //  that was pre-bootstrap for some range (so redundant and we may have gone ahead of), but had to be executed locally
        //  for another range
        CommandStore unsafeStore = safeStore.commandStore();
        // TODO (required, API): do we care about tracking the write persistence latency, when this is just a memtable write?
        //  the only reason it will be slow is because Memtable flushes are backed-up (which will be reported elsewhere)
        // TODO (required): this is anyway non-monotonic and milliseconds granularity
        long t0 = safeStore.node().elapsed(MICROSECONDS);
        TxnId txnId = command.txnId();
        Participants<?> executes = command.participants().stillExecutes(); // including any keys we aren't writing
        return command.writes().apply(safeStore, executes, command.partialTxn())
                      // TODO (expected): once we guarantee execution order KeyHistory can be ASYNC
               .flatMap(unused -> unsafeStore.build(contextFor(txnId, executes, SYNC), ss -> {
                   postApply(ss, txnId, t0, false);
                   return null;
               }));
    }

    @VisibleForImplementation
    public static AsyncChain<Void> applyWrites(SafeCommandStore safeStore, PreLoadContext context, Command command)
    {
        CommandStore unsafeStore = safeStore.commandStore();
        Command.Executed executed = command.asExecuted();
        Participants<?> executes = executed.participants().stillExecutes();
        if (executes.isEmpty())
            return AsyncChains.success(null);

        TxnId txnId = command.txnId();
        return command.writes().apply(safeStore, executes, command.partialTxn())
                      .flatMap(unused -> unsafeStore.build(context, ss -> {
                          postApply(ss, txnId, -1, true);
                          return null;
                      }));
    }

    public static boolean maybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        return maybeExecute(safeStore, safeCommand, safeCommand.current(), alwaysNotifyListeners, notifyWaitingOn);
    }

    public static boolean maybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: Maybe executing with status {}. Will notify listeners on noop: {}",
                         command.txnId(), command.status(), alwaysNotifyListeners);

        if (command.status() != Stable && command.saveStatus() != SaveStatus.PreApplied)
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

        TxnId txnId = command.txnId();
        switch (command.status())
        {
            case Stable:
                // TODO (required): maintain distinct ReadyToRead and ReadyToWrite states
                // immediately, as no transaction should take a local dependency on this transaction.
                // This handles both transactions whose ownership is lost, as well as those that become pre-bootstrap or stale
                safeCommand.readyToExecute(safeStore);
                logger.trace("{}: set to ReadyToExecute", txnId);
                safeStore.notifyListeners(safeCommand, command);
                return true;

            case PreApplied:
                Command.Executed executed = command.asExecuted();
                if (txnId.is(Write) && executed.writes().keys.intersects(executed.participants().stillExecutes()))
                {
                    safeCommand.applying(safeStore);
                    safeStore.notifyListeners(safeCommand, command);
                    logger.trace("{}: applying", command.txnId());
                    applyChain(safeStore, executed).begin(safeStore.agent());
                }
                else
                {
                    logger.trace("{}: applying no-op", txnId);
                    safeCommand.applied(safeStore);
                    safeStore.notifyListeners(safeCommand, command);
                }
                return true;
            default:
                throw illegalState("Unexpected status: " + command.status());
        }
    }

    protected static WaitingOn initialiseWaitingOn(SafeCommandStore safeStore, TxnId waitingId, Timestamp waitingExecuteAt, StoreParticipants participants, PartialDeps deps)
    {
        if (waitingId.awaitsOnlyDeps())
            waitingExecuteAt = Timestamp.maxForEpoch(waitingId.epoch());

        WaitingOn.Initialise initialise = Update.initialise(safeStore, waitingId, waitingExecuteAt, participants, deps);
        return updateWaitingOn(safeStore, initialise, waitingExecuteAt, initialise).build();
    }

    protected static Update updateWaitingOn(SafeCommandStore safeStore, ICommand waiting, Timestamp executeAt, WaitingOn.Update initialise)
    {
        RedundantBefore redundantBefore = safeStore.redundantBefore();
        TxnId minWaitingOnTxnId = initialise.minWaitingOnTxnId();
        if (minWaitingOnTxnId != null && redundantBefore.hasLocallyRedundantDependencies(initialise.minWaitingOnTxnId(), executeAt, waiting.participants().waitsOn()))
            redundantBefore.removeRedundantDependencies(waiting.participants().waitsOn(), initialise);

        initialise.forEachWaitingOnId(safeStore, initialise, waiting, executeAt, (store, upd, w, exec, i) -> {
            // we don't want cleanup to transitively invoke a listener we've registered,
            // as we might still be initialising the WaitingOn collection
            SafeCommand dep = store.unsafeGetNoCleanup(upd.txnId(i));
            if (dep == null || dep.isUnset() || !dep.current().hasBeen(PreCommitted))
                return;
            updateWaitingOn(store, w, exec, upd, dep);
        });

        return initialise;
    }

    /**
     * @param dependencySafeCommand is either committed truncated, or invalidated
     * @return true iff {@code maybeExecute} might now have a different outcome
     */
    private static boolean updateWaitingOn(SafeCommandStore safeStore, ICommand waiting, Timestamp waitingExecuteAt, Update waitingOn, SafeCommand dependencySafeCommand)
    {
        TxnId waitingId = waiting.txnId();
        Command dependency = dependencySafeCommand.current();
        Invariants.require(dependency.hasBeen(PreCommitted));
        TxnId dependencyId = dependency.txnId();
        if (waitingId.awaitsOnlyDeps() && dependency.known().isExecuteAtKnown() && dependency.executeAt().compareTo(waitingId) > 0)
            waitingOn.updateExecuteAtLeast(waitingId, dependency.executeAt());

        if (dependency.hasBeen(Truncated))
        {
            switch (dependency.saveStatus())
            {
                default: throw new AssertionError("Unhandled saveStatus: " + dependency.saveStatus());
                case TruncatedApplyWithOutcome:
                case TruncatedApply:
                case TruncatedUnapplied:
                    Invariants.require(dependency.executeAt().compareTo(waitingExecuteAt) < 0
                                       || !dependencyId.witnesses(waitingId)
                                       || waitingId.awaitsOnlyDeps()
                                       || waiting.participants().stillWaitsOn().isEmpty()
                                       || !markStaleIfCannotExecute(dependencyId)
                                       || safeStore.redundantBefore().status(dependencyId, null,
                                                 waiting.partialDeps().participants(dependencyId)).all(LOCALLY_DEFUNCT)
                    );
                case Vestigial:
                case Erased:
                    logger.trace("{}: {} is truncated. Stop listening and removing from waiting on commit set.", waitingId, dependencyId);
                    break;
                case Invalidated:
                    logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", waitingId, dependencyId);
            }
            return waitingOn.setAppliedOrInvalidated(dependencyId);
        }
        else if (dependency.executeAt().compareTo(waitingExecuteAt) > 0 && !waitingId.awaitsOnlyDeps())
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
            safeStore.registerListener(dependencySafeCommand, SaveStatus.Applied, waitingId);
            return false;
        }
        else
        {
            Participants<?> participants = waiting.partialDeps().participants(dependency.txnId());
            Participants<?> waitsOn = participants.intersecting(waiting.participants().stillWaitsOn(), Minimal);
            RedundantStatus status = safeStore.redundantBefore().status(dependencyId, waitingExecuteAt, waitsOn);

            if (status.all(LOCALLY_DEFUNCT))
                return false;

            throw illegalState("We have a dependency (" + dependency + ") to wait on, but have already finished waiting (" + waiting + ")");
        }
    }

    static void updateDependencyAndMaybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, SafeCommand predecessor, boolean notifyWaitingOn)
    {
        Command.Committed command = safeCommand.current().asCommitted();
        if (command.hasBeen(Applied))
            return;

        Update waitingOn = new Update(command);
        if (updateWaitingOn(safeStore, command, command.executeAt(), waitingOn, predecessor))
        {
            safeCommand.updateWaitingOn(waitingOn);
            // don't bother invoking maybeExecute if we weren't already blocked on the updated command
            if (waitingOn.hasUpdatedDirectDependency(command.waitingOn()))
                maybeExecute(safeStore, safeCommand, false, notifyWaitingOn);
            else Invariants.require(waitingOn.isWaiting());
        }
        else
        {
            Command pred = predecessor.current();
            if (pred.hasBeen(PreCommitted))
            {
                TxnId nextWaitingOn = command.waitingOn().nextWaitingOn();
                if (nextWaitingOn != null && nextWaitingOn.equals(pred.txnId()) && !pred.hasBeen(PreApplied))
                    safeStore.progressLog().waiting(CanApply, safeStore, predecessor, null, null, null);
            }
        }
    }

    public static void removeWaitingOnKeyAndMaybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, RoutingKey key, long uniqueHlc)
    {
        Command current = safeCommand.current();
        if (current.saveStatus().compareTo(SaveStatus.Applied) >= 0)
            return;

        if (current.saveStatus().compareTo(SaveStatus.Committed) < 0)
        {   // ephemeral reads can be erased without warning
            Invariants.require(current.txnId().is(EphemeralRead));
            return;
        }

        Command.Committed committed = safeCommand.current().asCommitted();

        WaitingOn currentWaitingOn = committed.waitingOn;
        int keyIndex = currentWaitingOn.keys.indexOf(key);
        if (keyIndex < 0 || !currentWaitingOn.isWaitingOnKey(keyIndex))
            return;

        Update waitingOn = new Update(committed);
        waitingOn.removeWaitingOnKey(keyIndex);
        if (uniqueHlc > 0)
            waitingOn.updateUniqueHlc(committed.executeAt(), uniqueHlc);
        safeCommand.updateWaitingOn(waitingOn);
        if (!waitingOn.isWaiting())
            maybeExecute(safeStore, safeCommand, false, true);
    }

    public static void setTruncatedOrVestigial(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
    {
        Command command = safeCommand.current();
        SaveStatus saveStatus = command.saveStatus();
        if (saveStatus.compareTo(TruncatedApply) >= 0) return;
        participants = command.participants().supplementOrMerge(saveStatus, participants);
        Timestamp executeAt = command.executeAtIfKnown();
        if (participants.route() == null || executeAt == null)
        {
            safeCommand.update(safeStore, vestigial(command));
            if (participants.route() != null && !safeStore.coordinateRanges(command.txnId()).contains(participants.route().homeKey()))
                safeStore.progressLog().clear(command.txnId());
        }
        else
        {
            safeCommand.update(safeStore, truncated(command, participants));
            safeStore.progressLog().clear(command.txnId());
        }
    }

    /**
     * Purge all or part of the metadata for a Commmand
     */
    public static Command purge(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, @Nonnull StoreParticipants participants, Cleanup cleanup, boolean notifyListeners)
    {
        return purge(safeStore, safeCommand, command, participants, cleanup, notifyListeners, false);
    }

    public static Command purge(SafeCommandStore safeStore, SafeCommand safeCommand, Cleanup cleanup, boolean notifyListeners)
    {
        return purge(safeStore, safeCommand, cleanup, notifyListeners, false);
    }

    public static Command purge(SafeCommandStore safeStore, SafeCommand safeCommand, Cleanup cleanup, boolean notifyListeners, boolean force)
    {
        Command command = safeCommand.current();
        return purge(safeStore, safeCommand, command, command.participants(), cleanup, notifyListeners, force);
    }

    public static Command purge(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, @Nonnull StoreParticipants participants, Cleanup cleanup, boolean notifyListeners, boolean force)
    {
        Command result = purge(safeStore, command, participants, cleanup, force);
        safeCommand.update(safeStore, result);
        if (notifyListeners)
            safeStore.notifyListeners(safeCommand, command);
        return result;
    }

    public static Command purge(SafeCommandStore safeStore, Command command, Cleanup cleanup)
    {
        return purge(safeStore, command, command.participants(), cleanup, false);
    }

    public static Command purge(SafeCommandStore safeStore, Command command, @Nonnull StoreParticipants participants, Cleanup cleanup, boolean force)
    {
        return purgeInternal(safeStore, command, participants, cleanup, force);
    }

    public static Command purgeUnsafe(CommandStore commandStore, Command command, Cleanup cleanup)
    {
        class Supplier implements RangesForEpochSupplier, RedundantBeforeSupplier
        {
            @Override public CommandStores.RangesForEpoch ranges() { return commandStore.unsafeGetRangesForEpoch(); }
            @Override public RedundantBefore redundantBefore() { return commandStore.unsafeGetRedundantBefore();}
        }
        return purgeInternal(new Supplier(), command, command.participants(), cleanup, false);
    }

    private static <S extends RangesForEpochSupplier & RedundantBeforeSupplier>
    Command purgeInternal(S store, Command command, @Nonnull StoreParticipants participants, Cleanup cleanup, boolean force)
    {
        //   1) a command has been applied; or
        //   2) has been coordinated but *will not* be applied (we just haven't witnessed the invalidation yet); or
        //   3) a command is durably decided and this shard only hosts its home data, so no explicit truncation is necessary to remove it
        //   4) we have tried to udpate the local command and failed because it has been erased remotely, and we do not execute it locally so it doesn't matter to us (this requires the force flag)
        // TODO (desired): consider if there are better invariants we can impose for undecided transactions, to verify they aren't later committed (should be detected already, but more is better)
        // note that our invariant here is imperfectly applied to keep the code cleaner: we don't verify that the caller was safe to invoke if we don't already have a route in the command and we're only PreCommitted

        Invariants.require(validateSafeToCleanup(store.redundantBefore(), command, participants, force), "Command %s could not be purged", command);
        return purge(command, participants, cleanup);
    }

    private static Command purge(Command command, @Nonnull StoreParticipants newParticipants, Cleanup cleanup)
    {
        Command result;
        switch (cleanup)
        {
            default: throw new AssertionError("Unexpected cleanup: " + cleanup);
            case INVALIDATE:
                Invariants.requireArgument(!command.hasBeen(PreCommitted));
                result = invalidated(command, newParticipants);
                break;

            case TRUNCATE_WITH_OUTCOME:
                Invariants.requireArgument(command.known().is(Apply));
                Invariants.requireArgument(command.known().is(ApplyAtKnown));
                result = truncated(command, newParticipants, cleanup.newStatus);
                break;

            case TRUNCATE:
                result = truncated(command, newParticipants);
                break;

            case VESTIGIAL:
                Invariants.require(command.saveStatus().compareTo(SaveStatus.PreApplied) < 0);
                result = vestigial(command, newParticipants);
                break;

            case ERASE:
                Invariants.require(command.saveStatus() != Erased);

            case EXPUNGE:
                result = erased(command, newParticipants);
                break;
        }
        return result;
    }

    private static boolean validateSafeToCleanup(RedundantBefore redundantBefore, Command command, @Nonnull StoreParticipants participants, boolean force)
    {
        if (command.hasBeen(Applied)) return true;
        if (!command.hasBeen(PreCommitted)) return true;
        if (participants.route() == null) return true;   // TODO (expected): tighten this e.g. with && participants.owns.isEmpty()

        TxnId txnId = command.txnId();
        RedundantStatus status = redundantBefore.status(txnId, null, participants.route());
        if (status.any(LOCALLY_APPLIED))
        {
            Invariants.paranoid(participants.stillTouches().isEmpty() ||
                                (participants.stillWaitsOn() != null && participants.stillWaitsOn().isEmpty()));
            return true;
        }

        if (status.all(SHARD_APPLIED, LOCALLY_REDUNDANT) || status.all(LOCALLY_DEFUNCT))
            return true;

        if (force && participants.waitsOn() != null && participants.stillWaitsOn().isEmpty())
            return true;

        return false;
    }

    public static boolean maybeCleanup(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, @Nonnull StoreParticipants newParticipants)
    {
        StoreParticipants cleanupParticipants = newParticipants.filter(LOAD, safeStore, command.txnId(), command.executeAtIfKnown());
        Cleanup cleanup = shouldCleanup(FULL, safeStore, command, cleanupParticipants);
        if (cleanup == NO)
        {
            if (cleanupParticipants == command.participants())
                return false;

            safeCommand.updateParticipants(safeStore, cleanupParticipants);
            return true;
        }

        Invariants.require(cleanup.appliesTo(command.saveStatus()));
        purge(safeStore, safeCommand, command, cleanupParticipants, cleanup, true);
        return true;
    }

    public static Command setDurability(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Durability durability, @Nullable Timestamp executeAt)
    {
        final Command command = safeCommand.current();
        if (command.is(Truncated))
            return command;

        if (command.durability().compareTo(durability) >= 0)
            return command;

        Command updated = supplementParticipants(command, participants);
        participants = updated.participants();
        if (executeAt != null && command.status().hasBeen(Committed) && !command.executeAt().equals(executeAt))
            safeStore.agent().onInconsistentTimestamp(command, command.asCommitted().executeAt(), executeAt);

        if (command.durability().compareTo(durability) < 0)
        {
            updated = updated.updateDurability(durability);
            TxnId txnId = command.txnId();
            if (dependencyElision() == IF_DURABLE && CommandsForKey.manages(txnId))
            {
                PreLoadContext context = PreLoadContext.contextFor(updated.participants().touches(), INCR);
                PreLoadContext execute = safeStore.canExecute(context);
                if (execute != null)
                {
                    setDurable(safeStore, execute, txnId);
                }
                if (execute != context)
                {
                    if (execute != null)
                        context = contextFor(context.keys().without(execute.keys()), INCR);

                    Invariants.require(!context.keys().isEmpty());
                    safeStore = safeStore; // prevent accidental usage inside lambda
                    safeStore.commandStore().execute(context, safeStore0 -> {
                        setDurable(safeStore0, safeStore0.context(), txnId);
                    }, safeStore.commandStore().agent);
                }
            }
        }

        updated = safeCommand.update(safeStore, updated);
        if (maybeCleanup(safeStore, safeCommand, updated, participants))
            updated = safeCommand.current();

        safeStore.notifyListeners(safeCommand, command);
        return updated;
    }

    private static void setDurable(SafeCommandStore safeStore, PreLoadContext context, TxnId txnId)
    {
        for (RoutingKey key : (AbstractUnseekableKeys)context.keys())
            safeStore.get(key).setDurable(txnId);
    }

    static class NotifyWaitingOn implements PreLoadContext, Consumer<SafeCommandStore>
    {
        final TxnId waitingId;
        TxnId loadDepId;

        public NotifyWaitingOn(SafeCommand root)
        {
            Invariants.requireArgument(root.current().hasBeen(Stable));
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
                    if (depSafe == null) // TODO (required): slice to waiting.participants().waitsOn? can simplify method
                        depSafe = initialiseOrRemoveDependency(safeStore, waitingSafe, loadDepId, waiting.partialDeps().participants(loadDepId));
                }
            }

            while (true)
            {
                Command waiting = waitingSafe.current();
                if (waiting.saveStatus().compareTo(Applying) >= 0)
                    return; // nothing to do

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
                                Invariants.require(executed);
                                return;
                        }
                    }

                    depSafe = safeStore.ifLoadedAndInitialised(directlyBlockedOn);
                    if (depSafe == null)
                    {
                        loadDepId = directlyBlockedOn;
                        safeStore.commandStore().execute(this, this, safeStore.agent());
                        return;
                    }
                }
                else
                {
                    Command dep = depSafe.current();
                    SaveStatus depStatus = dep.saveStatus();
                    SaveStatus.LocalExecution depExecution = depStatus.execution;
                    if (!waitingId.awaitsOnlyDeps() && depStatus.known.isExecuteAtKnown() && dep.executeAt().compareTo(waiting.executeAt()) > 0)
                        depExecution = SaveStatus.LocalExecution.Applied;

                    Participants<?> participants = null;
                    if (depExecution.compareTo(WaitingToExecute) < 0 && dep.participants().owns().isEmpty())
                    {
                        // TODO (desired): slightly costly to invert a large partialDeps collection
                        participants = waiting.partialDeps().participants(dep.txnId());
                        Participants<?> stillExecutes = participants.intersecting(waiting.participants().stillExecutes(), Minimal);

                        depSafe = maybeCleanupRedundantDependency(safeStore, waitingSafe, depSafe, stillExecutes);
                        if (depSafe == null)
                            continue;
                    }

                    switch (depExecution)
                    {
                        default: throw new UnhandledEnum(depStatus.execution);
                        case NotReady:
                            if (logger.isTraceEnabled()) logger.trace("{} blocked on {} until ReadyToExclude", waitingId, dep.txnId());
                            safeStore.registerListener(depSafe, HasDecidedExecuteAt.unblockedFrom, waitingId);
                            safeStore.progressLog().waiting(HasDecidedExecuteAt, safeStore, depSafe, null, participants, null);
                            return;

                        case ReadyToExclude:
                        case WaitingToExecute:
                        case ReadyToExecute:
                            safeStore.progressLog().waiting(CanApply, safeStore, depSafe, null, participants, null);

                        case Applying:
                            safeStore.registerListener(depSafe, SaveStatus.Applied, waitingId);
                            return;

                        case WaitingToApply:
                            if (dep.asCommitted().isWaitingOnDependency())
                            {
                                safeStore.registerListener(depSafe, SaveStatus.Applied, waitingId);
                                return;
                            }
                            else
                            {
                                maybeExecute(safeStore, depSafe, false, false);
                                switch (depSafe.current().saveStatus())
                                {
                                    default: throw illegalState("Invalid child status after attempt to execute: " + depSafe.current().saveStatus());
                                    case Applying:
                                        safeStore.registerListener(depSafe, SaveStatus.Applied, waitingId);
                                        return;

                                    case Applied:
                                        // fall-through to outer Applied branch
                                }
                            }

                        case Applied:
                        case CleaningUp:
                            updateDependencyAndMaybeExecute(safeStore, waitingSafe, depSafe, false);
                            waiting = waitingSafe.current();
                            Invariants.require(waiting.saveStatus().compareTo(Applying) >= 0 || !waiting.asCommitted().waitingOn().isWaitingOn(dep.txnId()));
                            depSafe = null;
                    }
                }
            }
        }

        static SafeCommand maybeCleanupRedundantDependency(SafeCommandStore safeStore, SafeCommand waitingSafe, SafeCommand depSafe, Participants<?> executes)
        {
            return maybeCleanupRedundantDependency(safeStore, waitingSafe, depSafe.txnId(), sc -> sc.current().saveStatus(), executes, depSafe);
        }

        static SafeCommand initialiseOrRemoveDependency(SafeCommandStore safeStore, SafeCommand waitingSafe, TxnId depId, Participants<?> executes)
        {
            depId = maybeCleanupRedundantDependency(safeStore, waitingSafe, depId, ignore -> Uninitialised, executes, depId);
            return depId != null ? safeStore.get(depId) : null;
        }

        // executes is not expected to be stillExecutes, i.e. does not need to remove pre-bootstrap, stale or was-owned+redundant
        // (although in the hot path this is the case)
        static <R> R maybeCleanupRedundantDependency(SafeCommandStore safeStore, SafeCommand waitingSafe, TxnId depId, Function<R, SaveStatus> saveStatusGetter, Participants<?> executes, R ifNotRedundant)
        {
            RedundantStatus status = safeStore.redundantBefore().status(depId, null, executes);
            if (status.none(LOCALLY_REDUNDANT))
                return ifNotRedundant;

            if (status.any(LOCALLY_APPLIED))
            {
                // we've been applied or invalidated
                SaveStatus saveStatus = saveStatusGetter.apply(ifNotRedundant);
                Invariants.require(saveStatus.hasBeen(Applied) || !saveStatus.hasBeen(PreCommitted));
                removeRedundantDependencies(safeStore, waitingSafe, depId);
                return null;
            }

            boolean remove = status.all(LOCALLY_REDUNDANT);
            // TODO (required): consider this logic again, incl. whether it is even needed
            if (remove && waitingSafe.txnId().is(ExclusiveSyncPoint) && depId.is(ExclusiveSyncPoint))
                remove = status.all(LOCALLY_SYNCED) || status.all(PRE_BOOTSTRAP_OR_STALE);

            if (!remove)
                return ifNotRedundant;

            if (status.all(WAS_OWNED)) removeNoLongerOwnedDependency(safeStore, waitingSafe, depId);
            else removeRedundantDependencies(safeStore, waitingSafe, depId);
            return null;
        }

        @Override
        public TxnId primaryTxnId()
        {
            return waitingId;
        }

        @Override
        public TxnId additionalTxnId()
        {
            return loadDepId;
        }
    }

    static Command removeRedundantDependencies(SafeCommandStore safeStore, SafeCommand safeCommand, @Nullable TxnId redundant)
    {
        Command.Committed current = safeCommand.current().asCommitted();

        RedundantBefore redundantBefore = safeStore.redundantBefore();
        Update update = new Update(current.waitingOn);
        TxnId minWaitingOnTxnId = update.minWaitingOnTxnId();
        if (minWaitingOnTxnId != null && redundantBefore.hasLocallyRedundantDependencies(update.minWaitingOnTxnId(), current.executeAt(), current.participants().owns()))
            redundantBefore.removeRedundantDependencies(current.participants().owns(), update);

        // if we are a range transaction, being redundant for this transaction does not imply we are redundant for all transactions
        if (redundant != null)
            update.removeWaitingOn(redundant);
        return safeCommand.updateWaitingOn(update);
    }

    static Command removeNoLongerOwnedDependency(SafeCommandStore safeStore, SafeCommand safeCommand, @Nonnull TxnId wasOwned)
    {
        Command.Committed current = safeCommand.current().asCommitted();
        if (!current.waitingOn.isWaitingOn(wasOwned))
            return current;

        Update update = new Update(current.waitingOn);
        update.removeWaitingOn(wasOwned);
        return safeCommand.updateWaitingOn(update);
    }

    public static Command supplementParticipants(Command command, StoreParticipants participants)
    {
        StoreParticipants curParticipants = command.participants();
        participants = curParticipants.supplementOrMerge(command.saveStatus(), participants);
        if (curParticipants == participants)
            return command;

        return command.updateParticipants(participants);
    }

    public static Command supplementParticipants(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants)
    {
        Command current = safeCommand.current();
        return safeCommand.updateParticipants(safeStore, current.participants().supplementOrMerge(current.saveStatus(), participants));
    }

    public static Command updateRoute(SafeCommandStore safeStore, SafeCommand safeCommand, Route<?> route)
    {
        Command current = safeCommand.current();
        if (current.hasBeen(Truncated))
            return current;

        StoreParticipants updated = current.participants().supplement(route);
        if (current.participants() == updated)
            return current;

        return safeCommand.updateParticipants(safeStore, updated);
    }

    private static PartialTxn prepareTxn(SaveStatus newSaveStatus, StoreParticipants newParticipants, Command upd, @Nullable Txn txn)
    {
        PartialTxn cur = upd.partialTxn();
        if (!newSaveStatus.known.definition().isKnown())
            return cur;

        if (cur != null)
            cur = cur.intersecting(newParticipants.owns(), true);

        if (txn == null)
            return cur;

        if (cur != null && cur.covers(newParticipants.stillOwns()))
            return cur;

        PartialTxn add = txn.intersecting(newParticipants.stillOwns(), true);
        return cur == null ? add : cur.with(add);
    }

    private static StoreParticipants prepareParticipants(Validated validated, StoreParticipants newParticipants, Command upd)
    {
        if (validated != UPDATE_TXN_KEEP_DEPS)
            return newParticipants.supplement(upd.participants());

        // unsafe to update participants.touches() without updating deps, as we expect them to cover the same keys and ranges
        StoreParticipants cur = upd.participants();
        if ((newParticipants.executes() != null && cur.executes() == null) || (!cur.hasFullRoute() && newParticipants.hasFullRoute()))
        {
            StoreParticipants result = cur;
            if (newParticipants.waitsOn() != null)
                result = result.withExecutes(newParticipants.executes(), newParticipants.stillExecutes(), newParticipants.waitsOn(), newParticipants.stillWaitsOn());
            if (!cur.hasFullRoute() && newParticipants.hasFullRoute())
                result = result.supplement(newParticipants.route());
            return result;
        }
        return cur.supplement(newParticipants);
    }

    private static PartialDeps prepareDeps(Validated validated, StoreParticipants participants, Command upd, Deps newDeps)
    {
        switch (validated)
        {
            default: throw new UnhandledEnum(validated);
            case UPDATE_TXN_KEEP_DEPS:
                return upd.partialDeps();
            case UPDATE_TXN_MERGE_DEPS:
                return upd.partialDeps().with(newDeps.intersecting(participants.stillTouches()));
            case UPDATE_TXN_AND_DEPS:
                return prepareNewDeps(participants, newDeps);
            case UPDATE_TXN_AND_DEPS_INTERSECT_STABLE:
                return prepareNewDeps(participants, newDeps).intersectStable(upd.partialDeps(), upd.txnId());
            case UPDATE_TXN_IGNORE_DEPS:
                return null;
        }
    }

    private static PartialDeps prepareNewDeps(StoreParticipants participants, Deps newDeps)
    {
        if (newDeps != null)
            return newDeps.intersecting(participants.stillTouches());

        Invariants.require(participants.stillTouches().isEmpty());
        return PartialDeps.NONE;
    }

    enum Validated { INSUFFICIENT, UPDATE_TXN_IGNORE_DEPS, UPDATE_TXN_KEEP_DEPS, UPDATE_TXN_MERGE_DEPS, UPDATE_TXN_AND_DEPS_INTERSECT_STABLE, UPDATE_TXN_AND_DEPS }

    private static Validated validate(@Nullable Ballot ballot, SaveStatus newStatus, Command cur, StoreParticipants participants,
                                      Route<?> addRoute, @Nullable Txn addPartialTxn, @Nullable Deps partialDeps)
    {
        return validate(ballot, newStatus, cur, participants, addRoute, addPartialTxn, partialDeps, null, null);
    }

    private static Validated validate(@Nullable Ballot ballot, SaveStatus newStatus, Command cur, StoreParticipants participants,
                                      Route<?> addRoute, @Nullable Txn addPartialTxn, @Nullable Deps partialDeps,
                                      @Nullable Commit.Kind commitKind, @Nullable Timestamp executeAt)
    {
        Known haveKnown = cur.known();
        Known expectKnown = newStatus.known;

        Invariants.require(addRoute == participants.route());
        if (expectKnown.has(FullRoute) && !isFullRoute(cur.route()) && !isFullRoute(addRoute))
            return INSUFFICIENT;

        if (expectKnown.definition().isKnown())
        {
            if (cur.txnId().isSystemTxn())
            {
                if (cur.partialTxn() == null && addPartialTxn == null)
                    return INSUFFICIENT;
            }
            else if (haveKnown.definition().isKnown())
            {
                // TODO (desired): avoid converting to participants before subtracting
                Participants<?> extraScope = participants.stillOwns();
                PartialTxn partialTxn = cur.partialTxn();
                if (partialTxn != null)
                    extraScope = extraScope.without(partialTxn.keys().toParticipants());
                if (!containsAll(addPartialTxn, extraScope))
                    return INSUFFICIENT;
            }
            else
            {
                if (!containsAll(addPartialTxn, participants.stillOwns()))
                    return INSUFFICIENT;
            }
        }

        if (!expectKnown.hasAnyDeps())
            return UPDATE_TXN_IGNORE_DEPS;

        if (commitKind == StableMediumPath)
        {
            if (haveKnown.is(DepsProposedFixed) && expectKnown.is(DepsKnown) && ballot != null && ballot.equals(Ballot.ZERO) && participants.stillTouches().equals(cur.participants().touches()))
                return UPDATE_TXN_MERGE_DEPS;
            return INSUFFICIENT;
        }

        if (haveKnown.is(DepsKnown) || (haveKnown.equalDeps(expectKnown) && (ballot == null || ballot.equals(cur.acceptedOrCommitted()))))
            return UPDATE_TXN_KEEP_DEPS;

        if (!containsAll(partialDeps, participants.stillTouches()))
            return INSUFFICIENT;

        if (executeAt != null && expectKnown.is(DepsKnown) && haveKnown.compareTo(DepsFromCoordinator) > 0 && executeAt.equals(cur.txnId()) && !cur.acceptedOrCommitted().equals(Ballot.ZERO))
            return UPDATE_TXN_AND_DEPS_INTERSECT_STABLE;

        return UPDATE_TXN_AND_DEPS;
    }

    private static boolean containsAll(Txn adding, Participants<?> required)
    {
        return adding == null ? required.isEmpty() : adding.covers(required);
    }

    private static <V> boolean containsAll(Deps adding, Participants<?> required)
    {
        return adding == null ? required.isEmpty() : adding.covers(required);
    }
}
