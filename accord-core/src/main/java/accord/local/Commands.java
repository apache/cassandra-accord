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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.Result;
import accord.api.Result.PersistableResult;
import accord.api.RoutingKey;
import accord.api.ViolationHandler.ViolationHandlerHolder;
import accord.local.Command.WaitingOn;
import accord.local.Command.WaitingOn.Update;
import accord.local.CommandStores.RangesForEpochSupplier;
import accord.local.MinimalCommand.MinimalWithConcreteDeps;
import accord.local.RedundantBefore.Bounds;
import accord.local.RedundantBefore.RedundantBeforeSupplier;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Known;
import accord.primitives.Known.KnownExecuteAt;
import accord.primitives.Known.KnownRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.SaveStatus.LocalExecution;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TimestampWithUniqueHlc;
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
import static accord.api.ProtocolModifiers.*;
import static accord.api.ProtocolModifiers.DependencyElision.IF_DURABLY_COMMITTED;
import static accord.api.ProtocolModifiers.DependencyElision.IF_DURABLY_PREAPPLIED;
import static accord.api.ProtocolModifiers.DependencyElision.OFF;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Cleanup.NO;
import static accord.local.Cleanup.shouldCleanup;
import static accord.local.Command.Truncated.erased;
import static accord.local.Command.Truncated.invalidated;
import static accord.local.Command.Truncated.truncated;
import static accord.local.Command.Truncated.vestigial;
import static accord.local.Commands.Validated.INSUFFICIENT;
import static accord.local.Commands.Validated.INSUFFICIENT_EPOCHS;
import static accord.local.Commands.Validated.UPDATE_TXN_AND_DEPS_INTERSECT_STABLE;
import static accord.local.Commands.Validated.UPDATE_TXN_IGNORE_DEPS;
import static accord.local.Commands.Validated.UPDATE_TXN_KEEP_DEPS;
import static accord.local.Commands.Validated.UPDATE_TXN_AND_DEPS;
import static accord.local.Commands.Validated.UPDATE_TXN_MERGE_DEPS;
import static accord.local.ExecutionContext.unsequencedIdempotentIncrementalWrite;
import static accord.local.LoadKeys.ASYNC;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DEFUNCT;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.UNREADY;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.RedundantStatus.Property.WAS_OWNED;
import static accord.local.StoreParticipants.Filter.LOAD;
import static accord.local.StoreParticipants.Filter.UPDATE;
import static accord.messages.Commit.Kind.StableFastPath;
import static accord.messages.Commit.Kind.StableMediumPath;
import static accord.primitives.Known.KnownDeps.DepsFromCoordinator;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsProposed;
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
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.SaveStatus.TruncatedApply;
import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Committed;
import static accord.primitives.Status.Durability;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.NotDefined;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Route.isFullRoute;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.nonNull;

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
        int compareBallots = command.promised().compareTo(ballot);
        if (command.hasBeen(Truncated) || compareBallots > 0)
        {
            AcceptOutcome outcome = !command.hasBeen(Truncated)
                                        ? AcceptOutcome.RejectedBallot
                                        : command.is(Invalidated)
                                           ? AcceptOutcome.RejectedBallot
                                           : participants.owns().isEmpty()
                                             ? AcceptOutcome.Retired : AcceptOutcome.Truncated;

            logger.trace("{}: skipping preaccept - {}", txnId, outcome);
            safeStore.agent().replicaEvents().onRejectPreAccept(safeStore, command, outcome);
            return outcome;
        }

        if (command.known().definition().isKnown())
        {
            Invariants.require(command.status() == Invalidated || command.executeAt() != null);
            // in case of Ballot.ZERO, we must either have a competing recovery coordinator or have late delivery of the
            // preaccept; in the former case we should abandon coordination, and in the latter we have already completed
            AcceptOutcome outcome;
            if (ballot.equals(Ballot.ZERO))
            {
                return AcceptOutcome.Redundant;
            }
            else
            {
                safeCommand.updatePromised(ballot);
                outcome = AcceptOutcome.Success;
            }
            logger.trace("{}: skipping preaccept - {}", txnId, outcome);
            safeStore.agent().replicaEvents().onRejectPreAccept(safeStore, command, outcome);
            return outcome;
        }

        if (command.known().deps().hasProposedOrDecidedDeps()) participants = command.participants().supplement(participants);
        else participants = participants.filter(UPDATE, safeStore, txnId, command.executeAtIfKnown()); // executeAt may be known if PreCommitted (without Definition)

        Validated validated = validate(safeStore, txnId, ballot, newSaveStatus, command, participants, participants.route(), txn, deps);
        Invariants.require(validated.compareTo(INSUFFICIENT) > 0);

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

        safeStore.agent().replicaEvents().onPreAccepted(safeStore, command);
        safeStore.notifyListeners(safeCommand, command);
        return AcceptOutcome.Success;
    }

    public static boolean preacceptInvalidate(SafeCommandStore safeStore, SafeCommand safeCommand, Ballot ballot)
    {
        Command command = safeCommand.current();

        if (command.hasBeen(Status.Committed) || command.promised().compareTo(ballot) > 0)
        {
            AcceptOutcome outcome = command.hasBeen(Committed) ? AcceptOutcome.Redundant : AcceptOutcome.RejectedBallot;
            logger.trace("{}: skipping preacceptInvalidate - {}", command.txnId(), outcome);
            safeStore.agent().replicaEvents().onRejectPreNotAccept(safeStore, command, outcome);
            return false;
        }

        safeStore.agent().replicaEvents().onPreNotAccepted(safeStore, command);
        safeCommand.updatePromised(ballot);
        return true;
    }

    private static AcceptOutcome maybeRejectAccept(Ballot ballot, Timestamp executeAt, Command command, boolean isNotAccept)
    {
        Status status = command.status();
        int compareStatus = status.compareTo(PreCommitted);
        if (compareStatus >= 0 && (compareStatus > 0 || isNotAccept))
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
            AcceptOutcome reject = maybeRejectAccept(ballot, executeAt, command, false);
            if (reject != null)
            {
                safeStore.agent().replicaEvents().onRejectAccept(safeStore, command, reject);
                return reject;
            }
        }

        SaveStatus newSaveStatus = SaveStatus.get(kind == Accept.Kind.MEDIUM ? Status.AcceptedMedium : Status.AcceptedSlow, command.known());
        participants = participants.filter(UPDATE, safeStore, txnId, null);
        Validated validated = validate(safeStore, txnId, ballot, newSaveStatus, command, participants, route, null, deps);
        Invariants.require(validated.compareTo(INSUFFICIENT) > 0);

        PartialTxn partialTxn = prepareTxn(newSaveStatus, participants, command, null);
        PartialDeps partialDeps = prepareDeps(validated, participants, command, deps);
        participants = prepareParticipants(validated, participants, command);

        Command accepted = safeCommand.accept(safeStore, txnId, newSaveStatus, participants, ballot, executeAt, partialTxn, partialDeps, ballot);
        safeStore.agent().replicaEvents().onAccepted(safeStore, accepted);
        safeStore.notifyListeners(safeCommand, command);

        return AcceptOutcome.Success;
    }

    public static AcceptOutcome notAccept(SafeCommandStore safeStore, SafeCommand safeCommand, Status status, Ballot ballot)
    {
        final Command command = safeCommand.current();
        {
            AcceptOutcome reject = maybeRejectAccept(ballot, null, command, true);
            if (reject != null)
            {
                safeStore.agent().replicaEvents().onRejectNotAccept(safeStore, command, reject);
                return reject;
            }
        }

        logger.trace("{}: not accepted ({})", command.txnId(), status);
        Command notAccepted = safeCommand.notAccept(safeStore, status, ballot);
        safeStore.agent().replicaEvents().onNotAccepted(safeStore, notAccepted);
        safeStore.notifyListeners(safeCommand, command);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Rejected, Redundant, Insufficient, InsufficientEpochs }


    // relies on mutual exclusion for each key
    public static CommitOutcome commit(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, SaveStatus newSaveStatus, Ballot ballot, TxnId txnId, Route<?> route, @Nullable Txn txn, Timestamp executeAt, Deps deps, @Nullable Commit.Kind kind)
    {
        final Command command = safeCommand.current();
        if (kind == StableFastPath && !command.promised().equals(Ballot.ZERO))
        {
            safeStore.agent().replicaEvents().onRejectCommitOrStable(safeStore, newSaveStatus, command, CommitOutcome.Rejected);
            return CommitOutcome.Rejected;
        }

        SaveStatus curStatus = command.saveStatus();
        Invariants.requireArgument(newSaveStatus == SaveStatus.Committed || newSaveStatus == SaveStatus.Stable);
        if (newSaveStatus == SaveStatus.Committed && ballot.compareTo(command.promised()) < 0)
        {
            CommitOutcome outcome = curStatus.is(Truncated) || participants.owns().isEmpty()
                                    ? CommitOutcome.Redundant : CommitOutcome.Rejected;
            safeStore.agent().replicaEvents().onRejectCommitOrStable(safeStore, newSaveStatus, command, outcome);
            return outcome;
        }

        if (curStatus.hasBeen(PreCommitted))
        {
            if (!curStatus.is(Truncated))
            {
                if (!executeAt.equals(command.executeAt()) || curStatus.status == Invalidated)
                    ViolationHandlerHolder.get().onTimestampViolation(safeStore, command, participants, executeAt);
            }

            if (curStatus.compareTo(newSaveStatus) > 0 || curStatus.hasBeen(Stable))
            {
                logger.trace("{}: skipping commit - already newer or stable ({})", txnId, command.status());
                safeStore.agent().replicaEvents().onRejectCommitOrStable(safeStore, newSaveStatus, command, CommitOutcome.Redundant);
                return CommitOutcome.Redundant;
            }

            if (curStatus == SaveStatus.Committed && newSaveStatus == SaveStatus.Committed)
            {
                if (ballot.equals(command.acceptedOrCommitted()))
                {
                    safeStore.agent().replicaEvents().onRejectCommitOrStable(safeStore, newSaveStatus, command, CommitOutcome.Redundant);
                    return CommitOutcome.Redundant;
                }

                Invariants.require(ballot.compareTo(command.acceptedOrCommitted()) > 0);
            }
        }

        participants = participants.filter(UPDATE, safeStore, txnId, executeAt);
        Validated validated = validate(safeStore, txnId, ballot, newSaveStatus, command, participants, route, txn, deps, kind, executeAt);
        if (validated.compareTo(INSUFFICIENT) <= 0)
        {
            CommitOutcome outcome = validated == INSUFFICIENT ? CommitOutcome.Insufficient : CommitOutcome.InsufficientEpochs;
            safeStore.agent().replicaEvents().onRejectCommitOrStable(safeStore, newSaveStatus, command, outcome);
            return outcome;
        }

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
            safeStore.agent().replicaEvents().onStable(safeStore, committed);
            maybeExecute(safeStore, safeCommand, true, true);
        }
        else
        {
            Invariants.requireArgument(command.acceptedOrCommitted().compareTo(ballot) <= 0);
            committed = safeCommand.commit(safeStore, participants, ballot, executeAt, partialTxn, partialDeps);
            safeStore.agent().replicaEvents().onCommitted(safeStore, committed);
            safeStore.notifyListeners(safeCommand, committed);
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

                ViolationHandlerHolder.get().onTimestampViolation(safeStore, command, participants, executeAt);
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
        participants = participants.filter(UPDATE, safeStore, txnId, txnId);
        Validated validated = validate(safeStore, txnId, null, SaveStatus.Stable, command, participants, route, txn, deps);
        Invariants.require(validated.compareTo(INSUFFICIENT) > 0);

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

        safeCommand.set(erased(txnId));
    }

    public static void commitInvalidate(SafeCommandStore safeStore, SafeCommand safeCommand, Participants<?> scope)
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
                    ViolationHandlerHolder.get().onTimestampViolation(safeStore, command, scope, null, Timestamp.NONE);
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

    public enum ApplyOutcome
    {
        Success,

        Redundant,

        Insufficient,

        InsufficientEpochs,

        /**
         * A command store apply was successful, but a recovery coordinator with a newer ballot had begun beforehand,
         * so we cannot safely count this towards durability for pruning transitive CommandsForKey, else this in-flight
         * recovery may reach an incorrect recovery decision by witnessing a superseding transaction without this transaction
         * as a dependency.
         *
         * NOTE: When merged with other ApplyOutcome into an ApplyReply, this no longer implies success, as it may
         * override an Insufficient or InsufficientEpochs reply.
         */
        RaceWithRecovery
    }

    public static ApplyOutcome apply(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Ballot ballot, TxnId txnId, Route<?> route, Timestamp executeAt, @Nullable Deps deps, @Nullable Txn txn, Writes writes, PersistableResult result)
    {
        return apply(SaveStatus.PreApplied, safeStore, safeCommand, participants, ballot, txnId, route, executeAt, deps, txn, writes, result);
    }

    public static ApplyOutcome apply(SaveStatus newSaveStatus, SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Ballot ballot, TxnId txnId, Route<?> route, Timestamp executeAt, @Nullable Deps deps, @Nullable Txn txn, Writes writes, PersistableResult result)
    {
        Invariants.require(newSaveStatus == SaveStatus.PreApplied || newSaveStatus == Applying || newSaveStatus == SaveStatus.Applied);
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
                ViolationHandlerHolder.get().onTimestampViolation(safeStore, command, participants, executeAt);
            return ApplyOutcome.Redundant;
        }
        else if (command.hasBeen(PreCommitted) && !executeAt.equals(command.executeAt()))
        {
            if (command.is(Truncated) && command.executeAt() == null)
                return ApplyOutcome.Redundant;
            ViolationHandlerHolder.get().onTimestampViolation(safeStore, command, participants, executeAt);
        }

        participants = participants.filter(UPDATE, safeStore, txnId, executeAt);
        Validated validated = validate(safeStore, txnId, Ballot.ZERO, SaveStatus.PreApplied, command, participants, route, txn, deps, null, executeAt);
        if (validated.compareTo(INSUFFICIENT) <= 0)
            return validated == INSUFFICIENT ? ApplyOutcome.Insufficient : ApplyOutcome.InsufficientEpochs;

        PartialTxn partialTxn = prepareTxn(SaveStatus.PreApplied, participants, command, txn);
        PartialDeps partialDeps = prepareDeps(validated, participants, command, deps);
        participants = prepareParticipants(validated, participants, command);

        // TODO (required): validate safe to fast apply against local state if running burn test
        // note: we may overwrite minUniqueHlc here
        WaitingOn waitingOn = newSaveStatus != SaveStatus.PreApplied
                              ? WaitingOn.none(txnId.domain(), partialDeps)
                              : command.hasBeen(Stable)
                                ? nonNull(command.asCommitted().waitingOn())
                                : initialiseWaitingOn(safeStore, txnId,  executeAt, participants, partialDeps);

        Ballot promised = command.promised();
        if (promised.compareTo(ballot) <= 0)
            promised = ballot;

        if (newSaveStatus == SaveStatus.PreApplied && !waitingOn.isWaiting())
            newSaveStatus = Applying;
        if (newSaveStatus == Applying && (!txnId.is(Write) || writes == null || !writes.keys.intersects(participants.stillExecutes())))
            newSaveStatus = SaveStatus.Applied;

        switch (newSaveStatus)
        {
            default: throw UnhandledEnum.invalid(newSaveStatus);
            case PreApplied:
            {
                Command.Executed executed = safeCommand.preapplied(safeStore, participants, ballot, executeAt, partialTxn, partialDeps, waitingOn, writes, result);
                logger.trace("{}: preapplied", executed.txnId());
                // must signal preapplied first, else we may be applied (and have cleared progress log state) already before maybeExecute exits
                safeStore.agent().replicaEvents().onPreApplied(safeStore, executed);
                maybeExecute(safeStore, safeCommand, true, true);
                break;
            }
            case Applying:
            {
                Invariants.require(!waitingOn.isWaiting());
                Command.Executed executed = safeCommand.applying(safeStore, participants, executeAt, partialTxn, partialDeps, waitingOn, writes, result);
                safeStore.agent().replicaEvents().onPreApplied(safeStore, executed);
                safeStore.notifyListeners(safeCommand, command);
                logger.trace("{}: applying", executed.txnId());
                applyChain(safeStore, executed).begin(safeStore.agent());
                break;
            }
            case Applied:
            {
                Command.Executed executed = safeCommand.applied(safeStore, participants, executeAt, partialTxn, partialDeps, waitingOn, writes, result);
                safeStore.agent().replicaEvents().onPreApplied(safeStore, executed);
                safeStore.agent().replicaEvents().onApplied(safeStore, executed);
                safeStore.notifyListeners(safeCommand, command);
                break;
            }
        }
        if (logger.isTraceEnabled())
            logger.trace("{}: apply, status set to {} with executeAt: {}, deps: {}", txnId, newSaveStatus, executeAt, partialDeps);

        return promised == ballot ? ApplyOutcome.Success : ApplyOutcome.RaceWithRecovery;
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

    public static void postApply(SafeCommandStore safeStore, TxnId txnId, boolean forceApply)
    {
        SafeCommand safeCommand = safeStore.get(txnId);
        Command command = safeCommand.current();
        logger.trace("{} applied, setting status to Applied and notifying listeners", command);
        if (command.hasBeen(Applied) && !forceApply)
            return;

        Command applied = safeCommand.applied(safeStore, forceApply);
        safeStore.agent().replicaEvents().onApplied(safeStore, applied);
        safeStore.notifyListeners(safeCommand, command);
    }

    private static class PostApply<V> extends AsyncChains.FlatMapLink<V, Void> implements Consumer<SafeCommandStore>, ExecutionContext
    {
        final CommandStore commandStore;
        final TxnId txnId;
        final Participants<?> participants;
        final boolean force;

        protected PostApply(Head<?> head, CommandStore commandStore, TxnId txnId, Participants<?> participants, boolean force)
        {
            super(head);
            this.commandStore = commandStore;
            this.txnId = txnId;
            this.participants = participants;
            this.force = force;
        }

        @Override
        public AsyncChain<Void> apply(V v)
        {
            return commandStore.continuationChain(this, this);
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            postApply(safeStore, txnId, force);
        }

        @Override public TxnId primaryTxnId() { return txnId; }
        @Override public Unseekables<?> keys() { return participants; }
        @Override public LoadKeys loadKeys() { return ASYNC; }
        @Override public String reason() { return "Post Apply"; }
        @Override public ExecutionKind executionKind() { return ExecutionKind.APPLY; }
    }

    private static class PostFastApply<V> extends AsyncChains.FlatMapLink<V, Void> implements Consumer<SafeCommandStore>, ExecutionContext
    {
        final CommandStore commandStore;
        final TxnId txnId;
        final Participants<?> participants;
        final Timestamp applyAt;
        final Writes writes;
        final PersistableResult result;
        final boolean force;

        protected PostFastApply(Head<?> head, CommandStore commandStore, TxnId txnId, Participants<?> participants, Timestamp applyAt, Writes writes, PersistableResult result, boolean force)
        {
            super(head);
            this.commandStore = commandStore;
            this.txnId = txnId;
            this.participants = participants;
            this.applyAt = applyAt;
            this.writes = writes;
            this.result = result;
            this.force = force;
        }

        @Override
        public AsyncChain<Void> apply(V v)
        {
            return commandStore.continuationChain(this, this);
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            SafeCommand safeCommand = safeStore.get(txnId);
            Command command = safeCommand.current();
            logger.trace("{} applied, setting status to Applied and notifying listeners", command);
            if (command.hasBeen(Applied) && !force)
                return;

            if (!command.hasBeen(PreApplied))
                safeCommand.preapplied(safeStore, applyAt, writes, result);

            Command applied = safeCommand.applied(safeStore, force);
            safeStore.agent().replicaEvents().onApplied(safeStore, applied);
            safeStore.notifyListeners(safeCommand, command);
        }

        @Override public TxnId primaryTxnId() { return txnId; }
        @Override public Unseekables<?> keys() { return participants; }
        @Override public LoadKeys loadKeys() { return ASYNC; }
        @Override public String reason() { return "Post Apply"; }
        @Override public ExecutionKind executionKind() { return ExecutionKind.APPLY; }
    }

    public static AsyncChain<Void> applyChain(SafeCommandStore safeStore, Command.Executed command)
    {
        // TODO (required): make sure we are correctly handling (esp. C* side with validation logic) executing a transaction
        //  that was pre-bootstrap for some range (so redundant and we may have gone ahead of), but had to be executed locally
        //  for another range
        CommandStore unsafeStore = safeStore.commandStore();
        TxnId txnId = command.txnId();
        //noinspection DataFlowIssue
        safeStore = safeStore; // disable reuse
        Participants<?> executes = command.participants().stillExecutes(); // including any keys we aren't writing
        if (executes.isEmpty())
        {
            postApply(safeStore, txnId, false);
            return AsyncChains.success(null);
        }
        else
        {
            return command.writes()
                          .apply(safeStore, executes, command.partialTxn())
                          .then(head -> new PostApply<>(head, unsafeStore, txnId, executes, false));
        }
    }

    public static boolean maybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        return maybeExecute(safeStore, safeCommand, safeCommand.current(), alwaysNotifyListeners, notifyWaitingOn, MaybeExecuteAdapter.DEFAULT);
    }

    public static boolean maybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        return maybeExecute(safeStore, safeCommand, command, alwaysNotifyListeners, notifyWaitingOn, MaybeExecuteAdapter.DEFAULT);
    }

    public interface MaybeExecuteAdapter
    {
        MaybeExecuteAdapter DEFAULT = new MaybeExecuteAdapter()
        {
            @Override
            public void notifyWaiting(SafeCommandStore safeStore, SafeCommand waiting)
            {
                new NotifyWaitingOn(waiting).start(safeStore);
            }

            @Override
            public void notWaiting(SafeCommandStore safeStore)
            {
            }
        };

        void notifyWaiting(SafeCommandStore safeStore, SafeCommand safeCommand);
        void notWaiting(SafeCommandStore safeStore);
    }

    // TODO (desired): merge notifyListeners and notifyWaitingOn into MaybeExecuteAdapter without allocation/megamorphic despatch penalty
    public static boolean maybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, boolean alwaysNotifyListeners, boolean notifyWaitingOn, MaybeExecuteAdapter adapter)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: Maybe executing with status {}. Will notify listeners on noop: {}",
                         command.txnId(), command.status(), alwaysNotifyListeners);

        SaveStatus saveStatus = command.saveStatus();
        if (saveStatus != SaveStatus.Stable && saveStatus != SaveStatus.PreApplied)
        {
            if (alwaysNotifyListeners)
                safeStore.notifyListeners(safeCommand, command);
            adapter.notWaiting(safeStore);
            return false;
        }

        WaitingOn waitingOn = command.waitingOn();
        if (waitingOn.isWaiting())
        {
            if (!removeRedundantDependencies(safeStore, safeCommand) || (waitingOn = safeCommand.current().waitingOn()).isWaiting())
            {
                if (alwaysNotifyListeners)
                    safeStore.notifyListeners(safeCommand, command);

                if (notifyWaitingOn && waitingOn.isWaitingOnCommand())
                    adapter.notifyWaiting(safeStore, safeCommand);
                else
                    adapter.notWaiting(safeStore);

                return false;
            }
        }

        TxnId txnId = command.txnId();
        switch (saveStatus)
        {
            default: throw UnhandledEnum.invalid(command.status());
            case Stable:
                if (executeAtReplica(txnId, command.partialTxn())
                    && !command.participants().executes().isEmpty()
                    && safeStore.safeToReadAt(command.executeAt()).containsAll(command.participants().executes())
                    && safeStore.commandStore().node().topology().epoch() >= command.executeAt.epoch()
                )
                {
                    if (null == (command = replicaExecute(safeStore, safeCommand, command, txnId)))
                        break;
                    // else fall-through
                }
                else
                {
                    // TODO (expected): maintain distinct ReadyToRead and ReadyToWrite states
                    safeCommand.readyToExecute(safeStore);
                    logger.trace("{}: set to ReadyToExecute", txnId);
                    safeStore.notifyListeners(safeCommand, command);
                    break;
                }

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
                    // apply immediately, as no transaction should take a local dependency on this transaction.
                    // This handles both transactions whose ownership is lost, as well as those that become pre-bootstrap or stale
                    logger.trace("{}: applying no-op", txnId);
                    safeCommand.applied(safeStore);
                    safeStore.notifyListeners(safeCommand, command);
                }
        }

        adapter.notWaiting(safeStore);
        return true;
    }

    private static Command replicaExecute(SafeCommandStore safeStore, SafeCommand safeCommand, Command command, TxnId txnId)
    {
        FullRoute<?> route = (FullRoute<?>) command.route();
        PartialTxn txn = command.partialTxn();
        txn.reconstitute(route);
        Timestamp applyAt;
        {
            Timestamp executeAt = command.executeAt();
            long minUniqueHlc = command.waitingOn().minUniqueHlc();
            if (!txnId.is(Txn.Kind.Write) || minUniqueHlc == 0) applyAt = executeAt;
            else applyAt = new TimestampWithUniqueHlc(executeAt, minUniqueHlc);
        }

        Ballot ballot = command.acceptedOrCommitted();
        if (!txn.read().keys().isEmpty())
        {
            Participants<?> executes = command.participants().executes();
            CommandStore unsafeStore = safeStore.commandStore();
            safeCommand.readyToExecute(safeStore);
            safeStore = safeStore;
            long stamp = safeStore.commandStore().node().currentStamp();
            txn.read(safeStore, applyAt, executes).begin((data, fail) -> {
                if (fail != null)
                {
                    unsafeStore.agent().onException(fail);
                    notifyAfterFailedFastApply(unsafeStore, txnId);
                }
                else
                {
                    if (!fastWritesMayBypassSafeStore())
                        replicaExecuteSlowApply(unsafeStore, ballot, txnId, route, txn, data, applyAt, stamp);
                    else if (stamp == unsafeStore.node.currentStamp() || unsafeStore.unsafeGetSafeToRead().lowerEntry(applyAt).getValue().containsAll(executes))
                        replicaExecuteFastApply(unsafeStore, ballot, txnId, route, txn, data, applyAt, executes);
                    else
                        notifyAfterFailedFastApply(unsafeStore, txnId);
                }
            });
            return null;
        }

        Writes writes = txn.execute(txnId, applyAt, null);
        Result result = txn.result(txnId, applyAt, null);

        safeStore.commandStore().node.reportLocalExecution(txnId, route, ballot, applyAt, writes, result);
        command = safeCommand.preapplied(safeStore, applyAt, writes, result.toPersistable());
        return command;
    }

    private static void replicaExecuteFastApply(CommandStore unsafeStore, Ballot ballot, TxnId txnId, Route<?> route, PartialTxn txn, Data data, Timestamp applyAt, Participants<?> executes)
    {
        Writes writes = txn.execute(txnId, applyAt, data);
        PersistableResult persistResult;
        {
            Result result = txn.result(txnId, applyAt, data);
            unsafeStore.node.reportLocalExecution(txnId, route, ballot, applyAt, writes, result);
            persistResult = result.toPersistable();
        }
        writes.applyDirect(unsafeStore, executes, txn)
              .then(head -> new PostFastApply<>(head, unsafeStore, txnId, executes, applyAt, writes, persistResult, false))
              .begin(unsafeStore.agent);
    }

    private static void replicaExecuteSlowApply(CommandStore unsafeStore, Ballot ballot, TxnId txnId, Route<?> route, PartialTxn txn, Data data, Timestamp applyAt, long stamp)
    {
        unsafeStore.execute(ExecutionContext.unsequenced(txnId, "Replica Apply"), safeStore -> {
            SafeCommand safeCommand = safeStore.unsafeGet(txnId);
            Command command = safeCommand.current();
            if (stamp != unsafeStore.node.currentStamp() && !safeStore.safeToReadAt(applyAt).containsAll(command.route()))
            {
                notifyAfterFailedFastApply(safeStore, txnId);
            }
            else
            {
                Writes writes = txn.execute(txnId, applyAt, data);
                PersistableResult persistResult;
                {
                    Result result = txn.result(txnId, applyAt, data);
                    unsafeStore.node.reportLocalExecution(txnId, route, ballot, applyAt, writes, result);
                    persistResult = result.toPersistable();
                }
                if (command.saveStatus.compareTo(SaveStatus.PreApplied) <= 0)
                    apply(Applying, safeStore, safeCommand, command.participants, command.acceptedOrCommitted(), txnId, command.route(), applyAt, command.partialDeps(), command.partialTxn(), writes, persistResult);
            }
        });
    }

    private static void notifyAfterFailedFastApply(CommandStore unsafeStore, TxnId txnId)
    {
        unsafeStore.execute(ExecutionContext.unsequenced(txnId, "Mark ReadyToExecute after failure to fast apply"), safeStore -> {
            notifyAfterFailedFastApply(safeStore, txnId);
        }, unsafeStore.agent());
    }

    private static void notifyAfterFailedFastApply(SafeCommandStore safeStore, TxnId txnId)
    {
        SafeCommand safeCommand = safeStore.get(txnId);
        Command command = safeCommand.current();
        if (command.saveStatus().compareTo(ReadyToExecute) <= 0)
            safeStore.notifyListeners(safeCommand, null);
    }

    protected static WaitingOn initialiseWaitingOn(SafeCommandStore safeStore, TxnId waitingId, Timestamp waitingExecuteAt, StoreParticipants participants, PartialDeps deps)
    {
        if (waitingId.awaitsOnlyDeps())
            waitingExecuteAt = Timestamp.maxForEpoch(waitingId.epoch());

        WaitingOn.Update initialise = Update.initialise(safeStore, waitingId, waitingExecuteAt, participants, deps);
        // TODO (desired): avoid allocating MinimalWithConcreteDeps here, but overall neater than prior approach of ICommand interface (which has minimal benefit to allocations)
        return updateWaitingOn(safeStore, new MinimalWithConcreteDeps(waitingId, null, null, participants, waitingExecuteAt, deps), waitingExecuteAt, initialise).build();
    }

    protected static Update updateWaitingOn(SafeCommandStore safeStore, MinimalWithConcreteDeps waiting, Timestamp executeAt, WaitingOn.Update initialise)
    {
        RedundantBefore redundantBefore = safeStore.redundantBefore();
        TxnId minWaitingOnTxnId = initialise.minWaitingOnTxn();
        if (minWaitingOnTxnId != null && redundantBefore.hasLocallyRedundantDependencies(minWaitingOnTxnId, executeAt, waiting.participants().waitsOn()))
            redundantBefore.removeRedundantDependencies(waiting.participants().waitsOn(), initialise);

        initialise.forEachWaitingOnId(safeStore, initialise, waiting, executeAt, (store, upd, w, exec, i) -> {
            // we don't want cleanup to transitively invoke a listener we've registered,
            // as we might still be initialising the WaitingOn collection
            SafeCommand dep = store.unsafeGetNoCleanup(upd.txnId(i));
            if (dep == null || !dep.current().hasBeen(PreCommitted))
                return;
            updateWaitingOn(store, w, exec, upd, dep);
        });

        initialise.forEachWaitingOnKey(safeStore, initialise, waiting, (store, upd, cmd, i) -> {
            SafeCommandsForKey safeCfk = store.ifLoadedAndInitialised(upd.keys.get(i));
            if (safeCfk == null)
                return;

            if (safeCfk.current().hasUniqueHlcAndIsReadyToExecute(cmd.txnId(), cmd.executeAt(), cmd.partialDeps()))
                upd.removeWaitingOnKey(i);
        });

        return initialise;
    }

    /**
     * @param dependencySafeCommand is either committed truncated, or invalidated
     * @return true iff {@code maybeExecute} might now have a different outcome
     */
    private static boolean updateWaitingOn(SafeCommandStore safeStore, MinimalWithConcreteDeps waiting, Timestamp waitingExecuteAt, Update waitingOn, SafeCommand dependencySafeCommand)
    {
        TxnId waitingId = waiting.txnId;
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
                                       || waiting.participants.stillWaitsOn().isEmpty()
                                       || !markStaleIfCannotExecute(dependencyId)
                                       || safeStore.redundantBefore().status(dependencyId, null,
                                                 waiting.partialDeps.participants(dependencyId)).all(LOCALLY_DEFUNCT)
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
            Participants<?> waitsOn = participants.intersecting(waiting.participants.stillWaitsOn(), Minimal);
            RedundantStatus status = safeStore.redundantBefore().status(dependencyId, waitingExecuteAt, waitsOn);

            if (waitsOn.isEmpty() || status.all(LOCALLY_DEFUNCT))
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
            safeCommand.updateWaitingOn(safeStore, waitingOn);
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
                TxnId nextWaitingOn = command.waitingOn().nextWaitingOnTxn();
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
            Invariants.expect(current.txnId().is(EphemeralRead), "%s was considered committed by %s but is only %s.", safeCommand.txnId(), key, current.saveStatus());
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
        safeCommand.updateWaitingOn(safeStore, waitingOn);
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
                result = erased(command.txnId());
                break;
        }
        return result;
    }

    private static boolean validateSafeToCleanup(RedundantBefore redundantBefore, Command command, @Nonnull StoreParticipants participants, boolean force)
    {
        if (participants.stillTouches().isEmpty()) return true;
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

        // TODO (required): should be an additional property so we can track correctly on merge...?
        if (status.all(SHARD_APPLIED, LOCALLY_REDUNDANT))
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

    public static Command setDurability(SafeCommandStore safeStore, SafeCommand safeCommand, StoreParticipants participants, Durability addDurability, @Nullable Timestamp executeAt)
    {
        final Command command = safeCommand.current();
        if (command.is(Truncated))
            return command;

        Durability oldDurability = command.durability();
        Durability newDurability = oldDurability.mergeMax(addDurability);
        if (newDurability == oldDurability)
            return command;

        Command updated = supplementParticipants(safeStore, safeCommand, participants);
        participants = updated.participants();
        if (executeAt != null && command.status().hasBeen(Committed) && !command.executeAt().equals(executeAt))
            ViolationHandlerHolder.get().onTimestampViolation(safeStore, command, participants, executeAt);

        updated = safeCommand.update(safeStore, updated.updateDurability(newDurability));
        TxnId txnId = command.txnId();
        DependencyElision updates = OFF;
        if (newDurability.isDurable() && !oldDurability.isDurable()) updates = IF_DURABLY_PREAPPLIED;
        else if (newDurability.isDurablyCommitted() && !oldDurability.isDurablyCommitted()) updates = IF_DURABLY_COMMITTED;
        if (updates.compareTo(dependencyElision()) >= 0 && CommandsForKey.manages(txnId))
        {
            AbstractUnseekableKeys keys = (AbstractUnseekableKeys)updated.participants().touches();
            ExecutionContext context = unsequencedIdempotentIncrementalWrite(keys, "Set Durable");
            ExecutionContext execute = safeStore.canExecute(context);
            if (execute != null)
            {
                setDurable(safeStore, execute, txnId, newDurability);
            }
            if (execute != context)
            {
                if (execute != null)
                    context = unsequencedIdempotentIncrementalWrite(keys.without(execute.keys()), "Set Durable");

                Invariants.require(!context.keys().isEmpty());
                safeStore = safeStore; // prevent accidental usage inside lambda
                safeStore.commandStore().execute(context, safeStore0 -> {
                    setDurable(safeStore0, safeStore0.context(), txnId, newDurability);
                }, safeStore.commandStore().agent);
            }
        }

        if (maybeCleanup(safeStore, safeCommand, updated, participants))
            updated = safeCommand.current();

        safeStore.notifyListeners(safeCommand, command);
        return updated;
    }

    private static void setDurable(SafeCommandStore safeStore, ExecutionContext context, TxnId txnId, Durability durability)
    {
        for (RoutingKey key : (AbstractUnseekableKeys)context.keys())
            safeStore.get(key).setDurable(txnId, durability);
    }

    static class NotifyWaitingOn implements ExecutionContext, Consumer<SafeCommandStore>
    {
        final TxnId waitingId;
        TxnId loadDepId;

        private NotifyWaitingOn(SafeCommand waiting)
        {
            Invariants.requireArgument(waiting.current().hasBeen(Stable));
            this.waitingId = waiting.txnId();
        }

        void start(SafeCommandStore safeStore)
        {
            if (safeStore.tryRecurse())
            {
                try { accept(safeStore); }
                finally { safeStore.unrecurse(); }
            }
            else
            {
                safeStore.commandStore().execute(this, this, safeStore.agent());
            }
        }

        @Override
        public final void accept(SafeCommandStore safeStore)
        {
            acceptInternal(safeStore);
        }

        // return false if done, true if continuing after loading a dependency
        boolean acceptInternal(SafeCommandStore safeStore)
        {
            SafeCommand waitingSafe = safeStore.get(waitingId);
            PartialDeps partialDeps;
            {
                Command waiting = waitingSafe.current();
                if (waiting.saveStatus().compareTo(Applying) >= 0)
                    return false;

                partialDeps = waiting.partialDeps();
                Invariants.require(partialDeps != null, "Trying to execute command without partialDeps: %s", waiting);
            }

            SafeCommand depSafe = null;
            if (loadDepId != null)
            {
                depSafe = safeStore.ifInitialised(loadDepId);
                if (depSafe == null) // TODO (required): slice to waiting.participants().waitsOn? can simplify method
                    depSafe = initialiseOrRemoveDependency(safeStore, waitingSafe, loadDepId, partialDeps.participants(loadDepId));
            }
            else
            {
                removeRedundantDependencies(safeStore, waitingSafe);
            }

            while (true)
            {
                Command waiting = waitingSafe.current();
                if (waiting.saveStatus().compareTo(Applying) >= 0)
                    return false; // nothing to do TODO (expected): NotifyWaitingOnPlus should be able to try redundantly applying to unblock

                if (depSafe == null)
                {
                    WaitingOn waitingOn = waiting.waitingOn();
                    TxnId directlyBlockedOn = waitingOn.nextWaitingOnTxn();
                    if (directlyBlockedOn == null)
                    {
                        if (waitingOn.isWaiting())
                            return false; // nothing more we can do; all direct dependencies are notified

                        switch (waiting.saveStatus())
                        {
                            default: throw illegalState("Invalid saveStatus with empty waitingOn: " + waiting.saveStatus());
                            case ReadyToExecute:
                            case Applied:
                            case Applying:
                                return false;

                            case Stable:
                            case PreApplied:
                                boolean executed = maybeExecute(safeStore, waitingSafe, true, false);
                                Invariants.require(executed);
                                return false;
                        }
                    }

                    depSafe = safeStore.ifLoadedAndInitialised(directlyBlockedOn);
                    if (depSafe == null)
                    {
                        loadDepId = directlyBlockedOn;
                        safeStore.commandStore().execute(this, this, safeStore.agent());
                        return true;
                    }
                }
                else
                {
                    Command dep = depSafe.current();
                    SaveStatus depStatus = dep.saveStatus();
                    LocalExecution depExecution = depStatus.execution;
                    if (!waitingId.awaitsOnlyDeps() && depStatus.known.isExecuteAtKnown() && dep.executeAt().compareTo(waiting.executeAt()) > 0)
                        depExecution = LocalExecution.Applied;

                    Participants<?> participants = null;
                    if (depExecution.compareTo(WaitingToExecute) < 0)
                    {
                        // transaction might have been invalidated;
                        // we should only need to check this after replay which might not replay potentially-invalidated transactions
                        // TODO (required): why isn't this case handled by maybeCleanup when we load the dependency?
                        // TODO (desired): slightly costly to invert a large partialDeps collection
                        participants = partialDeps.participants(dep.txnId());
                        Participants<?> waitsOn = participants.intersecting(waiting.participants().stillWaitsOn(), Minimal);

                        depSafe = maybeCleanupRedundantDependency(safeStore, waitingSafe, depSafe, waitsOn);
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
                            return false;

                        case ReadyToExclude:
                        case WaitingToExecute:
                        case ReadyToExecute:
                            safeStore.progressLog().waiting(CanApply, safeStore, depSafe, null, participants, null);

                        case Applying:
                            safeStore.registerListener(depSafe, SaveStatus.Applied, waitingId);
                            return false;

                        case WaitingToApply:
                            if (dep.asCommitted().isWaitingOnDependency())
                            {
                                safeStore.registerListener(depSafe, SaveStatus.Applied, waitingId);
                                maybeTransitivelyExecute(safeStore, depSafe);
                                return false;
                            }
                            else
                            {
                                transitivelyExecute(safeStore, depSafe);
                                switch (depSafe.current().saveStatus())
                                {
                                    default: throw illegalState("Invalid child status after attempt to execute: " + depSafe.current().saveStatus());
                                    case Applying:
                                        safeStore.registerListener(depSafe, SaveStatus.Applied, waitingId);
                                        return false;

                                    case Applied:
                                        // fall-through to outer Applied branch
                                }
                            }

                        case Applied:
                        case CleaningUp:
                            updateDependencyAndMaybeExecute(safeStore, waitingSafe, depSafe, false);
                            waiting = waitingSafe.current();
                            Invariants.require(waiting.saveStatus().compareTo(Applying) >= 0 || !waiting.waitingOn().isWaitingOn(dep.txnId()));
                            depSafe = null;
                    }
                }
            }
        }

        void maybeTransitivelyExecute(SafeCommandStore safeStore, SafeCommand depSafe)
        {
        }

        void transitivelyExecute(SafeCommandStore safeStore, SafeCommand depSafe)
        {
            maybeExecute(safeStore, depSafe, false, false);
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
            if (remove && waitingSafe.txnId().isSyncPoint() && depId.isSyncPoint())
                remove = status.all(LOCALLY_SYNCED) || status.all(UNREADY); // TODO (required): should be additional property for correct merge?

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
        public String reason()
        {
            return "NotifyWaitingOn{waiting=" + waitingId + ",on=" + loadDepId + '}';
        }

        @Override
        public TxnId additionalTxnId()
        {
            return loadDepId;
        }

        @Override
        public ExecutionSequence executionSequence()
        {
            return ExecutionSequence.UNSEQUENCED;
        }
    }

    public static class NotifyWaitingOnPlus extends NotifyWaitingOn implements MaybeExecuteAdapter
    {
        static class CountingConsumer
        {
            final Consumer<SafeCommandStore> onDone;
            int count;

            CountingConsumer(Consumer<SafeCommandStore> onDone)
            {
                this.onDone = onDone;
            }

            void increment()
            {
                ++count;
            }

            void decrement(SafeCommandStore safeStore)
            {
                if (--count == -1 && onDone != null)
                    onDone.accept(safeStore);
            }
        }

        final CountingConsumer onDone;
        final boolean transitivelyNotifyListeners;
        final boolean transitivelyNotifyWaitingOn;

        NotifyWaitingOnPlus(SafeCommand root, Consumer<SafeCommandStore> onDone, boolean transitivelyNotifyListeners, boolean transitivelyNotifyWaitingOn)
        {
            this(root, new CountingConsumer(onDone), transitivelyNotifyListeners, transitivelyNotifyWaitingOn);
        }

        NotifyWaitingOnPlus(SafeCommand root, CountingConsumer onDone, boolean transitivelyNotifyListeners, boolean transitivelyNotifyWaitingOn)
        {
            super(root);
            this.onDone = onDone;
            this.transitivelyNotifyListeners = transitivelyNotifyListeners;
            this.transitivelyNotifyWaitingOn = transitivelyNotifyWaitingOn;
        }

        @Override
        boolean acceptInternal(SafeCommandStore safeStore)
        {
            if (super.acceptInternal(safeStore))
                return true;

            onDone.decrement(safeStore);
            return false;
        }

        void maybeTransitivelyExecute(SafeCommandStore safeStore, SafeCommand depSafe)
        {
            transitivelyExecute(safeStore, depSafe);
        }

        @Override
        void transitivelyExecute(SafeCommandStore safeStore, SafeCommand depSafe)
        {
            onDone.increment();
            maybeExecute(safeStore, depSafe, depSafe.current(), transitivelyNotifyListeners, transitivelyNotifyWaitingOn, this);
        }

        @Override
        public void notifyWaiting(SafeCommandStore safeStore, SafeCommand waiting)
        {
            new NotifyWaitingOnPlus(waiting, onDone, transitivelyNotifyListeners, transitivelyNotifyWaitingOn).start(safeStore);
        }

        @Override
        public void notWaiting(SafeCommandStore safeStore)
        {
            onDone.decrement(safeStore);
        }

        public static MaybeExecuteAdapter adapter(Consumer<SafeCommandStore> onDone, boolean transitivelyNotifyListeners, boolean transitivelyNotifyWaitingOn)
        {
            return new Commands.MaybeExecuteAdapter()
            {
                @Override
                public void notifyWaiting(SafeCommandStore safeStore, SafeCommand waiting)
                {
                    new NotifyWaitingOnPlus(waiting, onDone, transitivelyNotifyListeners, transitivelyNotifyWaitingOn).start(safeStore);
                }

                @Override
                public void notWaiting(SafeCommandStore safeStore)
                {
                    onDone.accept(safeStore);
                }
            };
        }

        @Override
        public String reason()
        {
            return "NotifyWaitingOnPlus{waiting=" + waitingId + ",on=" + loadDepId + ',' + transitivelyNotifyListeners + ',' + transitivelyNotifyWaitingOn + '}';
        }
    }

    static boolean removeRedundantDependencies(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Update update = maybeRemoveRedundantDependencies(safeStore, safeCommand);
        if (update == null)
            return false;

        safeCommand.updateWaitingOn(safeStore, update);
        return true;
    }

    static boolean removeRedundantDependencies(SafeCommandStore safeStore, SafeCommand safeCommand, @Nullable TxnId redundant)
    {
        Update update = maybeRemoveRedundantDependencies(safeStore, safeCommand);

        if (redundant != null)
        {
            if (update == null)
                update = new Update(safeCommand.current().waitingOn());
            update.removeWaitingOn(redundant);
        }

        if (update == null)
            return false;

        safeCommand.updateWaitingOn(safeStore, update);
        return true;
    }

    private static Update maybeRemoveRedundantDependencies(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        RedundantBefore redundantBefore = safeStore.redundantBefore();
        Command command = safeCommand.current();
        WaitingOn waitingOn = command.waitingOn();
        TxnId minWaitingOnTxnId = waitingOn.minWaitingOnTxn();
        Participants<?> waitsOn = command.participants().waitsOn();
        if (minWaitingOnTxnId == null || !redundantBefore.hasLocallyRedundantDependencies(minWaitingOnTxnId, command.executeAt(), waitsOn))
            return null;

        Update update = new Update(waitingOn);
        redundantBefore.removeRedundantDependencies(waitsOn, update);
        return update;
    }

    static void removeNoLongerOwnedDependency(SafeCommandStore safeStore, SafeCommand safeCommand, @Nonnull TxnId wasOwned)
    {
        Command.Committed current = safeCommand.current().asCommitted();
        if (!current.waitingOn.isWaitingOn(wasOwned))
            return;

        Update update = new Update(current.waitingOn);
        update.removeWaitingOn(wasOwned);
        safeCommand.updateWaitingOn(safeStore, update);
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
                if (newDeps == null)
                    return upd.partialDeps();
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

    enum Validated { INSUFFICIENT_EPOCHS, INSUFFICIENT, UPDATE_TXN_IGNORE_DEPS, UPDATE_TXN_KEEP_DEPS, UPDATE_TXN_MERGE_DEPS, UPDATE_TXN_AND_DEPS_INTERSECT_STABLE, UPDATE_TXN_AND_DEPS }

    private static Validated validate(SafeCommandStore safeStore, TxnId txnId, @Nullable Ballot ballot, SaveStatus newStatus, Command cur, StoreParticipants participants,
                                      Route<?> addRoute, @Nullable Txn addPartialTxn, @Nullable Deps partialDeps)
    {
        return validate(safeStore, txnId, ballot, newStatus, cur, participants, addRoute, addPartialTxn, partialDeps, null, null);
    }

    private static Validated validate(SafeCommandStore safeStore, TxnId txnId,
                                      @Nullable Ballot ballot, SaveStatus newStatus, Command cur, StoreParticipants participants,
                                      Route<?> addRoute, @Nullable Txn addPartialTxn, @Nullable Deps partialDeps,
                                      @Nullable Commit.Kind commitKind, @Nullable Timestamp executeAt)
    {
        Known haveKnown = cur.known();
        Known expectKnown = newStatus.known;

        // TODO (desired): addRoute adds some validation we aren't losing the route from the update in any StoreParticipant updates
        //   but it might be nice to impose this earlier, or with some clearer semantics
        Invariants.require(addRoute == participants.route());
        Route<?> fullRoute = null;
        if (expectKnown.has(KnownRoute.FullRoute))
        {
            if (isFullRoute(cur.route())) fullRoute = cur.route();
            else if (isFullRoute(addRoute)) fullRoute = addRoute;
            else return INSUFFICIENT;
        }

        if (expectKnown.definition().isKnown())
        {
            if (cur.txnId().isSystemTxn())
            {
                if (cur.partialTxn() == null && addPartialTxn == null && !participants.stillOwns().isEmpty())
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
            if ((haveKnown.is(DepsProposedFixed) || (haveKnown.is(DepsProposed) && cur.acceptedOrCommitted().equals(Ballot.ZERO)))
                && expectKnown.is(DepsKnown) && ballot != null && ballot.equals(Ballot.ZERO) && participants.stillTouches().equals(cur.participants().touches()))
                return UPDATE_TXN_MERGE_DEPS;
            return INSUFFICIENT;
        }

        if (haveKnown.is(DepsKnown) || (haveKnown.equalDeps(expectKnown) && (ballot == null || ballot.equals(cur.acceptedOrCommitted()))))
            return UPDATE_TXN_KEEP_DEPS;

        if (!containsAll(partialDeps, participants.stillTouches()))
            return INSUFFICIENT;

        if (txnId.isSyncPoint() && expectKnown.is(DepsKnown))
        {
            Ranges missing = Ranges.ofSortedAndDeoverlapped(safeStore.redundantBefore().foldl(fullRoute, (Bounds b, List<Range> v, TxnId id) -> {
                if (b.endEpoch < Long.MAX_VALUE && !b.isLocallyRetiredOrUnready(id))
                    v.add(b.range);
                return v;
            }, new ArrayList<>(), txnId).toArray(Range[]::new)).intersecting(fullRoute, Minimal).without(participants.stillWaitsOn());
            if (!missing.isEmpty())
                return INSUFFICIENT_EPOCHS;
        }

        if (executeAt != null && expectKnown.is(DepsKnown) && haveKnown.compareTo(DepsFromCoordinator) > 0 && executeAt.equals(cur.txnId()) && !cur.acceptedOrCommitted().equals(Ballot.ZERO))
            return UPDATE_TXN_AND_DEPS_INTERSECT_STABLE;

        return UPDATE_TXN_AND_DEPS;
    }

    private static boolean containsAll(Txn adding, Participants<?> required)
    {
        return adding == null ? required.isEmpty() : adding.covers(required);
    }

    private static boolean containsAll(Deps adding, Participants<?> required)
    {
        return adding == null ? required.isEmpty() : adding.covers(required);
    }
}
