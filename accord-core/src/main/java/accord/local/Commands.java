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

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProgressLog.ProgressShard;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Command.ProxyListener;
import accord.local.Command.WaitingOn;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import javax.annotation.Nullable;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.api.ProgressLog.ProgressShard.No;
import static accord.api.ProgressLog.ProgressShard.UnmanagedHome;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.local.Command.Truncated.truncated;
import static accord.local.Commands.EnsureAction.Add;
import static accord.local.Commands.EnsureAction.Check;
import static accord.local.Commands.EnsureAction.Ignore;
import static accord.local.Commands.EnsureAction.Set;
import static accord.local.Commands.EnsureAction.TrySet;
import static accord.local.RedundantStatus.LIVE;
import static accord.local.SaveStatus.Uninitialised;
import static accord.local.Status.Accepted;
import static accord.local.Status.AcceptedInvalidate;
import static accord.local.Status.Applied;
import static accord.local.Status.Applying;
import static accord.local.Status.Committed;
import static accord.local.Status.Durability;
import static accord.local.Status.Durability.Universal;
import static accord.local.Status.Invalidated;
import static accord.local.Status.Known;
import static accord.local.Status.Known.ExecuteAtOnly;
import static accord.local.Status.NotDefined;
import static accord.local.Status.PreApplied;
import static accord.local.Status.PreCommitted;
import static accord.local.Status.ReadyToExecute;
import static accord.local.Status.Truncated;
import static accord.primitives.Route.isFullRoute;

public class Commands
{
    private static final Logger logger = LoggerFactory.getLogger(Commands.class);

    private Commands()
    {
    }

    private static Ranges covers(@Nullable PartialTxn txn)
    {
        return txn == null ? null : txn.covering();
    }

    private static Ranges covers(@Nullable PartialDeps deps)
    {
        return deps == null ? null : deps.covering;
    }

    private static boolean hasQuery(PartialTxn txn)
    {
        return txn != null && txn.query() != null;
    }

    /**
     * true iff this commandStore owns the given key on the given epoch
     */
    public static boolean owns(SafeCommandStore safeStore, long epoch, RoutingKey someKey)
    {
        return safeStore.ranges().allAt(epoch).contains(someKey);
    }

    public enum AcceptOutcome { Success, Redundant, RejectedBallot, Truncated }

    public static AcceptOutcome preaccept(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, long acceptEpoch, PartialTxn partialTxn, FullRoute<?> route, @Nullable RoutingKey progressKey)
    {
        return preacceptOrRecover(safeStore, safeCommand, txnId, acceptEpoch, partialTxn, route, progressKey, Ballot.ZERO);
    }

    public static AcceptOutcome recover(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, PartialTxn partialTxn, FullRoute<?> route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        // for recovery we only ever propose either the original epoch or an Accept that we witness; otherwise we invalidate
        return preacceptOrRecover(safeStore, safeCommand, txnId, txnId.epoch(), partialTxn, route, progressKey, ballot);
    }

    private static AcceptOutcome preacceptOrRecover(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, long acceptEpoch, PartialTxn partialTxn, FullRoute<?> route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        Command command = safeCommand.current();

        if (command.hasBeen(Truncated))
        {
            logger.trace("{}: skipping preaccept - command is truncated", txnId);
            return command.is(Invalidated) ? AcceptOutcome.Redundant : AcceptOutcome.Truncated;
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

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId, acceptEpoch);
        Invariants.checkState(!coordinateRanges.isEmpty());
        CommonAttributes attrs = updateRouteAndProgressShard(command, route, progressKey, coordinateRanges, Ranges.EMPTY);
        ProgressShard shard = attrs.progressShard();
        Invariants.checkState(validate(command.status(), attrs, Ranges.EMPTY, coordinateRanges, shard, route, Set, partialTxn, Set, null, Ignore));

        // FIXME: this should go into a consumer method
        attrs = set(safeStore, command, attrs, Ranges.EMPTY, coordinateRanges, shard, route, partialTxn, Set, null, Ignore);
        if (command.executeAt() == null)
        {
            // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
            //  - use a global logical clock to issue new timestamps; or
            //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
            // if we are performing recovery (i.e. non-zero ballot), do not permit a fast path decision as we want to
            // invalidate any transactions that were not completed by their initial coordinator
            // TODO (desired): limit preaccept to keys we include, to avoid inflating unnecessary state
            Timestamp executeAt = safeStore.commandStore().preaccept(txnId, partialTxn.keys(), safeStore, ballot.equals(Ballot.ZERO));
            command = safeCommand.preaccept(attrs, executeAt, ballot);
            safeStore.progressLog().preaccepted(command, shard);
        }
        else
        {
            // TODO (expected, ?): in the case that we are pre-committed but had not been preaccepted/accepted, should we inform progressLog?
            safeCommand.markDefined(attrs, ballot);
        }

        safeStore.notifyListeners(safeCommand);
        return AcceptOutcome.Success;
    }

    public static boolean preacceptInvalidate(SafeCommand safeCommand, TxnId txnId, Ballot ballot)
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

    public static AcceptOutcome accept(SafeCommandStore safeStore, TxnId txnId, Ballot ballot, PartialRoute<?> route, Seekables<?, ?> keys, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        SafeCommand safeCommand = safeStore.get(txnId, route);
        Command command = safeCommand.current();
        if (command.hasBeen(PreCommitted))
        {
            if (command.is(Invalidated))
            {
                logger.trace("{}: skipping accept - command is invalidated", txnId);
                return AcceptOutcome.Redundant;
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

        CommonAttributes attrs = updateRouteAndProgressShard(command, route, progressKey, coordinateRanges, acceptRanges);
        ProgressShard shard = attrs.progressShard();
        Invariants.checkState(validate(command.status(), attrs, coordinateRanges, acceptRanges, shard, route, Ignore, null, Ignore, partialDeps, Set));

        // TODO (desired, clarity/efficiency): we don't need to set the route here, and perhaps we don't even need to
        //  distributed partialDeps at all, since all we gain is not waiting for these transactions to commit during
        //  recovery. We probably don't want to directly persist a Route in any other circumstances, either, to ease persistence.
        attrs = set(safeStore, command, attrs, coordinateRanges, acceptRanges, shard, route, null, Ignore, partialDeps, Set);

        // set only registers by transaction keys, which we mightn't already have received
        if (!command.known().isDefinitionKnown())
            safeStore.register(keys, acceptRanges, command);

        command = safeCommand.accept(attrs, executeAt, ballot);
        safeStore.progressLog().accepted(command, shard);
        safeStore.notifyListeners(safeCommand);

        return AcceptOutcome.Success;
    }

    public static AcceptOutcome acceptInvalidate(SafeCommandStore safeStore, SafeCommand safeCommand, Ballot ballot)
    {
        // TODO (expected): save some partial route we can use to determine if it can be GC'd
        Command command = safeCommand.current();
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
            return AcceptOutcome.Redundant;
        }

        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept invalidated - witnessed higher ballot ({} > {})", command.txnId(), command.promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        logger.trace("{}: accepted invalidated", command.txnId());

        safeCommand.acceptInvalidated(ballot);
        safeStore.notifyListeners(safeCommand);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Redundant, Insufficient }


    // relies on mutual exclusion for each key
    public static CommitOutcome commit(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Route<?> route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        Command command = safeCommand.current();

        if (command.hasBeen(PreCommitted))
        {
            if (command.is(Truncated))
            {
                logger.trace("{}: skipping commit - already truncated ({})", txnId, command.status());
            }
            else
            {
                logger.trace("{}: skipping commit - already committed ({})", txnId, command.status());
                if (!executeAt.equals(command.executeAt()) || command.status() == Invalidated)
                    safeStore.agent().onInconsistentTimestamp(command, (command.status() == Invalidated ? Timestamp.NONE : command.executeAt()), executeAt);
            }

            if (command.hasBeen(Committed))
                return CommitOutcome.Redundant;
        }

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Ranges acceptRanges = acceptRanges(safeStore, txnId, executeAt, coordinateRanges);
        // TODO (expected, consider): consider ranges between coordinateRanges and executeRanges? Perhaps don't need them
        Ranges executeRanges = executeRanges(safeStore, executeAt);

        CommonAttributes attrs = updateRouteAndProgressShard(command, route, progressKey, coordinateRanges, acceptRanges);
        ProgressShard shard = attrs.progressShard();

        if (!validate(command.status(), attrs, acceptRanges, executeRanges, shard, route, Check, partialTxn, Add, partialDeps, Set))
        {
            safeCommand.updateAttributes(attrs);
            return CommitOutcome.Insufficient;
        }

        // FIXME: split up set
        attrs = set(safeStore, command, attrs, acceptRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);

        logger.trace("{}: committed with executeAt: {}, deps: {}", txnId, executeAt, partialDeps);
        WaitingOn waitingOn = initialiseWaitingOn(safeStore, txnId, executeAt, attrs.partialDeps(), attrs.route());
        command = safeCommand.commit(attrs, executeAt, waitingOn);

        safeStore.progressLog().committed(command, shard);

        // TODO (expected, safety): introduce intermediate status to avoid reentry when notifying listeners (which might notify us)
        maybeExecute(safeStore, safeCommand, shard, true, true);
        return CommitOutcome.Success;
    }

    // relies on mutual exclusion for each key
    public static void precommit(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Timestamp executeAt)
    {
        Command command = safeCommand.current();
        if (command.hasBeen(PreCommitted))
        {
            if (command.is(Truncated))
            {
                logger.trace("{}: skipping commit - already truncated ({})", txnId, command.status());
                return;
            }
            else
            {
                logger.trace("{}: skipping precommit - already committed ({})", txnId, command.status());
                if (executeAt.equals(command.executeAt()) && command.status() != Invalidated)
                    return;

                safeStore.agent().onInconsistentTimestamp(command, (command.status() == Invalidated ? Timestamp.NONE : command.executeAt()), executeAt);
            }
        }

        safeCommand.precommit(executeAt);
        safeStore.notifyListeners(safeCommand);
        logger.trace("{}: precommitted with executeAt: {}", txnId, executeAt);
    }

    public static void commitRecipientLocalSyncPoint(SafeCommandStore safeStore, TxnId localSyncId, SyncPoint syncPoint, Seekables<?, ?> keys)
    {
        SafeCommand safeCommand = safeStore.get(localSyncId);
        Command command = safeCommand.current();
        Invariants.checkState(!command.hasBeen(Committed));
        commitRecipientLocalSyncPoint(safeStore, localSyncId, keys, syncPoint.route());
    }

    private static void commitRecipientLocalSyncPoint(SafeCommandStore safeStore, TxnId localSyncId, Seekables<?, ?> keys, Route<?> route)
    {
        SafeCommand safeCommand = safeStore.get(localSyncId);
        Command command = safeCommand.current();
        if (command.hasBeen(Committed))
            return;

        Ranges coordinateRanges = coordinateRanges(safeStore, localSyncId);
        // TODO (desired, consider): in the case of sync points, the coordinator is unlikely to be a home shard, do we mind this? should document at least
        Txn emptyTxn = safeStore.agent().emptyTxn(localSyncId.rw(), keys);
        ProgressShard progressShard = coordinateRanges.contains(route.homeKey()) ? UnmanagedHome : No;
        CommonAttributes newAttributes = command.mutable().progressShard(progressShard);
        PartialDeps none = Deps.NONE.slice(coordinateRanges);
        PartialTxn partialTxn = emptyTxn.slice(coordinateRanges, true);
        Invariants.checkState(validate(command.status(), newAttributes, Ranges.EMPTY, coordinateRanges, progressShard, route, Set, partialTxn, Set, none, Set));
        newAttributes = set(safeStore, command, newAttributes, Ranges.EMPTY, coordinateRanges, progressShard, route, partialTxn, Set, none, Set);
        safeCommand.commit(newAttributes, localSyncId, WaitingOn.EMPTY);
        safeStore.notifyListeners(safeCommand);
    }

    public static void applyRecipientLocalSyncPoint(SafeCommandStore safeStore, TxnId localSyncId, Seekables<?, ?> keys)
    {
        SafeCommand safeCommand = safeStore.get(localSyncId);
        Command.Committed command = safeCommand.current().asCommitted();
        if (command.hasBeen(PreApplied))
            return;

        // NOTE: if this is ever made a non-empty txn this will introduce a potential bug where the txn is registered against CommandsForKeys
        Txn emptyTxn = safeStore.agent().emptyTxn(localSyncId.rw(), keys);
        safeCommand.preapplied(command, command.executeAt(), command.waitingOn(), emptyTxn.execute(localSyncId, localSyncId, null), emptyTxn.result(localSyncId, localSyncId, null));
        maybeExecute(safeStore, safeCommand, Unsure, true, false);
    }

    // TODO (expected, ?): commitInvalidate may need to update cfks _if_ possible
    public static void commitInvalidate(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        Command command = safeCommand.current();
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

        safeStore.progressLog().clear(command.txnId());

        safeCommand.commitInvalidated();
        logger.trace("{}: committed invalidated", safeCommand.txnId());

        safeStore.notifyListeners(safeCommand);
    }

    public enum ApplyOutcome {Success, Redundant, Insufficient}

    public static ApplyOutcome apply(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Route<?> route, @Nullable RoutingKey progressKey, Timestamp executeAt, @Nullable PartialDeps partialDeps, @Nullable PartialTxn partialTxn, Writes writes, Result result)
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
        Ranges executeRanges = executeRanges(safeStore, executeAt);

        CommonAttributes attrs = updateRouteAndProgressShard(command, route, progressKey, coordinateRanges, acceptRanges);
        ProgressShard shard = attrs.progressShard();

        if (!validate(command.status(), attrs, acceptRanges, executeRanges, shard, route, Check, partialTxn, Add, partialDeps, command.hasBeen(Committed) ? Check : TrySet))
        {
            safeCommand.updateAttributes(attrs);
            return ApplyOutcome.Insufficient; // TODO (expected, consider): this should probably be an assertion failure if !TrySet
        }

        attrs = set(safeStore, command, attrs, acceptRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, command.hasBeen(Committed) ? Check : TrySet);

        WaitingOn waitingOn = !command.hasBeen(Committed) ? initialiseWaitingOn(safeStore, txnId, executeAt, attrs.partialDeps(), attrs.route()) : command.asCommitted().waitingOn();
        safeCommand.preapplied(attrs, executeAt, waitingOn, writes, result);
        safeStore.notifyListeners(safeCommand);
        logger.trace("{}: apply, status set to Executed with executeAt: {}, deps: {}", txnId, executeAt, partialDeps);

        maybeExecute(safeStore, safeCommand, shard, true, true);
        safeStore.progressLog().executed(safeCommand.current(), shard);

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
            Invariants.checkState(listener.saveStatus() == Uninitialised || listener.is(Truncated));
            Invariants.checkState(updated.hasBeen(Applied) || updated.is(NotDefined));
            safeUpdated.removeListener(listener.asListener());
            return;
        }

        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     listener.txnId(), updated.txnId(), updated.status(), updated);
        switch (updated.status())
        {
            default:
                throw new IllegalStateException("Unexpected status: " + updated.status());
            case NotDefined:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                break;

            case PreCommitted:
            case Committed:
            case ReadyToExecute:
            case PreApplied:
            case Applying:
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
        safeCommand.applied();
        safeStore.notifyListeners(safeCommand);
    }

    /**
     * The ranges for which we participate in the consensus decision of when a transaction executes
     */
    private static Ranges coordinateRanges(SafeCommandStore safeStore, TxnId txnId)
    {
        return safeStore.ranges().coordinates(txnId);
    }

    /**
     * The ranges for which we participate in the consensus decision of when a transaction executes
     */
    private static Ranges coordinateRanges(SafeCommandStore safeStore, TxnId txnId, long untilEpoch)
    {
        return safeStore.ranges().allBetween(txnId.epoch(), untilEpoch);
    }

    private static boolean coordinates(SafeCommandStore safeStore, TxnId txnId, RoutingKey key)
    {
        return coordinateRanges(safeStore, txnId).contains(key);
    }

    private static Ranges acceptRanges(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, Ranges coordinateRanges)
    {
        return txnId.epoch() == executeAt.epoch() ? coordinateRanges : safeStore.ranges().allBetween(txnId, executeAt);
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

    private static AsyncChain<Void> applyChain(SafeCommandStore safeStore, PreLoadContext context, TxnId txnId)
    {
        Command.Executed command = safeStore.get(txnId).current().asExecuted();
        if (command.hasBeen(Applied))
            return AsyncChains.success(null);

        CommandStore unsafeStore = safeStore.commandStore();
        return command.writes().apply(safeStore, applyRanges(safeStore, command.executeAt()))
               .flatMap(unused -> unsafeStore.submit(context, ss -> {
                   postApply(ss, txnId);
                   return null;
               }));
    }

    private static void apply(SafeCommandStore safeStore, Command.Executed command)
    {
        CommandStore unsafeStore = safeStore.commandStore();
        TxnId txnId = command.txnId();
        PreLoadContext context = command.contextForSelf();
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

    // TODO (expected, API consistency): maybe split into maybeExecute and maybeApply?
    private static boolean maybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, ProgressShard shard, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        Command command = safeCommand.current();
        if (logger.isTraceEnabled())
            logger.trace("{}: Maybe executing with status {}. Will notify listeners on noop: {}", command.txnId(), command.status(), alwaysNotifyListeners);

        if (command.status() != Committed && command.status() != PreApplied)
        {
            if (alwaysNotifyListeners)
                safeStore.notifyListeners(safeCommand);
            return false;
        }

        if (command.asCommitted().isWaitingOnDependency())
        {
            if (alwaysNotifyListeners)
                safeStore.notifyListeners(safeCommand);

            if (notifyWaitingOn)
                new NotifyWaitingOn(safeCommand).accept(safeStore);
            return false;
        }

        // FIXME: need to communicate to caller that we didn't execute if we take one of the above paths

        switch (command.status())
        {
            case Committed:
                // TODO (desirable, efficiency): maintain distinct ReadyToRead and ReadyToWrite states
                command = safeCommand.readyToExecute();
                logger.trace("{}: set to ReadyToExecute", command.txnId());
                safeStore.progressLog().readyToExecute(command, shard);
                safeStore.notifyListeners(safeCommand);
                return true;

            case PreApplied:
                Ranges executeRanges = executeRanges(safeStore, command.executeAt());
                Command.Executed executed = command.asExecuted();
                boolean intersects = executed.writes().keys.intersects(executeRanges);

                if (intersects)
                {
                    safeCommand.applying();
                    logger.trace("{}: applying", command.txnId());
                    apply(safeStore, executed);
                    return true;
                }
                else
                {
                    // TODO (desirable, performance): This could be performed immediately upon Committed
                    //      but: if we later support transitive dependency elision this could be dangerous
                    logger.trace("{}: applying no-op", command.txnId());
                    safeCommand.applied();
                    safeStore.notifyListeners(safeCommand);
                    return true;
                }
            default:
                throw new IllegalStateException();
        }
    }

    protected static WaitingOn initialiseWaitingOn(SafeCommandStore safeStore, TxnId waitingId, Timestamp executeWaitingAt, PartialDeps partialDeps, Route<?> route)
    {
        Unseekables<?> executionParticipants = route.participants().slice(safeStore.ranges().allAt(executeWaitingAt));
        WaitingOn.Update update = new WaitingOn.Update(executionParticipants, partialDeps);
        return updateWaitingOn(safeStore, waitingId, executeWaitingAt, update, route.participants()).build();
    }

    protected static WaitingOn.Update updateWaitingOn(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, WaitingOn.Update update, Participants<?> participants)
    {
        CommandStore commandStore = safeStore.commandStore();
        TxnId minWaitingOnTxnId = update.minWaitingOnTxnId();
        if (minWaitingOnTxnId != null && commandStore.hasRedundantDependencies(update.minWaitingOnTxnId(), executeAt, participants))
            safeStore.commandStore().removeRedundantDependencies(participants, update);

        update.forEachWaitingOnCommit(safeStore, update, txnId, executeAt, (store, upd, id, exec, i) -> {
            // TODO (expected): load read-only to reduce overhead; upgrade only if we need to remove listener
            SafeCommand dep = store.ifLoadedAndInitialised(upd.deps.txnId(i));
            if (dep == null || !dep.current().hasBeen(PreCommitted))
                return;
            updateWaitingOn(id, exec, upd, dep);
        });

        update.forEachWaitingOnApply(safeStore, update, txnId, executeAt, (store, upd, id, exec, i) -> {
            SafeCommand dep = store.ifLoadedAndInitialised(upd.deps.txnId(i));
            if (dep == null || !dep.current().hasBeen(PreCommitted))
                return;
            updateWaitingOn(id, exec, upd, dep);
        });

        return update;
    }

    /**
     * @param dependencySafeCommand is either committed truncated, or invalidated
     * @return true iff {@code maybeExecute} might now have a different outcome
     */
    private static boolean updateWaitingOn(TxnId waitingId, Timestamp executeWaitingAt, WaitingOn.Update waitingOn, SafeCommand dependencySafeCommand)
    {
        Command dependency = dependencySafeCommand.current();
        Invariants.checkState(dependency.hasBeen(PreCommitted));
        if (dependency.is(Truncated))
        {
            logger.trace("{}: {} is truncated. Stop listening and removing from waiting on commit set.", waitingId, dependency.txnId());
            dependencySafeCommand.removeListener(new ProxyListener(waitingId));
            return waitingOn.removeInvalidatedOrTruncated(dependency.txnId());
        }
        else if (dependency.is(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", waitingId, dependency.txnId());
            dependencySafeCommand.removeListener(new ProxyListener(waitingId));
            return waitingOn.removeInvalidatedOrTruncated(dependency.txnId());
        }
        else if (dependency.executeAt().compareTo(executeWaitingAt) > 0 && !waitingId.rw().awaitsFutureDeps())
        {
            // dependency cannot be a predecessor if it executes later
            logger.trace("{}: {} executes after us. Stop listening and removing from waiting on apply set.", waitingId, dependency.txnId());
            dependencySafeCommand.removeListener(new ProxyListener(waitingId));
            return waitingOn.removeWaitingOn(dependency.txnId());
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Stop listening and removing from waiting on apply set.", waitingId, dependency.txnId());
            dependencySafeCommand.removeListener(new ProxyListener(waitingId));
            return waitingOn.removeApplied(dependency.txnId(), dependency.asCommitted().waitingOn());
        }
        else if (!waitingOn.isEmpty())
        {
            logger.trace("{}: adding {} to waiting on apply set.", waitingId, dependency.txnId());
            if (waitingOn.addWaitingOnApply(dependency.txnId()))
            {
                boolean removedWaitingOnCommit = waitingOn.removeWaitingOnCommit(dependency.txnId());
                Invariants.checkState(removedWaitingOnCommit);
                return true;
            }
            return false;
        }
        else
        {
            throw new IllegalStateException("We have a dependency to wait on, but have already finished waiting");
        }
    }

    static void updateDependencyAndMaybeExecute(SafeCommandStore safeStore, SafeCommand safeCommand, SafeCommand predecessor, boolean notifyWaitingOn)
    {
        Command.Committed command = safeCommand.current().asCommitted();
        if (command.hasBeen(Applied))
            return;

        WaitingOn.Update waitingOn = new WaitingOn.Update(command);
        if (updateWaitingOn(command.txnId(), command.executeAt(), waitingOn, predecessor))
        {
            command = safeCommand.updateWaitingOn(waitingOn);
            maybeExecute(safeStore, safeCommand, command.progressShard(), false, notifyWaitingOn);
        }
        else
        {
            Command pred = predecessor.current();
            if (pred.is(ReadyToExecute))
            {
                TxnId nextWaitingOn = command.waitingOn().nextWaitingOn();
                if (nextWaitingOn != null && nextWaitingOn.equals(pred.txnId()))
                    safeStore.progressLog().waiting(predecessor, Known.Done, pred.route(), null);
            }
        }
    }

    public enum Truncate { NO, TRUNCATE, ERASE }

    public static Command setTruncated(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        return setTruncated(safeStore, safeCommand, Truncate.TRUNCATE, true);
    }

    public static Command setTruncated(SafeCommandStore safeStore, SafeCommand safeCommand, Truncate truncate, boolean notifyListeners)
    {
        Command command = safeCommand.current();
        Invariants.checkState(!command.is(Truncated));

        //   1) a command has been applied; or
        //   2) has been coordinated but *will not* be applied (we just haven't witnessed the invalidation yet); or
        //   3) a command is durably decided and this shard only hosts its home data, so no explicit truncation is necessary to remove it
        // TODO (desired): consider if there are better invariants we can impose for undecided transactions, to verify they aren't later committed (should be detected already, but more is better)
        Invariants.checkState(command.hasBeen(Applied) || !command.hasBeen(PreCommitted) || command.partialTxn().keys().isEmpty());

        Command.Truncated result = truncated(command);
        safeCommand.set(result);
        safeStore.progressLog().clear(safeCommand.txnId());
        if (notifyListeners)
            safeStore.notifyListeners(safeCommand);

        if (truncate == Truncate.ERASE)
            safeStore.erase(safeCommand);
        return result;
    }

    public static Truncate shouldTruncate(SafeCommandStore safeStore, Command command)
    {
        if (safeStore.commandStore().globalDurability(command.txnId()) == Universal)
            return Truncate.ERASE;

        if (!command.hasBeen(Applied) || !command.known().hasCompleteRoute())
            return Truncate.NO;

        TxnId txnId = command.txnId();
        Timestamp executeAt = command.executeAt() != null && !command.executeAt().equals(Timestamp.NONE) ? command.executeAt() : txnId;
        Route<?> all = command.route();
        Participants<?> participants = all.participants();

        RedundantStatus redundant = safeStore.commandStore().redundantBefore().min(txnId, executeAt, participants);
        if (redundant == LIVE)
            return Truncate.NO;

        Durability min = safeStore.commandStore().durableBefore().min(txnId, all);
        switch (min)
        {
            default:
            case Local:
            case DurableOrInvalidated:
                throw new AssertionError();
            case NotDurable:
                return Truncate.NO;
            case Majority:
                return Truncate.TRUNCATE;
            case Universal:
                return Truncate.ERASE;
        }
    }

    public static boolean maybeTruncate(SafeCommandStore safeStore, SafeCommand safeCommand, Command command)
    {
        Truncate truncate = shouldTruncate(safeStore, command);
        switch (truncate)
        {
            default: throw new AssertionError();
            case NO:
                return false;

            case ERASE:
            case TRUNCATE:
                if (!command.is(Truncated))
                    setTruncated(safeStore, safeCommand, truncate, true);
                else if (truncate == Truncate.ERASE)
                    safeStore.erase(safeCommand);
                return true;
        }
    }

    // TODO (now): either ignore this message if we don't have a route, or else require FullRoute requiring route, or else require FullRoute
    private static Command setDurability(SafeCommandStore safeStore, SafeCommand safeCommand, Durability durability, @Nullable Route<?> route, boolean hasProgressKey, @Nullable RoutingKey progressKey, @Nullable Timestamp executeAt)
    {
        Command command = safeCommand.current();
        if (command.is(Truncated))
            return command;

        if (command.durability().compareTo(durability) >= 0)
            return command;

        Ranges coordinates = safeStore.ranges().coordinates(command.txnId());
        Ranges additionalRanges = Ranges.EMPTY;
        if (command.executeAt() != null && !command.executeAt().equals(Timestamp.NONE))
            additionalRanges = safeStore.ranges().allBetween(command.txnId().epoch(), command.executeAt().epoch());
        CommonAttributes attrs = route == null ? command :
                                 hasProgressKey ? updateRouteAndProgressShard(command, route, progressKey, coordinates, additionalRanges)
                                                : updateRouteOnly(command, route, coordinates, additionalRanges);
        if (executeAt != null && command.status().hasBeen(Committed) && !command.asCommitted().executeAt().equals(executeAt))
            safeStore.agent().onInconsistentTimestamp(command, command.asCommitted().executeAt(), executeAt);
        attrs = attrs.mutable().durability(durability);
        command = safeCommand.updateAttributes(attrs);

        if (maybeTruncate(safeStore, safeCommand, command))
            return safeCommand.current();

        safeStore.progressLog().durable(command);
        safeStore.notifyListeners(safeCommand);
        return command;
    }

    public static Command setDurability(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Durability durability, Route<?> route, boolean hasProgressKey, @Nullable RoutingKey progressKey, @Nullable Timestamp executeAt)
    {
        return setDurability(safeStore, safeCommand, durability, route, hasProgressKey, progressKey, executeAt);
    }

    public static Command setDurability(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Durability durability, Route<?> route, @Nullable Timestamp executeAt)
    {
        return setDurability(safeStore, safeCommand, durability, route, false, null, executeAt);
    }

    public static Command setDurability(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId txnId, Durability durability)
    {
        return setDurability(safeStore, safeCommand, durability, null, false, null, null);
    }

    // TODO (expected): this should integrate with LocalBarrier so that we do not need to register interest with every transaction
    static class NotifyWaitingOn implements PreLoadContext, Consumer<SafeCommandStore>
    {
        Known[] blockedUntil = new Known[4];
        TxnId[] txnIds = new TxnId[4];
        int depth;

        public NotifyWaitingOn(SafeCommand root)
        {
            txnIds[0] = root.txnId();
            blockedUntil[0] = Known.Done;
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            SafeCommand prevSafe = get(safeStore, depth - 1);
            while (depth >= 0)
            {
                Command prev = prevSafe != null ? prevSafe.current() : null;
                SafeCommand curSafe = ifLoaded(safeStore, depth);
                // TODO (now): we want to avoid inserting a new NotWitnessed record for redundant dependencies
                Command cur = curSafe != null ? curSafe.current() : null;
                Known until = blockedUntil[depth];
                if (cur == null)
                {
                    // need to load; schedule execution for later
                    safeStore.commandStore().execute(this, this).begin(safeStore.agent());
                    return;
                }

                if (prev != null)
                {
                    if (cur.has(until) || cur.hasBeen(Truncated) || (cur.hasBeen(PreCommitted) && cur.executeAt().compareTo(prev.executeAt()) > 0 && !prev.txnId().rw().awaitsFutureDeps()))
                    {
                        updateDependencyAndMaybeExecute(safeStore, prevSafe, curSafe, false);
                        --depth;
                        prevSafe = get(safeStore, depth - 1);
                        continue;
                    }
                }
                else if (cur.has(until))
                {
                    // we're done; have already applied
                    Invariants.checkState(depth == 0);
                    break;
                }

                WaitingOn waitingOn = cur.hasBeen(Committed) ? cur.asCommitted().waitingOn() : WaitingOn.EMPTY;
                TxnId directlyBlockedOnCommit;
                TxnId directlyBlockedOnApply = waitingOn.nextWaitingOnApply();
                if (directlyBlockedOnApply != null)
                {
                    // preferentially block on apply, as this probably saves us additional bookkeeping
                    // by giving other dependencies time to complete
                    push(directlyBlockedOnApply, Known.Done);
                    prevSafe = curSafe;
                }
                else if ((directlyBlockedOnCommit = waitingOn.nextWaitingOnCommit()) != null)
                {
                    push(directlyBlockedOnCommit, ExecuteAtOnly);
                    prevSafe = curSafe;
                }
                else
                {
                    if (cur.hasBeen(Committed) && !cur.is(ReadyToExecute) && !cur.is(Applying) && !cur.asCommitted().isWaitingOnDependency())
                    {
                        if (!maybeExecute(safeStore, curSafe, cur.progressShard(), false, false))
                            throw new AssertionError("Is able to Apply, but has not done so");
                        // loop and re-test the command's status; we may still want to notify blocking, esp. if not homeShard
                        continue;
                    }

                    Participants<?> someParticipants = null;
                    if (cur.route() != null)
                    {
                        someParticipants = cur.route().participants();
                    }
                    else if (prev != null)
                    {
                        someParticipants = prev.partialDeps().participants(cur.txnId());
                    }
                    Invariants.checkState(someParticipants != null);
                    if (safeStore.commandStore().isRedundant(cur.txnId(), cur.executeAt(), someParticipants))
                    {
                        if (prevSafe == null)
                        {
                            Invariants.checkState(cur.hasBeen(Applied) || !cur.hasBeen(PreCommitted));
                            return;
                        }

                        curSafe.removeListener(prev.asListener());
                        // we've been invalidated, or we're done
                        if (cur.hasBeen(Applied))
                        {
                            removeDependency(safeStore, prevSafe, cur.txnId());
                        }
                        else
                        {
                            Invariants.checkState(!cur.hasBeen(PreCommitted));
                            removeAndUpdateDependencies(safeStore, prevSafe, cur.txnId());
                        }
                        --depth;
                        prevSafe = get(safeStore, depth - 1);
                    }
                    else
                    {
                        logger.trace("{} blocked on {} until {}", txnIds[0], cur.txnId(), until);
                        safeStore.progressLog().waiting(curSafe, until, null, someParticipants);
                        break;
                    }
                }
            }
            for (int i = 1 ; i <= depth ; ++i)
                get(safeStore, i).addListener(get(safeStore, i - 1).current().asListener());
        }

        private SafeCommand ifLoaded(SafeCommandStore safeStore, int i)
        {
            if (i < 0) return null;
            return safeStore.ifLoadedAndInitialised(txnIds[i]);
        }

        private SafeCommand get(SafeCommandStore safeStore, int i)
        {
            if (i < 0) return null;
            return safeStore.get(txnIds[i]);
        }

        void push(TxnId by, Known until)
        {
            if (++depth == txnIds.length)
            {
                txnIds = Arrays.copyOf(txnIds, txnIds.length * 2);
                blockedUntil = Arrays.copyOf(blockedUntil, txnIds.length);
            }
            txnIds[depth] = by;
            blockedUntil[depth] = until;
        }

        @Override
        public TxnId primaryTxnId()
        {
            return txnIds[0];
        }

        @Override
        public Collection<TxnId> additionalTxnIds()
        {
            return Arrays.asList(txnIds).subList(1, depth + 1);
        }
    }

    static Command removeDependency(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId redundant)
    {
        Command.Committed current = safeCommand.current().asCommitted();
        WaitingOn.Update update = new WaitingOn.Update(current.waitingOn);
        update.removeInvalidatedOrTruncated(redundant);
        return safeCommand.updateWaitingOn(update);
    }

    static Command removeAndUpdateDependencies(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId redundant)
    {
        CommandStore commandStore = safeStore.commandStore();
        Command.Committed current = safeCommand.current().asCommitted();
        WaitingOn.Update update = new WaitingOn.Update(current.waitingOn);
        TxnId minWaitingOnTxnId = update.minWaitingOnTxnId();
        if (minWaitingOnTxnId != null && commandStore.hasRedundantDependencies(update.minWaitingOnTxnId(), current.executeAt(), current.route().participants()))
            safeStore.commandStore().removeRedundantDependencies(current.route().participants(), update);

        update.removeInvalidatedOrTruncated(redundant);
        return safeCommand.updateWaitingOn(update);
    }

    public static Command informHome(SafeCommandStore safeStore, SafeCommand dependencySafeCommand, Route<?> someRoute)
    {
        Command command = dependencySafeCommand.current();
        Invariants.checkState(owns(safeStore, command.txnId().epoch(), someRoute.homeKey()));
        if (command.known().isDefinitionKnown())
            return command;
        return dependencySafeCommand.updateAttributes(command.mutable().route(Route.merge((Route)someRoute, command.route())).progressShard(Home));
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
    public static CommonAttributes updateRouteAndProgressShard(Command command, Route<?> route, @Nullable RoutingKey progressKey, Ranges coordinateRanges, Ranges additionalRanges)
    {
        // TODO (desired): validate progress key remains same
        ProgressShard progressShard = progressShard(route, progressKey, coordinateRanges);
        Invariants.checkState(progressShard == command.progressShard() || command.progressShard() == Unsure);

        if (command.route() == null || !command.route().containsAll(route))
            return command.mutable().route(Route.merge((Route)route, command.route())).progressShard(progressShard);

        if (command.progressShard() != progressShard)
            return command.mutable().progressShard(progressShard);

        return command;
    }

    public static CommonAttributes updateRouteOnly(Command command, Route<?> route, Ranges coordinateRanges, Ranges additionalRanges)
    {
        // TODO (desired): validate progress key remains same
        if (command.route() == null || !command.route().containsAll(route))
            return command.mutable().route(Route.merge((Route)route, command.route()));
        return command;
    }

    private static ProgressShard progressShard(Route<?> route, @Nullable RoutingKey progressKey, Ranges coordinates)
    {
        if (progressKey == null)
            return No;

        if (!coordinates.contains(progressKey))
            return No;

        return progressKey.equals(route.homeKey()) ? Home : Local;
    }

    enum EnsureAction { Ignore, Check, Add, TrySet, Set }

    private static CommonAttributes set(SafeCommandStore safeStore, Command command, CommonAttributes attrs,
                                        Ranges existingRanges, Ranges additionalRanges, ProgressShard shard, Route<?> route,
                                        @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                                        @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        Invariants.checkState(attrs.progressShard() != Unsure);
        Ranges allRanges = existingRanges.with(additionalRanges);

        attrs = attrs.mutable().route(Route.merge(attrs.route(), (Route)route));

        // TODO (soon): stop round-robin hashing; partition only on ranges
        switch (ensurePartialTxn)
        {
            case Add:
                if (partialTxn == null)
                    break;

                if (attrs.partialTxn() != null)
                {
                    partialTxn = partialTxn.slice(allRanges, shard.isHome());
                    if (!command.txnId().rw().isLocal())
                    {
                        Invariants.checkState(attrs.partialTxn().covers(existingRanges));
                        Routables.foldl(partialTxn.keys(), additionalRanges, (keyOrRange, p, v, i) -> {
                            // TODO (expected, efficiency): we may register the same ranges more than once
                            safeStore.register(keyOrRange, allRanges, command);
                            return v;
                        }, 0, 0, 1);
                    }
                    attrs = attrs.mutable().partialTxn(attrs.partialTxn().with(partialTxn));
                    break;
                }

            case Set:
            case TrySet:
                // TODO (expected): only includeQuery if shard.isHome(); this affects state eviction and is low priority given size in C*
                attrs = attrs.mutable().partialTxn(partialTxn = partialTxn.slice(allRanges, true));
                // TODO (expected, efficiency): we may register the same ranges more than once
                // TODO (desirable, efficiency): no need to register on PreAccept if already Accepted
                if (!command.txnId().rw().isLocal())
                    safeStore.register(partialTxn.keys(), allRanges, command);
                break;
        }

        switch (ensurePartialDeps)
        {
            case Add:
                if (partialDeps == null)
                    break;

                if (attrs.partialDeps() != null)
                {
                    attrs = attrs.mutable().partialDeps(attrs.partialDeps().with(partialDeps.slice(allRanges)));
                    break;
                }

            case Set:
            case TrySet:
                attrs = attrs.mutable().partialDeps(partialDeps.slice(allRanges));
                break;
        }
        return attrs;
    }

    /**
     * Validate we have sufficient information for the route, partialTxn and partialDeps fields, and if so update them;
     * otherwise return false (or throw an exception if an illegal state is encountered)
     */
    private static boolean validate(Status status, CommonAttributes attrs, Ranges existingRanges, Ranges additionalRanges, ProgressShard shard,
                                    Route<?> route, EnsureAction ensureRoute,
                                    @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                                    @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        if (shard == Unsure)
            return false;

        // first validate route
        switch (ensureRoute)
        {
            default: throw new AssertionError();
            case Check:
                if (!isFullRoute(attrs.route()) && !isFullRoute(route))
                    return false;
            case Ignore:
                break;
            case Add:
            case Set:
                if (!isFullRoute(route))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") sent to home shard");
                break;
            case TrySet:
                if (!isFullRoute(route))
                    return false;
        }

        // invalid to Add deps to Accepted or AcceptedInvalidate statuses, as Committed deps are not equivalent
        // and we may erroneously believe we have covered a wider range than we have infact covered
        if (ensurePartialDeps == Add)
            Invariants.checkState(status != Accepted && status != AcceptedInvalidate);

        // validate new partial txn
        if (!validate(ensurePartialTxn, existingRanges, additionalRanges, covers(attrs.partialTxn()), covers(partialTxn), "txn", partialTxn))
            return false;

        Invariants.checkState(partialTxn == null || attrs.txnId().rw() == null || attrs.txnId().rw().equals(partialTxn.kind()), "Transaction has different kind to its TxnId");
        Invariants.checkState(!shard.isHome() || ensurePartialTxn == Ignore || hasQuery(attrs.partialTxn()) || hasQuery(partialTxn), "Home transaction should include query");

        return validate(ensurePartialDeps, existingRanges, additionalRanges, covers(attrs.partialDeps()), covers(partialDeps), "deps", partialDeps);
    }

    // FIXME (immutable-state): has this been removed?
    private static boolean validate(EnsureAction action, Ranges existingRanges, Ranges additionalRanges,
                                    Ranges existing, Ranges adding, String kind, Object obj)
    {
        switch (action)
        {
            default: throw new IllegalStateException();
            case Ignore:
                break;

            case TrySet:
                if (adding != null)
                {
                    if (!adding.containsAll(existingRanges))
                        return false;

                    if (additionalRanges != existingRanges && !adding.containsAll(additionalRanges))
                        return false;

                    break;
                }
            case Set:
                // failing any of these tests is always an illegal state
                Invariants.checkState(adding != null);
                if (!adding.containsAll(existingRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + existingRanges);

                if (additionalRanges != existingRanges && !adding.containsAll(additionalRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + additionalRanges);
                break;

            case Check:
            case Add:
                if (adding == null)
                {
                    if (existing == null)
                        return false;

                    Invariants.checkState(existing.containsAll(existingRanges));
                    if (existingRanges != additionalRanges && !existing.containsAll(additionalRanges))
                    {
                        if (action == Check)
                            return false;

                        throw new IllegalArgumentException("Missing additional " + kind + "; existing does not cover " + additionalRanges.subtract(existingRanges));
                    }
                }
                else if (existing != null)
                {
                    Ranges covering = adding.with(existing);
                    Invariants.checkState(covering.containsAll(existingRanges));
                    if (existingRanges != additionalRanges && !covering.containsAll(additionalRanges))
                    {
                        if (action == Check)
                            return false;

                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.subtract(existingRanges));
                    }
                }
                else
                {
                    if (!adding.containsAll(existingRanges))
                        return false;

                    if (existingRanges != additionalRanges && !adding.containsAll(additionalRanges))
                    {
                        if (action == Check)
                            return false;

                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.subtract(existingRanges));
                    }
                }
                break;
        }

        return true;
    }
}
