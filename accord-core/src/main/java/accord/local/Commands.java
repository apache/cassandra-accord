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
import java.util.BitSet;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ProgressLog.ProgressShard;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Command.WaitingOn;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
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
import accord.utils.SortedArrays;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import net.nicoulaj.compilecommand.annotations.Inline;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.api.ProgressLog.ProgressShard.No;
import static accord.api.ProgressLog.ProgressShard.UnmanagedHome;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.local.Commands.EnsureAction.Add;
import static accord.local.Commands.EnsureAction.Check;
import static accord.local.Commands.EnsureAction.Ignore;
import static accord.local.Commands.EnsureAction.Set;
import static accord.local.Commands.EnsureAction.TrySet;
import static accord.local.Status.Accepted;
import static accord.local.Status.AcceptedInvalidate;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Durability;
import static accord.local.Status.Invalidated;
import static accord.local.Status.Known;
import static accord.local.Status.Known.ExecuteAtOnly;
import static accord.local.Status.PreApplied;
import static accord.local.Status.PreCommitted;
import static accord.local.Status.ReadyToExecute;
import static accord.primitives.Routables.Slice.Minimal;
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

    public static RoutingKey noProgressKey()
    {
        return NO_PROGRESS_KEY;
    }

    public enum AcceptOutcome {Success, Redundant, RejectedBallot}

    public static AcceptOutcome preaccept(SafeCommandStore safeStore, TxnId txnId, long acceptEpoch, PartialTxn partialTxn, Route<?> route, @Nullable RoutingKey progressKey)
    {
        return preacceptOrRecover(safeStore, txnId, acceptEpoch, partialTxn, route, progressKey, Ballot.ZERO);
    }

    public static AcceptOutcome recover(SafeCommandStore safeStore, TxnId txnId, PartialTxn partialTxn, Route<?> route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        // for recovery we only ever propose either the original epoch or an Accept that we witness; otherwise we invalidate
        return preacceptOrRecover(safeStore, txnId, txnId.epoch(), partialTxn, route, progressKey, ballot);
    }

    private static AcceptOutcome preacceptOrRecover(SafeCommandStore safeStore, TxnId txnId, long acceptEpoch, PartialTxn partialTxn, Route<?> route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        SafeCommand safeCommand = safeStore.command(txnId);
        Command command = safeCommand.current();

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
        CommonAttributes attrs = updateHomeAndProgressKeys(safeStore, command.txnId(), command, route, progressKey, coordinateRanges);
        ProgressShard shard = progressShard(attrs, progressKey, coordinateRanges);
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
            Timestamp executeAt = ballot.equals(Ballot.ZERO)
                    ? safeStore.commandStore().preaccept(txnId, partialTxn.keys(), safeStore)
                    : safeStore.time().uniqueNow(txnId);
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

    public static boolean preacceptInvalidate(SafeCommandStore safeStore, TxnId txnId, Ballot ballot)
    {
        SafeCommand safeCommand = safeStore.command(txnId);
        Command command = safeCommand.current();
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
        SafeCommand safeCommand = safeStore.command(txnId);
        Command command = safeCommand.current();
        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept - witnessed higher ballot ({} > {})", txnId, command.promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        if (command.hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping accept - already committed ({})", txnId, command.status());
            return AcceptOutcome.Redundant;
        }

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Ranges acceptRanges = acceptRanges(safeStore, txnId, executeAt, coordinateRanges);
        Invariants.checkState(!acceptRanges.isEmpty());

        CommonAttributes attrs = updateHomeAndProgressKeys(safeStore, command.txnId(), command, route, progressKey, coordinateRanges);
        ProgressShard shard = progressShard(attrs, progressKey, coordinateRanges);
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
        Command command = safeCommand.current();
        if (command.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept invalidated - witnessed higher ballot ({} > {})", command.txnId(), command.promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        if (command.hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping accept invalidated - already committed ({})", command.txnId(), command.status());
            return AcceptOutcome.Redundant;
        }

        logger.trace("{}: accepted invalidated", command.txnId());

        safeCommand.acceptInvalidated(ballot);
        safeStore.notifyListeners(safeCommand);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome {Success, Redundant, Insufficient;}


    // relies on mutual exclusion for each key
    public static CommitOutcome commit(SafeCommandStore safeStore, TxnId txnId, Route<?> route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        SafeCommand safeCommand = safeStore.command(txnId);
        Command command = safeCommand.current();

        if (command.hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping commit - already committed ({})", txnId, command.status());
            if (!executeAt.equals(command.executeAt()) || command.status() == Invalidated)
                safeStore.agent().onInconsistentTimestamp(command, (command.status() == Invalidated ? Timestamp.NONE : command.executeAt()), executeAt);

            if (command.hasBeen(Committed))
                return CommitOutcome.Redundant;
        }

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Ranges acceptRanges = acceptRanges(safeStore, txnId, executeAt, coordinateRanges);
        // TODO (expected, consider): consider ranges between coordinateRanges and executeRanges? Perhaps don't need them
        Ranges executeRanges = executeRanges(safeStore, executeAt);

        CommonAttributes attrs = updateHomeAndProgressKeys(safeStore, command.txnId(), command, route, progressKey, coordinateRanges);
        ProgressShard shard = progressShard(attrs, progressKey, coordinateRanges);

        if (!validate(command.status(), attrs, acceptRanges, executeRanges, shard, route, Check, partialTxn, Add, partialDeps, Set))
        {
            safeCommand.updateAttributes(attrs);
            return CommitOutcome.Insufficient;
        }

        // FIXME: split up set
        attrs = set(safeStore, command, attrs, acceptRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);

        logger.trace("{}: committed with executeAt: {}, deps: {}", txnId, executeAt, partialDeps);
        WaitingOn waitingOn = populateWaitingOn(safeStore, txnId, executeAt, partialDeps);
        command = safeCommand.commit(attrs, executeAt, waitingOn);

        safeStore.progressLog().committed(command, shard);

        // TODO (expected, safety): introduce intermediate status to avoid reentry when notifying listeners (which might notify us)
        maybeExecute(safeStore, safeCommand, shard, true, true);
        return CommitOutcome.Success;
    }

    // relies on mutual exclusion for each key
    public static void precommit(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt)
    {
        SafeCommand safeCommand = safeStore.command(txnId);
        Command command = safeCommand.current();
        if (command.hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping precommit - already committed ({})", txnId, command.status());
            if (executeAt.equals(command.executeAt()) && command.status() != Invalidated)
                return;

            safeStore.agent().onInconsistentTimestamp(command, (command.status() == Invalidated ? Timestamp.NONE : command.executeAt()), executeAt);
        }

        safeCommand.precommit(executeAt);
        safeStore.notifyListeners(safeCommand);
        logger.trace("{}: precommitted with executeAt: {}", txnId, executeAt);
    }

    public static void commitRecipientLocalSyncPoint(SafeCommandStore safeStore, TxnId localSyncId, SyncPoint syncPoint, Seekables<?, ?> keys)
    {
        SafeCommand safeCommand = safeStore.command(localSyncId);
        Command command = safeCommand.current();
        Invariants.checkState(!command.hasBeen(Committed));
        commitRecipientLocalSyncPoint(safeStore, localSyncId, keys, syncPoint.route);
    }

    private static void commitRecipientLocalSyncPoint(SafeCommandStore safeStore, TxnId localSyncId, Seekables<?, ?> keys, Route<?> route)
    {
        SafeCommand safeCommand = safeStore.command(localSyncId);
        Command command = safeCommand.current();
        if (command.hasBeen(Committed))
            return;

        Ranges coordinateRanges = coordinateRanges(safeStore, localSyncId);
        // TODO (desired, consider): in the case of sync points, the coordinator is unlikely to be a home shard, do we mind this? should document at least
        ProgressShard progressShard = coordinateRanges.contains(route.homeKey()) ? UnmanagedHome : No;
        Txn emptyTxn = safeStore.agent().emptyTxn(localSyncId.rw(), keys);
        CommonAttributes newAttributes = new CommonAttributes.Mutable(command)
                                         .progressKey(progressShard == UnmanagedHome ? route.homeKey() : NO_PROGRESS_KEY);
        PartialDeps none = Deps.NONE.slice(coordinateRanges);
        PartialTxn partialTxn = emptyTxn.slice(coordinateRanges, true);
        Invariants.checkState(validate(command.status(), newAttributes, Ranges.EMPTY, coordinateRanges, progressShard, route, Set, partialTxn, Set, none, Set));
        newAttributes = set(safeStore, command, newAttributes, Ranges.EMPTY, coordinateRanges, progressShard, route, partialTxn, Set, none, Set);
        safeCommand.commit(newAttributes, localSyncId, WaitingOn.EMPTY);
        safeStore.notifyListeners(safeCommand);
    }

    public static void applyRecipientLocalSyncPoint(SafeCommandStore safeStore, TxnId localSyncId, Seekables<?, ?> keys)
    {
        SafeCommand safeCommand = safeStore.command(localSyncId);
        Command.Committed command = safeCommand.current().asCommitted();
        if (command.hasBeen(PreApplied))
            return;

        // NOTE: if this is ever made a non-empty txn this will introduce a potential bug where the txn is registered against CommandsForKeys
        Txn emptyTxn = safeStore.agent().emptyTxn(localSyncId.rw(), keys);
        safeCommand.preapplied(command, command.executeAt(), command.waitingOn(), emptyTxn.execute(localSyncId, localSyncId, null), emptyTxn.result(localSyncId, localSyncId, null));
        maybeExecute(safeStore, safeCommand, Unsure, true, false);
    }

    // TODO (expected, ?): commitInvalidate may need to update cfks _if_ possible
    public static void commitInvalidate(SafeCommandStore safeStore, TxnId txnId)
    {
        SafeCommand safeCommand = safeStore.command(txnId);
        Command command = safeCommand.current();
        if (command.hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping commit invalidated - already committed ({})", txnId, command.status());
            if (!command.hasBeen(Invalidated))
                safeStore.agent().onInconsistentTimestamp(command, Timestamp.NONE, command.executeAt());

            return;
        }

        ProgressShard shard = progressShard(safeStore, command);
        safeStore.progressLog().invalidated(command, shard);

        CommonAttributes attrs = command;
        if (command.partialDeps() == null)
            attrs = attrs.mutable().partialDeps(PartialDeps.NONE);
        safeCommand.commitInvalidated(attrs, txnId);
        logger.trace("{}: committed invalidated", txnId);

        safeStore.notifyListeners(safeCommand);
    }

    public enum ApplyOutcome {Success, Redundant, Insufficient}

    public static ApplyOutcome apply(SafeCommandStore safeStore, TxnId txnId, long untilEpoch, Route<?> route, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        SafeCommand safeCommand = safeStore.command(txnId);
        Command command = safeCommand.current();
        if (command.hasBeen(PreApplied) && executeAt.equals(command.executeAt()))
        {
            logger.trace("{}: skipping apply - already executed ({})", txnId, command.status());
            return ApplyOutcome.Redundant;
        }
        else if (command.hasBeen(PreCommitted) && !executeAt.equals(command.executeAt()))
        {
            safeStore.agent().onInconsistentTimestamp(command, command.executeAt(), executeAt);
        }

        Ranges coordinateRanges = coordinateRanges(safeStore, txnId);
        Ranges acceptRanges = acceptRanges(safeStore, txnId, executeAt, coordinateRanges);
        Ranges executeRanges = executeRanges(safeStore, executeAt);
        if (untilEpoch < safeStore.latestEpoch())
        {
            Ranges expectedRanges = safeStore.ranges().allBetween(executeAt.epoch(), untilEpoch);
            Invariants.checkState(expectedRanges.containsAll(executeRanges));
        }

        CommonAttributes attrs = updateHomeAndProgressKeys(safeStore, command.txnId(), command, route, coordinateRanges);
        ProgressShard shard = progressShard(attrs, coordinateRanges);

        if (!validate(command.status(), attrs, acceptRanges, executeRanges, shard, route, Check, null, Check, partialDeps, command.hasBeen(Committed) ? Add : TrySet))
        {
            safeCommand.updateAttributes(attrs);
            return ApplyOutcome.Insufficient; // TODO (expected, consider): this should probably be an assertion failure if !TrySet
        }

        WaitingOn waitingOn = !command.hasBeen(Committed) ? populateWaitingOn(safeStore, txnId, executeAt, partialDeps) : command.asCommitted().waitingOn();
        attrs = set(safeStore, command, attrs, acceptRanges, executeRanges, shard, route, null, Check, partialDeps, command.hasBeen(Committed) ? Add : TrySet);

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
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     listener.txnId(), updated.txnId(), updated.status(), updated);
        switch (updated.status())
        {
            default:
                throw new IllegalStateException("Unexpected status: " + updated.status());
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                break;

            case PreCommitted:
            case Committed:
            case ReadyToExecute:
            case PreApplied:
            case Applied:
            case Invalidated:
                updateDependencyAndMaybeExecute(safeStore, safeListener, safeUpdated, true);
                break;
        }
    }

    protected static void postApply(SafeCommandStore safeStore, TxnId txnId)
    {
        logger.trace("{} applied, setting status to Applied and notifying listeners", txnId);
        SafeCommand safeCommand = safeStore.command(txnId);
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
        return safeStore.ranges().allSince(executeAt.epoch());
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
        Command.Executed command = safeStore.command(txnId).current().asExecuted();
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

    protected static WaitingOn populateWaitingOn(SafeCommandStore safeStore, TxnId txnId, Timestamp executeAt, PartialDeps partialDeps)
    {
        Ranges ranges = applyRanges(safeStore, executeAt);
        if (ranges.isEmpty())
            return WaitingOn.EMPTY;

        return populateWaitingOn(safeStore, ranges, txnId, executeAt, partialDeps);
    }

    protected static <P> void visitWaitingOn(SafeCommandStore safeStore, TxnId txnId, Timestamp waitUntil, Deps waitOn, Timestamp executeAt, WaitingOnVisitor<P> visitor, P param)
    {
        Ranges ranges = applyRanges(safeStore, executeAt);
        if (ranges.isEmpty())
            return;

        visitWaitingOn(safeStore, ranges, txnId, waitUntil, waitOn, visitor, param);
    }

    protected static WaitingOn populateWaitingOn(SafeCommandStore safeStore, Ranges ranges, TxnId waitingId, Timestamp executeAt, PartialDeps partialDeps)
    {
        WaitingOn.Update update = new WaitingOn.Update();
        visitWaitingOn(safeStore, ranges, waitingId, executeAt, partialDeps, Commands::populateWaitingOn, update);
        return update.build();
    }

    public interface WaitingOnVisitor<P>
    {
        void visit(SafeCommandStore safeStore, TxnId waitingId, Timestamp executeAt, TxnId dependencyId, P param);
    }

    protected static <P> void visitWaitingOn(SafeCommandStore safeStore, Ranges ranges, TxnId waitingId, Timestamp waitUntil, Deps deps, WaitingOnVisitor<P> visitor, P param)
    {
        boolean isAffectedByBootstrap = safeStore.commandStore().isAffectedByBootstrap(deps);
        if (!isAffectedByBootstrap)
        {
            deps.forEachUniqueTxnId(ranges, dependencyId -> visitor.visit(safeStore, waitingId, waitUntil, dependencyId, param));
            return;
        }

        BitSet bits = new BitSet(Math.max(deps.keyDeps.txnIdCount(), deps.rangeDeps.txnIdCount()));
        if (!deps.keyDeps.isEmpty())
        {
            SortedArrays.SortedArrayList<TxnId> dependencyIds = deps.keyDeps.txnIds();
            // process each interval of TxnId we have a different incomplete range for in a batch,
            safeStore.commandStore().forEachBootstrapRange(dependencyIds, (kdeps, bootstrapId, rs, start, end) -> {
                // We do not need to depend on the bootstrap transaction for writes, as we have a timestamp store
                // (so any write we perform will persist past the bootstrap completing).
                // For most reads we can rely on safeToRead, but this is not the case for reads that depend on
                // writes that started before our ExclusiveSyncPoint but execute afterwards.
                // We must retain a dependency on these transactions.

                rs = ranges.slice(rs, Minimal);
                kdeps.forEach(rs, (bs, txnId, i) -> {
                    if (i < end && i >= start)
                        bs.set(i);
                }, bits);
            }, deps.keyDeps);
            int i = -1;
            while ((i = bits.nextSetBit(i + 1)) >= 0)
                visitor.visit(safeStore, waitingId, waitUntil, dependencyIds.get(i), param);
        }

        if (!deps.rangeDeps.isEmpty())
        {
            bits.clear();
            SortedArrays.SortedArrayList<TxnId> dependencyIds = deps.rangeDeps.txnIds();
            safeStore.commandStore().forEachBootstrapRange(dependencyIds, (rdeps, bootstrapId, rs, start, end) -> {
                rs = ranges.slice(rs, Minimal);
                rdeps.forEach(rs, (bs, i) -> {
                    if (i >= start && i < end)
                        bs.set(i);
                }, bits);
            }, deps.rangeDeps);
            int i = -1;
            while ((i = bits.nextSetBit(i + 1)) >= 0)
                visitor.visit(safeStore, waitingId, waitUntil, dependencyIds.get(i), param);
        }
    }

    @Inline
    private static void populateWaitingOn(SafeCommandStore safeStore, TxnId waitingId, Timestamp executeAt, TxnId dependencyId, WaitingOn.Update update)
    {
        SafeCommand dependencySafeCommand = safeStore.ifLoaded(dependencyId);
        if (dependencySafeCommand == null)
        {
            update.addWaitingOnCommit(dependencyId);
            safeStore.addAndInvokeListener(dependencyId, waitingId);
        }
        else
        {
            Command command = dependencySafeCommand.current();
            switch (command.status())
            {
                default:
                    throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                case PreCommitted:
                    // we don't know when these dependencies will execute, and cannot execute until we do

                    command = dependencySafeCommand.addListener(new Command.ProxyListener(waitingId));
                    update.addWaitingOnCommit(command.txnId());
                    break;
                case Committed:
                    // TODO (desired, efficiency): split into ReadyToRead and ReadyToWrite;
                    //                             the distributed read can be performed as soon as those keys are ready,
                    //                             and in parallel with any other reads. the client can even ACK immediately after;
                    //                             only the write needs to be postponed until other in-progress reads complete
                case ReadyToExecute:
                case PreApplied:
                    command = dependencySafeCommand.addListener(new Command.ProxyListener(waitingId));
                    insertWaitingOn(waitingId, executeAt, update, command);
                case Applied:
                case Invalidated:
                    break;
            }
        }
    }

    private static void insertWaitingOn(TxnId txnId, Timestamp executeAt, WaitingOn.Update update, Command dependencyId)
    {
        Invariants.checkState(dependencyId.hasBeen(Committed));
        if (dependencyId.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Do not insert.", txnId, dependencyId.txnId());
        }
        else if (dependencyId.executeAt().compareTo(executeAt) > 0)
        {
            // dependency cannot be a predecessor if it executes later
            logger.trace("{}: {} executes after us. Do not insert.", txnId, dependencyId.txnId());
        }
        else if (dependencyId.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Do not insert.", txnId, dependencyId.txnId());
        }
        else
        {
            logger.trace("{}: adding {} to waiting on apply set.", txnId, dependencyId.txnId());
            update.addWaitingOnApply(dependencyId.txnId(), dependencyId.executeAt());
        }
    }

    /**
     * @param safeDependency is either committed or invalidated
     * @return true iff {@code maybeExecute} might now have a different outcome
     */
    private static boolean updateWaitingOn(SafeCommand dependencySafeCommand, WaitingOn.Update waitingOn, SafeCommand safeDependency)
    {
        Command.Committed command = dependencySafeCommand.current().asCommitted();
        Command dependency = safeDependency.current();
        Invariants.checkState(dependency.hasBeen(PreCommitted));
        if (dependency.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", command.txnId(), dependency.txnId());
            safeDependency.removeListener(command.asListener());
            waitingOn.removeWaitingOnCommit(dependency.txnId());
            return true;
        }
        else if (dependency.executeAt().compareTo(command.executeAt()) > 0)
        {
            // dependency cannot be a predecessor if it executes later
            logger.trace("{}: {} executes after us. Stop listening and removing from waiting on apply set.", command.txnId(), dependency.txnId());
            waitingOn.removeWaitingOn(dependency.txnId(), dependency.executeAt());
            safeDependency.removeListener(command.asListener());
            return true;
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Stop listening and removing from waiting on apply set.", command.txnId(), dependency.txnId());
            waitingOn.removeWaitingOn(dependency.txnId(), dependency.executeAt());
            safeDependency.removeListener(command.asListener());
            return true;
        }
        else if (command.isWaitingOnDependency())
        {
            logger.trace("{}: adding {} to waiting on apply set.", command.txnId(), dependency.txnId());
            waitingOn.addWaitingOnApply(dependency.txnId(), dependency.executeAt());
            waitingOn.removeWaitingOnCommit(dependency.txnId());
            return false;
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    static void updateDependencyAndMaybeExecute(SafeCommandStore safeStore, SafeCommand dependencySafeCommand, SafeCommand livePredecessor, boolean notifyWaitingOn)
    {
        Command.Committed command = dependencySafeCommand.current().asCommitted();
        if (command.hasBeen(Applied))
            return;

        WaitingOn.Update waitingOn = new WaitingOn.Update(command);
        boolean attemptExecution = updateWaitingOn(dependencySafeCommand, waitingOn, livePredecessor);
        command = dependencySafeCommand.updateWaitingOn(waitingOn);

        if (attemptExecution)
            maybeExecute(safeStore, dependencySafeCommand, progressShard(safeStore, command), false, notifyWaitingOn);
    }

    // TODO (now): check/move methods below
    private static Command setDurability(SafeCommandStore safeStore, SafeCommand dependencySafeCommand, Durability durability, RoutingKey homeKey, @Nullable Timestamp executeAt)
    {
        Command command = dependencySafeCommand.current();
        CommonAttributes attrs = updateHomeKey(safeStore, command.txnId(), command, homeKey);
        if (executeAt != null && command.status().hasBeen(Committed) && !command.asCommitted().executeAt().equals(executeAt))
            safeStore.agent().onInconsistentTimestamp(command, command.asCommitted().executeAt(), executeAt);
        attrs = attrs.mutable().durability(durability);
        return dependencySafeCommand.updateAttributes(attrs);
    }

    public static Command setDurability(SafeCommandStore safeStore, TxnId txnId, Durability durability, RoutingKey homeKey, @Nullable Timestamp executeAt)
    {
        return setDurability(safeStore, safeStore.command(txnId), durability, homeKey, executeAt);
    }

    private static TxnId firstWaitingOnCommit(Command command)
    {
        if (!command.hasBeen(Committed))
            return null;

        Command.Committed committed = command.asCommitted();
        return committed.isWaitingOnCommit() ? committed.waitingOnCommit().first() : null;
    }

    private static TxnId firstWaitingOnApply(Command command, @Nullable TxnId ifExecutesBefore)
    {
        if (!command.hasBeen(Committed))
            return null;

        Command.Committed committed = command.asCommitted();
        if (!committed.isWaitingOnApply())
            return null;

        Map.Entry<Timestamp, TxnId> first = committed.waitingOnApply().firstEntry();
        if (ifExecutesBefore == null || first.getKey().compareTo(ifExecutesBefore) < 0)
            return first.getValue();

        return null;
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
                    if (cur.has(until) || (cur.hasBeen(PreCommitted) && cur.executeAt().compareTo(prev.executeAt()) > 0))
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

                TxnId directlyBlockedOnCommit = firstWaitingOnCommit(cur);
                TxnId directlyBlockedOnApply = firstWaitingOnApply(cur, directlyBlockedOnCommit);
                if (directlyBlockedOnApply != null)
                {
                    push(directlyBlockedOnApply, Known.Done);
                }
                else if (directlyBlockedOnCommit != null)
                {
                    push(directlyBlockedOnCommit, ExecuteAtOnly);
                }
                else
                {
                    if (cur.hasBeen(Committed) && !cur.hasBeen(ReadyToExecute) && !cur.asCommitted().isWaitingOnDependency())
                    {
                        if (!maybeExecute(safeStore, curSafe, progressShard(safeStore, cur), false, false))
                            throw new AssertionError("Is able to Apply, but has not done so");
                        // loop and re-test the command's status; we may still want to notify blocking, esp. if not homeShard
                        continue;
                    }

                    Unseekables<?, ?> someKeys = cur.maxUnseekables();
                    if (someKeys == null && prev != null) someKeys = prev.partialDeps().someUnseekables(cur.txnId());
                    Invariants.checkState(someKeys != null);
                    logger.trace("{} blocked on {} until {}", txnIds[0], cur.txnId(), until);
                    safeStore.progressLog().waiting(cur.txnId(), until, someKeys);
                    return;
                }
                prevSafe = curSafe;
            }
        }

        private SafeCommand ifLoaded(SafeCommandStore safeStore, int i)
        {
            if (i < 0) return null;
            return safeStore.ifLoaded(txnIds[i]);
        }

        private SafeCommand get(SafeCommandStore safeStore, int i)
        {
            if (i < 0) return null;
            return safeStore.command(txnIds[i]);
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

        @Override
        public Seekables<?, ?> keys()
        {
            return Keys.EMPTY;
        }
    }

    public static Command updateHomeKey(SafeCommandStore safeStore, SafeCommand dependencySafeCommand, RoutingKey homeKey)
    {
        Command command = dependencySafeCommand.current();
        CommonAttributes attrs = updateHomeKey(safeStore, command.txnId(), command, homeKey);
        return dependencySafeCommand.updateAttributes(attrs);
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
    public static CommonAttributes updateHomeKey(SafeCommandStore safeStore, TxnId txnId, CommonAttributes attrs, RoutingKey homeKey)
    {
        if (attrs.homeKey() == null)
        {
            attrs = attrs.mutable().homeKey(homeKey);
            // TODO (low priority, safety): if we're processed on a node that does not know the latest epoch,
            //      do we guarantee the home key calculation is unchanged since the prior epoch?
            if (attrs.progressKey() == null && owns(safeStore, txnId.epoch(), homeKey))
                attrs = attrs.mutable().progressKey(homeKey);
        }
        else if (!attrs.homeKey().equals(homeKey))
        {
            throw new IllegalStateException();
        }
        return attrs;
    }

    private static CommonAttributes updateHomeAndProgressKeys(SafeCommandStore safeStore, TxnId txnId, CommonAttributes attrs, Route<?> route, @Nullable RoutingKey progressKey, Ranges coordinateRanges)
    {
        attrs = updateHomeKey(safeStore, txnId, attrs, route.homeKey());
        if (progressKey == null || progressKey == NO_PROGRESS_KEY)
        {
            if (attrs.progressKey() == null)
                attrs = attrs.mutable().progressKey(NO_PROGRESS_KEY);
            return attrs;
        }
        if (attrs.progressKey() == null) attrs = attrs.mutable().progressKey(progressKey);
        else if (!attrs.progressKey().equals(progressKey))
            throw new AssertionError();
        return attrs;
    }

    private static CommonAttributes updateHomeAndProgressKeys(SafeCommandStore safeStore, TxnId txnId, CommonAttributes attrs, Route<?> route, Ranges coordinateRanges)
    {
        if (attrs.progressKey() == null)
            return attrs;

        return updateHomeAndProgressKeys(safeStore, txnId, attrs, route, attrs.progressKey(), coordinateRanges);
    }

    private static ProgressShard progressShard(CommonAttributes attrs, @Nullable RoutingKey progressKey, Ranges coordinateRanges)
    {
        if (progressKey == null || progressKey == NO_PROGRESS_KEY)
            return No;

        if (!coordinateRanges.contains(progressKey))
            return No;

        return progressKey.equals(attrs.homeKey()) ? Home : Local;
    }

    private static ProgressShard progressShard(CommonAttributes attrs, Ranges coordinateRanges)
    {
        if (attrs.progressKey() == null)
            return Unsure;

        return progressShard(attrs, attrs.progressKey(), coordinateRanges);
    }

    private static ProgressShard progressShard(SafeCommandStore safeStore, Command command)
    {
        RoutingKey progressKey = command.progressKey();
        if (progressKey == null)
            return Unsure;

        if (progressKey == noProgressKey())
            return No;

        Ranges coordinateRanges = safeStore.ranges().coordinates(command.txnId());
        if (!coordinateRanges.contains(progressKey))
            return No;

        return progressKey.equals(command.homeKey()) ? Home : Local;
    }

    enum EnsureAction { Ignore, Check, Add, TrySet, Set }

    private static CommonAttributes set(SafeCommandStore safeStore, Command command, CommonAttributes attrs,
                                        Ranges existingRanges, Ranges additionalRanges, ProgressShard shard, Route<?> route,
                                        @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                                        @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        Invariants.checkState(attrs.progressKey() != null);
        Ranges allRanges = existingRanges.with(additionalRanges);

        if (shard.isHome() || shard.isProgress())
            attrs = attrs.mutable().route(Route.merge(attrs.route(), (Route)route));
        else
            attrs = attrs.mutable().route(route.slice(allRanges));

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
                        Routables.foldlMissing((Seekables)partialTxn.keys(), attrs.partialTxn().keys(), (keyOrRange, p, v, i) -> {
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
                attrs = attrs.mutable().partialTxn(partialTxn = partialTxn.slice(allRanges, shard.isHome()));
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
        if (shard.isHome())
        {
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
        }
        else
        {
            // failing any of these tests is always an illegal state
            if (!route.covers(existingRanges))
                return false;

            if (existingRanges != additionalRanges && !route.covers(additionalRanges))
                throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
        }

        // invalid to Add deps to Accepted or AcceptedInvalidate statuses, as Committed deps are not equivalent
        // and we may erroneously believe we have covered a wider range than we have infact covered
        if (ensurePartialDeps == Add)
            Invariants.checkState(status != Accepted && status != AcceptedInvalidate);

        // validate new partial txn
        if (!validate(ensurePartialTxn, existingRanges, additionalRanges, covers(attrs.partialTxn()), covers(partialTxn), "txn", partialTxn))
            return false;

        if (partialTxn != null && attrs.txnId().rw() != null && !attrs.txnId().rw().equals(partialTxn.kind()))
            throw new IllegalArgumentException("Transaction has different kind to its TxnId");

        if (shard.isHome() && ensurePartialTxn != Ignore)
        {
            if (!hasQuery(attrs.partialTxn()) && !hasQuery(partialTxn))
                throw new IllegalStateException();
        }

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

                        throw new IllegalArgumentException("Missing additional " + kind + "; existing does not cover " + additionalRanges.difference(existingRanges));
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

                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
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

                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
                    }
                }
                break;
        }

        return true;
    }

    // TODO (low priority, API): this is an ugly hack, need to encode progress/homeKey/Route state combinations much more clearly
    //                           (perhaps introduce encapsulating class representing each possible arrangement)
    static class NoProgressKey implements RoutingKey
    {
        @Override
        public int compareTo(@Nonnull RoutableKey that)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Range asRange()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static final NoProgressKey NO_PROGRESS_KEY = new NoProgressKey();
}
