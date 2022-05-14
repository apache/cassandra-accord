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

import accord.api.*;
import accord.local.Status.Durability;
import accord.local.Status.ExecutionStatus;
import accord.local.Status.Known;
import accord.primitives.*;
import accord.primitives.Writes;
import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.local.Status.ExecutionStatus.Decided;
import static accord.local.Status.ExecutionStatus.Done;
import static accord.local.Status.Known.Definition;
import static accord.local.Status.Known.Nothing;
import static accord.utils.Utils.listOf;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.ProgressLog.ProgressShard;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRanges;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.api.ProgressLog.ProgressShard.No;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.local.Command.EnsureAction.Add;
import static accord.local.Command.EnsureAction.Check;
import static accord.local.Command.EnsureAction.Ignore;
import static accord.local.Command.EnsureAction.Set;
import static accord.local.Command.EnsureAction.TrySet;
import static accord.local.Status.Accepted;
import static accord.local.Status.AcceptedInvalidate;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.PreApplied;
import static accord.local.Status.Invalidated;
import static accord.local.Status.NotWitnessed;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.ReadyToExecute;

public abstract class Command implements CommandListener, BiConsumer<SafeCommandStore, CommandListener>, PreLoadContext
{
    private static final Logger logger = LoggerFactory.getLogger(Command.class);

    public abstract TxnId txnId();

    // TODO (now): pack this into TxnId
    public abstract Txn.Kind kind();

    public boolean hasBeen(Status status)
    {
        return status().hasBeen(status);
    }

    public boolean hasBeen(Known phase)
    {
        return known().compareTo(phase) >= 0;
    }

    public boolean hasBeen(ExecutionStatus phase)
    {
        return status().execution.compareTo(phase) >= 0;
    }

    public boolean is(Status status)
    {
        return status() == status;
    }

    /**
     * homeKey is a global value that defines the home shard - the one tasked with ensuring the transaction is finished.
     * progressKey is a local value that defines the local shard responsible for ensuring progress on the transaction.
     * This will be homeKey if it is owned by the node, and some other key otherwise. If not the home shard, the progress
     * shard has much weaker responsibilities, only ensuring that the home shard has durably witnessed the txnId.
     */
    public abstract RoutingKey homeKey();
    protected abstract void setHomeKey(RoutingKey key);

    public abstract RoutingKey progressKey();
    protected abstract void setProgressKey(RoutingKey key);

    /**
     * If this is the home shard, we require that this is a Route for all states &gt; NotWitnessed;
     * otherwise for the local progress shard this is ordinarily a PartialRoute, and for other shards this is not set,
     * so that there is only one copy per node that can be consulted to construct the full set of involved keys.
     *
     * If hasBeen(Committed) this must contain the keys for both txnId.epoch and executeAt.epoch
     *
     * TODO: maybe set this for all local shards, but slice to only those participating keys
     * (would probably need to remove hashIntersects)
     */
    public abstract AbstractRoute route();
    protected abstract void setRoute(AbstractRoute route);

    public abstract PartialTxn partialTxn();
    protected abstract void setPartialTxn(PartialTxn txn);

    public abstract Ballot promised();
    protected abstract void setPromised(Ballot ballot);

    public abstract Ballot accepted();
    protected abstract void setAccepted(Ballot ballot);

    public void saveRoute(SafeCommandStore safeStore, Route route)
    {
        setRoute(route);
        updateHomeKey(safeStore, route.homeKey);
    }

    public abstract Timestamp executeAt();
    protected abstract void setExecuteAt(Timestamp timestamp);

    /**
     * While !hasBeen(Committed), used only as a register for Accept state, used by Recovery
     * If hasBeen(Committed), represents the full deps owned by this range for execution at both txnId.epoch
     * AND executeAt.epoch so that it may be used for Recovery (which contacts only txnId.epoch topology),
     * but also for execution.
     */
    public abstract PartialDeps partialDeps();
    protected abstract void setPartialDeps(PartialDeps deps);

    public abstract Writes writes();
    protected abstract void setWrites(Writes writes);

    public abstract Result result();
    protected abstract void setResult(Result result);

    public abstract SaveStatus saveStatus();
    protected abstract void setSaveStatus(SaveStatus status);

    public Status status() { return saveStatus().status; }
    protected void setStatus(Status status) { setSaveStatus(SaveStatus.get(status, known())); }

    public abstract Known known();

    public abstract Durability durability();
    public abstract void setDurability(Durability v);

    public abstract Command addListener(CommandListener listener);
    public abstract void removeListener(CommandListener listener);
    protected abstract void notifyListeners(SafeCommandStore safeStore);

    protected abstract void addWaitingOnCommit(TxnId txnId);
    protected abstract boolean isWaitingOnCommit();
    protected abstract void removeWaitingOnCommit(TxnId txnId);
    protected abstract TxnId firstWaitingOnCommit();

    protected abstract void addWaitingOnApplyIfAbsent(TxnId txnId, Timestamp executeAt);
    protected abstract boolean isWaitingOnApply();
    protected abstract void removeWaitingOn(TxnId txnId, Timestamp executeAt);
    protected abstract TxnId firstWaitingOnApply();

    public boolean hasBeenWitnessed()
    {
        return partialTxn() != null;
    }

    public boolean isUnableToExecute()
    {
        return isWaitingOnCommit() || isWaitingOnApply();
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId());
    }

    @Override
    public Iterable<Key> keys()
    {
        // TODO (now): when do we need this, and will it always be sufficient?
        return partialTxn().keys();
    }

    public void setDurability(SafeCommandStore safeStore, Durability durability, RoutingKey homeKey, @Nullable Timestamp executeAt)
    {
        updateHomeKey(safeStore, homeKey);
        if (executeAt != null && hasBeen(Committed) && !this.executeAt().equals(executeAt))
            safeStore.agent().onInconsistentTimestamp(this, this.executeAt(), executeAt);
        setDurability(durability);
    }

    public enum AcceptOutcome
    {
        Success, Redundant, RejectedBallot
    }

    public AcceptOutcome preaccept(SafeCommandStore safeStore, PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey)
    {
        if (promised().compareTo(Ballot.ZERO) > 0)
            return AcceptOutcome.RejectedBallot;

        return preacceptInternal(safeStore, partialTxn, route, progressKey);
    }

    private AcceptOutcome preacceptInternal(SafeCommandStore safeStore, PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey)
    {
        if (known() != Nothing)
        {
            Preconditions.checkState(status() == Invalidated || executeAt() != null);
            logger.trace("{}: skipping preaccept - already preaccepted ({})", txnId(), status());
            return AcceptOutcome.Redundant;
        }

        KeyRanges coordinateRanges = coordinateRanges(safeStore);
        ProgressShard shard = progressShard(safeStore, route, progressKey, coordinateRanges);
        if (!validate(KeyRanges.EMPTY, coordinateRanges, shard, route, Set, partialTxn, Set, null, Ignore))
            throw new IllegalStateException();

        if (executeAt() == null)
        {
            Timestamp max = safeStore.maxConflict(partialTxn.keys());
            TxnId txnId = txnId();
            // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
            //  - use a global logical clock to issue new timestamps; or
            //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
            setExecuteAt(txnId.compareTo(max) > 0 && txnId.epoch >= safeStore.latestEpoch()
                    ? txnId : safeStore.uniqueNow(max));

            if (status() == NotWitnessed)
                setStatus(PreAccepted);
            safeStore.progressLog().preaccepted(this, shard);
        }
        else
        {
            setSaveStatus(SaveStatus.get(status(), Definition));
        }
        set(safeStore, KeyRanges.EMPTY, coordinateRanges, shard, route, partialTxn, Set, null, Ignore);

        notifyListeners(safeStore);
        return AcceptOutcome.Success;
    }

    public boolean preacceptInvalidate(Ballot ballot)
    {
        if (promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping preacceptInvalidate - witnessed higher ballot ({})", txnId(), promised());
            return false;
        }
        setPromised(ballot);
        return true;
    }

    public AcceptOutcome accept(SafeCommandStore safeStore, Ballot ballot, PartialRoute route, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (this.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept - witnessed higher ballot ({} > {})", txnId(), promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping accept - already committed ({})", txnId(), status());
            return AcceptOutcome.Redundant;
        }

        TxnId txnId = txnId();
        KeyRanges coordinateRanges = coordinateRanges(safeStore);
        KeyRanges executeRanges = txnId.epoch == executeAt.epoch ? coordinateRanges : safeStore.ranges().at(executeAt.epoch);
        ProgressShard shard = progressShard(safeStore, route, progressKey, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, Ignore, null, Ignore, partialDeps, Set))
            throw new AssertionError("Invalid response from validate function");

        setExecuteAt(executeAt);
        setPromised(ballot);
        setAccepted(ballot);
        set(safeStore, coordinateRanges, executeRanges, shard, route, null, Ignore, partialDeps, Set);
        setStatus(Accepted);

        safeStore.progressLog().accepted(this, shard);
        notifyListeners(safeStore);

        return AcceptOutcome.Success;
    }

    public AcceptOutcome acceptInvalidate(SafeCommandStore safeStore, Ballot ballot)
    {
        if (this.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept invalidated - witnessed higher ballot ({} > {})", txnId(), promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping accept invalidated - already committed ({})", txnId(), status());
            return AcceptOutcome.Redundant;
        }

        setPromised(ballot);
        setAccepted(ballot);
        setStatus(AcceptedInvalidate);
        logger.trace("{}: accepted invalidated", txnId());

        notifyListeners(safeStore);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Redundant, Insufficient }

    // relies on mutual exclusion for each key
    public CommitOutcome commit(SafeCommandStore safeStore, AbstractRoute route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping commit - already committed ({})", txnId(), status());
            if (executeAt.equals(executeAt()) && status() != Invalidated)
                return CommitOutcome.Redundant;

            safeStore.agent().onInconsistentTimestamp(this, (status() == Invalidated ? Timestamp.NONE : this.executeAt()), executeAt);
        }

        KeyRanges coordinateRanges = coordinateRanges(safeStore);
        // TODO (now): consider ranges between coordinateRanges and executeRanges? Perhaps don't need them
        KeyRanges executeRanges = executeRanges(safeStore, executeAt);
        ProgressShard shard = progressShard(safeStore, route, progressKey, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, Check, partialTxn, Add, partialDeps, Set))
            return CommitOutcome.Insufficient;

        setExecuteAt(executeAt);
        set(safeStore, coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);

        setStatus(Committed);
        logger.trace("{}: committed with executeAt: {}, deps: {}", txnId(), executeAt, partialDeps);
        populateWaitingOn(safeStore);

        safeStore.progressLog().committed(this, shard);

        // TODO (now): introduce intermediate status to avoid reentry when notifying listeners (which might notify us)
        maybeExecute(safeStore, shard, true, true);
        return CommitOutcome.Success;
    }

    protected void populateWaitingOn(SafeCommandStore safeStore)
    {
        KeyRanges ranges = safeStore.ranges().since(executeAt().epoch);
        if (ranges != null) {
            partialDeps().forEachOn(ranges, safeStore.commandStore()::hashIntersects, txnId -> {
                Command command = safeStore.ifLoaded(txnId);
                if (command == null)
                {
                    addWaitingOnCommit(txnId);
                    safeStore.addAndInvokeListener(txnId, this);
                }
                else
                {
                    switch (command.status()) {
                        default:
                            throw new IllegalStateException();
                        case NotWitnessed:
                        case PreAccepted:
                        case Accepted:
                        case AcceptedInvalidate:
                            // we don't know when these dependencies will execute, and cannot execute until we do
                            command.addListener(this);
                            addWaitingOnCommit(command.txnId());
                            break;
                        case Committed:
                            // TODO: split into ReadyToRead and ReadyToWrite;
                            //       the distributed read can be performed as soon as those keys are ready, and in parallel with any other reads
                            //       the client can even ACK immediately after; only the write needs to be postponed until other in-progress reads complete
                        case ReadyToExecute:
                        case PreApplied:
                        case Applied:
                            command.addListener(this);
                            insertPredecessor(command);
                        case Invalidated:
                            break;
                    }
                }
            });
        }
    }

    // TODO (now): commitInvalidate may need to update cfks _if_ possible
    public void commitInvalidate(SafeCommandStore safeStore)
    {
        if (hasBeen(Committed))
        {
            logger.trace("{}: skipping commit invalidated - already committed ({})", txnId(), status());
            if (!hasBeen(Invalidated))
                safeStore.agent().onInconsistentTimestamp(this, Timestamp.NONE, executeAt());

            return;
        }

        ProgressShard shard = progressShard(safeStore);
        safeStore.progressLog().invalidated(this, shard);
        setExecuteAt(txnId());
        if (partialDeps() == null)
            setPartialDeps(PartialDeps.NONE);
        setStatus(Invalidated);
        logger.trace("{}: committed invalidated", txnId());

        notifyListeners(safeStore);
    }

    public enum ApplyOutcome { Success, Redundant, Insufficient }

    public ApplyOutcome apply(SafeCommandStore safeStore, long untilEpoch, AbstractRoute route, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        if (hasBeen(PreApplied) && executeAt.equals(this.executeAt()))
        {
            logger.trace("{}: skipping apply - already executed ({})", txnId(), status());
            return ApplyOutcome.Redundant;
        }
        else if (hasBeen(Committed) && !executeAt.equals(this.executeAt()))
        {
            safeStore.agent().onInconsistentTimestamp(this, this.executeAt(), executeAt);
        }

        KeyRanges coordinateRanges = coordinateRanges(safeStore);
        KeyRanges executeRanges = executeRanges(safeStore, executeAt);
        if (untilEpoch < safeStore.latestEpoch())
        {
            KeyRanges expectedRanges = safeStore.ranges().between(executeAt.epoch, untilEpoch);
            Preconditions.checkState(expectedRanges.contains(executeRanges));
        }
        ProgressShard shard = progressShard(safeStore, route, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, Check, null, Check, partialDeps, hasBeen(Committed) ? Add : TrySet))
            return ApplyOutcome.Insufficient; // TODO: this should probably be an assertion failure if !TrySet

        setWrites(writes);
        setResult(result);
        setExecuteAt(executeAt);
        set(safeStore, coordinateRanges, executeRanges, shard, route, null, Check, partialDeps, hasBeen(Committed) ? Add : TrySet);

        if (!hasBeen(Committed))
            populateWaitingOn(safeStore);
        setStatus(PreApplied);
        logger.trace("{}: apply, status set to Executed with executeAt: {}, deps: {}", txnId(), executeAt, partialDeps);

        safeStore.progressLog().executed(this, shard);

        maybeExecute(safeStore, shard, true, true);
        return ApplyOutcome.Success;
    }

    public AcceptOutcome recover(SafeCommandStore safeStore, PartialTxn partialTxn, AbstractRoute route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        if (promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping preaccept invalidate - higher ballot witnessed ({})", txnId(), promised());
            return AcceptOutcome.RejectedBallot;
        }

        if (executeAt() == null)
        {
            Preconditions.checkState(status() == NotWitnessed || status() == AcceptedInvalidate);
            switch (preacceptInternal(safeStore, partialTxn, route, progressKey))
            {
                default:
                case RejectedBallot:
                case Redundant:
                    throw new IllegalStateException();

                case Success:
            }
        }

        setPromised(ballot);
        return AcceptOutcome.Success;
    }

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(listOf(txnId(), caller), Collections.emptyList());
    }

    @Override
    public void onChange(SafeCommandStore safeStore, Command command)
    {
        logger.trace("{}: updating as listener in response to change on {} with status {} ({})",
                     txnId(), command.txnId(), command.status(), command);
        switch (command.status())
        {
            default:
                throw new IllegalStateException();
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case AcceptedInvalidate:
                break;

            case Committed:
            case ReadyToExecute:
            case PreApplied:
            case Applied:
            case Invalidated:
                updatePredecessor(command);
                maybeExecute(safeStore, progressShard(safeStore), false, true);
                break;
        }
    }

    protected void postApply(SafeCommandStore safeStore)
    {
        logger.trace("{} applied, setting status to Applied and notifying listeners", txnId());
        setStatus(Applied);
        notifyListeners(safeStore);
    }

    private static Function<SafeCommandStore, Void> callPostApply(TxnId txnId)
    {
        return safeStore -> {
            safeStore.command(txnId).postApply(safeStore);
            return null;
        };
    }

    protected Future<Void> apply(SafeCommandStore safeStore)
    {
        // important: we can't include a reference to *this* in the lambda, since the C* implementation may evict
        // the command instance from memory between now and the write completing (and post apply being called)
        CommandStore unsafeStore = safeStore.commandStore();
        return writes().apply(safeStore).flatMap(unused ->
            unsafeStore.submit(this, callPostApply(txnId()))
        );
    }

    public Future<Data> read(SafeCommandStore safeStore)
    {
        return partialTxn().read(safeStore, this);
    }

    // TODO: maybe split into maybeExecute and maybeApply?
    private boolean maybeExecute(SafeCommandStore safeStore, ProgressShard shard, boolean alwaysNotifyListeners, boolean notifyWaitingOn)
    {
        if (logger.isTraceEnabled())
            logger.trace("{}: Maybe executing with status {}. Will notify listeners on noop: {}", txnId(), status(), alwaysNotifyListeners);

        if (status() != Committed && status() != PreApplied)
        {
            if (alwaysNotifyListeners)
                notifyListeners(safeStore);
            return false;
        }

        if (isUnableToExecute())
        {
            if (alwaysNotifyListeners)
                notifyListeners(safeStore);

            if (notifyWaitingOn)
                new NotifyWaitingOn(this).accept(safeStore);
            return false;
        }

        switch (status())
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                setStatus(ReadyToExecute);
                logger.trace("{}: set to ReadyToExecute", txnId());
                safeStore.progressLog().readyToExecute(this, shard);
                notifyListeners(safeStore);
                break;

            case PreApplied:
                if (executeRanges(safeStore, executeAt()).intersects(writes().keys, safeStore.commandStore()::hashIntersects))
                {
                    logger.trace("{}: applying", txnId());
                    apply(safeStore);
                }
                else
                {
                    logger.trace("{}: applying no-op", txnId());
                    setStatus(Applied);
                    notifyListeners(safeStore);
                }
        }
        return true;
    }

    /**
     * @param dependency is either committed or invalidated
     * @return true iff {@code maybeExecute} might now have a different outcome
     */
    private boolean updatePredecessor(Command dependency)
    {
        Preconditions.checkState(dependency.hasBeen(Committed));
        if (dependency.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", txnId(), dependency.txnId());
            dependency.removeListener(this);
            removeWaitingOnCommit(dependency.txnId()); // TODO (now): this was missing in partial-replication; might be redundant?
            return true;
        }
        else if (dependency.executeAt().compareTo(executeAt()) > 0)
        {
            // dependency cannot be a predecessor if it executes later
            logger.trace("{}: {} executes after us. Stop listening and removing from waiting on apply set.", txnId(), dependency.txnId());
            removeWaitingOn(dependency.txnId(), dependency.executeAt());
            dependency.removeListener(this);
            return true;
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Stop listening and removing from waiting on apply set.", txnId(), dependency.txnId());
            removeWaitingOn(dependency.txnId(), dependency.executeAt());
            dependency.removeListener(this);
            return true;
        }
        else if (isUnableToExecute())
        {
            logger.trace("{}: adding {} to waiting on apply set.", txnId(), dependency.txnId());
            addWaitingOnApplyIfAbsent(dependency.txnId(), dependency.executeAt());
            removeWaitingOnCommit(dependency.txnId());
            return false;
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    private void insertPredecessor(Command dependency)
    {
        Preconditions.checkState(dependency.hasBeen(Committed));
        if (dependency.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Do not insert.", txnId(), dependency.txnId());
        }
        else if (dependency.executeAt().compareTo(executeAt()) > 0)
        {
            // dependency cannot be a predecessor if it executes later
            logger.trace("{}: {} executes after us. Do not insert.", txnId(), dependency.txnId());
        }
        else if (dependency.hasBeen(Applied))
        {
            logger.trace("{}: {} has been applied. Do not insert.", txnId(), dependency.txnId());
        }
        else
        {
            logger.trace("{}: adding {} to waiting on apply set.", txnId(), dependency.txnId());
            addWaitingOnApplyIfAbsent(dependency.txnId(), dependency.executeAt());
        }
    }

    void updatePredecessorAndMaybeExecute(SafeCommandStore safeStore, Command predecessor, boolean notifyWaitingOn)
    {
        if (hasBeen(Applied))
            return;

        if (updatePredecessor(predecessor))
            maybeExecute(safeStore, progressShard(safeStore), false, notifyWaitingOn);
    }

    static class NotifyWaitingOn implements PreLoadContext, Consumer<SafeCommandStore>
    {
        ExecutionStatus[] blockedUntil = new ExecutionStatus[4];
        TxnId[] txnIds = new TxnId[4];
        int depth;

        public NotifyWaitingOn(Command command)
        {
            txnIds[0] = command.txnId();
            blockedUntil[0] = Done;
        }

        @Override
        public void accept(SafeCommandStore safeStore)
        {
            Command prev = get(safeStore, depth - 1);
            while (depth >= 0)
            {
                Command cur = safeStore.ifLoaded(txnIds[depth]);
                ExecutionStatus until = blockedUntil[depth];
                if (cur == null)
                {
                    // need to load; schedule execution for later
                    safeStore.execute(this, this);
                    return;
                }

                if (prev != null)
                {
                    if (cur.hasBeen(until) || (cur.hasBeen(Committed) && cur.executeAt().compareTo(prev.executeAt()) > 0))
                    {
                        prev.updatePredecessorAndMaybeExecute(safeStore, cur, false);
                        --depth;
                        prev = get(safeStore, depth - 1);
                        continue;
                    }
                }
                else if (cur.hasBeen(until))
                {
                    // we're done; have already applied
                    Preconditions.checkState(depth == 0);
                    break;
                }

                TxnId directlyBlockedBy = cur.firstWaitingOnCommit();
                if (directlyBlockedBy != null)
                {
                    push(directlyBlockedBy, Decided);
                }
                else if (null != (directlyBlockedBy = cur.firstWaitingOnApply()))
                {
                    push(directlyBlockedBy, Done);
                }
                else
                {
                    if (cur.hasBeen(Committed) && !cur.hasBeen(ReadyToExecute) && !cur.isUnableToExecute())
                    {
                        if (!cur.maybeExecute(safeStore, cur.progressShard(safeStore), false, false))
                            throw new AssertionError("Is able to Apply, but has not done so");
                        // loop and re-test the command's status; we may still want to notify blocking, esp. if not homeShard
                        continue;
                    }

                    RoutingKeys someKeys = cur.maxRoutingKeys();
                    if (someKeys == null && prev != null) someKeys = prev.partialDeps().someRoutingKeys(cur.txnId());
                    Preconditions.checkState(someKeys != null);
                    logger.trace("{} blocked on {} until {}", txnIds[0], cur.txnId(), until);
                    safeStore.progressLog().waiting(cur.txnId(), until.requires, someKeys);
                    return;
                }
                prev = cur;
            }
        }

        private Command get(SafeCommandStore safeStore, int i)
        {
            return i >= 0 ? safeStore.command(txnIds[i]) : null;
        }

        void push(TxnId by, ExecutionStatus until)
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
        public Iterable<TxnId> txnIds()
        {
            return Arrays.asList(txnIds).subList(0, depth + 1);
        }

        @Override
        public Iterable<Key> keys()
        {
            return Collections.emptyList();
        }
    }

    /**
     * A key nominated to represent the "home" shard - only members of the home shard may be nominated to recover
     * a transaction, to reduce the cluster-wide overhead of ensuring progress. A transaction that has only been
     * witnessed at PreAccept may however trigger a process of ensuring the home shard is durably informed of
     * the transaction.
     *
     * Note that for ProgressLog purposes the "home shard" is the shard as of txnId.epoch.
     * For recovery purposes the "home shard" is as of txnId.epoch until Committed, and executeAt.epoch once Executed
     *
     * TODO: Markdown documentation explaining the home shard and local shard concepts
     */

    public final void homeKey(RoutingKey homeKey)
    {
        RoutingKey current = homeKey();
        if (current == null) setHomeKey(homeKey);
        else if (!current.equals(homeKey)) throw new AssertionError();
    }

    public void updateHomeKey(SafeCommandStore safeStore, RoutingKey homeKey)
    {
        if (homeKey() == null)
        {
            setHomeKey(homeKey);
            if (progressKey() == null && owns(safeStore, txnId().epoch, homeKey))
                progressKey(homeKey);
        }
        else if (!this.homeKey().equals(homeKey))
        {
            throw new IllegalStateException();
        }
    }

    private ProgressShard progressShard(SafeCommandStore safeStore, AbstractRoute route, @Nullable RoutingKey progressKey, KeyRanges coordinateRanges)
    {
        updateHomeKey(safeStore, route.homeKey);

        if (progressKey == null || progressKey == NO_PROGRESS_KEY)
        {
            if (this.progressKey() == null)
                setProgressKey(NO_PROGRESS_KEY);

            return No;
        }

        if (this.progressKey() == null) setProgressKey(progressKey);
        else if (!this.progressKey().equals(progressKey)) throw new AssertionError();

        if (!coordinateRanges.contains(progressKey))
            return No;

        if (!safeStore.commandStore().hashIntersects(progressKey))
            return No;

        return progressKey.equals(homeKey()) ? Home : Local;
    }

    /**
     * A key nominated to be the primary shard within this node for managing progress of the command.
     * It is nominated only as of txnId.epoch, and may be null (indicating that this node does not monitor
     * the progress of this command).
     *
     * Preferentially, this is homeKey on nodes that replicate it, and otherwise any key that is replicated, as of txnId.epoch
     */

    public final void progressKey(RoutingKey progressKey)
    {
        RoutingKey current = progressKey();
        if (current == null) setProgressKey(progressKey);
        else if (!current.equals(progressKey)) throw new AssertionError();
    }

    private ProgressShard progressShard(SafeCommandStore safeStore, AbstractRoute route, KeyRanges coordinateRanges)
    {
        if (progressKey() == null)
            return Unsure;

        return progressShard(safeStore, route, progressKey(), coordinateRanges);
    }

    private ProgressShard progressShard(SafeCommandStore safeStore)
    {
        RoutingKey progressKey = progressKey();
        if (progressKey == null)
            return Unsure;

        if (progressKey == NO_PROGRESS_KEY)
            return No;

        KeyRanges coordinateRanges = safeStore.ranges().at(txnId().epoch);
        if (!coordinateRanges.contains(progressKey))
            return No;

        if (!safeStore.commandStore().hashIntersects(progressKey))
            return No;

        return progressKey.equals(homeKey()) ? Home : Local;
    }

    private KeyRanges coordinateRanges(SafeCommandStore safeStore)
    {
        return safeStore.ranges().at(txnId().epoch);
    }

    private KeyRanges executeRanges(SafeCommandStore safeStore, Timestamp executeAt)
    {
        return safeStore.ranges().since(executeAt.epoch);
    }

    enum EnsureAction { Ignore, Check, Add, TrySet, Set }

    /**
     * Validate we have sufficient information for the route, partialTxn and partialDeps fields, and if so update them;
     * otherwise return false (or throw an exception if an illegal state is encountered)
     */
    private boolean validate(KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard,
                             AbstractRoute route, EnsureAction ensureRoute,
                             @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                             @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        if (shard == Unsure)
            return false;

        // first validate route
        if (shard.isProgress())
        {
            // validate route
            if (shard.isHome())
            {
                switch (ensureRoute)
                {
                    default: throw new AssertionError();
                    case Check:
                        if (!(route() instanceof Route) && !(route instanceof Route))
                            return false;
                    case Ignore:
                        break;
                    case Add:
                    case Set:
                        if (!(route instanceof Route))
                            throw new IllegalArgumentException("Incomplete route (" + route + ") sent to home shard");
                        break;
                    case TrySet:
                        if (!(route instanceof Route))
                            return false;
                }
            }
            else if (route() == null)
            {
                // failing any of these tests is always an illegal state
                if (!route.covers(existingRanges))
                    return false;

                if (existingRanges != additionalRanges && !route.covers(additionalRanges))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
            }
            else if (existingRanges != additionalRanges && !route().covers(additionalRanges))
            {
                if (!route.covers(additionalRanges))
                    throw new IllegalArgumentException("Incomplete route (" + route + ") provided; does not cover " + additionalRanges);
            }
            else
            {
                if (!route().covers(existingRanges))
                    throw new IllegalStateException();
            }
        }

        // invalid to Add deps to Accepted or AcceptedInvalidate statuses, as Committed deps are not equivalent
        // and we may erroneously believe we have covered a wider range than we have infact covered
        if (ensurePartialDeps == Add)
            Preconditions.checkState(status() != Accepted && status() != AcceptedInvalidate);

        // validate new partial txn
        if (!validate(ensurePartialTxn, existingRanges, additionalRanges, covers(partialTxn()), covers(partialTxn), "txn", partialTxn))
            return false;

        if (shard.isHome() && ensurePartialTxn != Ignore)
        {
            if (!hasQuery(partialTxn()) && !hasQuery(partialTxn))
                throw new IllegalStateException();
        }

        return validate(ensurePartialDeps, existingRanges, additionalRanges, covers(partialDeps()), covers(partialDeps), "deps", partialDeps);
    }

    private void set(SafeCommandStore safeStore,
                     KeyRanges existingRanges, KeyRanges additionalRanges, ProgressShard shard, AbstractRoute route,
                     @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                     @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        Preconditions.checkState(progressKey() != null);
        KeyRanges allRanges = existingRanges.union(additionalRanges);

        if (shard.isProgress()) setRoute(AbstractRoute.merge(route(), route));
        else setRoute(AbstractRoute.merge(route(), route.slice(allRanges)));

        // TODO (soon): stop round-robin hashing; partition only on ranges
        switch (ensurePartialTxn)
        {
            case Add:
                if (partialTxn == null)
                    break;

                if (partialTxn() != null)
                {
                    partialTxn = partialTxn.slice(allRanges, shard.isHome());
                    partialTxn.keys().foldlDifference(partialTxn().keys(), (i, key, p, v) -> {
                        if (safeStore.commandStore().hashIntersects(key))
                            safeStore.commandsForKey(key).register(this);
                        return v;
                    }, 0, 0, 1);
                    this.setPartialTxn(partialTxn().with(partialTxn));
                    break;
                }

            case Set:
            case TrySet:
                setPartialTxn(partialTxn = partialTxn.slice(allRanges, shard.isHome()));
                partialTxn.keys().forEach(key -> {
                    if (safeStore.commandStore().hashIntersects(key))
                        safeStore.commandsForKey(key).register(this);
                });
                break;
        }

        switch (ensurePartialDeps)
        {
            case Add:
                if (partialDeps == null)
                    break;

                if (partialDeps() != null)
                {
                    setPartialDeps(partialDeps().with(partialDeps.slice(allRanges)));
                    break;
                }

            case Set:
            case TrySet:
                setPartialDeps(partialDeps.slice(allRanges));
                break;
        }
    }

    private static boolean validate(EnsureAction action, KeyRanges existingRanges, KeyRanges additionalRanges,
                                    KeyRanges existing, KeyRanges adding, String kind, Object obj)
    {
        switch (action)
        {
            default: throw new IllegalStateException();
            case Ignore:
                break;

            case TrySet:
                if (adding != null)
                {
                    if (!adding.contains(existingRanges))
                        return false;

                    if (additionalRanges != existingRanges && !adding.contains(additionalRanges))
                        return false;

                    break;
                }
            case Set:
                // failing any of these tests is always an illegal state
                Preconditions.checkState(adding != null);
                if (!adding.contains(existingRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + existingRanges);

                if (additionalRanges != existingRanges && !adding.contains(additionalRanges))
                    throw new IllegalArgumentException("Incomplete " + kind + " (" + obj + ") provided; does not cover " + additionalRanges);
                break;

            case Check:
            case Add:
                if (adding == null)
                {
                    if (existing == null)
                        return false;

                    Preconditions.checkState(existing.contains(existingRanges));
                    if (existingRanges != additionalRanges && !existing.contains(additionalRanges))
                    {
                        if (action == Check)
                            return false;

                        throw new IllegalArgumentException("Missing additional " + kind + "; existing does not cover " + additionalRanges.difference(existingRanges));
                    }
                }
                else if (existing != null)
                {
                    KeyRanges covering = adding.union(existing);
                    Preconditions.checkState(covering.contains(existingRanges));
                    if (existingRanges != additionalRanges && !covering.contains(additionalRanges))
                    {
                        if (action == Check)
                            return false;

                        throw new IllegalArgumentException("Incomplete additional " + kind + " (" + obj + ") provided; does not cover " + additionalRanges.difference(existingRanges));
                    }
                }
                else
                {
                    if (!adding.contains(existingRanges))
                        return false;

                    if (existingRanges != additionalRanges && !adding.contains(additionalRanges))
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

    // TODO: callers should try to consult the local progress shard (if any) to obtain the full set of keys owned locally
    public AbstractRoute someRoute()
    {
        if (route() != null)
            return route();

        if (homeKey() != null)
            return new PartialRoute(KeyRanges.EMPTY, homeKey(), new RoutingKey[0]);

        return null;
    }

    public RoutingKeys maxRoutingKeys()
    {
        AbstractRoute route = someRoute();
        if (route == null)
            return null;

        return route.with(route.homeKey);
    }

    /**
     * true iff this commandStore owns the given key on the given epoch
     */
    public boolean owns(SafeCommandStore safeStore, long epoch, RoutingKey someKey)
    {
        if (!safeStore.commandStore().hashIntersects(someKey))
            return false;

        return safeStore.ranges().at(epoch).contains(someKey);
    }

    private TxnId directlyBlockedBy()
    {
        // firstly we're waiting on every dep to commit
        TxnId directlyBlockedBy = firstWaitingOnCommit();
        if (directlyBlockedBy != null)
            return directlyBlockedBy;
        return firstWaitingOnApply();
    }

    @Override
    public void accept(SafeCommandStore safeStore, CommandListener listener)
    {
        listener.onChange(safeStore, this);
    }

    @Override
    public String toString()
    {
        return "Command{" +
               "txnId=" + txnId() +
               ", status=" + status() +
               ", partialTxn=" + partialTxn() +
               ", executeAt=" + executeAt() +
               ", partialDeps=" + partialDeps() +
               '}';
    }

    private static KeyRanges covers(@Nullable PartialTxn txn)
    {
        return txn == null ? null : txn.covering();
    }

    private static KeyRanges covers(@Nullable PartialDeps deps)
    {
        return deps == null ? null : deps.covering;
    }

    private static boolean hasQuery(PartialTxn txn)
    {
        return txn != null && txn.query() != null;
    }

    // TODO: this is an ugly hack, need to encode progress/homeKey/Route state combinations much more clearly
    //  (perhaps introduce encapsulating class representing each possible arrangement)
    private static final RoutingKey NO_PROGRESS_KEY = new RoutingKey()
    {
        @Override
        public int routingHash()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int compareTo(@Nonnull RoutingKey ignore)
        {
            throw new UnsupportedOperationException();
        }
    };
}
