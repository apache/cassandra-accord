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
import accord.local.Status.Known;
import accord.primitives.*;
import accord.primitives.Writes;
import accord.utils.Invariants;
import accord.utils.async.AsyncCallbacks;
import accord.utils.async.AsyncChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.local.Status.*;
import static accord.local.Status.Known.*;
import static accord.local.Status.Known.Done;
import static accord.local.Status.Known.ExecuteAtOnly;
import static accord.primitives.Route.isFullRoute;
import static accord.utils.Utils.listOf;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.ProgressLog.ProgressShard;
import accord.primitives.Ranges;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.api.Result;
import accord.api.RoutingKey;

import static accord.api.ProgressLog.ProgressShard.Home;
import static accord.api.ProgressLog.ProgressShard.Local;
import static accord.api.ProgressLog.ProgressShard.No;
import static accord.api.ProgressLog.ProgressShard.Unsure;
import static accord.local.Command.EnsureAction.Add;
import static accord.local.Command.EnsureAction.Check;
import static accord.local.Command.EnsureAction.Ignore;
import static accord.local.Command.EnsureAction.Set;
import static accord.local.Command.EnsureAction.TrySet;

public abstract class Command implements CommandListener, BiConsumer<SafeCommandStore, CommandListener>, PreLoadContext
{
    private static final Logger logger = LoggerFactory.getLogger(Command.class);

    public abstract TxnId txnId();

    // TODO (desirable, API consistency): should any of these calls be replaced by corresponding known() registers?
    public boolean hasBeen(Status status)
    {
        return status().hasBeen(status);
    }

    public boolean has(Known known)
    {
        return known.isSatisfiedBy(saveStatus().known);
    }

    public boolean has(Definition definition)
    {
        return known().definition.compareTo(definition) >= 0;
    }

    public boolean has(Outcome outcome)
    {
        return known().outcome.compareTo(outcome) >= 0;
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
     *
     * TODO (expected, efficiency): we probably do not want to save this on its own, as we probably want to
     *  minimize IO interactions and discrete registers, so will likely reference commit log entries directly
     *  At which point we may impose a requirement that only a Route can be saved, not a homeKey on its own.
     *  Once this restriction is imposed, we no longer need to pass around Routable.Domain with TxnId.
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
     */
    public abstract @Nullable Route<?> route();
    protected abstract void setRoute(Route<?> route);

    public abstract PartialTxn partialTxn();
    protected abstract void setPartialTxn(PartialTxn txn);

    public abstract Ballot promised();
    protected abstract void setPromised(Ballot ballot);

    public abstract Ballot accepted();
    protected abstract void setAccepted(Ballot ballot);

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

    public Known known() { return saveStatus().known; }

    public abstract Durability durability();
    public abstract void setDurability(Durability v);

    public abstract Command addListener(CommandListener listener);
    public abstract void removeListener(CommandListener listener);
    protected abstract void notifyListeners(SafeCommandStore safeStore);

    protected abstract void addWaitingOnCommit(TxnId txnId);
    protected abstract void removeWaitingOnCommit(TxnId txnId);
    protected abstract TxnId firstWaitingOnCommit();

    protected abstract void addWaitingOnApplyIfAbsent(TxnId txnId, Timestamp executeAt);
    protected abstract TxnId firstWaitingOnApply(@Nullable TxnId ifExecutesBefore);

    protected abstract void removeWaitingOn(TxnId txnId, Timestamp executeAt);
    protected abstract boolean isWaitingOnDependency();

    public boolean hasBeenWitnessed()
    {
        return partialTxn() != null;
    }

    @Override
    public Iterable<TxnId> txnIds()
    {
        return Collections.singleton(txnId());
    }

    @Override
    public Seekables<?, ?> keys()
    {
        // TODO (expected, consider): when do we need this, and will it always be sufficient?
        return partialTxn().keys();
    }

    public void setDurability(SafeCommandStore safeStore, Durability durability, RoutingKey homeKey, @Nullable Timestamp executeAt)
    {
        updateHomeKey(safeStore, homeKey);
        if (executeAt != null && hasBeen(PreCommitted) && !this.executeAt().equals(executeAt))
            safeStore.agent().onInconsistentTimestamp(this, this.executeAt(), executeAt);
        setDurability(durability);
    }

    public enum AcceptOutcome
    {
        Success, Redundant, RejectedBallot
    }

    public AcceptOutcome preaccept(SafeCommandStore safeStore, PartialTxn partialTxn, Route<?> route, @Nullable RoutingKey progressKey)
    {
        return preacceptOrRecover(safeStore, partialTxn, route, progressKey, Ballot.ZERO);
    }

    public AcceptOutcome recover(SafeCommandStore safeStore, PartialTxn partialTxn, Route<?> route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        return preacceptOrRecover(safeStore, partialTxn, route, progressKey, ballot);
    }

    private AcceptOutcome preacceptOrRecover(SafeCommandStore safeStore, PartialTxn partialTxn, Route<?> route, @Nullable RoutingKey progressKey, Ballot ballot)
    {
        int compareBallots = promised().compareTo(ballot);
        if (compareBallots > 0)
        {
            logger.trace("{}: skipping preaccept - higher ballot witnessed ({})", txnId(), promised());
            return AcceptOutcome.RejectedBallot;
        }
        else if (compareBallots < 0)
        {
            // save the new ballot as a promise
            setPromised(ballot);
        }

        if (known().definition.isKnown())
        {
            Invariants.checkState(status() == Invalidated || executeAt() != null);
            logger.trace("{}: skipping preaccept - already known ({})", txnId(), status());
            // in case of Ballot.ZERO, we must either have a competing recovery coordinator or have late delivery of the
            // preaccept; in the former case we should abandon coordination, and in the latter we have already completed
            return ballot.equals(Ballot.ZERO) ? AcceptOutcome.Redundant : AcceptOutcome.Success;
        }

        Ranges coordinateRanges = coordinateRanges(safeStore);
        Invariants.checkState(!coordinateRanges.isEmpty());
        ProgressShard shard = progressShard(safeStore, route, progressKey, coordinateRanges);
        if (!validate(Ranges.EMPTY, coordinateRanges, shard, route, Set, partialTxn, Set, null, Ignore))
            throw new IllegalStateException();

        if (executeAt() == null)
        {
            TxnId txnId = txnId();
            // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
            //  - use a global logical clock to issue new timestamps; or
            //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
            // if we are performing recovery (i.e. non-zero ballot), do not permit a fast path decision as we want to
            // invalidate any transactions that were not completed by their initial coordinator
            if (ballot.equals(Ballot.ZERO)) setExecuteAt(safeStore.preaccept(txnId, partialTxn.keys()));
            else setExecuteAt(safeStore.time().uniqueNow(txnId));

            if (status() == NotWitnessed)
                setStatus(PreAccepted);
            safeStore.progressLog().preaccepted(this, shard);
        }
        else
        {
            // TODO (expected, ?): in the case that we are pre-committed but had not been preaccepted/accepted, should we inform progressLog?
            setSaveStatus(SaveStatus.enrich(saveStatus(), DefinitionOnly));
        }
        set(safeStore, Ranges.EMPTY, coordinateRanges, shard, route, partialTxn, Set, null, Ignore);

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

    public AcceptOutcome accept(SafeCommandStore safeStore, Ballot ballot, PartialRoute<?> route, Seekables<?, ?> keys, @Nullable RoutingKey progressKey, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (this.promised().compareTo(ballot) > 0)
        {
            logger.trace("{}: skipping accept - witnessed higher ballot ({} > {})", txnId(), promised(), ballot);
            return AcceptOutcome.RejectedBallot;
        }

        if (hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping accept - already committed ({})", txnId(), status());
            return AcceptOutcome.Redundant;
        }

        TxnId txnId = txnId();
        Ranges coordinateRanges = coordinateRanges(safeStore);
        Ranges acceptRanges = txnId.epoch() == executeAt.epoch() ? coordinateRanges : safeStore.ranges().between(txnId.epoch(), executeAt.epoch());
        Invariants.checkState(!acceptRanges.isEmpty());
        ProgressShard shard = progressShard(safeStore, route, progressKey, coordinateRanges);

        if (!validate(coordinateRanges, Ranges.EMPTY, shard, route, Ignore, null, Ignore, partialDeps, Set))
            throw new AssertionError("Invalid response from validate function");

        setExecuteAt(executeAt);
        setPromised(ballot);
        setAccepted(ballot);

        // TODO (desired, clarity/efficiency): we don't need to set the route here, and perhaps we don't even need to
        //  distributed partialDeps at all, since all we gain is not waiting for these transactions to commit during
        //  recovery. We probably don't want to directly persist a Route in any other circumstances, either, to ease persistence.
        set(safeStore, coordinateRanges, acceptRanges, shard, route, null, Ignore, partialDeps, Set);

        // set only registers by transaction keys, which we mightn't already have received
        if (!known().isDefinitionKnown())
            safeStore.register(keys, acceptRanges, this);

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

        if (hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping accept invalidated - already committed ({})", txnId(), status());
            return AcceptOutcome.Redundant;
        }

        setPromised(ballot);
        setAccepted(ballot);
        setStatus(AcceptedInvalidate);
        setPartialDeps(null);
        logger.trace("{}: accepted invalidated", txnId());

        notifyListeners(safeStore);
        return AcceptOutcome.Success;
    }

    public enum CommitOutcome { Success, Redundant, Insufficient }

    // relies on mutual exclusion for each key
    public CommitOutcome commit(SafeCommandStore safeStore, Route<?> route, @Nullable RoutingKey progressKey, @Nullable PartialTxn partialTxn, Timestamp executeAt, PartialDeps partialDeps)
    {
        if (hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping commit - already committed ({})", txnId(), status());
            if (!executeAt.equals(executeAt()) || status() == Invalidated)
                safeStore.agent().onInconsistentTimestamp(this, (status() == Invalidated ? Timestamp.NONE : this.executeAt()), executeAt);

            if (hasBeen(Committed))
                return CommitOutcome.Redundant;
        }

        Ranges coordinateRanges = coordinateRanges(safeStore);
        // TODO (expected, consider): consider ranges between coordinateRanges and executeRanges? Perhaps don't need them
        Ranges executeRanges = executeRanges(safeStore, executeAt);
        ProgressShard shard = progressShard(safeStore, route, progressKey, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, Check, partialTxn, Add, partialDeps, Set))
            return CommitOutcome.Insufficient;

        setExecuteAt(executeAt);
        set(safeStore, coordinateRanges, executeRanges, shard, route, partialTxn, Add, partialDeps, Set);

        setStatus(Committed);
        logger.trace("{}: committed with executeAt: {}, deps: {}", txnId(), executeAt, partialDeps);
        populateWaitingOn(safeStore);

        safeStore.progressLog().committed(this, shard);

        // TODO (expected, safety): introduce intermediate status to avoid reentry when notifying listeners (which might notify us)
        maybeExecute(safeStore, shard, true, true);
        return CommitOutcome.Success;
    }

    // relies on mutual exclusion for each key
    public void precommit(SafeCommandStore safeStore, Timestamp executeAt)
    {
        if (hasBeen(PreCommitted))
        {
            logger.trace("{}: skipping precommit - already committed ({})", txnId(), status());
            if (executeAt.equals(executeAt()) && status() != Invalidated)
                return;

            safeStore.agent().onInconsistentTimestamp(this, (status() == Invalidated ? Timestamp.NONE : this.executeAt()), executeAt);
        }

        setExecuteAt(executeAt);
        setStatus(PreCommitted);
        notifyListeners(safeStore);
        logger.trace("{}: precommitted with executeAt: {}", txnId(), executeAt);
    }

    protected void populateWaitingOn(SafeCommandStore safeStore)
    {
        Ranges ranges = safeStore.ranges().since(executeAt().epoch());
        if (ranges != null)
        {
            partialDeps().forEach(ranges, txnId -> {
                Command command = safeStore.ifLoaded(txnId);
                if (command == null)
                {
                    addWaitingOnCommit(txnId);
                    safeStore.addAndInvokeListener(txnId, this);
                }
                else
                {
                    switch (command.status())
                    {
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
                        case PreCommitted:
                        case Committed:
                            // TODO (desired, efficiency): split into ReadyToRead and ReadyToWrite;
                            //                             the distributed read can be performed as soon as those keys are ready,
                            //                             and in parallel with any other reads. the client can even ACK immediately after;
                            //                             only the write needs to be postponed until other in-progress reads complete
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

    // TODO (expected, ?): commitInvalidate may need to update cfks _if_ possible
    public void commitInvalidate(SafeCommandStore safeStore)
    {
        if (hasBeen(PreCommitted))
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

    public ApplyOutcome apply(SafeCommandStore safeStore, long untilEpoch, Route<?> route, Timestamp executeAt, @Nullable PartialDeps partialDeps, Writes writes, Result result)
    {
        if (hasBeen(PreApplied) && executeAt.equals(this.executeAt()))
        {
            logger.trace("{}: skipping apply - already executed ({})", txnId(), status());
            return ApplyOutcome.Redundant;
        }
        else if (hasBeen(PreCommitted) && !executeAt.equals(this.executeAt()))
        {
            safeStore.agent().onInconsistentTimestamp(this, this.executeAt(), executeAt);
        }

        Ranges coordinateRanges = coordinateRanges(safeStore);
        Ranges executeRanges = executeRanges(safeStore, executeAt);
        if (untilEpoch < safeStore.latestEpoch())
        {
            Ranges expectedRanges = safeStore.ranges().between(executeAt.epoch(), untilEpoch);
            Invariants.checkState(expectedRanges.containsAll(executeRanges));
        }
        ProgressShard shard = progressShard(safeStore, route, coordinateRanges);

        if (!validate(coordinateRanges, executeRanges, shard, route, Check, null, Check, partialDeps, hasBeen(Committed) ? Add : TrySet))
            return ApplyOutcome.Insufficient; // TODO (expected, consider): this should probably be an assertion failure if !TrySet

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

    @Override
    public PreLoadContext listenerPreLoadContext(TxnId caller)
    {
        return PreLoadContext.contextFor(listOf(txnId(), caller));
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

            case PreCommitted:
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

    protected AsyncChain<Void> applyChain(SafeCommandStore safeStore)
    {
        // important: we can't include a reference to *this* in the lambda, since the C* implementation may evict
        // the command instance from memory between now and the write completing (and post apply being called)
        CommandStore unsafeStore = safeStore.commandStore();
        return writes().apply(safeStore).flatMap(unused -> unsafeStore.submit(this, callPostApply(txnId())));
    }

    private void apply(SafeCommandStore safeStore)
    {
        applyChain(safeStore).begin(AsyncCallbacks.noop());
    }

    public AsyncChain<Data> read(SafeCommandStore safeStore)
    {
        return partialTxn().read(safeStore, this);
    }

    // TODO (expected, API consistency): maybe split into maybeExecute and maybeApply?
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

        if (isWaitingOnDependency())
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
                // TODO (desirable, efficiency): maintain distinct ReadyToRead and ReadyToWrite states
                setStatus(ReadyToExecute);
                logger.trace("{}: set to ReadyToExecute", txnId());
                safeStore.progressLog().readyToExecute(this, shard);
                notifyListeners(safeStore);
                break;

            case PreApplied:
                Ranges executeRanges = executeRanges(safeStore, executeAt());
                boolean intersects = writes().keys.intersects(executeRanges);

                if (intersects)
                {
                    logger.trace("{}: applying", txnId());
                    apply(safeStore);
                }
                else
                {
                    // TODO (desirable, performance): This could be performed immediately upon Committed
                    //      but: if we later support transitive dependency elision this could be dangerous
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
        Invariants.checkState(dependency.hasBeen(PreCommitted));
        if (dependency.hasBeen(Invalidated))
        {
            logger.trace("{}: {} is invalidated. Stop listening and removing from waiting on commit set.", txnId(), dependency.txnId());
            dependency.removeListener(this);
            removeWaitingOnCommit(dependency.txnId());
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
        else if (isWaitingOnDependency())
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
        Invariants.checkState(dependency.hasBeen(PreCommitted));
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
        Known[] blockedUntil = new Known[4];
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
                Known until = blockedUntil[depth];
                if (cur == null)
                {
                    // need to load; schedule execution for later
                    safeStore.execute(this, this).begin(AsyncCallbacks.noop());
                    return;
                }

                if (prev != null)
                {
                    if (cur.has(until) || (cur.hasBeen(PreCommitted) && cur.executeAt().compareTo(prev.executeAt()) > 0))
                    {
                        prev.updatePredecessorAndMaybeExecute(safeStore, cur, false);
                        --depth;
                        prev = get(safeStore, depth - 1);
                        continue;
                    }
                }
                else if (cur.has(until))
                {
                    // we're done; have already applied
                    Invariants.checkState(depth == 0);
                    break;
                }

                TxnId directlyBlockedOnCommit = cur.firstWaitingOnCommit();
                TxnId directlyBlockedOnApply = cur.firstWaitingOnApply(directlyBlockedOnCommit);
                if (directlyBlockedOnApply != null)
                {
                    push(directlyBlockedOnApply, Done);
                }
                else if (directlyBlockedOnCommit != null)
                {
                    push(directlyBlockedOnCommit, ExecuteAtOnly);
                }
                else
                {
                    if (cur.hasBeen(Committed) && !cur.hasBeen(ReadyToExecute) && !cur.isWaitingOnDependency())
                    {
                        if (!cur.maybeExecute(safeStore, cur.progressShard(safeStore), false, false))
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
                prev = cur;
            }
        }

        private Command get(SafeCommandStore safeStore, int i)
        {
            return i >= 0 ? safeStore.command(txnIds[i]) : null;
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
        public Iterable<TxnId> txnIds()
        {
            return Arrays.asList(txnIds).subList(0, depth + 1);
        }

        @Override
        public Seekables<?, ?> keys()
        {
            return Keys.EMPTY;
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
            // TODO (low priority, safety): if we're processed on a node that does not know the latest epoch,
            //      do we guarantee the home key calculation is unchanged since the prior epoch?
            if (progressKey() == null && owns(safeStore, txnId().epoch(), homeKey))
                progressKey(homeKey);
        }
        else if (!this.homeKey().equals(homeKey))
        {
            throw new IllegalStateException();
        }
    }

    private ProgressShard progressShard(SafeCommandStore safeStore, Route<?> route, @Nullable RoutingKey progressKey, Ranges coordinateRanges)
    {
        updateHomeKey(safeStore, route.homeKey());

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

    private ProgressShard progressShard(SafeCommandStore safeStore, Route<?> route, Ranges coordinateRanges)
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

        Ranges coordinateRanges = safeStore.ranges().at(txnId().epoch());
        if (!coordinateRanges.contains(progressKey))
            return No;

        return progressKey.equals(homeKey()) ? Home : Local;
    }

    private Ranges coordinateRanges(SafeCommandStore safeStore)
    {
        return safeStore.ranges().at(txnId().epoch());
    }

    private Ranges executeRanges(SafeCommandStore safeStore, Timestamp executeAt)
    {
        return safeStore.ranges().since(executeAt.epoch());
    }

    enum EnsureAction { Ignore, Check, Add, TrySet, Set }

    /**
     * Validate we have sufficient information for the route, partialTxn and partialDeps fields, and if so update them;
     * otherwise return false (or throw an exception if an illegal state is encountered)
     */
    private boolean validate(Ranges existingRanges, Ranges additionalRanges, ProgressShard shard,
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
                    if (!isFullRoute(route()) && !isFullRoute(route))
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
            Invariants.checkState(status() != Accepted && status() != AcceptedInvalidate);

        // validate new partial txn
        if (!validate(ensurePartialTxn, existingRanges, additionalRanges, covers(partialTxn()), covers(partialTxn), "txn", partialTxn))
            return false;

        if (partialTxn != null && txnId().rw() != partialTxn.kind())
            throw new IllegalArgumentException("Transaction has different kind to its TxnId");

        if (shard.isHome() && ensurePartialTxn != Ignore)
        {
            if (!hasQuery(partialTxn()) && !hasQuery(partialTxn))
                throw new IllegalStateException();
        }

        return validate(ensurePartialDeps, existingRanges, additionalRanges, covers(partialDeps()), covers(partialDeps), "deps", partialDeps);
    }

    private void set(SafeCommandStore safeStore,
                     Ranges existingRanges, Ranges additionalRanges, ProgressShard shard, Route<?> route,
                     @Nullable PartialTxn partialTxn, EnsureAction ensurePartialTxn,
                     @Nullable PartialDeps partialDeps, EnsureAction ensurePartialDeps)
    {
        Invariants.checkState(progressKey() != null);
        Ranges allRanges = existingRanges.with(additionalRanges);

        if (shard.isProgress()) setRoute(Route.merge(route(), (Route)route));
        else setRoute(route.slice(allRanges));

        switch (ensurePartialTxn)
        {
            case Add:
                if (partialTxn == null)
                    break;

                if (partialTxn() != null)
                {
                    partialTxn = partialTxn.slice(allRanges, shard.isHome());
                    Routables.foldlMissing((Seekables)partialTxn.keys(), partialTxn().keys(), (keyOrRange, p, v, i) -> {
                        // TODO (expected, efficiency): we may register the same ranges more than once
                        safeStore.register(keyOrRange, allRanges, this);
                        return v;
                    }, 0, 0, 1);
                    this.setPartialTxn(partialTxn().with(partialTxn));
                    break;
                }

            case Set:
            case TrySet:
                setPartialTxn(partialTxn = partialTxn.slice(allRanges, shard.isHome()));
                // TODO (expected, efficiency): we may register the same ranges more than once
                // TODO (desirable, efficiency): no need to register on PreAccept if already Accepted
                safeStore.register(partialTxn.keys(), allRanges, this);
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

    // TODO (low priority, progress): callers should try to consult the local progress shard (if any) to obtain the full set of keys owned locally
    public Route<?> someRoute()
    {
        if (route() != null)
            return route();

        if (homeKey() != null)
            return PartialRoute.empty(txnId().domain(), homeKey());

        return null;
    }

    public Unseekables<?, ?> maxUnseekables()
    {
        Route<?> route = someRoute();
        if (route == null)
            return null;

        return route.toMaximalUnseekables();
    }

    /**
     * true iff this commandStore owns the given key on the given epoch
     */
    public boolean owns(SafeCommandStore safeStore, long epoch, RoutingKey someKey)
    {
        return safeStore.ranges().at(epoch).contains(someKey);
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
