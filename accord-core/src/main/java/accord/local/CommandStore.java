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

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.DataStore.FetchKind;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.ProtocolModifiers;
import accord.impl.AbstractAsyncExecutor;
import accord.impl.AbstractReplayer;
import accord.local.CommandStores.GainOwnership;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.Commands.NotifyWaitingOnPlus;
import accord.local.ExecutionContext.Empty;
import accord.local.MapReduceCommandStores.Refuse;
import accord.local.RedundantBefore.Bounds;
import accord.local.RedundantStatus.SomeStatus;
import accord.local.durability.DurabilityLevel;
import accord.local.durability.DurabilityResult;
import accord.primitives.RangeRoute;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.EpochReady;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.Reduce;
import accord.utils.ReducingRangeMap;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.AsyncResults.SettableResult;
import accord.utils.async.AsyncResults.SettableWithDescription;
import accord.utils.async.Cancellable;

import static accord.api.DataStore.FetchKind.Image;
import static accord.api.DataStore.FetchKind.Sync;
import static accord.api.ProtocolModifiers.dataStoreRequiresUniqueHlcs;
import static accord.local.BootstrapReason.GAIN_OWNERSHIP;
import static accord.local.MapReduceCommandStores.Refuse.ALL;
import static accord.local.MapReduceCommandStores.Refuse.NONE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.UNREADY;
import static accord.local.RedundantStatus.SomeStatus.GC_BEFORE_AND_LOCALLY_DURABLE;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_DURABLE_TO_COMMAND_STORE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_DURABLE_TO_DATA_STORE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_WITNESSED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.QUORUM_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.SHARD_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.UNREADY_ONLY;
import static accord.local.durability.DurabilityService.SyncLocal.KnownToSelf;
import static accord.local.durability.DurabilityService.SyncLocal.Self;
import static accord.local.durability.DurabilityService.SyncReadable.UnknownReadable;
import static accord.local.durability.DurabilityService.SyncRemote.NoRemote;
import static accord.messages.ReadData.unavailable;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Timestamp.Flag.REJECTED;
import static accord.primitives.Txn.Kind.VisibilitySyncPoint;
import static accord.topology.EpochReady.DONE;
import static accord.topology.EpochReady.done;
import static accord.utils.Invariants.nonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore implements AbstractAsyncExecutor, ExclusiveAsyncExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(CommandStore.class);

    public interface Factory
    {
        CommandStore create(int id,
                            NodeCommandStoreService node,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            LocalListeners.Factory listenersFactory,
                            RangesForEpoch rangesForEpoch,
                            Journal journal);
    }

    protected final int id;
    protected final NodeCommandStoreService node;
    protected final Agent agent;
    protected final DataStore dataStore;
    protected final ProgressLog progressLog;
    protected final LocalListeners listeners;

    // Used in markShardStale to make sure the staleness includes in progress bootstraps
    // TODO (desired): migrate to BTree
    private transient NavigableMap<TxnId, Ranges> bootstrapBeganAt = emptyBootstrapBeganAt(); // additive (i.e. once inserted, rolled-over until invalidated, and the floor entry contains additions)
    protected boolean hasResumedBootstraps;
    private RedundantBefore redundantBefore = RedundantBefore.EMPTY;
    private MaxConflicts maxConflicts = MaxConflicts.EMPTY;
    private MaxDecidedRX maxDecidedRX = MaxDecidedRX.EMPTY;
    private int maxConflictsUpdates = 0, prunedMaxConflictsSize;
    protected RangesForEpoch rangesForEpoch;
    protected @Nullable ReducingRangeMap<Refuse> refuses;
    List<SyncPointListener> syncPointListeners;

    /**
     * safeToRead is related to RedundantBefore, but a distinct concept.
     * While readyAt defines the txnId bounds we expect to maintain data for locally,
     * safeToRead defines executeAt bounds we can safely participate in transaction execution for.
     * safeToRead is defined by the no-op transaction we execute after a bootstrap is initiated,
     * and creates a global bound before which we know we have complete data from our bootstrap.
     *
     * There's a smearing period during bootstrap where some keys may be ahead of others, for instance,
     * since we do not create a precise instant in the transaction log for bootstrap to avoid impeding execution.
     *
     * We also update safeToRead when we go stale, to remove ranges we may have bootstrapped but that are now known to
     * be incomplete. In this case we permit transactions to execute in any order for the unsafe key ranges.
     * But they may still be ordered for other key ranges they participate in.
     */
    private NavigableMap<Timestamp, Ranges> safeToRead = emptySafeToRead();
    protected Ranges permanentlyUnsafeToRead = Ranges.EMPTY;
    private final Set<Bootstrap> bootstraps = Collections.synchronizedSet(new DeterministicIdentitySet<>());

    static class WaitingOnVisibility
    {
        final SettableResult<Void> whenDone;
        final Ranges allRanges;
        final TxnId min;
        Ranges waitingOn, waitingOnDurable;

        // for testing only
        volatile boolean invalid;

        WaitingOnVisibility(SettableResult<Void> whenDone, TxnId min, Ranges ranges)
        {
            this.whenDone = whenDone;
            this.min = min;
            this.allRanges = this.waitingOn = this.waitingOnDurable = ranges;
        }
    }
    private final TreeMap<TxnId, WaitingOnVisibility> waitingOnVisibility = new TreeMap<>();

    protected CommandStore(int id,
                           NodeCommandStoreService node,
                           Agent agent,
                           DataStore dataStore,
                           ProgressLog.Factory progressLogFactory,
                           LocalListeners.Factory listenersFactory,
                           RangesForEpoch rangesForEpoch)
    {
        this.id = id;
        this.node = node;
        this.agent = agent;
        this.dataStore = dataStore;
        this.progressLog = progressLogFactory.create(this);
        this.listeners = listenersFactory.create(this);
        loadRangesForEpoch(rangesForEpoch);
    }

    public final int id()
    {
        return id;
    }

    public abstract Journal.Replayer replayer(AbstractReplayer.Mode mode);
    // expected to invoke safeStore.upsertRedundantBefore at some future point, when the commandStore state is durably persisted
    protected abstract void ensureDurable(Ranges ranges, RedundantBefore onCommandStoreDurable);

    public Agent agent()
    {
        return agent;
    }

    public void unsafeClearForTesting()
    {
        progressLog.clear();
        bootstraps.clear();
        rangesForEpoch = null;
        bootstrapBeganAt = emptyBootstrapBeganAt();
        redundantBefore = RedundantBefore.EMPTY;
        maxConflicts = MaxConflicts.EMPTY;
        maxDecidedRX = MaxDecidedRX.EMPTY;
        safeToRead = emptySafeToRead();
        permanentlyUnsafeToRead = Ranges.EMPTY;
        listeners.clear();
        waitingOnVisibility.values().forEach(w -> w.invalid = true);
        waitingOnVisibility.clear();
    }

    public RangesForEpoch unsafeGetRangesForEpoch()
    {
        return rangesForEpoch;
    }

    public Ranges unsafeGetPermanentlyUnsafeToRead()
    {
        return permanentlyUnsafeToRead;
    }

    public MaxDecidedRX unsafeGetMaxDecidedRX()
    {
        return maxDecidedRX;
    }

    @VisibleForTesting
    public final void unsafeSetRangesForEpoch(RangesForEpoch newRangesForEpoch)
    {
        rangesForEpoch = nonNull(newRangesForEpoch);
        Invariants.require(redundantBefore == null || redundantBefore.foldlWithDefault(rangesForEpoch.all(), (b, v, p) -> v && b != null, null, true, null, v -> !v),
                           "%s owns %s but has insufficient RedundantBefore bounds", this, rangesForEpoch);
    }

    protected void loadRangesForEpoch(RangesForEpoch newRangesForEpoch)
    {
        Invariants.require(this.rangesForEpoch == null || rangesForEpoch.isPrefixOf(newRangesForEpoch));
        this.rangesForEpoch = nonNull(newRangesForEpoch);
        if (redundantBefore.isEmpty() && newRangesForEpoch.size() > 0)
        {
            long minEpoch = rangesForEpoch.epochAtIndex(0);
            loadRedundantBefore(RedundantBefore.create(rangesForEpoch.all(), minEpoch, Long.MAX_VALUE, TxnId.minForEpoch(minEpoch), UNREADY_ONLY));
        }
    }

    protected final void unsafeClearPermanentlyUnsafeToRead()
    {
        permanentlyUnsafeToRead = null;
    }

    protected void loadPermanentlyUnsafeToRead(Ranges newPermanentlyUnsafeToRead)
    {
        Invariants.require(this.permanentlyUnsafeToRead == null);
        unsafeSetPermanentlyUnsafeToRead(newPermanentlyUnsafeToRead);
    }

    public abstract boolean inStore();

    public boolean tryExecuteImmediately(Runnable run)
    {
        if (!inStore())
            return false;

        try { run.run(); }
        catch (Throwable t) { agent.onException(t); }
        return true;
    }

    public abstract AsyncChain<Void> chain(ExecutionContext context, Consumer<? super SafeCommandStore> consumer);
    
    /** See {@link #continuationChain(ExecutionContext, Function)} */
    public abstract AsyncChain<Void> continuationChain(ExecutionContext context, Consumer<? super SafeCommandStore> consumer);

    /** See {@link #continuationChain(ExecutionContext, Function)} */
    public Cancellable executeContinuation(ExecutionContext context, Consumer<? super SafeCommandStore> consumer, BiConsumer<? super Void, Throwable> callback)
    {
        return continuationChain(context, consumer).begin(callback);
    }

    public abstract <T> AsyncChain<T> chain(ExecutionContext context, Function<? super SafeCommandStore, T> apply);

    /**
     * As {@link #chain}, except the task logically continues its submitting task - note, this is the task that invokes {@code begin()},
     * not the task that invokes {@code continuationChain()}.
     * <p>
     * The effect is that:
     *  1) the parent's execution tranche will not complete until this task completes, so that happens-before relations
     *     at the executor include this task;
     *  2) If the enclosing task fails at an abortable point the continuation will also be cancelled;
     *  3) If the task fails and is ATOMIC, the keys over which it failed will become unavailable to prevent inconsistency
     */
    public abstract <T> AsyncChain<T> continuationChain(ExecutionContext context, Function<? super SafeCommandStore, T> apply);

    /**
     * See {@link #continuationChain(ExecutionContext, Function)}
     */
    public <T> Cancellable executeContinuation(ExecutionContext context, Function<? super SafeCommandStore, T> consumer, BiConsumer<? super T, Throwable> callback)
    {
        return continuationChain(context, consumer).begin(callback);
    }

    public Cancellable execute(ExecutionContext context, Consumer<? super SafeCommandStore> consumer, BiConsumer<? super Void, Throwable> callback)
    {
        return chain(context, consumer).begin(callback);
    }

    public AsyncResult<Void> execute(ExecutionContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return chain(context, consumer).beginAsResult();
    }

    public <T> Cancellable execute(ExecutionContext context, Function<? super SafeCommandStore, T> apply, BiConsumer<? super T, Throwable> callback)
    {
        return chain(context, apply).begin(callback);
    }

    public <T> AsyncResult<T> submit(ExecutionContext context, Function<? super SafeCommandStore, T> apply)
    {
        return chain(context, apply).beginAsResult();
    }

    public abstract void shutdown();

    protected void unsafeSetMaxDecidedRX(MaxDecidedRX newMaxDecidedRX)
    {
        this.maxDecidedRX = newMaxDecidedRX;
    }

    final void unsafeSetRedundantBefore(RedundantBefore newRedundantBefore)
    {
        redundantBefore = newRedundantBefore;
    }

    protected void unsafeClearRedundantBefore()
    {
        unsafeSetRedundantBefore(null);
    }

    protected void loadRedundantBefore(RedundantBefore newRedundantBefore)
    {
        Invariants.require(redundantBefore == null || redundantBefore.foldl((b, v) -> (Boolean)(v && b.bounds.length == 1 && b.status(0) == 0 && b.status(1) == UNREADY_ONLY.encoded), true));
        Invariants.require(newRedundantBefore != null && (redundantBefore == null || newRedundantBefore.isAtLeast(redundantBefore)));
        unsafeSetRedundantBefore(newRedundantBefore);
    }

    protected void unsafeUpsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        unsafeSetRedundantBefore(RedundantBefore.merge(redundantBefore, addRedundantBefore));
    }

    @VisibleForTesting
    public boolean unsafeIsRefusingAny()
    {
        return refuses != null;
    }

    /**
     * LLM-generated debugging method
     */
    public String describeState()
    {
        StringBuilder sb = new StringBuilder("store").append(id).append('{');
        ReducingRangeMap<Refuse> refuses = this.refuses;
        sb.append("refuses=").append(refuses == null ? "none" : refuses);
        List<Bootstrap> snapshot;
        // a defensive copy of a synchronizedSet: the copy itself locks, but only for the duration of the copy
        try { snapshot = new ArrayList<>(bootstraps); }
        catch (Throwable t) { snapshot = Collections.emptyList(); }
        if (!snapshot.isEmpty())
            sb.append(", bootstraps=").append(snapshot);
        return sb.append('}').toString();
    }

    protected void unsafeRefuseRequests(SafeCommandStore safeStore, Ranges refuse)
    {
        logger.info("{}: Refusing ALL requests for {}", this, refuse);
        Invariants.require(refuses == null || refuses.foldl(refuse, (v, a) -> v, null, Objects::nonNull) == null, "Already refusing %s", refuse);
        if (refuses == null) refuses = ReducingRangeMap.create(refuse, ALL);
        else refuses = ReducingRangeMap.merge(refuses, ReducingRangeMap.create(refuse, ALL), Refuse::max);
    }

    protected void unsafeAcceptNonDepsRequests(SafeCommandStore safeStore, Ranges accept)
    {
        logger.info("{}: Accepting non-DEPS requests for {}", this, accept);
        Invariants.require(refuses != null && refuses.foldlWithDefault(accept, (v, a) -> v, null, null, v -> ALL != v) == ALL, "Not refusing %s", accept);
        refuses = ReducingRangeMap.merge(refuses, ReducingRangeMap.create(accept, Refuse.DEPS), Refuse::min);
    }

    protected void unsafeAcceptRequests(SafeCommandStore safeStore, Ranges accept)
    {
        logger.info("{}: Accepting ALL requests for {}", this, accept);
        Invariants.require(refuses != null && refuses.foldlWithDefault(accept, (v, a) -> v, null, null, Objects::isNull) != null, "Not refusing %s", accept);
        refuses = ReducingRangeMap.merge(refuses, ReducingRangeMap.create(accept, NONE), (a, b) -> a == NONE || b == NONE || a == null || b == null ? null : a.max(b));
        if (refuses.isEmpty())
            refuses = null;
    }

    /**
     * This method may be invoked on a non-CommandStore thread
     */
    final void unsafeSetSafeToRead(@Nullable NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        if (newSafeToRead != null)
            newSafeToRead = purgeHistory(newSafeToRead, permanentlyUnsafeToRead);
        this.safeToRead = newSafeToRead;
        node.updateStamp();
    }

    final void unsafeSetPermanentlyUnsafeToRead(Ranges newPermanentlyUnsafeToRead)
    {
        this.permanentlyUnsafeToRead = newPermanentlyUnsafeToRead;
    }

    protected final void unsafeClearSafeToRead()
    {
        unsafeSetSafeToRead(null);
    }

    protected void loadSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        Invariants.require(safeToRead == null || safeToRead.equals(emptySafeToRead()));
        Invariants.require(newSafeToRead != null);
        unsafeSetSafeToRead(newSafeToRead);
        updateMaxConflicts(newSafeToRead);
    }

    final void unsafeSetBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        this.bootstrapBeganAt = newBootstrapBeganAt;
    }

    protected final void unsafeClearBootstrapBeganAt()
    {
        unsafeSetBootstrapBeganAt(null);
    }

    protected synchronized void loadBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        Invariants.require(bootstrapBeganAt == null || bootstrapBeganAt.equals(emptyBootstrapBeganAt()));
        Invariants.require(newBootstrapBeganAt != null);
        unsafeSetBootstrapBeganAt(newBootstrapBeganAt);
        updateMaxConflicts(newBootstrapBeganAt);
    }

    /**
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    protected void unsafeSetMaxConflicts(MaxConflicts maxConflicts)
    {
        this.maxConflicts = maxConflicts;
    }

    protected int dumpCounter = 0;

    protected void updateMaxConflicts(Command prev, Command updated, boolean force)
    {
        Timestamp executeAt = updated.executeAt();
        if (executeAt == null) return;
        if (prev != null && prev.executeAt() != null && prev.executeAt().compareToStrict(executeAt) >= 0 && !force) return;
        executeAt = executeAt.flattenUniqueHlc(); // this is what guarantees a bootstrap recipient can compute uniqueHlc safely
        MaxConflicts updatedMaxConflicts = maxConflicts.update(updated.txnId(), updated.participants().hasTouched(), executeAt);
        if (Invariants.isParanoid())
            Invariants.require(updatedMaxConflicts.getAny(updated.txnId(), updated.participants().hasTouched()).compareTo(executeAt) >= 0);
        updateMaxConflicts(executeAt, updatedMaxConflicts);
    }

    protected void updateMaxConflicts(Ranges ranges, Timestamp executeAt)
    {
        updateMaxConflicts(ranges, executeAt, Timestamp.NONE);
    }

    protected void updateMaxConflicts(Ranges ranges, Timestamp executeAt, Timestamp rejectBefore)
    {
        updateMaxConflicts(executeAt, maxConflicts.update(ranges, executeAt, executeAt, rejectBefore));
    }

    protected void updateMaxConflicts(NavigableMap<? extends Timestamp, Ranges> map)
    {
        Timestamp max = Timestamp.NONE;
        MaxConflicts updated = maxConflicts;
        for (Map.Entry<? extends Timestamp, Ranges> e : map.entrySet())
        {
            Timestamp at = e.getKey();
            if (at.compareTo(Timestamp.NONE) > 0)
            {
                updated = updated.update(e.getValue(), at, at);
                max = Timestamp.max(max, at);
            }
        }
        if (updated != maxConflicts)
            updateMaxConflicts(max, updated);
    }

    protected void updateMaxConflicts(Timestamp executeAt, MaxConflicts updatedMaxConflicts)
    {
        if (++maxConflictsUpdates >= Math.max(prunedMaxConflictsSize, 128))
        {
            if (updatedMaxConflicts.size() >= prunedMaxConflictsSize * 2)
            {
                long pruneHlc = executeAt.hlc() - agent.maxConflictsHlcPruneDelta();
                Timestamp pruneBefore = pruneHlc > 0 ? Timestamp.fromValues(executeAt.epoch(), pruneHlc, executeAt.node) : null;
                Ranges ranges = rangesForEpoch.all();
                if (pruneBefore != null)
                    updatedMaxConflicts = updatedMaxConflicts.update(ranges, pruneBefore, pruneBefore);

                maxConflictsUpdates = 0;
                prunedMaxConflictsSize = updatedMaxConflicts.size();
            }
            else
            {
                maxConflictsUpdates = prunedMaxConflictsSize * 2 - updatedMaxConflicts.size();
            }
        }
        unsafeSetMaxConflicts(updatedMaxConflicts);
    }

    final void markExclusiveSyncPointDecided(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        unsafeSetMaxDecidedRX(maxDecidedRX.update(ranges, txnId));
    }

    protected void markExclusiveSyncPointLocallyApplied(SafeCommandStore safeStore, TxnId txnId, TxnId txnIdWithFlags, RangeRoute route, Ranges ranges, SaveStatus prevStatus)
    {
        // TODO (desired): narrow ranges to those that are owned
        if (prevStatus.compareTo(SaveStatus.Applied) < 0)
        {
            String alreadyApplied = redundantBefore.foldl(ranges, (b, m) -> {
                if (b.maxBound(LOCALLY_APPLIED).compareTo(txnIdWithFlags) > 0 && b.maxBound(UNREADY).compareTo(txnIdWithFlags) <= 0 && !b.isLocallyRetired())
                    return m + (m.isEmpty() ? "" : ", ") + b.range + ": " + b;
                return m;
            }, "");
            Invariants.expect(alreadyApplied.isEmpty(), "%s should already have been applied: %s", txnIdWithFlags, alreadyApplied);
        }

        Invariants.requireArgument(txnIdWithFlags.isSyncPoint());
        RedundantBefore addNow = RedundantBefore.create(ranges, txnIdWithFlags, LOCALLY_APPLIED_ONLY);
        safeStore.upsertRedundantBefore(addNow);
        RedundantBefore addOnDataStoreDurable = RedundantBefore.create(ranges, txnIdWithFlags, LOCALLY_DURABLE_TO_DATA_STORE_ONLY);
        RedundantBefore addOnCommandStoreDurable = RedundantBefore.create(ranges, txnIdWithFlags, LOCALLY_DURABLE_TO_COMMAND_STORE_ONLY);
        dataStore.ensureDurable(this, ranges, addOnDataStoreDurable, 0);
        ensureDurable(ranges, addOnCommandStoreDurable);
        Ranges unavailable = unavailable(txnId, txnIdWithFlags, ranges, safeStore.ranges(), safeStore.safeToReadAt());
        SortedArrayList<Node.Id> onlySelf = SortedArrayList.ofSorted(node.id());
        node.durability().report(new DurabilityResult(txnId, ranges.without(unavailable), new DurabilityLevel(Self, NoRemote, UnknownReadable, onlySelf), onlySelf, SortedArrayList.empty(), null));
    }

    /**
     * We expect keys to be sliced to those owned by the replica in the coordination epoch
     */
    final Timestamp preaccept(TxnId txnId, Routables<?> keys, SafeCommandStore safeStore, boolean permitFastPath)
    {
        NodeCommandStoreService node = safeStore.node();

        Timestamp maxConflict = maxConflicts.get(txnId, keys);
        boolean isExpired = maxConflict.is(REJECTED) || (safeStore.agent().rejectPreAccept(safeStore.node(), txnId) && !txnId.isSyncPoint());

        if (isExpired)
            return uniqueTimestampOnConflict(txnId).asRejected();

        Timestamp min = TxnId.mergeMax(txnId, maxConflict);
        if (permitFastPath && txnId == min && txnId.epoch() >= node.epoch())
            return txnId;

        return uniqueTimestampOnConflict(min);
    }

    private Timestamp uniqueTimestampOnConflict(Timestamp min)
    {
        switch (ProtocolModifiers.uniqueTimestampOnConflict())
        {
            default: throw UnhandledEnum.unknown(ProtocolModifiers.uniqueTimestampOnConflict());
            case NOW: return node.uniqueTimestamp(min);
            case STALE: return node.uniqueStaleTimestamp(min);
        }
    }

    /**
     * We expect keys to be sliced to those owned by the replica in the coordination epoch
     */
    public final Timestamp maxConflict(TxnId txnId, Routables<?> keys)
    {
        return maxConflicts.get(txnId, keys);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{id=" + id + ", node=" + node.id().id + '}';
    }

    public final AsyncResult<Void> cancelBootstraps()
    {
        return submit((Empty)() -> "Cancel Bootstraps", safeStore -> {
            cancelBootstraps(safeStore, safeStore.ranges().all());
            return null;
        });
    }

    public final void cancelBootstraps(SafeCommandStore safeStore, Ranges ranges)
    {
        Invariants.require(safeStore.commandStore() == this && inStore());
        bootstraps.forEach(b -> b.invalidate(ranges));
    }

    public final AsyncResult<EpochReady> resumeBootstrap(Node node, BootstrapReason reason)
    {
        synchronized (this)
        {
            Invariants.require(!hasResumedBootstraps);
            hasResumedBootstraps = true;
        }

        return submit((Empty)() -> "Resume Bootstrap", safeStore -> {
            Ranges unfinished = rangesForEpoch.all();
            unfinished = unfinished.without(safeToRead.lastEntry().getValue());
            unfinished = redundantBefore.removeLostOrStale(unfinished);
            for (Bootstrap bootstrap : bootstraps)
                unfinished = unfinished.without(bootstrap.all);

            long epoch = rangesForEpoch.epochAtIndex(0);
            if (unfinished.isEmpty())
                return done(epoch);

            logger.info("{}: Resuming bootstrap of {}", this, unfinished);
            return startBootstrapInternal(node, safeStore, unfinished, epoch, Image, reason);
        });
    }

    /**
     * Defer submitting the work until we have wired up any changes to topology in memory, then first submit the work
     * to setup any state in the command store, and finally submit the distributed work to bootstrap the data locally.
     * So, the outer future's success is sufficient for the topology to be acknowledged, and the inner future for the
     * bootstrap to be complete.
     */
    final Supplier<EpochReady> bootstrapper(Node node, Ranges newRanges, long epoch, GainOwnership action)
    {
        switch (action)
        {
            default: throw new UnhandledEnum(action);
            case NEW_RANGE:
                return () -> {
                    AsyncResult<Void> done = execute((Empty) () -> "Initialise New Epoch", (safeStore) -> {
                        logger.info("{}: Initialising {} for epoch {}", this, newRanges, (Long)epoch);
                        // Merge in a base for any ranges that needs to be covered
                        Ranges newBootstrapRanges = newRanges;
                        for (Ranges existing : bootstrapBeganAt.values())
                            newBootstrapRanges = newBootstrapRanges.without(existing);
                        if (!newBootstrapRanges.isEmpty())
                            safeStore.setBootstrapBeganAt(bootstrap(TxnId.NONE, newBootstrapRanges, bootstrapBeganAt));
                        safeStore.setSafeToRead(purgeAndInsert(safeToRead, TxnId.NONE, newRanges));
                        markExclusiveSyncPointDecided(safeStore, TxnId.NONE, newRanges);
                    });

                    return EpochReady.all(epoch, done);
                };
            case REASSIGNED:
                return () -> startBootstrap(node, newRanges, epoch, Image, GAIN_OWNERSHIP);
        }
    }

    public EpochReady rebootstrap(Node node, BootstrapReason reason)
    {
        return rebootstrap(node, null, reason);
    }

    public EpochReady rebootstrap(Node node, @Nullable Ranges ranges, BootstrapReason reason)
    {
        Invariants.requireArgument(reason != GAIN_OWNERSHIP);
        RangesForEpoch rfe = unsafeGetRangesForEpoch();
        long epoch = node.epoch();
        Invariants.require(epoch >= rfe.epochAtIndex(rfe.size() - 1));
        Ranges bootstrapRanges = rfe.currentRanges();
        if (ranges != null)
            bootstrapRanges = bootstrapRanges.slice(ranges, Minimal);
        return startBootstrap(node, bootstrapRanges, epoch, Sync, reason);
    }

    protected EpochReady startBootstrap(Node node, Ranges newRanges, long epoch, FetchKind fetchKind, BootstrapReason reason)
    {
        if (newRanges.isEmpty())
            return EpochReady.done(epoch);

        return EpochReady.wrap(epoch, node.withEpochAtLeast(epoch, null, () -> chain((Empty) () -> "New Epoch", safeStore -> {
            return startBootstrapInternal(node, safeStore, newRanges, epoch, fetchKind, reason);
        })).beginAsResult());
    }

    private static final AsyncResult<Void> MUST_OVERWRITE = AsyncResults.failure(new IllegalStateException());
    private EpochReady startBootstrapInternal(Node node, SafeCommandStore safeStore, Ranges newRanges, long epoch, FetchKind fetchKind, BootstrapReason reason)
    {
        logger.info("{}: Starting Safe Bootstrap for {} for epoch {}", this, newRanges, (Long)epoch);
        Bootstrap bootstrap = new Bootstrap(node, this, epoch, newRanges, fetchKind, reason);
        bootstraps.add(bootstrap);
        bootstrap.start(safeStore);
        return new EpochReady(epoch,
                              MUST_OVERWRITE,
                              bootstrap.refusing,
                              bootstrap.notRefusing,
                              bootstrap.coordinate,
                              bootstrap.data,
                              bootstrap.reads);
    }

    /**
     * Defer submitting the work until we have wired up any changes to topology in memory, then first submit the work
     * to setup any state in the command store, and finally submit the distributed work to bootstrap the data locally.
     * So, the outer future's success is sufficient for the topology to be acknowledged, and the inner future for the
     * bootstrap to be complete.
     */
    protected Supplier<EpochReady> refreshReadyToCoordinate(Node node, Ranges ranges, long epoch)
    {
        return () -> {
            AsyncResult<Void> readyToCoordinate = readyToCoordinate(ranges, epoch);
            return new EpochReady(epoch, DONE, readyToCoordinate, DONE, DONE);
        };
    }

    // may be invoked by any thread without holding the command store lock
    AsyncResult<Void> readyToCoordinate(Ranges ranges, long epoch)
    {
        TxnId min = TxnId.max(TxnId.minForEpoch(epoch), redundantBefore.max(ranges, b -> b == null ? TxnId.NONE : b.maxBound(UNREADY)));
        if (ranges.isEmpty() || redundantBefore.min(ranges, Bounds::locallyWitnessedBefore).compareTo(min) >= 0)
            return DONE;

        Ranges remaining = redundantBefore.removeWitnessed(min, ranges);
        Invariants.require(!remaining.isEmpty(), "%s has nothing left to witness before %s in %s", this, min, ranges);

        SettableResult<Void> whenDone = new SettableWithDescription<>(this + " is ready to coordinate " + ranges + " after " + min);
        WaitingOnVisibility sync = new WaitingOnVisibility(whenDone, min, remaining);
        synchronized (waitingOnVisibility)
        {
            WaitingOnVisibility prev = waitingOnVisibility.putIfAbsent(min, sync);
            Invariants.require(prev == null, "%s is already waiting on visibility for %s: %s", this, min, prev == null ? null : prev.whenDone);
        }
        ensureReadyToCoordinate(min, ranges, sync, 0);
        return whenDone;
    }

    private void ensureReadyToCoordinate(TxnId min, Ranges ranges, WaitingOnVisibility waiting, int attempts)
    {
        String id = "epoch " + min.epoch() + (min.equals(TxnId.minForEpoch(min.epoch())) ? "" : " (after " + min + ')');
        node.durability().close("[" + this + ' ' + id + ']', VisibilitySyncPoint, min, ranges, KnownToSelf, 1, TimeUnit.HOURS)
            .invoke((success, fail) -> onReadyToCoordinateDurabilityResult(id, ranges, waiting, min, fail, 0, attempts));
    }

    private void onReadyToCoordinateDurabilityResult(String id, Ranges ranges, WaitingOnVisibility waiting, TxnId min, Throwable fail, long deferredIfAwaitingDurabilityMicros, int attempts)
    {
        if (waiting.invalid)
            return;

        Ranges notRetired = redundantBefore.removeLocallyRetired(ranges);
        Ranges retired = ranges.without(notRetired);
        Ranges remaining = redundantBefore.removeWitnessed(min, notRetired);

        if (!retired.isEmpty())
        {
            logger.info("{}, Failed to close {} for ranges {}, but some are retired; marking these as synced.", this, id, ranges, fail);
            execute((Empty)() -> "Mark Retired Ranges Synced", safeStore -> {
                markVisibleInternal(safeStore, min, retired, "(Retired)");
            }, agent);
        }
        else if (remaining.isEmpty())
        {
            if (fail != null)
                logger.info("{}, Failed to close {} for ranges {}, but none remaining. Aborting.", this, id, ranges, fail);
        }

        if (!remaining.isEmpty())
        {
            Ranges waitingOn, waitingOnDurable;
            synchronized (waitingOnVisibility)
            {
                waitingOn = waiting.waitingOn;
                waitingOnDurable = waiting.waitingOnDurable;
            }

            long cycleTimeMicros = node.durability().shards().shardCycleTimeMicros();
            if (waitingOn.isEmpty() && waitingOnDurable.containsAll(remaining) && deferredIfAwaitingDurabilityMicros < cycleTimeMicros * 2)
            {
                // schedule this check for later, to give durability some time to run
                long deferMicros = deferredIfAwaitingDurabilityMicros * 2;
                if (deferMicros == 0) deferMicros = TimeUnit.SECONDS.toMicros(1L);
                if (deferMicros > cycleTimeMicros) deferMicros = cycleTimeMicros;
                long newDeferredIfAwaitingDurabilityMicros = deferredIfAwaitingDurabilityMicros + deferMicros;
                node.scheduler().once(() -> onReadyToCoordinateDurabilityResult(id, ranges, waiting, min, fail, newDeferredIfAwaitingDurabilityMicros, attempts), deferMicros, MICROSECONDS);
                return;
            }
            if (fail != null) logger.error("{} Failed to close {} for ranges {}. Retrying.", this, id, remaining, fail);
            else logger.error("{} Durability request completed successfully, but still awaiting visibility for ranges: {} on {}. Retrying.", this, remaining, id);
            node.scheduler().once(() -> ensureReadyToCoordinate(min, remaining, waiting, attempts + 1), 30L, SECONDS);
        }
    }

    Supplier<EpochReady> unbootstrap(long epoch, RangesForEpoch newRangesForEpoch, Ranges removeRanges)
    {
        return () -> {
            AsyncResult<Void> done = submit((Empty) () -> "Unbootstrap", safeStore -> {
                for (Bootstrap prev : bootstraps)
                {
                    Ranges abort = prev.allValid.slice(removeRanges, Minimal);
                    if (!abort.isEmpty())
                        prev.invalidate(abort);
                }
                Invariants.require(rangesForEpoch.isPrefixOf(newRangesForEpoch));
                RedundantBefore addRedundantBefore = RedundantBefore.create(removeRanges, Long.MIN_VALUE, epoch, TxnId.NONE, SomeStatus.NONE);
                safeStore.setRangesForEpoch(newRangesForEpoch);
                safeStore.upsertRedundantBefore(addRedundantBefore);
                return null;
            });

            return new EpochReady(epoch, done, done, done, done);
        };
    }

    final void complete(Bootstrap bootstrap)
    {
        bootstraps.remove(bootstrap);
    }

    final void markBootstrapping(SafeCommandStore safeStore, NavigableMap<TxnId, Ranges> rangesByBound)
    {
        NavigableMap<TxnId, Ranges> newBootstrapBeganAt = bootstrapBeganAt;
        NavigableMap<Timestamp, Ranges> newSafeToReadAt = safeToRead;
        MaxConflicts newMaxConflicts = maxConflicts;
        RedundantBefore addRedundantBefore = RedundantBefore.EMPTY;
        for (Map.Entry<TxnId, Ranges> e : rangesByBound.entrySet())
        {
            newBootstrapBeganAt = bootstrap(e.getKey(), e.getValue(), newBootstrapBeganAt);
            newSafeToReadAt = purgeHistory(newSafeToReadAt, e.getValue());
            newMaxConflicts = newMaxConflicts.update(e.getValue(), e.getKey(), e.getKey());
            addRedundantBefore = RedundantBefore.merge(addRedundantBefore, RedundantBefore.create(e.getValue(), Long.MIN_VALUE, Long.MAX_VALUE, e.getKey(), UNREADY_ONLY));
        }
        safeStore.setBootstrapBeganAt(newBootstrapBeganAt);
        safeStore.setSafeToRead(newSafeToReadAt);
        unsafeSetMaxConflicts(newMaxConflicts);
        safeStore.upsertRedundantBefore(addRedundantBefore);
    }

    // TODO (expected): we can immediately truncate dependencies locally once an exclusiveSyncPoint applies, we don't need to wait for the whole shard
    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges durableRanges, HasOutcome durability)
    {
        if (!durability.isDurable())
            return;

        SomeStatus status = durability.isUniversal() ? SHARD_APPLIED_ONLY : QUORUM_APPLIED_ONLY;
        final Ranges slicedRanges = durableRanges.slice(safeStore.ranges().allUntil(globalSyncId.epoch()), Minimal);
        TxnId locallyRedundantBefore = safeStore.redundantBefore().min(slicedRanges, Bounds::maxLocallyAppliedBefore);
        RedundantBefore addNow = RedundantBefore.create(slicedRanges, globalSyncId, status);
        safeStore.upsertRedundantBefore(addNow);

        if (status != SHARD_APPLIED_ONLY)
            return;

        if (locallyRedundantBefore.compareTo(globalSyncId) < 0)
        {
            // TODO (expected): if bootstrapping only part of the range, mark the rest for GC; or relax this as can safely GC behind bootstrap
            TxnId maxBootstrap = safeStore.redundantBefore().max(slicedRanges, Bounds::maxReadyAt);
            if (maxBootstrap.compareTo(globalSyncId) >= 0)
                logger.info("Ignoring markShardDurable for a point we are bootstrapping. Bootstrapping: {}, Global: {}, Ranges: {}", maxBootstrap, globalSyncId, slicedRanges);
            else
                logger.warn("Trying to markShardDurable a point we have not yet caught-up to locally. Local: {}, Global: {}, Ranges: {}", locallyRedundantBefore, globalSyncId, slicedRanges);
            return;
        }

        // TODO (desired): not all systems care about HLC_BOUND for GC, make configurable
        if (globalSyncId.is(HLC_BOUND) || !dataStoreRequiresUniqueHlcs())
        {
            RedundantBefore addOnDataStoreDurable = RedundantBefore.create(slicedRanges, globalSyncId, GC_BEFORE_AND_LOCALLY_DURABLE);
            dataStore.ensureDurable(this, slicedRanges, addOnDataStoreDurable, 0);
        }
    }

    protected void upsertedRedundantBefore(SafeCommandStore safeStore, RedundantBefore added)
    {
        TxnId clearWaitingBefore = redundantBefore.minShardAndLocallyAppliedBefore();
        TxnId clearAllBefore = TxnId.min(clearWaitingBefore, durableBefore().min.quorum);
        progressLog.clearBefore(safeStore, clearWaitingBefore, clearAllBefore);
        listeners.cleanupBefore(clearWaitingBefore, added.maxUnreadyBefore());
    }

    @VisibleForTesting
    public AsyncResult<Void> awaitVisibility(long epoch, Ranges ranges)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return AsyncResults.success(null);

            List<AsyncResult<Void>> awaiting = new ArrayList<>();
            for (Map.Entry<TxnId, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey().epoch() > epoch)
                    break;

                Ranges remaining = e.getValue().waitingOn;
                Ranges intersecting = remaining.slice(ranges, Minimal);
                if (!intersecting.isEmpty())
                {
                    awaiting.add(e.getValue().whenDone);
                    ranges = ranges.without(intersecting);
                }
            }

            if (awaiting.isEmpty())
                return AsyncResults.success(null);
            return AsyncResults.debuggableReduce(awaiting, Reduce.toNull());
        }
    }

    protected final Ranges isWaitingOnVisibility(TxnId syncId, Ranges ranges)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return Ranges.EMPTY;

            Ranges waitingOn = Ranges.EMPTY;
            for (Map.Entry<TxnId, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey().compareTo(syncId) > 0)
                    break;

                Ranges remaining = e.getValue().waitingOn;
                Ranges intersecting = remaining.slice(ranges, Minimal);
                if (!intersecting.isEmpty())
                {
                    ranges = ranges.without(intersecting);
                    waitingOn = waitingOn.with(intersecting);
                }
            }

            return waitingOn;
        }
    }

    protected final void markingVisible(TxnId syncId, Ranges ranges)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return;

            for (Map.Entry<TxnId, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey().compareTo(syncId) > 0)
                    break;

                Ranges remaining = e.getValue().waitingOn.without(ranges);
                if (e.getValue().waitingOn != remaining)
                    e.getValue().waitingOn = remaining;
            }
        }
    }

    protected final void cancelMarkingVisible(TxnId syncId, Ranges ranges)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return;

            for (Map.Entry<TxnId, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey().compareTo(syncId) > 0)
                    break;

                Ranges unmark = e.getValue().waitingOnDurable.slice(ranges, Minimal);
                if (!unmark.isEmpty())
                    e.getValue().waitingOn = e.getValue().waitingOn.with(unmark);
            }
        }
    }

    protected final void markVisible(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        Invariants.require(syncId.is(VisibilitySyncPoint));
        RedundantBefore addRedundantBefore = RedundantBefore.create(ranges, syncId, LOCALLY_WITNESSED_ONLY);
        safeStore.upsertRedundantBefore(addRedundantBefore);
        markVisibleInternal(safeStore, syncId, ranges, syncId);
    }

    private void markVisibleInternal(SafeCommandStore safeStore, TxnId achieved, Ranges ranges, Object describe)
    {
        synchronized (waitingOnVisibility)
        {
            if (waitingOnVisibility.isEmpty())
                return;

            HashSet<TxnId> remove = null;
            for (Map.Entry<TxnId, WaitingOnVisibility> e : waitingOnVisibility.entrySet())
            {
                if (e.getKey().compareTo(achieved) > 0)
                    break;

                Ranges waitingOn = e.getValue().waitingOn;
                Ranges waitingOnDurable = e.getValue().waitingOnDurable;
                Ranges synced = waitingOnDurable.slice(ranges, Minimal);
                boolean intersects = waitingOnDurable.intersects(ranges);
                if (intersects)
                {
                    e.getValue().waitingOn = waitingOn = waitingOn.without(ranges);
                    e.getValue().waitingOnDurable = waitingOnDurable = waitingOnDurable.without(ranges);
                    if (waitingOnDurable.isEmpty())
                    {
                        SettableResult<Void> done = e.getValue().whenDone;
                        logger.debug("{} completed full visibility sync for {} on epoch {} using {}", this, e.getValue().allRanges, e.getKey(), describe);
                        done.trySuccess(null);
                        if (remove == null)
                            remove = new HashSet<>();
                        remove.add(e.getKey());
                    }
                    else
                    {
                        logger.debug("{} completed partial visibility sync for {} on epoch {} using {}; {} still to sync and {} to sync durably", this, synced, e.getKey(), describe, waitingOn, waitingOnDurable);
                    }
                }
            }
            if (remove != null)
                remove.forEach(waitingOnVisibility::remove);
        }
    }

    public void markShardStale(SafeCommandStore safeStore, Timestamp staleSince, Ranges ranges, boolean isSincePrecise)
    {
        Timestamp staleUntilAtLeast = staleSince;
        if (isSincePrecise)
        {
            ranges = ranges.slice(safeStore.ranges().allAt(staleSince.epoch()), Minimal);
        }
        else
        {
            ranges = ranges.slice(safeStore.ranges().allSince(staleSince.epoch()), Minimal);
            // make sure no in-progress bootstrap attempts will override the stale since for commands whose staleness bounds are unknown
            staleUntilAtLeast = Timestamp.max(bootstrapBeganAt.lastKey(), staleUntilAtLeast);
        }

        if (ranges.isEmpty())
            return;

        agent.ownershipEvents().onStale(staleSince, ranges);

        RedundantBefore addRedundantBefore = RedundantBefore.createStale(ranges, staleUntilAtLeast);
        safeStore.upsertRedundantBefore(addRedundantBefore);
        // find which ranges need to bootstrap, subtracting those already in progress that cover the id

        markUnsafeToRead(ranges);
    }

    /**
     * This method may need to be called on restart to initialise state for transactions that are waiting for
     * a transaction that has not - and won't be - committed, if state is restored by replay.
     * This is because a transaction that will be invalidated may be behind the durable-to-command-store replay point.
     *
     * This may also be used by an operator to unstick waiting transactions whose transitive dependencies
     * may already be applied.
     */
    public final AsyncResult<Void> tryToExecuteListeningTxns(boolean loopUntilNoNewListeners)
    {
        SettableResult<Void> done = new SettableResult<>();
        execute((Empty)() -> "Try Execute Listening", safeStore -> {
            if (!loopUntilNoNewListeners)
            {
                tryExecuteListening(safeStore, listeners.txnListenersWaitingOn().iterator(), done);
            }
            else
            {
                List<TxnId> txnIds = new ArrayList<>();
                listeners.txnListenersWaitingOn().forEach(txnIds::add);
                if (txnIds.isEmpty()) done.trySuccess(null);
                else
                {
                    txnIds.sort(TxnId::compareTo);
                    Set<TxnId> visited = new HashSet<>(txnIds);
                    TxnId limit = txnIds.get(txnIds.size() - 1);
                    tryExecuteListeningLoop(safeStore, visited, limit, txnIds, done);
                }
            }
        }, agent);
        return done;
    }

    private void tryExecuteListeningLoop(SafeCommandStore safeStore, Set<TxnId> visited, TxnId limit, List<TxnId> txnIds, SettableResult<Void> done)
    {
        SettableResult<Void> continuation = new SettableResult<>();
        continuation.invoke((success, fail) -> {
            if (fail != null) done.tryFailure(fail);
            else
            {
                List<TxnId> newTxnIds = new ArrayList<>();
                listeners.txnListenersWaitingOn().forEach(txnId -> {
                    if (txnId.compareTo(limit) < 0 && visited.add(txnId))
                        newTxnIds.add(txnId);
                });

                if (newTxnIds.isEmpty()) done.trySuccess(null);
                else
                {
                    newTxnIds.sort(TxnId::compareTo);
                    tryExecuteListeningLoop(safeStore, visited, limit, newTxnIds, done);
                }
            }
        });
        tryExecuteListening(safeStore, txnIds.iterator(), continuation);
    }

    private void tryExecuteListening(SafeCommandStore safeStore, Iterator<TxnId> iterator, SettableResult<Void> done)
    {
        if (!iterator.hasNext())
        {
            done.trySuccess(null);
            return;
        }

        try
        {
            TxnId waitingOn = iterator.next();
            ExecutionContext context = ExecutionContext.unsequenced(waitingOn, "Try Execute Listening");
            if (!safeStore.canExecuteWith(context) || !safeStore.tryRecurse())
            {
                //noinspection DataFlowIssue
                safeStore = safeStore;
                execute(context, safeStore0 -> { tryExecuteListening(safeStore0, waitingOn, iterator, done); }, agent);
            }
            else
            {
                try { tryExecuteListening(safeStore, waitingOn, iterator, done); }
                finally { safeStore.unrecurse(); }
            }
        }
        catch (Throwable t)
        {
            done.tryFailure(t);
        }
    }

    private void tryExecuteListening(SafeCommandStore safeStore, TxnId waitingOn, Iterator<TxnId> iterator, SettableResult<Void> done)
    {
        try
        {
            SafeCommand safeCommand = safeStore.unsafeTryGetNoLogFault(waitingOn);
            if (safeCommand == null)
            {
                tryExecuteListening(safeStore, iterator, done);
                return;
            }

            //noinspection DataFlowIssue
            safeStore = safeStore;
            //noinspection DataFlowIssue
            safeCommand = safeCommand;
            boolean wasApplied = safeCommand.current().hasBeen(Status.Applied);
            Consumer<SafeCommandStore> continuation = safeStore0 -> {
                if (!wasApplied)
                {
                    SafeCommand safeCommand0 = safeStore0.unsafeIfLoadedAndInitialisedNoCleanup(waitingOn);
                    if (safeCommand0 != null && safeCommand0.current().saveStatus().hasBeen(Status.Applied))
                        logger.warn("{} was successfully applied by tryToExecuteListening", waitingOn);
                }
                tryExecuteListening(safeStore0, iterator, done);
            };

            Commands.maybeExecute(safeStore, safeCommand, safeCommand.current(), true, true, NotifyWaitingOnPlus.adapter(continuation, true, true));
        }
        catch (Throwable t)
        {
            done.tryFailure(t);
        }
    }

    public final boolean isRejectedIfNotPreAccepted(TxnId txnId, Unseekables<?> participants)
    {
        return maxConflicts.get(txnId, participants).is(REJECTED);
    }

    public final MaxConflicts unsafeGetMaxConflicts()
    {
        return maxConflicts;
    }

    public final RedundantBefore unsafeGetRedundantBefore()
    {
        return redundantBefore;
    }

    public final LocalListeners unsafeGetListeners()
    {
        return listeners;
    }

    public final DurableBefore durableBefore()
    {
        return node.durableBefore();
    }

    public final ProgressLog unsafeProgressLog()
    {
        return progressLog;
    }

    @VisibleForTesting
    public final NavigableMap<TxnId, Ranges> unsafeGetBootstrapBeganAt() { return bootstrapBeganAt; }

    @VisibleForTesting
    public NavigableMap<Timestamp, Ranges> unsafeGetSafeToRead() { return safeToRead; }

    final void markUnsafeToRead(Ranges ranges)
    {
        if (safeToRead.values().stream().anyMatch(r -> r.intersects(ranges)))
        {
            execute((Empty) () -> "Mark Unsafe To Read", safeStore -> {
                safeStore.markUnsafeToRead(ranges);
            }, agent);
        }
    }

    final AsyncChain<Void> markPermanentlyUnsafeToRead(Ranges ranges)
    {
        return chain((Empty) () -> "Mark Range As Permanently Unsafe To Read", safeStore -> {
            safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));
            safeStore.setPermanentlyUnsafeToRead(permanentlyUnsafeToRead.union(MERGE_ADJACENT, ranges));
        });
    }

    public final DataStore unsafeGetDataStore()
    {
        return dataStore;
    }

    final synchronized AsyncResult<Void> markSafeToRead(Timestamp forBootstrapAt, Timestamp at, Ranges ranges)
    {
        return execute((Empty) () -> "Mark Safe To Read", safeStore -> {
            // TODO (required): handle weird edge cases like newer at having a lower HLC than prior existing at, but higher epoch
            Ranges validatedSafeToRead = redundantBefore.validateSafeToRead(forBootstrapAt, ranges);
            safeStore.setSafeToRead(purgeAndInsert(safeToRead, at, validatedSafeToRead));
            updateMaxConflicts(ranges, at);
        });
    }

    public static ImmutableSortedMap<TxnId, Ranges> bootstrap(TxnId at, Ranges ranges, NavigableMap<TxnId, Ranges> readyAt)
    {
        Invariants.requireArgument(!ranges.isEmpty());
        if (at == TxnId.NONE)
        {
            for (Ranges rs : readyAt.values())
                Invariants.require(!ranges.intersects(rs));
        }
        // if we're bootstrapping these ranges, then any period we previously owned the ranges for is effectively invalidated
        return purgeAndInsert(readyAt, at, ranges);
    }

    private static <T extends Timestamp> ImmutableSortedMap<T, Ranges> purgeAndInsert(NavigableMap<T, Ranges> in, T insertAt, Ranges insert)
    {
        TreeMap<T, Ranges> build = new TreeMap<>(in);
        build.headMap(insertAt, false).entrySet().forEach(e -> e.setValue(e.getValue().without(insert)));
        build.tailMap(insertAt, true).entrySet().forEach(e -> e.setValue(e.getValue().union(MERGE_ADJACENT, insert)));
        build.entrySet().removeIf(e -> e.getKey().compareTo(Timestamp.NONE) > 0 && e.getValue().isEmpty());
        Map.Entry<T, Ranges> prev = build.floorEntry(insertAt);
        build.putIfAbsent(insertAt, prev.getValue().with(insert));
        return ImmutableSortedMap.copyOf(build);
    }

    static ImmutableSortedMap<Timestamp, Ranges> purgeHistory(NavigableMap<Timestamp, Ranges> in, Ranges remove)
    {
        return ImmutableSortedMap.copyOf(purgeHistoryIterator(in, remove));
    }

    private static <T extends Timestamp> Iterable<Map.Entry<T, Ranges>> purgeHistoryIterator(NavigableMap<T, Ranges> in, Ranges removeRanges)
    {
        return () -> in.entrySet().stream()
                       .map(e -> without(e, removeRanges))
                       .filter(e -> !e.getValue().isEmpty() || e.getKey().equals(TxnId.NONE))
                       .iterator();
    }

    private static <T extends Timestamp> Map.Entry<T, Ranges> without(Map.Entry<T, Ranges> in, Ranges remove)
    {
        Ranges without = in.getValue().without(remove);
        if (without == in.getValue())
            return in;
        return new SimpleImmutableEntry<>(in.getKey(), without);
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    public boolean isBootstrapping()
    {
        return !bootstraps.isEmpty();
    }

    public void updateMinHlc(long minHlc)
    {
        Timestamp timestamp = Timestamp.fromValues(rangesForEpoch.epochs[rangesForEpoch.epochs.length - 1], minHlc, 0, node.id());
        MaxConflicts updated = maxConflicts.update(rangesForEpoch.all(), timestamp, timestamp);
        unsafeSetMaxConflicts(updated);
    }

    public static NavigableMap<TxnId, Ranges> emptyBootstrapBeganAt()
    {
        return ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
    }

    public static NavigableMap<Timestamp, Ranges> emptySafeToRead()
    {
        return ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
    }

    public NodeCommandStoreService node()
    {
        return node;
    }

    void unsafeRegister(SyncPointListener listener)
    {
        Invariants.require(inStore());
        List<SyncPointListener> newListeners = new ArrayList<>();
        if (syncPointListeners != null)
            newListeners.addAll(syncPointListeners);
        newListeners.add(listener);
        syncPointListeners = newListeners;
    }

    void unsafeUnregister(SyncPointListener listener)
    {
        Invariants.require(inStore());
        if (syncPointListeners != null)
        {
            List<SyncPointListener> newListeners = new ArrayList<>(syncPointListeners);
            newListeners.remove(listener);
            if (newListeners.isEmpty())
                newListeners = null;
            syncPointListeners = newListeners;
        }
    }
}
