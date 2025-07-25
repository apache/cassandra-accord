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

import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.DataStore;

import javax.annotation.Nullable;
import accord.api.Agent;

import accord.local.CommandStores.RangesForEpoch;
import accord.local.RedundantBefore.Bounds;
import accord.local.RedundantStatus.SomeStatus;
import accord.primitives.RangeDeps;
import accord.primitives.Routables;
import accord.primitives.Status.Durability;
import accord.primitives.Unseekables;
import accord.utils.async.AsyncChain;

import accord.api.ConfigurationService.EpochReady;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResults;
import org.agrona.collections.LongHashSet;

import static accord.api.ConfigurationService.EpochReady.DONE;
import static accord.api.ProtocolModifiers.Toggles.requiresUniqueHlcs;
import static accord.local.PreLoadContext.empty;
import static accord.local.RedundantStatus.SomeStatus.GC_BEFORE_AND_LOCALLY_APPLIED;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_WITNESSED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.MAJORITY_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.PRE_BOOTSTRAP_ONLY;
import static accord.local.RedundantStatus.SomeStatus.SHARD_APPLIED_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOCALLY_INCOMPLETE_ONLY;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Invariants.nonNull;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore implements SequentialAsyncExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(CommandStore.class);

    public static class EpochUpdate
    {
        public final RangesForEpoch newRangesForEpoch;
        public final RedundantBefore addRedundantBefore;

        EpochUpdate(RangesForEpoch newRangesForEpoch, RedundantBefore addRedundantBefore)
        {
            this.newRangesForEpoch = newRangesForEpoch;
            this.addRedundantBefore = addRedundantBefore;
        }
    }

    public static class EpochUpdateHolder extends AtomicReference<EpochUpdate>
    {
        // TODO (desired): can better encapsulate by accepting only the newRangesForEpoch and deriving the add/remove ranges
        public void add(long epoch, RangesForEpoch newRangesForEpoch, Ranges addRanges)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.create(addRanges, epoch, Long.MAX_VALUE, TxnId.minForEpoch(epoch), PRE_BOOTSTRAP_ONLY);
            update(newRangesForEpoch, addRedundantBefore);
        }

        public void remove(long epoch, RangesForEpoch newRangesForEpoch, Ranges removeRanges)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.create(removeRanges, Long.MIN_VALUE, epoch, TxnId.NONE, SomeStatus.NONE);
            update(newRangesForEpoch, addRedundantBefore);
        }

        private void update(RangesForEpoch newRangesForEpoch, RedundantBefore addRedundantBefore)
        {
            EpochUpdate baseUpdate = new EpochUpdate(newRangesForEpoch, addRedundantBefore);
            EpochUpdate cur = get();
            if (cur == null || !compareAndSet(cur, new EpochUpdate(newRangesForEpoch, RedundantBefore.merge(cur.addRedundantBefore, addRedundantBefore))))
                set(baseUpdate);
        }
    }

    public interface Factory
    {
        CommandStore create(int id,
                            NodeCommandStoreService node,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            LocalListeners.Factory listenersFactory,
                            EpochUpdateHolder rangesForEpoch,
                            Journal journal);
    }

    protected final int id;
    protected final NodeCommandStoreService node;
    protected final Agent agent;
    protected final DataStore dataStore;
    protected final ProgressLog progressLog;
    protected final LocalListeners listeners;
    protected final EpochUpdateHolder epochUpdateHolder;

    // Used in markShardStale to make sure the staleness includes in progress bootstraps
    // TODO (desired): migrate to BTree
    private transient NavigableMap<TxnId, Ranges> bootstrapBeganAt = emptyBootstrapBeganAt(); // additive (i.e. once inserted, rolled-over until invalidated, and the floor entry contains additions)
    private RedundantBefore redundantBefore = RedundantBefore.EMPTY;
    private MaxConflicts maxConflicts = MaxConflicts.EMPTY;
    private MaxDecidedRX maxDecidedRX = MaxDecidedRX.EMPTY;
    private int maxConflictsUpdates = 0;
    protected RangesForEpoch rangesForEpoch;
    private boolean rebootstrapping = false;

    /**
     * safeToRead is related to RedundantBefore, but a distinct concept.
     * While bootstrappedAt defines the txnId bounds we expect to maintain data for locally,
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
    private final Set<Bootstrap> bootstraps = Collections.synchronizedSet(new DeterministicIdentitySet<>());
    @Nullable private RejectBefore rejectBefore;

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
    }

    static class WaitingOnSync
    {
        final AsyncResults.SettableResult<Void> whenDone;
        final Ranges allRanges;
        Ranges ranges;

        WaitingOnSync(AsyncResults.SettableResult<Void> whenDone, Ranges ranges)
        {
            this.whenDone = whenDone;
            this.allRanges = this.ranges = ranges;
        }
    }
    private final TreeMap<Long, WaitingOnSync> waitingOnSync = new TreeMap<>();

    protected CommandStore(int id,
                           NodeCommandStoreService node,
                           Agent agent,
                           DataStore dataStore,
                           ProgressLog.Factory progressLogFactory,
                           LocalListeners.Factory listenersFactory,
                           EpochUpdateHolder epochUpdateHolder)
    {
        this.id = id;
        this.node = node;
        this.agent = agent;
        this.dataStore = dataStore;
        this.progressLog = progressLogFactory.create(this);
        this.listeners = listenersFactory.create(this);
        this.epochUpdateHolder = epochUpdateHolder;
    }

    public final int id()
    {
        return id;
    }

    public void restore() {};

    public abstract Journal.Loader loader();

    public Agent agent()
    {
        return agent;
    }

    public void updateRangesForEpoch(SafeCommandStore safeStore)
    {
        EpochUpdate update = epochUpdateHolder.get();
        if (update == null)
            return;

        update = epochUpdateHolder.getAndSet(null);
        if (update.addRedundantBefore.size() > 0)
            safeStore.upsertRedundantBefore(update.addRedundantBefore);
        if (update.newRangesForEpoch != null)
            safeStore.setRangesForEpoch(update.newRangesForEpoch);

        safeStore.persistFieldUpdates();
    }

    @VisibleForTesting
    public void unsafeUpdateRangesForEpoch()
    {
        EpochUpdate update = epochUpdateHolder.getAndSet(null);
        if (update == null)
            return;

        if (update.addRedundantBefore.size() > 0)
            unsafeUpsertRedundantBefore(update.addRedundantBefore);
        if (update.newRangesForEpoch != null)
            unsafeSetRangesForEpoch(update.newRangesForEpoch);
    }

    public RangesForEpoch unsafeGetRangesForEpoch()
    {
        return rangesForEpoch;
    }

    public MaxDecidedRX unsafeGetMaxDecidedRX()
    {
        return maxDecidedRX;
    }

    @VisibleForTesting
    public final void unsafeSetRangesForEpoch(RangesForEpoch newRangesForEpoch)
    {
        rangesForEpoch = nonNull(newRangesForEpoch);
    }

    protected final void unsafeClearRangesForEpoch()
    {
        rangesForEpoch = null;
    }

    protected void loadRangesForEpoch(RangesForEpoch newRangesForEpoch)
    {
        Invariants.require(this.rangesForEpoch == null);
        unsafeSetRangesForEpoch(newRangesForEpoch);
    }

    public abstract boolean inStore();

    public void maybeExecuteImmediately(Runnable task)
    {
        if (inStore())
        {
            try { task.run(); }
            catch (Throwable t) { agent.onUncaughtException(t); }
        }
        else
        {
            execute(task);
        }
    }

    public abstract AsyncChain<Void> build(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    public abstract <T> AsyncChain<T> build(PreLoadContext context, Function<? super SafeCommandStore, T> apply);

    @Override
    public void execute(Runnable command)
    {
        execute(command, agent);
    }

    public void execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer, BiConsumer<? super Void, Throwable> callback)
    {
        build(context, consumer).begin(callback);
    }

    public AsyncResult<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return build(context, consumer).beginAsResult();
    }

    public <T> void submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply, BiConsumer<? super T, Throwable> callback)
    {
        build(context, apply).begin(callback);
    }

    public <T> AsyncResult<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply)
    {
        return build(context, apply).beginAsResult();
    }

    public abstract void shutdown();

    protected abstract void registerTransitive(SafeCommandStore safeStore, RangeDeps deps);

    protected void unsafeSetMaxDecidedRX(MaxDecidedRX newMaxDecidedRX)
    {
        this.maxDecidedRX = newMaxDecidedRX;
    }

    protected void unsafeSetRejectBefore(RejectBefore newRejectBefore)
    {
        this.rejectBefore = newRejectBefore;
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
        Invariants.require(redundantBefore == null || redundantBefore.equals(RedundantBefore.EMPTY));
        Invariants.require(newRedundantBefore != null);
        unsafeSetRedundantBefore(newRedundantBefore);
    }

    protected void unsafeUpsertRedundantBefore(RedundantBefore addRedundantBefore)
    {
        redundantBefore = RedundantBefore.merge(redundantBefore, addRedundantBefore);
    }

    protected void unsafeSetRebootstrapping(boolean val)
    {
        rebootstrapping = val;
    }

    protected boolean unsafeGetRebootstrapping()
    {
        return rebootstrapping;
    }


    /**
     * This method may be invoked on a non-CommandStore thread
     */
    final void unsafeSetSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        node.updateStamp();
        this.safeToRead = newSafeToRead;
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
    protected void setMaxConflicts(MaxConflicts maxConflicts)
    {
        this.maxConflicts = maxConflicts;
    }

    protected int dumpCounter = 0;

    protected void updateMaxConflicts(Command prev, Command updated)
    {
        Timestamp executeAt = updated.executeAt();
        if (executeAt == null) return;
        if (prev != null && prev.executeAt() != null && prev.executeAt().compareToStrict(executeAt) >= 0) return;
        executeAt = executeAt.flattenUniqueHlc(); // this is what guarantees a bootstrap recipient can compute uniqueHlc safely
        MaxConflicts updatedMaxConflicts = maxConflicts.update(updated.participants().hasTouched(), executeAt);
        updateMaxConflicts(executeAt, updatedMaxConflicts);
    }

    protected void updateMaxConflicts(Ranges ranges, Timestamp executeAt)
    {
        updateMaxConflicts(executeAt, maxConflicts.update(ranges, executeAt));
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
                updated = updated.update(e.getValue(), at);
                max = Timestamp.max(max, at);
            }
        }
        if (updated != maxConflicts)
            updateMaxConflicts(max, updated);
    }

    protected void updateMaxConflicts(Timestamp executeAt, MaxConflicts updatedMaxConflicts)
    {
        if (++maxConflictsUpdates >= agent.maxConflictsPruneInterval())
        {
            int initialSize = updatedMaxConflicts.size();
            MaxConflicts initialConflicts = updatedMaxConflicts;
            long pruneHlc = executeAt.hlc() - agent.maxConflictsHlcPruneDelta();
            Timestamp pruneBefore = pruneHlc > 0 ? Timestamp.fromValues(executeAt.epoch(), pruneHlc, executeAt.node) : null;
            Ranges ranges = rangesForEpoch.all();
            if (pruneBefore != null)
                updatedMaxConflicts = updatedMaxConflicts.update(ranges, pruneBefore);

            int prunedSize = updatedMaxConflicts.size();
            if (initialSize > 100 && prunedSize == initialSize)
            {
                logger.debug("Ineffective prune for {}. Initial size: {}, pruned size: {}, executeAt: {}, pruneBefore: {}", ranges, initialSize, prunedSize, executeAt, pruneBefore);
                if (dumpCounter == 0)
                {
                    logger.trace("initial MaxConflicts dump: {}", initialConflicts);
                    logger.trace("pruned MaxConflicts dump: {}", updatedMaxConflicts);
                }
                dumpCounter++;
                dumpCounter %= 100;
            }
            else if (prunedSize != initialSize)
            {
                logger.trace("Successfully pruned {} to {}", initialSize, prunedSize);
            }


            maxConflictsUpdates = 0;
        }
        setMaxConflicts(updatedMaxConflicts);
    }

    final void markExclusiveSyncPoint(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        // TODO (desired): narrow ranges to those that are owned
        Invariants.requireArgument(txnId.is(ExclusiveSyncPoint));
        RejectBefore newRejectBefore = rejectBefore != null ? rejectBefore : new RejectBefore();
        newRejectBefore = RejectBefore.add(newRejectBefore, ranges, txnId);
        unsafeSetRejectBefore(newRejectBefore);
    }

    final void markExclusiveSyncPointDecided(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        unsafeSetMaxDecidedRX(maxDecidedRX.update(ranges, txnId));
    }

    final void markExclusiveSyncPointLocallyApplied(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        // TODO (desired): narrow ranges to those that are owned
        Invariants.requireArgument(txnId.is(ExclusiveSyncPoint));
        RedundantBefore newRedundantBefore = RedundantBefore.merge(redundantBefore, RedundantBefore.create(ranges, txnId, LOCALLY_APPLIED_ONLY));
        safeStore.upsertRedundantBefore(newRedundantBefore);
        unsafeSetRedundantBefore(newRedundantBefore);
        updatedRedundantBefore(safeStore, txnId, ranges);
    }

    /**
     * We expect keys to be sliced to those owned by the replica in the coordination epoch
     */
    final Timestamp preaccept(TxnId txnId, Routables<?> keys, SafeCommandStore safeStore, boolean permitFastPath)
    {
        NodeCommandStoreService node = safeStore.node();

        boolean isExpired = safeStore.agent().rejectPreAccept(safeStore.node(), txnId) && !txnId.isSyncPoint();
        if (rejectBefore != null && !isExpired)
            isExpired = rejectBefore.rejects(txnId, keys);

        if (isExpired)
            return node.uniqueTimestamp(txnId).asRejected();

        Timestamp min = TxnId.mergeMax(txnId, maxConflicts.get(keys));
        if (permitFastPath && txnId == min && txnId.epoch() >= node.epoch())
            return txnId;

        return node.uniqueTimestamp(min);
    }

    /**
     * We expect keys to be sliced to those owned by the replica in the coordination epoch
     */
    public final Timestamp maxConflict(Routables<?> keys)
    {
        return maxConflicts.get(keys);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{id=" + id + ", node=" + node.id().id + '}';
    }

    /**
     * Defer submitting the work until we have wired up any changes to topology in memory, then first submit the work
     * to setup any state in the command store, and finally submit the distributed work to bootstrap the data locally.
     * So, the outer future's success is sufficient for the topology to be acknowledged, and the inner future for the
     * bootstrap to be complete.
     */
    final Supplier<EpochReady> bootstrapper(Node node, Ranges newRanges, long epoch)
    {
        return () -> {
            AsyncResult<EpochReady> metadata = submit(empty(), safeStore -> {
                Bootstrap bootstrap = new Bootstrap(node, this, epoch, newRanges);
                bootstraps.add(bootstrap);
                bootstrap.start(safeStore);
                return new EpochReady(epoch, null, null, bootstrap.data, bootstrap.reads);
            });

            AsyncResult<Void> readyToCoordinate = readyToCoordinate(newRanges, epoch);
            return new EpochReady(epoch, metadata.<Void>map(ignore -> null).beginAsResult(),
                readyToCoordinate.beginAsResult(),
                metadata.flatMap(e -> e.data).beginAsResult(),
                metadata.flatMap(e -> e.reads).beginAsResult());
        };
    }

    /**
     * Rebootstraps some of the ranges for the command store. It follows steps similar to what
     * bootstrap would go through, with two differences:
     *
     *   * Marks pre-rebootstrap transactions with LOCALLY_LOST status, which means the node can not
     *     safely participate in pre-rebootstrap transactions, _even_ if they're coming after the node is
     *     done bootstrapping.
     *   * Marks the store as rebootstrapping, which will preclude rebootstrapping node from responding
     *     to PreAccept, Accept, and BeginRecovery and computing dependencies while node is being rebootstrapped,
     *     and ranges aren't ready to coordinate.
     */
    protected EpochReady rebootstrap(Node node, Ranges ranges, long epoch)
    {
        AsyncResult<EpochReady> metadata = submit(empty(), safeStore -> {
            safeStore.unsafeSetRebootstrapping(true);
            // Mark unsafe to read first
            safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));

            Bootstrap bootstrap = new Bootstrap(node, this, epoch, ranges, DataStore.RequestKind.Sync);
            bootstraps.add(bootstrap);
            // If rebootstrap can grab a later timestamp for subsequent attempts, but this timestamp is enough for us
            // to establish which transactions, for which ranges the node can safely participate in).
            TxnId unsafeBefore = bootstrap.start(safeStore);
            logger.debug("Rebootstrap timestamp on {}@{}: {}", id, node.id(), unsafeBefore);
            safeStore.unsafeUpsertRedundantBefore(RedundantBefore.create(ranges, unsafeBefore, LOCALLY_INCOMPLETE_ONLY));
            return new EpochReady(epoch, null, null,
                                  bootstrap.data,
                                  bootstrap.reads);
        });

        AsyncResult<Void> readyToCoordinate = readyToCoordinate(ranges, epoch);
        return new EpochReady(epoch,
                              metadata.<Void>map(ignore -> null).beginAsResult(),
                              readyToCoordinate.flatMap(ignore -> {
                                  return this.<Void>submit(empty(), safeStore -> {
                                      logger.debug("Finished rebootstrap timestamp on {}@{}, marking safe", id, node.id());
                                      safeStore.unsafeSetRebootstrapping(false);
                                      return null;
                                  });
                              }).beginAsResult(),
                              metadata.flatMap(e -> e.data).beginAsResult(),
                              metadata.flatMap(e -> e.reads).beginAsResult());
    }

    /**
     * Defer submitting the work until we have wired up any changes to topology in memory, then first submit the work
     * to setup any state in the command store, and finally submit the distributed work to bootstrap the data locally.
     * So, the outer future's success is sufficient for the topology to be acknowledged, and the inner future for the
     * bootstrap to be complete.
     */
    protected Supplier<EpochReady> sync(Node node, Ranges ranges, long epoch)
    {
        return () -> {
            AsyncResult<Void> readyToCoordinate = readyToCoordinate(ranges, epoch);
            return new EpochReady(epoch, DONE, readyToCoordinate, DONE, DONE);
        };
    }

    private AsyncResult<Void> readyToCoordinate(Ranges ranges, long epoch)
    {
        if (redundantBefore.min(ranges, Bounds::locallyWitnessedBefore).epoch() >= epoch)
            return DONE;

        TxnId minForEpoch = TxnId.minForEpoch(epoch);
        Ranges remaining = redundantBefore.removeWitnessed(minForEpoch, ranges);
        AsyncResults.SettableResult<Void> whenDone = new AsyncResults.SettableResult<>();
        waitingOnSync.put(epoch, new WaitingOnSync(whenDone, remaining));
        ensureReadyToCoordinate(epoch, ranges);
        return whenDone;
    }

    private void ensureReadyToCoordinate(long epoch, Ranges ranges)
    {
        TxnId minForEpoch = TxnId.minForEpoch(epoch);
        node.durability().close("[" + this + " Epoch " + epoch + ']', minForEpoch, ranges, 1, TimeUnit.HOURS)
            .begin((success, fail) -> {
                if (fail != null)
                {
                    Ranges remaining = redundantBefore.removeRetired(redundantBefore.removeWitnessed(minForEpoch, ranges));
                    if (!remaining.isEmpty())
                    {
                        logger.error("Failed to close epoch {} for ranges {} on store {}. Retrying.", epoch, remaining, id, fail);
                        node.someExecutor().execute(() -> ensureReadyToCoordinate(epoch, remaining));
                    }
                }
            });
    }

    Supplier<EpochReady> unbootstrap(long epoch, Ranges removedRanges)
    {
        return () -> {
            AsyncResult<Void> done = submit(empty(), safeStore -> {
                for (Bootstrap prev : bootstraps)
                {
                    Ranges abort = prev.allValid.slice(removedRanges, Minimal);
                    if (!abort.isEmpty())
                        prev.invalidate(abort);
                }
                return null;
            });

            return new EpochReady(epoch, done, done, done, done);
        };
    }

    final void complete(Bootstrap bootstrap)
    {
        bootstraps.remove(bootstrap);
    }

    final void markBootstrapping(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        safeStore.setBootstrapBeganAt(bootstrap(globalSyncId, ranges, bootstrapBeganAt));
        safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));
        updateMaxConflicts(ranges, globalSyncId);
        RedundantBefore addRedundantBefore = RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, globalSyncId, PRE_BOOTSTRAP_ONLY);
        safeStore.upsertRedundantBefore(addRedundantBefore);
        updatedRedundantBefore(safeStore, globalSyncId, ranges);
    }

    // TODO (expected): we can immediately truncate dependencies locally once an exclusiveSyncPoint applies, we don't need to wait for the whole shard
    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges durableRanges, Durability durability)
    {
        if (durability.compareTo(Durability.MajorityOrInvalidated) < 0)
            return;

        SomeStatus status = durability.compareTo(Durability.UniversalOrInvalidated) >= 0 ? SHARD_APPLIED_ONLY : MAJORITY_APPLIED_ONLY;
        final Ranges slicedRanges = durableRanges.slice(safeStore.ranges().allUntil(globalSyncId.epoch()), Minimal);
        TxnId locallyRedundantBefore = safeStore.redundantBefore().min(slicedRanges, Bounds::maxLocallyAppliedBefore);
        RedundantBefore addShardRedundant = RedundantBefore.create(slicedRanges, globalSyncId, status);
        safeStore.upsertRedundantBefore(addShardRedundant);
        updatedRedundantBefore(safeStore, globalSyncId, slicedRanges);

        if (status != SHARD_APPLIED_ONLY)
            return;

        if (locallyRedundantBefore.compareTo(globalSyncId) < 0)
        {
            // TODO (expected): if bootstrapping only part of the range, mark the rest for GC; or relax this as can safely GC behind bootstrap
            TxnId maxBootstrap = safeStore.redundantBefore().max(slicedRanges, Bounds::maxBootstrappedAt);
            if (maxBootstrap.compareTo(globalSyncId) >= 0)
                logger.info("Ignoring markShardDurable for a point we are bootstrapping. Bootstrapping: {}, Global: {}, Ranges: {}", maxBootstrap, globalSyncId, slicedRanges);
            else
                logger.warn("Trying to markShardDurable a point we have not yet caught-up to locally. Local: {}, Global: {}, Ranges: {}", locallyRedundantBefore, globalSyncId, slicedRanges);
            return;
        }

        // TODO (desired): not all systems care about HLC_BOUND for GC, make configurable
        if (globalSyncId.is(HLC_BOUND) || !requiresUniqueHlcs())
        {
            safeStore = safeStore; // make unusable in lambda
            safeStore.dataStore().snapshot(slicedRanges, globalSyncId).begin((success, fail) -> {
                if (fail != null)
                {
                    agent.onCaughtException(fail, "Unsuccessful dataStore snapshot; unable to update GC markers");
                    return;
                }

                execute(PreLoadContext.empty(), safeStore0 -> {
                    RedundantBefore addGc = RedundantBefore.create(slicedRanges, globalSyncId, GC_BEFORE_AND_LOCALLY_APPLIED);
                    safeStore0.upsertRedundantBefore(addGc);
                }, agent());
            });
        }
    }

    protected void updatedRedundantBefore(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        TxnId clearWaitingBefore = redundantBefore.minShardAndLocallyAppliedBefore();
        TxnId clearAllBefore = TxnId.min(clearWaitingBefore, durableBefore().min.majorityBefore);
        progressLog.clearBefore(safeStore, clearWaitingBefore, clearAllBefore);
        listeners.clearBefore(this, clearWaitingBefore);
    }

    protected void markSynced(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        RedundantBefore newRedundantBefore = RedundantBefore.merge(redundantBefore, RedundantBefore.create(ranges, syncId, LOCALLY_WITNESSED_ONLY));
        unsafeSetRedundantBefore(newRedundantBefore);
        updatedRedundantBefore(safeStore, syncId, ranges);

        if (waitingOnSync.isEmpty())
            return;

        LongHashSet remove = null;
        for (Map.Entry<Long, WaitingOnSync> e : waitingOnSync.entrySet())
        {
            if (e.getKey() > syncId.epoch())
                break;

            Ranges remaining = e.getValue().ranges;
            Ranges synced = remaining.slice(ranges, Minimal);
            boolean intersects = remaining.intersects(ranges);
            if (intersects)
            {
                e.getValue().ranges = remaining = remaining.without(ranges);
                if (e.getValue().ranges.isEmpty())
                {
                    logger.debug("Completed full sync for {} on epoch {} using {}", e.getValue().allRanges, e.getKey(), syncId);
                    e.getValue().whenDone.trySuccess(null);
                    if (remove == null)
                        remove = new LongHashSet();
                    remove.add(e.getKey());
                }
                else
                {
                    logger.debug("Completed partial sync for {} on epoch {} using {}; {} still to sync", synced, e.getKey(), syncId, remaining);
                }
            }
        }
        if (remove != null)
            remove.forEach(waitingOnSync::remove);
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
        agent.onStale(staleSince, ranges);

        RedundantBefore addRedundantBefore = RedundantBefore.createStale(ranges, staleUntilAtLeast);
        safeStore.upsertRedundantBefore(addRedundantBefore);
        // find which ranges need to bootstrap, subtracting those already in progress that cover the id

        markUnsafeToRead(ranges);
    }

    // MUST be invoked before CommandStore reference leaks to anyone
    // The integration may have already loaded persisted values for these fields before this is called
    // so it must be a merge for each field with the initialization values. These starting values don't need to be
    // persisted since we can synthesize them at startup every time
    // TODO (review): This needs careful thought about not persisting and that purgeAndInsert is doing the right thing
    // with safeToRead
    Supplier<EpochReady> initialise(long epoch, Ranges ranges)
    {
        return () -> {
            AsyncResult<Void> done = execute(empty(), (safeStore) -> {
                // Merge in a base for any ranges that needs to be covered
                Ranges newBootstrapRanges = ranges;
                for (Ranges existing : bootstrapBeganAt.values())
                    newBootstrapRanges = newBootstrapRanges.without(existing);
                if (!newBootstrapRanges.isEmpty())
                    safeStore.setBootstrapBeganAt(bootstrap(TxnId.NONE, newBootstrapRanges, bootstrapBeganAt));
                safeStore.setSafeToRead(purgeAndInsert(safeToRead, TxnId.NONE, ranges));
            });

            return new EpochReady(epoch, done, done, done, done);
        };
    }

    public final boolean isRejectedIfNotPreAccepted(TxnId txnId, Unseekables<?> participants)
    {
        if (rejectBefore == null)
            return false;

        return rejectBefore.rejects(txnId, participants);
    }

    public final MaxConflicts unsafeGetMaxConflicts()
    {
        return maxConflicts;
    }

    public final RedundantBefore unsafeGetRedundantBefore()
    {
        return redundantBefore;
    }

    @Nullable
    public final RejectBefore unsafeGetRejectBefore()
    {
        return rejectBefore;
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
            execute(empty(), safeStore -> {
                safeStore.setSafeToRead(purgeHistory(safeToRead, ranges));
            }, agent);
        }
    }

    public final DataStore unsafeGetDataStore()
    {
        return dataStore;
    }

    final synchronized void markSafeToRead(Timestamp forBootstrapAt, Timestamp at, Ranges ranges)
    {
        execute(empty(), safeStore -> {
            // TODO (required): handle weird edge cases like newer at having a lower HLC than prior existing at, but higher epoch
            Ranges validatedSafeToRead = redundantBefore.validateSafeToRead(forBootstrapAt, ranges);
            safeStore.setSafeToRead(purgeAndInsert(safeToRead, at, validatedSafeToRead));
            updateMaxConflicts(ranges, at);
        }, agent);
    }

    public static ImmutableSortedMap<TxnId, Ranges> bootstrap(TxnId at, Ranges ranges, NavigableMap<TxnId, Ranges> bootstrappedAt)
    {
        Invariants.requireArgument(bootstrappedAt.lastKey().compareTo(at) < 0 || at == TxnId.NONE);
        if (at == TxnId.NONE)
            for (Ranges rs : bootstrappedAt.values())
                Invariants.require(!ranges.intersects(rs));
        Invariants.requireArgument(!ranges.isEmpty());
        // if we're bootstrapping these ranges, then any period we previously owned the ranges for is effectively invalidated
        return purgeAndInsert(bootstrappedAt, at, ranges);
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

    private static ImmutableSortedMap<Timestamp, Ranges> purgeHistory(NavigableMap<Timestamp, Ranges> in, Ranges remove)
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

    public boolean isRebootstrapping()
    {
        return rebootstrapping;
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

    /**
     * Command store has lost information about this transaction and can not respond to queries related to it.
     */
    public static class TransactionLostException extends RuntimeException
    {
    }

    /**
     * Exception indicating that the node is not ready to compute dependencies due to rebootstrap
     */
    public static class NotReadyException extends RuntimeException
    {
    }
}
