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
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSortedMap;

import accord.api.Agent;
import accord.api.ConfigurationService.EpochReady;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.coordinate.CollectDeps;
import accord.local.Command.WaitingOn;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.Participants;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.RelationMultiMap.SortedRelationList;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.local.CommandStores.RangesForEpoch;

import static accord.api.ConfigurationService.EpochReady.DONE;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.PreLoadContext.empty;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore implements AgentExecutor
{
    static class EpochUpdate
    {
        final RangesForEpoch newRangesForEpoch;
        final RedundantBefore addRedundantBefore;
        final Ranges addGlobalRanges;

        EpochUpdate(RangesForEpoch newRangesForEpoch, RedundantBefore addRedundantBefore, Ranges addGlobalRanges)
        {
            this.newRangesForEpoch = newRangesForEpoch;
            this.addRedundantBefore = addRedundantBefore;
            this.addGlobalRanges = addGlobalRanges;
        }
    }

    public static class EpochUpdateHolder extends AtomicReference<EpochUpdate>
    {
        // TODO (required, eventually): support removing ranges
        public void updateGlobal(Ranges addGlobalRanges)
        {
            EpochUpdate baseUpdate = new EpochUpdate(null, RedundantBefore.EMPTY, addGlobalRanges);
            EpochUpdate cur = get();
            if (cur == null || !compareAndSet(cur, new EpochUpdate(cur.newRangesForEpoch, cur.addRedundantBefore, cur.addGlobalRanges.with(addGlobalRanges))))
                set(baseUpdate);
        }

        // TODO (desired): can better encapsulate by accepting only the newRangesForEpoch and deriving the add/remove ranges
        public void add(long epoch, RangesForEpoch newRangesForEpoch, Ranges addRanges)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.create(addRanges, epoch, Long.MAX_VALUE, TxnId.NONE, TxnId.minForEpoch(epoch));
            update(newRangesForEpoch, addRedundantBefore);
        }

        public void remove(long epoch, RangesForEpoch newRangesForEpoch, Ranges removeRanges)
        {
            RedundantBefore addRedundantBefore = RedundantBefore.create(removeRanges, Long.MIN_VALUE, epoch, TxnId.NONE, TxnId.NONE);
            update(newRangesForEpoch, addRedundantBefore);
        }

        private void update(RangesForEpoch newRangesForEpoch, RedundantBefore addRedundantBefore)
        {
            EpochUpdate baseUpdate = new EpochUpdate(newRangesForEpoch, addRedundantBefore, Ranges.EMPTY);
            EpochUpdate cur = get();
            if (cur == null || !compareAndSet(cur, new EpochUpdate(newRangesForEpoch, RedundantBefore.merge(cur.addRedundantBefore, addRedundantBefore), cur.addGlobalRanges)))
                set(baseUpdate);
        }
    }

    public interface Factory
    {
        CommandStore create(int id,
                            NodeTimeService time,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            EpochUpdateHolder rangesForEpoch);
    }

    private static final ThreadLocal<CommandStore> CURRENT_STORE = new ThreadLocal<>();

    protected final int id;
    protected final NodeTimeService time;
    protected final Agent agent;
    protected final DataStore store;
    protected final ProgressLog progressLog;
    protected final EpochUpdateHolder epochUpdateHolder;

    // TODO (expected): schedule regular pruning of these collections
    // bootstrapBeganAt and shardDurableAt are both canonical data sets mostly used for debugging / constructing
    private NavigableMap<TxnId, Ranges> bootstrapBeganAt = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY); // additive (i.e. once inserted, rolled-over until invalidated, and the floor entry contains additions)
    private RedundantBefore redundantBefore = RedundantBefore.EMPTY;
    // TODO (expected): store this only once per node
    private DurableBefore durableBefore = DurableBefore.EMPTY;
    protected RangesForEpoch rangesForEpoch;

    // TODO (desired): merge with redundantBefore?
    private NavigableMap<Timestamp, Ranges> safeToRead = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
    private final Set<Bootstrap> bootstraps = Collections.synchronizedSet(new DeterministicIdentitySet<>());
    // TODO (required): we must synchronise this on joining, prior to marking ourselves ready to coordinate
    @Nullable private ReducingRangeMap<Timestamp> rejectBefore;

    protected CommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, EpochUpdateHolder epochUpdateHolder)
    {
        this.id = id;
        this.time = time;
        this.agent = agent;
        this.store = store;
        this.progressLog = progressLogFactory.create(this);
        this.epochUpdateHolder = epochUpdateHolder;
    }

    public final int id()
    {
        return id;
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    public RangesForEpoch updateRangesForEpoch()
    {
        EpochUpdate update = epochUpdateHolder.get();
        if (update == null)
            return rangesForEpoch;

        update = epochUpdateHolder.getAndSet(null);
        if (!update.addGlobalRanges.isEmpty())
            setDurableBefore(DurableBefore.merge(durableBefore, DurableBefore.create(update.addGlobalRanges, TxnId.NONE, TxnId.NONE)));
        if (update.addRedundantBefore.size() > 0)
            setRedundantBefore(RedundantBefore.merge(redundantBefore, update.addRedundantBefore));
        if (update.newRangesForEpoch != null)
            rangesForEpoch = update.newRangesForEpoch;
        return rangesForEpoch;
    }

    public RangesForEpoch unsafeRangesForEpoch()
    {
        return rangesForEpoch;
    }

    public abstract boolean inStore();

    public abstract AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);

    public abstract <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply);
    public abstract void shutdown();

    private static Timestamp maxApplied(SafeCommandStore safeStore, Seekables<?, ?> keysOrRanges, Ranges slice)
    {
        return safeStore.mapReduce(keysOrRanges, slice, SafeCommandStore.TestKind.Ws,
                                   SafeCommandStore.TestTimestamp.STARTED_AFTER, Timestamp.NONE,
                                   SafeCommandStore.TestDep.ANY_DEPS, null,
                                   Status.Applied, Status.Applied,
                                   (key, txnId, executeAt, status, max) -> Timestamp.max(max, executeAt),
                                   Timestamp.NONE, Timestamp.MAX);
    }

    public AsyncChain<Timestamp> maxAppliedFor(Seekables<?, ?> keysOrRanges, Ranges slice)
    {
        return submit(PreLoadContext.contextFor(keysOrRanges), safeStore -> maxApplied(safeStore, keysOrRanges, slice));
    }

    // implementations are expected to override this for persistence
    protected void setRejectBefore(ReducingRangeMap<Timestamp> newRejectBefore)
    {
        this.rejectBefore = newRejectBefore;
    }

    /**
     * To be overridden by implementations, to ensure the new state is persisted
     *
     * TODO (required): consider handling asynchronicity of persistence
     *  (could leave to impls to call this parent method once persisted)
     * TODO (desired): compact Ranges, merging overlaps
     */
    protected void setBootstrapBeganAt(NavigableMap<TxnId, Ranges> newBootstrapBeganAt)
    {
        this.bootstrapBeganAt = newBootstrapBeganAt;
    }

    public DurableBefore durableBefore()
    {
        return durableBefore;
    }

    /**
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    public void setDurableBefore(DurableBefore durableBefore)
    {
        this.durableBefore = durableBefore;
    }


    /**
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    protected void setRedundantBefore(RedundantBefore newRedundantBefore)
    {
        this.redundantBefore = newRedundantBefore;
    }

    /**
     * This method may be invoked on a non-CommandStore thread
     */
    protected synchronized void setSafeToRead(NavigableMap<Timestamp, Ranges> newSafeToRead)
    {
        this.safeToRead = newSafeToRead;
    }

    public NavigableMap<TxnId, Ranges> bootstrapBeganAt()
    {
        return bootstrapBeganAt;
    }

    public NavigableMap<Timestamp, Ranges> safeToRead()
    {
        return safeToRead;
    }

    public final void markExclusiveSyncPoint(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        // TODO (desired): narrow ranges to those that are owned
        Invariants.checkArgument(txnId.rw() == ExclusiveSyncPoint);
        ReducingRangeMap<Timestamp> newRejectBefore = rejectBefore != null ? rejectBefore : new ReducingRangeMap<>();
        newRejectBefore = ReducingRangeMap.add(newRejectBefore, ranges, txnId, Timestamp::max);
        setRejectBefore(newRejectBefore);
    }

    final Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys, SafeCommandStore safeStore, boolean permitFastPath)
    {
        NodeTimeService time = safeStore.time();
        boolean isExpired = agent().isExpired(txnId, safeStore.time().now());
        if (rejectBefore != null && !isExpired)
            isExpired = null == rejectBefore.foldl(keys, (rejectIfBefore, test) -> rejectIfBefore.compareTo(test) > 0 ? null : test, txnId, Objects::isNull);

        if (isExpired)
            return time.uniqueNow(txnId).asRejected();

        if (txnId.rw() == ExclusiveSyncPoint)
        {
            markExclusiveSyncPoint(safeStore, txnId, (Ranges)keys);
            return txnId;
        }

        Timestamp maxConflict = safeStore.maxConflict(keys, safeStore.ranges().coordinates(txnId));
        if (permitFastPath && txnId.compareTo(maxConflict) > 0 && txnId.epoch() >= time.epoch())
            return txnId;

        return time.uniqueNow(maxConflict);
    }

    protected void unsafeRunIn(Runnable fn)
    {
        CommandStore prev = maybeCurrent();
        CURRENT_STORE.set(this);
        try
        {
            fn.run();
        }
        finally
        {
            if (prev == null) CURRENT_STORE.remove();
            else CURRENT_STORE.set(prev);
        }
    }

    protected <T> T unsafeRunIn(Callable<T> fn) throws Exception
    {
        CommandStore prev = maybeCurrent();
        CURRENT_STORE.set(this);
        try
        {
            return fn.call();
        }
        finally
        {
            if (prev == null) CURRENT_STORE.remove();
            else CURRENT_STORE.set(prev);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "id=" + id +
               '}';
    }

    @Nullable
    public static CommandStore maybeCurrent()
    {
        return CURRENT_STORE.get();
    }

    public static CommandStore current()
    {
        CommandStore cs = maybeCurrent();
        if (cs == null)
            throw new IllegalStateException("Attempted to access current CommandStore, but not running in a CommandStore");
        return cs;
    }

    protected static void register(CommandStore store)
    {
        if (!store.inStore())
            throw new IllegalStateException("Unable to register a CommandStore when not running in it; store " + store);
        CURRENT_STORE.set(store);
    }

    public static void checkInStore()
    {
        CommandStore store = maybeCurrent();
        if (store == null) throw new IllegalStateException("Expected to be running in a CommandStore but is not");
    }

    public static void checkNotInStore()
    {
        CommandStore store = maybeCurrent();
        if (store != null)
            throw new IllegalStateException("Expected to not be running in a CommandStore, but running in " + store);
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
                return new EpochReady(epoch, null, bootstrap.coordination, bootstrap.data, bootstrap.reads);
            }).beginAsResult();

            return new EpochReady(epoch, metadata.<Void>map(ignore -> null).beginAsResult(),
                metadata.flatMap(e -> e.coordination).beginAsResult(),
                metadata.flatMap(e -> e.data).beginAsResult(),
                metadata.flatMap(e -> e.reads).beginAsResult());
        };
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
            AsyncResults.SettableResult<Void> whenDone = new AsyncResults.SettableResult<>();
            fetchMajorityDeps(whenDone, node, epoch, ranges);
            return new EpochReady(epoch, DONE, whenDone, whenDone, whenDone);
        };
    }

    private void fetchMajorityDeps(AsyncResults.SettableResult<Void> coordination, Node node, long epoch, Ranges ranges)
    {
        TxnId id = TxnId.fromValues(epoch - 1, 0, node.id());
        Timestamp before = Timestamp.minForEpoch(epoch);
        FullRoute<?> route = node.computeRoute(id, ranges);
        CollectDeps.withDeps(node, id, route, ranges, before, (deps, fail) -> {
            if (fail != null)
            {
                fetchMajorityDeps(coordination, node, epoch, ranges);
            }
            else
            {
                // TODO (correcness) : PreLoadContext only works with Seekables, which doesn't allow mixing Keys and Ranges... But Deps has both Keys AND Ranges!
                // ATM all known implementations store ranges in-memory, but this will not be true soon, so this will need to be addressed
                execute(contextFor(null, deps.txnIds(), deps.keyDeps.keys()), safeStore -> {
                    safeStore.registerHistoricalTransactions(deps);
                }).begin((success, fail2) -> {
                    if (fail2 != null) fetchMajorityDeps(coordination, node, epoch, ranges);
                    else coordination.setSuccess(null);
                });
            }
        });
    }

    Supplier<EpochReady> unbootstrap(long epoch, Ranges removedRanges)
    {
        return () -> {
            AsyncResult<Void> done = this.<Void>submit(empty(), safeStore -> {
                for (Bootstrap prev : bootstraps)
                {
                    Ranges abort = prev.allValid.subtract(removedRanges);
                    if (!abort.isEmpty())
                        prev.invalidate(abort);
                }
                return null;
            }).beginAsResult();

            return new EpochReady(epoch, done, done, done, done);
        };
    }

    final void complete(Bootstrap bootstrap)
    {
        bootstraps.remove(bootstrap);
    }

    final void markBootstrapping(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        setBootstrapBeganAt(bootstrap(globalSyncId, ranges, bootstrapBeganAt));
        RedundantBefore addRedundantBefore = RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, TxnId.NONE, globalSyncId);
        setRedundantBefore(RedundantBefore.merge(redundantBefore, addRedundantBefore));
        DurableBefore addDurableBefore = DurableBefore.create(ranges, TxnId.NONE, TxnId.NONE);
        setDurableBefore(DurableBefore.merge(durableBefore, addDurableBefore));
    }

    // TODO (expected): we can immediately truncate dependencies locally once an exclusiveSyncPoint applies, we don't need to wait for the whole shard
    public void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        // TODO (now): IMPORTANT we should insert additional entries for each epoch our ownership differs
        ranges = ranges.slice(safeStore.ranges().allUntil(globalSyncId.epoch()), Minimal);
        RedundantBefore addRedundantBefore = RedundantBefore.create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, globalSyncId, TxnId.NONE);
        setRedundantBefore(RedundantBefore.merge(redundantBefore, addRedundantBefore));
        DurableBefore addDurableBefore = DurableBefore.create(ranges, globalSyncId, globalSyncId);
        setDurableBefore(DurableBefore.merge(durableBefore, addDurableBefore));
    }

    // MUST be invoked before CommandStore reference leaks to anyone
    Supplier<EpochReady> initialise(long epoch, Ranges ranges)
    {
        DurableBefore addDurableBefore = DurableBefore.create(ranges, TxnId.NONE, TxnId.NONE);
        setDurableBefore(DurableBefore.merge(durableBefore, addDurableBefore));
        setBootstrapBeganAt(ImmutableSortedMap.of(TxnId.NONE, ranges));
        setSafeToRead(ImmutableSortedMap.of(Timestamp.NONE, ranges));
        return () -> new EpochReady(epoch, DONE, DONE, DONE, DONE);
    }

    final Ranges safeToReadAt(Timestamp at)
    {
        return safeToRead.floorEntry(at).getValue();
    }

    // TODO (desired): Commands.durability() can use this to upgrade to Majority without further info
    public final Status.Durability globalDurability(TxnId txnId)
    {
        return durableBefore.min(txnId);
    }

    public final RedundantBefore redundantBefore()
    {
        return redundantBefore;
    }

    public final boolean isRejectedIfNotPreAccepted(TxnId txnId, Unseekables<?> participants)
    {
        if (rejectBefore == null)
            return false;

        return null != rejectBefore.foldl(participants, (rejectIfBefore, test) -> rejectIfBefore.compareTo(test) > 0 ? null : test, txnId, Objects::isNull);
    }

    public final void removeRedundantDependencies(Unseekables<?> participants, WaitingOn.Update builder)
    {
        // Note: we do not need to track the bootstraps we implicitly depend upon, because we will not serve any read requests until this has completed
        //  and since we are a timestamp store, and we write only this will sort itself out naturally
        // TODO (required): make sure we have no races on HLC around SyncPoint else this resolution may not work (we need to know the micros equivalent timestamp of the snapshot)
        KeyDeps keyDeps = builder.deps.keyDeps;
        redundantBefore().foldl(keyDeps.keys(), (e, b, d, p2, ki, kj) -> {
            int txnIdx = d.txnIds().find(e.locallyRedundantBefore());
            if (txnIdx < 0) txnIdx = -1 - txnIdx;
            while (ki < kj)
            {
                SortedRelationList<TxnId> txnIdsForKey = d.txnIdsForKeyIndex(ki++);
                int ti = txnIdsForKey.findNext(0, txnIdx);
                if (ti < 0)
                    ti = -1 - ti;
                for (int i = 0 ; i < ti ; ++i)
                    b.setAppliedOrInvalidated(txnIdsForKey.getValueIndex(i));
            }
            return b;
        }, builder, keyDeps, null, ignore -> false);
        RangeDeps rangeDeps = builder.deps.rangeDeps;
        // TODO (now): slice to only those ranges we own, maybe don't even construct rangeDeps.covering()
        redundantBefore().foldl(participants, (e, b, d, rs, pi, pj) -> {
            // TODO (now): foldlInt so we can track the lower rangeidx bound and not revisit unnecessarily
            boolean useBootstrap = e.isLocalRedundancyViaBootstrap();
            int txnIdx;
            {
                int tmp = d.txnIds().find(useBootstrap ? e.bootstrappedAt : e.redundantBefore);
                txnIdx = tmp < 0 ? -1 - tmp : tmp;
            }
            rangeDeps.forEach(participants, e.range, pi, pj, d, (d0, txnIdx0) -> {
                if (txnIdx0 < txnIdx)
                    b.setAppliedOrInvalidatedRangeIdx(txnIdx0);
            });
            return b;
        }, builder, rangeDeps, participants, ignore -> false);
    }

    public final boolean hasLocallyRedundantDependencies(TxnId minimumDependencyId, Timestamp executeAt, Participants<?> participantsOfWaitingTxn)
    {
        // TODO (required): consider race conditions when bootstrapping into an active command store, that may have seen a higher txnId than this?
        //   might benefit from maintaining a per-CommandStore largest TxnId register to ensure we allocate a higher TxnId for our ExclSync,
        //   or from using whatever summary records we have for the range, once we maintain them
        return redundantBefore.status(minimumDependencyId, executeAt, participantsOfWaitingTxn).compareTo(RedundantStatus.PARTIALLY_PRE_BOOTSTRAP) >= 0;
    }

    final synchronized void markUnsafeToRead(Ranges ranges)
    {
        if (safeToRead.values().stream().anyMatch(r -> r.intersects(ranges)))
            setSafeToRead(purgeHistory(safeToRead, ranges));
    }

    final synchronized void markSafeToRead(Timestamp at, Ranges ranges)
    {
        setSafeToRead(purgeAndInsert(safeToRead, at, ranges));
    }

    private static <T extends Timestamp> ImmutableSortedMap<T, Ranges> bootstrap(T at, Ranges ranges, NavigableMap<T, Ranges> bootstrappedAt)
    {
        Invariants.checkArgument(bootstrappedAt.lastKey().compareTo(at) < 0);
        Invariants.checkArgument(!ranges.isEmpty());
        // if we're bootstrapping these ranges, then any period we previously owned the ranges for is effectively invalidated
        return purgeAndInsert(bootstrappedAt, at, ranges);
    }

    private static <T extends Timestamp> ImmutableSortedMap<T, Ranges> purgeAndInsert(NavigableMap<T, Ranges> in, T insertAt, Ranges insert)
    {
        TreeMap<T, Ranges> build = new TreeMap<>(in);
        build.headMap(insertAt, false).entrySet().forEach(e -> e.setValue(e.getValue().subtract(insert)));
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
        Ranges without = in.getValue().subtract(remove);
        if (without == in.getValue())
            return in;
        return new SimpleImmutableEntry<>(in.getKey(), without);
    }
}
