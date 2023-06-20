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

import accord.api.ProgressLog;
import accord.api.DataStore;
import accord.coordinate.CollectDeps;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.primitives.Timestamp;

import javax.annotation.Nullable;
import accord.api.Agent;

import accord.utils.async.AsyncChain;

import accord.api.ConfigurationService.EpochReady;
import accord.primitives.*;
import accord.utils.DeterministicIdentitySet;
import accord.utils.IndexedRangeTriConsumer;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResult;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;

import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import accord.primitives.Seekables;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResults;

import com.google.common.collect.ImmutableSortedMap;

import static accord.api.ConfigurationService.EpochReady.DONE;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.PreLoadContext.empty;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore implements AgentExecutor
{
    public interface Factory
    {
        CommandStore create(int id,
                            NodeTimeService time,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpochHolder rangesForEpoch);
    }

    private static final ThreadLocal<CommandStore> CURRENT_STORE = new ThreadLocal<>();

    protected final int id;
    protected final NodeTimeService time;
    protected final Agent agent;
    protected final DataStore store;
    protected final ProgressLog progressLog;
    protected final RangesForEpochHolder rangesForEpochHolder;

    // TODO (expected): schedule regular pruning of these collections
    // TODO (required): by itself this doesn't handle re-bootstrapping of a range previously owned by this command store
    private NavigableMap<TxnId, Ranges> bootstrapBeganAt = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
    private NavigableMap<Timestamp, Ranges> safeToRead = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
    private final Set<Bootstrap> bootstraps = Collections.synchronizedSet(new DeterministicIdentitySet<>());
    private long maxBootstrapEpoch;
    @Nullable private ReducingRangeMap<Timestamp> rejectBefore;

    protected CommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder)
    {
        this.id = id;
        this.time = time;
        this.agent = agent;
        this.store = store;
        this.progressLog = progressLogFactory.create(this);
        this.rangesForEpochHolder = rangesForEpochHolder;
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

    public RangesForEpochHolder rangesForEpochHolder()
    {
        return rangesForEpochHolder;
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
                                   (key, txnId, executeAt, max) -> Timestamp.max(max, executeAt),
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

    public long maxBootstrapEpoch()
    {
        return maxBootstrapEpoch;
    }

    public void markExclusiveSyncPoint(TxnId txnId, Ranges ranges, SafeCommandStore safeStore)
    {
        Invariants.checkArgument(txnId.rw() == ExclusiveSyncPoint);
        ReducingRangeMap<Timestamp> newRejectBefore = rejectBefore != null ? rejectBefore : new ReducingRangeMap<>(Timestamp.NONE);
        newRejectBefore = ReducingRangeMap.add(newRejectBefore, ranges, txnId, Timestamp::max);
        setRejectBefore(newRejectBefore);
    }

    Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys, SafeCommandStore safeStore)
    {
        NodeTimeService time = safeStore.time();
        boolean isExpired = agent().isExpired(txnId, safeStore.time().now());
        if (rejectBefore != null && !isExpired && txnId.rw().isWrite())
            isExpired = null == rejectBefore.foldl(keys, (rejectIfBefore, test) -> rejectIfBefore.compareTo(test) > 0 ? null : test, txnId, Objects::isNull);

        if (isExpired)
            return time.uniqueNow(txnId).asRejected();

        if (txnId.rw() == ExclusiveSyncPoint)
        {
            markExclusiveSyncPoint(txnId, (Ranges)keys, safeStore);
            return txnId;
        }

        Timestamp maxConflict = safeStore.maxConflict(keys, safeStore.ranges().coordinates(txnId));
        if (txnId.compareTo(maxConflict) > 0 && txnId.epoch() >= time.epoch())
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
    Supplier<EpochReady> bootstrapper(Node node, Ranges newRanges, long epoch)
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

    Supplier<EpochReady> interruptBootstraps(long epoch, Ranges allRanges)
    {
        return () -> {
            AsyncResult<Void> done = this.<Void>submit(empty(), safeStore -> {
                // TODO (required): to enable this we just need to ensure we have a chain of bootstrapped nodes
                //    linking each epoch, else new owners may not finish before their ownership changes, so that if they
                //    cancel their bootstrap and there's no chain bridging old to new the system gets stalled
                for (Bootstrap prev : bootstraps)
                {
                    Ranges abort = prev.allValid.difference(allRanges);
                    if (!abort.isEmpty())
                        prev.invalidate(abort);
                }
                return null;
            }).beginAsResult();

            return new EpochReady(epoch, done, done, done, done);
        };
    }

    void complete(Bootstrap bootstrap)
    {
        bootstraps.remove(bootstrap);
    }

    void markBootstrapping(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        setBootstrapBeganAt(bootstrap(globalSyncId, ranges, bootstrapBeganAt));
        maxBootstrapEpoch = Math.max(maxBootstrapEpoch, globalSyncId.epoch());
    }

    // TODO (required): load from disk and restore bootstraps
    EpochReady initialise(long epoch, Ranges ranges)
    {
        AsyncResult<Void> done = execute(empty(), safeStore -> {
            setBootstrapBeganAt(ImmutableSortedMap.of(TxnId.NONE, ranges));
            setSafeToRead(ImmutableSortedMap.of(Timestamp.NONE, ranges));
        }).beginAsResult();
        return new EpochReady(epoch, done, done, done, done);
    }

    final Ranges safeToReadAt(Timestamp at)
    {
        return safeToRead.floorEntry(at).getValue();
    }

    /**
     * True iff one or more of these dependencies may need to be expunged due to occurring prior to a bootstrap event,
     * indicating that they may not be executed locally.
     */
    public final boolean isAffectedByBootstrap(Deps deps)
    {
        return !deps.isEmpty() && isAffectedByBootstrap(deps.minTxnId());
    }

    /**
     * True iff this transaction may itself not execute locally
     */
    public final boolean isAffectedByBootstrap(TxnId txnId)
    {
        // TODO (required): consider race conditions when bootstrapping into an active command store, that may have seen a higher txnId than this?
        //   might benefit from maintaining a per-CommandStore largest TxnId register to ensure we allocate a higher TxnId for our ExclSync,
        //   or from using whatever summary records we have for the range, once we maintain them
        return txnId.epoch() <= maxBootstrapEpoch && bootstrapBeganAt.higherEntry(txnId) != null;
    }

    /**
     * True iff this transaction's execution overlaps a range that is unsafe to read.
     * This permits us to ignore its dependencies for these keys, as we cannot locally execute an overlapping read,
     * so a write may execute at any time.
     */
    public final boolean isPartiallyInvisibleToReads(Timestamp executeAt, Seekables<?, ?> readScope)
    {
        Map.Entry<Timestamp, Ranges> entry = safeToRead.higherEntry(executeAt);
        if (entry == null)
            return false;

        // TODO (desired): this is potentially pessimistic, depending on how we eventually manage loss of ownership of a range
        //   i.e., if we remove from safeToRead when we lose ownership (or, if we remove due to a network partition causing inconsistency,
        //   and then do not regain it)
        return !readScope.containsAll(entry.getValue());
    }

    final <P1> void forEachBootstrapRange(SortedArrayList<TxnId> txnIds, IndexedRangeTriConsumer<P1, TxnId, Ranges> forEach, P1 p1)
    {
        if (txnIds.isEmpty())
            return;

        int i = 0;
        while (i < txnIds.size())
        {
            Map.Entry<TxnId, Ranges> from = bootstrapBeganAt.floorEntry(txnIds.get(i));
            Map.Entry<TxnId, Ranges> to = bootstrapBeganAt.higherEntry(txnIds.get(i));
            int nexti = to == null ? txnIds.size() : txnIds.findNext(i, to.getKey());
            if (nexti < 0) nexti = -1 - nexti;
            if (from != null && !from.getValue().isEmpty())
            {
                TxnId bootstrapId = i == 0 && from.getKey() == TxnId.NONE ? null : from.getKey();
                forEach.accept(p1, bootstrapId, from.getValue(), i, nexti);
            }
            i = nexti;
        }
    }

    synchronized void markUnsafeToRead(Ranges ranges)
    {
        if (safeToRead.values().stream().anyMatch(r -> r.intersects(ranges)))
            setSafeToRead(purgeHistory(safeToRead, ranges));
    }

    synchronized void markSafeToRead(Timestamp at, Ranges ranges)
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
        build.headMap(insertAt, false).entrySet().forEach(e -> e.setValue(e.getValue().difference(insert)));
        build.tailMap(insertAt, true).entrySet().forEach(e -> e.setValue(e.getValue().with(insert)));
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
        Ranges without = in.getValue().difference(remove);
        if (without == in.getValue())
            return in;
        return new SimpleImmutableEntry<>(in.getKey(), without);
    }
}
