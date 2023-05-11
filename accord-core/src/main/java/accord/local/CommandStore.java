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
import accord.api.RoutingKey;
import accord.coordinate.CollectDeps;
import accord.local.Command.Truncated;
import accord.local.Command.WaitingOn;
import accord.local.CommandStores.RangesForEpochHolder;

import javax.annotation.Nullable;
import accord.api.Agent;

import accord.utils.Functions;
import accord.utils.IndexedRangeTriConsumer;
import accord.utils.async.AsyncChain;

import accord.api.ConfigurationService.EpochReady;
import accord.primitives.*;
import accord.utils.DeterministicIdentitySet;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncResult;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;

import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import accord.utils.async.AsyncResults;

import com.google.common.collect.ImmutableSortedMap;

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
    // bootstrapBeganAt and shardDurableAt are both canonical data sets mostly used for debugging / constructing
    private NavigableMap<TxnId, Ranges> bootstrapBeganAt = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY); // additive (i.e. once inserted, rolled-over until invalidated, and the floor entry contains additions)
    private NavigableMap<TxnId, Ranges> shardDurableBefore = ImmutableSortedMap.of(); // subtractive (i.e. inserts are propagated backwards, not forwards, and the ceiling entry contains the relevant truncation)
    // redundantBefore is union of bootstrapBeganAt and shardDurableBefore
    private NavigableMap<TxnId, Ranges> redundantBefore = ImmutableSortedMap.of(); // subtractive (i.e. inserts are propagated backwards, not forwards, and the ceiling entry contains the relevant truncation)
    // truncatedBefore is intersection of shardDurableBefore and globallyDurableBefore
    private NavigableMap<TxnId, Ranges> truncatedBefore = ImmutableSortedMap.of(); // subtractive (i.e. inserts are propagated backwards, not forwards, and the ceiling entry contains the relevant truncation)
    private TxnId globallyDurableBefore = TxnId.NONE;
    private NavigableMap<Timestamp, Ranges> safeToRead = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
    private final Set<Bootstrap> bootstraps = Collections.synchronizedSet(new DeterministicIdentitySet<>());
    @Nullable private ReducingRangeMap<Timestamp> rejectBefore;

    static class DurableOrBootstrapped
    {
        final Ranges bootstrapped;
        final Ranges truncated;

        DurableOrBootstrapped(Ranges bootstrapped, Ranges truncated)
        {
            this.bootstrapped = bootstrapped;
            this.truncated = truncated;
        }

        DurableOrBootstrapped truncate(Ranges addTruncated)
        {
            return new DurableOrBootstrapped(bootstrapped.subtract(addTruncated), truncated.with(addTruncated));
        }

        DurableOrBootstrapped bootstrap(Ranges addBootstrapped)
        {
            // we shouldn't be bootstrapping a range we've already truncated
            return new DurableOrBootstrapped(bootstrapped.with(addBootstrapped.subtract(truncated)), truncated);
        }

        DurableOrBootstrapped unbootstrap(Ranges removeBootstrapped)
        {
            return new DurableOrBootstrapped(bootstrapped.subtract(removeBootstrapped), truncated);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof DurableOrBootstrapped))
                return false;
            DurableOrBootstrapped that = (DurableOrBootstrapped) obj;
            return bootstrapped.equals(that.bootstrapped) && truncated.equals(that.truncated);
        }

        @Override
        public String toString()
        {
            return "+" + bootstrapped + ", -" + truncated;
        }
    }

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
    protected abstract Truncated truncate(SafeCommand safeCommand, Ranges truncated);

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

    public TxnId durableBefore(SafeCommandStore safeStore, long epoch)
    {
        // TODO (now): introduce a faster shardDurableBefore only for invalidations
        Ranges ranges = rangesForEpochHolder.get().allAt(epoch);
        if (ranges.isEmpty())
            return null;

        TxnId durableBefore = TxnId.NONE;
        for (Map.Entry<TxnId, Ranges> e : shardDurableBefore.headMap(TxnId.maxForEpoch(epoch), true).entrySet())
        {
            if (!e.getValue().containsAll(ranges))
                break;

            durableBefore = e.getKey();
        }
        return durableBefore;
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
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    protected void setTruncatedBefore(NavigableMap<TxnId, Ranges> newTruncatedAt)
    {
        this.truncatedBefore = newTruncatedAt;
    }

    /**
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    protected void setShardDurableBefore(NavigableMap<TxnId, Ranges> newShardDurableBefore)
    {
        this.shardDurableBefore = newShardDurableBefore;
    }

    /**
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    protected void setRedundantBefore(NavigableMap<TxnId, Ranges> newRedundantBefore)
    {
        this.redundantBefore = newRedundantBefore;
    }

    /**
     * To be overridden by implementations, to ensure the new state is persisted.
     */
    protected NavigableMap<TxnId, Ranges> getRedundantBefore()
    {
        return redundantBefore;
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

    final Truncated truncate(SafeCommand safeCommand)
    {
        Map.Entry<TxnId, Ranges> entry = truncatedBefore.higherEntry(safeCommand.txnId());
        // we can trigger a truncate via external knowledge (e.g. peers having truncated)
        return truncate(safeCommand, entry == null ? Ranges.EMPTY : entry.getValue());
    }

    public final void markExclusiveSyncPoint(SafeCommandStore safeStore, TxnId txnId, Ranges ranges)
    {
        // TODO (desired): narrow ranges to those that are owned
        Invariants.checkArgument(txnId.rw() == ExclusiveSyncPoint);
        ReducingRangeMap<Timestamp> newRejectBefore = rejectBefore != null ? rejectBefore : new ReducingRangeMap<>(Timestamp.NONE);
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

    Supplier<EpochReady> interruptBootstraps(long epoch, Ranges allRanges)
    {
        return () -> {
            AsyncResult<Void> done = this.<Void>submit(empty(), safeStore -> {
                for (Bootstrap prev : bootstraps)
                {
                    Ranges abort = prev.allValid.subtract(allRanges);
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
        setRedundantBefore(truncate(globalSyncId, ranges, redundantBefore));
    }

    // TODO (expected): we can immediately truncate dependencies locally once an exclusiveSyncPoint applies, we don't need to wait for the whole shard
    public final void markShardDurable(SafeCommandStore safeStore, TxnId globalSyncId, Ranges ranges)
    {
        // TODO (now): IMPORTANT we should insert additional entries for each epoch our ownership differs
        ranges = ranges.slice(rangesForEpochHolder.get().allUntil(globalSyncId.epoch()), Minimal);
        setShardDurableBefore(truncate(globalSyncId, ranges, shardDurableBefore));
        setRedundantBefore(truncate(globalSyncId, ranges, redundantBefore));

        TxnId truncateAt = Functions.reduceNonNull(TxnId::min, globalSyncId, globallyDurableBefore);
        setTruncatedBefore(truncate(truncateAt, ranges, truncatedBefore));
    }

    public void setGloballyDurable(SafeCommandStore safeStore, TxnId before)
    {
        if (globallyDurableBefore.compareTo(before) < 0)
        {
            TxnId prev = globallyDurableBefore;
            globallyDurableBefore = before;

            NavigableMap<TxnId, Ranges> truncatedAt = this.truncatedBefore;
            for (Map.Entry<TxnId, Ranges> e : shardDurableBefore.subMap(prev, true, globallyDurableBefore, false).entrySet())
                truncatedAt = truncate(e.getKey(), e.getValue(), truncatedAt);

            if (!truncatedAt.equals(this.truncatedBefore))
                setTruncatedBefore(truncatedAt);
        }
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

    // TODO (desired): Commands.durability() can use this to upgrade to Majority without further info
    public final boolean isGloballyDurable(TxnId txnId)
    {
        return globallyDurableBefore != null && globallyDurableBefore.compareTo(txnId) > 0;
    }

    public final boolean isTruncated(Command command)
    {
        Map.Entry<TxnId, Ranges> entry = truncatedBefore.higherEntry(command.txnId());
        if (entry == null)
            return false;

        return isTruncated(command, entry.getValue());
    }

    protected final boolean isTruncated(Command command, Ranges truncated)
    {
        TxnId txnId = command.txnId();
        Ranges coordinates = rangesForEpochHolder.get().allAt(txnId.epoch());
        Ranges additionalRanges = command.executeAt() != null && !command.executeAt().equals(Timestamp.NONE) && command.executeAt().epoch() > 0
                                  ? rangesForEpochHolder.get().allBetween(txnId.epoch(), command.executeAt().epoch()) : Ranges.EMPTY;

        Route<?> route = command.route();
        Unseekables<?, ?> participants;
        if (route == null) participants = coordinates.with(additionalRanges);
        else participants = route.participants();

        return isTruncated(participants, coordinates, additionalRanges, truncated);
    }

    public final boolean isShardDurableAt(TxnId txnId, long executeAt, Unseekables<?, ?> participants)
    {
        Map.Entry<TxnId, Ranges> entry = shardDurableBefore.higherEntry(txnId);
        if (entry == null)
            return false;

        Ranges durable = entry.getValue();
        Ranges coordinates = rangesForEpochHolder.get().allAt(txnId.epoch());
        if (participants.intersects(durable.slice(coordinates, Minimal)))
            return true;

        if (txnId.epoch() == executeAt)
            return false;

        Ranges additionalRanges = rangesForEpochHolder.get().allAt(executeAt);
        return participants.intersects(durable.slice(additionalRanges, Minimal));
    }

    public final boolean isTruncatedAt(TxnId txnId, long executeAt, Unseekables<?, ?> participants)
    {
        Map.Entry<TxnId, Ranges> entry = truncatedBefore.higherEntry(txnId);
        if (entry == null)
            return false;

        if (Route.isRoute(participants))
            participants = Route.castToRoute(participants).participants();

        Ranges coordinates = rangesForEpochHolder.get().allAt(txnId.epoch());
        Ranges additionalRanges = null;
        if (txnId.epoch() != executeAt)
            additionalRanges = rangesForEpochHolder.get().allAt(executeAt);
        return isTruncated(participants, coordinates, additionalRanges, entry.getValue());
    }

    public final boolean isTruncated(TxnId txnId, RoutingKey someKey)
    {
        Map.Entry<TxnId, Ranges> entry = truncatedBefore.higherEntry(txnId);
        if (entry == null)
            return false;

        return entry.getValue().contains(someKey);
    }

    public final boolean isFullyTruncatedAt(TxnId txnId, long executeAt)
    {
        Map.Entry<TxnId, Ranges> entry = truncatedBefore.higherEntry(txnId);
        if (entry == null)
            return false;

        Ranges coordinates = rangesForEpochHolder.get().allAt(txnId.epoch());
        Ranges additionalRanges = null;
        if (txnId.epoch() != executeAt)
            additionalRanges = rangesForEpochHolder.get().allAt(executeAt);
        Ranges truncated = entry.getValue();
        return truncated.containsAll(coordinates) && (additionalRanges == null || truncated.containsAll(additionalRanges));
    }

    public final boolean isRejectedIfNotPreAccepted(TxnId txnId, Unseekables<?, ?> participants)
    {
        if (rejectBefore == null)
            return false;

        if (txnId.compareTo(globallyDurableBefore) >= 0)
            return false;

        return null != rejectBefore.foldl(participants, (rejectIfBefore, test) -> rejectIfBefore.compareTo(test) > 0 ? null : test, txnId, Objects::isNull);
    }

    public final boolean isTruncated(TxnId txnId, Timestamp executeAt, Route<?> route)
    {
        return isTruncatedAt(txnId, executeAt.epoch(), route);
    }

    private boolean isTruncated(Unseekables<?, ?> participants, Ranges coordinates, Ranges additionalRanges, Ranges truncated)
    {
        return truncated.containsAll(participants.slice(coordinates, Minimal))
               && (additionalRanges == null || truncated.containsAll(participants.slice(additionalRanges, Minimal)));
    }

    public final void removeRedundantDependencies(WaitingOn.Update builder)
    {
        // first make sure we only include
        forEachRedundantRange(builder.deps.keyDeps.txnIds(), builder, builder.deps.keyDeps, (bld, deps, ranges, from, to) -> {
            deps.forEach(ranges, from, to, bld, null, (bld0, ignore, i) -> bld0.setAppliedOrInvalidated(i));
        });
        forEachRedundantRange(builder.deps.rangeDeps.txnIds(), builder, builder.deps.rangeDeps, (bld, deps, ranges, from, to) -> {
            deps.forEach(ranges, bld, (bld0, i) -> {
                if (i >= from && i < to)
                    bld0.setAppliedOrInvalidated(i + bld0.deps.keyDeps.txnIdCount());
            });
        });
    }

    /**
     * True iff this transaction may itself not execute locally
     */
    public final boolean hasRedundantDependencies(TxnId txnId)
    {
        // TODO (required): consider race conditions when bootstrapping into an active command store, that may have seen a higher txnId than this?
        //   might benefit from maintaining a per-CommandStore largest TxnId register to ensure we allocate a higher TxnId for our ExclSync,
        //   or from using whatever summary records we have for the range, once we maintain them
        // TODO (expected): consider whether ranges are affected
        // we use ceilingEntry only to ensure consistency with {@code isTruncated}
        return redundantBefore.higherEntry(txnId) != null;
    }

    final boolean isRedundant(Command command)
    {
        if (command.route() == null)
            return false;
        Map.Entry<TxnId, Ranges> redundant = redundantBefore.higherEntry(command.txnId());
        return redundant != null && redundant.getValue().intersects(command.route().participants());
    }

    final boolean isRedundant(TxnId txnId, Unseekables<?, ?> someUnseekables)
    {
        Map.Entry<TxnId, Ranges> redundant = redundantBefore.higherEntry(txnId);
        return redundant != null && redundant.getValue().intersects(someUnseekables);
    }

    /**
     * Loop over each range that has been truncated for a TxnId interval.
     */
    final <P1, P2> void forEachRedundantRange(SortedArrayList<TxnId> txnIds, P1 p1, P2 p2, IndexedRangeTriConsumer<P1, P2, Ranges> forEach)
    {
        if (txnIds.isEmpty())
            return;

        int i = 0;
        while (i < txnIds.size())
        {
            Map.Entry<TxnId, Ranges> toInclusive = redundantBefore.higherEntry(txnIds.get(i));
            if (toInclusive == null)
                return;

            int nexti = txnIds.findNext(i, toInclusive.getKey());
            if (nexti < 0) nexti = -1 - nexti;
            else ++nexti;

            if (!toInclusive.getValue().isEmpty())
            {
                forEach.accept(p1, p2, toInclusive.getValue(), i, nexti);
            }
            i = nexti;
        }
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

    private static <T extends Timestamp> ImmutableSortedMap<T, DurableOrBootstrapped> truncateShared(T at, Ranges ranges, NavigableMap<T, DurableOrBootstrapped> truncatedOrBootstrappedAt)
    {
        Invariants.checkArgument(!ranges.isEmpty());
        // if we're bootstrapping these ranges, then any period we previously owned the ranges for is effectively invalidated
        TreeMap<T, DurableOrBootstrapped> build = new TreeMap<>(truncatedOrBootstrappedAt);
        Map.Entry<T, DurableOrBootstrapped> prev = build.floorEntry(at);
        Map.Entry<T, DurableOrBootstrapped> next = build.ceilingEntry(at);
        Ranges prevBootstrapped = prev == null ? Ranges.EMPTY : prev.getValue().bootstrapped;
        Ranges nextTruncated = next == null ? Ranges.EMPTY : next.getValue().truncated;
        DurableOrBootstrapped insert = new DurableOrBootstrapped(prevBootstrapped, nextTruncated);
        build.put(at, insert);
        build.headMap(at, false).entrySet().forEach(e -> e.setValue(e.getValue().truncate(ranges)));
        return trimAndBuild(build);
    }

    private static <T extends Timestamp> ImmutableSortedMap<T, DurableOrBootstrapped> bootstrapShared(T at, Ranges ranges, NavigableMap<T, DurableOrBootstrapped> truncatedOrBootstrappedAt)
    {
        Invariants.checkArgument(!ranges.isEmpty());
        // if we're bootstrapping these ranges, then any period we previously owned the ranges for is effectively invalidated
        TreeMap<T, DurableOrBootstrapped> build = new TreeMap<>(truncatedOrBootstrappedAt);
        Map.Entry<T, DurableOrBootstrapped> prev = build.floorEntry(at);
        Map.Entry<T, DurableOrBootstrapped> next = build.ceilingEntry(at);
        Ranges bootstrapped = prev == null ? Ranges.EMPTY : prev.getValue().bootstrapped;
        Ranges truncated = next == null ? Ranges.EMPTY : next.getValue().truncated;
        DurableOrBootstrapped insert = new DurableOrBootstrapped(bootstrapped.with(ranges.subtract(truncated)), truncated);
        build.put(at, insert);
        build.headMap(at, false).entrySet().forEach(e -> e.setValue(e.getValue().unbootstrap(ranges)));
        build.tailMap(at, false).entrySet().forEach(e -> e.setValue(e.getValue().bootstrap(ranges)));
        return trimAndBuild(build);
    }

    private static <T extends Timestamp> ImmutableSortedMap<T, DurableOrBootstrapped> trimAndBuild(TreeMap<T, DurableOrBootstrapped> truncatedOrBootstrappedAt)
    {
        Iterator<Map.Entry<T, DurableOrBootstrapped>> iterator = truncatedOrBootstrappedAt.descendingMap().entrySet().iterator();
        Map.Entry<T, DurableOrBootstrapped> prev = null;
        T firstKey = truncatedOrBootstrappedAt.firstKey();
        while (iterator.hasNext())
        {
            Map.Entry<T, DurableOrBootstrapped> next = iterator.next();
            if (next.getKey() == firstKey)
                break;

            if (prev != null && prev.getValue().equals(next.getValue())) iterator.remove();
            else prev = next;
        }
        return ImmutableSortedMap.copyOf(truncatedOrBootstrappedAt);
    }

    private static <T extends Timestamp> ImmutableSortedMap<T, Ranges> truncate(T at, Ranges ranges, NavigableMap<T, Ranges> truncatedAt)
    {
        Invariants.checkArgument(!ranges.isEmpty());
        return purgeAndInsertBackwards(truncatedAt, at, ranges);
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

    private static <T extends Timestamp> ImmutableSortedMap<T, Ranges> purgeAndInsertBackwards(NavigableMap<T, Ranges> in, T insertAt, Ranges insert)
    {
        TreeMap<T, Ranges> build = new TreeMap<>(in);
        build.headMap(insertAt, false).entrySet().forEach(e -> e.setValue(e.getValue().union(MERGE_ADJACENT, insert)));
        Map.Entry<T, Ranges> prev = build.ceilingEntry(insertAt);
        build.putIfAbsent(insertAt, prev == null ? insert : prev.getValue().with(insert));
        Iterator<Map.Entry<T, Ranges>> iterator = build.descendingMap().entrySet().iterator();
        prev = null;
        while (iterator.hasNext())
        {
            Map.Entry<T, Ranges> next = iterator.next();
            if (prev != null && prev.getValue().equals(next.getValue())) iterator.remove();
            else prev = next;
        }
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
