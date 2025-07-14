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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.AsyncExecutor;
import accord.api.ConfigurationService;
import accord.api.ConfigurationService.EpochReady;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.MessageSink;
import accord.api.ProgressLog;
import accord.api.RemoteListeners;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.api.Timeouts;
import accord.api.TopologySorter;
import accord.api.Tracing;
import accord.coordinate.CoordinateEphemeralRead;
import accord.coordinate.CoordinateTransaction;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.CoordinationAdapter.Factory.Kind;
import accord.coordinate.Infer.InvalidIf;
import accord.coordinate.Outcome;
import accord.coordinate.RecoverWithRoute;
import accord.local.CommandStores.StoreSelector;
import accord.local.durability.DurabilityService;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.primitives.Ballot;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Routables;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.TxnId.Cardinality;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.DeterministicSet;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;
import accord.utils.PersistentField;
import accord.utils.PersistentField.Persister;
import accord.utils.RandomSource;
import accord.utils.Reduce;
import accord.utils.SortedList;
import accord.utils.SortedListMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.api.ProtocolModifiers.Toggles.defaultMediumPath;
import static accord.api.ProtocolModifiers.Toggles.ensurePermitted;
import static accord.api.ProtocolModifiers.Toggles.usePrivilegedCoordinator;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.Any;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Node implements ConfigurationService.Listener, NodeCommandStoreService
{
    private static final Logger logger = LoggerFactory.getLogger(Node.class);

    public static class Id implements Comparable<Id>
    {
        public static final Id NONE = new Id(0);
        public static final Id MAX = new Id(Integer.MAX_VALUE);

        public final int id;

        public Id(int id)
        {
            this.id = id;
        }

        @Override
        public int hashCode()
        {
            return Integer.hashCode(id);
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof Id && equals((Id) that);
        }

        public boolean equals(Id that)
        {
            if (that == null) return false;
            return id == that.id;
        }

        @Override
        public int compareTo(Id that)
        {
            return Integer.compareUnsigned(this.id, that.id);
        }

        public String toString()
        {
            return Integer.toString(id);
        }
    }

    public boolean isCoordinating(TxnId txnId, Ballot promised)
    {
        // TODO (required): on a prod system expire coordination ownership by time for safety
        return promised.node.equals(id) && coordinating.containsKey(txnId);
    }

    private final Id id;
    private final MessageSink messageSink;
    private final ConfigurationService configService;
    private final TopologyManager topology;
    private final RemoteListeners listeners;
    private final Timeouts timeouts;
    private final CommandStores commandStores;
    private final CoordinationAdapter.Factory coordinationAdapters;

    private final TimeService time;
    private final UniqueTimeService uniqueTime;
    private final Agent agent;
    private final RandomSource random;

    private final Scheduler scheduler;
    private final DurabilityService durabilityService;

    // TODO (expected, liveness): monitor the contents of this collection for stalled coordination, and excise them
    private final Map<TxnId, AsyncResult<? extends Outcome>> coordinating = new ConcurrentHashMap<>();
    private volatile DurableBefore durableBefore = DurableBefore.EMPTY;
    private DurableBefore minDurableBefore = DurableBefore.EMPTY;
    private final ReentrantLock durableBeforeLock = new ReentrantLock();
    private final PersistentField<DurableBefore, DurableBefore> persistDurableBefore;

    /**
     * Used to guard some operations that should normally operate on consistent information, but in rare cases may need to repeat work.
     * For simplicity we have a global stamp counter for this.
     * At present, only used for managing unavailable() computations.
     */
    private volatile long stamp;
    private static final AtomicLongFieldUpdater<Node> stampUpdater = AtomicLongFieldUpdater.newUpdater(Node.class, "stamp");

    public Node(Id id, MessageSink messageSink,
                ConfigurationService configService, TimeService time, UniqueTimeService uniqueTime,
                Supplier<DataStore> dataSupplier, ShardDistributor shardDistributor, Agent agent, RandomSource random, Scheduler scheduler, TopologySorter.Supplier topologySorter,
                Function<Node, RemoteListeners> remoteListenersFactory, Function<Node, Timeouts> requestTimeoutsFactory, Function<Node, ProgressLog.Factory> progressLogFactory,
                Function<Node, LocalListeners.Factory> localListenersFactory, CommandStores.Factory factory, CoordinationAdapter.Factory coordinationAdapters,
                Persister<DurableBefore, DurableBefore> durableBeforePersister,
                Journal journal)
    {
        this.id = id;
        this.scheduler = scheduler; // we set scheduler first so that e.g. requestTimeoutsFactory and progressLogFactory can take references to it
        this.messageSink = messageSink;
        this.configService = configService;
        this.coordinationAdapters = coordinationAdapters;
        this.time = time;
        this.uniqueTime = uniqueTime;
        this.timeouts = requestTimeoutsFactory.apply(this);
        this.topology = new TopologyManager(topologySorter, agent, id, time, timeouts);
        this.listeners = remoteListenersFactory.apply(this);
        this.agent = agent;
        this.random = random;
        this.persistDurableBefore = new PersistentField<>(() -> durableBefore, DurableBefore::merge, safeDurableBeforePersister(durableBeforePersister), this::setPersistedDurableBefore);
        this.commandStores = factory.create(this, agent, dataSupplier.get(), random.fork(), journal, shardDistributor, progressLogFactory.apply(this), localListenersFactory.apply(this));
        this.durabilityService = new DurabilityService(this);
        // TODO (desired): make frequency configurable
        scheduler.recurring(() -> commandStores.forEachCommandStore(store -> store.progressLog.maybeNotify()), 1, SECONDS);
        scheduler.recurring(timeouts::maybeNotify, 100, MILLISECONDS);
        configService.registerListener(this);
    }

    public Map<TxnId, AsyncResult<? extends Outcome>> coordinating()
    {
        return ImmutableMap.copyOf(coordinating);
    }

    public void load()
    {
        persistDurableBefore.load();
    }

    public DurabilityService durability()
    {
        return durabilityService;
    }

    /**
     * This starts the node for tests and makes sure that the provided topology is acknowledged correctly.  This method is not
     * safe for production systems as it doesn't handle restarts and partially acknowledged histories
     * @return {@link EpochReady#metadata}
     */
    @VisibleForTesting
    public AsyncResult<Void> unsafeStart()
    {
        EpochReady ready = onTopologyUpdateInternal(configService.currentTopology(), false);
        ready.coordinate.invokeIfSuccess(() -> this.topology.onEpochSyncComplete(id, topology.epoch()));
        configService.acknowledgeEpoch(ready, false);
        return ready.metadata;
    }

    public CommandStores commandStores()
    {
        return commandStores;
    }

    public ConfigurationService configService()
    {
        return configService;
    }

    public MessageSink messageSink()
    {
        return messageSink;
    }

    public final DurableBefore durableBefore()
    {
        return durableBefore;
    }

    public void addNewRangesToDurableBefore(Ranges ranges, long epoch)
    {
        durableBeforeLock.lock();
        try
        {
            TxnId from = TxnId.minForEpoch(epoch);
            DurableBefore addDurableBefore = DurableBefore.create(ranges, from, from);
            DurableBefore newDurableBefore = DurableBefore.merge(durableBefore, addDurableBefore);
            // TODO (required): it is possible for this invariant to be breached if topologies are received out of order.
            //  We should not update min past the max known epoch.
            Invariants.require(newDurableBefore.min.majorityBefore.compareTo(durableBefore.min.majorityBefore) >= 0 || durableBefore.fullyContainedIn(newDurableBefore),
                    "Previous durable before: %s, new: %s", durableBefore, newDurableBefore);

            minDurableBefore = DurableBefore.merge(minDurableBefore, addDurableBefore);
            durableBefore = newDurableBefore;
        }
        finally
        {
            durableBeforeLock.unlock();
        }
    }

    private Persister<DurableBefore, DurableBefore> safeDurableBeforePersister(Persister<DurableBefore, DurableBefore> wrap)
    {
        return new Persister<>()
        {
            @Override
            public AsyncResult<?> persist(DurableBefore addValue, DurableBefore newValue)
            {
                Invariants.require(addValue.maxEpoch() <= epoch());
                return wrap.persist(addValue, newValue);
            }
            @Override public DurableBefore load() { return wrap.load(); }
        };
    }

    private void setPersistedDurableBefore(DurableBefore newDurableBefore)
    {
        durableBeforeLock.lock();
        try
        {
            // TODO (desired): do not re-merge any minDurableBefore that was already known when we created the update
            durableBefore = DurableBefore.merge(newDurableBefore, minDurableBefore);
        }
        finally
        {
            durableBeforeLock.unlock();
        }
    }

    public AsyncResult<?> markDurable(Ranges ranges, TxnId majorityBefore, TxnId universalBefore)
    {
        return markDurable(DurableBefore.create(ranges, majorityBefore, universalBefore));
    }

    public AsyncResult<?> markDurable(DurableBefore addDurableBefore)
    {
        return withEpochExact(addDurableBefore.maxEpoch(), (Executor)null, () -> persistDurableBefore.mergeAndUpdate(addDurableBefore))
               .beginAsResult();
    }

    @Override
    public long epoch()
    {
        return topology().epoch();
    }

    private synchronized EpochReady onTopologyUpdateInternal(Topology topology, boolean startSync)
    {
        Supplier<EpochReady> bootstrap = commandStores.updateTopology(this, topology, startSync);
        Supplier<EpochReady> orderFastPathReporting = () -> {
            if (this.topology.isEmpty()) return bootstrap.get();
            return orderFastPathReporting(this.topology.epochReady(topology.epoch() - 1), bootstrap.get());
        };

        return this.topology.onTopologyUpdate(topology, orderFastPathReporting, configService::reportEpochRemoved);
    }

    private static EpochReady orderFastPathReporting(EpochReady previous, EpochReady next)
    {
        if (previous.epoch + 1 != next.epoch)
            throw new IllegalArgumentException("Attempted to order epochs but they are not next to each other... previous=" + previous.epoch + ", next=" + next.epoch);
        if (previous.coordinate.isDone()) return next;
        return new EpochReady(next.epoch,
                              next.metadata,
                              previous.coordinate.flatMap(ignore -> next.coordinate).beginAsResult(),
                              next.data,
                              next.reads);
    }

    @Override
    public synchronized AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync)
    {
        if (topology.epoch() <= this.topology.epoch())
            return AsyncResults.success(null);
        EpochReady ready = onTopologyUpdateInternal(topology, startSync);
        ready.coordinate.invokeIfSuccess(() -> this.topology.onEpochSyncComplete(id, topology.epoch()));
        configService.acknowledgeEpoch(ready, startSync);
        return ready.coordinate;
    }

    @Override
    public void onRemoteSyncComplete(Id node, long epoch)
    {
        topology.onEpochSyncComplete(node, epoch);
    }

    @Override
    public void onEpochClosed(Ranges ranges, long epoch)
    {
        topology.onEpochClosed(ranges, epoch);
    }

    @Override
    public void onEpochRetired(Ranges ranges, long epoch)
    {
        topology.onEpochRetired(ranges, epoch);
    }

    // TODO (required): audit use of withEpochAtLeast vs withEpochExact
    // TODO (required): audit error handling, as the refactor to provide epoch timeouts appears to have broken a number of coordination
    // TODO (expected): provide a deadline
    public void withEpochAtLeast(EpochSupplier epochSupplier, @Nullable Executor executor, BiConsumer<Void, Throwable> callback)
    {
        if (epochSupplier == null)
            callback.accept(null, null);
        else
            withEpochAtLeast(epochSupplier.epoch(), executor, callback);
    }

    public void withEpochAtLeast(long epoch, @Nullable Executor ifAsync, BiConsumer<Void, Throwable> callback)
    {
        if (topology.hasAtLeastEpoch(epoch))
        {
            callback.accept(null, null);
        }
        else
        {
            topology.awaitEpoch(epoch, ifAsync).begin(callback);
            configService.fetchTopologyForEpoch(epoch);
        }
    }

    public void withEpochAtLeast(long epoch, @Nullable Executor ifAsync, BiConsumer<?, ? super Throwable> ifFailure, Runnable ifSuccess)
    {
        if (topology.hasAtLeastEpoch(epoch))
        {
            ifSuccess.run();
        }
        else
        {
            topology.awaitEpoch(epoch, ifAsync).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, fail);
                else ifSuccess.run();
            });
            configService.fetchTopologyForEpoch(epoch);
        }
    }

    public void withEpochExact(long epoch, @Nullable Executor ifAsync, BiConsumer<?, Throwable> ifFailure, Function<Throwable, Throwable> onFailure, Runnable ifSuccess)
    {
        if (epoch < topology.minEpoch())
        {
            ifFailure.accept(null, onFailure.apply(new TopologyManager.TopologyRetiredException(epoch, topology.minEpoch())));
        }
        else if (topology.hasEpoch(epoch))
        {
            ifSuccess.run();
        }
        else
        {
            topology.awaitEpoch(epoch, ifAsync).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, onFailure.apply(fail));
                else ifSuccess.run();
            });
            configService.fetchTopologyForEpoch(epoch);
        }
    }

    @Inline
    public <T> AsyncChain<T> withEpochExact(long epoch, @Nullable Executor executor, Supplier<? extends AsyncChain<T>> supplier)
    {
        if (epoch < topology.minEpoch())
        {
            return AsyncChains.failure(new TopologyManager.TopologyRetiredException(epoch, topology.minEpoch()));
        }
        else if (topology.hasEpoch(epoch))
        {
            return supplier.get();
        }
        else
        {
            AsyncChain<T> res = topology.awaitEpoch(epoch, executor).flatMap(ignore -> supplier.get());
            configService.fetchTopologyForEpoch(epoch);
            return res;
        }
    }

    @Inline
    public <T> AsyncChain<T> withEpochAtLeast(long epoch, @Nullable Executor executor, Supplier<? extends AsyncChain<T>> supplier)
    {
        if (topology.hasAtLeastEpoch(epoch))
        {
            return supplier.get();
        }
        else
        {
            AsyncChain<T> res = topology.awaitEpoch(epoch, executor).flatMap(ignore -> supplier.get());
            configService.fetchTopologyForEpoch(epoch);
            return res;
        }
    }

    public void withEpochAtLeast(long epoch, @Nullable Executor ifAsync, BiConsumer<?, Throwable> ifFailure, Function<Throwable, Throwable> onFailure, Runnable ifSuccess)
    {
        if (topology.hasAtLeastEpoch(epoch))
        {
            ifSuccess.run();
        }
        else
        {
            topology.awaitEpoch(epoch, ifAsync).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, onFailure.apply(fail));
                else ifSuccess.run();
            });
            configService.fetchTopologyForEpoch(epoch);
        }
    }


    public TopologyManager topology()
    {
        return topology;
    }

    @Override
    public AsyncExecutor someExecutor()
    {
        return commandStores.someExecutor();
    }

    @Override
    public SequentialAsyncExecutor someSequentialExecutor()
    {
        return commandStores.someSequentialExecutor();
    }

    public void shutdown()
    {
        commandStores.shutdown();
    }

    public long uniqueNow()
    {
        return uniqueTime.uniqueNow();
    }

    @Override
    public long uniqueNow(long greaterThan)
    {
        return uniqueTime.uniqueNow(greaterThan);
    }

    @Override
    public long uniqueStale(long greaterThan)
    {
        return uniqueTime.uniqueStale(greaterThan);
    }

    @Override
    public long now()
    {
        return time.now();
    }

    @Override
    public long elapsed(TimeUnit timeUnit)
    {
        return time.elapsed(timeUnit);
    }

    public AsyncChain<Void> forEachLocal(PreLoadContext context, Unseekables<?> unseekables, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return commandStores.forEach(context, unseekables, minEpoch, maxEpoch, forEach);
    }

    public AsyncChain<Void> forEachLocal(PreLoadContext context, StoreSelector selector, Consumer<SafeCommandStore> forEach)
    {
        return commandStores.forEach(context, selector, forEach);
    }

    public <T> Cancellable mapReduceConsumeLocal(TxnRequest<?> request, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        return commandStores.mapReduceConsume(request, request.scope(), minEpoch, maxEpoch, mapReduceConsume);
    }

    public <T> Cancellable mapReduceConsumeLocal(Unseekables<?> keys, long minEpoch, long maxEpoch, Function<? super CommandStore, AsyncChain<T>> map, Reduce<T, T> reduce, BiConsumer<? super T, Throwable> consume)
    {
        return commandStores.mapReduceConsume(keys, minEpoch, maxEpoch, map, reduce, consume);
    }

    public <T> Cancellable mapReduceConsumeLocal(PreLoadContext context, RoutingKey key, long atEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        return mapReduceConsumeLocal(context, key, atEpoch, atEpoch, mapReduceConsume);
    }

    public <T> Cancellable mapReduceConsumeLocal(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        return commandStores.mapReduceConsume(context, key, minEpoch, maxEpoch, mapReduceConsume);
    }

    public <T> Cancellable mapReduceConsumeLocal(PreLoadContext context, Unseekables<?> keys, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        return commandStores.mapReduceConsume(context, keys, minEpoch, maxEpoch, mapReduceConsume);
    }

    public <T> Cancellable mapReduceConsumeLocal(PreLoadContext context, StoreSelector selector, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        return commandStores.mapReduceConsume(context, selector, mapReduceConsume);
    }

    public <T> Cancellable mapReduceConsumeAllLocal(PreLoadContext context, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        return commandStores.mapReduceConsume(context, mapReduceConsume);
    }

    // send to every node besides ourselves
    public void send(Topology topology, Request send)
    {
        topology.nodes().forEach(id -> send(id, send));
    }

    public void send(Shard shard, Request send)
    {
        shard.nodes.forEach(node -> messageSink.send(node, send));
    }

    private void send(Shard shard, Request send, @Nonnull AsyncExecutor executor, Callback callback)
    {
        shard.nodes.forEach(node -> messageSink.send(node, send, executor, callback));
    }

    public void send(Collection<Id> to, Request send)
    {
        checkIterationSafe(to);
        to.forEach(dst -> send(dst, send));
    }

    public void send(Collection<Id> to, Function<Id, Request> requestFactory)
    {
        checkIterationSafe(to);
        to.forEach(dst -> send(dst, requestFactory.apply(dst)));
    }

    public <T> void send(Collection<Id> to, Request send, @Nonnull AsyncExecutor executor, Callback<T> callback)
    {
        checkIterationSafe(to);
        to.forEach(dst -> messageSink.send(dst, send, executor, callback));
    }

    // TODO (required): callback must be invoked if for any reason send fails
    public <T> void send(Collection<Id> to, Function<Id, Request> requestFactory, @Nonnull AsyncExecutor executor, Callback<T> callback)
    {
        checkIterationSafe(to);
        to.forEach(dst -> messageSink.send(dst, requestFactory.apply(dst), executor, callback));
    }

    private static void checkIterationSafe(Collection<?> collection)
    {
        if (!Invariants.isParanoid())
            return;
        if (collection instanceof List) return;
        if (collection instanceof NavigableSet
            || collection instanceof LinkedHashSet
            || collection instanceof SortedList
            || collection instanceof SortedListMap
            || collection instanceof SortedListMap.SetView
            || collection instanceof SortedListMap.CollectionView
            || "java.util.LinkedHashMap.LinkedKeySet".equals(collection.getClass().getCanonicalName())
            || collection instanceof DeterministicSet) return;
        throw new IllegalArgumentException("Attempted to use a collection that is unsafe for iteration: " + collection.getClass());
    }

    // send to a specific node
    public <T> void send(Id to, Request send, @Nonnull AsyncExecutor executor, Callback<T> callback)
    {
        messageSink.send(to, send, executor, callback);
    }

    // send to a specific node
    public void send(Id to, Request send)
    {
        messageSink.send(to, send);
    }

    public void reply(Id replyingToNode, ReplyContext replyContext, Reply send, Throwable failure)
    {
        if (failure != null)
        {
            agent.onUncaughtException(failure);
            if (send != null)
                agent().onUncaughtException(new IllegalArgumentException(String.format("fail (%s) and send (%s) are both not null", failure, send)));
            messageSink.replyWithUnknownFailure(replyingToNode, replyContext, failure);
            return;
        }
        else if (send == null)
        {
            NullPointerException e = new NullPointerException();
            agent.onUncaughtException(e);
            throw e;
        }
        messageSink.reply(replyingToNode, replyContext, send);
    }

    public TxnId nextTxnId(Txn.Kind rw, Domain domain)
    {
        return nextTxnId(rw, domain, Any, defaultMediumPath().bit());
    }

    public TxnId nextTxnId(Timestamp min, Txn.Kind rw, Domain domain)
    {
        return nextTxnId(min, rw, domain, Any, defaultMediumPath().bit());
    }

    public TxnId nextStaleTxnId(long minEpoch, long minHlc, Txn.Kind rw, Domain domain)
    {
        return nextStaleTxnId(minEpoch, minHlc, rw, domain, Any, defaultMediumPath().bit());
    }

    public TxnId nextTxnId(Txn.Kind rw, Domain domain, Cardinality cardinality)
    {
        return nextTxnId(rw, domain, cardinality, defaultMediumPath().bit());
    }

    public TxnId nextTxnId(long minHlc, Txn.Kind rw, Domain domain, Cardinality cardinality)
    {
        return newTxnId(epoch(), uniqueNow(minHlc), rw, domain, cardinality, defaultMediumPath().bit(), id);
    }

    public TxnId nextTxnId(Timestamp min, Txn.Kind rw, Domain domain, Cardinality cardinality)
    {
        return nextTxnId(min, rw, domain, cardinality, defaultMediumPath().bit());
    }

    public TxnId nextTxnId(Txn.Kind rw, Domain domain, int flags)
    {
        return nextTxnId(rw, domain, Any, flags);
    }

    public TxnId nextTxnId(Timestamp min, Txn.Kind rw, Domain domain, int flags)
    {
        return nextTxnId(min, rw, domain, Any, flags);
    }

    /**
     * TODO (required): Make sure we cannot re-issue the same txnid on startup
     * TODO (required): Don't use a new epoch for the TxnId at least until we know its definition
     */
    public TxnId nextTxnId(Txn.Kind rw, Domain domain, Cardinality cardinality, int flags)
    {
        return newTxnId(epoch(), uniqueNow(), rw, domain, cardinality, flags, id);
    }

    public TxnId nextTxnId(Timestamp min, Txn.Kind rw, Domain domain, Cardinality cardinality, int flags)
    {
        long epoch = min == null ? epoch() : Math.max(min.epoch(), epoch());
        long hlc = uniqueNow(min == null ? 0 : min.hlc());
        return newTxnId(epoch, hlc, rw, domain, cardinality, flags, id);
    }

    public TxnId nextStaleTxnId(long minEpoch, long minHlc, Txn.Kind rw, Domain domain, Cardinality cardinality, int flags)
    {
        long epoch = Math.max(minEpoch, epoch());
        long hlc = uniqueStale(minHlc);
        return newTxnId(epoch, hlc, rw, domain, cardinality, flags, id);
    }

    private static TxnId newTxnId(long epoch, long now, Txn.Kind rw, Domain domain, Cardinality cardinality, int flags, Node.Id node)
    {
        Invariants.require(domain == Key || rw != Write, "Range writes not supported without forwarding uniqueHlc information to WaitingOn for direct dependencies");
        Invariants.require(domain == Range || rw != Txn.Kind.ExclusiveSyncPoint, "Key ExclusiveSyncPoint not supported without improvements to CommandsForKey for managing execution");
        TxnId txnId = new TxnId(epoch, now, flags, rw, domain, cardinality, node);
        Invariants.require((txnId.lsb & (0xffff & ~TxnId.IDENTITY_FLAGS)) == 0);
        return txnId;
    }

    public TxnId nextTxnId(Txn txn)
    {
        Seekables<?, ?> keys = txn.keys();
        Txn.Kind kind = txn.kind();
        return nextTxnId(keys, kind);
    }

    public TxnId nextTxnId(Seekables<?, ?> keys, Txn.Kind kind)
    {
        return nextTxnId(null, keys, kind);
    }

    public TxnId nextTxnId(@Nullable Timestamp min, Seekables<?, ?> keys, Txn.Kind kind)
    {
        Domain domain = keys.domain();
        Cardinality cardinality = cardinality(domain, keys);

        if (!usePrivilegedCoordinator() || (kind != Read && kind != Write))
            return nextTxnId(min, kind, domain, cardinality);

        long epoch = min == null ? epoch() : Math.max(min.epoch(), epoch());
        long hlc = uniqueNow(min == null ? 0 : min.hlc());
        int flags = computeBestDefaultTxnIdFlags(keys, epoch);
        TxnId txnId = new TxnId(epoch, hlc, flags, kind, domain, cardinality, id);
        Invariants.require((txnId.lsb & (0xffff & ~TxnId.IDENTITY_FLAGS)) == 0);
        return txnId;
    }

    private int computeBestDefaultTxnIdFlags(Routables<?> keys, long epoch)
    {
        if (!topology.hasEpoch(epoch) || !usePrivilegedCoordinator())
            return defaultMediumPath().bit();

        TxnId.FastPath fastPath = ensurePermitted(topology().selectFastPath(keys, epoch));
        return fastPath.bits | defaultMediumPath().bit();
    }

    public TxnId nextTxnId(Txn txn, TxnId.FastPath fastPath, TxnId.MediumPath mediumPath)
    {
        Seekables<?, ?> keys = txn.keys();
        Txn.Kind kind = txn.kind();
        Domain domain = keys.domain();

        long epoch = epoch();
        long now = uniqueNow();
        fastPath = ensurePermitted(fastPath);
        if (fastPath != Unoptimised && (!topology.hasEpoch(epoch) || !topology.supportsPrivilegedFastPath(keys, epoch)))
            fastPath = Unoptimised;

        Cardinality cardinality = cardinality(domain, keys);
        return newTxnId(epoch, now, kind, domain, cardinality, fastPath.bits | mediumPath.bit(), id);
    }

    public AsyncResult<Result> coordinate(Txn txn)
    {
        TxnId txnId = nextTxnId(txn);
        return coordinate(txnId, txn);
    }

    public AsyncResult<Result> coordinate(TxnId txnId, Txn txn)
    {
        return coordinate(txnId, txn, txnId.epoch(), Long.MAX_VALUE);
    }

    // TODO (required): plumb deadlineNanos in (perhaps on integration side, but maybe introduce some context we can pass through for the MessageSink)
    public AsyncResult<Result> coordinate(TxnId txnId, Txn txn, long minEpoch, long deadlineNanos)
    {
        AsyncResult<Result> result = withEpochExact(Math.max(txnId.epoch(), minEpoch), (Executor) null, () -> initiateCoordination(txnId, txn)).beginAsResult();
        coordinating.putIfAbsent(txnId, result);
        result.invoke((success, fail) -> coordinating.remove(txnId, result));
        return result;
    }

    private AsyncResult<Result> initiateCoordination(TxnId txnId, Txn txn)
    {
        if (txnId.kind() == Txn.Kind.EphemeralRead)
        {
            // TODO (expected): once non-participating home keys are removed, this can be simplified to share computeRoute
            FullRoute<?> route = txn.keys().toRoute(txn.keys().get(0).someIntersectingRoutingKey(null));
            return CoordinateEphemeralRead.coordinate(this, route, txnId, txn);
        }
        else
        {
            FullRoute<?> route = computeRoute(txnId, txn.keys());
            return CoordinateTransaction.coordinate(this, route, txnId, txn);
        }
    }

    public FullRoute<?> computeRoute(TxnId txnId, Routables<?> keysOrRanges)
    {
        return computeRoute(txnId.epoch(), keysOrRanges);
    }

    public FullRoute<?> computeRoute(long epoch, Routables<?> keysOrRanges)
    {
        Invariants.requireArgument(!keysOrRanges.isEmpty(), "Attempted to compute a route from empty keys or ranges");
        RoutingKey homeKey = selectHomeKey(epoch, keysOrRanges);
        return keysOrRanges.toRoute(homeKey);
    }

    private RoutingKey selectHomeKey(long epoch, Routables<?> keysOrRanges)
    {
        Ranges owned = topology().localForEpoch(epoch).ranges();
        int i = (int)keysOrRanges.findNextIntersection(0, owned, 0);
        if (i >= 0)
            return keysOrRanges.get(i).someIntersectingRoutingKey(owned);

        return keysOrRanges.get(random.nextInt(keysOrRanges.size())).someIntersectingRoutingKey(null);
    }

    static class RecoverFuture<T> extends AsyncResults.SettableResult<T> implements BiConsumer<T, Throwable>
    {
        @Override
        public void accept(T success, Throwable fail)
        {
            if (fail != null) tryFailure(fail);
            else trySuccess(success);
        }
    }

    public AsyncResult<? extends Outcome> recover(TxnId txnId, InvalidIf invalidIf, FullRoute<?> route, StoreSelector reportTo, @Nullable Tracing tracing)
    {
        {
            AsyncResult<? extends Outcome> result = coordinating.get(txnId);
            if (result != null)
                return result;
        }

        SequentialAsyncExecutor executor = someSequentialExecutor();
        AsyncResult<Outcome> result = withEpochExact(txnId.epoch(), executor, () -> {
            RecoverFuture<Outcome> future = new RecoverFuture<>();
            RecoverWithRoute.recover(this, executor, txnId, invalidIf, route, null, reportTo, future, tracing);
            return future;
        }).beginAsResult();
        coordinating.putIfAbsent(txnId, result);
        result.invoke((success, fail) -> coordinating.remove(txnId, result));
        return result;
    }

    public void receive(Request request, Id from, ReplyContext replyContext)
    {
        long waitForEpoch = request.waitForEpoch();
        withEpochAtLeast(waitForEpoch, null, agent, () -> {
            try
            {
                request.process(this, from, replyContext);
            }
            catch (Throwable t)
            {
                reply(from, replyContext, null, t);
            }
        });
    }

    public <R> CoordinationAdapter<R> coordinationAdapter(TxnId txnId, Kind kind)
    {
        return coordinationAdapters.get(txnId, kind);
    }

    public void updateMinHlc(long minHlc)
    {
        commandStores().forEach((commandStore, ranges) -> {
            commandStore.updateMinHlc(minHlc);
        });
    }

    public Scheduler scheduler()
    {
        return scheduler;
    }

    public Agent agent()
    {
        return agent;
    }

    public RemoteListeners remoteListeners()
    {
        return listeners;
    }

    @Override
    public Timeouts timeouts()
    {
        return timeouts;
    }

    @Override
    public Id id()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Node{" + id + '}';
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(RoutingKey key)
    {
        return commandStores.unsafeForKey(key);
    }

    public CommandStore unsafeByIndex(int index)
    {
        return commandStores.current.shards[index].store;
    }

    public TimeService time()
    {
        return time;
    }

    public final long currentStamp()
    {
        return stamp;
    }

    public void updateStamp()
    {
        stampUpdater.incrementAndGet(this);
    }
}