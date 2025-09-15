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
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

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
import accord.coordinate.CoordinateEphemeralRead;
import accord.coordinate.CoordinateTransaction;
import accord.coordinate.Coordination;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.CoordinationAdapter.Factory.Kind;
import accord.coordinate.Coordinations;
import accord.coordinate.Infer.InvalidIf;
import accord.coordinate.Outcome;
import accord.coordinate.PrepareRecovery;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.CommandStores.StoreSelector;
import accord.local.durability.DurabilityService;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.primitives.Ballot;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.TxnId.Cardinality;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.DeterministicSet;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import accord.utils.PersistentField.Persister;
import accord.utils.RandomSource;
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
import static accord.coordinate.Coordination.CoordinationKind.COORDINATES_STATE_MACHINE;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.Cardinality.Any;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Node implements ConfigurationService.Listener, NodeCommandStoreService
{
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

    private volatile DurableBefore durableBefore = DurableBefore.EMPTY;
    private DurableBefore minDurableBefore = DurableBefore.EMPTY;
    private final ReentrantLock durableBeforeLock = new ReentrantLock();
    private final PersistentField<DurableBefore, DurableBefore> persistDurableBefore;

    private final Coordinations coordinations = new Coordinations();
    private final AtomicLong nextCoordinationId = new AtomicLong();

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
        this.persistDurableBefore = new PersistentField<>(() -> durableBefore,
                                                          (input, prev) -> {
                                                              DurableBefore next = DurableBefore.merge(input, prev);
                                                              if (next.equals(prev))
                                                                  return prev;
                                                              return next.equals(prev) ? prev : next;
                                                          },
                                                          safeDurableBeforePersister(durableBeforePersister),
                                                          this::setPersistedDurableBefore);
        this.commandStores = factory.create(this, agent, dataSupplier.get(), random.fork(), journal, shardDistributor, progressLogFactory.apply(this), localListenersFactory.apply(this));
        this.durabilityService = new DurabilityService(this);
        // TODO (desired): make frequency configurable
        scheduler.recurring(() -> commandStores.forAllUnsafe(store -> store.progressLog.maybeNotify()), 1, SECONDS);
        scheduler.recurring(timeouts::maybeNotify, 100, MILLISECONDS);
        configService.registerListener(this);
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
        ready.coordinate.invokeIfSuccess(() -> this.topology.onEpochSyncComplete(id, ready.epoch));
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
            Invariants.require(newDurableBefore.min.quorumBefore.compareTo(durableBefore.min.quorumBefore) >= 0,
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
        return withEpochExact(addDurableBefore.maxEpoch(), (AsyncExecutor)null, () -> persistDurableBefore.mergeAndUpdate(addDurableBefore).chain())
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
                              previous.coordinate.flatMap(ignore -> next.coordinate),
                              next.data,
                              next.reads);
    }

    @Override
    public synchronized AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync)
    {
        if (topology.epoch() <= this.topology.epoch())
            return AsyncResults.success(null);
        EpochReady ready = onTopologyUpdateInternal(topology, startSync);
        long epoch = topology.epoch();
        ready.coordinate.invokeIfSuccess(() -> this.topology.onEpochSyncComplete(id, epoch)).invoke(agent);
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
    public void withEpochAtLeast(EpochSupplier epochSupplier, @Nullable AsyncExecutor executor, BiConsumer<Void, Throwable> callback)
    {
        if (epochSupplier == null)
            callback.accept(null, null);
        else
            withEpochAtLeast(epochSupplier.epoch(), executor, callback);
    }

    public void withEpochAtLeast(long epoch, @Nullable AsyncExecutor ifAsync, BiConsumer<Void, Throwable> callback)
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

    public Object withEpochAtLeast(long epoch, @Nullable AsyncExecutor ifAsync, BiConsumer<?, ? super Throwable> ifFailure, Runnable ifSuccess)
    {
        if (topology.hasAtLeastEpoch(epoch))
        {
            ifSuccess.run();
            return ifSuccess;
        }
        else
        {
            configService.fetchTopologyForEpoch(epoch);
            return topology.awaitEpoch(epoch, ifAsync).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, fail);
                else ifSuccess.run();
            });
        }
    }

    public void withEpochExact(long epoch, @Nullable AsyncExecutor ifAsync, BiConsumer<?, Throwable> ifFailure, Function<Throwable, Throwable> onFailure, Runnable ifSuccess)
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
    public <T> AsyncChain<T> withEpochExact(long epoch, @Nullable AsyncExecutor executor, Supplier<? extends AsyncChain<T>> supplier)
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
            AsyncChain<T> res = topology.awaitEpoch(epoch, executor).flatMapOverride(supplier);
            configService.fetchTopologyForEpoch(epoch);
            return res;
        }
    }

    @Inline
    public <T> AsyncChain<T> withEpochAtLeast(long epoch, @Nullable AsyncExecutor executor, Supplier<? extends AsyncChain<T>> supplier)
    {
        if (topology.hasAtLeastEpoch(epoch))
        {
            return supplier.get();
        }
        else
        {
            AsyncChain<T> res = topology.awaitEpoch(epoch, executor).flatMapOverride(supplier);
            configService.fetchTopologyForEpoch(epoch);
            return res;
        }
    }

    public void withEpochAtLeast(long epoch, @Nullable AsyncExecutor ifAsync, BiConsumer<?, Throwable> ifFailure, Function<Throwable, Throwable> onFailure, Runnable ifSuccess)
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
    public <T> Cancellable send(Id to, Request send, @Nonnull AsyncExecutor executor, Callback<T> callback)
    {
        return messageSink.send(to, send, executor, callback);
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

    public TxnId nextTxnIdWithDefaultFlags(Txn.Kind rw, Domain domain)
    {
        return nextTxnIdWithFlags(rw, domain, Any, defaultMediumPath().bit());
    }

    public TxnId nextStaleTxnIdWithDefaultFlags(long minEpoch, long minHlc, Txn.Kind rw, Domain domain)
    {
        return nextStaleTxnIdWithFlags(minEpoch, minHlc, rw, domain, Any, defaultMediumPath().bit());
    }

    public TxnId nextTxnIdWithDefaultFlags(Txn.Kind rw, Domain domain, Cardinality cardinality)
    {
        return nextTxnIdWithFlags(rw, domain, cardinality, defaultMediumPath().bit());
    }

    public TxnId nextTxnIdWithDefaultFlags(long minEpoch, long minHlc, Txn.Kind rw, Domain domain, Cardinality cardinality)
    {
        return newTxnId(Math.max(minEpoch, epoch()), uniqueNow(minHlc), rw, domain, cardinality, defaultMediumPath().bit(), id);
    }

    /**
     * TODO (required): Make sure we cannot re-issue the same txnid on startup
     * TODO (required): Don't use a new epoch for the TxnId at least until we know its definition
     */
    public TxnId nextTxnIdWithFlags(Txn.Kind rw, Domain domain, Cardinality cardinality, int flags)
    {
        return newTxnId(epoch(), uniqueNow(), rw, domain, cardinality, flags, id);
    }

    public TxnId nextStaleTxnIdWithFlags(long minEpoch, long minHlc, Txn.Kind rw, Domain domain, Cardinality cardinality, int flags)
    {
        long epoch = Math.max(minEpoch, epoch());
        long hlc = uniqueStale(minHlc);
        return newTxnId(epoch, hlc, rw, domain, cardinality, flags, id);
    }

    private static TxnId newTxnId(long epoch, long now, Txn.Kind rw, Domain domain, Cardinality cardinality, int flags, Node.Id node)
    {
        Invariants.require(domain == Key || rw != Write, "Range writes not supported without forwarding uniqueHlc information to WaitingOn for direct dependencies");
        Invariants.require(domain == Range || !rw.isSyncPoint, "Key ExclusiveSyncPoint not supported without improvements to CommandsForKey for managing execution");
        TxnId txnId = new TxnId(epoch, now, flags, rw, domain, cardinality, node);
        Invariants.require((txnId.lsb & (0xffff & ~TxnId.IDENTITY_FLAGS)) == 0);
        return txnId;
    }

    public TxnId nextTxnId(Txn txn)
    {
        return nextTxnId(0, 0, txn);
    }

    public TxnId nextTxnId(long minEpoch, long minHlc, Txn txn)
    {
        Seekables<?, ?> keys = txn.keys();
        Txn.Kind kind = txn.kind();
        return nextTxnId(minEpoch, minHlc, keys, kind);
    }

    public TxnId nextTxnId(@Nullable Timestamp min, Seekables<?, ?> keys, Txn.Kind kind)
    {
        return nextTxnId(min == null ? 0 : min.epoch(), min == null ? 0 : min.hlc(), keys, kind);
    }

    public TxnId nextTxnId(long minEpoch, long minHlc, Seekables<?, ?> keys, Txn.Kind kind)
    {
        Domain domain = keys.domain();
        Cardinality cardinality = cardinality(domain, keys);

        if (!usePrivilegedCoordinator() || (kind != Read && kind != Write))
            return nextTxnIdWithDefaultFlags(minEpoch, minHlc, kind, domain, cardinality);

        long epoch = Math.max(minEpoch, epoch());
        long hlc = uniqueNow(minHlc);
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

    public AsyncChain<Result> coordinate(Txn txn)
    {
        TxnId txnId = nextTxnId(txn);
        return coordinate(txnId, txn);
    }

    public AsyncChain<Result> coordinate(TxnId txnId, Txn txn)
    {
        return coordinate(txnId, txn, txnId.epoch(), Long.MAX_VALUE);
    }

    // TODO (required): plumb deadlineNanos in (perhaps on integration side, but maybe introduce some context we can pass through for the MessageSink)
    public AsyncChain<Result> coordinate(TxnId txnId, Txn txn, long minEpoch, long deadlineNanos)
    {
        return withEpochExact(Math.max(txnId.epoch(), minEpoch), (AsyncExecutor) null, () -> initiateCoordination(txnId, txn));
    }

    private AsyncChain<Result> initiateCoordination(TxnId txnId, Txn txn)
    {
        FullRoute<?> route = computeRoute(txnId, txn.keys());
        if (txnId.kind() == Txn.Kind.EphemeralRead)
            return CoordinateEphemeralRead.coordinate(this, route, txnId, txn);
        else
            return CoordinateTransaction.coordinate(this, route, txnId, txn);
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

    public AsyncChain<? extends Outcome> recover(TxnId txnId, InvalidIf invalidIf, FullRoute<?> route, LatentStoreSelector reportTo)
    {
        SequentialAsyncExecutor executor = someSequentialExecutor();
        return withEpochExact(txnId.epoch(), executor, () -> new AsyncChains.Head<>()
        {
            @Override
            protected Cancellable start(BiConsumer<? super Outcome, Throwable> callback)
            {
                PrepareRecovery.recover(Node.this, executor, txnId, invalidIf, route, null, reportTo, callback);
                return null;
            }
        });
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

    public AsyncChain<Void> updateMinHlc(long minHlc)
    {
        // TODO (required): command stores that are not ready due to bootstrap need to refresh their min HLC on bootstrap completion
        StoreSelector selector = snapshot -> Stream.of(snapshot.shards).map(sh -> sh.store).iterator();
        return commandStores().mapReduce(selector, new MapReduceCommandStores<>(RoutingKeys.EMPTY)
        {
            @Override public Void reduce(Void o1, Void o2) { return null; }
            @Override public TxnId primaryTxnId() { return null; }
            @Override public String reason() { return "Update Min HLC"; }
            @Override protected Void applyInternal(SafeCommandStore safeStore)
            {
                safeStore.commandStore().updateMinHlc(minHlc);
                return null;
            }
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

    public long nextCoordinationId()
    {
        long startedAtNanos = time.elapsed(NANOSECONDS);
        long nextId = nextCoordinationId.get();
        if (startedAtNanos >= nextId && nextCoordinationId.compareAndSet(nextId, startedAtNanos))
            return startedAtNanos;
        return nextCoordinationId.incrementAndGet();
    }

    public void register(Coordination coordination)
    {
        coordinations.register(coordination);
    }

    public void unregister(Coordination coordination)
    {
        coordinations.unregister(coordination);
    }

    public Coordinations coordinations()
    {
        return coordinations;
    }

    public boolean isCoordinatingWithBallot(TxnId txnId, Ballot ballot)
    {
        long mostRecent = coordinations.mostRecent(txnId, COORDINATES_STATE_MACHINE, ballot);
        if (mostRecent < 0)
            return false;
        long ageNanos = Math.max(elapsed(NANOSECONDS) - mostRecent, 0);
        return !agent.isSlowCoordinator(ageNanos, NANOSECONDS, txnId, 1);
    }

    public void updateStamp()
    {
        stampUpdater.incrementAndGet(this);
    }
}