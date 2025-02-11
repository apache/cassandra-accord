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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.ConfigurationService;
import accord.api.ConfigurationService.EpochReady;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalConfig;
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
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.CoordinationAdapter.Factory.Kind;
import accord.coordinate.Infer.InvalidIf;
import accord.coordinate.Outcome;
import accord.coordinate.RecoverWithRoute;
import accord.impl.DurabilityScheduling;
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
import accord.utils.SortedList;
import accord.utils.SortedListMap;
import accord.utils.WrappableException;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncExecutor;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.api.ProtocolModifiers.Toggles.ensurePermitted;
import static accord.api.ProtocolModifiers.Toggles.defaultMediumPath;
import static accord.api.ProtocolModifiers.Toggles.usePrivilegedCoordinator;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.TxnId.Cardinality.Any;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static accord.utils.Invariants.illegalState;
import static java.lang.String.format;
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
            return Integer.compare(this.id, that.id);
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
    private final AtomicReference<Timestamp> now;
    private final Agent agent;
    private final RandomSource random;
    private final LocalConfig localConfig;

    private final Scheduler scheduler;
    private final DurabilityScheduling durabilityScheduling;

    // TODO (expected, liveness): monitor the contents of this collection for stalled coordination, and excise them
    private final Map<TxnId, AsyncResult<? extends Outcome>> coordinating = new ConcurrentHashMap<>();
    private volatile DurableBefore durableBefore = DurableBefore.EMPTY;
    private DurableBefore minDurableBefore = DurableBefore.EMPTY;
    private final ReentrantLock durableBeforeLock = new ReentrantLock();
    private final PersistentField<DurableBefore, DurableBefore> persistDurableBefore;

    public Node(Id id, MessageSink messageSink,
                ConfigurationService configService, TimeService time,
                Supplier<DataStore> dataSupplier, ShardDistributor shardDistributor, Agent agent, RandomSource random, Scheduler scheduler, TopologySorter.Supplier topologySorter,
                Function<Node, RemoteListeners> remoteListenersFactory, Function<Node, Timeouts> requestTimeoutsFactory, Function<Node, ProgressLog.Factory> progressLogFactory,
                Function<Node, LocalListeners.Factory> localListenersFactory, CommandStores.Factory factory, CoordinationAdapter.Factory coordinationAdapters,
                Persister<DurableBefore, DurableBefore> durableBeforePersister,
                LocalConfig localConfig, Journal journal)
    {
        this.id = id;
        this.scheduler = scheduler; // we set scheduler first so that e.g. requestTimeoutsFactory and progressLogFactory can take references to it
        this.localConfig = localConfig;
        this.messageSink = messageSink;
        this.configService = configService;
        this.coordinationAdapters = coordinationAdapters;
        this.topology = new TopologyManager(topologySorter, agent, id, scheduler, time, localConfig);
        topology.scheduleTopologyUpdateWatchdog();
        this.listeners = remoteListenersFactory.apply(this);
        this.timeouts = requestTimeoutsFactory.apply(this);
        this.time = time;
        this.now = new AtomicReference<>(Timestamp.fromValues(topology.epoch(), time.now(), id));
        this.agent = agent;
        this.random = random;
        this.persistDurableBefore = new PersistentField<>(() -> durableBefore, DurableBefore::merge, durableBeforePersister, this::setPersistedDurableBefore);
        this.commandStores = factory.create(this, agent, dataSupplier.get(), random.fork(), journal, shardDistributor, progressLogFactory.apply(this), localListenersFactory.apply(this));
        this.durabilityScheduling = new DurabilityScheduling(this);
        // TODO (desired): make frequency configurable
        scheduler.recurring(() -> commandStores.forEachCommandStore(store -> store.progressLog.maybeNotify()), 1, SECONDS);
        scheduler.recurring(timeouts::maybeNotify, 100, MILLISECONDS);
        configService.registerListener(this);
        configService.registerListener(durabilityScheduling);
    }

    public LocalConfig localConfig()
    {
        return localConfig;
    }

    public Map<TxnId, AsyncResult<? extends Outcome>> coordinating()
    {
        return ImmutableMap.copyOf(coordinating);
    }

    public void load()
    {
        persistDurableBefore.load();
    }

    public DurabilityScheduling durabilityScheduling()
    {
        return durabilityScheduling;
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
        durabilityScheduling.updateTopology();
        ready.coordinate.addCallback(() -> this.topology.onEpochSyncComplete(id, topology.epoch()));
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
            Invariants.require(newDurableBefore.min.majorityBefore.compareTo(durableBefore.min.majorityBefore) >= 0);
            minDurableBefore = DurableBefore.merge(minDurableBefore, addDurableBefore);
            durableBefore = newDurableBefore;
        }
        finally
        {
            durableBeforeLock.unlock();
        }
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
        return persistDurableBefore.mergeAndUpdate(DurableBefore.merge(durableBefore, addDurableBefore));
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
        return this.topology.onTopologyUpdate(topology, orderFastPathReporting);
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
        ready.coordinate.addCallback(() -> this.topology.onEpochSyncComplete(id, topology.epoch()));
        configService.acknowledgeEpoch(ready, startSync);
        return ready.coordinate;
    }

    @Override
    public void onRemoteSyncComplete(Id node, long epoch)
    {
        topology.onEpochSyncComplete(node, epoch);
    }

    @Override
    public void onRemoveNode(long epoch, Id removed)
    {
        topology.onRemoveNode(epoch, removed);
    }

    @Override
    public void truncateTopologyUntil(long epoch)
    {
        topology.truncateTopologyUntil(epoch);
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

    // TODO (required): audit error handling, as the refactor to provide epoch timeouts appears to have broken a number of coordination
    public void withEpoch(EpochSupplier epochSupplier, BiConsumer<Void, Throwable> callback)
    {
        if (epochSupplier == null)
            callback.accept(null, null);
        else
            withEpoch(epochSupplier.epoch(), callback);
    }

    public void withEpoch(long epoch, BiConsumer<Void, Throwable> callback)
    {
        if (topology.hasAtLeastEpoch(epoch))
        {
            callback.accept(null, null);
        }
        else
        {
            configService.fetchTopologyForEpoch(epoch);
            topology.awaitEpoch(epoch).begin(callback);
        }
    }

    public void withEpoch(long epoch, BiConsumer<?, Throwable> ifFailure, Runnable ifSuccess)
    {
        if (topology.hasEpoch(epoch))
        {
            ifSuccess.run();
        }
        else
        {
            configService.fetchTopologyForEpoch(epoch);
            topology.awaitEpoch(epoch).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, fail);
                else ifSuccess.run();
            });
        }
    }

    public void withEpoch(long epoch, BiConsumer<?, Throwable> ifFailure, Function<Throwable, Throwable> onFailure, Runnable ifSuccess)
    {
        if (topology.hasEpoch(epoch))
        {
            ifSuccess.run();
        }
        else
        {
            configService.fetchTopologyForEpoch(epoch);
            topology.awaitEpoch(epoch).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, onFailure.apply(fail));
                else ifSuccess.run();
            });
        }
    }

    @Inline
    public <T> AsyncChain<T> withEpoch(long epoch, Supplier<? extends AsyncChain<T>> supplier)
    {
        if (topology.hasEpoch(epoch))
        {
            return supplier.get();
        }
        else
        {
            configService.fetchTopologyForEpoch(epoch);
            return topology.awaitEpoch(epoch).flatMap(ignore -> supplier.get());
        }
    }

    public TopologyManager topology()
    {
        return topology;
    }

    public void shutdown()
    {
        commandStores.shutdown();
        topology.shutdown();
    }

    public Timestamp uniqueNow()
    {
        while (true)
        {
            Timestamp cur = now.get();
            Timestamp next = cur.withNextHlc(time.now())
                                .withEpochAtLeast(topology.epoch());

            if (now.compareAndSet(cur, next))
                return next;
        }
    }

    @Override
    public Timestamp uniqueNow(Timestamp atLeast)
    {
        Timestamp cur = now.get();
        if (!cur.isAtLeastByEpochAndHlc(atLeast))
        {
            long topologyEpoch = topology.epoch();
            if (atLeast.epoch() > topologyEpoch)
                configService.fetchTopologyForEpoch(atLeast.epoch());
            now.accumulateAndGet(atLeast, Node::nowAtLeast);
        }
        return uniqueNow();
    }

    private static Timestamp nowAtLeast(Timestamp current, Timestamp proposed)
    {
        long currentEpoch = current.epoch(), proposedEpoch = proposed.epoch();
        long maxEpoch = Math.max(currentEpoch, proposedEpoch);

        long currentHlc = current.hlc(), proposedHlc = proposed.hlc();
        if (currentHlc == proposedHlc)
        {
            // we want to produce a zero Hlc
            int currentFlags = current.flags(), proposedFlags = proposed.flags();
            if (proposedFlags > currentFlags) ++proposedHlc;
            else if (proposedFlags == currentFlags && proposed.node.id > current.node.id) ++proposedHlc;
        }
        long maxHlc = Math.max(currentHlc, proposedHlc);

        if (maxEpoch == currentEpoch && maxHlc == currentHlc)
            return current;

        return Timestamp.fromValues(maxEpoch, maxHlc, current.flags(), current.node);
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

    public AsyncChain<Void> forEachLocalSince(PreLoadContext context, Unseekables<?> unseekables, Timestamp since, Consumer<SafeCommandStore> forEach)
    {
        return commandStores.forEach(context, unseekables, since.epoch(), Long.MAX_VALUE, forEach);
    }

    public AsyncChain<Void> ifLocal(PreLoadContext context, RoutingKey key, long epoch, Consumer<SafeCommandStore> ifLocal)
    {
        return commandStores.ifLocal(context, key, epoch, epoch, ifLocal);
    }

    public <T> Cancellable mapReduceConsumeLocal(TxnRequest<?> request, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        return commandStores.mapReduceConsume(request, request.scope(), minEpoch, maxEpoch, mapReduceConsume);
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

    public void send(Shard shard, Request send, Callback callback)
    {
        send(shard, send, CommandStore.current(), callback);
    }

    private void send(Shard shard, Request send, AgentExecutor executor, Callback callback)
    {
        checkStore(executor);
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

    public <T> void send(Collection<Id> to, Request send, Callback<T> callback)
    {
        send(to, send, CommandStore.current(), callback);
    }

    public <T> void send(Collection<Id> to, Request send, AgentExecutor executor, Callback<T> callback)
    {
        checkStore(executor);
        checkIterationSafe(to);
        to.forEach(dst -> messageSink.send(dst, send, executor, callback));
    }

    public <T> void send(Collection<Id> to, Function<Id, Request> requestFactory, Callback<T> callback)
    {
        send(to, requestFactory, CommandStore.current(), callback);
    }

    public <T> void send(Collection<Id> to, Function<Id, Request> requestFactory, AgentExecutor executor, Callback<T> callback)
    {
        checkStore(executor);
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
    public <T> void send(Id to, Request send, Callback<T> callback)
    {
        send(to, send, CommandStore.current(), callback);
    }

    // send to a specific node
    public <T> void send(Id to, Request send, AgentExecutor executor, Callback<T> callback)
    {
        checkStore(executor);
        messageSink.send(to, send, executor, callback);
    }

    private void checkStore(AsyncExecutor executor)
    {
        CommandStore current = CommandStore.maybeCurrent();
        if (current != null && current != executor)
            throw illegalState(format("Used wrong CommandStore %s; current is %s", executor, current));
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

    public TxnId nextTxnId(Txn.Kind rw, Domain domain, Cardinality cardinality)
    {
        return nextTxnId(rw, domain, cardinality, defaultMediumPath().bit());
    }

    public TxnId nextTxnId(Txn.Kind rw, Domain domain, int flags)
    {
        return nextTxnId(rw, domain, Any, flags);
    }

    /**
     * TODO (required): Make sure we cannot re-issue the same txnid on startup
     * TODO (required): Don't use a new epoch for the TxnId at least until we know its definition
     */
    public TxnId nextTxnId(Txn.Kind rw, Domain domain, Cardinality cardinality, int flags)
    {
        return nextTxnId(uniqueNow(), rw, domain, cardinality, flags);
    }

    private static TxnId nextTxnId(Timestamp now, Txn.Kind rw, Domain domain, Cardinality cardinality, int flags)
    {
        Invariants.require(domain == Key || rw != Txn.Kind.Write, "Range writes not supported without forwarding uniqueHlc information to WaitingOn for direct dependencies");
        TxnId txnId = new TxnId(now, flags, rw, domain, cardinality);
        Invariants.require((txnId.lsb & (0xffff & ~TxnId.IDENTITY_FLAGS)) == 0);
        return txnId;
    }

    public TxnId nextTxnId(Txn txn)
    {
        Seekables<?, ?> keys = txn.keys();
        Txn.Kind kind = txn.kind();
        Domain domain = keys.domain();
        Cardinality cardinality = cardinality(domain, keys);
        if (!usePrivilegedCoordinator())
            return nextTxnId(kind, domain, cardinality);

        Timestamp now = uniqueNow();
        int flags = computeBestDefaultTxnIdFlags(keys, now.epoch());
        TxnId txnId = new TxnId(now, flags, kind, domain, cardinality);
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

        Timestamp now = uniqueNow();
        fastPath = ensurePermitted(fastPath);
        if (fastPath != Unoptimised && (!topology.hasEpoch(now.epoch()) || !topology.supportsPrivilegedFastPath(keys, now.epoch())))
            fastPath = Unoptimised;

        Cardinality cardinality = cardinality(domain, keys);
        return nextTxnId(now, kind, domain, cardinality, fastPath.bits | mediumPath.bit());
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
        AsyncResult<Result> result = withEpoch(Math.max(txnId.epoch(), minEpoch), () -> initiateCoordination(txnId, txn)).beginAsResult();
        coordinating.putIfAbsent(txnId, result);
        result.addCallback((success, fail) -> coordinating.remove(txnId, result));
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

    public AsyncResult<? extends Outcome> recover(TxnId txnId, InvalidIf invalidIf, FullRoute<?> route, long reportLowEpoch, long reportHighEpoch)
    {
        {
            AsyncResult<? extends Outcome> result = coordinating.get(txnId);
            if (result != null)
                return result;
        }

        AsyncResult<Outcome> result = withEpoch(txnId.epoch(), () -> {
            RecoverFuture<Outcome> future = new RecoverFuture<>();
            RecoverWithRoute.recover(this, txnId, invalidIf, route, null, reportLowEpoch, reportHighEpoch, future);
            return future;
        }).beginAsResult();
        coordinating.putIfAbsent(txnId, result);
        result.addCallback((success, fail) -> coordinating.remove(txnId, result));
        return result;
    }

    public void receive(Request request, Id from, ReplyContext replyContext)
    {
        long waitForEpoch = request.waitForEpoch();
        if (waitForEpoch > topology.epoch())
        {
            configService.fetchTopologyForEpoch(waitForEpoch);
            topology().awaitEpoch(waitForEpoch).addCallback((ignored, failure) -> {
                if (failure != null)
                    agent().onUncaughtException(WrappableException.wrap(failure));
                else
                    receive(request, from, replyContext);
            });
            return;
        }

        Runnable processMsg = () -> {
            try
            {
                request.process(this, from, replyContext);
            }
            catch (Throwable t)
            {
                reply(from, replyContext, null, t);
            }
        };
        scheduler.now(processMsg);
    }

    public <R> CoordinationAdapter<R> coordinationAdapter(TxnId txnId, Kind kind)
    {
        return coordinationAdapters.get(txnId, kind);
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
}
