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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.ConfigurationService;
import accord.api.ConfigurationService.EpochReady;
import accord.api.DataStore;
import accord.api.LocalConfig;
import accord.api.LocalListeners;
import accord.api.MessageSink;
import accord.api.ProgressLog;
import accord.api.RemoteListeners;
import accord.api.RequestTimeouts;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.api.TopologySorter;
import accord.coordinate.CoordinateEphemeralRead;
import accord.coordinate.CoordinateTransaction;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.CoordinationAdapter.Factory.Step;
import accord.coordinate.CoordinationFailed;
import accord.coordinate.MaybeRecover;
import accord.coordinate.Outcome;
import accord.coordinate.RecoverWithRoute;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.TxnRequest;
import accord.primitives.Ballot;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
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
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncExecutor;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import net.nicoulaj.compilecommand.annotations.Inline;

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
    private final RequestTimeouts timeouts;
    private final CommandStores commandStores;
    private final CoordinationAdapter.Factory coordinationAdapters;

    private final TimeService time;
    private final AtomicReference<Timestamp> now;
    private final Agent agent;
    private final RandomSource random;
    private final LocalConfig localConfig;

    // TODO (expected, consider): this really needs to be thought through some more, as it needs to be per-instance in some cases, and per-node in others
    private final Scheduler scheduler;

    // TODO (expected, liveness): monitor the contents of this collection for stalled coordination, and excise them
    private final Map<TxnId, AsyncResult<? extends Outcome>> coordinating = new ConcurrentHashMap<>();
    private volatile DurableBefore durableBefore = DurableBefore.EMPTY;
    private DurableBefore minDurableBefore = DurableBefore.EMPTY;
    private final ReentrantLock durableBeforeLock = new ReentrantLock();
    private final PersistentField<DurableBefore, DurableBefore> persistDurableBefore;

    public Node(Id id, MessageSink messageSink,
                ConfigurationService configService, TimeService time,
                Supplier<DataStore> dataSupplier, ShardDistributor shardDistributor, Agent agent, RandomSource random, Scheduler scheduler, TopologySorter.Supplier topologySorter,
                Function<Node, RemoteListeners> remoteListenersFactory, Function<Node, RequestTimeouts> requestTimeoutsFactory, Function<Node, ProgressLog.Factory> progressLogFactory,
                Function<Node, LocalListeners.Factory> localListenersFactory, CommandStores.Factory factory, CoordinationAdapter.Factory coordinationAdapters,
                Persister<DurableBefore, DurableBefore> durableBeforePersister,
                LocalConfig localConfig)
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
        this.commandStores = factory.create(this, agent, dataSupplier.get(), random.fork(), shardDistributor, progressLogFactory.apply(this), localListenersFactory.apply(this));
        // TODO (desired): make frequency configurable
        scheduler.recurring(() -> commandStores.forEachCommandStore(store -> store.progressLog.maybeNotify()), 1, SECONDS);
        scheduler.recurring(timeouts::maybeNotify, 100, MILLISECONDS);
        configService.registerListener(this);
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

    /**
     * This starts the node for tests and makes sure that the provided topology is acknowledged correctly.  This method is not
     * safe for production systems as it doesn't handle restarts and partially acknowledged histories
     * @return {@link EpochReady#metadata}
     */
    @VisibleForTesting
    public AsyncResult<Void> unsafeStart()
    {
        EpochReady ready = onTopologyUpdateInternal(configService.currentTopology(), false, false);
        ready.coordination.addCallback(() -> this.topology.onEpochSyncComplete(id, topology.epoch()));
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

    public void addNewRangesToDurableBefore(Ranges ranges)
    {
        durableBeforeLock.lock();
        try
        {
            DurableBefore addDurableBefore = DurableBefore.create(ranges, TxnId.NONE, TxnId.NONE);
            minDurableBefore = DurableBefore.merge(minDurableBefore, addDurableBefore);
            durableBefore = DurableBefore.merge(durableBefore, addDurableBefore);
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

    private synchronized EpochReady onTopologyUpdateInternal(Topology topology, boolean isLoad, boolean startSync)
    {
        Supplier<EpochReady> bootstrap = commandStores.updateTopology(this, topology, isLoad, startSync);
        Supplier<EpochReady> ordering = () -> {
            if (this.topology.isEmpty()) return bootstrap.get();
            return order(this.topology.epochReady(topology.epoch() - 1), bootstrap.get());
        };
        return this.topology.onTopologyUpdate(topology, ordering);
    }

    private static EpochReady order(EpochReady previous, EpochReady next)
    {
        if (previous.epoch + 1 != next.epoch)
            throw new IllegalArgumentException("Attempted to order epochs but they are not next to each other... previous=" + previous.epoch + ", next=" + next.epoch);
        if (previous.coordination.isDone()) return next;
        return new EpochReady(next.epoch,
                              next.metadata,
                              previous.coordination.flatMap(ignore -> next.coordination).beginAsResult(),
                              next.data,
                              next.reads);
    }

    @Override
    public synchronized AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync)
    {
        if (topology.epoch() <= this.topology.epoch())
            return AsyncResults.success(null);
        EpochReady ready = onTopologyUpdateInternal(topology, isLoad, startSync);
        ready.coordination.addCallback(() -> this.topology.onEpochSyncComplete(id, topology.epoch()));
        configService.acknowledgeEpoch(ready, startSync);
        return ready.coordination;
    }

    @Override
    public void onRemoteSyncComplete(Id node, long epoch)
    {
        topology.onEpochSyncComplete(node, epoch);
    }

    @Override
    public void onRemoveNodes(long epoch, Collection<Id> removed)
    {
        topology.onRemoveNodes(epoch, removed);
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
    public void onEpochRedundant(Ranges ranges, long epoch)
    {
        topology.onEpochRedundant(ranges, epoch);
    }

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
                if (fail != null) ifFailure.accept(null, CoordinationFailed.wrap(fail));
                else ifSuccess.run();;
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
        if (cur.compareTo(atLeast) < 0)
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

    public <T> void mapReduceConsumeLocal(TxnRequest<?> request, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        commandStores.mapReduceConsume(request, request.scope(), minEpoch, maxEpoch, mapReduceConsume);
    }

    public <T> void mapReduceConsumeLocal(PreLoadContext context, RoutingKey key, long atEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        mapReduceConsumeLocal(context, key, atEpoch, atEpoch, mapReduceConsume);
    }

    public <T> void mapReduceConsumeLocal(PreLoadContext context, RoutingKey key, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        commandStores.mapReduceConsume(context, key, minEpoch, maxEpoch, mapReduceConsume);
    }

    public <T> void mapReduceConsumeLocal(PreLoadContext context, Routables<?> keys, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        commandStores.mapReduceConsume(context, keys, minEpoch, maxEpoch, mapReduceConsume);
    }

    public <T> void mapReduceConsumeAllLocal(PreLoadContext context, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        commandStores.mapReduceConsume(context, mapReduceConsume);
    }

    // send to every node besides ourselves
    public void send(Topology topology, Request send)
    {
        Set<Id> contacted = new HashSet<>();
        topology.forEach(shard -> send(shard, send, contacted));
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

    private <T> void send(Shard shard, Request send, Set<Id> alreadyContacted)
    {
        shard.nodes.forEach(node -> {
            if (alreadyContacted.add(node))
                send(node, send);
        });
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

    /**
     * TODO (required): Make sure we cannot re-issue the same txnid on startup
     */
    public TxnId nextTxnId(Txn.Kind rw, Domain domain)
    {
        return new TxnId(uniqueNow(), rw, domain);
    }

    public AsyncResult<Result> coordinate(Txn txn)
    {
        return coordinate(nextTxnId(txn.kind(), txn.keys().domain()), txn);
    }

    public AsyncResult<Result> coordinate(TxnId txnId, Txn txn)
    {
        // TODO (desirable, consider): The combination of updating the epoch of the next timestamp with epochs we don't have topologies for,
        //  and requiring preaccept to talk to its topology epoch means that learning of a new epoch via timestamp
        //  (ie not via config service) will halt any new txns from a node until it receives this topology
        AsyncResult<Result> result = withEpoch(txnId.epoch(), () -> initiateCoordination(txnId, txn)).beginAsResult();
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
        Invariants.checkArgument(!keysOrRanges.isEmpty(), "Attempted to compute a route from empty keys or ranges");
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

    public AsyncResult<? extends Outcome> recover(TxnId txnId, FullRoute<?> route)
    {
        {
            AsyncResult<? extends Outcome> result = coordinating.get(txnId);
            if (result != null)
                return result;
        }

        AsyncResult<Outcome> result = withEpoch(txnId.epoch(), () -> {
            RecoverFuture<Outcome> future = new RecoverFuture<>();
            RecoverWithRoute.recover(this, txnId, route, null, future);
            return future;
        }).beginAsResult();
        coordinating.putIfAbsent(txnId, result);
        result.addCallback((success, fail) -> coordinating.remove(txnId, result));
        return result;
    }

    // TODO (low priority, API/efficiency): coalesce maybeRecover calls? perhaps have mutable knownStatuses so we can inject newer ones?
    public AsyncResult<? extends Outcome> maybeRecover(TxnId txnId, @Nonnull Route<?> someRoute, ProgressToken prevProgress)
    {
        AsyncResult<? extends Outcome> result = coordinating.get(txnId);
        if (result != null)
            return result;

        RecoverFuture<Outcome> future = new RecoverFuture<>();
        MaybeRecover.maybeRecover(this, txnId, someRoute, prevProgress, future);
        return future;
    }

    public void receive(Request request, Id from, ReplyContext replyContext)
    {
        long waitForEpoch = request.waitForEpoch();
        if (waitForEpoch > topology.epoch())
        {
            configService.fetchTopologyForEpoch(waitForEpoch);
            topology().awaitEpoch(waitForEpoch).addCallback((ignored, failure) -> {
                if (failure != null)
                    agent().onUncaughtException(CoordinationFailed.wrap(failure));
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

    public <R> CoordinationAdapter<R> coordinationAdapter(TxnId txnId, Step step)
    {
        return coordinationAdapters.get(txnId, step);
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

    public RequestTimeouts requestTimeouts()
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
