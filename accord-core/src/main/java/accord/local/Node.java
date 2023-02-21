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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.coordinate.*;
import accord.messages.*;
import accord.primitives.*;
import accord.primitives.Routable.Domain;
import accord.utils.MapReduceConsume;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import com.google.common.annotations.VisibleForTesting;

import accord.api.*;

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.Key;
import accord.api.MessageSink;
import accord.api.Result;
import accord.api.ProgressLog;
import accord.api.Scheduler;
import accord.api.DataStore;
import accord.messages.Callback;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.Reply;
import accord.coordinate.RecoverWithRoute;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import net.nicoulaj.compilecommand.annotations.Inline;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;

public class Node implements ConfigurationService.Listener, NodeTimeService
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
        return promised.node.equals(id) && coordinating.containsKey(txnId);
    }

    private final Id id;
    private final MessageSink messageSink;
    private final ConfigurationService configService;
    private final TopologyManager topology;
    private final CommandStores<?> commandStores;

    private final LongSupplier nowSupplier;
    private final AtomicReference<Timestamp> now;
    private final Agent agent;
    private final Random random;

    // TODO (expected, consider): this really needs to be thought through some more, as it needs to be per-instance in some cases, and per-node in others
    private final Scheduler scheduler;

    // TODO (expected, liveness): monitor the contents of this collection for stalled coordination, and excise them
    private final Map<TxnId, AsyncResult<? extends Outcome>> coordinating = new ConcurrentHashMap<>();

    public Node(Id id, MessageSink messageSink, ConfigurationService configService, LongSupplier nowSupplier,
                Supplier<DataStore> dataSupplier, ShardDistributor shardDistributor, Agent agent, Random random, Scheduler scheduler, TopologySorter.Supplier topologySorter,
                Function<Node, ProgressLog.Factory> progressLogFactory, CommandStores.Factory factory)
    {
        this.id = id;
        this.messageSink = messageSink;
        this.configService = configService;
        this.topology = new TopologyManager(topologySorter, id);
        this.nowSupplier = nowSupplier;
        Topology topology = configService.currentTopology();
        this.now = new AtomicReference<>(Timestamp.fromValues(topology.epoch(), nowSupplier.getAsLong(), id));
        this.agent = agent;
        this.random = random;
        this.scheduler = scheduler;
        this.commandStores = factory.create(this, agent, dataSupplier.get(), shardDistributor, progressLogFactory.apply(this));

        configService.registerListener(this);
        onTopologyUpdate(topology, false);
    }

    public CommandStores<?> commandStores()
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

    @Override
    public long epoch()
    {
        return topology().epoch();
    }

    private synchronized void onTopologyUpdate(Topology topology, boolean acknowledge)
    {
        if (topology.epoch() <= this.topology.epoch())
            return;
        commandStores.updateTopology(topology);
        this.topology.onTopologyUpdate(topology);
        if (acknowledge)
            configService.acknowledgeEpoch(topology.epoch());
    }

    @Override
    public synchronized void onTopologyUpdate(Topology topology)
    {
        onTopologyUpdate(topology, true);
    }

    @Override
    public void onEpochSyncComplete(Id node, long epoch)
    {
        topology.onEpochSyncComplete(node, epoch);
    }

    public void withEpoch(long epoch, Runnable runnable)
    {
        if (topology.hasEpoch(epoch))
        {
            runnable.run();
        }
        else
        {
            configService.fetchTopologyForEpoch(epoch);
            topology.awaitEpoch(epoch).addCallback(runnable);
        }
    }

    @Inline
    public <T> AsyncResult<T> withEpoch(long epoch, Supplier<AsyncResult<T>> supplier)
    {
        if (topology.hasEpoch(epoch))
        {
            return supplier.get();
        }
        else
        {
            configService.fetchTopologyForEpoch(epoch);
            return topology.awaitEpoch(epoch).flatMap(ignore -> supplier.get()).beginAsResult();
        }
    }

    public TopologyManager topology()
    {
        return topology;
    }

    public void shutdown()
    {
        commandStores.shutdown();
    }

    public Timestamp uniqueNow()
    {
        return now.updateAndGet(cur -> {
            // TODO (low priority, proof): this diverges from proof; either show isomorphism or make consistent
            long now = nowSupplier.getAsLong();
            long epoch = Math.max(cur.epoch(), topology.epoch());
            return now > cur.hlc()
                 ? Timestamp.fromValues(epoch, now, id)
                 : Timestamp.fromValues(epoch, cur.hlc() + 1, id);
        });
    }

    @Override
    public Timestamp uniqueNow(Timestamp atLeast)
    {
        if (now.get().compareTo(atLeast) < 0)
            now.accumulateAndGet(atLeast, (current, proposed) -> {
                long minEpoch = topology.epoch();
                current = current.withEpochAtLeast(minEpoch);
                proposed = proposed.withEpochAtLeast(minEpoch);
                return proposed.compareTo(current) <= 0 ? current.logicalNext(id) : proposed;
            });
        return uniqueNow();
    }

    @Override
    public long now()
    {
        return nowSupplier.getAsLong();
    }

    public AsyncChain<Void> forEachLocal(PreLoadContext context, Unseekables<?, ?> unseekables, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return commandStores.forEach(context, unseekables, minEpoch, maxEpoch, forEach);
    }

    public AsyncChain<Void> forEachLocalSince(PreLoadContext context, Unseekables<?, ?> unseekables, Timestamp since, Consumer<SafeCommandStore> forEach)
    {
        return commandStores.forEach(context, unseekables, since.epoch(), Long.MAX_VALUE, forEach);
    }

    public AsyncChain<Void> ifLocal(PreLoadContext context, RoutingKey key, long epoch, Consumer<SafeCommandStore> ifLocal)
    {
        return commandStores.ifLocal(context, key, epoch, epoch, ifLocal);
    }

    public AsyncChain<Void> ifLocalSince(PreLoadContext context, RoutingKey key, Timestamp since, Consumer<SafeCommandStore> ifLocal)
    {
        return commandStores.ifLocal(context, key, since.epoch(), Long.MAX_VALUE, ifLocal);
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

    public <T> void mapReduceConsumeLocal(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
    {
        commandStores.mapReduceConsume(context, keys, minEpoch, maxEpoch, mapReduceConsume);
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
        shard.nodes.forEach(node -> messageSink.send(node, send, callback));
    }

    private <T> void send(Shard shard, Request send, Set<Id> alreadyContacted)
    {
        shard.nodes.forEach(node -> {
            if (alreadyContacted.add(node))
                send(node, send);
        });
    }

    public <T> void send(Collection<Id> to, Request send)
    {
        to.forEach(dst -> send(dst, send));
    }

    public <T> void send(Collection<Id> to, Function<Id, Request> requestFactory)
    {
        to.forEach(dst -> send(dst, requestFactory.apply(dst)));
    }

    public <T> void send(Collection<Id> to, Request send, Callback<T> callback)
    {
        to.forEach(dst -> send(dst, send, callback));
    }

    public <T> void send(Collection<Id> to, Function<Id, Request> requestFactory, Callback<T> callback)
    {
        to.forEach(dst -> send(dst, requestFactory.apply(dst), callback));
    }

    // send to a specific node
    public <T> void send(Id to, Request send, Callback<T> callback)
    {
        messageSink.send(to, send, callback);
    }

    // send to a specific node
    public void send(Id to, Request send)
    {
        messageSink.send(to, send);
    }

    public void reply(Id replyingToNode, ReplyContext replyContext, Reply send)
    {
        if (send == null)
            throw new NullPointerException();
        messageSink.reply(replyingToNode, replyContext, send);
    }

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
        AsyncResult<Result> result = withEpoch(txnId.epoch(), () -> initiateCoordination(txnId, txn));
        coordinating.putIfAbsent(txnId, result);
        result.addCallback((success, fail) -> coordinating.remove(txnId, result));
        return result;
    }

    private AsyncResult<Result> initiateCoordination(TxnId txnId, Txn txn)
    {
        return Coordinate.coordinate(this, txnId, txn, computeRoute(txnId, txn.keys()));
    }

    public FullRoute<?> computeRoute(TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        RoutingKey homeKey = trySelectHomeKey(txnId, keysOrRanges);
        if (homeKey == null)
            homeKey = selectRandomHomeKey(txnId);

        return keysOrRanges.toRoute(homeKey);
    }

    private @Nullable RoutingKey trySelectHomeKey(TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        Ranges owned = topology().localForEpoch(txnId.epoch()).ranges();
        int i = (int)keysOrRanges.findNextIntersection(0, owned, 0);
        return i >= 0 ? keysOrRanges.get(i).someIntersectingRoutingKey(owned) : null;
    }

    public RoutingKey selectProgressKey(TxnId txnId, Route<?> route, RoutingKey homeKey)
    {
        return selectProgressKey(txnId.epoch(), route, homeKey);
    }

    public RoutingKey selectProgressKey(long epoch, Route<?> route, RoutingKey homeKey)
    {
        RoutingKey progressKey = trySelectProgressKey(epoch, route, homeKey);
        if (progressKey == null)
            throw new IllegalStateException();
        return progressKey;
    }

    public RoutingKey trySelectProgressKey(TxnId txnId, Route<?> route)
    {
        return trySelectProgressKey(txnId, route, route.homeKey());
    }

    public RoutingKey trySelectProgressKey(TxnId txnId, Route<?> route, RoutingKey homeKey)
    {
        return trySelectProgressKey(txnId.epoch(), route, homeKey);
    }

    public RoutingKey trySelectProgressKey(long epoch, Route<?> route, RoutingKey homeKey)
    {
        return trySelectProgressKey(this.topology.localForEpoch(epoch), route, homeKey);
    }

    private static RoutingKey trySelectProgressKey(Topology topology, Route<?> route, RoutingKey homeKey)
    {
        if (topology.ranges().contains(homeKey))
            return homeKey;

        int i = (int)route.findNextIntersection(0, topology.ranges(), 0);
        if (i < 0)
            return null;
        return route.get(i).someIntersectingRoutingKey(topology.ranges());
    }

    public RoutingKey selectRandomHomeKey(TxnId txnId)
    {
        Ranges ranges = topology().localForEpoch(txnId.epoch()).ranges();
        Range range = ranges.get(ranges.size() == 1 ? 0 : random.nextInt(ranges.size()));
        return range.someIntersectingRoutingKey(null);
    }

    static class RecoverFuture<T> extends AsyncResults.Settable<T> implements BiConsumer<T, Throwable>
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
        });
        coordinating.putIfAbsent(txnId, result);
        result.addCallback((success, fail) -> coordinating.remove(txnId, result));
        return result;
    }

    // TODO (low priority, API/efficiency): coalesce maybeRecover calls? perhaps have mutable knownStatuses so we can inject newer ones?
    public AsyncResult<? extends Outcome> maybeRecover(TxnId txnId, RoutingKey homeKey, @Nullable Route<?> route, ProgressToken prevProgress)
    {
        AsyncResult<? extends Outcome> result = coordinating.get(txnId);
        if (result != null)
            return result;

        RecoverFuture<Outcome> future = new RecoverFuture<>();
        MaybeRecover.maybeRecover(this, txnId, homeKey, route, prevProgress, future);
        return future;
    }

    public void receive(Request request, Id from, ReplyContext replyContext)
    {
        long unknownEpoch = topology().maxUnknownEpoch(request);
        if (unknownEpoch > 0)
        {
            configService.fetchTopologyForEpoch(unknownEpoch);
            topology().awaitEpoch(unknownEpoch).addListener(() -> receive(request, from, replyContext));
            return;
        }
        scheduler.now(() -> request.process(this, from, replyContext));
    }

    public Scheduler scheduler()
    {
        return scheduler;
    }

    public Agent agent()
    {
        return agent;
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
    public CommandStore unsafeForKey(Key key)
    {
        return commandStores.unsafeForKey(key);
    }

    public CommandStore unsafeByIndex(int index)
    {
        return commandStores.current.shards[index].store;
    }

    public LongSupplier unsafeGetNowSupplier()
    {
        return nowSupplier;
    }
}
