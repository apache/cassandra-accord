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
import accord.utils.MapReduceConsume;
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
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

public class Node implements ConfigurationService.Listener, NodeTimeService
{
    public static class Id implements Comparable<Id>
    {
        public static final Id NONE = new Id(0);
        public static final Id MAX = new Id(Long.MAX_VALUE);

        public final long id;

        public Id(long id)
        {
            this.id = id;
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(id);
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
            return Long.compare(this.id, that.id);
        }

        public String toString()
        {
            return Long.toString(id);
        }
    }

    public boolean isCoordinating(TxnId txnId, Ballot promised)
    {
        return promised.node.equals(id) && coordinating.containsKey(txnId);
    }

    public static int numCommandShards()
    {
        return 8; // TODO: make configurable
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

    // TODO: this really needs to be thought through some more, as it needs to be per-instance in some cases, and per-node in others
    private final Scheduler scheduler;

    // TODO (soon): monitor the contents of this collection for stalled coordination, and excise them
    private final Map<TxnId, Future<? extends Outcome>> coordinating = new ConcurrentHashMap<>();

    public Node(Id id, MessageSink messageSink, ConfigurationService configService, LongSupplier nowSupplier,
                Supplier<DataStore> dataSupplier, Agent agent, Random random, Scheduler scheduler, TopologySorter.Supplier topologySorter,
                Function<Node, ProgressLog.Factory> progressLogFactory, CommandStores.Factory factory)
    {
        this.id = id;
        this.messageSink = messageSink;
        this.configService = configService;
        this.topology = new TopologyManager(topologySorter, id);
        this.nowSupplier = nowSupplier;
        Topology topology = configService.currentTopology();
        this.now = new AtomicReference<>(new Timestamp(topology.epoch(), nowSupplier.getAsLong(), 0, id));
        this.agent = agent;
        this.random = random;
        this.scheduler = scheduler;
        this.commandStores = factory.create(numCommandShards(), this, agent, dataSupplier.get(), progressLogFactory.apply(this));

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
            topology.awaitEpoch(epoch).addListener(runnable);
        }
    }

    public <T> Future<T> withEpoch(long epoch, Supplier<Future<T>> supplier)
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
    }

    public Timestamp uniqueNow()
    {
        return now.updateAndGet(cur -> {
            // TODO: this diverges from proof; either show isomorphism or make consistent
            long now = nowSupplier.getAsLong();
            long epoch = Math.max(cur.epoch, topology.epoch());
            return (now > cur.real)
                 ? new Timestamp(epoch, now, 0, id)
                 : new Timestamp(epoch, cur.real, cur.logical + 1, id);
        });
    }

    public Timestamp uniqueNow(Timestamp atLeast)
    {
        if (now.get().compareTo(atLeast) < 0)
            now.accumulateAndGet(atLeast, (current, proposed) -> {
                long minEpoch = topology.epoch();
                current = current.withMinEpoch(minEpoch);
                proposed = proposed.withMinEpoch(minEpoch);
                return proposed.compareTo(current) <= 0 ? current.logicalNext(id) : proposed;
            });
        return uniqueNow();
    }

    public long now()
    {
        return nowSupplier.getAsLong();
    }

    public Future<Void> forEachLocal(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, Consumer<SafeCommandStore> forEach)
    {
        return commandStores.forEach(context, keys, minEpoch, maxEpoch, forEach);
    }

    public Future<Void> forEachLocalSince(PreLoadContext context, AbstractKeys<?, ?> keys, Timestamp since, Consumer<SafeCommandStore> forEach)
    {
        return commandStores.forEach(context, keys, since.epoch, Long.MAX_VALUE, forEach);
    }

    public Future<Void> ifLocal(PreLoadContext context, RoutingKey key, long epoch, Consumer<SafeCommandStore> ifLocal)
    {
        return commandStores.ifLocal(context, key, epoch, epoch, ifLocal);
    }

    public Future<Void> ifLocalSince(PreLoadContext context, RoutingKey key, Timestamp since, Consumer<SafeCommandStore> ifLocal)
    {
        return commandStores.ifLocal(context, key, since.epoch, Long.MAX_VALUE, ifLocal);
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

    public <T> void mapReduceConsumeLocal(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<SafeCommandStore, T> mapReduceConsume)
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

    public TxnId nextTxnId()
    {
        return new TxnId(uniqueNow());
    }

    public Future<Result> coordinate(Txn txn)
    {
        return coordinate(nextTxnId(), txn);
    }

    public Future<Result> coordinate(TxnId txnId, Txn txn)
    {
        // TODO: The combination of updating the epoch of the next timestamp with epochs we don't have topologies for,
        //  and requiring preaccept to talk to its topology epoch means that learning of a new epoch via timestamp
        //  (ie not via config service) will halt any new txns from a node until it receives this topology
        Future<Result> result = withEpoch(txnId.epoch, () -> initiateCoordination(txnId, txn));
        coordinating.putIfAbsent(txnId, result);
        // TODO: if we fail, nominate another node to try instead
        result.addCallback((success, fail) -> coordinating.remove(txnId, result));
        return result;
    }

    private Future<Result> initiateCoordination(TxnId txnId, Txn txn)
    {
        return Coordinate.coordinate(this, txnId, txn, computeRoute(txnId, txn.keys()));
    }

    public Route computeRoute(TxnId txnId, Keys keys)
    {
        RoutingKey homeKey = trySelectHomeKey(txnId, keys);
        if (homeKey == null)
            homeKey = selectRandomHomeKey(txnId);

        return keys.toRoute(homeKey);
    }

    private @Nullable RoutingKey trySelectHomeKey(TxnId txnId, Keys keys)
    {
        int i = topology().localForEpoch(txnId.epoch).ranges().findFirstKey(keys);
        return i >= 0 ? keys.get(i).toRoutingKey() : null;
    }

    public RoutingKey selectProgressKey(long epoch, AbstractKeys<?, ?> keys, RoutingKey homeKey)
    {
        RoutingKey progressKey = trySelectProgressKey(epoch, keys, homeKey);
        if (progressKey == null)
            throw new IllegalStateException();
        return progressKey;
    }

    public RoutingKey trySelectProgressKey(TxnId txnId, AbstractRoute route)
    {
        return trySelectProgressKey(txnId, route, route.homeKey);
    }

    public RoutingKey trySelectProgressKey(TxnId txnId, AbstractKeys<?, ?> keys, RoutingKey homeKey)
    {
        return trySelectProgressKey(txnId.epoch, keys, homeKey);
    }

    public RoutingKey trySelectProgressKey(long epoch, AbstractKeys<?, ?> keys, RoutingKey homeKey)
    {
        return trySelectProgressKey(this.topology.localForEpoch(epoch), keys, homeKey);
    }

    private static RoutingKey trySelectProgressKey(Topology topology, AbstractKeys<?, ?> keys, RoutingKey homeKey)
    {
        if (topology.ranges().contains(homeKey))
            return homeKey;

        int i = topology.ranges().findFirstKey(keys);
        if (i < 0)
            return null;
        return keys.get(i).toRoutingKey();
    }

    public RoutingKey selectRandomHomeKey(TxnId txnId)
    {
        KeyRanges ranges = topology().localForEpoch(txnId.epoch).ranges();
        KeyRange range = ranges.get(ranges.size() == 1 ? 0 : random.nextInt(ranges.size()));
        return range.endInclusive() ? range.end() : range.start();
    }

    static class RecoverFuture<T> extends AsyncFuture<T> implements BiConsumer<T, Throwable>
    {
        @Override
        public void accept(T success, Throwable fail)
        {
            if (fail != null) tryFailure(fail);
            else trySuccess(success);
        }
    }

    public Future<? extends Outcome> recover(TxnId txnId, Route route)
    {
        {
            Future<? extends Outcome> result = coordinating.get(txnId);
            if (result != null)
                return result;
        }

        Future<Outcome> result = withEpoch(txnId.epoch, () -> {
            RecoverFuture<Outcome> future = new RecoverFuture<>();
            RecoverWithRoute.recover(this, txnId, route, future);
            return future;
        });
        coordinating.putIfAbsent(txnId, result);
        result.addCallback((success, fail) -> {
            coordinating.remove(txnId, result);
            // TODO: if we fail, nominate another node to try instead
        });
        return result;
    }

    // TODO: coalesce other maybeRecover calls also? perhaps have mutable knownStatuses so we can inject newer ones?
    public Future<? extends Outcome> maybeRecover(TxnId txnId, RoutingKey homeKey, @Nullable AbstractRoute route, ProgressToken prevProgress)
    {
        Future<? extends Outcome> result = coordinating.get(txnId);
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
        return commandStores.current.ranges[0].shards[index];
    }

    public LongSupplier unsafeGetNowSupplier()
    {
        return nowSupplier;
    }
}
