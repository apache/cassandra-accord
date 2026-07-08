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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.api.AsyncExecutor;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.TopologyService;
import accord.api.Tracing;
import accord.coordinate.ExecuteTxn;
import accord.impl.LocalDelivery;
import accord.local.CommandStores.UnrestrictedStoreSelector;
import accord.local.cfk.ExecuteTxnBacklog;
import accord.messages.RemoteSuccess;
import accord.messages.ReplyContext.NoReplyContext;
import accord.primitives.Route;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.topology.ActiveEpoch;
import accord.topology.ActiveEpochs;
import accord.topology.EpochReady;
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
import accord.local.cfk.CommandsForKey;
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
import accord.topology.Topology;
import accord.topology.TopologyException;
import accord.topology.TopologyManager;
import accord.topology.TopologyRetiredException;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import accord.utils.PersistentField.Persister;
import accord.utils.RandomSource;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.api.ProtocolModifiers.defaultMediumPath;
import static accord.api.ProtocolModifiers.ensurePermitted;
import static accord.api.ProtocolModifiers.permitLocalDelivery;
import static accord.api.ProtocolModifiers.usePrivilegedCoordinator;
import static accord.coordinate.Coordination.CoordinationKind.COORDINATES_STATE_MACHINE;
import static accord.coordinate.Coordination.CoordinationKind.Execute;
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

public class Node implements NodeCommandStoreService
{
    public static class Id implements Comparable<Id>
    {
        private static final ConcurrentHashMap<Id, Id> interned = new ConcurrentHashMap<>();
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

        public Id intern()
        {
            return interned.computeIfAbsent(this, Function.identity());
        }
    }

    private final Id id;
    private final MessageSink messageSink;
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

    private final ExecuteTxnBacklog executeBacklogSink;

    /**
     * Used to guard some operations that should normally operate on consistent information, but in rare cases may need to repeat work.
     * For simplicity we have a global stamp counter for this.
     * At present, only used for managing unavailable() computations.
     */
    private volatile long stamp;
    private static final AtomicLongFieldUpdater<Node> stampUpdater = AtomicLongFieldUpdater.newUpdater(Node.class, "stamp");
    private volatile boolean replaying;

    public Node(Id id, MessageSink messageSink,
                TopologyService topologyService, TimeService time, UniqueTimeService uniqueTime,
                Supplier<DataStore> dataSupplier, ShardDistributor shardDistributor, Agent agent, RandomSource random, Scheduler scheduler, TopologySorter.Supplier topologySorter,
                Function<Node, RemoteListeners> remoteListenersFactory, Function<Node, Timeouts> requestTimeoutsFactory, Function<Node, ProgressLog.Factory> progressLogFactory,
                Function<Node, LocalListeners.Factory> localListenersFactory, CommandStores.Factory factory, CoordinationAdapter.Factory coordinationAdapters,
                Persister<DurableBefore, DurableBefore> durableBeforePersister,
                Journal journal)
    {
        this.id = id;
        this.scheduler = scheduler; // we set scheduler first so that e.g. requestTimeoutsFactory and progressLogFactory can take references to it
        this.coordinationAdapters = coordinationAdapters;
        this.time = time;
        this.uniqueTime = uniqueTime;
        this.timeouts = requestTimeoutsFactory.apply(this);
        this.listeners = remoteListenersFactory.apply(this);
        this.agent = agent;
        this.random = random;
        this.persistDurableBefore = new PersistentField<>(() -> durableBefore,
                                                          DurableBefore::merge, DurableBefore::mergeIfDifferent,
                                                          safeDurableBeforePersister(durableBeforePersister),
                                                          this::setPersistedDurableBefore,
                                                          run -> someExecutor().execute(run));
        this.commandStores = factory.create(this, agent, dataSupplier.get(), random.fork(), journal, shardDistributor, progressLogFactory.apply(this), localListenersFactory.apply(this));
        this.topology = new TopologyManager(topologySorter, this, topologyService, time, timeouts);
        this.durabilityService = new DurabilityService(this);
        this.executeBacklogSink = new ExecuteTxnBacklog(this);
        this.messageSink = messageSink;

        // TODO (desired): make frequency configurable
        scheduler.recurring(() -> commandStores.forAllUnsafe(store -> store.progressLog.maybeNotify()), 1, SECONDS);
        scheduler.recurring(timeouts::maybeNotify, 100, MILLISECONDS);
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
     * @return {@link EpochReady#active}
     */
    @VisibleForTesting
    public AsyncResult<Void> unsafeStart()
    {
        topology.topologyService().onStartup(this);
        ActiveEpochs epochs = topology.active();
        if (epochs.isEmpty())
            return AsyncResults.success(null);

        return epochs.epochReady(epochs.epoch()).active;
    }

    public CommandStores commandStores()
    {
        return commandStores;
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
            DurableBefore newDurableBefore = durableBefore.update(ranges, from, from);
            // TODO (required): it is possible for this invariant to be breached if topologies are received out of order.
            //  We should not update min past the max known epoch.
            Invariants.require(newDurableBefore.min.quorum.compareTo(durableBefore.min.quorum) >= 0,
                    "Previous durable before: %s, new: %s", durableBefore, newDurableBefore);

            minDurableBefore = minDurableBefore.update(ranges, from, from);
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
        return markDurable(DurableBefore.create(ranges, new DurableBefore.Entry(majorityBefore, universalBefore)));
    }

    public AsyncResult<?> markDurable(DurableBefore addDurableBefore)
    {
        return withEpochExact(addDurableBefore.maxEpoch(), (AsyncExecutor)null, () -> persistDurableBefore.save(addDurableBefore).chain())
               .beginAsResult();
    }

    @Override
    public long epoch()
    {
        return topology().epoch();
    }

    // TODO (required): audit use of withEpochAtLeast vs withEpochExact
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
        ActiveEpochs epochs = topology().active();
        if (epochs.hasAtLeastEpoch(epoch))
        {
            callback.accept(null, null);
        }
        else
        {
            topology.await(epoch, ifAsync).begin(callback);
        }
    }

    public Object withEpochAtLeast(long epoch, @Nullable AsyncExecutor ifAsync, BiConsumer<?, ? super Throwable> ifFailure, Runnable ifSuccess)
    {
        ActiveEpochs epochs = topology().active();
        if (epochs.hasAtLeastEpoch(epoch))
        {
            ifSuccess.run();
            return ifSuccess;
        }
        else
        {
            return topology.await(epoch, ifAsync).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, fail);
                else ifSuccess.run();
            });
        }
    }

    public void withEpochExact(long epoch, @Nullable AsyncExecutor ifAsync, BiConsumer<?, Throwable> ifFailure, Function<Throwable, Throwable> onFailure, Runnable ifSuccess)
    {
        ActiveEpochs epochs = topology().active();
        if (epoch < epochs.minEpoch())
        {
            ifFailure.accept(null, onFailure.apply(new TopologyRetiredException(epoch, epochs.minEpoch())));
        }
        else if (epochs.hasEpoch(epoch))
        {
            ifSuccess.run();
        }
        else
        {
            topology.await(epoch, ifAsync).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, onFailure.apply(fail));
                else ifSuccess.run();
            });
        }
    }

    @Inline
    public <T> AsyncChain<T> withEpochExact(long epoch, @Nullable AsyncExecutor executor, Supplier<? extends AsyncChain<T>> supplier)
    {
        ActiveEpochs epochs = topology().active();
        if (epoch < epochs.minEpoch())
        {
            return AsyncChains.failure(new TopologyRetiredException(epoch, epochs.minEpoch()));
        }
        else if (epochs.hasEpoch(epoch))
        {
            return supplier.get();
        }
        else
        {
            return topology.await(epoch, executor).flatMapOverride(supplier);
        }
    }

    @Inline
    public <T> AsyncChain<T> withEpochAtLeast(long epoch, @Nullable AsyncExecutor executor, Supplier<? extends AsyncChain<T>> supplier)
    {
        ActiveEpochs epochs = topology().active();
        if (epochs.hasAtLeastEpoch(epoch))
        {
            return supplier.get();
        }
        else
        {
            return topology.await(epoch, executor).flatMapOverride(supplier);
        }
    }

    public void withEpochAtLeast(long epoch, @Nullable AsyncExecutor ifAsync, BiConsumer<?, Throwable> ifFailure, Function<Throwable, Throwable> onFailure, Runnable ifSuccess)
    {
        ActiveEpochs epochs = topology().active();
        if (epochs.hasAtLeastEpoch(epoch))
        {
            ifSuccess.run();
        }
        else
        {
            topology.await(epoch, ifAsync).begin((success, fail) -> {
                if (fail != null) ifFailure.accept(null, onFailure.apply(fail));
                else ifSuccess.run();
            });
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
    public ExclusiveAsyncExecutor someExclusiveExecutor()
    {
        return commandStores.someExclusiveExecutor();
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

    @Override
    public void reportLocalExecution(TxnId txnId, Route<?> route, Ballot ballot, @Nullable Timestamp applyAt, @Nullable Writes writes, Result result)
    {
        if (applyAt != null && writes != null)
        {
            class Finder implements Consumer<Coordination>
            {
                ExecuteTxn coordinator;

                @Override
                public void accept(Coordination coordination)
                {
                    if (coordination.kind() == Execute && coordination instanceof ExecuteTxn)
                        coordinator = (ExecuteTxn) coordination;
                }
            }
            Finder finder = new Finder();
            coordinations().forEach(txnId, finder);
            if (finder.coordinator != null)
                finder.coordinator.onLocalDirectSuccess(applyAt, writes, result);
        }

        if (Ballot.ZERO.equals(ballot) && !txnId.node.equals(id) && agent.reportRemoteSuccess(result))
        {
            if (applyAt != null && applyAt.epoch() == txnId.epoch())
            {
                ActiveEpoch epoch = topology().active().ifExists(txnId.epoch());
                if (epoch != null)
                {
                    Topology.NodeInfo info = epoch.all().nodeInfo(txnId.node);
                    if (info != null && info.ranges.containsAll(route))
                        return;
                }
            }
            send(txnId.node, new RemoteSuccess(txnId, result), null);
        }
        agent.replicaEvents().onLocalExecution(this, txnId, result);
    }

    public void send(Topologies topologies, Request send, @Nullable Tracing tracing)
    {
        SortedArrayList<Node.Id> nodes = topologies.nodes();
        for (int i = 0 ; i < nodes.size() ; ++i)
        {
            Node.Id to = nodes.get(i);
            if (!topologies.isFaulty(nodes.get(i)))
                send(to, send, tracing);
        }
    }

    public void send(Topologies topologies, Function<Id, Request> requestFactory, @Nullable Tracing tracing)
    {
        SortedArrayList<Node.Id> nodes = topologies.nodes();
        for (int i = 0 ; i < nodes.size() ; ++i)
        {
            Node.Id to = nodes.get(i);
            if (!topologies.isFaulty(nodes.get(i)))
                send(to, requestFactory.apply(to), tracing);
        }
    }

    // send to a specific node
    public <T extends Reply> Cancellable send(Id to, Request send, @Nonnull AsyncExecutor executor, Callback<T> callback, @Nullable Tracing tracing)
    {
        maybeTraceRemote(to, send, tracing);
        if (permitLocalDelivery() && to.equals(id)) return new LocalDelivery<>(this, callback).deliver(send);
        else return messageSink.send(to, send, executor, callback);
    }

    // send to a specific node
    public void send(Id to, Request send, @Nullable Tracing tracing)
    {
        maybeTrace(to, send, tracing);
        if (to.equals(id)) send.process(this, to, new NoReplyContext(this, send));
        else messageSink.send(to, send);
    }

    private void maybeTrace(Node.Id to, Request send, @Nullable Tracing tracing)
    {
        if (tracing != null)
        {
            if (to.equals(id)) tracing.trace(null, "Delivering locally: %s", send);
            else tracing.trace(null, "Sending to %s: %s", to, send);
        }
    }

    public void maybeTraceRemote(Node.Id to, Request send, @Nullable Tracing tracing)
    {
        if (tracing != null  && send instanceof MapReduceCommandStores<?, ?> && (tracing = tracing.send()) != null)
        {
            if (to.equals(id)) tracing.trace(null, "Delivering locally: %s", send);
            else tracing.trace(null, "Sending to %s: %s", to, send);
            ((MapReduceCommandStores<?, ?>)send).setTracing(tracing);
        }
    }

    public void reply(Id replyingToNode, ReplyContext replyContext, Reply success, Throwable failure, @Nullable Tracing tracing)
    {
        if (failure != null)
        {
            if (tracing != null)
                tracing.trace(null, "Replying: %s", failure);
            agent.onException(failure);
            if (success != null)
                agent().onException(new IllegalArgumentException(String.format("fail (%s) and send (%s) are both not null", failure, success)));
        }
        else if (success == null)
        {
            NullPointerException e = new NullPointerException();
            agent.onException(e);
            throw e;
        }
        else
        {
            if (tracing != null)
                tracing.trace(null, "Replying: %s", success);
        }
        replyContext.reply(replyingToNode, messageSink, success, failure);
    }

    public TxnId nextTxnIdWithDefaultFlags(Seekables<?, ?> keys, Txn.Kind kind, Domain domain)
    {
        return nextTxnIdWithFlags(keys, kind, domain, Any, defaultMediumPath().bit());
    }

    public TxnId nextStaleTxnIdWithDefaultFlags(long minEpoch, long minHlc, Seekables<?, ?> keys, Txn.Kind kind, Domain domain)
    {
        return nextStaleTxnIdWithFlags(minEpoch, minHlc, keys, kind, domain, Any, defaultMediumPath().bit());
    }

    public TxnId nextTxnIdWithDefaultFlags(Seekables<?, ?> keys, Txn.Kind kind, Domain domain, Cardinality cardinality)
    {
        return nextTxnIdWithFlags(keys, kind, domain, cardinality, defaultMediumPath().bit());
    }

    private long epoch(long minEpoch, Seekables<?, ?> keys, Txn.Kind kind)
    {
        if (!kind.isSyncPoint())
            return Math.max(minEpoch, epoch());

        return topology.active().maxEpoch(minEpoch, ActiveEpoch::all, keys);
    }

    public TxnId nextTxnIdWithDefaultFlags(long minEpoch, long minHlc, Seekables<?, ?> keys, Txn.Kind kind, Domain domain, Cardinality cardinality)
    {
        long epoch = epoch(minEpoch, keys, kind);
        return newTxnId(epoch, uniqueNow(minHlc), kind, domain, cardinality, defaultMediumPath().bit(), id);
    }

    /**
     * TODO (required): Make sure we cannot re-issue the same txnid on startup
     * TODO (required): Don't use new epoch for TxnId until a quorum is ready to coordinate it
     */
    public TxnId nextTxnIdWithFlags(Seekables<?, ?> keys, Txn.Kind kind, Domain domain, Cardinality cardinality, int flags)
    {
        return newTxnId(epoch(Long.MIN_VALUE, keys, kind), uniqueNow(), kind, domain, cardinality, flags, id);
    }

    public TxnId nextStaleTxnIdWithFlags(long minEpoch, long minHlc, Seekables<?, ?> keys, Txn.Kind kind, Domain domain, Cardinality cardinality, int flags)
    {
        long epoch = epoch(minEpoch, keys, kind);
        long hlc = uniqueStale(minHlc);
        return newTxnId(epoch, hlc, kind, domain, cardinality, flags, id);
    }

    private static TxnId newTxnId(long epoch, long now, Txn.Kind kind, Domain domain, Cardinality cardinality, int flags, Node.Id node)
    {
        Invariants.require(domain == Key || kind != Write, "Range writes not supported without forwarding uniqueHlc information to WaitingOn for direct dependencies");
        Invariants.require(domain == Range || !kind.isSyncPoint, "Key ExclusiveSyncPoint not supported without improvements to CommandsForKey for managing execution");
        TxnId txnId = new TxnId(epoch, now, flags, kind, domain, cardinality, node);
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
            return nextTxnIdWithDefaultFlags(minEpoch, minHlc, keys, kind, domain, cardinality);

        long epoch = epoch(minEpoch, keys, kind);
        long hlc = uniqueNow(minHlc);
        int flags = computeBestDefaultTxnIdFlags(keys, epoch);
        TxnId txnId = new TxnId(epoch, hlc, flags, kind, domain, cardinality, id);
        Invariants.require((txnId.lsb & (0xffff & ~TxnId.IDENTITY_FLAGS)) == 0);
        return txnId;
    }

    private int computeBestDefaultTxnIdFlags(Routables<?> keys, long epoch)
    {
        ActiveEpochs epochs = topology().active();
        if (!epochs.hasEpoch(epoch) || !usePrivilegedCoordinator())
            return defaultMediumPath().bit();

        TxnId.FastPath fastPath = ensurePermitted(epochs.selectFastPath(keys, epoch));
        return fastPath.bits | defaultMediumPath().bit();
    }

    public TxnId nextTxnId(Txn txn, TxnId.FastPath fastPath, TxnId.MediumPath mediumPath)
    {
        ActiveEpochs epochs = topology().active();
        Seekables<?, ?> keys = txn.keys();
        Txn.Kind kind = txn.kind();
        Domain domain = keys.domain();

        long epoch = epoch(Long.MIN_VALUE, keys, kind);
        long now = uniqueNow();
        fastPath = ensurePermitted(fastPath);
        if (fastPath != Unoptimised && (!epochs.hasEpoch(epoch) || !epochs.supportsPrivilegedFastPath(keys, epoch)))
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
        if (txnId.kind() == Txn.Kind.EphemeralRead)
            return CoordinateEphemeralRead.coordinate(this, txnId, txn);
        else
            return CoordinateTransaction.coordinate(this, txnId, txn);
    }

    public FullRoute<?> computeRoute(TxnId txnId, Routables<?> keysOrRanges) throws TopologyException
    {
        return computeRoute(txnId.epoch(), keysOrRanges, topology.active());
    }

    public FullRoute<?> computeRoute(long epoch, Routables<?> keysOrRanges, ActiveEpochs active) throws TopologyException
    {
        Invariants.requireArgument(!keysOrRanges.isEmpty(), "Attempted to compute a route from empty keys or ranges");

        RoutingKey homeKey = selectHomeKey(active.get(epoch), keysOrRanges);

        return keysOrRanges.toRoute(homeKey);
    }

    private RoutingKey selectHomeKey(ActiveEpoch e, Routables<?> keysOrRanges)
    {
        Ranges owned = e.local().ranges();
        int i = (int)keysOrRanges.findNextIntersection(0, owned, 0);
        if (i >= 0)
            return keysOrRanges.get(i).someIntersectingRoutingKey(owned);

        return keysOrRanges.get(random.nextInt(keysOrRanges.size())).someIntersectingRoutingKey(null);
    }

    public AsyncChain<? extends Outcome> recover(TxnId txnId, InvalidIf invalidIf, FullRoute<?> route, LatentStoreSelector reportTo)
    {
        ExclusiveAsyncExecutor executor = someExclusiveExecutor();
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
                reply(from, replyContext, null, t, null);
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
        UnrestrictedStoreSelector selector = CommandStores.StoreSelection::allOf;
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

    public ExecuteTxnBacklog executeBacklogSink()
    {
        return executeBacklogSink;
    }

    public boolean isCoordinatingWithBallot(TxnId txnId, Ballot ballot)
    {
        long mostRecent = coordinations.mostRecent(txnId, COORDINATES_STATE_MACHINE, ballot);
        if (mostRecent < 0)
            return false;
        long ageNanos = Math.max(recentElapsed(NANOSECONDS) - mostRecent, 0);
        return !agent.isSlowCoordinator(ageNanos, NANOSECONDS, txnId, 1);
    }

    public void updateStamp()
    {
        stampUpdater.incrementAndGet(this);
    }

    @Override
    public boolean isReplaying()
    {
        return replaying;
    }

    public void unsafeSetReplaying(boolean replaying)
    {
        this.replaying = replaying;
        if (replaying) CommandsForKey.disableLinearizabilityViolationsReporting();
        else CommandsForKey.enableLinearizabilityViolationsReporting();
    }
}