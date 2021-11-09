package accord.local;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import accord.api.*;
import accord.coordinate.Coordinate;
import accord.messages.Callback;
import accord.messages.Request;
import accord.messages.Reply;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyTracker;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;

public class Node implements ConfigurationService.Listener
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

    public static int numCommandShards()
    {
        return 8; // TODO: make configurable
    }

    private final CommandStores commandStores;
    private final Id id;
    private final MessageSink messageSink;
    private final ConfigurationService configurationService;
    private final TopologyTracker topologyTracker;
    private final Random random;

    private final LongSupplier nowSupplier;
    private final AtomicReference<Timestamp> now;
    private final Agent agent;

    // TODO: this really needs to be thought through some more, as it needs to be per-instance in some cases, and per-node in others
    private final Scheduler scheduler;

    private final Map<TxnId, CompletionStage<Result>> coordinating = new ConcurrentHashMap<>();
    private final Set<TxnId> pendingRecovery = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public Node(Id id, MessageSink messageSink, ConfigurationService configurationService, Random random, LongSupplier nowSupplier,
                Supplier<Store> dataSupplier, Agent agent, Scheduler scheduler, CommandStore.Factory commandStoreFactory)
    {
        this.id = id;
        this.random = random;
        this.agent = agent;
        this.messageSink = messageSink;
        this.configurationService = configurationService;
        this.topologyTracker = new TopologyTracker();
        Topology topology = configurationService.currentTopology();
        this.now = new AtomicReference<>(new Timestamp(topology.epoch(), nowSupplier.getAsLong(), 0, id));
        this.nowSupplier = nowSupplier;
        this.scheduler = scheduler;
        this.commandStores = new CommandStores(numCommandShards(),
                                               id,
                                               configurationService,
                                               this::uniqueNow,
                                               agent,
                                               dataSupplier.get(),
                                               commandStoreFactory, topologyTracker);

        configurationService.registerListener(this);
        onTopologyUpdate(topology);
    }

    public ConfigurationService configurationService()
    {
        return configurationService;
    }

    public void maybeReportEpoch(long epoch)
    {
        if (epoch > configurationService.currentEpoch())
            configurationService.fetchTopologyForEpoch(epoch);
    }

    @Override
    public synchronized void onTopologyUpdate(Topology topology)
    {
        if (topology.epoch() <= topologyTracker.epoch())
            return;
        commandStores.updateTopology(topology);
        topologyTracker.onTopologyUpdate(topology);
        configurationService.acknowledgeEpoch(topology.epoch());
    }

    @Override
    public void onEpochAcknowledgement(Id node, long epoch)
    {
        topologyTracker.onEpochAcknowledgement(node, epoch);
    }

    @Override
    public void onEpochSyncComplete(Id node, long epoch)
    {
        topologyTracker.onEpochSyncComplete(node, epoch);
    }

    public TopologyTracker topologyTracker()
    {
        return topologyTracker;
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
            long epoch = Math.max(cur.epoch, topologyTracker.epoch());
            return (now > cur.real)
                 ? new Timestamp(epoch, now, 0, id)
                 : new Timestamp(epoch, cur.real, cur.logical + 1, id);
        });
    }

    public Timestamp uniqueNow(Timestamp atLeast)
    {
        if (now.get().compareTo(atLeast) < 0)
            now.accumulateAndGet(atLeast, (current, proposed) -> {
                Timestamp timestamp = proposed.compareTo(current) <= 0
                        ? new Timestamp(current.epoch, current.real, current.logical + 1, id)
                        : proposed;
                return timestamp.withMinEpoch(topologyTracker.epoch());
            });
        return uniqueNow();
    }

    public long now()
    {
        return nowSupplier.getAsLong();
    }

    public Stream<CommandStore> local(Keys keys)
    {
        return commandStores.forKeys(keys);
    }

    public Stream<CommandStore> local()
    {
        return commandStores.stream();
    }

    public Optional<CommandStore> local(Key key)
    {
        return local(Keys.of(key)).reduce((i1, i2) -> {
            throw new IllegalStateException("more than one instance encountered for key");
        });
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

    private <T> void send(Shard shard, Request send, Set<Id> alreadyContacted)
    {
        shard.nodes.forEach(node -> {
            if (alreadyContacted.add(node))
                send(node, send);
        });
    }

    public <T> void send(Collection<Id> to, Request send)
    {
        for (Id dst: to)
            send(dst, send);
    }

    public <T> void send(Collection<Id> to, Request send, Callback<T> callback)
    {
        for (Id dst: to)
            send(dst, send, callback);
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

    public void reply(Id replyingToNode, long replyingToMessage, Reply send)
    {
        messageSink.reply(replyingToNode, replyingToMessage, send);
    }

    public TxnId nextTxnId()
    {
        return new TxnId(uniqueNow());
    }

    public CompletionStage<Result> coordinate(TxnId txnId, Txn txn)
    {
        CompletionStage<Result> result = Coordinate.execute(this, txnId, txn);
        coordinating.put(txnId, result);
        result.handle((success, fail) ->
                      {
                          coordinating.remove(txnId);
                          // if we don't succeed, try again in 30s to make sure somebody finishes it
                          // TODO: this is an ugly liveness mechanism
                          if (fail != null && pendingRecovery.add(txnId))
                              scheduler.once(() -> { pendingRecovery.remove(txnId); recover(txnId, txn); } , 30L, TimeUnit.SECONDS);
                          return null;
                      });
        return result;
    }

    public CompletionStage<Result> coordinate(Txn txn)
    {
        return coordinate(nextTxnId(), txn);
    }

    // TODO: encapsulate in Coordinate, so we can request that e.g. commits be re-sent?
    public CompletionStage<Result> recover(TxnId txnId, Txn txn)
    {
        CompletionStage<Result> result = coordinating.get(txnId);
        if (result != null)
            return result;

        result = Coordinate.recover(this, txnId, txn);
        coordinating.putIfAbsent(txnId, result);
        result.handle((success, fail) -> {
            coordinating.remove(txnId);
            agent.onRecover(this, success, fail);
            // if we don't succeed, try again in 30s to make sure somebody finishes it
            // TODO: this is an ugly liveness mechanism
            if (fail != null && pendingRecovery.add(txnId))
                scheduler.once(() -> { pendingRecovery.remove(txnId); recover(txnId, txn); } , 30L, TimeUnit.SECONDS);
            return null;
        });
        return result;
    }

    public void receive(Request request, Id from, long messageId)
    {
        scheduler.now(() -> request.process(this, from, messageId));
    }

    public Scheduler scheduler()
    {
        return scheduler;
    }

    public Random random()
    {
        return random;
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
}
