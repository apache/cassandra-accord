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

package accord.impl.basic;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.AsyncExecutor;
import accord.api.Journal;
import accord.api.MessageSink;
import accord.api.OwnershipEventListener;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.api.Scheduler.Scheduled;
import accord.burn.BurnTestTopologyService;
import accord.burn.TopologyUpdates;
import accord.burn.random.FrequentLargeRange;
import accord.coordinate.CoordinationAdapter;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.impl.DefaultTimeouts;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStore.GlobalCommand;
import accord.impl.MessageListener;
import accord.impl.PrefixedIntHashKey;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TopologyFactory;
import accord.impl.basic.DelayedCommandStores.DelayedCommandStore;
import accord.impl.basic.TestProgressLogs.TestProgressLog;
import accord.impl.list.ListAgent;
import accord.impl.list.ListStore;
import accord.local.Catchup;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.LogUnavailableException;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.RedundantBefore;
import accord.local.ShardDistributor;
import accord.local.StoreParticipants;
import accord.local.TimeService;
import accord.local.UniqueTimeService.AtomicUniqueTimeWithStaleReservation;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.Serialize;
import accord.local.durability.DurabilityService;
import accord.messages.Callback;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.Request;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.topology.TopologyRandomizer;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.LazyToString;
import accord.utils.QueueScheduler;
import accord.utils.RandomSource;
import accord.utils.ReflectionUtils;
import accord.utils.Timestamped;
import accord.utils.TriFunction;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.agrona.collections.Int2ObjectHashMap;

import static accord.impl.basic.Cluster.OverrideLinksKind.NONE;
import static accord.impl.basic.Cluster.OverrideLinksKind.RANDOM_BIDIRECTIONAL;
import static accord.impl.basic.NodeSink.Action.DELIVER;
import static accord.impl.basic.NodeSink.Action.DROP;
import static accord.local.Cleanup.EXPUNGE;
import static accord.local.Cleanup.INVALIDATE;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.Command.NotDefined.uninitialised;
import static accord.local.StoreParticipants.Filter.LOAD;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.HIGH;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class Cluster
{
    public static final Logger trace = LoggerFactory.getLogger("accord.impl.basic.Trace");

    public static class Stats
    {
        final Object key;
        int count;

        public Stats(Object key)
        {
            this.key = key;
        }

        public int count() { return count; }
        public String toString() { return key + ": " + count; }
    }

    public static class LinkConfig
    {
        final Function<List<Id>, BiFunction<Id, Id, Link>> overrideLinks;
        final BiFunction<Id, Id, Link> defaultLinks;

        public LinkConfig(Function<List<Id>, BiFunction<Id, Id, Link>> overrideLinks, BiFunction<Id, Id, Link> defaultLinks)
        {
            this.overrideLinks = overrideLinks;
            this.defaultLinks = defaultLinks;
        }
    }

    static class Link
    {
        final Supplier<NodeSink.Action> action;
        final LongSupplier latencyMicros;

        Link(Supplier<NodeSink.Action> action, LongSupplier latencyMicros)
        {
            this.action = action;
            this.latencyMicros = latencyMicros;
        }
    }

    public class ClusterScheduler extends QueueScheduler
    {
        ClusterScheduler(int source)
        {
            super(source, pending);
        }

        public void onDone(Runnable run)
        {
            Cluster.this.onDone(run);
        }
    }

    final Map<MessageType, Stats> statsMap = new HashMap<>();

    final RandomSource random;
    final LinkConfig linkConfig;
    final Function<Id, Node> lookup;
    final PendingQueue pending;
    final Runnable checkFailures;
    final List<Runnable> onDone = new ArrayList<>();
    final Consumer<Packet> responseSink;
    final Map<Id, NodeSink> sinks = new HashMap<>();
    final MessageListener messageListener;
    int clock;
    BiFunction<Id, Id, Link> links;
    long droppedAt;

    public Cluster(RandomSource random, MessageListener messageListener, Supplier<PendingQueue> queueSupplier, Runnable checkFailures, Function<Id, Node> lookup, IntSupplier rf, Consumer<Packet> responseSink)
    {
        this.random = random;
        this.messageListener = messageListener;
        this.pending = queueSupplier.get();
        this.checkFailures = checkFailures;
        this.lookup = lookup;
        this.responseSink = responseSink;
        this.linkConfig = defaultLinkConfig(random, rf);
        this.links = linkConfig.defaultLinks;
    }

    NodeSink create(Id self, NodeSink.TimeoutSupplier timeouts)
    {
        NodeSink sink = new NodeSink(self, lookup, this, timeouts);
        sinks.put(self, sink);
        return sink;
    }

    void add(Packet packet, long delay, TimeUnit unit)
    {
        MessageType type = packet.message.type();
        if (type != null)
            statsMap.computeIfAbsent(type, Stats::new).count++;
        if (trace.isTraceEnabled())
            trace.trace("{} {} {}", clock++, packet.message instanceof Reply ? "RPLY" : "SEND", packet);
        if (lookup.apply(packet.dst) == null) responseSink.accept(packet);
        else                                  pending.add(packet, delay, unit);

    }

    public void processAll()
    {
        List<Pending> pending = new ArrayList<>();
        {
            // TODO (expected): this doesn't actually process all pending, as any queued tasks on executors aren't processed.
            //   should we perhaps queue them and then process them?
            Pending next;
            while (null != (next = this.pending.poll()))
                pending.add(next);
        }

        for (Pending next : pending)
        {
            Pending.Global.setActiveOrigin(next);
            processNext(next);
            Pending.Global.clearActiveOrigin();
            checkFailures.run();
        }
    }

    boolean hasNonRecurring()
    {
        if (pending.hasNonRecurring())
            return true;

        for (Pending p : pending)
        {
            if (!(p instanceof RecurringPendingRunnable))
                continue;

            RecurringPendingRunnable r = (RecurringPendingRunnable) p.origin();
            if (r.requeue != null && r.requeue.hasNonRecurring())
                return true;
        }

        return false;
    }

    public boolean processPending()
    {
        checkFailures.run();
        // All remaining tasks are recurring
        if (!hasNonRecurring() && pending.nowInMillis() > droppedAt + TimeUnit.MINUTES.toMillis(1L))
            return false;

        Pending next = pending.poll();
        if (next == null)
            return false;

        Pending.Global.setActiveOrigin(next);
        processNext(next);
        Pending.Global.clearActiveOrigin();

        checkFailures.run();
        return true;
    }

    /**
     * Drain tasks that match predicate.
     *
     * Returns whether any tasks were processed
     */
    public boolean drain(Predicate<Pending> process)
    {
        List<Pending> pending = this.pending.drain(process);
        for (Pending p : pending)
            processNext(p);
        return !pending.isEmpty();
    }

    private void processNext(Object next)
    {
        if (next instanceof Packet)
        {
            Packet deliver = (Packet) next;
            Node on = lookup.apply(deliver.dst);

            if (trace.isTraceEnabled())
                trace.trace("{} RECV[{}] {}", clock++, on.epoch(), deliver);

            if (deliver.message instanceof Reply)
            {
                Reply reply = (Reply) deliver.message;
                Callback callback = reply.isFinal()
                                    ? sinks.get(deliver.dst).callbacks.remove(deliver.replyId)
                                    : sinks.get(deliver.dst).callbacks.get(deliver.replyId);

                if (callback != null)
                {
                    if (reply instanceof Reply.FailureReply) callback.onFailure(deliver.src, ((Reply.FailureReply) reply).failure);
                    else callback.onSuccess(deliver.src, reply);
                }
            }

            else on.receive((Request) deliver.message, deliver.src, deliver);
        }
        else
        {
            ((Runnable) next).run();
        }
    }

    public void notifyDropped(Node.Id from, Node.Id to, long id, Message message)
    {
        if (trace.isTraceEnabled())
            trace.trace("{} DROP[{}] (from:{}, to:{}, {}:{}, body:{})", clock++, lookup.apply(to).epoch(), from, to, message instanceof Reply ? "replyTo" : "id", id, message);
        if (!RecurringPendingRunnable.isRecurring(Pending.Global.activeOrigin()))
            droppedAt = pending.nowInMillis();
    }

    public void onDone(Runnable run)
    {
        onDone.add(run);
    }

    // TODO (testing): merge with BurnTest.burn
    public static Map<MessageType, Cluster.Stats> run(Supplier<RandomSource> randomSupplier,
                                                      int[] prefixes,
                                                      List<Node.Id> nodes,
                                                      Topology initialTopology,
                                                      Function<Map<Node.Id, Node>, Request> init)
    {
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        MonitoredPendingQueue queue = new MonitoredPendingQueue(failures, new RandomDelayQueue(randomSupplier.get()));
        Consumer<Runnable> retryBootstrap;
        {
            RandomSource rnd = randomSupplier.get();
            retryBootstrap = retry -> {
                long delay = rnd.nextInt(1, 15);
                queue.add(PendingRunnable.create(retry::run), delay, TimeUnit.SECONDS);
            };
        }
        IntSupplier coordinationDelays, progressDelays, timeoutDelays;
        {
            RandomSource rnd = randomSupplier.get();
            timeoutDelays = progressDelays = coordinationDelays = () -> rnd.nextInt(100, 1000);
        }
        RandomSource nowRandom = randomSupplier.get();
        Supplier<LongSupplier> nowSupplier = () -> {
            RandomSource forked = nowRandom.fork();
            // TODO (testing): meta-randomise scale of clock drift
            return FrequentLargeRange.builder(forked)
                                     .ratio(1, 5)
                                     .small(50, 5000, TimeUnit.MICROSECONDS)
                                     .large(1, 10, TimeUnit.MILLISECONDS)
                                     .build()
                                     .mapAsLong(j -> Math.max(0, queue.nowInMillis() + TimeUnit.NANOSECONDS.toMillis(j)))
                                     .asLongSupplier(forked);
        };
        Supplier<TimeService> timeServiceSupplier = () -> TimeService.ofNonMonotonic(nowSupplier.get(), MILLISECONDS);
        TriFunction<OwnershipEventListener, Scheduler, NodeSink.TimeoutSupplier, Agent> agentSupplier = (ownershipEvents, scheduler, timeoutSupplier) -> new ListAgent(scheduler, randomSupplier.get(), 1000L, failures::add, retryBootstrap, ownershipEvents, coordinationDelays, progressDelays, timeoutDelays, queue::nowInMillis, timeServiceSupplier.get(), timeoutSupplier);
        SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, new ListAgent(null, randomSupplier.get(), 1000L, failures::add, retryBootstrap, OwnershipEventListener.FAIL, timeoutDelays, timeoutDelays, timeoutDelays, queue::nowInMillis, timeServiceSupplier.get(), null), null);
        TopologyFactory topologyFactory = new TopologyFactory(initialTopology.maxRf(), initialTopology.ranges().stream().toArray(Range[]::new))
        {
            @Override
            public Topology toTopology(Node.Id[] cluster)
            {
                return initialTopology;
            }
        };
        AtomicInteger counter = new AtomicInteger();
        AtomicReference<Map<Id, Node>> nodeMap = new AtomicReference<>();
        Map<MessageType, Cluster.Stats> stats = Cluster.run(nodes.toArray(Node.Id[]::new),
                                                            prefixes,
                                                            MessageListener.get(),
                                                            () -> queue,
                                                            (id) -> globalExecutor,
                                                            agentSupplier,
                                                            queue::checkFailures,
                                                            ignore -> {},
                                                            randomSupplier,
                                                            timeServiceSupplier,
                                                            topologyFactory,
                                                            new Supplier<>()
                                                            {
                                                                private Iterator<Request> requestIterator = null;
                                                                private final RandomSource rs = randomSupplier.get();
                                                                @Override
                                                                public Packet get()
                                                                {
                                                                    if (requestIterator == null)
                                                                    {
                                                                        Map<Node.Id, Node> nodes = nodeMap.get();
                                                                        requestIterator = Collections.singleton(init.apply(nodes)).iterator();
                                                                    }
                                                                    if (!requestIterator.hasNext())
                                                                        return null;
                                                                    Node.Id id = rs.pick(nodes);
                                                                    return new Packet(id, id, Long.MAX_VALUE, counter.incrementAndGet(), requestIterator.next(), true);
                                                                }
                                                            },
                                                            Runnable::run,
                                                            nodeMap::set,
                                                            InMemoryJournal::new);
        if (!failures.isEmpty())
        {
            AssertionError error = new AssertionError("Unexpected errors detected");
            failures.forEach(error::addSuppressed);
            throw error;
        }
        return stats;
    }

    public static class RandomLoader
    {
        private final BooleanSupplier cacheEmptyChance;
        private final BooleanSupplier cacheFullChance;
        private final BooleanSupplier commandLoadedChance;
        private final BooleanSupplier cfkLoadedChance;
        private final BooleanSupplier tfkLoadedChance;

        final BooleanSupplier cmdCheckChance;
        final BooleanSupplier cfkCheckChance;

        public static float CMD_BASE_CHECK_CHANCE = 0.01f;
        static int cmdCounter, cfkCounter;

        RandomLoader(RandomSource random)
        {
            this(random.nextBoolean() ? 1.0f : random.nextFloat(),
                 random.nextFloat() * CMD_BASE_CHECK_CHANCE,
                 random.nextFloat() * 0.2f,
                 random);
        }

        RandomLoader(float presentChance, float cmdCheckChance, float cfkCheckChance, RandomSource random)
        {
            this(Gens.supplier(Gens.bools().mixedDistribution().next(random), random),
                 Gens.supplier(Gens.bools().mixedDistribution().next(random), random),
                 random.biasedUniformBools(presentChance),
                 random.biasedUniformBools(presentChance),
                 random.biasedUniformBools(presentChance),
                 random.biasedUniformBools(cmdCheckChance),
                 random.biasedUniformBools(cfkCheckChance)
            );
        }

        RandomLoader(BooleanSupplier cacheEmptyChance, BooleanSupplier cacheFullChance,
                     BooleanSupplier commandLoadedChance, BooleanSupplier cfkLoadedChance, BooleanSupplier tfkLoadedChance,
                     BooleanSupplier cmdCheckChance, BooleanSupplier cfkCheckChance)
        {
            this.cacheEmptyChance = cacheEmptyChance;
            this.cacheFullChance = cacheFullChance;
            this.commandLoadedChance = commandLoadedChance;
            this.cfkLoadedChance = cfkLoadedChance;
            this.tfkLoadedChance = tfkLoadedChance;
            this.cmdCheckChance = cmdCheckChance;
            this.cfkCheckChance = cfkCheckChance;
        }

        public boolean cacheEmpty() { return cacheEmptyChance.getAsBoolean();}
        public boolean cacheFull() { return cacheFullChance.getAsBoolean(); }
        public boolean commandLoaded() { return commandLoadedChance.getAsBoolean(); }
        public boolean cfkLoaded() { return cfkLoadedChance.getAsBoolean(); }
        public boolean tfkLoaded() { return tfkLoadedChance.getAsBoolean(); }

        DelayedCommandStores.CacheLoading newLoader(Journal journal)
        {
            return new DelayedCommandStores.CacheLoading()
            {
                @Override
                public boolean cacheEmpty()
                {
                    return cacheEmptyChance.getAsBoolean();
                }

                @Override
                public boolean cacheFull()
                {
                    return cacheFullChance.getAsBoolean();
                }

                @Override
                public boolean isLoaded(TxnId txnId)
                {
                    return commandLoadedChance.getAsBoolean();
                }

                @Override
                public boolean isLoaded(RoutingKey key)
                {
                    return cfkLoadedChance.getAsBoolean();
                }

                @Override
                public boolean tfkLoaded()
                {
                    return tfkLoadedChance.getAsBoolean();
                }

                @Override
                public void validateRead(CommandStore commandStore, Command command)
                {
                    validate(commandStore, command, false);
                }

                @Override
                public void validateWrite(CommandStore commandStore, Command command)
                {
                    validate(commandStore, command, true);
                }

                public void validate(CommandStore commandStore, Command command, boolean isWrite)
                {
                    if (command.txnId().kind() == Txn.Kind.EphemeralRead
                        || command.saveStatus() == SaveStatus.Uninitialised
                        || command.saveStatus() == SaveStatus.Vestigial
                        || command.saveStatus() == SaveStatus.Erased)
                        return;

                    if (!cmdCheckChance.getAsBoolean())
                        return;

                    ++cmdCounter;
                    command = command.updateParticipants(command.participants().filter(LOAD, commandStore.unsafeGetRedundantBefore(), command.txnId(), command.executeAtIfKnown()));
                    // Journal will not have result persisted. This part is here for test purposes and ensuring that we have strict object equality.
                    Command reconstructed = null;
                    try { reconstructed = journal.loadCommand(commandStore.id(), command.txnId(), commandStore.unsafeGetRedundantBefore(), commandStore.durableBefore()); }
                    catch (LogUnavailableException ignore) {}
                    if (reconstructed == null || reconstructed.saveStatus().hasBeen(Status.Truncated))
                        return;

                    List<ReflectionUtils.Difference<?>> diff = ReflectionUtils.recursiveEquals(command, reconstructed);
                    if (!diff.isEmpty() && command.saveStatus().compareTo(SaveStatus.Erased) >= 0)
                        diff.removeIf(v -> v.path.equals(".participants."));
                    Invariants.require(diff.isEmpty(), "Commands did not match: expected %s, given %s on %s, diff %s", command, reconstructed, commandStore, new LazyToString(() -> String.join("\n", Iterables.transform(diff, Object::toString))));
                }

                @Override
                public void validateRead(CommandStore commandStore, CommandsForKey cfk)
                {
                    if (cfk == null) return;
                    if (cfk.isLoadingPruned()) return;

                    if (!cfkCheckChance.getAsBoolean())
                        return;

                    ++cfkCounter;
                    cfk = cfk.maximalPrune();
                    ByteBuffer encoded = Serialize.toBytesWithoutKey(cfk);
                    CommandsForKey decoded = Serialize.fromBytes(cfk.key(), encoded);
                    Invariants.require(cfk.equalContents(decoded));
                }
            };
        }
    }

    public static Map<MessageType, Stats> run(Id[] nodes, int[] prefixes, MessageListener messageListener, Supplier<PendingQueue> queueSupplier,
                                              Function<Id, AsyncExecutor> nodeExecutorSupplier,
                                              TriFunction<OwnershipEventListener, Scheduler, NodeSink.TimeoutSupplier, Agent> agentSupplier,
                                              Runnable checkFailures, Consumer<Packet> responseSink,
                                              Supplier<RandomSource> randomSupplier,
                                              Supplier<TimeService> timeServiceSupplier,
                                              TopologyFactory topologyFactory, Supplier<Packet> in, Consumer<Runnable> noMoreWorkSignal,
                                              Consumer<Map<Id, Node>> readySignal, BiFunction<Node.Id, RandomSource, Journal> journalFactory)
    {
        Topology topology = topologyFactory.toTopology(nodes);
        Map<Id, Node> nodeMap = new LinkedHashMap<>();
        Map<Id, AsyncExecutor> executorMap = new LinkedHashMap<>();
        Map<Id, Journal> journalMap = new LinkedHashMap<>();
        try
        {
            RandomSource random = randomSupplier.get();
            Cluster sinks = new Cluster(randomSupplier.get(), messageListener, queueSupplier, checkFailures, nodeMap::get, () -> topologyFactory.rf, responseSink);
            TopologyUpdates topologyUpdates = new TopologyUpdates(executorMap::get);
            TopologyRandomizer.Listener schemaApply = t -> {
                for (Node node : nodeMap.values())
                {
                    ListStore store = (ListStore) node.commandStores().dataStore();
                    store.onTopologyUpdate(node, t);
                }
                messageListener.onTopologyChange(t);
            };
            NodeSink.TimeoutSupplier timeouts = new NodeSink.TimeoutSupplier()
            {
                final RandomSource random = randomSupplier.get();
                // TODO (testing): slow/expires should be broadly in sync with our link latency config
                final LongSupplier localDelay, slowDelay, expiresDelay, failsDelay;
                {
                    int medianLocalDelay = random.nextInt(10, 20);
                    int medianSlowDelay = random.nextInt(100, 200);
                    int medianExpiresDelay = random.nextInt(1000, 2000);
                    int medianFailsDelay = random.nextInt(1000, 2000);

                    int minSlowDelay = random.nextInt(0, 100);
                    int minExpiresDelay = random.nextBiasedInt(500, 800, 1000);
                    int minFailsDelay = random.nextBiasedInt(500, 800, 1000);

                    int maxLocalDelay = random.nextBiasedInt(100, 200, 1000);
                    int maxSlowDelay = random.nextBiasedInt(medianSlowDelay + 100, medianSlowDelay + 200, 1000);
                    int maxExpiresDelay = random.nextBiasedInt(medianExpiresDelay + 500, 3000, 10000);
                    int maxFailsDelay = random.nextBiasedInt(medianFailsDelay + 500, 3000, 10000);

                    float localDelayChance = random.nextFloat();
                    LongSupplier innerLocalDelay = random.biasedUniformLongs(0, medianLocalDelay, maxLocalDelay);
                    localDelay = () -> random.decide(localDelayChance) ? innerLocalDelay.getAsLong() : 0;
                    slowDelay = random.biasedUniformLongs(minSlowDelay, medianSlowDelay, maxSlowDelay);
                    expiresDelay = random.biasedUniformLongs(minExpiresDelay, medianExpiresDelay, maxExpiresDelay);
                    failsDelay = random.biasedUniformLongs(minFailsDelay, medianFailsDelay, maxFailsDelay);
                }
                @Override public long localDelay() { return localDelay.getAsLong();}
                @Override public long slowDelay() { return slowDelay.getAsLong();}
                @Override public long expiresDelay() { return expiresDelay.getAsLong(); }
                @Override public long slowAt() { return now() + slowDelay.getAsLong();}
                @Override public long expiresAt() { return now() + expiresDelay.getAsLong(); }
                @Override public long failsAt() { return now() + failsDelay.getAsLong(); }
                @Override public long now() { return sinks.pending.nowInMillis(); }
                @Override public TimeUnit units() { return MILLISECONDS; }
            };
            TopologyRandomizer topologyRandomizer = new TopologyRandomizer(randomSupplier, prefixes, topology, topologyUpdates, nodeMap::get, schemaApply);
            List<DurabilityService> durabilityServices = new ArrayList<>();
            List<Service> services = new ArrayList<>();
            for (Id id : nodes)
            {
                ClusterScheduler scheduler = sinks.new ClusterScheduler(id.id);
                MessageSink messageSink = sinks.create(id, timeouts);
                TimeService timeService = timeServiceSupplier.get();
                OwnershipEventListener ownershipEventListener = topologyRandomizer.listener(id);
                AsyncExecutor nodeExecutor = nodeExecutorSupplier.apply(id);
                Agent agent = agentSupplier.apply(ownershipEventListener, scheduler, timeouts);
                executorMap.put(id, nodeExecutor);
                Journal journal = journalFactory.apply(id, random);
                journalMap.put(id, journal);
                BurnTestTopologyService topologyService = new BurnTestTopologyService(id, nodeExecutor, agent, randomSupplier, topology, nodeMap::get, topologyUpdates);
                DelayedCommandStores.CacheLoading cacheLoading = new RandomLoader(random).newLoader(journal);
                Node node = new Node(id, messageSink, topologyService, timeService, new AtomicUniqueTimeWithStaleReservation(timeService),
                                     () -> new ListStore(scheduler, random, id), new ShardDistributor.EvenSplit<>(8, ignore -> new PrefixedIntHashKey.Splitter()),
                                     agent,
                                     randomSupplier.get(), scheduler, SizeOfIntersectionSorter.SUPPLIER, DefaultRemoteListeners::new, time -> new DefaultTimeouts(time, Runnable::run),
                                     TestProgressLogs::new, DefaultLocalListeners.Factory::new, DelayedCommandStores.factory(sinks.pending, cacheLoading), new CoordinationAdapter.DefaultFactory(),
                                     journal.durableBeforePersister(), journal);
                journal.start(node);
                durabilityServices.add(node.durability());
                nodeMap.put(id, node);
                durabilityServices.add(new DurabilityService(node));
            }

            for (Node node : nodeMap.values())
            {
                node.topology().addListener((ListStore) node.commandStores().dataStore());
                node.topology().addListener(node.durability());
            }

            Runnable updateDurabilityRate;
            {
                IntSupplier targetSplits           = random.biasedUniformIntsSupplier(1, 16,  2,  4, 4, 16).get();
                IntSupplier shardCycleTimeSeconds  = random.biasedUniformIntsSupplier(5, 60, 10, 60, 1, 30).get();
                IntSupplier globalCycleTimeSeconds = random.biasedUniformIntsSupplier(1, 90, 10, 30,10, 60).get();
                updateDurabilityRate = () -> {
                    int c = targetSplits.getAsInt();
                    int s = shardCycleTimeSeconds.getAsInt() * topologyFactory.rf;
                    int g = globalCycleTimeSeconds.getAsInt();
                    durabilityServices.forEach(d -> {
                        d.shards().setTargetShardSplits(c);
                        d.shards().setShardCycleTime(s, SECONDS);
                        d.global().setGlobalCycleTime(g, SECONDS);
                    });
                };
            }
            updateDurabilityRate.run();

            Runnable updateProgressLogConcurrency;
            {
                updateProgressLogConcurrency = () -> {
                    nodeMap.values().forEach(node -> node.commandStores().forAllUnsafe(cs -> cs.execute(() -> {
                        ((TestProgressLog)cs.unsafeProgressLog()).config().concurrency = random.nextInt(8, 32);
                    })));
                };
            }
            updateProgressLogConcurrency.run();

            schemaApply.onUpdate(topology);

            Pending.Global.setNoActiveOrigin();
            AsyncResult<?> startup = AsyncResults.reduce(nodeMap.values().stream().map(Node::unsafeStart).collect(toList()), (a, b) -> null);
            Pending.Global.clearActiveOrigin();

            while (sinks.processPending());
            Invariants.requireArgument(startup.isDone());

            ClusterScheduler clusterScheduler = sinks.new ClusterScheduler(-1);
            List<Id> nodesList = new ArrayList<>(Arrays.asList(nodes));
            Scheduled chaos = clusterScheduler.recurring(() -> {
                sinks.links = sinks.linkConfig.overrideLinks.apply(nodesList);
                if (random.decide(0.1f))
                    updateDurabilityRate.run();
                if (random.decide(0.1f))
                    updateProgressLogConcurrency.run();
            }, 5L, SECONDS);

            Scheduled reconfigure = clusterScheduler.recurring(topologyRandomizer::maybeUpdateTopology, 1, SECONDS);

            Purge purge = new Purge(clusterScheduler, random, nodesList, nodeMap, journalMap);

            Scheduled restart = clusterScheduler.recurring(() -> {
                Id id = pickNodeNotRefusing(random, nodesList, nodeMap);

                Node node = nodeMap.get(id);
                CommandStores stores = node.commandStores();
                stores.forAllUnsafe(CommandStore::cancelBootstraps);
                while (sinks.drain(getPendingPredicate(id, stores.all()))) ;
                topologyRandomizer.rotateBootstrapping(id);

                // TODO (expected): we include too many ranges here, including any that are retired
                Ranges ranges = Ranges.of(Stream.of(node.commandStores().all()).flatMap(cs -> cs.unsafeGetRangesForEpoch().all().stream()).toArray(Range[]::new));
                boolean rebootstrap = topologyFactory.rf > 2 && random.nextBoolean() && !topologyRandomizer.unsafeToRebootstrap(ranges);
                trace.debug(String.format("Triggering %s for node %s",
                                          rebootstrap ? "rebootstrap" : "bounce and journal replay",
                                          id));

                // Clean data and restore from snapshot
                ListStore listStore = (ListStore) node.commandStores().dataStore();
                NavigableMap<RoutableKey, Timestamped<int[]>> prevData = listStore.copyOfCurrentData();
                listStore.clear();

                // We are simulating node restart, so its remote listeners will also be gone
                ((DefaultRemoteListeners) node.remoteListeners()).clear();
                Int2ObjectHashMap<NavigableMap<TxnId, Command>> beforeStores = copyCommands(stores.all());

                Journal journal = journalMap.get(id);

                Journal.TopologyUpdate lastUpdate = null;
                {
                    Iterator<? extends Journal.TopologyUpdate> iter = journal.loadTopologies().iterator();
                    while (iter.hasNext())
                    {
                        Journal.TopologyUpdate update = iter.next();
                        Invariants.require(lastUpdate == null || update.global.epoch() > lastUpdate.global.epoch());
                        lastUpdate = update;
                    }
                }

                for (CommandStore store : stores.all())
                    store.unsafeClearForTesting();

                if (rebootstrap)
                {
                    node.durability().stop();

                    // make sure to flush anything in flight before truncating journal
                    ((InMemoryJournal)journal).dropAll();
                    if (lastUpdate != null)
                        node.commandStores().resetTopology(lastUpdate);

                    // TODO (expected): we seem to hit Log exceptions when rebootstrapping, suggesting we are handling them poorly
                    topologyRandomizer.markRebootstrapping(node);
                    stores.rebootstrap(node).invoke(node.agent());
                    Catchup.catchup(node);

                    while (sinks.drain(getPendingPredicate(id, stores.all())));

                    node.durability().start();
                }
                else
                {
                    node.unsafeSetReplaying(true);
                    if (lastUpdate != null)
                        ((DelayedCommandStores) node.commandStores()).validateShardStateForTesting(lastUpdate);

                    listStore.restore();
                    for (CommandStore store : stores.all())
                        ((ListAgent) store.agent()).restore((InMemoryCommandStore) store);
                    journal.replay(stores, null);
                    Catchup.catchup(node);

                    // Re-enable safety checks
                    while (sinks.drain(getPendingPredicate(id, stores.all()))) ;
                    node.unsafeSetReplaying(false);
                    verifyConsistentRestore(beforeStores, stores.all());
                    stores.forAllUnsafe(commandStore -> commandStore.resumeBootstrap(node));
                    // we can get ahead of prior state by executing further if we skip some earlier phase's dependencies
                    listStore.checkAtLeast(stores, prevData);
                }
                trace.debug("Done with replay.");
            }, () -> random.nextInt(10, 30), SECONDS);

            durabilityServices.forEach(DurabilityService::start);
            services.forEach(Service::start);

            Runnable stop = () -> {
                reconfigure.cancel();
                durabilityServices.forEach(DurabilityService::stop);
                purge.cancel();
                restart.cancel();
                services.forEach(Service::close);
                chaos.cancel();
                sinks.links = sinks.linkConfig.defaultLinks;
            };
            noMoreWorkSignal.accept(stop);
            readySignal.accept(nodeMap);

            Packet next;
            while ((next = in.get()) != null)
                sinks.add(next, 0, TimeUnit.NANOSECONDS);

            while (sinks.processPending());

            stop.run();

            // give progress log et al a chance to finish
            // TODO (desired, testing): would be nice to make this more certain than an arbitrary number of additional rounds
            for (int i = 0 ; i < 10 ; ++i)
            {
                sinks.processAll();
                while (sinks.processPending());
            }

            while (!sinks.onDone.isEmpty())
            {
                List<Runnable> onDone = new ArrayList<>(sinks.onDone);
                sinks.onDone.clear();
                onDone.forEach(Runnable::run);
                while (sinks.processPending());
            }

            return sinks.statsMap;
        }
        finally
        {
            nodeMap.values().forEach(Node::shutdown);
        }
    }

    private static class Purge
    {
        Scheduled scheduled;

        Purge(Scheduler clusterScheduler, RandomSource rs, List<Id> nodes, Map<Id, Node> nodeMap, Map<Id, Journal> journalMap)
        {
            schedule(clusterScheduler, rs, nodes, nodeMap, journalMap);
        }

        void cancel()
        {
            scheduled.cancel();
        }

        private void schedule(Scheduler clusterScheduler, RandomSource rs, List<Id> nodes, Map<Id, Node> nodeMap, Map<Id, Journal> journalMap)
        {
            scheduled = clusterScheduler.selfRecurring(() -> run(clusterScheduler, rs, nodes, nodeMap, journalMap), rs.nextInt(1, 2), SECONDS);
        }

        private void run(Scheduler clusterScheduler, RandomSource rs, List<Id> nodes, Map<Id, Node> nodeMap, Map<Id, Journal> journalMap)
        {
            Id id = rs.pick(nodes);
            Node node = nodeMap.get(id);

            Journal journal = journalMap.get(node.id());
            CommandStores stores = nodeMap.get(node.id()).commandStores();
            // run on node scheduler so doesn't run during replay
            scheduled = node.scheduler().selfRecurring(() -> {
                journal.purge(stores, node.topology()::minEpoch);
                schedule(clusterScheduler, rs, nodes, nodeMap, journalMap);
            }, 0, SECONDS);
        }

    }

    private static Int2ObjectHashMap<NavigableMap<TxnId, Command>> copyCommands(CommandStore[] stores)
    {
        Int2ObjectHashMap<NavigableMap<TxnId, Command>> result = new Int2ObjectHashMap<>();
        for (CommandStore s : stores)
        {
            DelayedCommandStores.DelayedCommandStore store = (DelayedCommandStores.DelayedCommandStore) s;
            NavigableMap<TxnId, Command> commands = new TreeMap<>();
            result.put(store.id(), commands);
            for (Map.Entry<TxnId, GlobalCommand> e : store.unsafeCommands().entrySet())
            {
                Command command = e.getValue().value();
                Invariants.require(command.saveStatus() != SaveStatus.Uninitialised,
                                   "Found uninitialized command in the log: %s", command);
                commands.put(e.getKey(), command);
            }

        }
        return result;
    }

    private static Id pickNodeNotRefusing(RandomSource random, List<Id> ids, Map<Id, Node> nodeMap)
    {
        List<Id> remaining = new ArrayList<>(ids);
        while (!remaining.isEmpty())
        {
            int i = random.nextInt(remaining.size());
            Id id = remaining.get(i);
            CommandStore[] stores = nodeMap.get(id).commandStores().all();
            if (!Stream.of(stores).anyMatch(CommandStore::unsafeIsRefusingAny))
                return id;

            remaining.set(i, remaining.get(remaining.size() - 1));
            remaining.remove(remaining.size() - 1);
        }
        return null;
    }

    private static void verifyConsistentRestore(Int2ObjectHashMap<NavigableMap<TxnId, Command>> beforeStores, CommandStore[] stores)
    {
        for (CommandStore s : stores)
        {
            DelayedCommandStores.DelayedCommandStore store = (DelayedCommandStores.DelayedCommandStore) s;
            NavigableMap<TxnId, Command> before = beforeStores.get(store.id());
            for (Map.Entry<TxnId, GlobalCommand> e : store.unsafeCommands().entrySet())
            {
                Command beforeCommand = before.get(e.getKey());
                Command afterCommand = e.getValue().value();
                if (beforeCommand == null)
                {
                    Invariants.require(afterCommand.is(Status.NotDefined) || afterCommand.saveStatus().compareTo(SaveStatus.Vestigial) >= 0);
                    continue;
                }
                if (afterCommand.hasBeen(Status.Truncated))
                {
                    if (afterCommand.is(Status.Invalidated))
                        Invariants.require(beforeCommand.hasBeen(Status.Truncated) || (!beforeCommand.hasBeen(Status.PreCommitted)
                               && Cleanup.shouldCleanup(FULL, e.getKey(), beforeCommand.executeAtIfKnown(), beforeCommand.saveStatus(), beforeCommand.durability(), afterCommand.participants(), store.unsafeGetRedundantBefore(), store.durableBefore()).compareTo(INVALIDATE) >= 0));
                    continue;
                }
                if (beforeCommand.hasBeen(Status.Truncated))
                {
                    Invariants.require(!beforeCommand.is(Status.Invalidated) || afterCommand.is(Status.Invalidated));
                    Invariants.require(beforeCommand.is(Status.Invalidated) || afterCommand.is(Status.Truncated) || afterCommand.is(Status.Applied));
                    continue;
                }
                Invariants.require(isConsistent(beforeCommand.saveStatus(), afterCommand.saveStatus()),
                                   "%s != %s", beforeCommand.saveStatus(), afterCommand.saveStatus());
                Invariants.require(beforeCommand.executeAtOrTxnId().equals(afterCommand.executeAtOrTxnId()),
                                   "%s != %s", beforeCommand.executeAtOrTxnId(), afterCommand.executeAtOrTxnId());
                Invariants.require(beforeCommand.acceptedOrCommitted().equals(afterCommand.acceptedOrCommitted()),
                                   "%s != %s", beforeCommand.acceptedOrCommitted(), afterCommand.acceptedOrCommitted());
                Invariants.require(beforeCommand.promised().equals(afterCommand.promised()),
                                   "%s != %s", beforeCommand.promised(), afterCommand.promised());
                Invariants.require(beforeCommand.durability().equals(afterCommand.durability()),
                                   "%s != %s", beforeCommand.durability(), afterCommand.durability());
            }

            if (before.size() > store.unsafeCommands().size())
            {
                for (Map.Entry<TxnId, Command> entry : before.entrySet())
                {
                    TxnId txnId = entry.getKey();
                    if (!store.unsafeCommands().containsKey(txnId))
                    {
                        Command beforeCommand = entry.getValue();
                        if (beforeCommand.saveStatus() == SaveStatus.Erased)
                            continue;

                        if (Cleanup.shouldCleanup(FULL, beforeCommand, store.unsafeGetRedundantBefore(), store.durableBefore()) == EXPUNGE)
                            continue;

                        if (store.unsafeGetRedundantBefore().min(beforeCommand.participants().owns(), RedundantBefore.Bounds::shardAndLocallyRedundantBefore).compareTo(txnId) > 0)
                            continue;

                        if (beforeCommand.participants().owns().isEmpty() && store.durableBefore().min(txnId).compareTo(Status.Durability.HasOutcome.Quorum) >= 0)
                            continue;

                        if (!beforeCommand.saveStatus().hasBeen(Status.PreCommitted) && store.unsafeGetRedundantBefore().min(beforeCommand.participants().owns(), RedundantBefore.Bounds::locallyRedundantBefore).compareTo(txnId) > 0)
                            continue;

                        Invariants.require(false, "Found a command in an unexpected state: %s", beforeCommand);
                    }
                }
            }
        }
    }

    private static boolean isConsistent(SaveStatus before, SaveStatus after)
    {
        if (before == after)
            return true;

        if (before == SaveStatus.Uninitialised || before == SaveStatus.NotDefined)
            return after == SaveStatus.Uninitialised || after == SaveStatus.NotDefined;

        // depending on arrival order, an unmanaged txn may be ready to execute immediately or have to wait for another transaction to commit
        if (before == SaveStatus.Stable || before == SaveStatus.ReadyToExecute)
            return after == SaveStatus.Stable || after == SaveStatus.ReadyToExecute || after == SaveStatus.Applied;

        if (before == SaveStatus.PreApplied || before == SaveStatus.Applying || before == SaveStatus.Applied)
            return after == SaveStatus.PreApplied || after == SaveStatus.Applying || after == SaveStatus.Applied;

        return false;
    }

    private static Predicate<Pending> getPendingPredicate(Id nodeId, CommandStore[] stores)
    {
        Set<CommandStore> nodeStores = new HashSet<>(Arrays.asList(stores));
        return item -> {
            if (item instanceof DelayedCommandStore.DelayedTask<?>)
            {
                DelayedCommandStore.DelayedTask<?> task = (DelayedCommandStore.DelayedTask<?>) item;
                if (nodeStores.contains(task.owner()))
                    return true;
                item = item.origin();
            }
            if (item instanceof SimulatedDelayedExecutorService.RegularTask<?>)
            {
                SimulatedDelayedExecutorService.RegularTask<?> task = (SimulatedDelayedExecutorService.RegularTask<?>) item;
                Object owner = task.owner();
                if (owner != null && owner.equals(nodeId))
                    return true;
                item = item.origin();
            }
            if (item instanceof RecurringPendingRunnable)
            {
                RecurringPendingRunnable recurring = (RecurringPendingRunnable) item;
                return recurring.source == nodeId.id && (!recurring.isRecurring || recurring.origin() != recurring);
            }
            return false;
        };
    }

    private interface Service extends AutoCloseable
    {
        void start();
        @Override
        void close();
    }

    private static BiFunction<Id, Id, Link> partition(List<Id> nodes, RandomSource random, int rf, BiFunction<Id, Id, Link> up)
    {
        Collections.shuffle(nodes, random.asJdkRandom());
        int partitionSize = random.nextInt((rf+1)/2);
        Set<Id> partition = new LinkedHashSet<>(nodes.subList(0, partitionSize));
        BiFunction<Id, Id, Link> down = (from, to) -> new Link(() -> DROP, up.apply(from, to).latencyMicros);
        return (from, to) -> (partition.contains(from) == partition.contains(to) ? up : down).apply(from, to);
    }

    /**
     * pair every node with one other node in one direction with a network behaviour override
     */
    private static BiFunction<Id, Id, Link> pairedUnidirectionalOverrides(Function<Link, Link> linkOverride, List<Id> nodes, RandomSource random, BiFunction<Id, Id, Link> fallback)
    {
        Map<Id, Map<Id, Link>> map = new HashMap<>();
        Collections.shuffle(nodes, random.asJdkRandom());
        for (int i = 0 ; i + 1 < nodes.size() ; i += 2)
        {
            Id from = nodes.get(i);
            Id to = nodes.get(i + 1);
            Link link = linkOverride.apply(fallback.apply(from, to));
            map.put(from, singletonMap(to, link));
        }
        return (from, to) -> nonNullOrGet(map.getOrDefault(from, emptyMap()).get(to), from, to, fallback);
    }

    private static BiFunction<Id, Id, Link> randomOverrides(boolean bidirectional, Function<Link, Link> linkOverride, int count, List<Id> nodes, RandomSource random, BiFunction<Id, Id, Link> fallback)
    {
        Map<Id, Map<Id, Link>> map = new HashMap<>();
        while (count > 0)
        {
            Id from = nodes.get(random.nextInt(nodes.size()));
            Id to = nodes.get(random.nextInt(nodes.size()));
            Link fwd = linkOverride.apply(fallback.apply(from, to));
            if (null == map.computeIfAbsent(from, ignore -> new HashMap<>()).putIfAbsent(to, fwd))
            {
                if (bidirectional)
                {
                    Link rev = linkOverride.apply(fallback.apply(to, from));
                    map.computeIfAbsent(to, ignore -> new HashMap<>()).put(from, rev);
                }
                --count;
            }
        }
        return (from, to) -> nonNullOrGet(map.getOrDefault(from, emptyMap()).get(to), from, to, fallback);
    }

    private static Link nonNullOrGet(Link ifNotNull, Id from, Id to, BiFunction<Id, Id, Link> function)
    {
        if (ifNotNull != null)
            return ifNotNull;
        return function.apply(from, to);
    }

    private static Link healthy(LongSupplier latency)
    {
        return new Link(() -> DELIVER, latency);
    }

    private static Link down(LongSupplier latency)
    {
        return new Link(() -> DROP, latency);
    }

    private LongSupplier defaultRandomWalkLatencyMicros(RandomSource random)
    {
        LongSupplier range = FrequentLargeRange.builder(random)
                                               .ratio(1, 5)
                                               .small(500, TimeUnit.MICROSECONDS, 5, MILLISECONDS)
                                               .large(50, MILLISECONDS, 5, SECONDS)
                                               .build().asLongSupplier(random);

        return () -> NANOSECONDS.toMicros(range.getAsLong());
    }

    enum OverrideLinkKind { LATENCY, ACTION, BOTH }

    private Supplier<Function<Link, Link>> linkOverrideSupplier(RandomSource random)
    {
        Supplier<OverrideLinkKind> nextKind = random.randomWeightedPicker(OverrideLinkKind.values());
        Supplier<LongSupplier> latencySupplier = random.biasedUniformLongsSupplier(
            MILLISECONDS.toMicros(1L), SECONDS.toMicros(2L),
            MILLISECONDS.toMicros(1L), MILLISECONDS.toMicros(300L), SECONDS.toMicros(1L),
            MILLISECONDS.toMicros(1L), MILLISECONDS.toMicros(300L), SECONDS.toMicros(1L)
        );
        NodeSink.Action[] actions = NodeSink.Action.values();
        Supplier<Supplier<NodeSink.Action>> actionSupplier = () -> random.randomWeightedPicker(actions);
        return () -> {
            OverrideLinkKind kind = nextKind.get();
            switch (kind)
            {
                default: throw new UnhandledEnum(kind);
                case BOTH: return ignore -> new Link(actionSupplier.get(), latencySupplier.get());
                case ACTION: return override -> new Link(actionSupplier.get(), override.latencyMicros);
                case LATENCY: return override -> new Link(override.action, latencySupplier.get());
            }
        };
    }

    enum OverrideLinksKind { NONE, PAIRED_UNIDIRECTIONAL, RANDOM_UNIDIRECTIONAL, RANDOM_BIDIRECTIONAL }

    private Function<List<Id>, BiFunction<Id, Id, Link>> overrideLinks(RandomSource random, IntSupplier rf, BiFunction<Id, Id, Link> defaultLinks)
    {
        Supplier<Function<Link, Link>> linkOverrideSupplier = linkOverrideSupplier(random);
        BooleanSupplier partitionChance = random.biasedUniformBools(random.nextFloat());
        Supplier<OverrideLinksKind> nextKind = random.randomWeightedPicker(OverrideLinksKind.values());
        return nodesList -> {
            BiFunction<Id, Id, Link> links = defaultLinks;
            if (partitionChance.getAsBoolean()) // 50% chance of a whole network partition
                links = partition(nodesList, random, rf.getAsInt(), links);

            OverrideLinksKind kind = nextKind.get();
            if (kind == NONE)
                return links;

            Function<Link, Link> linkOverride = linkOverrideSupplier.get();
            switch (kind)
            {
                default: throw new UnhandledEnum(kind);
                case PAIRED_UNIDIRECTIONAL:
                    return pairedUnidirectionalOverrides(linkOverride, nodesList, random, defaultLinks);
                case RANDOM_BIDIRECTIONAL:
                case RANDOM_UNIDIRECTIONAL:
                    boolean bidirectional = kind == RANDOM_BIDIRECTIONAL;
                    int count = random.nextInt(bidirectional || random.nextBoolean() ? nodesList.size() : Math.max(1, (nodesList.size() * nodesList.size())/2));
                    return randomOverrides(bidirectional, linkOverride, count, nodesList, random, defaultLinks);
            }
        };
    }

    private BiFunction<Id, Id, Link> defaultLinks(RandomSource random)
    {
        return caching((from, to) -> healthy(defaultRandomWalkLatencyMicros(random)));
    }

    private BiFunction<Id, Id, Link> caching(BiFunction<Id, Id, Link> uncached)
    {
        Map<Id, Map<Id, Link>> stash = new HashMap<>();
        return (from, to) -> stash.computeIfAbsent(from, ignore -> new HashMap<>())
                                  .computeIfAbsent(to, ignore -> uncached.apply(from, to));
    }

    private LinkConfig defaultLinkConfig(RandomSource random, IntSupplier rf)
    {
        BiFunction<Id, Id, Link> defaultLinks = defaultLinks(random);
        Function<List<Id>, BiFunction<Id, Id, Link>> overrideLinks = overrideLinks(random, rf, defaultLinks);
        return new LinkConfig(overrideLinks, defaultLinks);
    }

    public static class BlockingTransaction
    {
        final TxnId txnId;
        final Command command;
        final DelayedCommandStore commandStore;
        final Command blockedOn;
        final Object blockedVia;

        public BlockingTransaction(TxnId txnId, Command command, DelayedCommandStore commandStore, @Nullable Command blockedOn, @Nullable Object blockedVia)
        {
            this.txnId = txnId;
            this.command = command;
            this.commandStore = commandStore;
            this.blockedOn = blockedOn;
            this.blockedVia = blockedVia;
            Invariants.requireArgument(blockedOn == null || !txnId.equals(blockedOn.txnId()));
        }

        @Override
        public String toString()
        {
            return txnId + ":" + command.saveStatus() + "@"
                   + commandStore.toString().replaceAll("DelayedCommandStore", "")
                   + (command.homeKey() != null && commandStore.unsafeGetRangesForEpoch().allAt(txnId.epoch()).contains(command.homeKey()) ? "(Home)" : "");
        }
    }

    public List<BlockingTransaction> findBlockedCommitted(@Nullable Txn.Kind first, Txn.Kind ... rest)
    {
        return findBlocked(SaveStatus.Committed, first, rest);
    }

    public List<BlockingTransaction> findBlocked(@Nullable SaveStatus minSaveStatus, @Nullable Txn.Kind first, Txn.Kind ... rest)
    {
        List<BlockingTransaction> result = new ArrayList<>();
        BlockingTransaction cur = findMin(SaveStatus.Committed, SaveStatus.ReadyToExecute, first, rest);
        while (cur != null)
        {
            result.add(cur);
            Command command = cur.commandStore.unsafeCommands().get(cur.txnId).value();
            if (!command.hasBeen(Status.Stable) || cur.blockedOn == null)
                break;

            cur = find(cur.blockedOn.txnId(), null, SaveStatus.Stable);
        }
        return result;
    }

    public BlockingTransaction findMinUnstable()
    {
        return findMin(true, null, SaveStatus.Committed, null);
    }

    public BlockingTransaction findMinUnstable(@Nullable Txn.Kind first, Txn.Kind ... rest)
    {
        return findMin(null, SaveStatus.Committed, first, rest);
    }

    public BlockingTransaction findMin(@Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus, @Nullable Txn.Kind first, Txn.Kind ... rest)
    {
        Predicate<Txn.Kind> testKind = first == null ? ignore -> true : EnumSet.of(first, rest)::contains;
        return findMin(minSaveStatus, maxSaveStatus, id -> testKind.test(id.kind()));
    }

    public BlockingTransaction find(TxnId txnId, @Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus)
    {
        return findMin(minSaveStatus, maxSaveStatus, txnId::equals);
    }

    public BlockingTransaction find(boolean onlyIfOwned, TxnId txnId, @Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus)
    {
        return findMin(onlyIfOwned, minSaveStatus, maxSaveStatus, txnId::equals);
    }

    public List<BlockingTransaction> findTransitivelyBlocking(TxnId txnId)
    {
        return findTransitivelyBlocking(true, txnId);
    }

    public List<BlockingTransaction> findTransitivelyBlocking(boolean onlyIfOwned, TxnId txnId)
    {
        Set<TxnId> detectLoop = new HashSet<>();
        BlockingTransaction txn = find(onlyIfOwned, txnId, null, null);
        if (txn == null)
            return null;

        List<BlockingTransaction> result = new ArrayList<>();
        while (true)
        {
            if (txn.blockedOn == null || txn.command.saveStatus().compareTo(SaveStatus.Stable) < 0)
            {
                // look for another copy that is still blocked, and continue from there
                BlockingTransaction alt = find(txn.txnId, SaveStatus.Stable, null);
                if (alt != null)
                    txn = alt;
            }

            result.add(txn);
            if (!detectLoop.add(txn.txnId) || txn.blockedOn == null || txn.command.saveStatus().compareTo(SaveStatus.Stable) < 0)
                return result;

            Command blockedOn = txn.blockedOn;
            GlobalCommand command = txn.commandStore.unsafeCommands().get(blockedOn.txnId());
            if (command != null && command.value().saveStatus().compareTo(SaveStatus.Applied) < 0)
                txn = toBlocking(command.value(), txn.commandStore);
            else
                txn = find(txn.blockedOn.txnId(), null, null);
        }
    }

    public BlockingTransaction findMin(@Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus, Predicate<TxnId> testTxnId)
    {
        return findMin(false, minSaveStatus, maxSaveStatus, testTxnId);
    }

    public BlockingTransaction findMin(boolean onlyIfOwned, @Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus, Predicate<TxnId> testTxnId)
    {
        return find(onlyIfOwned, minSaveStatus, maxSaveStatus, testTxnId, (min, test) -> {
            int c;
            if (min == null || (c = test.txnId.compareTo(min.txnId)) <= 0 && (c < 0 || test.command.saveStatus().compareTo(min.command.saveStatus()) < 0))
                min = test;
            return min;
        }, null);
    }

    public List<BlockingTransaction> findAll(TxnId txnId)
    {
        return findAll(null, null, txnId::equals);
    }

    public List<BlockingTransaction> findAll(@Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus, Predicate<TxnId> testTxnId)
    {
        List<BlockingTransaction> result = new ArrayList<>();
        find(false, minSaveStatus, maxSaveStatus, testTxnId, (r, c) -> { r.add(c); return r; }, result);
        return result;
    }

    public <T> T find(boolean onlyIfOwned, @Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus, Predicate<TxnId> testTxnId, BiFunction<T, BlockingTransaction, T> fold, T accumulate)
    {
        for (Node.Id id : sinks.keySet())
        {
            Node node = lookup.apply(id);

            DelayedCommandStores stores = (DelayedCommandStores) node.commandStores();
            for (DelayedCommandStore store : stores.unsafeStores())
            {
                for (Map.Entry<TxnId, GlobalCommand> e : store.unsafeCommands().entrySet())
                {
                    Command command = e.getValue().value();
                    if ((!onlyIfOwned || owns(command, store)) &&
                    (minSaveStatus == null || command.saveStatus().compareTo(minSaveStatus) >= 0) &&
                        (maxSaveStatus == null || command.saveStatus().compareTo(maxSaveStatus) <= 0) &&
                        (testTxnId == null || testTxnId.test(command.txnId())))
                    {
                        accumulate = fold.apply(accumulate, toBlocking(command, store));
                        break;
                    }
                }
            }
        }
        return accumulate;
    }

    private boolean owns(Command command, CommandStore commandStore)
    {
        StoreParticipants participants = command.participants();
        if (participants == null)
            return false;

        return participants.owns().intersects(commandStore.unsafeGetRangesForEpoch().allBetween(command.txnId().epoch(), command.executeAtIfKnownElseTxnId()));
    }

    private BlockingTransaction toBlocking(Command command, DelayedCommandStore store)
    {
        Object blockedVia = null;
        TxnId blockedOnId = null;
        if (command.hasBeen(Status.Stable) && !command.hasBeen(Status.Truncated))
        {
            Command.WaitingOn waitingOn = command.asCommitted().waitingOn();
            RoutingKey blockedOnKey = waitingOn.lastWaitingOnKey();
            if (blockedOnKey == null)
            {
                blockedOnId = waitingOn.nextWaitingOnTxn();
                Invariants.require(!command.txnId().equals(blockedOnId));
                if (blockedOnId != null)
                    blockedVia = command.partialDeps().participants(blockedOnId);
            }
            else
            {
                CommandsForKey cfk = store.unsafeCommandsForKey().get(blockedOnKey).value();
                blockedOnId = cfk.blockedOnTxnId(command.txnId(), command.executeAt());
                if (blockedOnId != null)
                    blockedVia = cfk;
                Invariants.require(!command.txnId().equals(blockedOnId));
            }
        }
        Command blockedOn = null;
        if (blockedOnId != null)
        {
            GlobalCommand cmd = store.unsafeCommands().get(blockedOnId);
            if (cmd == null) blockedOn = uninitialised(blockedOnId);
            else blockedOn = cmd.value();
        }
        return new BlockingTransaction(command.txnId(), command, store, blockedOn, blockedVia);
    }

}
