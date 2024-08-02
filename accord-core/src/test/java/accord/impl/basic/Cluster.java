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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.burn.BurnTestConfigurationService;
import accord.burn.TopologyUpdates;
import accord.burn.random.FrequentLargeRange;
import accord.config.LocalConfig;
import accord.config.MutableLocalConfig;
import accord.coordinate.Barrier;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.Exhausted;
import accord.coordinate.Invalidated;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.impl.CoordinateDurabilityScheduling;
import accord.impl.MessageListener;
import accord.impl.PrefixedIntHashKey;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TopologyFactory;
import accord.impl.list.ListAgent;
import accord.impl.list.ListStore;
import accord.local.AgentExecutor;
import accord.local.Node.Id;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.ShardDistributor;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.Request;
import accord.messages.SafeCallback;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.topology.Topology;
import accord.topology.TopologyRandomizer;
import accord.utils.Gens;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;

import static accord.impl.basic.Cluster.OverrideLinksKind.NONE;
import static accord.impl.basic.Cluster.OverrideLinksKind.RANDOM_BIDIRECTIONAL;
import static accord.impl.basic.NodeSink.Action.DELIVER;
import static accord.impl.basic.NodeSink.Action.DROP;
import static accord.utils.AccordGens.keysInsideRanges;
import static accord.utils.AccordGens.rangeInsideRange;
import static accord.utils.Gens.mixedDistribution;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class Cluster implements Scheduler
{
    public static final Logger trace = LoggerFactory.getLogger("accord.impl.basic.Trace");

    public static class Stats
    {
        final MessageType type;
        int count;

        public Stats(MessageType type)
        {
            this.type = type;
        }

        public int count() { return count; }
        public String toString() { return type + ": " + count; }
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

    final Map<MessageType, Stats> statsMap = new HashMap<>();

    final RandomSource random;
    final LinkConfig linkConfig;
    final Function<Id, Node> lookup;
    final Function<Id, Journal> journalLookup;
    final PendingQueue pending;
    final Runnable checkFailures;
    final List<Runnable> onDone = new ArrayList<>();
    final Consumer<Packet> responseSink;
    final Map<Id, NodeSink> sinks = new HashMap<>();
    final MessageListener messageListener;
    int clock;
    int recurring;
    BiFunction<Id, Id, Link> links;

    public Cluster(RandomSource random, MessageListener messageListener, Supplier<PendingQueue> queueSupplier, Runnable checkFailures, Function<Id, Node> lookup, Function<Id, Journal> journalLookup, IntSupplier rf, Consumer<Packet> responseSink)
    {
        this.random = random;
        this.messageListener = messageListener;
        this.pending = queueSupplier.get();
        this.checkFailures = checkFailures;
        this.lookup = lookup;
        this.journalLookup = journalLookup;
        this.responseSink = responseSink;
        this.linkConfig = defaultLinkConfig(random, rf);
        this.links = linkConfig.defaultLinks;
    }

    NodeSink create(Id self, RandomSource random)
    {
        NodeSink sink = new NodeSink(self, lookup, this, random);
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
        List<Object> pending = new ArrayList<>();
        while (this.pending.size() > 0)
            pending.add(this.pending.poll());

        for (Object next : pending)
            processNext(next);
    }

    public boolean processPending()
    {
        checkFailures.run();
        if (pending.size() == recurring)
            return false;

        Object next = pending.poll();
        if (next == null)
            return false;

        processNext(next);
        checkFailures.run();
        return true;
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
                SafeCallback callback = reply.isFinal()
                                        ? sinks.get(deliver.dst).callbacks.remove(deliver.replyId)
                                        : sinks.get(deliver.dst).callbacks.get(deliver.replyId);

                if (callback != null)
                {
                    if (reply instanceof Reply.FailureReply) callback.failure(deliver.src, ((Reply.FailureReply) reply).failure);
                    else callback.success(deliver.src, reply);
                }
            }
            else journalLookup.apply(deliver.dst).handle((Request) deliver.message, deliver.src, deliver);
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
    }

    @Override
    public Scheduled recurring(Runnable run, long delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(pending, run, delay, units);
        ++recurring;
        result.onCancellation(() -> --recurring);
        pending.add(result, delay, units);
        return result;
    }

    @Override
    public Scheduled once(Runnable run, long delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(null, run, delay, units);
        pending.add(result, delay, units);
        return result;
    }

    public void onDone(Runnable run)
    {
        onDone.add(run);
    }

    @Override
    public void now(Runnable run)
    {
        run.run();
    }

    public static Map<MessageType, Cluster.Stats> run(Supplier<RandomSource> randomSupplier,
                                                      List<Node.Id> nodes,
                                                      Topology initialTopology,
                                                      Function<Map<Node.Id, Node>, Request> init)
    {
        List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
        PropagatingPendingQueue queue = new PropagatingPendingQueue(failures, new RandomDelayQueue(randomSupplier.get()));
        RandomSource retryRandom = randomSupplier.get();
        Consumer<Runnable> retryBootstrap = retry -> {
            long delay = retryRandom.nextInt(1, 15);
            queue.add((PendingRunnable) retry::run, delay, TimeUnit.SECONDS);
        };
        Function<BiConsumer<Timestamp, Ranges>, ListAgent> agentSupplier = onStale -> new ListAgent(1000L, failures::add, retryBootstrap, onStale);
        RandomSource nowRandom = randomSupplier.get();
        Supplier<LongSupplier> nowSupplier = () -> {
            RandomSource forked = nowRandom.fork();
            // TODO (now): meta-randomise scale of clock drift
            return FrequentLargeRange.builder(forked)
                                     .ratio(1, 5)
                                     .small(50, 5000, TimeUnit.MICROSECONDS)
                                     .large(1, 10, TimeUnit.MILLISECONDS)
                                     .build()
                                     .mapAsLong(j -> Math.max(0, queue.nowInMillis() + TimeUnit.NANOSECONDS.toMillis(j)))
                                     .asLongSupplier(forked);
        };
        SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, new ListAgent(1000L, failures::add, retryBootstrap, (i1, i2) -> {
            throw new IllegalAccessError("Global executor should enver get a stale event");
        }));
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
                                                            MessageListener.get(),
                                                            () -> queue,
                                                            (id, onStale) -> globalExecutor.withAgent(agentSupplier.apply(onStale)),
                                                            queue::checkFailures,
                                                            ignore -> {
                                                            },
                                                            randomSupplier,
                                                            nowSupplier,
                                                            topologyFactory,
                                                            new Supplier<>()
                                                            {
                                                                private Iterator<Request> requestIterator = null;
                                                                private final RandomSource rs = randomSupplier.get();
                                                                @Override
                                                                public Packet get()
                                                                {
                                                                    // ((Cluster) node.scheduler()).onDone(() -> checkOnResult(homeKey, txnId, 0, null));
                                                                    if (requestIterator == null)
                                                                    {
                                                                        Map<Node.Id, Node> nodes = nodeMap.get();
                                                                        requestIterator = Collections.singleton(init.apply(nodes)).iterator();
                                                                    }
                                                                    if (!requestIterator.hasNext())
                                                                        return null;
                                                                    Node.Id id = rs.pick(nodes);
                                                                    return new Packet(id, id, counter.incrementAndGet(), requestIterator.next());
                                                                }
                                                            },
                                                            Runnable::run,
                                                            nodeMap::set);
        if (!failures.isEmpty())
        {
            AssertionError error = new AssertionError("Unexpected errors detected");
            failures.forEach(error::addSuppressed);
            throw error;
        }
        return stats;
    }

    public static Map<MessageType, Stats> run(Id[] nodes, MessageListener messageListener, Supplier<PendingQueue> queueSupplier,
                                              BiFunction<Id, BiConsumer<Timestamp, Ranges>, AgentExecutor> nodeExecutorSupplier,
                                              Runnable checkFailures, Consumer<Packet> responseSink,
                                              Supplier<RandomSource> randomSupplier, Supplier<LongSupplier> nowSupplierSupplier,
                                              TopologyFactory topologyFactory, Supplier<Packet> in, Consumer<Runnable> noMoreWorkSignal,
                                              Consumer<Map<Id, Node>> readySignal)
    {
        Topology topology = topologyFactory.toTopology(nodes);
        Map<Id, Node> nodeMap = new LinkedHashMap<>();
        Map<Id, AgentExecutor> executorMap = new LinkedHashMap<>();
        Map<Id, Journal> journalMap = new LinkedHashMap<>();
        try
        {
            RandomSource random = randomSupplier.get();
            Cluster sinks = new Cluster(randomSupplier.get(), messageListener, queueSupplier, checkFailures, nodeMap::get, journalMap::get, () -> topologyFactory.rf, responseSink);
            TopologyUpdates topologyUpdates = new TopologyUpdates(executorMap::get);
            TopologyRandomizer.Listener schemaApply = t -> {
                for (Node node : nodeMap.values())
                {
                    ListStore store = (ListStore) node.commandStores().dataStore();
                    store.onTopologyUpdate(node, t);
                }
                messageListener.onTopologyChange(t);
            };
            TopologyRandomizer configRandomizer = new TopologyRandomizer(randomSupplier, topology, topologyUpdates, nodeMap::get, schemaApply);
            List<CoordinateDurabilityScheduling> durabilityScheduling = new ArrayList<>();
            List<Service> services = new ArrayList<>();
            for (Id id : nodes)
            {
                MessageSink messageSink = sinks.create(id, randomSupplier.get());
                LongSupplier nowSupplier = nowSupplierSupplier.get();
                LocalConfig localConfig = new MutableLocalConfig();
                BiConsumer<Timestamp, Ranges> onStale = (sinceAtLeast, ranges) -> configRandomizer.onStale(id, sinceAtLeast, ranges);
                AgentExecutor nodeExecutor = nodeExecutorSupplier.apply(id, onStale);
                executorMap.put(id, nodeExecutor);
                Journal journal = new Journal(messageListener);
                journalMap.put(id, journal);
                BurnTestConfigurationService configService = new BurnTestConfigurationService(id, nodeExecutor, randomSupplier, topology, nodeMap::get, topologyUpdates);
                BooleanSupplier isLoadedCheck = Gens.supplier(Gens.bools().mixedDistribution().next(random), random);
                Node node = new Node(id, messageSink, journal, configService, nowSupplier, NodeTimeService.unixWrapper(TimeUnit.MILLISECONDS, nowSupplier),
                                     () -> new ListStore(id), new ShardDistributor.EvenSplit<>(8, ignore -> new PrefixedIntHashKey.Splitter()),
                                     nodeExecutor.agent(),
                                     randomSupplier.get(), sinks, SizeOfIntersectionSorter.SUPPLIER,
                                     SimpleProgressLog::new, DelayedCommandStores.factory(sinks.pending, isLoadedCheck, journal), new CoordinationAdapter.DefaultFactory(),
                                     localConfig);
                CoordinateDurabilityScheduling durability = new CoordinateDurabilityScheduling(node);
                // TODO (desired): randomise
                durability.setFrequency(60, SECONDS);
                durability.setGlobalCycleTime(180, SECONDS);
                durabilityScheduling.add(durability);
                nodeMap.put(id, node);
                durabilityScheduling.add(new CoordinateDurabilityScheduling(node));
                services.add(new BarrierService(node, randomSupplier.get()));
            }

            Runnable updateDurabilityRate;
            {
                IntSupplier frequencySeconds       = random.biasedUniformIntsSupplier( 1, 120, 10,  30, 10,  60).get();
                IntSupplier shardCycleTimeSeconds  = random.biasedUniformIntsSupplier(5, 60, 10, 30, 1, 30).get();
                IntSupplier globalCycleTimeSeconds = random.biasedUniformIntsSupplier( 1,  90, 10,  30, 10,  60).get();
                updateDurabilityRate = () -> {
                    int f = frequencySeconds.getAsInt();
                    int s = shardCycleTimeSeconds.getAsInt();
                    int g = globalCycleTimeSeconds.getAsInt();
                    durabilityScheduling.forEach(d -> {
                        d.setFrequency(f, SECONDS);
                        d.setShardCycleTime(s, SECONDS);
                        d.setGlobalCycleTime(g, SECONDS);
                    });
                };
            }
            updateDurabilityRate.run();
            schemaApply.onUpdate(topology);

            // startup
            journalMap.entrySet().forEach(e -> e.getValue().start(nodeMap.get(e.getKey())));
            AsyncResult<?> startup = AsyncChains.reduce(nodeMap.values().stream().map(Node::unsafeStart).collect(toList()), (a, b) -> null).beginAsResult();
            while (sinks.processPending());
            Assertions.assertTrue(startup.isDone());

            List<Id> nodesList = new ArrayList<>(Arrays.asList(nodes));
            Scheduled chaos = sinks.recurring(() -> {
                sinks.links = sinks.linkConfig.overrideLinks.apply(nodesList);
                if (random.decide(0.1f))
                    updateDurabilityRate.run();
            }, 5L, SECONDS);

            Scheduled reconfigure = sinks.recurring(configRandomizer::maybeUpdateTopology, 1, SECONDS);
            durabilityScheduling.forEach(CoordinateDurabilityScheduling::start);
            services.forEach(Service::start);

            noMoreWorkSignal.accept(() -> {
                reconfigure.cancel();
                durabilityScheduling.forEach(CoordinateDurabilityScheduling::stop);
                services.forEach(Service::close);
            });
            readySignal.accept(nodeMap);

            Packet next;
            while ((next = in.get()) != null)
                sinks.add(next, 0, TimeUnit.NANOSECONDS);

            while (sinks.processPending());

            chaos.cancel();
            reconfigure.cancel();
            durabilityScheduling.forEach(CoordinateDurabilityScheduling::stop);
            services.forEach(Service::close);
            sinks.links = sinks.linkConfig.defaultLinks;

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
            journalMap.values().forEach(Journal::shutdown);
            nodeMap.values().forEach(Node::shutdown);
        }
    }

    private interface Service extends AutoCloseable
    {
        void start();
        @Override
        void close();
    }

    private static abstract class AbstractService implements Service, Runnable
    {
        protected final Node node;
        protected final RandomSource rs;
        private Scheduled scheduled;

        protected AbstractService(Node node, RandomSource rs)
        {
            this.node = node;
            this.rs = rs;
        }

        @Override
        public void start()
        {
            Invariants.checkState(scheduled == null, "Start already called...");
            this.scheduled = node.scheduler().recurring(this, 1, SECONDS);
        }

        protected abstract void doRun() throws Exception;

        @Override
        public final void run()
        {
            try
            {
                doRun();
            }
            catch (Throwable t)
            {
                node.agent().onUncaughtException(t);
            }
        }

        @Override
        public void close()
        {
            if (scheduled != null)
            {
                scheduled.cancel();
                scheduled = null;
            }
        }
    }

    private static class BarrierService extends AbstractService
    {
        private final Supplier<BarrierType> typeSupplier;
        private final Supplier<Boolean> includeRangeSupplier;
        private final Supplier<Boolean> wholeOrPartialSupplier;

        private BarrierService(Node node, RandomSource rs)
        {
            super(node, rs);
            this.typeSupplier = mixedDistribution(BarrierType.values()).next(rs).asSupplier(rs);
            this.includeRangeSupplier = Gens.bools().mixedDistribution().next(rs).asSupplier(rs);
            this.wholeOrPartialSupplier = Gens.bools().mixedDistribution().next(rs).asSupplier(rs);
        }

        @Override
        public void doRun()
        {
            Topology current = node.topology().current();
            Ranges ranges = current.rangesForNode(node.id());
            if (ranges.isEmpty())
                return;
            BarrierType type = typeSupplier.get();
            if (type == BarrierType.local)
            {
                run(node, Keys.of(keysInsideRanges(ranges).next(rs)), current.epoch(), type);
            }
            else
            {
                List<Range> subset = new ArrayList<>();
                for (Range range : ranges)
                {
                    if (includeRangeSupplier.get())
                        subset.add(wholeOrPartialSupplier.get() ? range : rangeInsideRange(range).next(rs));
                }
                if (subset.isEmpty())
                    return;
                run(node, Ranges.of(subset.toArray(Range[]::new)), current.epoch(), type);
            }
        }

        private <S extends Seekables<?, ?>> void run(Node node, S keysOrRanges, long epoch, BarrierType type)
        {
            Barrier.barrier(node, keysOrRanges, epoch, type).begin((s, f) -> {
                if (f != null)
                {
                    // ignore specific errors
                    if (f instanceof Invalidated || f instanceof Timeout || f instanceof Preempted || f instanceof Exhausted)
                        return;
                    node.agent().onUncaughtException(f);
                }
            });
        }
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
                default: throw new AssertionError("Unhandled: " + kind);
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
                default: throw new AssertionError("Unhandled: " + kind);
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

}
