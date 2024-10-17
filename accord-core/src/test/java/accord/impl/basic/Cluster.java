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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.LocalConfig;
import accord.api.MessageSink;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.burn.BurnTestConfigurationService;
import accord.burn.TopologyUpdates;
import accord.burn.random.FrequentLargeRange;
import accord.coordinate.Barrier;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.Exhausted;
import accord.coordinate.Invalidated;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.coordinate.Truncated;
import accord.impl.DurabilityScheduling;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.impl.DefaultTimeouts;
import accord.impl.InMemoryCommandStore.GlobalCommand;
import accord.impl.MessageListener;
import accord.impl.PrefixedIntHashKey;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TopologyFactory;
import accord.impl.basic.DelayedCommandStores.DelayedCommandStore;
import accord.impl.list.ListAgent;
import accord.impl.list.ListStore;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.local.AgentExecutor;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.TimeService;
import accord.primitives.SaveStatus;
import accord.local.ShardDistributor;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.local.cfk.CommandsForKey;
import accord.messages.Message;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.Request;
import accord.messages.SafeCallback;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
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

    boolean hasNonRecurring()
    {
        boolean hasNonRecurring = false;
        for (Pending p : pending)
        {
            if (!(p instanceof RecurringPendingRunnable))
                continue;
            RecurringPendingRunnable r = (RecurringPendingRunnable) p;
            if (r.requeue.hasNonRecurring())
            {
                hasNonRecurring = true;
                break;
            }
        }

        return hasNonRecurring;
    }

    public boolean processPending()
    {
        checkFailures.run();
        // All remaining tasks are recurring
        if (recurring > 0 && pending.size() == recurring && !hasNonRecurring())
            return false;

        Pending next = pending.poll();
        if (next == null)
            return false;

        processNext(next);

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
                SafeCallback callback = reply.isFinal()
                                        ? sinks.get(deliver.dst).callbacks.remove(deliver.replyId)
                                        : sinks.get(deliver.dst).callbacks.get(deliver.replyId);

                if (callback != null)
                {
                    if (reply instanceof Reply.FailureReply) callback.failure(deliver.src, ((Reply.FailureReply) reply).failure);
                    else callback.success(deliver.src, reply);
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
    }

    @Override
    public Scheduled recurring(Runnable run, long delay, TimeUnit units)
    {
        return recurring(run, () -> delay, units);
    }

    @Override
    public Scheduled once(Runnable run, long delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(null, run, () -> delay, units, false);
        pending.add(result, delay, units);
        return result;
    }

    @Override
    public Scheduled selfRecurring(Runnable run, long delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(null, run, () -> delay, units, true);
        pending.add(result, delay, units);
        return result;
    }

    public Scheduled recurring(Runnable run, LongSupplier delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(pending, run, delay, units, true);
        ++recurring;
        result.onCancellation(() -> --recurring);
        pending.add(result, delay.getAsLong(), units);
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

    // TODO (expected): merge with BurnTest.burn
    public static Map<MessageType, Cluster.Stats> run(Supplier<RandomSource> randomSupplier,
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
                queue.add((PendingRunnable) retry::run, delay, TimeUnit.SECONDS);
            };
        }
        IntSupplier coordinationDelays, progressDelays, timeoutDelays;
        {
            RandomSource rnd = randomSupplier.get();
            timeoutDelays = progressDelays = coordinationDelays = () -> rnd.nextInt(100, 1000);
        }
        Function<BiConsumer<Timestamp, Ranges>, ListAgent> agentSupplier = onStale -> new ListAgent(randomSupplier.get(), 1000L, failures::add, retryBootstrap, onStale, coordinationDelays, progressDelays, timeoutDelays);
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
        SimulatedDelayedExecutorService globalExecutor = new SimulatedDelayedExecutorService(queue, new ListAgent(randomSupplier.get(), 1000L, failures::add, retryBootstrap, (i1, i2) -> {
            throw new IllegalAccessError("Global executor should never get a stale event");
        }, () -> { throw new UnsupportedOperationException(); }, () -> { throw new UnsupportedOperationException(); }, timeoutDelays));
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
            List<DurabilityScheduling> durabilityScheduling = new ArrayList<>();
            List<Service> services = new ArrayList<>();
            for (Id id : nodes)
            {
                MessageSink messageSink = sinks.create(id, randomSupplier.get());
                LongSupplier nowSupplier = nowSupplierSupplier.get();
                LocalConfig localConfig = LocalConfig.DEFAULT;
                BiConsumer<Timestamp, Ranges> onStale = (sinceAtLeast, ranges) -> configRandomizer.onStale(id, sinceAtLeast, ranges);
                AgentExecutor nodeExecutor = nodeExecutorSupplier.apply(id, onStale);
                executorMap.put(id, nodeExecutor);
                Journal journal = new Journal(id);
                journalMap.put(id, journal);
                BurnTestConfigurationService configService = new BurnTestConfigurationService(id, nodeExecutor, randomSupplier, topology, nodeMap::get, topologyUpdates);
                BooleanSupplier isLoadedCheck = Gens.supplier(Gens.bools().mixedDistribution().next(random), random);
                Node node = new Node(id, messageSink, configService, TimeService.ofNonMonotonic(nowSupplier, MILLISECONDS),
                                     () -> new ListStore(sinks, random, id), new ShardDistributor.EvenSplit<>(8, ignore -> new PrefixedIntHashKey.Splitter()),
                                     nodeExecutor.agent(),
                                     randomSupplier.get(), sinks, SizeOfIntersectionSorter.SUPPLIER, DefaultRemoteListeners::new, DefaultTimeouts::new,
                                     DefaultProgressLogs::new, DefaultLocalListeners.Factory::new, DelayedCommandStores.factory(sinks.pending, isLoadedCheck, journal), new CoordinationAdapter.DefaultFactory(),
                                     DurableBefore.NOOP_PERSISTER, localConfig);
                DurabilityScheduling durability = node.durabilityScheduling();
                // TODO (desired): randomise
                durability.setShardCycleTime(30, SECONDS);
                durability.setGlobalCycleTime(180, SECONDS);
                durabilityScheduling.add(durability);
                nodeMap.put(id, node);
                durabilityScheduling.add(new DurabilityScheduling(node));
                services.add(new BarrierService(node, randomSupplier.get()));
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
                    durabilityScheduling.forEach(d -> {
                        d.setTargetShardSplits(c);
                        d.setShardCycleTime(s, SECONDS);
                        d.setGlobalCycleTime(g, SECONDS);
                    });
                };
            }
            updateDurabilityRate.run();
            schemaApply.onUpdate(topology);

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

            Scheduled purge = sinks.recurring(() -> {
                trace.debug("Triggering purge.");
                int numNodes = random.nextInt(1, nodeMap.size());
                for (int i = 0; i < numNodes; i++)
                {
                    Id id = random.pick(nodes);
                    Node node = nodeMap.get(id);

                    Journal journal = journalMap.get(node.id());
                    CommandStore[] stores = nodeMap.get(node.id()).commandStores().all();
                    journal.purge((j) -> stores[j]);
                }
            }, () -> random.nextInt(1, 10), SECONDS);

            Scheduled restart = sinks.recurring(() -> {
                Id id = random.pick(nodes);
                CommandStore[] stores = nodeMap.get(id).commandStores().all();
                ((DelayedCommandStore)stores[0]).unsafeRunIn(() -> {
                    Predicate<Pending> pred = getPendingPredicate(stores);
                    while (sinks.drain(pred));

                    // Journal cleanup is a rough equivalent of a node restart.
                    trace.debug("Triggering journal cleanup for node " + id);
                    CommandsForKey.disableLinearizabilityViolationsReporting();
                    ListStore listStore = (ListStore) nodeMap.get(id).commandStores().dataStore();
                    listStore.clear();
                    listStore.restoreFromSnapshot();

                    Journal journal = journalMap.get(id);
                    for (CommandStore s : stores)
                    {
                        DelayedCommandStores.DelayedCommandStore store = (DelayedCommandStores.DelayedCommandStore) s;
                        store.clearForTesting();
                        journal.reconstructAll(store.loader(), store.id());
                    }
                    while (sinks.drain(pred));
                    CommandsForKey.enableLinearizabilityViolationsReporting();
                    trace.debug("Done with cleanup.");
                });
            }, () -> random.nextInt(1, 10), SECONDS);

            durabilityScheduling.forEach(DurabilityScheduling::start);
            services.forEach(Service::start);

            Runnable stop = () -> {
                reconfigure.cancel();
                purge.cancel();
                restart.cancel();
                durabilityScheduling.forEach(DurabilityScheduling::stop);
                services.forEach(Service::close);
            };
            noMoreWorkSignal.accept(stop);
            readySignal.accept(nodeMap);

            Packet next;
            while ((next = in.get()) != null)
                sinks.add(next, 0, TimeUnit.NANOSECONDS);

            while (sinks.processPending());

            stop.run();
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
            nodeMap.values().forEach(Node::shutdown);
        }
    }

    private static Predicate<Pending> getPendingPredicate(CommandStore[] stores)
    {
        Set<CommandStore> nodeStores = new HashSet<>(Arrays.asList(stores));
        return item -> {
            if (item instanceof DelayedCommandStore.DelayedTask)
            {
                DelayedCommandStore.DelayedTask<?> task = (DelayedCommandStore.DelayedTask<?>) item;
                if (nodeStores.contains(task.parent()))
                    return true;
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
                Keys keys = Keys.of(keysInsideRanges(ranges).next(rs));
                run(node, keys, node.computeRoute(current.epoch(), keys), current.epoch(), type);
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
                Ranges rs = Ranges.of(subset.toArray(Range[]::new));
                run(node, rs, node.computeRoute(current.epoch(), rs), current.epoch(), type);
            }
        }

        private void run(Node node, Seekables<?, ?> keysOrRanges, FullRoute<?> route, long epoch, BarrierType type)
        {
            Barrier.barrier(node, keysOrRanges, route, epoch, type).begin((s, f) -> {
                if (f != null)
                {
                    // ignore specific errors
                    if (f instanceof Invalidated || f instanceof Timeout || f instanceof Preempted || f instanceof Exhausted || f instanceof Truncated)
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
            Invariants.checkArgument(blockedOn == null || !txnId.equals(blockedOn.txnId()));
        }

        @Override
        public String toString()
        {
            return txnId + ":" + command.saveStatus() + "@"
                   + commandStore.toString().replaceAll("DelayedCommandStore", "")
                   + (command.homeKey() != null && commandStore.unsafeRangesForEpoch().allAt(txnId.epoch()).contains(command.homeKey()) ? "(Home)" : "");
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
        return findMin(null, SaveStatus.Committed, null);
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

    public List<BlockingTransaction> findTransitivelyBlocking(TxnId txnId)
    {
        BlockingTransaction txn = find(txnId, null, null);
        if (txn == null)
            return null;

        List<BlockingTransaction> result = new ArrayList<>();
        while (true)
        {
            result.add(txn);
            if (txn.command.saveStatus().compareTo(SaveStatus.Stable) < 0)
                return result;

            if (txn.blockedOn == null)
            {
                // look for another copy that is still blocked, and continue from there
                txn = find(txn.txnId, null, null);
                if (txn.blockedOn == null)
                    return result;
            }

            Command blockedOn = txn.blockedOn;
            GlobalCommand command = txn.commandStore.unsafeCommands().get(blockedOn.txnId());
            if (command == null)
                return result;
            else if (command.value().saveStatus().compareTo(SaveStatus.Applied) < 0)
                txn = toBlocking(command.value(), txn.commandStore);
            else
                txn = find(txn.blockedOn.txnId(), null, null);
        }
    }

    public BlockingTransaction findMin(@Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus, Predicate<TxnId> testTxnId)
    {
        return find(minSaveStatus, maxSaveStatus, testTxnId, (min, test) -> {
            int c = -1;
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
        find(minSaveStatus, maxSaveStatus, testTxnId, (r, c) -> { r.add(c); return r; }, result);
        return result;
    }

    public <T> T find(@Nullable SaveStatus minSaveStatus, @Nullable SaveStatus maxSaveStatus, Predicate<TxnId> testTxnId, BiFunction<T, BlockingTransaction, T> fold, T accumulate)
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
                    if ((minSaveStatus == null || command.saveStatus().compareTo(minSaveStatus) >= 0) &&
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
                blockedOnId = waitingOn.nextWaitingOn();
                Invariants.checkState(!command.txnId().equals(blockedOnId));
                if (blockedOnId != null)
                    blockedVia = command.partialDeps().participants(blockedOnId);
            }
            else
            {
                CommandsForKey cfk = store.unsafeCommandsForKey().get(blockedOnKey).value();
                blockedOnId = cfk.blockedOnTxnId(command.txnId(), command.executeAt());
                if (blockedOnId != null)
                    blockedVia = cfk;
                Invariants.checkState(!command.txnId().equals(blockedOnId));
            }
        }
        Command blockedOn = null;
        if (blockedOnId != null)
            blockedOn = store.unsafeCommands().get(blockedOnId).value();
        return new BlockingTransaction(command.txnId(), command, store, blockedOn, blockedVia);
    }

}
