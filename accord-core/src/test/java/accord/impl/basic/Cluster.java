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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.config.LocalConfig;
import accord.impl.MessageListener;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.burn.BurnTestConfigurationService;
import accord.burn.TopologyUpdates;
import accord.coordinate.TxnExecute;
import accord.coordinate.TxnPersist;
import accord.impl.CoordinateDurabilityScheduling;
import accord.impl.IntHashKey;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TopologyFactory;
import accord.impl.list.ListStore;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.ShardDistributor;
import accord.messages.Apply;
import accord.config.MutableLocalConfig;
import accord.messages.LocalMessage;
import accord.messages.MessageType;
import accord.messages.Message;
import accord.messages.Reply;
import accord.messages.Request;
import accord.messages.SafeCallback;
import accord.topology.Topology;
import accord.topology.TopologyRandomizer;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class Cluster implements Scheduler
{
    public static final Logger trace = LoggerFactory.getLogger("accord.impl.basic.Trace");

    public static class Stats
    {
        int count;

        public int count() { return count; }
        public String toString() { return Integer.toString(count); }
    }

    Map<MessageType, Stats> statsMap = new HashMap<>();

    final RandomSource randomSource;
    final Function<Id, Node> lookup;
    final PendingQueue pending;
    final Runnable checkFailures;
    final List<Runnable> onDone = new ArrayList<>();
    final Consumer<Packet> responseSink;
    final Map<Id, NodeSink> sinks = new HashMap<>();
    final MessageListener messageListener;
    int clock;
    int recurring;
    Set<Id> partitionSet;

    public Cluster(RandomSource randomSource, MessageListener messageListener, Supplier<PendingQueue> queueSupplier, Runnable checkFailures, Function<Id, Node> lookup, Consumer<Packet> responseSink)
    {
        this.randomSource = randomSource;
        this.messageListener = messageListener;
        this.pending = queueSupplier.get();
        this.checkFailures = checkFailures;
        this.lookup = lookup;
        this.responseSink = responseSink;
        this.partitionSet = new HashSet<>();
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
            statsMap.computeIfAbsent(type, ignore -> new Stats()).count++;
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
                    callback.success(deliver.src, reply);
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

    public static Map<MessageType, Stats> run(Id[] nodes, MessageListener messageListener, Supplier<PendingQueue> queueSupplier, Runnable checkFailures, Consumer<Packet> responseSink, AgentExecutor executor, Supplier<RandomSource> randomSupplier, Supplier<LongSupplier> nowSupplierSupplier, TopologyFactory topologyFactory, Supplier<Packet> in, Consumer<Runnable> noMoreWorkSignal)
    {
        Topology topology = topologyFactory.toTopology(nodes);
        Map<Id, Node> lookup = new LinkedHashMap<>();
        try
        {
            Cluster sinks = new Cluster(randomSupplier.get(), messageListener, queueSupplier, checkFailures, lookup::get, responseSink);
            TopologyUpdates topologyUpdates = new TopologyUpdates(executor);
            TopologyRandomizer configRandomizer = new TopologyRandomizer(randomSupplier, topology, topologyUpdates, lookup::get);
            List<CoordinateDurabilityScheduling> durabilityScheduling = new ArrayList<>();
            for (Id id : nodes)
            {
                MessageSink messageSink = sinks.create(id, randomSupplier.get());
                LongSupplier nowSupplier = nowSupplierSupplier.get();
                BurnTestConfigurationService configService = new BurnTestConfigurationService(id, executor, randomSupplier, topology, lookup::get, topologyUpdates);
                LocalConfig localConfig = new MutableLocalConfig();
                Node node = new Node(id, messageSink, LocalMessage::process, configService, nowSupplier, NodeTimeService.unixWrapper(TimeUnit.MILLISECONDS, nowSupplier),
                                     () -> new ListStore(id), new ShardDistributor.EvenSplit<>(8, ignore -> new IntHashKey.Splitter()),
                                     executor.agent(),
                                     randomSupplier.get(), sinks, SizeOfIntersectionSorter.SUPPLIER,
                                     SimpleProgressLog::new, DelayedCommandStores.factory(sinks.pending), TxnExecute.FACTORY, TxnPersist.FACTORY, Apply.FACTORY,
                                     localConfig);
                lookup.put(id, node);
                CoordinateDurabilityScheduling durability = new CoordinateDurabilityScheduling(node);
                // TODO (desired): randomise
                durability.setFrequency(60, SECONDS);
                durability.setGlobalCycleTime(180, SECONDS);
                durabilityScheduling.add(durability);
            }

            // startup
            AsyncResult<?> startup = AsyncChains.reduce(lookup.values().stream().map(Node::unsafeStart).collect(toList()), (a, b) -> null).beginAsResult();
            while (sinks.processPending());
            Assertions.assertTrue(startup.isDone());

            List<Id> nodesList = new ArrayList<>(Arrays.asList(nodes));
            RandomSource shuffleRandom = randomSupplier.get();
            Scheduled chaos = sinks.recurring(() -> {
                Collections.shuffle(nodesList, shuffleRandom.asJdkRandom());
                int partitionSize = shuffleRandom.nextInt((topologyFactory.rf+1)/2);
                sinks.partitionSet = new LinkedHashSet<>(nodesList.subList(0, partitionSize));
            }, 5L, SECONDS);

            Scheduled reconfigure = sinks.recurring(configRandomizer::maybeUpdateTopology, 1, SECONDS);
            durabilityScheduling.forEach(CoordinateDurabilityScheduling::start);

            noMoreWorkSignal.accept(() -> {
                reconfigure.cancel();
                durabilityScheduling.forEach(CoordinateDurabilityScheduling::stop);
            });

            Packet next;
            while ((next = in.get()) != null)
                sinks.add(next, 0, TimeUnit.NANOSECONDS);

            while (sinks.processPending());

            chaos.cancel();
            reconfigure.cancel();
            durabilityScheduling.forEach(CoordinateDurabilityScheduling::stop);
            sinks.partitionSet = Collections.emptySet();

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
            lookup.values().forEach(Node::shutdown);
        }
    }
}
