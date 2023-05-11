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
import java.util.EnumMap;
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

import org.junit.jupiter.api.Assertions;

import accord.api.MessageSink;
import accord.burn.BurnTestConfigurationService;
import accord.burn.TopologyUpdates;
import accord.impl.*;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Scheduler;
import accord.impl.list.ListStore;
import accord.local.ShardDistributor;
import accord.messages.SafeCallback;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.Request;
import accord.topology.TopologyRandomizer;
import accord.topology.Topology;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    EnumMap<MessageType, Stats> statsMap = new EnumMap<>(MessageType.class);

    final Function<Id, Node> lookup;
    final PendingQueue pending;
    final List<Runnable> onDone = new ArrayList<>();
    final Consumer<Packet> responseSink;
    final Map<Id, NodeSink> sinks = new HashMap<>();
    int clock;
    int recurring;
    Set<Id> partitionSet;

    public Cluster(Supplier<PendingQueue> queueSupplier, Function<Id, Node> lookup, Consumer<Packet> responseSink)
    {
        this.pending = queueSupplier.get();
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

    private void add(Packet packet)
    {
        MessageType type = packet.message.type();
        if (type != null)
            statsMap.computeIfAbsent(type, ignore -> new Stats()).count++;
        boolean isReply = packet.message instanceof Reply;
        if (trace.isTraceEnabled())
            trace.trace("{} {} {}", clock++, isReply ? "RPLY" : "SEND", packet);
        if (lookup.apply(packet.dst) == null) responseSink.accept(packet);
        else pending.add(packet);
    }

    void add(Id from, Id to, long messageId, Request send)
    {
        add(new Packet(from, to, messageId, send));
    }

    void add(Id from, Id to, long replyId, Reply send)
    {
        add(new Packet(from, to, replyId, send));
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
        if (pending.size() == recurring)
            return false;

        Object next = pending.poll();
        if (next == null)
            return false;

        processNext(next);
        return true;
    }

    private void processNext(Object next)
    {
        if (next instanceof Packet)
        {
            Packet deliver = (Packet) next;
            Node on = lookup.apply(deliver.dst);

            // TODO (required, testing): random drop chance independent of partition; also port flaky connections etc. from simulator
            // Drop the message if it goes across the partition
            boolean drop = ((Packet) next).src.id >= 0 &&
                           !(partitionSet.contains(deliver.src) && partitionSet.contains(deliver.dst)
                             || !partitionSet.contains(deliver.src) && !partitionSet.contains(deliver.dst));
            if (drop)
            {
                if (trace.isTraceEnabled())
                    trace.trace("{} DROP[{}] {}", clock++, on.epoch(), deliver);
                return;
            }

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

    public static EnumMap<MessageType, Stats> run(Id[] nodes, Supplier<PendingQueue> queueSupplier, Consumer<Packet> responseSink, AgentExecutor executor, Supplier<RandomSource> randomSupplier, Supplier<LongSupplier> nowSupplier, TopologyFactory topologyFactory, Supplier<Packet> in, Consumer<Runnable> noMoreWorkSignal)
    {
        Topology topology = topologyFactory.toTopology(nodes);
        Map<Id, Node> lookup = new LinkedHashMap<>();
        try
        {
            Cluster sinks = new Cluster(queueSupplier, lookup::get, responseSink);
            TopologyUpdates topologyUpdates = new TopologyUpdates(executor);
            TopologyRandomizer configRandomizer = new TopologyRandomizer(randomSupplier, topology, topologyUpdates, lookup::get);
            for (Id node : nodes)
            {
                MessageSink messageSink = sinks.create(node, randomSupplier.get());
                BurnTestConfigurationService configService = new BurnTestConfigurationService(node, executor, randomSupplier, topology, lookup::get, topologyUpdates);
                lookup.put(node, new Node(node, messageSink, configService, nowSupplier.get(),
                                          () -> new ListStore(node), new ShardDistributor.EvenSplit<>(8, ignore -> new IntHashKey.Splitter()),
                                          executor.agent(),
                                          randomSupplier.get(), sinks, SizeOfIntersectionSorter.SUPPLIER,
                                          SimpleProgressLog::new, DelayedCommandStores.factory(sinks.pending)));
            }

            // startup
            AsyncResult<?> startup = AsyncChains.reduce(lookup.values().stream().map(Node::start).collect(toList()), (a, b) -> null).beginAsResult();
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
            Scheduled rangeDurable = sinks.recurring(configRandomizer::rangeDurable, 1, SECONDS);
            Scheduled globallyDurable = sinks.recurring(configRandomizer::globallyDurable, 1, SECONDS);

            noMoreWorkSignal.accept(() -> {
                reconfigure.cancel();
                rangeDurable.cancel();
                globallyDurable.cancel();
            });

            Packet next;
            while ((next = in.get()) != null)
                sinks.add(next);

            while (sinks.processPending());

            chaos.cancel();
            reconfigure.cancel();
            rangeDurable.cancel();
            globallyDurable.cancel();
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
                sinks.onDone.forEach(Runnable::run);
                sinks.onDone.clear();
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
