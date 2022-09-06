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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.api.MessageSink;
import accord.burn.BurnTestConfigurationService;
import accord.local.CommandStores;
import accord.impl.SimpleProgressLog;
import accord.impl.InMemoryCommandStores;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Scheduler;
import accord.impl.TopologyFactory;
import accord.impl.list.ListAgent;
import accord.impl.list.ListStore;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;
import accord.topology.TopologyRandomizer;
import accord.topology.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster implements Scheduler
{
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);
    public static final Logger trace = LoggerFactory.getLogger("accord.impl.basic.Trace");

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

    NodeSink create(Id self, Random random)
    {
        NodeSink sink = new NodeSink(self, lookup, this, random);
        sinks.put(self, sink);
        return sink;
    }

    private void add(Packet packet)
    {
        boolean isReply = packet.message instanceof Reply;
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

    public boolean processPending()
    {
        if (pending.size() == recurring)
            return false;

        Object next = pending.poll();
        if (next == null)
            return false;

        if (next instanceof Packet)
        {
            Packet deliver = (Packet) next;
            Node on = lookup.apply(deliver.dst);

            // TODO (soon): random drop chance independent of partition; also port flaky connections etc. from simulator
            // Drop the message if it goes across the partition
            boolean drop = ((Packet) next).src.id >= 0 &&
                    !(partitionSet.contains(deliver.src) && partitionSet.contains(deliver.dst)
                            || !partitionSet.contains(deliver.src) && !partitionSet.contains(deliver.dst));
            if (drop)
            {
                trace.trace("{} DROP[{}] {}", clock++, on.epoch(), deliver);
                return true;
            }
            trace.trace("{} RECV[{}] {}", clock++, on.epoch(), deliver);
            if (deliver.message instanceof Reply)
            {
                Reply reply = (Reply) deliver.message;
                Callback callback = reply.isFinal() ? sinks.get(deliver.dst).callbacks.remove(deliver.replyId)
                                                    : sinks.get(deliver.dst).callbacks.get(deliver.replyId);

                if (callback != null)
                {
                    on.scheduler().now(() -> {
                        try
                        {
                            callback.onSuccess(deliver.src, reply);
                        }
                        catch (Throwable t)
                        {
                            callback.onCallbackFailure(deliver.src, t);
                            on.agent().onUncaughtException(t);
                        }
                    });
                }
            }
            else on.receive((Request) deliver.message, deliver.src, deliver);
        }
        else
        {
            ((Runnable) next).run();
        }
        return true;
    }

    @Override
    public Scheduled recurring(Runnable run, long delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(pending, run, delay, units);
        ++recurring;
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

    // TODO (now): there remains some inconsistency of execution, at least causing different partitions if prior runs have happened;
    //             unclear what source is, but might be deterministic based on prior runs (some evidence of this), or non-deterministic
    public static void run(Id[] nodes, Supplier<PendingQueue> queueSupplier, Consumer<Packet> responseSink, Consumer<Throwable> onFailure, Supplier<Random> randomSupplier, Supplier<LongSupplier> nowSupplier, TopologyFactory topologyFactory, Supplier<Packet> in)
    {
        Topology topology = topologyFactory.toTopology(nodes);
        Map<Id, Node> lookup = new LinkedHashMap<>();
        TopologyRandomizer configRandomizer = new TopologyRandomizer(randomSupplier, topology, lookup::get);
        try
        {
            Cluster sinks = new Cluster(queueSupplier, lookup::get, responseSink);
            for (Id node : nodes)
            {
                MessageSink messageSink = sinks.create(node, randomSupplier.get());
                BurnTestConfigurationService configService = new BurnTestConfigurationService(node, messageSink, randomSupplier, topology, lookup::get);
                lookup.put(node, new Node(node, messageSink, configService,
                                          nowSupplier.get(), () -> new ListStore(node), new ListAgent(onFailure),
                                          randomSupplier.get(), sinks, SimpleProgressLog::new, InMemoryCommandStores.Synchronized::new));
            }

            List<Id> nodesList = new ArrayList<>(Arrays.asList(nodes));
            Random shuffleRandom = randomSupplier.get();
            sinks.recurring(() -> {
                Collections.shuffle(nodesList, shuffleRandom);
                int partitionSize = shuffleRandom.nextInt((topologyFactory.rf+1)/2);
                sinks.partitionSet = new LinkedHashSet<>(nodesList.subList(0, partitionSize));
            }, 5L, TimeUnit.SECONDS);
            sinks.recurring(configRandomizer::maybeUpdateTopology, 1L, TimeUnit.SECONDS);

            Packet next;
            while ((next = in.get()) != null)
                sinks.add(next);

            while (sinks.processPending());
            sinks.onDone.forEach(Runnable::run);
            sinks.onDone.clear();
            while (sinks.processPending());
            System.out.println();
        }
        finally
        {
            lookup.values().forEach(Node::shutdown);
        }
    }
}
