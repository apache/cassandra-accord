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

package accord.maelstrom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.api.LocalConfig;
import accord.coordinate.CoordinationAdapter;
import accord.impl.DefaultRequestTimeouts;
import accord.impl.InMemoryCommandStores;
import accord.impl.DefaultLocalListeners;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.impl.DefaultRemoteListeners;
import accord.impl.SizeOfIntersectionSorter;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.ShardDistributor;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Reply.FailureReply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.SafeCallback;
import accord.topology.Topology;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;

import static java.util.stream.Collectors.toList;

// TODO (low priority, testing): merge with accord.impl.basic.Cluster
public class Cluster implements Scheduler
{
    public interface Queue<T>
    {
        void add(T item);
        void add(T item, long delay, TimeUnit units);
        T poll();
        int size();
    }

    public interface QueueSupplier
    {
        <T> Queue<T> get();
    }

    public static class InstanceSink implements MessageSink
    {
        final Id self;
        final Function<Id, Node> lookup;
        final Cluster parent;
        final RandomSource random;

        int nextMessageId = 0;
        Map<Long, SafeCallback> callbacks = new LinkedHashMap<>();

        public InstanceSink(Id self, Function<Id, Node> lookup, Cluster parent, RandomSource random)
        {
            this.self = self;
            this.lookup = lookup;
            this.parent = parent;
            this.random = random;
        }

        @Override
        public synchronized void send(Id to, Request send)
        {
            parent.add(self, to, Body.SENTINEL_MSG_ID, send);
        }

        @Override
        public void send(Id to, Request send, AgentExecutor executor, Callback callback)
        {
            long messageId = nextMessageId++;
            SafeCallback sc = new SafeCallback(executor, callback);
            callbacks.put(messageId, sc);
            parent.add(self, to, messageId, send);
            parent.pending.add((Runnable)() -> {
                if (sc == callbacks.remove(messageId))
                    sc.timeout(to);
            }, 1000 + random.nextInt(10000), TimeUnit.MILLISECONDS);
        }

        @Override
        public void reply(Id replyToNode, ReplyContext replyContext, Reply reply)
        {
            long replyToMessage = ((Packet) replyContext).body.msg_id;
            parent.add(self, replyToNode, replyToMessage, reply);
        }

        @Override
        public void replyWithUnknownFailure(Id replyingToNode, ReplyContext replyContext, Throwable failure)
        {
            reply(replyingToNode, replyContext, new FailureReply(failure));
        }
    }

    final Function<Id, Node> lookup;
    final Queue<Object> pending;
    final Consumer<Packet> responseSink;
    final Map<Id, InstanceSink> sinks = new HashMap<>();
    final PrintWriter err;
    int clock;
    int recurring;
    Set<Id> partitionSet;

    public Cluster(QueueSupplier queueSupplier, Function<Id, Node> lookup, Consumer<Packet> responseSink, OutputStream stderr)
    {
        this.pending = queueSupplier.get();
        this.lookup = lookup;
        this.responseSink = responseSink;
        this.err = new PrintWriter(stderr);
        this.partitionSet = new HashSet<>();
    }

    InstanceSink create(Id self, RandomSource random)
    {
        InstanceSink sink = new InstanceSink(self, lookup, this, random);
        sinks.put(self, sink);
        return sink;
    }

    private void add(Packet packet)
    {
        if (packet == null)
            throw new IllegalArgumentException();

        err.println(clock++ + " SEND " + packet);
        err.flush();
        if (lookup.apply(packet.dest) == null) responseSink.accept(packet);
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
            Node on = lookup.apply(deliver.dest);
            switch (deliver.body.type)
            {
                case init:
                    throw new IllegalStateException();
                case txn:
                    err.println(clock++ + " RECV " + deliver);
                    err.flush();
                    on.receive((MaelstromRequest)deliver.body, deliver.src, deliver);
                    break;
                default:
                    // Drop the message if it goes across the partition
                    boolean drop = !(partitionSet.contains(deliver.src) && partitionSet.contains(deliver.dest)
                                    || !partitionSet.contains(deliver.src) && !partitionSet.contains(deliver.dest));
                    if (drop)
                    {
                        err.println(clock++ + " DROP " + deliver);
                        err.flush();
                        break;
                    }
                    err.println(clock++ + " RECV " + deliver);
                    err.flush();
                    Object body = ((Wrapper)deliver.body).body;
                    // for some reason InformOfTxnReply has deliver.body.in_reply_to == Body.SENTINEL_MSG_ID, so is unique
                    // for all reply types
                    if (deliver.body.in_reply_to > Body.SENTINEL_MSG_ID || body instanceof Reply)
                    {
                        Reply reply = (Reply) body;
                        SafeCallback callback = sinks.get(deliver.dest).callbacks.remove(deliver.body.in_reply_to);
                        if (callback != null)
                        {
                            if (reply instanceof Reply.FailureReply) callback.failure(deliver.src, ((Reply.FailureReply) reply).failure);
                            else callback.success(deliver.src, reply);
                        }
                    }
                    else on.receive((Request) body, deliver.src, deliver);
            }
        }
        else
        {
            ((Runnable) next).run();
        }
        return true;
    }

    class CancellableRunnable implements Runnable, Scheduled
    {
        final boolean recurring;
        final long delay;
        final TimeUnit units;
        Runnable run;

        CancellableRunnable(Runnable run, boolean recurring, long delay, TimeUnit units)
        {
            this.run = run;
            this.recurring = recurring;
            this.delay = delay;
            this.units = units;
        }

        @Override
        public void run()
        {
            if (run != null)
            {
                run.run();
                if (recurring) pending.add(this, delay, units);
                else run = null;
            }
        }

        @Override
        public void cancel()
        {
            run = null;
        }

        @Override
        public boolean isDone()
        {
            return run == null;
        }
    }

    @Override
    public Scheduled recurring(Runnable run, long delay, TimeUnit units)
    {
        CancellableRunnable result = new CancellableRunnable(run, true, delay, units);
        ++recurring;
        pending.add(result, delay, units);
        return result;
    }

    @Override
    public Scheduled once(Runnable run, long delay, TimeUnit units)
    {
        CancellableRunnable result = new CancellableRunnable(run, false, delay, units);
        pending.add(result, delay, units);
        return result;
    }

    @Override
    public void now(Runnable run)
    {
        run.run();
    }

    public static void run(Id[] nodes, QueueSupplier queueSupplier, Consumer<Packet> responseSink, Supplier<RandomSource> randomSupplier, Supplier<LongSupplier> nowSupplierSupplier, TopologyFactory topologyFactory, InputStream stdin, OutputStream stderr) throws IOException
    {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(stdin)))
        {
            run(nodes, queueSupplier, responseSink, randomSupplier, nowSupplierSupplier, topologyFactory, () -> {
                try
                {
                    return Packet.parse(in.readLine());
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }, stderr);
        }
    }

    public static void run(Id[] nodes, QueueSupplier queueSupplier, Consumer<Packet> responseSink, Supplier<RandomSource> randomSupplier, Supplier<LongSupplier> nowSupplierSupplier, TopologyFactory topologyFactory, Supplier<Packet> in, OutputStream stderr)
    {
        Topology topology = topologyFactory.toTopology(nodes);
        Map<Id, Node> lookup = new HashMap<>();
        try
        {
            Cluster sinks = new Cluster(queueSupplier, lookup::get, responseSink, stderr);
            for (Id node : nodes)
            {
                MessageSink messageSink = sinks.create(node, randomSupplier.get());
                LongSupplier nowSupplier = nowSupplierSupplier.get();
                LocalConfig localConfig = LocalConfig.DEFAULT;
                lookup.put(node, new Node(node, messageSink, new SimpleConfigService(topology),
                                          nowSupplier, NodeTimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, nowSupplier),
                                          MaelstromStore::new, new ShardDistributor.EvenSplit(8, ignore -> new MaelstromKey.Splitter()),
                                          MaelstromAgent.INSTANCE,
                                          randomSupplier.get(), sinks, SizeOfIntersectionSorter.SUPPLIER, DefaultRemoteListeners::new, DefaultRequestTimeouts::new,
                                          DefaultProgressLogs::new, DefaultLocalListeners.Factory::new, InMemoryCommandStores.SingleThread::new, new CoordinationAdapter.DefaultFactory(),
                                          localConfig));
            }

            AsyncResult<?> startup = AsyncChains.reduce(lookup.values().stream().map(Node::unsafeStart).collect(toList()), (a, b) -> null).beginAsResult();
            while (sinks.processPending());
            if (!startup.isDone()) throw new AssertionError();

            List<Id> nodesList = new ArrayList<>(Arrays.asList(nodes));
            sinks.recurring(() ->
                            {
                                Collections.shuffle(nodesList, randomSupplier.get().asJdkRandom());
                                int partitionSize = randomSupplier.get().nextInt((topologyFactory.rf+1)/2);
                                sinks.partitionSet = new HashSet<>(nodesList.subList(0, partitionSize));
                            }, 5L, TimeUnit.SECONDS);

            Packet next;
            while ((next = in.get()) != null)
                sinks.add(next);

            while (sinks.processPending());
        }
        finally
        {
            lookup.values().forEach(Node::shutdown);
        }
    }
}
