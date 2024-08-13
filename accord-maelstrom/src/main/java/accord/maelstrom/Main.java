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
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.api.LocalConfig;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.Timeout;
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
import accord.maelstrom.Packet.Type;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Reply.FailureReply;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.topology.Topology;
import accord.utils.DefaultRandom;
import accord.utils.ThreadPoolScheduler;

import static accord.utils.async.AsyncChains.awaitUninterruptibly;

public class Main
{
    static class CallbackInfo
    {
        final Callback callback;
        final Id to;
        final long timeout;

        CallbackInfo(Callback callback, Id to, long timeout)
        {
            this.callback = callback;
            this.to = to;
            this.timeout = timeout;
        }
    }

    public static class StdoutSink implements MessageSink
    {
        private final AtomicLong nextMessageId = new AtomicLong(1);
        private final Map<Long, CallbackInfo> callbacks = new ConcurrentHashMap<>();

        final LongSupplier nowSupplier;
        final Scheduler scheduler;
        final long start;
        final Id self;
        final PrintStream out, err;

        public StdoutSink(LongSupplier nowSupplier, Scheduler scheduler, long start, Id self, PrintStream stdout, PrintStream stderr)
        {
            this.nowSupplier = nowSupplier;
            this.scheduler = scheduler;
            this.start = start;
            this.self = self;
            this.out = stdout;
            this.err = stderr;
            this.scheduler.recurring(() -> {
                long now = nowSupplier.getAsLong();
                callbacks.forEach((messageId, info) -> {
                    if (info.timeout < now && callbacks.remove(messageId, info))
                        info.callback.onFailure(info.to, new Timeout(null, null));
                });
            }, 1L, TimeUnit.SECONDS);
        }

        private void send(Packet packet)
        {
            err.println("Sending " + (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)) + " " + packet);
            err.flush();
            out.println(packet);
            out.flush();
        }

        public synchronized void send(Id to, Body body)
        {
            send(new Packet(self, to, body));
        }

        @Override
        public synchronized void send(Id to, Request send)
        {
            send(new Packet(self, to, Body.SENTINEL_MSG_ID, send));
        }

        @Override
        public void send(Id to, Request send, AgentExecutor ignored, Callback callback)
        {
            // Executor is ignored due to the fact callbacks are applied in a single thread already
            long messageId = nextMessageId.incrementAndGet();
            callbacks.put(messageId, new CallbackInfo(callback, to, nowSupplier.getAsLong() + 1000L));
            send(new Packet(self, to, messageId, send));
        }

        @Override
        public void reply(Id replyToNode, ReplyContext replyContext, Reply reply)
        {
            send(new Packet(self, replyToNode, MaelstromReplyContext.messageIdFor(replyContext), reply));
        }

        @Override
        public void replyWithUnknownFailure(Id replyingToNode, ReplyContext replyContext, Throwable failure)
        {
            reply(replyingToNode, replyContext, new FailureReply(failure));
        }
    }

    public static void listen(TopologyFactory topologyFactory, InputStream stdin, PrintStream out, PrintStream err) throws IOException
    {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(stdin)))
        {
            listen(topologyFactory, () -> {
                try
                {
                    return in.readLine();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }, out, err);
        }
    }

    public static void listen(TopologyFactory topologyFactory, Supplier<String> in, PrintStream out, PrintStream err) throws IOException
    {
        long start = System.nanoTime();
        err.println("Starting...");
        err.flush();
        ThreadPoolScheduler scheduler = new ThreadPoolScheduler();
        Node on;
        Topology topology;
        StdoutSink sink;
        {
            String line = in.get();
            err.println("Received " + (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)) + " " + line);
            err.flush();
            Packet packet = Json.GSON.fromJson(line, Packet.class);
            MaelstromInit init = (MaelstromInit) packet.body;
            topology = topologyFactory.toTopology(init.cluster);
            sink = new StdoutSink(System::currentTimeMillis, scheduler, start, init.self, out, err);
            LocalConfig localConfig = LocalConfig.DEFAULT;
            on = new Node(init.self, sink, new SimpleConfigService(topology),
                          System::currentTimeMillis, NodeTimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MILLISECONDS, System::currentTimeMillis),
                          MaelstromStore::new, new ShardDistributor.EvenSplit(8, ignore -> new MaelstromKey.Splitter()),
                          MaelstromAgent.INSTANCE, new DefaultRandom(), scheduler, SizeOfIntersectionSorter.SUPPLIER,
                          DefaultRemoteListeners::new, DefaultRequestTimeouts::new, DefaultProgressLogs::new, DefaultLocalListeners.Factory::new,
                          InMemoryCommandStores.SingleThread::new, new CoordinationAdapter.DefaultFactory(),
                          localConfig);
            awaitUninterruptibly(on.unsafeStart());
            err.println("Initialized node " + init.self);
            err.flush();
            sink.send(packet.src, new Body(Type.init_ok, Body.SENTINEL_MSG_ID, init.msg_id));
        }
        try
        {
            while (true)
            {
                String line = in.get();
                if (line == null)
                {
                    err.println("Received EOF; terminating");
                    err.flush();
                    scheduler.stop();
                    err.println("Terminated");
                    err.flush();
                    return;
                }
                err.println("Received " + (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)) + " " + line);
                err.flush();
                Packet next = Packet.parse(line);
                switch (next.body.type)
                {
                    case txn:
                        on.receive((MaelstromRequest)next.body, next.src, MaelstromReplyContext.contextFor(next.body.msg_id));
                        break;
                    default:
                        if (next.body.in_reply_to > Body.SENTINEL_MSG_ID)
                        {
                            Reply reply = (Reply)((Wrapper)next.body).body;
                            CallbackInfo callback = sink.callbacks.remove(next.body.in_reply_to);
                            if (callback != null)
                                scheduler.now(() -> {
                                    try
                                    {
                                        callback.callback.onSuccess(next.src, reply);
                                    }
                                    catch (Throwable t)
                                    {
                                        callback.callback.onCallbackFailure(next.src, t);
                                    }
                                });
                        }
                        else on.receive((Request)((Wrapper)next.body).body, next.src, MaelstromReplyContext.contextFor(next.body.msg_id));
                }
            }
        }
        finally
        {
            on.shutdown();
        }
    }

    public static void main(String[] args) throws IOException
    {
        listen(new TopologyFactory(64, 3), System.in, System.out, System.err);
    }
}
