package accord.maelstrom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.coordinate.Timeout;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Scheduler;
import accord.topology.Topology;
import accord.utils.ThreadPoolScheduler;
import accord.maelstrom.Packet.Type;
import accord.api.MessageSink;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;

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
                        info.callback.onFailure(info.to, new Timeout());
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
        public void send(Id to, Request send, Callback callback)
        {
            long messageId = nextMessageId.incrementAndGet();
            callbacks.put(messageId, new CallbackInfo(callback, to, nowSupplier.getAsLong() + 1000L));
            send(new Packet(self, to, messageId, send));
        }

        @Override
        public void reply(Id replyToNode, long replyToMessage, Reply reply)
        {
            send(new Packet(self, replyToNode, replyToMessage, reply));
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
            on = new Node(init.self, topology, sink, new Random(), System::currentTimeMillis, MaelstromStore::new, MaelstromAgent.INSTANCE, scheduler, CommandStore.Factory.SINGLE_THREAD);
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
                        on.receive((MaelstromRequest)next.body, next.src, next.body.msg_id);
                        break;
                    default:
                        if (next.body.in_reply_to > Body.SENTINEL_MSG_ID)
                        {
                            Reply reply = (Reply)((Wrapper)next.body).body;
                            CallbackInfo callback = reply.isFinal() ? sink.callbacks.remove(next.body.in_reply_to)
                                    : sink.callbacks.get(next.body.in_reply_to);
                            if (callback != null)
                                scheduler.now(() -> callback.callback.onSuccess(next.src, reply));
                        }
                        else on.receive((Request)((Wrapper)next.body).body, next.src, next.body.msg_id);
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
