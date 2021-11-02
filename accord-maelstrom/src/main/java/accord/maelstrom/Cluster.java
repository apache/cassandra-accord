package accord.maelstrom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import accord.coordinate.Timeout;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.MessageSink;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;
import accord.api.Scheduler;
import accord.topology.Topology;

// TODO: merge with accord.impl.basic.Cluster
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
        final Random random;

        int nextMessageId = 0;
        Map<Long, Callback> callbacks = new LinkedHashMap<>();

        public InstanceSink(Id self, Function<Id, Node> lookup, Cluster parent, Random random)
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
        public void send(Id to, Request send, Callback callback)
        {
            long messageId = nextMessageId++;
            callbacks.put(messageId, callback);
            parent.add(self, to, messageId, send);
            parent.pending.add((Runnable)() -> {
                if (callback == callbacks.remove(messageId))
                    callback.onFailure(to, new Timeout());
            }, 1000 + random.nextInt(10000), TimeUnit.MILLISECONDS);
        }

        @Override
        public void reply(Id replyToNode, long replyToMessage, Reply reply)
        {
            parent.add(self, replyToNode, replyToMessage, reply);
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

    InstanceSink create(Id self, Random random)
    {
        InstanceSink sink = new InstanceSink(self, lookup, this, random);
        sinks.put(self, sink);
        return sink;
    }

    private void add(Packet packet)
    {
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
                    on.receive((MaelstromRequest)deliver.body, deliver.src, deliver.body.msg_id);
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
                    if (deliver.body.in_reply_to > Body.SENTINEL_MSG_ID)
                    {
                        Reply reply = (Reply)((Wrapper)deliver.body).body;
                        Callback callback = reply.isFinal() ? sinks.get(deliver.dest).callbacks.remove(deliver.body.in_reply_to)
                                                            : sinks.get(deliver.dest).callbacks.get(deliver.body.in_reply_to);
                        if (callback != null)
                            on.scheduler().now(() -> callback.onSuccess(deliver.src, reply));
                    }
                    else on.receive((Request)((Wrapper)deliver.body).body, deliver.src, deliver.body.msg_id);
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
                if (recurring)
                    pending.add(this, delay, units);
            }
        }

        @Override
        public void cancel()
        {
            run = null;
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

    public static void run(Id[] nodes, QueueSupplier queueSupplier, Consumer<Packet> responseSink, Supplier<Random> randomSupplier, Supplier<LongSupplier> nowSupplier, TopologyFactory topologyFactory, InputStream stdin, OutputStream stderr) throws IOException
    {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(stdin)))
        {
            run(nodes, queueSupplier, responseSink, randomSupplier, nowSupplier, topologyFactory, () -> {
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

    public static void run(Id[] nodes, QueueSupplier queueSupplier, Consumer<Packet> responseSink, Supplier<Random> randomSupplier, Supplier<LongSupplier> nowSupplier, TopologyFactory topologyFactory, Supplier<Packet> in, OutputStream stderr)
    {
        Topology topology = topologyFactory.toTopology(nodes);
        Map<Id, Node> lookup = new HashMap<>();
        try
        {
            Cluster sinks = new Cluster(queueSupplier, lookup::get, responseSink, stderr);
            for (Id node : nodes)
            {
                MessageSink messageSink = sinks.create(node, randomSupplier.get());
                lookup.put(node, new Node(node, messageSink, new SimpleConfigService(topology), randomSupplier.get(),
                                          nowSupplier.get(), MaelstromStore::new, MaelstromAgent.INSTANCE, sinks, CommandStore.Factory.SINGLE_THREAD));
            }

            List<Id> nodesList = new ArrayList<>(Arrays.asList(nodes));
            sinks.recurring(() ->
                            {
                                Collections.shuffle(nodesList, randomSupplier.get());
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
