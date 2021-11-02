package accord.impl.basic;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import accord.impl.mock.MockConfigurationService;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Scheduler;
import accord.impl.TopologyFactory;
import accord.impl.list.ListAgent;
import accord.impl.list.ListStore;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;
import accord.topology.Topology;

public class Cluster implements Scheduler
{
    final Function<Id, Node> lookup;
    final PendingQueue pending;
    final Consumer<Packet> responseSink;
    final Map<Id, NodeSink> sinks = new HashMap<>();
    final PrintWriter err;
    int clock;
    int recurring;
    Set<Id> partitionSet;

    public Cluster(Supplier<PendingQueue> queueSupplier, Function<Id, Node> lookup, Consumer<Packet> responseSink, OutputStream stderr)
    {
        this.pending = queueSupplier.get();
        this.lookup = lookup;
        this.responseSink = responseSink;
        this.err = new PrintWriter(stderr);
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
        err.println(clock++ + " SEND " + packet);
        err.flush();
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
            // Drop the message if it goes across the partition
            boolean drop = ((Packet) next).src.id >= 0 &&
                           !(partitionSet.contains(deliver.src) && partitionSet.contains(deliver.dst)
                             || !partitionSet.contains(deliver.src) && !partitionSet.contains(deliver.dst));
            if (drop)
            {
                err.println(clock++ + " DROP " + deliver);
                err.flush();
                return true;
            }
            err.println(clock++ + " RECV " + deliver);
            err.flush();
            if (deliver.message instanceof Reply)
            {
                Reply reply = (Reply) deliver.message;
                Callback callback = reply.isFinal() ? sinks.get(deliver.dst).callbacks.remove(deliver.replyId)
                                                    : sinks.get(deliver.dst).callbacks.get(deliver.replyId);
                if (callback != null)
                    on.scheduler().now(() -> callback.onSuccess(deliver.src, reply));
            }
            else on.receive((Request)deliver.message, deliver.src, deliver.requestId);
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
        RecurringPendingRunnable result = new RecurringPendingRunnable(pending, run, true, delay, units);
        ++recurring;
        pending.add(result, delay, units);
        return result;
    }

    @Override
    public Scheduled once(Runnable run, long delay, TimeUnit units)
    {
        RecurringPendingRunnable result = new RecurringPendingRunnable(null, run, false, delay, units);
        pending.add(result, delay, units);
        return result;
    }

    @Override
    public void now(Runnable run)
    {
        run.run();
    }

    public static void run(Id[] nodes, Supplier<PendingQueue> queueSupplier, Consumer<Packet> responseSink, Supplier<Random> randomSupplier, Supplier<LongSupplier> nowSupplier, TopologyFactory topologyFactory, Supplier<Packet> in, OutputStream stderr)
    {
        Topology topology = topologyFactory.toTopology(nodes);
        Map<Id, Node> lookup = new HashMap<>();
        try
        {
            Cluster sinks = new Cluster(queueSupplier, lookup::get, responseSink, stderr);
            for (Id node : nodes)
            {
                MessageSink messageSink = sinks.create(node, randomSupplier.get());
                MockConfigurationService configurationService = new MockConfigurationService(messageSink, topology);
                lookup.put(node, new Node(node, messageSink, configurationService, randomSupplier.get(),
                                          nowSupplier.get(), ListStore::new, ListAgent.INSTANCE, sinks, CommandStore.Factory.SYNCHRONIZED));
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
