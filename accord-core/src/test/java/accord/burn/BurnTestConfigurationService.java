package accord.burn;

import accord.api.ConfigurationService;
import accord.api.MessageSink;
import accord.api.TestableConfigurationService;
import accord.local.Node;
import accord.messages.*;
import accord.topology.Topology;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

// TODO: merge with MockConfigurationService?
public class BurnTestConfigurationService implements TestableConfigurationService
{
    private static final Logger logger = LoggerFactory.getLogger(BurnTestConfigurationService.class);
    private static final Future<Void> SUCCESS = ImmediateFuture.success(null);

    private final Node.Id node;
    private final MessageSink messageSink;
    private final Function<Node.Id, Node> lookup;
    private final Supplier<Random> randomSupplier;
    private final List<Topology> epochs = new ArrayList<>();
    private final List<ConfigurationService.Listener> listeners = new ArrayList<>();

    public BurnTestConfigurationService(Node.Id node, MessageSink messageSink, Supplier<Random> randomSupplier, Topology topology, Function<Node.Id, Node> lookup)
    {
        this.node = node;
        this.messageSink = messageSink;
        this.randomSupplier = randomSupplier;
        this.lookup = lookup;
        epochs.add(Topology.EMPTY);
        epochs.add(topology);
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    @Override
    public synchronized Topology currentTopology()
    {
        return epochs.get(epochs.size() - 1);
    }

    @Override
    public synchronized Topology getTopologyForEpoch(long epoch)
    {
        return epoch >= epochs.size() ? null : epochs.get((int) epoch);
    }

    private static class FetchTopologyRequest implements Request
    {
        private final long epoch;

        public FetchTopologyRequest(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public void process(Node on, Node.Id from, ReplyContext replyContext)
        {
            Topology topology = on.configService().getTopologyForEpoch(epoch);
            on.reply(from, replyContext, new FetchTopologyReply(topology));
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "FetchTopologyRequest{" + epoch + '}';
        }
    }

    private static class FetchTopologyReply implements Reply
    {
        public final Topology topology;

        public FetchTopologyReply(Topology topology)
        {
            this.topology = topology;
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            String epoch = topology == null ? "null" : Long.toString(topology.epoch());
            return "FetchTopologyReply{" + epoch + '}';
        }
    }

    private class FetchTopology extends AsyncPromise<Void> implements Callback<FetchTopologyReply>
    {
        private final FetchTopologyRequest request;
        private final List<Node.Id> candidates;

        private final Set<Runnable> onComplete = new HashSet<>();

        public FetchTopology(long epoch)
        {
            this.request = new FetchTopologyRequest(epoch);
            this.candidates = new ArrayList<>();
            sendNext();
        }

        synchronized void sendNext()
        {
            if (candidates.isEmpty())
            {
                candidates.addAll(currentTopology().nodes());
                candidates.remove(node);
            }
            int idx = randomSupplier.get().nextInt(candidates.size());
            Node.Id node = candidates.remove(idx);
            messageSink.send(node, request, this);
        }

        @Override
        public void onSuccess(Node.Id from, FetchTopologyReply response)
        {
            if (response.topology != null)
                reportTopology(response.topology);
            else
                sendNext();
        }

        @Override
        public synchronized void onFailure(Node.Id from, Throwable throwable)
        {
            sendNext();
        }
    }

    private final Map<Long, FetchTopology> pendingEpochs = new HashMap<>();

    @Override
    public synchronized Future<Void> fetchTopologyForEpoch(long epoch)
    {
        if (epoch < epochs.size())
        {
            return SUCCESS;
        }

        FetchTopology fetch = pendingEpochs.computeIfAbsent(epoch, FetchTopology::new);
        return fetch;
    }

    @Override
    public void acknowledgeEpoch(long epoch)
    {
        Topology topology = getTopologyForEpoch(epoch);
        Node originator = lookup.apply(node);
        TopologyUpdate.syncEpoch(originator, epoch - 1, topology.nodes());
    }

    @Override
    public synchronized void reportTopology(Topology topology)
    {
        if (topology.epoch() < epochs.size())
            return;

        if (topology.epoch() > epochs.size())
        {
            fetchTopologyForEpoch(epochs.size() + 1).addListener(() -> reportTopology(topology));
            return;
        }
        logger.trace("Epoch {} received by {}", topology.epoch(), node);

        epochs.add(topology);

        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);

        FetchTopology fetch = pendingEpochs.remove(topology.epoch());
        if (fetch == null)
            return;

        fetch.setSuccess(null);
    }

}
