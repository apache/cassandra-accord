package accord.burn;

import accord.api.ConfigurationService;
import accord.api.MessageSink;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;
import accord.topology.Topology;

import java.util.*;
import java.util.function.Supplier;

// TODO: merge with MockConfigurationService?
public class RandomConfigurationService implements ConfigurationService
{
    private final Node.Id node;
    private final MessageSink messageSink;
    private final Supplier<Random> randomSupplier;
    private final List<Topology> epochs = new ArrayList<>();
    private final List<ConfigurationService.Listener> listeners = new ArrayList<>();

    public RandomConfigurationService(Node.Id node, MessageSink messageSink, Supplier<Random> randomSupplier, Topology topology)
    {
        this.node = node;
        this.messageSink = messageSink;
        this.randomSupplier = randomSupplier;
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
        public void process(Node on, Node.Id from, long messageId)
        {
            Topology topology = on.configService().getTopologyForEpoch(epoch);
            on.reply(from, messageId, new FetchTopologyReply(topology));
            if (topology == null)
                on.configService().fetchTopologyForEpoch(epoch);
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
        public String toString()
        {
            String epoch = topology == null ? "null" : Long.toString(topology.epoch());
            return "FetchTopologyReply{" + epoch + '}';
        }
    }

    private class FetchTopology implements Callback<FetchTopologyReply>
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

        void onComplete(Runnable runnable)
        {
            onComplete.add(runnable);
        }

        synchronized void fireCallbacks()
        {
            onComplete.forEach(Runnable::run);
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
    public synchronized void fetchTopologyForEpoch(long epoch, Runnable onComplete)
    {
        if (epoch < epochs.size())
        {
            onComplete.run();
            return;
        }

        FetchTopology fetch = pendingEpochs.computeIfAbsent(epoch, FetchTopology::new);
        if (onComplete != null)
            fetch.onComplete(onComplete);
    }

    public static class EpochAcknowledgeMessage implements Request
    {
        public final long epoch;

        public EpochAcknowledgeMessage(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public void process(Node node, Node.Id from, long messageId)
        {
            node.onEpochAcknowledgement(from, epoch);
        }

        @Override
        public String toString()
        {
            return "EpochAcknowledgeMessage{" + epoch + '}';
        }
    }

    @Override
    public void acknowledgeEpoch(long epoch)
    {
        EpochAcknowledgeMessage message = new EpochAcknowledgeMessage(epoch);
        Topology topology = getTopologyForEpoch(epoch);
        topology.nodes().forEach(to -> messageSink.send(to, message));
    }

    public synchronized void reportTopology(Topology topology)
    {
        if (topology.epoch() < epochs.size())
            return;

        if (topology.epoch() > epochs.size())
        {
            fetchTopologyForEpoch(epochs.size() + 1, () -> reportTopology(topology));
            return;
        }

        epochs.add(topology);

        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);

        FetchTopology fetch = pendingEpochs.remove(topology.epoch());
        if (fetch == null)
            return;

        fetch.fireCallbacks();
    }

}
