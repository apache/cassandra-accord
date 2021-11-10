package accord.impl.mock;

import accord.api.ConfigurationService;
import accord.api.MessageSink;
import accord.local.Node;
import accord.messages.Request;
import accord.topology.Topology;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;

import java.util.*;

public class MockConfigurationService implements ConfigurationService
{
    private final MessageSink messageSink;
    private final List<Topology> epochs = new ArrayList<>();
    private final List<Listener> listeners = new ArrayList<>();
    private final Map<Long, Set<Runnable>> waiting = new HashMap<>();

    public MockConfigurationService(MessageSink messageSink)
    {
        this.messageSink = messageSink;
        epochs.add(Topology.EMPTY);
    }

    public MockConfigurationService(MessageSink messageSink, Topology initialTopology)
    {
        this(messageSink);
        reportTopology(initialTopology);
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

    @Override
    public synchronized void fetchTopologyForEpoch(long epoch, Runnable onComplete)
    {
        if (epoch < epochs.size())
            onComplete.run();

        Set<Runnable> runnables = waiting.computeIfAbsent(epoch, e -> new HashSet<>());
        if (onComplete != null)
            runnables.add(onComplete);
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
        Assertions.assertEquals(topology.epoch(), epochs.size());
        epochs.add(topology);

        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);

        Set<Runnable> runnables = waiting.remove(topology.epoch());
        if (runnables == null)
            return;

        runnables.forEach(Runnable::run);
    }

    public synchronized Set<Long> pendingEpochs()
    {
        return ImmutableSet.copyOf(waiting.keySet());
    }

    public synchronized void reportSyncComplete(Node.Id node, long epoch)
    {
        for (Listener listener : listeners)
            listener.onEpochSyncComplete(node, epoch);
    }
}
