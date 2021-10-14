package accord.impl.mock;

import accord.api.MessageSink;
import accord.api.TestableConfigurationService;
import accord.local.Node;
import accord.topology.Topology;
import accord.utils.EpochFunction;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;

import java.util.*;

public class MockConfigurationService implements TestableConfigurationService
{
    private final MessageSink messageSink;
    private final List<Topology> epochs = new ArrayList<>();
    private final List<Listener> listeners = new ArrayList<>();
    private final Map<Long, Set<Runnable>> waiting = new HashMap<>();
    private final EpochFunction<MockConfigurationService> fetchTopologyHandler;

    public MockConfigurationService(MessageSink messageSink, EpochFunction<MockConfigurationService> fetchTopologyHandler)
    {
        this.messageSink = messageSink;
        this.fetchTopologyHandler = fetchTopologyHandler;
        epochs.add(Topology.EMPTY);
    }

    public MockConfigurationService(MessageSink messageSink, EpochFunction<MockConfigurationService> fetchTopologyHandler, Topology initialTopology)
    {
        this(messageSink, fetchTopologyHandler);
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
        {
            if (onComplete != null) onComplete.run();
            return;
        }

        Set<Runnable> runnables = waiting.computeIfAbsent(epoch, e -> new HashSet<>());
        if (onComplete != null)
            runnables.add(onComplete);

        fetchTopologyHandler.apply(epoch, this);
    }

    @Override
    public void acknowledgeEpoch(long epoch)
    {
    }

    @Override
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
