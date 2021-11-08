package accord.impl.mock;

import accord.api.ConfigurationService;
import accord.api.MessageSink;
import accord.topology.Topology;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;

import java.util.*;

public class MockConfigurationService implements ConfigurationService
{
    private long epochLowBound = 0;
    private final MessageSink messageSink;
    private final List<Topology> epochs = new ArrayList<>();
    private final List<Listener> listeners = new ArrayList<>();
    private final Map<Long, Set<Runnable>> waiting = new HashMap<>();

    public MockConfigurationService(MessageSink messageSink)
    {
        this.messageSink = messageSink;
        this.epochLowBound = 0;
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
    public synchronized long getEpochLowBound()
    {
        return epochLowBound;
    }

    @Override
    public synchronized Topology currentTopology()
    {
        return epochs.get(epochs.size() - 1);
    }

    @Override
    public synchronized Topology getTopologyForEpoch(long epoch)
    {
        return epoch < epochLowBound || epoch >= epochs.size() ? null : epochs.get((int) epoch);
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
}
