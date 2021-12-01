package accord.impl.mock;

import accord.api.MessageSink;
import accord.api.TestableConfigurationService;
import accord.local.Node;
import accord.topology.Topology;
import accord.utils.EpochFunction;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.junit.jupiter.api.Assertions;

import java.util.*;

public class MockConfigurationService implements TestableConfigurationService
{
    private static final Future<Void> SUCCESS = ImmediateFuture.success(null);
    private final MessageSink messageSink;
    private final List<Topology> epochs = new ArrayList<>();
    private final List<Listener> listeners = new ArrayList<>();
    private final Map<Long, AsyncPromise<Void>> pending = new HashMap<>();
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
    public synchronized Future<Void> fetchTopologyForEpoch(long epoch)
    {
        if (epoch < epochs.size())
        {
            return SUCCESS;
        }

        Future<Void> future = pending.computeIfAbsent(epoch, e -> new AsyncPromise<>());
        fetchTopologyHandler.apply(epoch, this);
        return future;
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

        AsyncPromise<Void> promise = pending.remove(topology.epoch());
        if (promise == null)
            return;
        promise.setSuccess(null);
    }

    public synchronized void reportSyncComplete(Node.Id node, long epoch)
    {
        for (Listener listener : listeners)
            listener.onEpochSyncComplete(node, epoch);
    }
}
