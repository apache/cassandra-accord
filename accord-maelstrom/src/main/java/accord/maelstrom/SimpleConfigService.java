package accord.maelstrom;

import accord.api.ConfigurationService;
import accord.topology.Topology;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

public class SimpleConfigService implements ConfigurationService
{
    private static final Future<Void> SUCCESS = ImmediateFuture.success(null);
    private final Topology topology;

    public SimpleConfigService(Topology topology)
    {
        this.topology = topology;
    }

    @Override
    public void registerListener(Listener listener)
    {

    }

    @Override
    public Topology currentTopology()
    {
        return topology;
    }

    @Override
    public Topology getTopologyForEpoch(long epoch)
    {
        assert epoch == topology.epoch();
        return topology;
    }

    @Override
    public void fetchTopologyForEpoch(long epoch)
    {
        return;
    }

    @Override
    public void acknowledgeEpoch(long epoch)
    {

    }
}

