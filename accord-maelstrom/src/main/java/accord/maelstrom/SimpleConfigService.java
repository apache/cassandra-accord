package accord.maelstrom;

import accord.api.ConfigurationService;
import accord.topology.Topology;

public class SimpleConfigService implements ConfigurationService
{
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
    public long getEpochLowBound()
    {
        return 0;
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
    public void fetchTopologyForEpoch(long epoch, Runnable onComplete)
    {

    }
}
