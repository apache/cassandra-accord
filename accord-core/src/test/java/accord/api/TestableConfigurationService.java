package accord.api;

import accord.topology.Topology;

public interface TestableConfigurationService extends ConfigurationService
{
    void reportTopology(Topology topology);
}
