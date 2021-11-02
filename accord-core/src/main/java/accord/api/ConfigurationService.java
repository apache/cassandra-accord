package accord.api;

import accord.topology.Topology;

/**
 * Cluster configuration service. Manages linearizable cluster configuration changes. Any node reporting a
 * configuration with epoch n is guaranteed to have previously seen applied every previous epoch configuration
 */
public interface ConfigurationService
{
    interface Listener
    {
        void onTopologyUpdate(Topology topology);
        void onEpochLowBoundChange(long epoch);
    }

    void registerListener(Listener listener);

    /**
     * Return the lowest epoch data exists for
     * @return
     */
    long getEpochLowBound();


    Topology currentTopology();

    Topology getTopologyForEpoch(long epoch);

    /**
     * if a process becomes aware of a configuration with a higher epoch, it can report it here. The configuration
     * service will fetch the given epoch, and any preceding epochs, and call onComplete when finished.
     */
    void fetchTopologyForEpoch(long epoch, Runnable onComplete);
}
