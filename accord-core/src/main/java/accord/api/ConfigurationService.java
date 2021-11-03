package accord.api;

import accord.local.Node;
import accord.topology.Topology;

/**
 * Cluster configuration service. Manages linearizable cluster configuration changes. Any node reporting a
 * configuration with epoch n is guaranteed to have previously seen applied every previous epoch configuration
 */
public interface ConfigurationService
{
    interface Listener
    {
        /**
         * Informs listeners of new topology. This is guaranteed to be called sequentially for each epoch after
         * the initial topology returned by `currentTopology` on startup.
         */
        void onTopologyUpdate(Topology topology);

        /**
         * Called when the given node the configuration service learns that a node has acknowledged a config epoch
         */
        void onEpochAcknowledgement(Node.Id node, long epoch);
    }

    void registerListener(Listener listener);

    /**
     * Return the lowest epoch data exists for
     * @return
     */
    long getEpochLowBound();


    /**
     * Returns the current topology. Also called on startup for the "starting point" topology.
     */
    Topology currentTopology();

    Topology getTopologyForEpoch(long epoch);

    /**
     * if a process becomes aware of a configuration with a higher epoch, it can report it here. The configuration
     * service will fetch the given epoch, and any preceding epochs, and call onComplete when finished.
     */
    void fetchTopologyForEpoch(long epoch, Runnable onComplete);
}
