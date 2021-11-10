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

        /**
         * Called when accord data associated with a superseded epoch has been sync'd across current replicas
         */
        void onEpochSyncComplete(Node.Id node, long epoch);
    }

    void registerListener(Listener listener);

    /**
     * Returns the current topology. Also called on startup for the "starting point" topology.
     */
    Topology currentTopology();

    default long currentEpoch()
    {
        return currentTopology().epoch();
    }

    Topology getTopologyForEpoch(long epoch);

    /**
     * if a process becomes aware of a configuration with a higher epoch, it can report it here. The configuration
     * service will fetch the given epoch, and any preceding epochs, and call onComplete when finished.
     */
    void fetchTopologyForEpoch(long epoch, Runnable onComplete);

    default void fetchTopologyForEpoch(long epoch)
    {
        fetchTopologyForEpoch(epoch, null);
    }

    /**
     * Called after this node learns of an epoch as part of the {@code Listener#onTopologyUpdate} call. Indicates
     * the new epoch has been setup locally and the node is ready to process commands for it.
     */
    void acknowledgeEpoch(long epoch);
}
