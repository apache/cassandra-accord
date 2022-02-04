package accord.api;

import accord.local.Node;
import accord.topology.Topology;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Cluster configuration service. Manages linearizable cluster configuration changes. Any node reporting a
 * configuration with epoch n is guaranteed to have previously seen and applied every previous epoch configuration
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
     * Method for reporting epochs the configuration service may not be aware of, and optionally running a supplied
     * runnable once the corresponding topology has been received and applied. If the configuration service is already
     * aware of the reported epoch, the runnable should be run immediately.
     */
    void fetchTopologyForEpoch(long epoch, Runnable onComplete);

    /**
     * Alert the configuration service of epochs it may not be aware of. This is called called for every TxnRequest
     * received by Accord, so implementations should be lightweight, and avoid blocking or heavy computation.
     */
    default void reportEpoch(long epoch)
    {
        fetchTopologyForEpoch(epoch, null);
    }

    default CompletionStage<Void> fetchTopologyForEpochStage(long epoch)
    {
        CompletableFuture<Void> result = new CompletableFuture<>();
        fetchTopologyForEpoch(epoch, () -> result.complete(null));
        return result;
    }

    /**
     * Called after this node learns of an epoch as part of the {@code Listener#onTopologyUpdate} call. Indicates
     * the new epoch has been setup locally and the node is ready to process commands for it.
     */
    void acknowledgeEpoch(long epoch);
}
