package accord.api;

import accord.local.Node;
import accord.topology.Topology;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * ConfigurationService is responsible for:
 *  - coordinating linearizable configuration / topology changes across the cluster
 *  - notifying the local node of topology changes
 *  - syncing transaction metadata between replicas on epoch changes
 *  - updating the fast path electorate when appropriate
 *  - acting as a database of previous epoch configurations
 *
 * The high level flow of a topology change is as follows:
 *
 *  - ConfigurationService becomes aware of a new epoch/Topology
 *
 *  - ConfigurationService notifies the node of the new configuration service by calling
 *      {@link accord.api.ConfigurationService.Listener#onTopologyUpdate(accord.topology.Topology)}
 *
 *  - Once the node has setup the new topology, it will call {@link accord.api.ConfigurationService#acknowledgeEpoch(long)}
 *      which indicates the node will no longer create txnIds for the previous epoch, and it's commits can now be synced
 *      with other replicas.
 *
 *  - ConfigurationService is now expected to begin syncing fast path decisions made in the previous epoch to other
 *      replicas. In the case of a replication change (new replica, ownership movement), the configuration service
 *      needs to notify the new replica of the transaction history of the relevant ranges.
 *
 *  - Once the previous epoch data has been synced to other nodes, the configuration service needs to notify the other
 *      nodes that this node has synced data for the previous epoch.
 *
 *  - ConfigurationService will notify the node when other nodes complete syncing an epoch by calling
 *      {@link accord.api.ConfigurationService.Listener#onEpochSyncComplete(accord.local.Node.Id, long)}
 *
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
         * Called when accord data associated with a superseded epoch has been sync'd across current replicas. Before
         * calling this, implementations need to ensure any new electorates are aware of all fast path decisions made
         * in previous epochs, and that replicas of new ranges have learned of the transaction history for their
         * replicated ranges.
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
     * Method for reporting epochs the configuration service may not be aware of. To be notified when the new epoch
     * is available locally, use {@link accord.topology.TopologyManager#awaitEpoch(long)}
     */
    void fetchTopologyForEpoch(long epoch);

    /**
     * Alert the configuration service of epochs it may not be aware of. This is called called for every TxnRequest
     * received by Accord, so implementations should be lightweight, and avoid blocking or heavy computation.
     */
    default void reportEpoch(long epoch)
    {
        fetchTopologyForEpoch(epoch);
    }

    /**
     * Called after this node learns of an epoch as part of the {@code Listener#onTopologyUpdate} call. Indicates
     * the new epoch has been setup locally and the node is ready to process commands for it.
     */
    void acknowledgeEpoch(long epoch);
}
