/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.api;

import java.util.Collection;
import javax.annotation.Nullable;

import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

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
 *  - Once the node has setup the new topology, it will call {@link accord.api.ConfigurationService#acknowledgeEpoch(EpochReady)}
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
 *      {@link accord.api.ConfigurationService.Listener#onRemoteSyncComplete(accord.local.Node.Id, long)}
 *
 */
public interface ConfigurationService
{
    /**
     *
     */
    class EpochReady
    {
        public static final AsyncResult<Void> DONE = AsyncResults.success(null);

        public final long epoch;

        /**
         * The new epoch has been setup locally and the node is ready to process commands for it.
         */
        public final AsyncResult<Void> metadata;

        /**
         * The node has retrieved enough remote information to become a fast path coordinator for the new epoch.
         */
        public final AsyncResult<Void> fastPath;

        /**
         * The node has successfully replicated the underlying DataStore information for the new epoch, but may need
         * to perform some additional coordination before it can execute the read portion of a transaction.
         */
        public final AsyncResult<Void> data;

        /**
         * The node has retrieved enough remote information to safely process reads, including replicating all
         * necessary DataStore information, and any additional transactions necessary for consistency.
         */
        public final AsyncResult<Void> reads;

        public EpochReady(long epoch, AsyncResult<Void> metadata, AsyncResult<Void> fastPath, AsyncResult<Void> data, AsyncResult<Void> reads)
        {
            this.epoch = epoch;
            this.metadata = metadata;
            this.fastPath = fastPath;
            this.data = data;
            this.reads = reads;
        }

        public static EpochReady done(long epoch)
        {
            return new EpochReady(epoch, DONE, DONE, DONE, DONE);
        }

        @Override
        public String toString()
        {
            return "EpochReady{" +
                   "epoch=" + epoch +
                   ", metadata=" + metadata +
                   ", fastPath=" + fastPath +
                   ", data=" + data +
                   ", reads=" + reads +
                   '}';
        }
    }


    // TODO (exepected): do we need two implementations of this? Can't we drive it all through Node?
    interface Listener
    {
        /**
         * Informs listeners of new topology. This is guaranteed to be called sequentially for each epoch after
         * the initial topology returned by `currentTopology` on startup.
         *
         * TODO (desired): document what this Future represents, or maybe refactor it away - only used for testing
         */
        AsyncResult<Void> onTopologyUpdate(Topology topology, boolean isLoad, boolean startSync);

        /**
         * Called when accord data associated with a superseded epoch has been sync'd from previous replicas.
         * This should be invoked on each replica once EpochReady.coordination has returned on a replica.
         *
         * Once a quorum of these notifications have been received, no new TxnId may be executed in this epoch
         * (though this is not a transitive property; earlier epochs may yet agree to execute TxnId if they have not been sync'd)
         */
        void onRemoteSyncComplete(Node.Id node, long epoch);

        /**
         * Called when the configuration service is meant to truncate it's topology data up to (but not including)
         * the given epoch
         */
        void truncateTopologyUntil(long epoch);

        /**
         * Called when no new TxnId may be agreed with an epoch less than or equal to the provided one.
         * This means future epochs are now aware of all TxnId with this epoch or earlier that may be executed
         * on this range.
         */
        void onEpochClosed(Ranges ranges, long epoch);

        /**
         * Called when all TxnId with an epoch equal to or before this that interact with this range have been executed,
         * in whatever epoch they execute in. Once the whole range is covered this epoch is redundant, and may be cleaned up.
         */
        void onEpochRedundant(Ranges ranges, long epoch);

        default void onRemoveNodes(long epoch, Collection<Node.Id> removed) {}
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

    /**
     * Returns the topology for the given epoch if it's available, null otherwise
     */
    @Nullable Topology getTopologyForEpoch(long epoch);

    /**
     * Method for reporting epochs the configuration service may not be aware of. To be notified when the new epoch
     * is available locally, use {@link accord.topology.TopologyManager#awaitEpoch(long)}
     */
    void fetchTopologyForEpoch(long epoch);

    /**
     * Called after this node learns of an epoch as part of the {@code Listener#onTopologyUpdate} call.
     * On invocation the system is not necessarily ready to process the epoch, and the BootstrapReady parameter
     * provides indications of when the bootstrap has completed various phases of setup.
     */
    void acknowledgeEpoch(EpochReady ready, boolean startSync);

    void reportEpochClosed(Ranges ranges, long epoch);

    void reportEpochRedundant(Ranges ranges, long epoch);
}
