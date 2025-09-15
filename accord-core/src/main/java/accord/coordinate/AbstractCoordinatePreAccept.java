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

package accord.coordinate;

import java.util.function.BiConsumer;
import javax.annotation.Nonnull;

import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Callback;
import accord.primitives.FullRoute;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.SortedList;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

/**
 * Abstract parent class for implementing preaccept-like operations where we may need to fetch additional replies
 * from future epochs.
 */
abstract class AbstractCoordinatePreAccept<Result, Reply extends accord.messages.Reply, Ok> extends AbstractCoordination<FullRoute<?>, Result, Reply, Ok> implements Callback<Reply>
{
    final Topologies topologies;

    AbstractCoordinatePreAccept(Node node, SequentialAsyncExecutor executor, FullRoute<?> route, @Nonnull TxnId txnId, BiConsumer<? super Result, Throwable> callback)
    {
        this(node, executor, route, txnId, node.topology().select(route, txnId, txnId, SHARE, QuorumEpochIntersections.preaccept.include), callback);
    }

    AbstractCoordinatePreAccept(Node node, SequentialAsyncExecutor executor, FullRoute<?> route, @Nonnull TxnId txnId, Topologies topologies, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, executor, txnId, route, topologies.nodes(), callback);
        this.topologies = topologies;
    }

    abstract void onNewEpochTopologyMismatch(TopologyMismatch mismatch);
    abstract void onPreAccepted(Topologies topologies);
    abstract long executeAtEpoch();

    final void onPreAcceptedOrNewEpoch()
    {
        long latestEpoch = executeAtEpoch();
        if (latestEpoch > topologies.currentEpoch()) awaitEpochExactToFinish(latestEpoch, () -> onPreAcceptedInNewEpoch(topologies, latestEpoch));
        else onPreAccepted(topologies);
    }

    final void onPreAcceptedInNewEpoch(Topologies topologies, long latestEpoch)
    {
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(latestEpoch), txnId, scope.homeKey(), scope);
        if (mismatch == null) onPreAccepted(topologies);
        else onNewEpochTopologyMismatch(mismatch);
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.PreAccept;
    }

    @Override
    public SortedList<Id> nodes()
    {
        return topologies.nodes();
    }
}
