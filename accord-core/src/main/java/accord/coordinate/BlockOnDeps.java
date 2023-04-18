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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.WhenReadyToExecute.ExecuteNack;
import accord.messages.WhenReadyToExecute.ExecuteReply;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.messages.Commit.Kind.Maximal;

/**
 * Block on deps at quorum for a sync point transaction, and then move the transaction to the applied state
 */
public class BlockOnDeps implements Callback<ExecuteReply>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(BlockOnDeps.class);

    final Node node;
    final TxnId txnId;
    final Txn txn;
    final FullRoute<?> route;
    final Deps deps;
    final Topologies blockOn;
    final QuorumTracker tracker;
    final BiConsumer<? super Result, Throwable> callback;

    private boolean isDone = false;

    private BlockOnDeps(Node node, TxnId txnId, Txn txn, FullRoute<?> route,  Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.deps = deps;
        // TODO Is this an acceptable topology? txnId and executeAt are always the same for these so there shouldn't be multiple topologies?
        // Should this come from tracker topologies like it does for apply in CoordinateSyncPoint?
        this.blockOn = node.topology().forEpoch(route, txnId.epoch());
        this.tracker = new QuorumTracker(blockOn);
        this.callback = callback;
    }

    public static void blockOnDeps(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        BlockOnDeps blockOnDeps = new BlockOnDeps(node, txnId, txn, route, deps, callback);
        blockOnDeps.start();
    }

    void start()
    {
        Commit.commitMinimalAndBlockOnDeps(node, blockOn, txnId, txn, route, deps, this);
    }

    @Override
    public void onSuccess(Id from, ExecuteReply reply)
    {
        if (isDone)
            return;

        if (reply.isOk())
        {
            if (tracker.recordSuccess(from) == RequestStatus.Success)
            {
                isDone = true;
                callback.accept(txn.result(txnId, txnId, null), null);
            }
            return;
        }

        ExecuteNack nack = (ExecuteNack) reply;
        switch (nack)
        {
            default: throw new IllegalStateException();
            case Error:
                onFailure(from, new RuntimeException("Unknown error"));
                // TODO (expected): report content of error
                break;
            case Redundant:
                // TODO is this the right way to handle redundant?
                isDone = true;
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                break;
            case NotCommitted:
                // the replica may be missing the original commit, or the additional commit, so send everything
                Topologies topology = node.topology().preciseEpochs(route, txnId.epoch(), txnId.epoch());
                Topology coordinateTopology = topology.forEpoch(txnId.epoch());
                node.send(from, new Commit(Maximal, from, coordinateTopology, topology, txnId, txn, route, txn.keys(), txnId, deps, false));
                break;
            case Invalid:
                onFailure(from, new IllegalStateException("Submitted a read command to a replica that did not own the range"));
                break;
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (tracker.recordFailure(from) == Failed)
        {
            isDone = true;
            callback.accept(null, failure);
        }
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        isDone = true;
        callback.accept(null, failure);
    }
}
