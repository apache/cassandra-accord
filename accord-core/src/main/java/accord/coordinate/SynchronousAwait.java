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

import javax.annotation.Nullable;

import accord.api.ProgressLog.BlockedUntil;
import accord.api.RoutingKey;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.ResponseTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Await;
import accord.messages.Await.AwaitOk;
import accord.messages.Callback;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.async.AsyncResults;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;

/**
 * Synchronously await some set of replicas reaching a given wait condition.
 * This may or may not be a condition we expect to reach promptly, but we will wait only until the timeout passes
 * at which point we will report failure.
 */
public class SynchronousAwait extends AsyncResults.SettableResult<Void> implements Callback<AwaitOk>
{
    // TODO (desired, efficiency): this should collect the executeAt of any commit, and terminate as soon as one is found
    //                             that is earlier than TxnId for the Txn we are recovering; if all commits we wait for
    //                             are given earlier timestamps we can retry without restarting.

    final TxnId txnId;
    final @Nullable RoutingKey homeKey;
    final ResponseTracker tracker;

    public SynchronousAwait(TxnId txnId, @Nullable RoutingKey homeKey, ResponseTracker tracker)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.tracker = tracker;
    }

    public static SynchronousAwait awaitQuorum(Node node, Topology topology, TxnId txnId, @Nullable RoutingKey homeKey, BlockedUntil awaiting, Participants<?> participants)
    {
        QuorumTracker tracker = new QuorumTracker(new Topologies.Single(node.topology().sorter(), topology));
        SynchronousAwait result = new SynchronousAwait(txnId, homeKey, tracker);
        result.start(node, topology, participants, awaiting);
        return result;
    }

    private void start(Node node, Topology topology, Participants<?> participants, BlockedUntil blockedUntil)
    {
        node.send(topology.nodes(), to -> new Await(to, topology, txnId, participants, blockedUntil), this);
    }

    @Override
    public synchronized void onSuccess(Id from, AwaitOk reply)
    {
        if (isDone()) return;

        if (tracker.recordSuccess(from) == Success)
            trySuccess(null);
    }

    @Override
    public synchronized void onFailure(Id from, Throwable failure)
    {
        if (isDone()) return;

        if (tracker.recordFailure(from) == Failed)
            tryFailure(new Timeout(txnId, homeKey));
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        tryFailure(failure);
    }
}

