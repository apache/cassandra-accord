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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.tracking.AbstractTracker.ShardOutcomes;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.QuorumTracker.QuorumShardTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.messages.Callback;
import accord.primitives.*;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Accept;
import accord.messages.Accept.AcceptReply;
import accord.utils.Invariants;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.utils.Invariants.debug;

class Propose implements Callback<AcceptReply>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final FullRoute<?> route;
    final Deps deps;

    private final List<AcceptReply> acceptOks;
    private final Map<Id, AcceptReply> debug = debug() ? new HashMap<>() : null;
    private final Timestamp executeAt;
    private final QuorumTracker acceptTracker;
    private final BiConsumer<Result, Throwable> callback;
    private boolean isDone;

    Propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Deps deps, Timestamp executeAt, BiConsumer<Result, Throwable> callback)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.deps = deps;
        this.executeAt = executeAt;
        this.callback = callback;
        this.acceptOks = new ArrayList<>();
        this.acceptTracker = new QuorumTracker(topologies);
    }

    public static void propose(Node node, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route,
                               Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, executeAt);
        propose(node, topologies, ballot, txnId, txn, route, executeAt, deps, callback);
    }

    public static void propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route,
                               Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        Propose propose = new Propose(node, topologies, ballot, txnId, txn, route, deps, executeAt, callback);
        node.send(propose.acceptTracker.nodes(), to -> new Accept(to, topologies, ballot, txnId, route, executeAt, txn.keys(), deps), propose);
    }

    @Override
    public void onSuccess(Id from, AcceptReply reply)
    {
        if (isDone)
            return;

        if (debug != null) debug.put(from, reply);

        switch (reply.outcome())
        {
            default: throw new IllegalStateException();
            case Redundant:
            case RejectedBallot:
                isDone = true;
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                break;
            case Success:
                acceptOks.add(reply);
                if (acceptTracker.recordSuccess(from) == RequestStatus.Success)
                    onAccepted();
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (acceptTracker.recordFailure(from) == Failed)
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, route.homeKey()));
        }
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        isDone = true;
        callback.accept(null, failure);
    }

    private void onAccepted()
    {
        isDone = true;
        Deps deps = Deps.merge(acceptOks, ok -> ok.deps);
        Execute.execute(node, txnId, txn, route, executeAt, deps, callback);
    }

    // A special version for proposing the invalidation of a transaction; only needs to succeed on one shard
    static class Invalidate implements Callback<AcceptReply>
    {
        final Node node;
        final Ballot ballot;
        final TxnId txnId;
        final RoutingKey invalidateWithKey;
        final BiConsumer<Void, Throwable> callback;

        private final QuorumShardTracker acceptTracker;
        private boolean isDone;

        Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, RoutingKey invalidateWithKey, BiConsumer<Void, Throwable> callback)
        {
            this.node = node;
            this.acceptTracker = new QuorumShardTracker(shard);
            this.ballot = ballot;
            this.txnId = txnId;
            this.invalidateWithKey = invalidateWithKey;
            this.callback = callback;
        }

        public static Invalidate proposeInvalidate(Node node, Ballot ballot, TxnId txnId, RoutingKey invalidateWithKey, BiConsumer<Void, Throwable> callback)
        {
            Shard shard = node.topology().forEpochIfKnown(invalidateWithKey, txnId.epoch());
            Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, invalidateWithKey, callback);
            node.send(shard.nodes, to -> new Accept.Invalidate(ballot, txnId, invalidateWithKey), invalidate);
            return invalidate;
        }

        @Override
        public void onSuccess(Id from, AcceptReply reply)
        {
            if (isDone)
                return;

            if (!reply.isOk())
            {
                isDone = true;
                callback.accept(null, new Preempted(txnId, null));
                return;
            }

            if (acceptTracker.onSuccess(from) == ShardOutcomes.Success)
            {
                isDone = true;
                callback.accept(null, null);
            }
        }

        @Override
        public void onFailure(Id from, Throwable failure)
        {
            if (acceptTracker.onFailure(from) == Fail)
            {
                isDone = true;
                callback.accept(null, new Timeout(txnId, null));
            }
        }

        @Override
        public void onCallbackFailure(Id from, Throwable failure)
        {
            if (isDone)
                return;

            isDone = true;
            callback.accept(null, failure);
        }
    }
}
