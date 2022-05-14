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
import java.util.List;
import java.util.function.BiConsumer;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.messages.Callback;
import accord.primitives.Route;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.primitives.Ballot;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.primitives.Deps;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.messages.Accept;
import accord.messages.Accept.AcceptOk;
import accord.messages.Accept.AcceptReply;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

class Propose implements Callback<AcceptReply>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final Route route;
    final Deps deps;

    private final List<AcceptOk> acceptOks;
    private final Timestamp executeAt;
    private final QuorumTracker acceptTracker;
    private final BiConsumer<Result, Throwable> callback;
    private boolean isDone;

    Propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, Route route, Deps deps, Timestamp executeAt, BiConsumer<Result, Throwable> callback)
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

    public static void propose(Node node, Ballot ballot, TxnId txnId, Txn txn, Route route,
                               Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, executeAt);
        propose(node, topologies, ballot, txnId, txn, route, executeAt, deps, callback);
    }

    public static void propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, Route route,
                               Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        Propose propose = new Propose(node, topologies, ballot, txnId, txn, route, deps, executeAt, callback);
        node.send(propose.acceptTracker.nodes(), to -> new Accept(to, topologies, ballot, txnId, route, executeAt, txn.keys(), deps, txn.kind()), propose);
    }

    @Override
    public void onSuccess(Id from, AcceptReply reply)
    {
        if (isDone)
            return;

        switch (reply.outcome())
        {
            default: throw new IllegalStateException();
            case Redundant:
            case RejectedBallot:
                isDone = true;
                callback.accept(null, new Preempted(txnId, route.homeKey));
                break;
            case Success:
                AcceptOk ok = (AcceptOk) reply;
                acceptOks.add(ok);
                if (acceptTracker.success(from))
                    onAccepted();
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (acceptTracker.failure(from))
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, route.homeKey));
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
    static class Invalidate extends AsyncFuture<Void> implements Callback<AcceptReply>
    {
        final Node node;
        final Ballot ballot;
        final TxnId txnId;
        final RoutingKey someKey;

        private final QuorumShardTracker acceptTracker;

        Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, RoutingKey someKey)
        {
            this.node = node;
            this.acceptTracker = new QuorumShardTracker(shard);
            this.ballot = ballot;
            this.txnId = txnId;
            this.someKey = someKey;
        }

        public static Invalidate proposeInvalidate(Node node, Ballot ballot, TxnId txnId, RoutingKey someKey)
        {
            Shard shard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
            Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, someKey);
            node.send(shard.nodes, to -> new Accept.Invalidate(ballot, txnId, someKey), invalidate);
            return invalidate;
        }

        @Override
        public void onSuccess(Id from, AcceptReply reply)
        {
            if (isDone())
                return;

            if (!reply.isOk())
            {
                tryFailure(new Preempted(txnId, null));
                return;
            }

            if (acceptTracker.success(from))
                trySuccess(null);
        }

        @Override
        public void onFailure(Id from, Throwable failure)
        {
            if (acceptTracker.failure(from))
                tryFailure(new Timeout(txnId, null));
        }

        @Override
        public void onCallbackFailure(Id from, Throwable failure)
        {
            tryFailure(failure);
        }
    }
}
