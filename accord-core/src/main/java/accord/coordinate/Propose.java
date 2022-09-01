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

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.primitives.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.primitives.Deps;
import accord.txn.Txn;
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
    final Key homeKey;

    private final List<AcceptOk> acceptOks;
    private final Timestamp executeAt;
    private final QuorumTracker acceptTracker;
    private final BiConsumer<Result, Throwable> callback;
    private boolean isDone;

    Propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, BiConsumer<Result, Throwable> callback)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
        this.callback = callback;
        this.acceptOks = new ArrayList<>();
        this.acceptTracker = new QuorumTracker(topologies);
    }

    public static void propose(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey,
                               Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        Topologies topologies = node.topology().withUnsyncEpochs(txn, txnId, executeAt);
        propose(node, topologies, ballot, txnId, txn, homeKey, executeAt, deps, callback);
    }

    public static void propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, Key homeKey,
                               Timestamp executeAt, Deps deps, BiConsumer<Result, Throwable> callback)
    {
        Propose propose = new Propose(node, topologies, ballot, txnId, txn, homeKey, executeAt, callback);
        node.send(propose.acceptTracker.nodes(), to -> new Accept(to, topologies, ballot, txnId, homeKey, txn, executeAt, deps), propose);
    }

    @Override
    public void onSuccess(Id from, AcceptReply reply)
    {
        if (isDone)
            return;

        if (!reply.isOK())
        {
            isDone = true;
            callback.accept(null, new Preempted(txnId, homeKey));
            return;
        }

        AcceptOk ok = (AcceptOk) reply;
        acceptOks.add(ok);
        if (acceptTracker.success(from))
            onAccepted();
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (acceptTracker.failure(from))
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, homeKey));
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
        Execute.execute(node, txnId, txn, homeKey, executeAt, deps, callback);
    }

    // A special version for proposing the invalidation of a transaction; only needs to succeed on one shard
    static class Invalidate extends AsyncFuture<Void> implements Callback<AcceptReply>
    {
        final Node node;
        final Ballot ballot;
        final TxnId txnId;
        final Key someKey;

        private final QuorumShardTracker acceptTracker;

        Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, Key someKey)
        {
            this.node = node;
            this.acceptTracker = new QuorumShardTracker(shard);
            this.ballot = ballot;
            this.txnId = txnId;
            this.someKey = someKey;
        }

        public static Invalidate proposeInvalidate(Node node, Ballot ballot, TxnId txnId, Key someKey)
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

            if (!reply.isOK())
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
