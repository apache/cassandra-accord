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
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.coordinate.tracking.AbstractTracker.ShardOutcomes;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.QuorumTracker.QuorumShardTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Accept;
import accord.messages.Accept.AcceptReply;
import accord.messages.Callback;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.messages.Commit.Invalidate.commitInvalidate;
import static accord.utils.Invariants.debug;

abstract class Propose<R> implements Callback<AcceptReply>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final FullRoute<?> route;
    final Deps deps;

    final List<AcceptReply> acceptOks;
    private final Map<Id, AcceptReply> debug = debug() ? new TreeMap<>() : null;
    final Timestamp executeAt;
    final QuorumTracker acceptTracker;
    final BiConsumer<? super R, Throwable> callback;
    private boolean isDone;

    Propose(Node node, Topologies topologies, Ballot ballot, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
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
        Invariants.checkState(txnId.kind().isSyncPoint() || deps.maxTxnId(txnId).compareTo(executeAt) <= 0,
                              "Attempted to propose %s with an earlier executeAt than a conflicting transaction it witnessed: %s vs executeAt: %s", txnId, deps, executeAt);
    }

    void start()
    {
        node.send(acceptTracker.nodes(), to -> new Accept(to, acceptTracker.topologies(), ballot, txnId, route, executeAt, txn.keys(), deps), this);
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
            case Truncated:
                isDone = true;
                callback.accept(null, new Truncated(txnId, route.homeKey()));
                break;
            case Redundant:
            case RejectedBallot:
                isDone = true;
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                break;
            case Success:
                acceptOks.add(reply);
                if (acceptTracker.recordSuccess(from) == RequestStatus.Success)
                {
                    isDone = true;
                    onAccepted();
                }
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

    abstract void onAccepted();

    // A special version for proposing the invalidation of a transaction; only needs to succeed on one shard
    static class Invalidate implements Callback<AcceptReply>
    {
        final Node node;
        final Ballot ballot;
        final TxnId txnId;
        final RoutingKey someParticipant;
        final BiConsumer<Void, Throwable> callback;
        final Map<Id, AcceptReply> debug = debug() ? new TreeMap<>() : null;

        private final QuorumShardTracker acceptTracker;
        private boolean isDone;

        Invalidate(Node node, Shard shard, Ballot ballot, TxnId txnId, RoutingKey someParticipant, BiConsumer<Void, Throwable> callback)
        {
            this.node = node;
            this.acceptTracker = new QuorumShardTracker(shard);
            this.ballot = ballot;
            this.txnId = txnId;
            this.someParticipant = someParticipant;
            this.callback = callback;
        }

        public static Invalidate proposeInvalidate(Node node, Ballot ballot, TxnId txnId, RoutingKey invalidateWithParticipant, BiConsumer<Void, Throwable> callback)
        {
            Shard shard = node.topology().forEpochIfKnown(invalidateWithParticipant, txnId.epoch());
            Invalidate invalidate = new Invalidate(node, shard, ballot, txnId, invalidateWithParticipant, callback);
            node.send(shard.nodes, to -> new Accept.Invalidate(ballot, txnId, invalidateWithParticipant), invalidate);
            return invalidate;
        }

        public static Invalidate proposeAndCommitInvalidate(Node node, Ballot ballot, TxnId txnId, RoutingKey invalidateWithParticipant, Route<?> commitInvalidationTo, Timestamp invalidateUntil, BiConsumer<?, Throwable> callback)
        {
            return proposeInvalidate(node, ballot, txnId, invalidateWithParticipant, (success, fail) -> {
                if (fail != null)
                {
                    callback.accept(null, fail);
                }
                else
                {
                    node.withEpoch(invalidateUntil.epoch(), (ignored, withEpochFailure) -> {
                        if (withEpochFailure != null)
                        {
                            callback.accept(null, CoordinationFailed.wrap(withEpochFailure));
                            return;
                        }
                        commitInvalidate(node, txnId, commitInvalidationTo, invalidateUntil);
                        callback.accept(null, new Invalidated(txnId, invalidateWithParticipant));
                    });
                }
            });
        }

        @Override
        public void onSuccess(Id from, AcceptReply reply)
        {
            if (isDone)
                return;

            if (debug != null) debug.put(from, reply);

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
