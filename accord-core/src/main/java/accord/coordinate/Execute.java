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

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.DataResolver.FollowupReader;
import accord.api.Key;
import accord.api.ResolveResult;
import accord.api.Result;
import accord.api.UnresolvedData;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.ReadData;
import accord.messages.WhenReadyToExecute.ExecuteNack;
import accord.messages.WhenReadyToExecute.ExecuteOk;
import accord.messages.WhenReadyToExecute.ExecuteReply;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.async.AsyncChain;

import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApproveIfQuorum;
import static accord.messages.Commit.Kind.Maximal;
import static accord.utils.Invariants.checkArgument;

class Execute extends ReadCoordinator<ExecuteReply>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(Execute.class);

    final Txn txn;
    final Seekables<?, ?> readScope;
    final FullRoute<?> route;
    final Timestamp executeAt;
    final Deps deps;
    final Topologies applyTo;
    final BiConsumer<? super Result, Throwable> callback;
    private UnresolvedData unresolvedData;

    private Execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, node.topology().forEpoch(txn.keys().toUnseekables(), executeAt.epoch()), txnId, txn.readDataCL());
        this.txn = txn;
        this.route = route;
        this.readScope = txn.read().keys();
        this.executeAt = executeAt;
        this.deps = deps;
        this.applyTo = node.topology().forEpoch(route, executeAt.epoch());
        this.callback = callback;
    }

    public static void execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        Seekables<?, ?> readScope = txn.read().keys();
        // Recovery calls execute and we would like execute to run BlockOnDeps because that will notify the agent
        // of the local barrier
        // TODO we don't really need to run BlockOnDeps, executing the empty txn would also be fine
        if (txn.kind() == Kind.SyncPoint)
        {
            checkArgument(txnId.equals(executeAt));
            BlockOnDeps.blockOnDeps(node, txnId, txn, route, deps, callback);
        }
        else if (readScope.isEmpty())
        {
            Topologies sendTo = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
            Topologies applyTo = node.topology().forEpoch(route, executeAt.epoch());
            Result result = txn.result(txnId, executeAt, null);
            Consumer<Throwable> onAppliedToQuorum = null;
            DataConsistencyLevel writeDataCL = txn.writeDataCL();
            if (!writeDataCL.requiresSynchronousCommit)
                callback.accept(result, null);
            else
                onAppliedToQuorum = (applyFailure) -> callback.accept(applyFailure == null ? result : null, applyFailure);
            Persist.persist(node, sendTo, applyTo, txnId, route, txn, executeAt, deps, txn.execute(executeAt, null, null), result, onAppliedToQuorum, writeDataCL);
        }
        else
        {
            Execute execute = new Execute(node, txnId, txn, route, executeAt, deps, callback);
            execute.start(readScope.toUnseekables());
        }
    }

    @Override
    protected void sendInitialReads(Map<Id, RoutingKeys> readSet)
    {
        Commit.commitMinimalAndRead(node, applyTo, txnId, txn, route, readScope, executeAt, deps, readSet, this);
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new ReadData(to, topologies(), txnId, readScope, executeAt, null, null), this);
    }

    @Override
    protected Action process(Id from, ExecuteReply reply)
    {
        if (reply.isOk())
        {
            UnresolvedData next = ((ExecuteOk) reply).unresolvedData;
            if (next != null)
                unresolvedData = unresolvedData == null ? next : unresolvedData.merge(next);
            return txn.readDataCL().requiresDigestReads ? ApproveIfQuorum : Approve;
        }

        ExecuteNack nack = (ExecuteNack) reply;
        switch (nack)
        {
            default: throw new IllegalStateException();
            case Error:
                // TODO (expected): report content of error
                return Action.Reject;
            case Redundant:
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                return Action.Abort;
            case NotCommitted:
                // the replica may be missing the original commit, or the additional commit, so send everything
                Topologies topology = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                Topology coordinateTopology = topology.forEpoch(txnId.epoch());
                node.send(from, new Commit(Maximal, from, coordinateTopology, topology, txnId, txn, route, readScope, executeAt, deps, null));
                // also try sending a read command to another replica, in case they're ready to serve a response
                return Action.TryAlternative;
            case Invalid:
                onFailure(from, new IllegalStateException("Submitted a read command to a replica that did not own the range"));
                return Action.Abort;
        }
    }

    private void onDataResolutionDone(ResolveResult resolveResult, Throwable failure)
    {
        if (failure == null)
        {
            Data data = resolveResult.data;
            Result result = txn.result(txnId, executeAt, data);

            // Accord allows you to specify read CL separately from write CL
            // so if we want reads to be monotonic we may need to upgrade the CL when writing the repairs
            DataConsistencyLevel txnWriteDataCL = txn.writeDataCL();
            DataConsistencyLevel writeDataCL = resolveResult.repairWrites != null ?
                                                   DataConsistencyLevel.max(txnWriteDataCL, txn.readDataCL()) :
                                                   txnWriteDataCL;

            // If this transaction generates repair writes then we don't want to acknowledge it until the writes are committed
            // to make sure the transaction's reads are monotonic from the perspective of the caller
            // If the transaction specified a writeDataCL then we don't want to acknowledge until the CL is met
            Consumer<Throwable> onAppliedToQuorum = null;
            if (resolveResult.repairWrites != null || writeDataCL.requiresSynchronousCommit)
                onAppliedToQuorum = (applyFailure) -> callback.accept(applyFailure == null ? result : null, applyFailure);
            else
                callback.accept(result, null);

            // avoid re-calculating topologies if it is unchanged
            Topologies sendTo = txnId.epoch() == executeAt.epoch() ? applyTo : node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
            Persist.persist(node, sendTo, applyTo, txnId, route, txn, executeAt, deps, txn.execute(executeAt, data, resolveResult.repairWrites), result, onAppliedToQuorum, writeDataCL);
        }
        else
        {
            callback.accept(null, failure);
        }
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure == null)
        {
            AsyncChain<ResolveResult> resolveResultChain = txn.readResolver().resolve(executeAt, txn.read(), unresolvedData, getFollowupReader());
            resolveResultChain.begin(this::onDataResolutionDone);
        }
        else
        {
            callback.accept(null, failure);
        }
    }

    private FollowupReader getFollowupReader()
    {
        return (read, to, callback) -> {
            // It's possible for the follow up read to be sent to the wrong replica
            // if the integration has a different view of cluster metadata during txn resolution
            // Definitely need to validate before reading and the txn resolver should attempt to use the correct
            // epoch when picking which nodes to contact
            Topology executeTopology = topologies().get(0);
            if (!executeTopology.contains(to))
            {
                callback.onFailure(null, new IllegalArgumentException(to + " is not a replica in executeAt epoch" + executeAt.epoch()));
                return;
            }
            Seekables keys = read.keys();
            if (keys.size() > 1)
            {
                callback.onFailure(to, new IllegalArgumentException("Multiple keys are not expected"));
                return;
            }
            Key key = (Key)read.keys().get(0);
            if (!executeTopology.rangesForNode(to).contains(key.toUnseekable()))
            {
                callback.onFailure(to, new IllegalArgumentException(key + " is not replicated by node " + to + " in executeAt epoch " + executeAt.epoch()));
                return;
            }
            if (!readScope.contains(key))
            {
                callback.onFailure(to, new IllegalArgumentException(key + " is not one of the read keys for this transaction"));
                return;
            }

            node.send(to, new ReadData(to, topologies(), txnId, read.keys(), executeAt, null, read), new Callback<ExecuteReply>() {
                @Override
                public void onSuccess(Id from, ExecuteReply reply)
                {
                    callback.onSuccess(from, ((ExecuteOk)reply).unresolvedData);
                }

                // TODO slow response?

                @Override
                public void onFailure(Id from, Throwable failure)
                {
                    callback.onFailure(from, failure);
                }

                @Override
                public void onCallbackFailure(Id from, Throwable failure)
                {
                    failure.printStackTrace();
                }
            });
        };
    }
}
