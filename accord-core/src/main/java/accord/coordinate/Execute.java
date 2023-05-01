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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
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

    private Execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Seekables<?, ?> readScope, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, node.topology().forEpoch(readScope.toUnseekables(), executeAt.epoch()), txnId, txn.read().readDataCL());
        this.txn = txn;
        this.route = route;
        this.readScope = readScope;
        this.executeAt = executeAt;
        this.deps = deps;
        this.applyTo = node.topology().forEpoch(route, executeAt.epoch());
        this.callback = callback;
    }

    public static void execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        // Recovery calls execute and we would like execute to run BlockOnDeps because that will notify the agent
        // of the local barrier
        // TODO we don't really need to run BlockOnDeps, executing the empty txn would also be fine
        if (txn.kind() == Kind.SyncPoint)
        {
            checkArgument(txnId.equals(executeAt));
            BlockOnDeps.blockOnDeps(node, txnId, txn, route, deps, callback);
        }
        else if (txn.read().keys().isEmpty())
        {
            Topologies sendTo = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
            Topologies applyTo = node.topology().forEpoch(route, executeAt.epoch());
            Result result = txn.result(txnId, executeAt, null);
            Persist.persist(node, sendTo, applyTo, txnId, route, txn, executeAt, deps, txn.execute(executeAt, null, null), result);
            callback.accept(result, null);
        }
        else
        {
            Execute execute = new Execute(node, txnId, txn, route, txn.keys(), executeAt, deps, callback);
            execute.start((RoutingKeys)txn.read().keys().toUnseekables());
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
            return txn.read().readDataCL().requiresDigestReads ? ApproveIfQuorum : Approve;
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

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure == null)
        {
            AsyncChain<ResolveResult> resolveResultChain = txn.readResolver().resolve(txn.read(), unresolvedData, (read, to, callback) -> {
                node.send(to, new ReadData(to, topologies(), txnId, read.keys(), executeAt, null, read), new Callback<ExecuteReply>() {

                    @Override
                    public void onSuccess(Id from, ExecuteReply reply)
                    {
                        logger.info("Got repair response back");
                        callback.onSuccess(from, ((ExecuteOk)reply).unresolvedData);
                    }

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
            });
            resolveResultChain.begin((resolveResult, failure2) -> {
                if (failure2 == null)
                {
                    Data data = resolveResult.data;
                    Result result = txn.result(txnId, executeAt, data);
                    callback.accept(result, null);
                    // avoid re-calculating topologies if it is unchanged
                    Topologies sendTo = txnId.epoch() == executeAt.epoch() ? applyTo : node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                    Persist.persist(node, sendTo, applyTo, txnId, route, txn, executeAt, deps, txn.execute(executeAt, data, resolveResult.repairWrites), result);
                }
                else
                {
                    callback.accept(null, failure2);
                }
            });
        }
        else
        {
            callback.accept(null, failure);
        }
    }
}
