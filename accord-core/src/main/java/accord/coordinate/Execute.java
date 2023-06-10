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

import java.util.Set;
import java.util.function.BiConsumer;

import accord.api.Data;
import accord.api.Result;
import accord.local.Node;
import accord.messages.ReadTxnData;
import accord.messages.ReadData.ReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.primitives.*;
import accord.topology.Topologies;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.topology.Topology;

import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;
import static accord.messages.Commit.Kind.Maximal;

class Execute extends ReadCoordinator<ReadReply>
{
    final Txn txn;
    final Participants<?> readScope;
    final FullRoute<?> route;
    final Timestamp executeAt;
    final Deps deps;
    final Topologies executes;
    final BiConsumer<? super Result, Throwable> callback;
    private Data data;

    private Execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Participants<?> readScope, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, node.topology().forEpoch(readScope, executeAt.epoch()), txnId);
        this.txn = txn;
        this.route = route;
        this.readScope = readScope;
        this.executeAt = executeAt;
        this.deps = deps;
        this.executes = node.topology().forEpoch(route, executeAt.epoch());
        this.callback = callback;
    }

    public static void execute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        if (txn.read().keys().isEmpty())
        {
            Result result = txn.result(txnId, executeAt, null);
            Persist.persist(node, txnId, route, txn, executeAt, deps, txn.execute(txnId, executeAt, null), result);
            callback.accept(result, null);
        }
        else
        {
            Execute execute = new Execute(node, txnId, txn, route, txn.keys().toParticipants(), executeAt, deps, callback);
            execute.start();
        }
    }

    @Override
    protected void start(Set<Id> readSet)
    {
        Commit.commitMinimalAndRead(node, executes, txnId, txn, route, readScope, executeAt, deps, readSet, this);
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new ReadTxnData(to, topologies(), txnId, readScope, executeAt), this);
    }

    @Override
    protected Ranges unavailable(ReadReply reply)
    {
        return ((ReadOk)reply).unavailable;
    }

    @Override
    protected Action process(Id from, ReadReply reply)
    {
        if (reply.isOk())
        {
            ReadOk ok = (ReadOk) reply;
            Data next = ok.data;
            if (next != null)
                data = data == null ? next : data.merge(next);

            return ok.unavailable == null ? Approve : ApprovePartial;
        }

        ReadNack nack = (ReadNack) reply;
        switch (nack)
        {
            default: throw new IllegalStateException();
            case Error:
                // TODO (expected): report content of error
                return Action.Reject;
            case Redundant:
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                return Action.Aborted;
            case NotCommitted:
                // the replica may be missing the original commit, or the additional commit, so send everything
                Topologies topology = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                Topology coordinateTopology = topology.forEpoch(txnId.epoch());
                node.send(from, new Commit(Maximal, from, coordinateTopology, topology, txnId, txn, route, readScope, executeAt, deps, false));
                // also try sending a read command to another replica, in case they're ready to serve a response
                return Action.TryAlternative;
            case Invalid:
                callback.accept(null, new IllegalStateException("Submitted a read command to a replica that did not own the range"));
                return Action.Aborted;
        }
    }


    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure == null)
        {
            Result result = txn.result(txnId, executeAt, data);
            callback.accept(result, null);
            // avoid re-calculating topologies if it is unchanged
            Persist.persist(node, executes, txnId, route, txn, executeAt, deps, txn.execute(txnId, executeAt, data), result);
        }
        else
        {
            callback.accept(null, failure);
        }
    }
}
