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

import accord.api.Data;
import accord.api.Result;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadOkWithFutureEpoch;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadEphemeralTxnData;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.WrappableException;

import static accord.coordinate.ReadCoordinator.Action.Aborted;
import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.utils.Invariants.illegalState;

public class ExecuteEphemeralRead extends ReadCoordinator<ReadReply>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(ExecuteEphemeralRead.class);

    final Txn txn;
    final FullRoute<?> route;
    final Deps deps;
    final Topologies allTopologies;
    final BiConsumer<? super Result, Throwable> callback;
    private Data data;

    ExecuteEphemeralRead(Node node, Topologies topologies, FullRoute<?> route, TxnId txnId, Txn txn, long executionEpoch, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        // we need to send Stable to the origin epoch as well as the execution epoch
        // TODO (desired): permit slicing Topologies by key (though unnecessary if we eliminate the concept of non-participating home keys)
        super(node, topologies, txnId);
        Invariants.checkArgument(txnId.kind() == EphemeralRead);
        this.txn = txn;
        this.route = route;
        this.allTopologies = topologies;
        this.deps = deps;
        this.callback = callback;
    }

    @Override
    protected void start(Iterable<Id> to)
    {
        to.forEach(this::contact);
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new ReadEphemeralTxnData(to, allTopologies, txnId, route, allTopologies.currentEpoch(), txn, deps, route), this);
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
            ReadOkWithFutureEpoch ok = ((ReadOkWithFutureEpoch) reply);
            if (ok.futureEpoch > allTopologies.currentEpoch())
            {
                // TODO (expected): only submit new requests for the keys that execute in a later epoch
                node.withEpoch(ok.futureEpoch, callback, t -> WrappableException.wrap(t), () -> {
                    new ExecuteEphemeralRead(node, node.topology().preciseEpochs(route, ok.futureEpoch, ok.futureEpoch), route, txnId.withEpoch(ok.futureEpoch), txn, ok.futureEpoch, deps, callback).start();
                });
                return Aborted;
            }

            Data next = ok.data;
            if (next != null)
                data = data == null ? next : data.merge(next);

            return ok.unavailable == null ? Approve : ApprovePartial;
        }

        CommitOrReadNack nack = (CommitOrReadNack) reply;
        switch (nack)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Rejected:
                // TODO (expected): shouldn't be preemptible (can be made redundant, but should be a special case)
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                return Action.Aborted;
            case Insufficient:
                // the replica may be missing the original commit, or the additional commit, so send everything
                // also try sending a read command to another replica, in case they're ready to serve a response
                callback.accept(null, illegalState("Received Insufficient response to ephemeral read request"));
                return Action.Aborted;
            case Invalid:
                callback.accept(null, illegalState("Submitted a read command to a replica that did not own the range"));
                return Action.Aborted;
        }
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure == null)
        {
            callback.accept(txn.result(txnId, txnId.withEpochAtLeast(allTopologies.currentEpoch()), data), null);
        }
        else
        {
            callback.accept(null, failure);
        }
    }
}
