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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.Result;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.messages.ReadData.ReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadTxnData;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.coordinate.ReadCoordinator.Action.Approve;
import static accord.coordinate.ReadCoordinator.Action.ApprovePartial;

public class TxnExecute extends ReadCoordinator<ReadReply> implements Execute
{
    public static final Execute.Factory FACTORY = TxnExecute::new;

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnExecute.class);

    final Txn txn;
    final Participants<?> readScope;
    final FullRoute<?> route;
    final Timestamp executeAt;
    final Deps deps;
    final Topologies executes;
    final BiConsumer<? super Result, Throwable> callback;
    private Data data;

    private TxnExecute(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Participants<?> readScope, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
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
            ReadOk ok = ((ReadOk) reply);
            Data next = ((ReadOk) reply).data;
            if (next != null)
                data = data == null ? next : data.merge(next);

            return ok.unavailable == null ? Approve : ApprovePartial;
        }

        ReadNack nack = (ReadNack) reply;
        switch (nack)
        {
            default: throw new IllegalStateException();
            case Redundant:
                callback.accept(null, new Preempted(txnId, route.homeKey()));
                return Action.Aborted;
            case NotCommitted:
                // the replica may be missing the original commit, or the additional commit, so send everything
                Commit.commitMaximal(node, from, txn, txnId, executeAt, route, deps, readScope);
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
            Persist.persist(node, executes, txnId, route, txn, executeAt, deps, txn.execute(txnId, executeAt, data), result, callback);
        }
        else
        {
            callback.accept(null, failure);
        }
    }
}
