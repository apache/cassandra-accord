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

import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadTxnData;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListMap;

import static accord.coordinate.ExecutePath.SLOW;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.messages.Commit.Kind.CommitWithTxn;
import static accord.utils.Invariants.debug;

public abstract class Stabilise<R> implements Callback<ReadReply>
{
    final Node node;
    final Txn txn;
    final FullRoute<?> route;
    final TxnId txnId;
    final Ballot ballot;
    final Timestamp executeAt;
    final Deps stabiliseDeps;

    final QuorumTracker stableTracker;
    final Topologies allTopologies;
    private final Map<Node.Id, Object> debug;
    final BiConsumer<? super R, Throwable> callback;
    private boolean isDone;

    public Stabilise(Node node, Topologies coordinates, Topologies allTopologies, FullRoute<?> route, TxnId txnId, Ballot ballot, Txn txn, Timestamp executeAt, Deps stabiliseDeps, BiConsumer<? super R, Throwable> callback)
    {
        this.node = node;
        this.txn = txn;
        this.route = route;
        this.txnId = txnId;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.stabiliseDeps = stabiliseDeps;
        // we only care about coordination epoch for stability, as it is a recovery condition
        this.stableTracker = new QuorumTracker(coordinates);
        this.allTopologies = allTopologies;
        this.debug = debug() ? new SortedListMap<>(allTopologies.nodes(), Object[]::new) : null;
        this.callback = callback;
    }

    void start()
    {
        SortedArrayList<Node.Id> contact = stableTracker.filterAndRecordFaulty();
        if (allTopologies.size() > 1)
            contact = contact.with(allTopologies.nodes().without(stableTracker.nodes()).without(allTopologies::isFaulty));

        if (contact == null) callback.accept(null, new Exhausted(txnId, route.homeKey(), null));
        else Commit.commitMinimalNoRead(contact, node, stableTracker.topologies(), allTopologies, ballot, txnId, txn, route, executeAt, stabiliseDeps, this);
    }

    @Override
    public void onSuccess(Node.Id from, ReadReply reply)
    {
        if (isDone)
            return;

        if (debug != null) debug.put(from, reply);

        if (reply.isOk())
        {
            if (stableTracker.recordSuccess(from) == RequestStatus.Success)
            {
                isDone = true;
                onStabilised();
            }
        }
        else
        {
            switch ((CommitOrReadNack)reply)
            {
                default: throw new AssertionError("Unhandled CommitOrReadNack: " + reply);
                case Redundant:
                    isDone = true;
                    callback.accept(null, new Redundant(txnId, route.homeKey(), executeAt));
                    break;
                case Rejected:
                case Invalid:
                    isDone = true;
                    callback.accept(null, new Preempted(txnId, route.homeKey()));
                    break;
                case Insufficient:
                    node.send(from, new Commit(CommitWithTxn, from, allTopologies.forEpoch(txnId.epoch()), allTopologies,
                                               txnId, txn, route, ballot, executeAt, stabiliseDeps, (ReadTxnData) null));
                    break;
            }
        }
    }

    @Override
    public void onFailure(Node.Id from, Throwable failure)
    {
        if (isDone)
            return;

        if (debug != null) debug.put(from, failure);

        if (stableTracker.recordFailure(from) == Failed)
        {
            isDone = true;
            callback.accept(null, new Timeout(txnId, route.homeKey()));
        }
    }

    @Override
    public void onCallbackFailure(Node.Id from, Throwable failure)
    {
        if (isDone)
            return;

        isDone = true;
        callback.accept(null, failure);
    }

    protected void onStabilised()
    {
        adapter().execute(node, allTopologies, route, SLOW, txnId, txn, executeAt, stabiliseDeps, callback);
    }

    protected abstract CoordinationAdapter<R> adapter();
}
