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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import accord.api.Result;
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
import accord.utils.Faults;

import static accord.coordinate.Execute.Path.SLOW;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.messages.Commit.Kind.CommitWithTxn;
import static accord.utils.Invariants.debug;

public abstract class Stabilise implements Callback<ReadReply>
{
    final Node node;
    final Txn txn;
    final FullRoute<?> route;
    final TxnId txnId;
    final Ballot ballot;
    final Timestamp executeAt;
    final Deps stabiliseDeps;

    private final Map<Node.Id, Object> debug = debug() ? new HashMap<>() : null;
    final QuorumTracker stableTracker;
    final Topologies allTopologies;
    final BiConsumer<? super Result, Throwable> callback;
    private boolean isDone;

    public Stabilise(Node node, Topologies coordinates, Topologies allTopologies, FullRoute<?> route, TxnId txnId, Ballot ballot, Txn txn, Timestamp executeAt, Deps stabiliseDeps, BiConsumer<? super Result, Throwable> callback)
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
        this.callback = callback;
    }

    static void stabilise(Node node, Topologies anyTopologies, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        Topologies coordinates = anyTopologies.forEpochs(txnId.epoch(), txnId.epoch());
        Topologies allTopologies;
        if (txnId.epoch() == executeAt.epoch()) allTopologies = coordinates;
        else if (anyTopologies.currentEpoch() >= executeAt.epoch() && anyTopologies.oldestEpoch() <= txnId.epoch()) allTopologies = anyTopologies.forEpochs(txnId.epoch(), executeAt.epoch());
        else allTopologies = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());

        stabilise(node, coordinates, allTopologies, route, ballot, txnId, txn, executeAt, deps, callback);
    }

    public static void stabilise(Node node, Topologies coordinates, Topologies allTopologies, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
    {
        switch (txnId.kind())
        {
            default:
                throw new AssertionError("Unhandled Txn.Kind: " + txnId.kind());
            case LocalOnly:
                throw new AssertionError("Invalid Txn.Kind to stabilise: " + txnId.kind());
            case SyncPoint:
            case ExclusiveSyncPoint:
                // TODO (expected): merge with branch below, as identical besides fault condition
                if (Faults.SYNCPOINT_INSTABILITY) Execute.execute(node, allTopologies, route, SLOW, txnId, txn, executeAt, deps, callback);
                else new StabiliseTxn(node, coordinates, allTopologies, route, txnId, ballot, txn, executeAt, deps, callback).start();
                break;
            case NoOp:
            case Read:
            case Write:
                if (Faults.TRANSACTION_INSTABILITY) Execute.execute(node, allTopologies, route, SLOW, txnId, txn, executeAt, deps, callback);
                else new StabiliseTxn(node, coordinates, allTopologies, route,
                                      txnId, ballot, txn, executeAt, deps, callback).start();
        }
    }

    void start()
    {
        Commit.commitMinimal(node, stableTracker.topologies(), ballot, txnId, txn, route, executeAt, stabiliseDeps, this);
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
                case Rejected:
                case Redundant:
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

    abstract void onStabilised();
}
