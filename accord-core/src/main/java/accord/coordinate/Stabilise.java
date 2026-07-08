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

import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.Callback;
import accord.messages.Commit;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.TopologyException;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.sendMinimal;
import static accord.coordinate.ExecutePath.SLOW;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.messages.Commit.Kind.CommitSlowPath;
import static accord.messages.Commit.Kind.CommitWithTxn;
import static accord.topology.SelectShards.ALL;

public abstract class Stabilise<R> extends AbstractCoordination<FullRoute<?>, R, ReadReply, Void> implements Callback<ReadReply>
{
    final Txn txn;
    final Route<?> sendTo;
    final Ballot ballot;
    final Timestamp executeAt;
    final Deps stabiliseDeps;

    final QuorumTracker tracker;
    final Topologies allTopologies;

    public Stabilise(Node node, ExclusiveAsyncExecutor executor, Topologies coordinates, Topologies allTopologies, Route<?> sendTo, FullRoute<?> route, TxnId txnId, Ballot ballot, Txn txn, Timestamp executeAt, Deps stabiliseDeps, BiConsumer<? super R, Throwable> callback)
    {
        super(node, executor, txnId, route, coordinates.nodes(), callback);
        this.txn = txn;
        this.sendTo = sendTo;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.stabiliseDeps = stabiliseDeps;
        // we only care about coordination epoch for stability, as it is a recovery condition
        this.tracker = new QuorumTracker(coordinates);
        this.allTopologies = allTopologies;
    }

    @Override
    void start()
    {
        super.start();
        contact(to -> new Commit(commitKind(to), to, allTopologies, txnId, txn, scope, ballot, executeAt, stabiliseDeps));
        if (allTopologies.size() > 1)
        {
            SortedArrayList<Node.Id> extra = allTopologies.nodes().without(tracker.nodes()).without(allTopologies::isFaulty);
            for (Node.Id to : extra)
                node.send(to, new Commit(commitKind(to), to, allTopologies, txnId, txn, scope, ballot, executeAt, stabiliseDeps), tracing);
        }
    }

    @Override
    public void onSuccessInternal(Node.Id from, int fromIndex, ReadReply reply)
    {
        if (reply.isOk())
        {
            if (tracker.recordSuccess(from) == RequestStatus.Success)
                onStabilised();
        }
        else
        {
            CommitOrReadNack nack = (CommitOrReadNack) reply;
            switch (nack.kind)
            {
                default: throw new UnhandledEnum(nack.kind);
                case Redundant:
                    finishOnFailure(new Redundant(txnId, scope.homeKey(), executeAt));
                    break;
                case Rejected:
                    recordFailure(from, Preempted.preempted(node.agent(), txnId, scope.homeKey()));
                    break;
                case InsufficientAndWaiting:
                    recontact(from, new Commit(CommitWithTxn, from, allTopologies, txnId, txn, scope, ballot, executeAt, stabiliseDeps));
                    break;
                case InsufficientEpochs:
                {
                    Topologies topologies = allTopologies;
                    if (nack.minEpoch() < topologies.oldestEpoch())
                    {
                        try { topologies = node.topology().active().preciseEpochs(scope, Math.min(allTopologies.oldestEpoch(), nack.minEpoch()), allTopologies.currentEpoch(), ALL); }
                        catch (TopologyException e) { node.agent().onException(e); }
                    }
                    node.send(from, new Commit(CommitWithTxn, from, topologies,
                                               txnId, txn, scope, ballot, executeAt, stabiliseDeps),
                              tracing);
                    break;
                }
            }
        }
    }

    private Commit.Kind commitKind(Node.Id to)
    {
        return sendMinimal() && !to.equals(node.id()) ? CommitSlowPath : CommitWithTxn;
    }

    @Override
    public void onFailureInternal(Node.Id from, int fromIndex, Throwable failure)
    {
        recordFailure(from, failure);
    }

    private void recordFailure(Node.Id from, Throwable failure)
    {
        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
            finishOnFailure();
    }

    protected void onStabilised()
    {
        adapter().execute(node, executor, allTopologies, scope, ballot, SLOW, CoordinationFlags.none(), txnId, txn, executeAt, stabiliseDeps, stabiliseDeps, finishAndTakeCallback());
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.Stabilise;
    }

    @Override
    public Ballot ballot()
    {
        return ballot;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }

    @Override
    public String describe()
    {
        return "ballot=" + ballot + ", executeAt=" + executeAt;
    }

    protected abstract CoordinationAdapter<R> adapter();
}
