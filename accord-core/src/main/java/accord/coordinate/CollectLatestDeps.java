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

import javax.annotation.Nullable;

import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.GetLatestDeps;
import accord.messages.GetLatestDeps.GetLatestDepsOk;
import accord.messages.GetLatestDeps.GetLatestDepsReply;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedList;
import accord.utils.SortedListMap;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.Routables.Slice.Minimal;

public class CollectLatestDeps extends AbstractCoordination<Route<?>, List<LatestDeps>, GetLatestDepsReply, GetLatestDepsOk>
{
    final Timestamp executeAt;
    final @Nullable Ballot ballot;

    private final QuorumTracker tracker;

    CollectLatestDeps(Node node, Topologies topologies, TxnId txnId, Route<?> route, @Nullable Ballot ballot, Timestamp executeAt, BiConsumer<List<LatestDeps>, Throwable> callback)
    {
        super(node, node.someSequentialExecutor(), txnId, route, topologies.nodes(), callback);
        this.executeAt = executeAt;
        this.ballot = ballot;
        this.tracker = new QuorumTracker(topologies);
    }

    public static void withLatestDeps(Node node, TxnId txnId, FullRoute<?> fullRoute, Unseekables<?> collectFrom, @Nullable Ballot ballot, Timestamp executeAt, BiConsumer<List<LatestDeps>, Throwable> callback)
    {
        Route<?> route = fullRoute.intersecting(collectFrom, Minimal);
        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, executeAt);
        CollectLatestDeps collect = new CollectLatestDeps(node, topologies, txnId, route, ballot, executeAt, callback);
        collect.start();
    }

    @Override
    void start()
    {
        SortedArrayList<Id> contact = tracker.filterAndRecordFaulty();
        if (contact == null)
        {
            finishOnExaustion();
        }
        else
        {
            super.start();
            contact(to -> new GetLatestDeps(to, tracker.topologies(), scope, txnId, ballot, executeAt));
        }
    }

    @Override
    public void onSuccessInternal(Id from, int fromIndex, GetLatestDepsReply ok)
    {
        if (ok.isOk())
        {
            recordOk(fromIndex, (GetLatestDepsOk) ok);
            if (tracker.recordSuccess(from) == Success)
                onQuorum();
        }
        else
        {
            onFailureInternal(from, fromIndex, null);
        }
    }

    @Override
    public void onFailureInternal(Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
            finishOnFailure();
    }

    private void onQuorum()
    {
        Invariants.require(!isDone());
        SortedListMap<Node.Id, GetLatestDepsOk> oks = finishOks();
        List<LatestDeps> result = new ArrayList<>(oks.size());
        for (GetLatestDepsOk ok : oks.values())
            result.add(ok.deps);
        finishWithSuccess(result);
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.CollectLatestDeps;
    }

    @Override
    public SortedList<Id> nodes()
    {
        return tracker.nodes();
    }
}
