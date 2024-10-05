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

import java.util.HashSet;
import java.util.Set;

import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Apply;
import accord.messages.Apply.ApplyReply;
import accord.messages.Callback;
import accord.messages.InformDurable;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.SortedArrays;

import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.Status.Durability.Majority;

public abstract class Persist implements Callback<ApplyReply>
{
    protected final Node node;
    protected final TxnId txnId;
    protected final FullRoute<?> route;
    protected final Txn txn;
    protected final Timestamp executeAt;
    protected final Deps stableDeps;
    protected final Writes writes;
    protected final Result result;
    protected final Topologies topologies;
    protected final QuorumTracker tracker;
    protected final Set<Id> persistedOn;
    boolean isDone;

    protected Persist(Node node, Topologies all, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps stableDeps, Writes writes, Result result)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.txn = txn;
        this.executeAt = executeAt;
        this.stableDeps = stableDeps;
        this.writes = writes;
        this.result = result;
        this.topologies = all;
        this.tracker = new QuorumTracker(all);
        this.persistedOn = new HashSet<>();
    }

    @Override
    public void onSuccess(Id from, ApplyReply reply)
    {
        switch (reply)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Applied:
                persistedOn.add(from);
                if (tracker.recordSuccess(from) == Success)
                {
                    if (!isDone)
                    {
                        isDone = true;
                        Topologies topologies = tracker.topologies();
                        Topology topology = topologies.forEpoch(txnId.epoch());
                        int homeShardIndex = topology.indexForKey(route.homeKey());
                        // we can persist only partially if some shards are already completed; in this case the home shard may not participate
                        if (homeShardIndex >= 0)
                            node.send(topology.get(homeShardIndex).nodes, to -> new InformDurable(to, topologies, route, txnId, executeAt, Majority));
                    }
                }
                break;
            case Insufficient:
                Apply.sendMaximal(node, from, txnId, route, txn, executeAt, stableDeps, writes, result);
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        // TODO (desired, consider): send knowledge of partial persistence?
        // TODO (expected): we should presumably report total request failure somewhere?
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        // TODO (expected): handle exception
    }

    public void start(Apply.Factory factory, Apply.Kind kind, Topologies all, Writes writes, Result result)
    {
        // applyMinimal is used for transaction execution by the original coordinator so it's important to use
        // Node's Apply factory in case the factory has to do synchronous Apply.
        SortedArrays.SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        if (contact == null)
        {
            // TODO (expected): we should presumably report this somewhere?
        }
        else
        {
            node.send(contact, to -> factory.create(kind, to, all, txnId, route, txn, executeAt, stableDeps, writes, result), this);
        }
    }
}
