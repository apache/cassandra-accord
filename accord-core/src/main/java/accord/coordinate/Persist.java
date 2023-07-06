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
import accord.messages.Commit;
import accord.messages.InformDurable;
import accord.primitives.*;
import accord.topology.Topologies;

import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.local.Status.Durability.Majority;
import static accord.messages.Commit.Kind.Maximal;

public class Persist implements Callback<ApplyReply>
{
    final Node node;
    final TxnId txnId;
    final FullRoute<?> route;
    final Txn txn;
    final Timestamp executeAt;
    final Deps deps;
    final QuorumTracker tracker;
    final Set<Id> persistedOn;
    boolean isDone;

    // persistTo should be a superset of applyTo, and includes those replicas/ranges that no longer replicate at the execution epoch
    // but did replicate for coordination and would like to be informed of the transaction's status (i.e. apply a no-op apply)
    public static void persist(Node node, Topologies persistTo, Topologies appliesTo, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Persist persist = new Persist(node, appliesTo, txnId, route, txn, executeAt, deps);
        node.send(persistTo.nodes(), to -> new Apply(to, persistTo, appliesTo, txnId, route, txn, executeAt, deps, writes, result), persist);
    }

    public static void persistAndCommitMaximal(Node node, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies coordinate = node.topology().forEpoch(route, txnId.epoch());
        Topologies applyTo = node.topology().forEpoch(route, executeAt.epoch());
        Topologies persistTo = txnId.epoch() == executeAt.epoch() ? applyTo : node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
        Persist persist = new Persist(node, persistTo, txnId, route, txn, executeAt, deps);
        node.send(persistTo.nodes(), to -> new Commit(Maximal, to, coordinate.current(), persistTo, txnId, txn, route, null, executeAt, deps, false));
        node.send(applyTo.nodes(), to -> new Apply(to, persistTo, applyTo, txnId, route, txn, executeAt, deps, writes, result), persist);
    }

    private Persist(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.deps = deps;
        this.route = route;
        this.tracker = new QuorumTracker(topologies);
        this.executeAt = executeAt;
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
                        node.send(topologies.nodes(), to -> new InformDurable(to, topologies, route, txnId, executeAt, Majority));
                    }
                }
                break;
            case Insufficient:
                Topologies topologies = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                // TODO (easy, cleanup): use static method in Commit
                node.send(from, new Commit(Maximal, from, topologies.forEpoch(txnId.epoch()), topologies, txnId, txn, route, null, executeAt, deps, false));
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        // TODO (desired, consider): send knowledge of partial persistence?
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
    }
}
