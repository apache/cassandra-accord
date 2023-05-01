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
import java.util.function.BiConsumer;

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
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Topologies;

import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.local.Status.Durability.Majority;
import static accord.messages.Apply.executes;
import static accord.messages.Apply.participates;

public abstract class Persist implements Callback<ApplyReply>
{
    public interface Factory
    {
        Persist create(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result);
    }

    protected final Node node;
    protected final TxnId txnId;
    protected final FullRoute<?> route;
    protected final Txn txn;
    protected final Timestamp executeAt;
    protected final Deps deps;
    protected final Writes writes;
    protected final Result result;
    protected final Topologies topologies;
    protected final QuorumTracker tracker;
    protected final Set<Id> persistedOn;
    boolean isDone;

    public static void persist(Node node, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        Topologies executes = executes(node, route, executeAt);
        persist(node, executes, txnId, route, txn, executeAt, deps, writes, result, clientCallback);
    }

    public static void persist(Node node, Topologies executes, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        node.persistFactory().create(node, executes, txnId, route, txn, executeAt, deps, writes, result)
                             .applyMinimal(participates, executes, writes, result, clientCallback);
    }

    public static void persistMaximal(Node node, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies executes = executes(node, route, executeAt);
        Topologies participates = participates(node, route, txnId, executeAt, executes);
        node.persistFactory().create(node, participates, txnId, route, txn, executeAt, deps, writes, result)
                             .applyMaximal(participates, executes, writes, result, null);
    }

    public static void persistPartialMaximal(Node node, TxnId txnId, Unseekables<?> sendTo, FullRoute<?> route, PartialTxn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        Topologies executes = executes(node, sendTo, executeAt);
        Topologies participates = participates(node, sendTo, txnId, executeAt, executes);
        Persist persist = node.persistFactory().create(node, participates, txnId, route, txn, executeAt, deps, writes, result);
        node.send(participates.nodes(), to -> Apply.applyMaximal(Apply.FACTORY, to, participates, executes, txnId, route, txn, executeAt, deps, writes, result), persist);
    }

    protected Persist(Node node, Topologies topologies, TxnId txnId, FullRoute<?> route, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.txn = txn;
        this.executeAt = executeAt;
        this.deps = deps;
        this.writes = writes;
        this.result = result;
        this.topologies = topologies;
        this.tracker = new QuorumTracker(topologies);
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
                Apply.sendMaximal(node, from, txnId, route, txn, executeAt, deps, writes, result);
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

    public void applyMinimal(Topologies participates, Topologies executes, Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        registerClientCallback(writes, result, clientCallback);
        // applyMinimal is used for transaction execution by the original coordinator so it's important to use
        // Node's Apply factory in case the factory has to do synchronous Apply.
        node.send(participates.nodes(), to -> Apply.applyMinimal(node.applyFactory(), to, participates, executes, txnId, route, txn, executeAt, deps, writes, result), this);
    }
    public void applyMaximal(Topologies participates, Topologies executes, Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback)
    {
        registerClientCallback(writes, result, clientCallback);
        node.send(participates.nodes(), to -> Apply.applyMaximal(Apply.FACTORY, to, participates, executes, txnId, route, txn, executeAt, deps, writes, result), this);
    }

    public abstract void registerClientCallback(Writes writes, Result result, BiConsumer<? super Result, Throwable> clientCallback);
}
