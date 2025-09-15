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

import accord.api.Result;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Apply;
import accord.messages.Apply.ApplyReply;
import accord.messages.InformDurable;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.SortedArrays;

import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.messages.Apply.Kind.Maximal;
import static accord.primitives.Status.Durability.AllQuorums;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

public abstract class Persist extends AbstractCoordination<FullRoute<?>, Void, ApplyReply, Void>
{
    protected final Ballot ballot;
    protected final Route<?> sendTo;
    protected final Txn txn;
    protected final Timestamp executeAt;
    protected final Deps stableDeps;
    protected final Writes writes;
    protected final Result result;
    protected final CoordinationFlags flags;
    // TODO (expected): track separate ALL and Quorum, so we can report Universal durability to permit faster GC
    protected final QuorumTracker tracker;
    protected final Apply.Factory factory;
    protected final Apply.Kind applyKind;
    protected final boolean informDurableOnDone;

    protected Persist(Node node, SequentialAsyncExecutor executor, Topologies all, TxnId txnId, Ballot ballot, Route<?> sendTo, Txn txn, Timestamp executeAt, Deps stableDeps, Writes writes, Result result, FullRoute<?> route, CoordinationFlags flags, boolean informDurableOnDone, Apply.Factory factory, Apply.Kind applyKind)
    {
        super(node, executor, txnId, route, all.nodes(), node.agent());
        this.ballot = ballot;
        this.sendTo = sendTo;
        this.txn = txn;
        this.executeAt = executeAt;
        this.stableDeps = stableDeps;
        this.writes = writes;
        this.result = result;
        this.flags = flags;
        this.tracker = new QuorumTracker(all);
        this.factory = factory;
        this.informDurableOnDone = informDurableOnDone;
        this.applyKind = applyKind;
        Invariants.require((writes != null) == txnId.is(Txn.Kind.Write), "%s: writes %s", txnId, writes);
    }

    @Override
    public void onSuccessInternal(Id from, int fromIndex, ApplyReply reply)
    {
        switch (reply.kind)
        {
            default: throw new IllegalStateException();
            case Redundant:
            case Applied:
                if (tracker.recordSuccess(from) == Success)
                {
                    // note: we enter this branch whether or not sendTo == route,
                    // since we should only invoke Persist with sendTo != route when the remainder of the route is already persisted and truncated
                    // but we make this explicit for the caller with informDurableOnDone
                    finishWithSuccess(null);
                    if (informDurableOnDone)
                        InformDurable.informDefault(node, tracker.topologies(), txnId, scope, ballot, executeAt, AllQuorums);
                }
                break;
            case RaceWithRecovery:
                // don't count this towards durability; otherwise it is possible (though very unlikely)
                // for InformDurable to reach a replica, the CommandsForKey to prune this transaction as durably applied
                // and some superseding transaction to be coordinated and not witness it before the in-progress recovery
                // reached another response in its quorum; in this case it may incorrectly determine the transaction should be invalidated
                onFailureInternal(from, fromIndex, null);
                break;
            case Insufficient:
                Invariants.expect(applyKind != Maximal, "Received Insufficient reply from %s, but already sent Maximal", from);
                node.send(from, factory.create(Maximal, from, tracker.topologies(), txnId, ballot, sendTo, txn, executeAt, stableDeps, writes, result, scope, flags.get(from)));
                break;

            case InsufficientEpochs:
                Invariants.requireArgument(txnId.isSyncPoint());
                node.send(from, factory.create(Maximal, from, node.topology().preciseEpochs(scope, reply.minEpoch(), tracker.topologies().currentEpoch(), SHARE) , txnId, ballot, sendTo, txn, executeAt, stableDeps, writes, result, scope, flags.get(from)));
                break;
        }
    }

    @Override
    public void onFailureInternal(Id from, int fromIndex, Throwable failure)
    {
        // TODO (desired, consider): send knowledge of partial persistence?
        // TODO (expected): we should presumably report total request failure somewhere?
        if (tracker.recordFailure(from) == RequestStatus.Failed)
            finishOnFailure();
    }

    @Override
    protected void start()
    {
        node.agent().coordinatorEvents().onExecuted(txnId, ballot);
        // applyMinimal is used for transaction execution by the original coordinator so it's important to use
        // Node's Apply factory in case the factory has to do synchronous Apply.
        SortedArrays.SortedArrayList<Node.Id> contact = tracker.filterAndRecordFaulty();
        if (contact == null)
        {
            finishOnExaustion();
        }
        else
        {
            super.start();
            contact(to -> factory.create(applyKind, to, tracker.topologies(), txnId, ballot, sendTo, txn, executeAt, stableDeps, writes, result, scope, flags.get(to)));
        }
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.Persist;
    }

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }
}
