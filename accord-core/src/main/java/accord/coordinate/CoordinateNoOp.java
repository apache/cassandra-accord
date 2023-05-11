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

import java.util.List;
import java.util.Set;

import accord.api.Result;
import accord.local.Node;
import accord.messages.Apply;
import accord.messages.Commit;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;

import static accord.coordinate.Propose.Invalidate.proposeAndCommitInvalidate;
import static accord.messages.Commit.Kind.Maximal;
import static accord.primitives.Txn.Kind.NoOp;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateNoOp extends CoordinatePreAccept<Timestamp>
{
    private CoordinateNoOp(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(node, txnId, txn, route);
    }

    public static AsyncResult<Timestamp> coordinate(Node node, Seekables<?, ?> keysOrRanges)
    {
        TxnId txnId = node.nextTxnId(NoOp, keysOrRanges.domain());
        return coordinate(node, txnId, keysOrRanges);
    }

    public static AsyncResult<Timestamp> coordinate(Node node, TxnId txnId, Seekables<?, ?> keysOrRanges)
    {
        Invariants.checkArgument(txnId.rw() == NoOp);
        FullRoute<?> route = node.computeRoute(txnId, keysOrRanges);
        CoordinateNoOp coordinate = new CoordinateNoOp(node, txnId, node.agent().emptyTxn(NoOp, keysOrRanges), route);
        coordinate.start();
        return coordinate;
    }

    @Override
    void onPreAccepted(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> successes)
    {
        if (executeAt.isRejected())
        {
            proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, executeAt, this);
        }
        else
        {
            new Propose<Timestamp>(node, topologies, Ballot.ZERO, txnId, txn, route, executeAt, Deps.NONE, this)
            {
                @Override
                void onAccepted()
                {
                    Topologies applyTo = node.topology().forEpoch(route, executeAt.epoch());
                    Topologies persistTo = txnId.epoch() == executeAt.epoch() ? applyTo : node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                    Writes writes = txn.execute(txnId, txnId, null);
                    Result result = txn.result(txnId, executeAt, null);
                    // TODO (now): permit Apply to Commit in these cases
                    Set<Node.Id> nodes = persistTo.nodes();
                    node.send(nodes, id -> new Commit(Maximal, id, persistTo.forEpoch(txnId.epoch()), persistTo, txnId, txn, route, Ranges.EMPTY, executeAt, deps, false));
                    node.send(nodes, id -> new Apply(id, persistTo, applyTo, txnId, route, txn, executeAt, deps, writes, result));
                    accept(executeAt, null);
                }
            }.start();
        }
    }
}
