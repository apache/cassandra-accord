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

import accord.api.Result;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.messages.PreAccept;
import accord.topology.Topologies;
import accord.local.Node;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.coordinate.CoordinationAdapter.Factory.Step.Continue;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.Propose.Invalidate.proposeAndCommitInvalidate;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateTransaction extends CoordinatePreAccept<Result>
{
    final Txn txn;

    private CoordinateTransaction(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(node, txnId, txn, route);
        this.txn = txn;
    }

    public static AsyncResult<Result> coordinate(Node node, FullRoute<?> route, TxnId txnId, Txn txn)
    {
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatchOrPendingRemoval(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), txn.keys());
        if (mismatch != null)
            return AsyncResults.failure(mismatch);
        CoordinateTransaction coordinate = new CoordinateTransaction(node, txnId, txn, route);
        coordinate.start();
        return coordinate;
    }

    @Override
    void onPreAccepted(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> oks)
    {
        // TODO (expected): this condition is too strong - we only need fast path _votes_ in the proposal epoch
        //  but must have replied from a fast path quorum in earlier epochs
        if (tracker.hasFastPathAccepted())
        {
            Deps deps = Deps.merge(oks, oks.size(), List::get, ok -> ok.witnessedAt.equals(txnId) ? ok.deps : null);
            executeAdapter().execute(node, topologies, route, FAST, txnId, txn, txnId, deps, settingCallback());
            node.agent().metricsEventsListener().onFastPathTaken(txnId, deps);
        }
        else
        {
            Deps deps = Deps.merge(oks, oks.size(), List::get, ok -> ok.deps);

            // TODO (low priority, efficiency): perhaps don't submit Accept immediately if we almost have enough for fast-path,
            //                                  but by sending accept we rule out hybrid fast-path
            // TODO (low priority, efficiency): if we receive an expired response, perhaps defer to permit at least one other
            //                                  node to respond before invalidating
            if (executeAt.isRejected() || executeAt.hlc() - txnId.hlc() >= node.agent().preAcceptTimeout())
            {
                proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, executeAt,this);
            }
            else
            {
                if (PreAccept.rejectExecuteAt(txnId, topologies))
                    proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, executeAt, this);
                else
                    proposeAdapter().propose(node, topologies, route, Ballot.ZERO, txnId, txn, executeAt, deps, this);
            }

            node.agent().metricsEventsListener().onSlowPathTaken(txnId, deps);
        }
    }

    protected CoordinationAdapter<Result> proposeAdapter()
    {
        return Adapters.standard();
    }

    // TODO (expected): override in C* rather than default to configurability here
    protected CoordinationAdapter<Result> executeAdapter()
    {
        return node.coordinationAdapter(txnId, Continue);
    }
}
