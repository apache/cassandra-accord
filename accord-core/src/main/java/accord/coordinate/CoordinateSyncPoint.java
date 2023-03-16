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

import accord.local.Node;
import accord.messages.Apply;
import accord.messages.Commit;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;

import static accord.coordinate.Propose.Invalidate.proposeAndCommitInvalidate;
import static accord.primitives.Timestamp.mergeMax;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Functions.foldl;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateSyncPoint extends CoordinatePreAccept<SyncPoint>
{
    private CoordinateSyncPoint(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        super(node, txnId, txn, route);
    }

    public static AsyncResult<SyncPoint> exclusive(Node node, Seekables<?, ?> keysOrRanges)
    {
        return coordinate(ExclusiveSyncPoint, node, keysOrRanges);
    }

    public static AsyncResult<SyncPoint> inclusive(Node node, Seekables<?, ?> keysOrRanges)
    {
        return coordinate(Kind.SyncPoint, node, keysOrRanges);
    }

    private static AsyncResult<SyncPoint> coordinate(Kind kind, Node node, Seekables<?, ?> keysOrRanges)
    {
        TxnId txnId = node.nextTxnId(kind, keysOrRanges.domain());
        FullRoute<?> route = node.computeRoute(txnId, keysOrRanges);
        CoordinateSyncPoint coordinate = new CoordinateSyncPoint(node, txnId, node.agent().emptyTxn(kind, keysOrRanges), route);
        coordinate.start();
        return coordinate;
    }

    void onPreAccepted(List<PreAcceptOk> successes)
    {
        Deps deps = Deps.merge(successes, ok -> ok.deps);
        Timestamp executeAt = foldl(successes, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        if (executeAt.isRejected())
        {
            proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, executeAt, this);
        }
        else
        {
            // SyncPoint transactions always propose their own txnId as their executeAt, as they are not really executed.
            // They only create happens-after relationships wrt their dependencies, which represent all transactions
            // that *may* execute before their txnId, so once these dependencies apply we can say that any action that
            // awaits these dependencies applies after them. In the case of ExclusiveSyncPoint, we additionally guarantee
            // that no lower TxnId can later apply.
            new Propose<SyncPoint>(node, tracker.topologies(), Ballot.ZERO, txnId, txn, route, deps, txnId, this)
            {
                @Override
                void onAccepted()
                {
                    Topologies commit = node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch());
                    Topologies latest = commit.size() == 1 ? commit : node.topology().forEpoch(route, executeAt.epoch());
                    if (latest != commit)
                        node.send(commit.nodes(), id -> new Commit(Commit.Kind.Maximal, id, commit.forEpoch(txnId.epoch()), commit, txnId, txn, route, null, executeAt, deps, false));
                    node.send(latest.nodes(), id -> new Apply(id, latest, latest, executeAt.epoch(), txnId, route, txn, executeAt, deps, txn.execute(txnId, null), txn.result(txnId, null)));
                    accept(new SyncPoint(txnId, deps, route), null);
                }
            }.start();
        }
    }
}
