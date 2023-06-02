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
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;
import accord.utils.Invariants;

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
    final Ranges ranges;
    private CoordinateSyncPoint(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Ranges ranges)
    {
        super(node, txnId, txn, route, node.topology().withOpenEpochs(route, txnId, txnId));
        this.ranges = ranges;
    }

    public static AsyncResult<SyncPoint> exclusive(Node node, Ranges keysOrRanges)
    {
        return coordinate(node, ExclusiveSyncPoint, keysOrRanges);
    }

    public static AsyncResult<SyncPoint> inclusive(Node node, Ranges keysOrRanges)
    {
        return coordinate(node, Kind.SyncPoint, keysOrRanges);
    }

    private static AsyncResult<SyncPoint> coordinate(Node node, Kind kind, Ranges ranges)
    {
        TxnId txnId = node.nextTxnId(kind, ranges.domain());
        return node.withEpoch(txnId.epoch(), () -> {
            FullRangeRoute route = (FullRangeRoute) node.computeRoute(txnId, ranges);
            CoordinateSyncPoint coordinate = new CoordinateSyncPoint(node, txnId, node.agent().emptyTxn(kind, ranges), route, ranges);
            coordinate.start();
            return coordinate;
        }).beginAsResult();
    }

    public static AsyncResult<SyncPoint> coordinate(Node node, TxnId txnId, Ranges ranges)
    {
        Invariants.checkState(txnId.rw() == Kind.SyncPoint || txnId.rw() == ExclusiveSyncPoint);
        FullRangeRoute route = (FullRangeRoute) node.computeRoute(txnId, ranges);
        CoordinateSyncPoint coordinate = new CoordinateSyncPoint(node, txnId, node.agent().emptyTxn(txnId.rw(), ranges), route, ranges);
        coordinate.start();
        return coordinate;
    }

    @Override
    void onNewEpoch(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> successes)
    {
        // SyncPoint transactions always propose their own txnId as their executeAt, as they are not really executed.
        // They only create happens-after relationships wrt their dependencies, which represent all transactions
        // that *may* execute before their txnId, so once these dependencies apply we can say that any action that
        // awaits these dependencies applies after them. In the case of ExclusiveSyncPoint, we additionally guarantee
        // that no lower TxnId can later apply.
        onPreAccepted(topologies, executeAt, successes);
    }

    @Override
    void onPreAccepted(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> successes)
    {
        Deps deps = Deps.merge(successes, ok -> ok.deps);
        Timestamp checkRejected = foldl(successes, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        if (checkRejected.isRejected())
        {
            proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, checkRejected, this);
        }
        else
        {
            executeAt = txnId;
            // we don't need to fetch deps from Accept replies, so we don't need to contact unsynced epochs
            topologies = node.topology().forEpoch(route, txnId.epoch());
            new Propose<SyncPoint>(node, topologies, Ballot.ZERO, txnId, txn, route, executeAt, deps, this)
            {
                @Override
                void onAccepted()
                {
                    Apply.sendMaximal(node, txnId, route, txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null));
                    node.configService().reportEpochClosed(ranges, txnId.epoch());
                    accept(new SyncPoint(txnId, deps, ranges, (FullRangeRoute) route), null);
                }
            }.start();
        }
    }

    static void sendApply(Node node, Node.Id to, SyncPoint syncPoint)
    {
        TxnId txnId = syncPoint.syncId;
        Timestamp executeAt = txnId;
        Txn txn = node.agent().emptyTxn(txnId.rw(), syncPoint.ranges);
        Deps deps = syncPoint.waitFor;
        Apply.sendMaximal(node, to, txnId, syncPoint.route(), txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null));
    }
}
