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

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.CoordinationAdapter.Adapters.SyncPointAdapter;
import accord.local.Node;
import accord.messages.Apply;
import accord.messages.Callback;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.Propose.Invalidate.proposeAndCommitInvalidate;
import static accord.messages.Apply.executes;
import static accord.primitives.Timestamp.mergeMax;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.utils.Functions.foldl;
import static accord.utils.Invariants.checkArgument;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateSyncPoint<U extends Unseekable> extends CoordinatePreAccept<SyncPoint<U>>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CoordinateSyncPoint.class);

    final CoordinationAdapter<SyncPoint<U>> adapter;

    private CoordinateSyncPoint(Node node, TxnId txnId, Topologies topologies, Txn txn, FullRoute<?> route, SyncPointAdapter<U> adapter)
    {
        super(node, txnId, txn, route, topologies, adapter.preacceptTrackerFactory);
        this.adapter = adapter;
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> exclusiveSyncPoint(Node node, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, ExclusiveSyncPoint, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> exclusiveSyncPoint(Node node, TxnId txnId, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, txnId, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> exclusiveSyncPoint(Node node, TxnId txnId, FullRoute<U> route)
    {
        return coordinate(node, txnId, route, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusive(Node node, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, Kind.SyncPoint, keysOrRanges, Adapters.inclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusive(Node node, FullRoute<U> route)
    {
        return coordinate(node, Kind.SyncPoint, route, Adapters.inclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusive(Node node, TxnId txnId, FullRoute<U> route)
    {
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, route, Adapters.inclusiveSyncPoint())).beginAsResult();
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusiveAndAwaitQuorum(Node node, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, Kind.SyncPoint, keysOrRanges, Adapters.inclusiveSyncPointBlocking());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusiveAndAwaitQuorum(Node node, FullRoute<U> route)
    {
        return coordinate(node, Kind.SyncPoint, route, Adapters.inclusiveSyncPointBlocking());
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> inclusiveAndAwaitQuorum(Node node, TxnId txnId, FullRoute<U> route)
    {
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, route, Adapters.inclusiveSyncPointBlocking())).beginAsResult();
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> coordinate(Node node, Kind kind, Unseekables<U> keysOrRanges, SyncPointAdapter<U> adapter)
    {
        checkArgument(kind.isSyncPoint());
        TxnId txnId = node.nextTxnId(kind, keysOrRanges.domain());
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, keysOrRanges, adapter)).beginAsResult();
    }

    public static <U extends Unseekable> AsyncResult<SyncPoint<U>> coordinate(Node node, Kind kind, FullRoute<U> route, SyncPointAdapter<U> adapter)
    {
        checkArgument(kind.isSyncPoint());
        TxnId txnId = node.nextTxnId(kind, route.domain());
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, route, adapter)).beginAsResult();
    }

    private static <U extends Unseekable> AsyncResult<SyncPoint<U>> coordinate(Node node, TxnId txnId, Unseekables<U> keysOrRanges, SyncPointAdapter<U> adapter)
    {
        checkArgument(txnId.isSyncPoint());
        FullRoute<U> route = (FullRoute<U>) node.computeRoute(txnId, keysOrRanges);
        return coordinate(node, txnId, route, adapter);
    }

    private static <U extends Unseekable> AsyncResult<SyncPoint<U>> coordinate(Node node, TxnId txnId, FullRoute<U> route, SyncPointAdapter<U> adapter)
    {
        checkArgument(txnId.isSyncPoint());
        TopologyMismatch mismatch = txnId.kind() == ExclusiveSyncPoint
                                    ? TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), route)
                                    : TopologyMismatch.checkForMismatchOrPendingRemoval(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), route);
        if (mismatch != null)
            return AsyncResults.failure(mismatch);
        CoordinateSyncPoint<U> coordinate = new CoordinateSyncPoint<>(node, txnId, adapter.forDecision(node, route, txnId), node.agent().emptySystemTxn(txnId.kind(), txnId.domain()), route, adapter);
        coordinate.start();
        return coordinate;
    }

    @Override
    long executeAtEpoch()
    {
        return txnId.epoch();
    }

    @Override
    void onPreAccepted(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> oks)
    {
        Deps deps = Deps.merge(oks, oks.size(), List::get, ok -> ok.deps);
        Timestamp checkRejected = foldl(oks, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        if (checkRejected.isRejected())
        {
            proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, checkRejected, this);
        }
        else
        {
            if (tracker.hasFastPathAccepted())
                adapter.execute(node, topologies, route, FAST, txnId, txn, txnId, deps, this);
            else
                adapter.propose(node, topologies, route, Ballot.ZERO, txnId, txn, executeAt, deps, this);
        }
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint)
    {
        // TODO (required): consider, document and add invariants checking if this topologies is correct in all cases
        //  (notably ExclusiveSyncPoints should execute in earlier epochs for durability, but not for fetching )
        sendApply(node, to, syncPoint, executes(node, syncPoint.route, syncPoint.syncId));
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint, Topologies executes)
    {
        sendApply(node, to, syncPoint, executes, null);
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint, @Nullable Callback<Apply.ApplyReply> callback)
    {
        sendApply(node, to, syncPoint, executes(node, syncPoint.route, syncPoint.syncId), callback);
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint, Topologies executes, @Nullable Callback<Apply.ApplyReply> callback)
    {
        TxnId txnId = syncPoint.syncId;
        Timestamp executeAt = txnId;
        Txn txn = node.agent().emptySystemTxn(txnId.kind(), txnId.domain());
        Deps deps = syncPoint.waitFor;
        Apply.sendMaximal(node, to, executes, txnId, syncPoint.route(), txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null), syncPoint.route(), callback);
    }
}
