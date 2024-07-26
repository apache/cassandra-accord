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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.coordinate.CoordinationAdapter.Adapters;
import accord.local.Node;
import accord.messages.Apply;
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
import accord.topology.TopologyManager;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.coordinate.CoordinationAdapter.Invoke.execute;
import static accord.coordinate.CoordinationAdapter.Invoke.propose;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.Propose.Invalidate.proposeAndCommitInvalidate;
import static accord.primitives.Timestamp.mergeMax;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.topology.TopologyManager.EpochSufficiencyMode.AT_LEAST;
import static accord.utils.Functions.foldl;
import static accord.utils.Invariants.checkArgument;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateSyncPoint<S extends Seekables<?, ?>> extends CoordinatePreAccept<SyncPoint<S>>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CoordinateSyncPoint.class);

    final CoordinationAdapter<SyncPoint<S>> adapter;

    private CoordinateSyncPoint(Node node, TxnId txnId, Txn txn, FullRoute<?> route, CoordinationAdapter<SyncPoint<S>> adapter)
    {
        super(node, txnId, txn, route, node.topology().withOpenEpochs(route, txnId, txnId, AT_LEAST));
        this.adapter = adapter;
    }

    public static <S extends Seekables<?, ?>> AsyncResult<SyncPoint<S>> exclusive(Node node, S keysOrRanges)
    {
        return coordinate(node, ExclusiveSyncPoint, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <S extends Seekables<?, ?>> AsyncResult<SyncPoint<S>> exclusive(Node node, TxnId txnId, S keysOrRanges)
    {
        return coordinate(node, txnId, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <S extends Seekables<?, ?>> AsyncResult<SyncPoint<S>> inclusive(Node node, S keysOrRanges)
    {
        return coordinate(node, Kind.SyncPoint, keysOrRanges, Adapters.inclusiveSyncPoint());
    }

    public static <S extends Seekables<?, ?>> AsyncResult<SyncPoint<S>> inclusiveAndAwaitQuorum(Node node, S keysOrRanges)
    {
        return coordinate(node, Kind.SyncPoint, keysOrRanges, Adapters.inclusiveSyncPointBlocking());
    }

    public static <S extends Seekables<?, ?>> AsyncResult<SyncPoint<S>> coordinate(Node node, Kind kind, S keysOrRanges, CoordinationAdapter<SyncPoint<S>> adapter)
    {
        checkArgument(kind == Kind.SyncPoint || kind == ExclusiveSyncPoint);
        TxnId txnId = node.nextTxnId(kind, keysOrRanges.domain());
        return node.withEpoch(txnId.epoch(), () -> coordinate(node, txnId, keysOrRanges, adapter)).beginAsResult();
    }

    private static <S extends Seekables<?, ?>> AsyncResult<SyncPoint<S>> coordinate(Node node, TxnId txnId, S keysOrRanges, CoordinationAdapter<SyncPoint<S>> adapter)
    {
        checkArgument(txnId.kind() == Kind.SyncPoint || txnId.kind() == ExclusiveSyncPoint);
        FullRoute<?> route = node.computeRoute(txnId, keysOrRanges);
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), keysOrRanges);
        if (mismatch != null)
            return AsyncResults.failure(mismatch);
        CoordinateSyncPoint<S> coordinate = new CoordinateSyncPoint<>(node, txnId, node.agent().emptyTxn(txnId.kind(), keysOrRanges), route, adapter);
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
        Deps deps = Deps.merge(oks, ok -> ok.deps);
        Timestamp checkRejected = foldl(oks, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        if (checkRejected.isRejected())
        {
            proposeAndCommitInvalidate(node, Ballot.ZERO, txnId, route.homeKey(), route, checkRejected, this);
        }
        else
        {
            // we don't need to fetch deps from Accept replies, so we don't need to contact unsynced epochs
            topologies = node.topology().forEpoch(route, txnId.epoch());
            // TODO (required): consider the required semantics of a SyncPoint
            if (tracker.hasFastPathAccepted() && txnId.kind() == Kind.SyncPoint)
                execute(adapter, node, topologies, route, FAST, txnId, txn, txnId, deps, this);
            else
                propose(adapter, node, topologies, route, Ballot.ZERO, txnId, txn, executeAt, deps, this);
        }
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint)
    {
        TxnId txnId = syncPoint.syncId;
        Timestamp executeAt = txnId;
        Txn txn = node.agent().emptyTxn(txnId.kind(), syncPoint.keysOrRanges);
        Deps deps = syncPoint.waitFor;
        Apply.sendMaximal(node, to, txnId, syncPoint.route(), txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null));
    }
}
