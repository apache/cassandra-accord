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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.CommandStore;
import accord.local.Node;
import accord.messages.Callback;
import accord.messages.GetEphemeralReadDeps;
import accord.messages.GetEphemeralReadDeps.GetEphemeralReadDepsOk;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Seekables;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;

/**
 * An Ephemeral Read is a single-key linearizable read, that is invisible to other transactions so can be non-durable.
 * We do not need to agree any execution timestamp, we just fetch execution dependencies that represent any
 * commands that _might_ have finished before we started, and we wait for those commands to execute before executing our read.
 *
 * Being non-durable, we do not need to be recovered and so no Accept or Commit rounds are necessary.
 *
 * We must still settle on an "execution epoch" where the replicas represent an active quorum so that our dependencies
 * are accurately computed. We then may later execute in an even later epoch, if one of our dependencies agrees an execution
 * time in that later epoch.
 *
 * For single-key reads this is strict-serializable, and for multi-key or range-reads this is per-key linearizable.
 */
public class CoordinateEphemeralRead extends AbstractCoordinatePreAccept<Result, GetEphemeralReadDepsOk>
{
    public static AsyncResult<Result> coordinate(Node node, FullRoute<?> route, TxnId txnId, Txn txn)
    {
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), txn.keys());
        if (mismatch != null)
            return AsyncResults.failure(mismatch);

        Topologies topologies = node.topology().withUnsyncedEpochs(route, txnId, txnId);
        CoordinateEphemeralRead coordinate = new CoordinateEphemeralRead(node, topologies, route, txnId, txn);
        coordinate.start();
        return coordinate;
    }

    private final Txn txn;

    private final QuorumTracker tracker;
    private final List<GetEphemeralReadDepsOk> oks;
    private long executeAtEpoch;

    CoordinateEphemeralRead(Node node, Topologies topologies, FullRoute<?> route, TxnId txnId, Txn txn)
    {
        super(node, route, txnId);
        this.txn = txn;
        this.tracker = new QuorumTracker(topologies);
        this.executeAtEpoch = txnId.epoch();
        this.oks = new ArrayList<>(topologies.estimateUniqueNodes());
    }

    @Override
    Seekables<?, ?> keysOrRanges()
    {
        return txn.keys();
    }

    @Override
    void contact(Collection<Node.Id> nodes, Topologies topologies, Callback<GetEphemeralReadDepsOk> callback)
    {
        CommandStore commandStore = CommandStore.maybeCurrent();
        if (commandStore == null) commandStore = node.commandStores().select(route.homeKey());
        node.send(nodes, to -> new GetEphemeralReadDeps(to, topologies, route, txnId, txn.keys(), executeAtEpoch), commandStore, callback);
    }

    @Override
    long executeAtEpoch()
    {
        return executeAtEpoch;
    }

    @Override
    public void onSuccessInternal(Node.Id from, GetEphemeralReadDepsOk ok)
    {
        oks.add(ok);
        if (ok.latestEpoch > executeAtEpoch)
            executeAtEpoch = ok.latestEpoch;

        if (tracker.recordSuccess(from) == Success)
            onPreAcceptedOrNewEpoch();
    }

    @Override
    boolean onExtraSuccessInternal(Node.Id from, GetEphemeralReadDepsOk ok)
    {
        if (ok.latestEpoch > executeAtEpoch)
            executeAtEpoch = ok.latestEpoch;

        oks.add(ok);
        return true;
    }

    @Override
    public void onFailureInternal(Node.Id from, Throwable failure)
    {
        if (tracker.recordFailure(from) == Failed)
            setFailure(new Timeout(txnId, route.homeKey()));
    }

    @Override
    void onNewEpochTopologyMismatch(TopologyMismatch mismatch)
    {
        accept(null, mismatch);
    }

    @Override
    void onPreAccepted(Topologies topologies)
    {
        Deps deps = Deps.merge(oks, ok -> ok.deps);
        topologies = topologies.forEpochs(executeAtEpoch, executeAtEpoch);
        new ExecuteEphemeralRead(node, topologies, route, txnId, txn, executeAtEpoch, deps, this).start();
    }
}
