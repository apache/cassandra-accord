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

import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.GetEphemeralReadDeps;
import accord.messages.GetEphemeralReadDeps.GetEphemeralReadDepsOk;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.TopologyMismatch;
import accord.utils.Invariants;
import accord.utils.SortedListMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Owned;
import static accord.coordinate.ExecuteFlag.CoordinationFlags.empty;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.topology.SelectShards.LIVE;

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
public class CoordinateEphemeralRead extends AbstractCoordinatePreAccept<Result, GetEphemeralReadDepsOk, GetEphemeralReadDepsOk>
{
    public static AsyncChain<Result> coordinate(Node node, TxnId txnId, Txn txn)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            public @Nullable Cancellable start(BiConsumer<? super Result, Throwable> callback)
            {
                coordinate(node, txnId, txn, callback);
                return null;
            }
        };
    }

    public static void coordinate(Node node, TxnId txnId, Txn txn, BiConsumer<? super Result, Throwable> callback)
    {
        CoordinateEphemeralRead coordinate;
        try
        {
            FullRoute<?> route = node.computeRoute(txnId, txn.keys());
            Topologies topologies = node.topology().active().withUnsyncedEpochs(route, txnId, txnId);
            coordinate = new CoordinateEphemeralRead(node, node.someExclusiveExecutor(), topologies, route, txnId, txn, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        coordinate.start();
    }

    private final Txn txn;

    private final QuorumTracker tracker;
    private long executeAtEpoch;
    private long retryInEpoch;

    CoordinateEphemeralRead(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, FullRoute<?> route, TxnId txnId, Txn txn, BiConsumer<? super Result, Throwable> callback)
    {
        super(node, executor, topologies, route, txnId, callback);
        this.txn = txn;
        this.tracker = new QuorumTracker(topologies);
        this.executeAtEpoch = txnId.epoch();
    }

    @Override
    void start()
    {
        super.start();
        contact(to -> new GetEphemeralReadDeps(to, topologies, scope, txnId, executeAtEpoch));
    }

    @Override
    long executeAtEpoch()
    {
        return executeAtEpoch;
    }

    @Override
    public void onSuccessInternal(Node.Id from, int fromIndex, GetEphemeralReadDepsOk ok)
    {
        if (ok.deps != null)
        {
            recordOk(fromIndex, ok);
            if (ok.latestEpoch > executeAtEpoch)
                executeAtEpoch = ok.latestEpoch;

            if (tracker.recordSuccess(from) == Success)
                onPreAcceptedOrNewEpoch();
        }
        else
        {
            retryInEpoch = ok.latestEpoch;
            if (tracker.recordFailure(from) == Failed)
                retry();
        }
    }

    @Override
    public void onFailureInternal(Node.Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        if (tracker.recordFailure(from) == Failed)
        {
            if (retryInEpoch > 0) retry();
            else finishOnFailure();
        }
    }

    private void retry()
    {
        awaitEpochExactToFinish(retryInEpoch, () -> coordinate(node, txnId.withEpoch(retryInEpoch), txn, finishAndTakeCallback()));
    }

    @Override
    void onNewEpochTopologyMismatch(TopologyMismatch mismatch)
    {
        finishOnFailure(mismatch);
    }

    @Override
    void onPreAccepted()
    {
        node.agent().coordinatorEvents().onPreAccepted(txnId);
        SortedListMap<Node.Id, GetEphemeralReadDepsOk> oks = finishOks();
        Deps deps = Deps.merge(oks, oks.domainSize(), SortedListMap::getValue, ok -> ok.deps);
        Topologies topologies;
        try { topologies = node.topology().active().reselect(this.topologies, QuorumEpochIntersections.preaccept.include, scope, executeAtEpoch, executeAtEpoch, LIVE, Owned); }
        catch (Throwable t)
        {
            finishOnFailure(t);
            return;
        }
        CoordinationFlags flags = oks.foldlNonNull((d, k, v, out) -> {
            ExecuteFlags.collect(out, k, v.flags, d, v.deps);
            return out;
        }, deps, empty(oks.domain()));
        new ExecuteEphemeralRead(node, executor, topologies, scope, txnId.withEpoch(executeAtEpoch), txn, deps, flags, finishAndTakeCallback()).start();
        if (!Invariants.debug()) oks.clear();
    }

    @Nonnull
    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }
}
