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
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Result.PersistableResult;
import accord.api.Tracing;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.CoordinationAdapter.Adapters.SyncPointAdapter;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialSyncPoint;
import accord.primitives.SyncPoint;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.TopologyException;
import accord.utils.Invariants;
import accord.utils.SortedListMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.coordinate.CoordinationAdapter.Adapters.exclusiveSyncPoint;
import static accord.coordinate.Propose.NotAccept.proposeAndCommitInvalidate;
import static accord.messages.Apply.Kind.Maximal;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Timestamp.Flag.REJECTED;
import static accord.primitives.Timestamp.mergeMaxAndFlags;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static accord.topology.SelectShards.ALL;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
public class CoordinateSyncPoint<R> extends CoordinatePreAccept<R>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CoordinateSyncPoint.class);

    final CoordinationAdapter<R> adapter;

    private CoordinateSyncPoint(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, Topologies topologies, Txn txn, FullRoute<?> route, SyncPointAdapter<R> adapter, BiConsumer<? super R, Throwable> callback)
    {
        super(node, executor, txnId, txn, route, topologies, adapter.preacceptTrackerFactory, callback);
        this.adapter = adapter;
    }

    public static AsyncChain<SyncPoint> exclusive(Node node, Ranges ranges)
    {
        return coordinate(node, ExclusiveSyncPoint, ranges, Adapters.exclusiveSyncPoint());
    }

    public static AsyncChain<SyncPoint> exclusive(Node node, TxnId txnId, Ranges ranges)
    {
        return coordinate(node, txnId, ranges, Adapters.exclusiveSyncPoint());
    }

    public static AsyncChain<SyncPoint> coordinate(Node node, Txn.Kind kind, Ranges ranges, SyncPointAdapter<SyncPoint> adapter)
    {
        Invariants.requireArgument(kind.isSyncPoint());
        TxnId txnId = node.nextTxnIdWithDefaultFlags(ranges, kind, ranges.domain(), cardinality(ranges));
        return node.withEpochExact(txnId.epoch(), null, () -> coordinate(node, txnId, ranges, adapter));
    }

    private static AsyncChain<SyncPoint> coordinate(Node node, TxnId txnId, Ranges ranges, SyncPointAdapter<SyncPoint> adapter)
    {
        Invariants.requireArgument(txnId.isSyncPoint());
        try
        {
            return new AsyncChains.Head<>()
            {
                @Override
                protected @Nullable Cancellable start(BiConsumer<? super SyncPoint, Throwable> callback)
                {
                    CoordinateSyncPoint<SyncPoint> coordinate;
                    try
                    {
                        FullRoute<Range> route = (FullRoute<Range>) node.computeRoute(txnId, ranges);
                        Txn txn = node.agent().emptySystemTxn(txnId.kind(), txnId.domain());
                        Topologies topologies = adapter.forDecision(node, route, txnId, txnId);
                        coordinate = new CoordinateSyncPoint<>(node, node.someExclusiveExecutor(), txnId, topologies, txn, route, adapter, callback);
                    }
                    catch (Throwable t)
                    {
                        callback.accept(null, t);
                        return null;
                    }

                    coordinate.start();
                    return null;
                }
            };
        }
        catch (Throwable t)
        {
            return AsyncChains.failure(t);
        }
    }

    @Override
    void start()
    {
        super.start();
        contact(null, false);
    }

    @Override
    long executeAtEpoch()
    {
        return txnId.epoch();
    }

    @Override
    void onPreAccepted(Topologies topologies, SortedListMap<Node.Id, PreAcceptOk> oks)
    {
        Timestamp executeAt = oks.foldlNonNullValues((ok, prev) -> mergeMaxAndFlags(ok.witnessedAt, prev), Timestamp.NONE);
        if (executeAt.is(REJECTED))
        {
            node.agent().coordinatorEvents().onRejected(txnId);
            proposeAndCommitInvalidate(node, executor, Ballot.ZERO, txnId, scope.homeKey(), scope, executeAt, tracing, finishAndTakeCallback());
        }
        else
        {
            TxnId withFlags = txnId;
            Invariants.require(txnId.isSyncPoint());
            if (txnId.epoch() == executeAt.epoch())
                withFlags = txnId.addFlag(HLC_BOUND);
            Deps deps = Deps.merge(oks.valuesAsNullableList(), oks.domainSize(), List::get, ok -> ok.deps);
            node.agent().coordinatorEvents().onPreAccepted(txnId);
            if (tracker.hasMediumPathAccepted() && txnId.hasMediumPath())
                adapter.propose(node, executor, topologies, scope, Accept.Kind.MEDIUM, Ballot.ZERO, txnId, txn, withFlags, deps, finishAndTakeCallback());
            else
                adapter.propose(node, executor, topologies, scope, Accept.Kind.SLOW, Ballot.ZERO, txnId, txn, executeAt, deps, finishAndTakeCallback());
        }
    }

    public static void sendApply(Node node, Node.Id to, PartialSyncPoint syncPoint, Tracing tracing)
    {
        // TODO (expected): consider, document and add invariants checking if this topologies is correct in all cases
        //  (notably ExclusiveSyncPoints should execute in earlier epochs for durability, but not for fetching)
        Topologies topologies;
        try
        {
            topologies = exclusiveSyncPoint().forExecution(node, syncPoint.route, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor);
        }
        catch (TopologyException e)
        {
            node.agent().onException(e);
            return;
        }
        sendApply(node, to, syncPoint, topologies, Ballot.ZERO, tracing);
    }

    public static void sendApply(Node node, Node.Id to, PartialSyncPoint syncPoint, long minEpoch, long maxEpoch, @Nullable Tracing tracing)
    {
        // TODO (expected): consider, document and add invariants checking if this topologies is correct in all cases
        //  (notably ExclusiveSyncPoints should execute in earlier epochs for durability, but not for fetching)
        Topologies topologies;
        try
        {
            topologies = node.topology().active().preciseEpochs(syncPoint.route, minEpoch, maxEpoch, ALL);
        }
        catch (Throwable t)
        {
            node.agent().onException(t);
            return;
        }
        sendApply(node, to, syncPoint, topologies, Ballot.ZERO, tracing);
    }

    private static void sendApply(Node node, Node.Id to, PartialSyncPoint syncPoint, Topologies participates, Ballot ballot, @Nullable Tracing tracing)
    {
        TxnId txnId = syncPoint.syncId;
        Timestamp executeAt = syncPoint.executeAt;
        Txn txn = node.agent().emptySystemTxn(txnId.kind(), txnId.domain());
        Deps deps = syncPoint.waitFor;
        Route<?> route = syncPoint.route;
        PersistableResult result = txn.result(txnId, executeAt, null).toPersistable();
        Apply apply = Apply.FACTORY.create(Maximal, to, participates, txnId, ballot, route, txn, executeAt, deps, null, result, syncPoint.fullRoute, ExecuteFlags.none());
        node.send(to, apply, tracing);
    }
}
