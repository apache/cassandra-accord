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

import accord.api.Result;
import accord.coordinate.CoordinationAdapter.Adapters;
import accord.coordinate.CoordinationAdapter.Adapters.SyncPointAdapter;
import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.messages.PreAccept.PreAcceptOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.topology.Topologies;
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
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.TxnId.Cardinality.cardinality;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;

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

    private CoordinateSyncPoint(Node node, SequentialAsyncExecutor executor, TxnId txnId, Topologies topologies, Txn txn, FullRoute<?> route, SyncPointAdapter<R> adapter, BiConsumer<? super R, Throwable> callback)
    {
        super(node, executor, txnId, txn, route, topologies, adapter.preacceptTrackerFactory, callback);
        this.adapter = adapter;
    }

    public static <U extends Unseekable> AsyncChain<SyncPoint<U>> exclusive(Node node, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, ExclusiveSyncPoint, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncChain<SyncPoint<U>> exclusive(Node node, TxnId txnId, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, txnId, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncChain<SyncPoint<U>> exclusive(Node node, TxnId txnId, FullRoute<U> route)
    {
        return coordinate(node, txnId, route, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncChain<SyncPoint<U>> executeAtQuorum(Node node, Unseekables<U> keysOrRanges)
    {
        return coordinate(node, ExclusiveSyncPoint, keysOrRanges, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncChain<SyncPoint<U>> executeAtQuorum(Node node, TxnId txnId, FullRoute<U> route)
    {
        return coordinate(node, txnId, route, Adapters.exclusiveSyncPoint());
    }

    public static <U extends Unseekable> AsyncChain<SyncPoint<U>> coordinate(Node node, Txn.Kind kind, Unseekables<U> keysOrRanges, SyncPointAdapter<SyncPoint<U>> adapter)
    {
        Invariants.requireArgument(kind.isSyncPoint());
        TxnId txnId = node.nextTxnIdWithDefaultFlags(kind, keysOrRanges.domain(), cardinality(keysOrRanges));
        return node.withEpochExact(txnId.epoch(), null, () -> coordinate(node, txnId, keysOrRanges, adapter));
    }

    public static <U extends Unseekable> AsyncChain<SyncPoint<U>> coordinate(Node node, Txn.Kind kind, FullRoute<U> route, SyncPointAdapter<SyncPoint<U>> adapter)
    {
        Invariants.requireArgument(kind.isSyncPoint());
        TxnId txnId = node.nextTxnIdWithDefaultFlags(kind, route.domain(), cardinality(route));
        return node.withEpochExact(txnId.epoch(), null, () -> coordinate(node, txnId, route, adapter));
    }

    private static <U extends Unseekable> AsyncChain<SyncPoint<U>> coordinate(Node node, TxnId txnId, Unseekables<U> keysOrRanges, SyncPointAdapter<SyncPoint<U>> adapter)
    {
        Invariants.requireArgument(txnId.isSyncPoint());
        FullRoute<U> route = (FullRoute<U>) node.computeRoute(txnId, keysOrRanges);
        return coordinate(node, txnId, route, adapter);
    }

    private static <U extends Unseekable> AsyncChain<SyncPoint<U>> coordinate(Node node, TxnId txnId, FullRoute<U> route, SyncPointAdapter<SyncPoint<U>> adapter)
    {
        try
        {
            Invariants.requireArgument(txnId.isSyncPoint());
            TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(txnId.epoch()), txnId, route.homeKey(), route);
            if (mismatch != null)
                throw mismatch;

            return new AsyncChains.Head<>()
            {
                @Override
                protected @Nullable Cancellable start(BiConsumer<? super SyncPoint<U>, Throwable> callback)
                {
                    new CoordinateSyncPoint<>(node, node.someSequentialExecutor(), txnId, adapter.forDecision(node, route, SHARE, txnId, txnId), node.agent().emptySystemTxn(txnId.kind(), txnId.domain()), route, adapter, callback)
                    .start();
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
    void onPreAccepted(Topologies topologies, Timestamp executeAt, SortedListMap<Node.Id, PreAcceptOk> oks)
    {
        if (executeAt.is(REJECTED))
        {
            node.agent().coordinatorEvents().onRejected(txnId);
            proposeAndCommitInvalidate(node, executor, Ballot.ZERO, txnId, scope.homeKey(), scope, executeAt, finishAndTakeCallback());
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

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint)
    {
        // TODO (expected): consider, document and add invariants checking if this topologies is correct in all cases
        //  (notably ExclusiveSyncPoints should execute in earlier epochs for durability, but not for fetching)
        Topologies topologies = exclusiveSyncPoint().forExecution(node, syncPoint.route(), SHARE, syncPoint.syncId, syncPoint.syncId, syncPoint.waitFor);
        sendApply(node, to, syncPoint, topologies, Ballot.ZERO);
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint, long minEpoch, long maxEpoch)
    {
        // TODO (expected): consider, document and add invariants checking if this topologies is correct in all cases
        //  (notably ExclusiveSyncPoints should execute in earlier epochs for durability, but not for fetching)
        Topologies topologies = node.topology().preciseEpochs(syncPoint.route(), minEpoch, maxEpoch, SHARE);
        sendApply(node, to, syncPoint, topologies, Ballot.ZERO);
    }

    private static void sendApply(Node node, Node.Id to, SyncPoint<?> syncPoint, Topologies participates, Ballot ballot)
    {
        TxnId txnId = syncPoint.syncId;
        Timestamp executeAt = syncPoint.executeAt;
        Txn txn = node.agent().emptySystemTxn(txnId.kind(), txnId.domain());
        Deps deps = syncPoint.waitFor;
        FullRoute<?> route = syncPoint.route;
        Result result = txn.result(txnId, executeAt, null);
        Apply apply = Apply.FACTORY.create(Maximal, to, participates, txnId, ballot, route, txn, executeAt, deps, null, result, route, ExecuteFlags.none());
        node.send(to, apply);
    }
}
