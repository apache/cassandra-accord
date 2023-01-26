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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;

import static accord.coordinate.Propose.Invalidate.proposeAndCommitInvalidate;
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
public class CoordinateSyncPoint<S extends Seekables<?, ?>> extends CoordinatePreAccept<SyncPoint<S>>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CoordinateSyncPoint.class);

    final S keysOrRanges;
    // Whether to wait on the dependencies applying globally before returning a result
    final boolean async;

    private CoordinateSyncPoint(Node node, TxnId txnId, Txn txn, FullRoute<?> route, S keysOrRanges, boolean async)
    {
        super(node, txnId, txn, route, node.topology().withOpenEpochs(route, txnId, txnId));
        checkArgument(txnId.rw() == Kind.SyncPoint || async, "Exclusive sync points only support async application");
        this.keysOrRanges = keysOrRanges;
        this.async = async;
    }

    public static <S extends Seekables<?, ?>> AsyncResult<CoordinateSyncPoint<S>> exclusive(Node node, S keysOrRanges)
    {
        return coordinate(node, ExclusiveSyncPoint, keysOrRanges, true);
    }

    public static <S extends Seekables<?, ?>> CoordinateSyncPoint<S> exclusive(Node node, TxnId txnId, S keysOrRanges)
    {
        return coordinate(node, txnId, keysOrRanges, true);
    }

    public static <S extends Seekables<?, ?>> AsyncResult<CoordinateSyncPoint<S>> inclusive(Node node, S keysOrRanges, boolean async)
    {
        return coordinate(node, Kind.SyncPoint, keysOrRanges, async);
    }

    private static <S extends Seekables<?, ?>> AsyncResult<CoordinateSyncPoint<S>> coordinate(Node node, Kind kind, S keysOrRanges, boolean async)
    {
        checkArgument(kind == Kind.SyncPoint || kind == ExclusiveSyncPoint);
        node.nextTxnId(Kind.SyncPoint, keysOrRanges.domain());
        TxnId txnId = node.nextTxnId(kind, keysOrRanges.domain());
        return node.withEpoch(txnId.epoch(), () ->
                AsyncChains.success(coordinate(node, txnId, keysOrRanges, async))
        ).beginAsResult();
    }

    private static <S extends Seekables<?, ?>> CoordinateSyncPoint<S> coordinate(Node node, TxnId txnId, S keysOrRanges, boolean async)
    {
        checkArgument(txnId.rw() == Kind.SyncPoint || txnId.rw() == ExclusiveSyncPoint);
        FullRoute route = node.computeRoute(txnId, keysOrRanges);
        CoordinateSyncPoint<S> coordinate = new CoordinateSyncPoint(node, txnId, node.agent().emptyTxn(txnId.rw(), keysOrRanges), route, keysOrRanges, async);
        coordinate.start();
        return coordinate;
    }

    static <S extends Seekables<?, ?>> void blockOnDeps(Node node, Txn txn, TxnId txnId, FullRoute<?> route, S keysOrRanges, Deps deps, BiConsumer<SyncPoint<S>, Throwable> callback, boolean async)
    {
        // If deps are empty there is nothing to wait on application for so we can return immediately
        boolean processAsyncCompletion = deps.isEmpty() || async;
        BlockOnDeps.blockOnDeps(node, txnId, txn, route, deps, (result, throwable) -> {
            // Don't want to process completion twice
            if (processAsyncCompletion)
            {
                // Don't lose the error
                if (throwable != null)
                    node.agent().onUncaughtException(throwable);
                return;
            }
            if (throwable != null)
                callback.accept(null, throwable);
            else
                callback.accept(new SyncPoint<S>(txnId, deps, keysOrRanges, route, false), null);
        });
        // Notify immediately and the caller can add a listener to command completion to track local application
        if (processAsyncCompletion)
            callback.accept(new SyncPoint<S>(txnId, deps, keysOrRanges, route, true), null);
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
            if (tracker.hasFastPathAccepted() && txnId.rw() == Kind.SyncPoint)
                blockOnDeps(node, txn, txnId, route, keysOrRanges, deps, this, async);
            else
                ProposeSyncPoint.proposeSyncPoint(node, topologies, Ballot.ZERO, txnId, txn, route, deps, executeAt, this, async, tracker.nodes(), keysOrRanges);
        }
    }

    public static void sendApply(Node node, Node.Id to, SyncPoint syncPoint)
    {
        TxnId txnId = syncPoint.syncId;
        Timestamp executeAt = txnId;
        Txn txn = node.agent().emptyTxn(txnId.rw(), syncPoint.keysOrRanges);
        Deps deps = syncPoint.waitFor;
        Apply.sendMaximal(node, to, txnId, syncPoint.route(), txn, executeAt, deps, txn.execute(txnId, executeAt, null), txn.result(txnId, executeAt, null));
    }
}
