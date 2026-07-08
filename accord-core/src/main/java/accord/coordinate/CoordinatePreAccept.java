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
import java.util.function.BiFunction;

import javax.annotation.Nullable;

import accord.coordinate.tracking.AbstractTracker;
import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.PreAcceptTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.TopologyMismatch;
import accord.utils.SortedListMap;

import static accord.coordinate.Propose.NotAccept.proposeAndCommitInvalidate;
import static accord.coordinate.tracking.RequestStatus.Success;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
abstract class CoordinatePreAccept<T> extends AbstractCoordinatePreAccept<T, PreAcceptReply, PreAcceptOk>
{
    final PreAcceptTracker<?> tracker;
    final Txn txn;
    boolean fastPathEnabled = true;

    CoordinatePreAccept(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, FullRoute<?> route, TxnId txnId, Txn txn, BiConsumer<? super T, Throwable> callback)
    {
        this(node, executor, txnId, txn, route, topologies, FastPathTracker::new, callback);
    }

    CoordinatePreAccept(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, Txn txn, FullRoute<?> route, Topologies topologies, BiFunction<Topologies, TxnId, PreAcceptTracker<?>> trackerFactory, BiConsumer<? super T, Throwable> callback)
    {
        super(node, executor, topologies, route, txnId, callback);
        this.tracker = trackerFactory.apply(topologies, txnId);
        this.txn = txn;
    }

    void contact(@Nullable Deps deps, boolean hasCoordinatorVote)
    {
        contact(to -> new PreAccept(to, topologies, txnId, txn, deps, hasCoordinatorVote, scope));
    }

    void contactNotSelf(@Nullable Deps deps, boolean hasCoordinatorVote)
    {
        contact(to -> new PreAccept(to, topologies, txnId, txn, deps, hasCoordinatorVote, scope), id -> !id.equals(node.id()));
    }

    @Override
    long executeAtEpoch()
    {
        return foldlOks((ok, prev) -> ok.witnessedAt.epoch() > prev.epoch() ? ok.witnessedAt : prev, Timestamp.NONE).epoch();
    }

    @Override
    public void onFailureInternal(Id from, int fromIndex, Throwable failure)
    {
        recordFailure(failure);
        switch (tracker.recordFailure(from))
        {
            default: throw new AssertionError();
            case NoChange:
                break;
            case Failed:
                // TODO (expected): can we improve reentrancy logic to avoid these surprising cases without losing benefits?
                // finishOnFailure first as can trigger reentrancy via proposeAndCommitInvalidate triggering timeouts
                finishOnFailure();
                proposeAndCommitInvalidate(node, executor, Ballot.ZERO, txnId, scope.homeKey(), scope, txnId, tracing, null);
                break;
            case Success:
                onPreAcceptedOrNewEpoch();
        }
    }

    @Override
    public void onSuccessInternal(Id from, int fromIndex, PreAcceptReply reply)
    {
        if (!reply.isOk())
        {
            // we've been preempted by a recovery coordinator; defer to it, and wait to hear any result
            finishOnFailure(Preempted.preempted(node.agent(), txnId, scope.homeKey()));
        }
        else
        {
            PreAcceptOk ok = (PreAcceptOk) reply;
            recordOk(fromIndex, ok);

            boolean fastPath = ok.witnessedAt.compareTo(txnId) == 0 && fastPathEnabled;
            if (tracker.recordSuccess(from, fastPath) == Success)
                onPreAcceptedOrNewEpoch();
        }
    }

    @Override
    void onSlowResponseInternal(Id from)
    {
        if (tracker.recordDelayed(from) == Success)
            onPreAcceptedOrNewEpoch();
    }

    @Override
    void onNewEpochTopologyMismatch(TopologyMismatch mismatch)
    {
        /**
         * We cannot execute the transaction because the execution epoch's topology no longer contains all of the
         * participating keys/ranges, so we propose that the transaction is invalidated in its coordination epoch
         */
        finishWithFailure(mismatch);
    }

    @Override
    protected void finishWithFailure(Throwable failure)
    {
        if (failure instanceof Preempted)
        {
            // cannot expect to successfully propose invalidation, and no point as already being recovered
            super.finishWithFailure(failure);
        }
        else
        {
            BiConsumer<?, Throwable> callback = finishAndTakeCallback();
            proposeAndCommitInvalidate(node, executor, Ballot.ZERO, txnId, scope.homeKey(), scope, txnId, tracing, (success, invalidated) -> {
                failure.addSuppressed(invalidated);
                callback.accept(null, failure);
                node.agent().coordinatorEvents().onFailed(failure, txnId, scope, this);
            });
        }
    }

    @Override
    void onPreAccepted()
    {
        SortedListMap<Node.Id, PreAcceptOk> oks = finishOks();
        onPreAccepted(topologies, oks);
    }

    abstract void onPreAccepted(Topologies topologies, SortedListMap<Id, PreAcceptOk> oks);

    @Override
    public AbstractTracker<?> tracker()
    {
        return tracker;
    }
}
