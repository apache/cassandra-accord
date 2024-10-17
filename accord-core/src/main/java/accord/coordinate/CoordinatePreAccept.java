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
import java.util.function.Function;

import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.PreAcceptTracker;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.Ballot;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.Timestamp.mergeMax;
import static accord.utils.Functions.foldl;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
abstract class CoordinatePreAccept<T> extends AbstractCoordinatePreAccept<T, PreAcceptReply>
{
    final PreAcceptTracker<?> tracker;
    // TODO (expected): this can be cleared after preaccept
    // TODO (expected): back by SortedListMap; must handle additional preaccepts (but this is no longer ordinarily enabled)
    private final List<PreAcceptOk> oks;
    final Txn txn;

    CoordinatePreAccept(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        this(node, txnId, txn, route, node.topology().select(route, txnId, txnId, QuorumEpochIntersections.preaccept.include));
    }

    CoordinatePreAccept(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Topologies topologies)
    {
        this(node, txnId, txn, route, topologies, FastPathTracker::new);
    }

    CoordinatePreAccept(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Topologies topologies, Function<Topologies, PreAcceptTracker<?>> trackerFactory)
    {
        super(node, route, txnId, topologies);
        this.tracker = trackerFactory.apply(topologies);
        this.oks = new ArrayList<>(topologies.nodes().size());
        this.txn = txn;
    }

    void contact(Collection<Id> nodes, Topologies topologies, Callback<PreAcceptReply> callback)
    {
        // TODO (desired, efficiency): consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
        // note that we must send to all replicas of old topology, as electorate may not be reachable
        CommandStore commandStore = CommandStore.maybeCurrent();
        if (commandStore == null) commandStore = node.commandStores().select(route.homeKey());
        node.send(nodes, to -> new PreAccept(to, topologies, txnId, txn, route), commandStore, callback);
    }

    @Override
    long executeAtEpoch()
    {
        return foldl(oks, (ok, prev) -> ok.witnessedAt.epoch() > prev.epoch() ? ok.witnessedAt : prev, Timestamp.NONE).epoch();
    }

    @Override
    public void onFailureInternal(Id from, Throwable failure)
    {
        switch (tracker.recordFailure(from))
        {
            default: throw new AssertionError();
            case NoChange:
                break;
            case Failed:
                setFailure(new Timeout(txnId, route.homeKey()));
                break;
            case Success:
                onPreAcceptedOrNewEpoch();
        }
    }

    @Override
    public void onSuccessInternal(Id from, PreAcceptReply reply)
    {
        if (!reply.isOk())
        {
            // we've been preempted by a recovery coordinator; defer to it, and wait to hear any result
            setFailure(new Preempted(txnId, route.homeKey()));
        }
        else
        {
            PreAcceptOk ok = (PreAcceptOk) reply;
            oks.add(ok);

            boolean fastPath = ok.witnessedAt.compareTo(txnId) == 0;
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
    boolean onExtraSuccessInternal(Id from, PreAcceptReply reply)
    {
        if (!reply.isOk())
            return false;

        PreAcceptOk ok = (PreAcceptOk) reply;
        oks.add(ok);
        return true;
    }

    @Override
    void onNewEpochTopologyMismatch(TopologyMismatch mismatch)
    {
        /**
         * We cannot execute the transaction because the execution epoch's topology no longer contains all of the
         * participating keys/ranges, so we propose that the transaction is invalidated in its coordination epoch
         */
        Propose.Invalidate.proposeInvalidate(node, new Ballot(node.uniqueNow()), txnId, route.homeKey(), (outcome, failure) -> {
            if (failure != null)
                mismatch.addSuppressed(failure);
            accept(null, mismatch);
        });
    }

    @Override
    void onPreAccepted(Topologies topologies)
    {
        // TODO (expected): we do not have to take max here if we have enough fast path votes (this may unnecessarily force us onto the slow path)
        Timestamp executeAt = foldl(oks, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        node.withEpoch(executeAt.epoch(), (ignored, withEpochFailure) -> {
            if (withEpochFailure != null)
                tryFailure(CoordinationFailed.wrap(withEpochFailure));
            else
                onPreAccepted(topologies, executeAt, oks);
        });
    }

    abstract void onPreAccepted(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> oks);
}
