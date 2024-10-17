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

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.primitives.FullRoute;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections.ChaseFixedPoint.DoNotChase;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;

/**
 * Abstract parent class for implementing preaccept-like operations where we may need to fetch additional replies
 * from future epochs.
 */
abstract class AbstractCoordinatePreAccept<T, R> extends SettableResult<T> implements Callback<R>, BiConsumer<T, Throwable>
{
    class ExtraEpochs implements Callback<R>
    {
        final QuorumTracker tracker;
        private boolean extraRoundIsDone;

        ExtraEpochs(long fromEpoch, long toEpoch)
        {
            Topologies topologies = node.topology().preciseEpochs(route, fromEpoch, toEpoch);
            this.tracker = new QuorumTracker(topologies);
        }

        void start()
        {
            // TODO (desired, efficiency): consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
            // note that we must send to all replicas of old topology, as electorate may not be reachable
            contact(tracker.topologies().nodes(), topologies, this);
        }

        @Override
        public void onFailure(Id from, Throwable failure)
        {
            synchronized (AbstractCoordinatePreAccept.this)
            {
                if (!extraRoundIsDone && tracker.recordFailure(from) == Failed)
                    setFailure(failure);
            }
        }

        @Override
        public void onCallbackFailure(Id from, Throwable failure)
        {
            AbstractCoordinatePreAccept.this.onCallbackFailure(from, failure);
        }

        @Override
        public void onSuccess(Id from, R reply)
        {
            synchronized (AbstractCoordinatePreAccept.this)
            {
                if (!extraRoundIsDone)
                {
                    if (!onExtraSuccessInternal(from, reply))
                        setFailure(new Preempted(txnId, route.homeKey()));
                    else if (tracker.recordSuccess(from) == Success)
                        onPreAcceptedOrNewEpoch();
                }
            }
        }
    }

    final Node node;
    @Nullable
    final TxnId txnId;
    final FullRoute<?> route;

    private Topologies topologies;
    private boolean initialRoundIsDone;
    private ExtraEpochs extraEpochs;
    private final Map<Id, Object> debug = Invariants.debug() ? new TreeMap<>() : null;

    AbstractCoordinatePreAccept(Node node, FullRoute<?> route, TxnId txnId)
    {
        this(node, route, txnId, node.topology().select(route, txnId, txnId, QuorumEpochIntersections.preaccept.include));
    }

    AbstractCoordinatePreAccept(Node node, FullRoute<?> route, @Nullable TxnId txnId, Topologies topologies)
    {
        this.node = node;
        this.txnId = txnId;
        this.route = route;
        this.topologies = topologies;
    }

    final void start()
    {
        contact(topologies.nodes(), topologies, this);
    }

    abstract void contact(Collection<Id> nodes, Topologies topologies, Callback<R> callback);
    abstract void onSuccessInternal(Id from, R reply);
    void onSlowResponseInternal(Id from) {}
    /**
     * The tracker for the extra rounds only is provided by the AbstractCoordinatePreAccept, so we expect a boolean back
     * indicating if the "success" reply was actually a good response or a failure (i.e. preempted)
     */
    abstract boolean onExtraSuccessInternal(Id from, R reply);
    abstract void onFailureInternal(Id from, Throwable failure);
    abstract void onNewEpochTopologyMismatch(TopologyMismatch mismatch);
    abstract void onPreAccepted(Topologies topologies);
    abstract long executeAtEpoch();

    @Override
    public synchronized final void onFailure(Id from, Throwable failure)
    {
        if (debug != null) debug.putIfAbsent(from, failure);
        if (!initialRoundIsDone)
            onFailureInternal(from, failure);
    }

    @Override
    public final synchronized void onCallbackFailure(Id from, Throwable failure)
    {
        initialRoundIsDone = true;
        if (extraEpochs != null)
            extraEpochs.extraRoundIsDone = true;

        tryFailure(failure);
    }

    @Override
    // TODO (expected): shouldn't need synchronized
    public final synchronized void onSuccess(Id from, R reply)
    {
        if (debug != null) debug.putIfAbsent(from, reply);
        if (!initialRoundIsDone)
            onSuccessInternal(from, reply);
    }

    @Override
    public final synchronized void onSlowResponse(Id from)
    {
        if (!initialRoundIsDone)
            onSlowResponseInternal(from);
    }

    @Override
    public final boolean tryFailure(Throwable failure)
    {
        if (!super.tryFailure(failure))
            return false;
        onFailure(failure);
        return true;
    }

    private void onFailure(Throwable failure)
    {
        // we may already be complete, as we may receive a failure from a later phase; but it's fine to redundantly mark done
        initialRoundIsDone = true;
        if (extraEpochs != null)
            extraEpochs.extraRoundIsDone = true;

        if (failure instanceof CoordinationFailed)
        {
            ((CoordinationFailed) failure).set(txnId, route.homeKey());
            if (failure instanceof Timeout)
                node.agent().metricsEventsListener().onTimeout(txnId);
            else if (failure instanceof Preempted)
                node.agent().metricsEventsListener().onPreempted(txnId);
            else if (failure instanceof Invalidated)
                node.agent().metricsEventsListener().onInvalidated(txnId);
        }

    }

    final void onPreAcceptedOrNewEpoch()
    {
        Invariants.checkState(!initialRoundIsDone || (extraEpochs != null && !extraEpochs.extraRoundIsDone));
        initialRoundIsDone = true;
        if (QuorumEpochIntersections.preaccept.chase == DoNotChase)
        {
            long latestEpoch = executeAtEpoch();
            if (latestEpoch > topologies.currentEpoch()) node.withEpoch(latestEpoch, node.agent(), () -> onPreAcceptedInNewEpoch(topologies, latestEpoch));
            else onPreAccepted(topologies);
            return;
        }

        if (extraEpochs != null)
            extraEpochs.extraRoundIsDone = true;

        // if the epoch we are accepting in is later, we *must* contact the later epoch for pre-accept, as this epoch
        // could have moved ahead, and the timestamp we may propose may be stale.
        // Note that these future epochs are non-voting, they only serve to inform the timestamp we decide
        long latestEpoch = executeAtEpoch();
        if (latestEpoch <= topologies.currentEpoch())
            onPreAccepted(topologies);
        else
            onNewEpoch(topologies, latestEpoch);
    }

    final void onPreAcceptedInNewEpoch(Topologies topologies, long latestEpoch)
    {
        TopologyMismatch mismatch = TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(latestEpoch), txnId, route.homeKey(), route);
        if (mismatch == null) onPreAccepted(topologies);
        else onNewEpochTopologyMismatch(mismatch);
    }

    final void onNewEpoch(Topologies prevTopologies, long latestEpoch)
    {
        // TODO (desired, efficiency): check if we have already have a valid quorum for the future epoch
        //  (noting that nodes may have adopted new ranges, in which case they should be discounted, and quorums may have changed shape)
        node.withEpoch(latestEpoch, (ignored, withEpochFailure) -> {
            // TODO (required): this isn't synchronized, and isn't submitted back to the calling executor.
            if (withEpochFailure != null)
            {
                tryFailure(CoordinationFailed.wrap(withEpochFailure));
                return;
            }
            TopologyMismatch mismatch = txnId.kind() == Txn.Kind.ExclusiveSyncPoint
                                        ? TopologyMismatch.checkForMismatch(node.topology().globalForEpoch(latestEpoch), txnId, route.homeKey(), route)
                                        : TopologyMismatch.checkForMismatchOrPendingRemoval(node.topology().globalForEpoch(latestEpoch), txnId, route.homeKey(), route);
            if (mismatch != null)
            {
                initialRoundIsDone = true;
                onNewEpochTopologyMismatch(mismatch);
                return;
            }
            topologies = node.topology().withUnsyncedEpochs(route, earliestEpoch(), latestEpoch);
            boolean equivalent = topologies.oldestEpoch() <= prevTopologies.currentEpoch();
            for (long epoch = topologies.currentEpoch() ; equivalent && epoch > prevTopologies.currentEpoch() ; --epoch)
                equivalent = topologies.forEpoch(epoch).shards().equals(prevTopologies.current().shards());

            if (equivalent)
            {
                onPreAccepted(topologies);
            }
            else
            {
                extraEpochs = new ExtraEpochs(prevTopologies.currentEpoch() + 1, latestEpoch);
                extraEpochs.start();
            }
        });
    }

    protected long earliestEpoch()
    {
        return txnId == null ? executeAtEpoch() : txnId.epoch();
    }

    @Override
    public final void accept(T success, Throwable failure)
    {
        if (success != null) trySuccess(success);
        else tryFailure(failure);
    }
}
