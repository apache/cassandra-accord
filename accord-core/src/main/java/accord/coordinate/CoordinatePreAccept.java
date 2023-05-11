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
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import accord.coordinate.tracking.FastPathTracker;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.FullRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.async.AsyncResults.SettableResult;

import static accord.coordinate.tracking.RequestStatus.Failed;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.Timestamp.mergeMax;
import static accord.utils.Functions.foldl;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 *
 * TODO (desired, testing): dedicated burn test to validate outcomes
 */
abstract class CoordinatePreAccept<T> extends SettableResult<T> implements Callback<PreAcceptReply>, BiConsumer<T, Throwable>
{
    class ExtraPreAccept implements Callback<PreAcceptReply>
    {
        final QuorumTracker tracker;
        private boolean extraPreAcceptIsDone;

        ExtraPreAccept(long fromEpoch, long toEpoch)
        {
            Topologies topologies = node.topology().preciseEpochs(route, fromEpoch, toEpoch);
            this.tracker = new QuorumTracker(topologies);
        }

        void start()
        {
            // TODO (desired, efficiency): consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
            // note that we must send to all replicas of old topology, as electorate may not be reachable
            contact(tracker.topologies().nodes(), this);
        }

        @Override
        public void onFailure(Id from, Throwable failure)
        {
            synchronized (CoordinatePreAccept.this)
            {
                if (!extraPreAcceptIsDone && tracker.recordFailure(from) == Failed)
                    setFailure(failure);
            }
        }

        @Override
        public void onCallbackFailure(Id from, Throwable failure)
        {
            CoordinatePreAccept.this.onCallbackFailure(from, failure);
        }

        @Override
        public void onSuccess(Id from, PreAcceptReply reply)
        {
            synchronized (CoordinatePreAccept.this)
            {
                if (extraPreAcceptIsDone)
                    return;

                if (!reply.isOk())
                {
                    // we've been preempted by a recovery coordinator; defer to it, and wait to hear any result
                    setFailure(new Preempted(txnId, route.homeKey()));
                }
                else
                {
                    PreAcceptOk ok = (PreAcceptOk) reply;
                    successes.add(ok);
                    if (tracker.recordSuccess(from) == Success)
                        onPreAcceptedOrNewEpoch();
                }
            }
        }
    }

    final Node node;
    final TxnId txnId;
    final Txn txn;
    final FullRoute<?> route;

    final FastPathTracker tracker;
    private Topologies topologies;
    private final List<PreAcceptOk> successes;
    private boolean initialPreAcceptIsDone;
    private ExtraPreAccept extraPreAccept;

    CoordinatePreAccept(Node node, TxnId txnId, Txn txn, FullRoute<?> route)
    {
        this(node, txnId, txn, route, node.topology().withUnsyncedEpochs(route, txnId, txnId));
    }

    CoordinatePreAccept(Node node, TxnId txnId, Txn txn, FullRoute<?> route, Topologies topologies)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.topologies = topologies;
        this.tracker = new FastPathTracker(topologies);
        this.successes = new ArrayList<>(topologies.estimateUniqueNodes());
    }

    void start()
    {
        contact(topologies.nodes(), this);
    }

    private void contact(Set<Id> nodes, Callback<PreAcceptReply> callback)
    {
        // TODO (desired, efficiency): consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
        // note that we must send to all replicas of old topology, as electorate may not be reachable
        node.send(nodes, to -> new PreAccept(to, topologies, txnId, txn, route),
                  node.commandStores().select(route.homeKey()), callback);
    }

    @Override
    public synchronized void onFailure(Id from, Throwable failure)
    {
        if (initialPreAcceptIsDone)
            return;

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
    public synchronized void onCallbackFailure(Id from, Throwable failure)
    {
        initialPreAcceptIsDone = true;
        if (extraPreAccept != null)
            extraPreAccept.extraPreAcceptIsDone = true;

        tryFailure(failure);
    }

    @Override
    public synchronized void onSuccess(Id from, PreAcceptReply reply)
    {
        if (initialPreAcceptIsDone)
            return;

        if (!reply.isOk())
        {
            // we've been preempted by a recovery coordinator; defer to it, and wait to hear any result
            setFailure(new Preempted(txnId, route.homeKey()));
        }
        else
        {
            PreAcceptOk ok = (PreAcceptOk) reply;
            successes.add(ok);

            boolean fastPath = ok.witnessedAt.compareTo(txnId) == 0;
            if (tracker.recordSuccess(from, fastPath) == Success)
                onPreAcceptedOrNewEpoch();
        }
    }

    @Override
    public void setFailure(Throwable failure)
    {
        Invariants.checkState(!initialPreAcceptIsDone || (extraPreAccept != null && !extraPreAccept.extraPreAcceptIsDone));
        initialPreAcceptIsDone = true;
        if (extraPreAccept != null)
            extraPreAccept.extraPreAcceptIsDone = true;
        if (failure instanceof CoordinationFailed)
            ((CoordinationFailed) failure).set(txnId, route.homeKey());
        super.setFailure(failure);
    }

    void onPreAcceptedOrNewEpoch()
    {
        Invariants.checkState(!initialPreAcceptIsDone || (extraPreAccept != null && !extraPreAccept.extraPreAcceptIsDone));
        initialPreAcceptIsDone = true;
        if (extraPreAccept != null)
            extraPreAccept.extraPreAcceptIsDone = true;

        // if the epoch we are accepting in is later, we *must* contact the later epoch for pre-accept, as this epoch
        // could have moved ahead, and the timestamp we may propose may be stale.
        // Note that these future epochs are non-voting, they only serve to inform the timestamp we decide
        Timestamp executeAt = foldl(successes, (ok, prev) -> mergeMax(ok.witnessedAt, prev), Timestamp.NONE);
        if (executeAt.epoch() <= topologies.currentEpoch())
            onPreAccepted(topologies, executeAt, successes);
        else
            onNewEpoch(topologies, executeAt, successes);
    }

    void onNewEpoch(Topologies prevTopologies, Timestamp executeAt, List<PreAcceptOk> successes)
    {
        // TODO (desired, efficiency): check if we have already have a valid quorum for the future epoch
        //  (noting that nodes may have adopted new ranges, in which case they should be discounted, and quorums may have changed shape)
        node.withEpoch(executeAt.epoch(), () -> {
            synchronized (CoordinatePreAccept.this)
            {
                topologies = node.topology().withUnsyncedEpochs(route, txnId.epoch(), executeAt.epoch());
                boolean equivalent = topologies.oldestEpoch() <= prevTopologies.currentEpoch();
                for (long epoch = topologies.currentEpoch() ; equivalent && epoch > prevTopologies.currentEpoch() ; --epoch)
                    equivalent = topologies.forEpoch(epoch).shards().equals(prevTopologies.current().shards());

                if (equivalent)
                {
                    onPreAccepted(topologies, executeAt, successes);
                }
                else
                {
                    extraPreAccept = new ExtraPreAccept(prevTopologies.currentEpoch() + 1, executeAt.epoch());
                    extraPreAccept.start();
                }
            }
        });
    }

    abstract void onPreAccepted(Topologies topologies, Timestamp executeAt, List<PreAcceptOk> successes);

    @Override
    public void accept(T success, Throwable failure)
    {
        if (failure instanceof CoordinationFailed)
            ((CoordinationFailed) failure).set(txnId, route.homeKey());

        if (success != null) trySuccess(success);
        else tryFailure(failure);
    }
}
