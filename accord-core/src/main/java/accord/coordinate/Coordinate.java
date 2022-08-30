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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.FastPathTracker;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.primitives.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.primitives.Deps;
import accord.local.Node.Id;
import accord.primitives.Timestamp;
import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.txn.Txn;
import accord.primitives.TxnId;
import accord.messages.PreAccept.PreAcceptReply;
import com.google.common.collect.Sets;

import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;

/**
 * Perform initial rounds of PreAccept and Accept until we have reached agreement about when we should execute.
 * If we are preempted by a recovery coordinator, we abort and let them complete (and notify us about the execution result)
 */
public class Coordinate extends AsyncFuture<Result> implements Callback<PreAcceptReply>, BiConsumer<Result, Throwable>
{
    static class ShardTracker extends FastPathTracker.FastPathShardTracker
    {
        public ShardTracker(Shard shard)
        {
            super(shard);
        }

        @Override
        public boolean includeInFastPath(Node.Id node, boolean withFastPathTimestamp)
        {
            return withFastPathTimestamp && shard.fastPathElectorate.contains(node);
        }

        @Override
        public boolean hasMetFastPathCriteria()
        {
            return fastPathAccepts >= shard.fastPathQuorumSize;
        }
    }

    static class PreacceptTracker extends FastPathTracker<ShardTracker>
    {
        volatile long supersedingEpoch = -1;
        private final boolean fastPathPermitted;
        private final Set<Id> successes = new HashSet<>();
        private Set<Id> failures;

        public PreacceptTracker(Topologies topologies, boolean fastPathPermitted)
        {
            super(topologies, Coordinate.ShardTracker[]::new, Coordinate.ShardTracker::new);
            this.fastPathPermitted = fastPathPermitted;
        }

        public PreacceptTracker(Topologies topologies)
        {
            this(topologies, topologies.fastPathPermitted());
        }

        @Override
        public boolean failure(Id node)
        {
            if (failures == null)
                failures = new HashSet<>();
            failures.add(node);
            return super.failure(node);
        }

        @Override
        public void recordSuccess(Id node, boolean withFastPathTimestamp)
        {
            successes.add(node);
            super.recordSuccess(node, withFastPathTimestamp);
        }

        public void recordSuccess(Id node)
        {
            recordSuccess(node, false);
        }

        public synchronized boolean recordSupersedingEpoch(long epoch)
        {
            if (epoch <= supersedingEpoch)
                return false;
            supersedingEpoch = epoch;
            return true;
        }

        public boolean hasSupersedingEpoch()
        {
            return supersedingEpoch > 0;
        }

        public PreacceptTracker withUpdatedTopologies(Topologies topologies)
        {
            PreacceptTracker tracker = new PreacceptTracker(topologies, false);
            successes.forEach(tracker::recordSuccess);
            if (failures != null)
                failures.forEach(tracker::failure);
            return tracker;
        }

        @Override
        public boolean hasMetFastPathCriteria()
        {
            return fastPathPermitted && super.hasMetFastPathCriteria();
        }

        boolean shouldSlowPathAccept()
        {
            return (!fastPathPermitted || !hasInFlight()) && hasReachedQuorum();
        }
    }

    final Node node;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;

    private PreacceptTracker tracker;
    private final List<PreAcceptOk> preAcceptOks = new ArrayList<>();
    private boolean preAcceptIsDone;

    // TODO: hybrid fast path? or at least short-circuit accept if we gain a fast-path quorum _and_ proposed one by accept
    boolean permitHybridFastPath;

    private Coordinate(Node node, TxnId txnId, Txn txn, Key homeKey)
    {
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        Topologies topologies = node.topology().withUnsyncedEpochs(txn.keys(), txnId.epoch, txnId.epoch);
        this.tracker = new PreacceptTracker(topologies);
    }

    private void start()
    {
        // TODO: consider sending only to electorate of most recent topology (as only these PreAccept votes matter)
        // note that we must send to all replicas of old topology, as electorate may not be reachable
        node.send(tracker.nodes(), to -> new PreAccept(to, tracker.topologies(), txnId, txn, homeKey), this);
    }

    public static Future<Result> coordinate(Node node, TxnId txnId, Txn txn, Key homeKey)
    {
        Coordinate coordinate = new Coordinate(node, txnId, txn, homeKey);
        coordinate.start();
        return coordinate;
    }

    @Override
    public synchronized void onFailure(Id from, Throwable failure)
    {
        if (preAcceptIsDone)
            return;

        if (tracker.failure(from))
        {
            preAcceptIsDone = true;
            tryFailure(new Timeout(txnId, homeKey));
        }

        // if no other responses are expected and the slow quorum has been satisfied, proceed
        if (tracker.shouldSlowPathAccept())
            onPreAccepted();
    }

    @Override
    public void onCallbackFailure(Id from, Throwable failure)
    {
        tryFailure(failure);
    }

    // TODO (soon): do we need to preaccept in later epochs? the sync logic should take care of it for us, since
    //              either we haven't synced between a majority and the earlier epochs are still involved for
    //              later preaccepts, or they have been sync'd and the earlier transactions are known to the later epochs
    private synchronized void onEpochUpdate()
    {
        if (!tracker.hasSupersedingEpoch())
            return;
        Topologies newTopologies = node.topology().withUnsyncedEpochs(txn.keys(), txnId.epoch, tracker.supersedingEpoch);
        if (newTopologies.currentEpoch() < tracker.supersedingEpoch)
            return;
        Set<Id> previousNodes = tracker.nodes();
        tracker = tracker.withUpdatedTopologies(newTopologies);

        // send messages to new nodes
        Set<Id> needMessages = Sets.difference(tracker.nodes(), previousNodes);
        if (!needMessages.isEmpty())
            node.send(needMessages, to -> new PreAccept(to, newTopologies, txnId, txn, homeKey), this);

        if (tracker.shouldSlowPathAccept())
            onPreAccepted();
    }

    public synchronized void onSuccess(Id from, PreAcceptReply receive)
    {
        if (preAcceptIsDone)
            return;

        if (!receive.isOK())
        {
            // we've been preempted by a recovery coordinator; defer to it, and wait to hear any result
            tryFailure(new Preempted(txnId, homeKey));
            return;
        }

        PreAcceptOk ok = (PreAcceptOk) receive;
        preAcceptOks.add(ok);

        boolean fastPath = ok.witnessedAt.compareTo(txnId) == 0;
        tracker.recordSuccess(from, fastPath);

        // TODO: we should only update epoch if we need to in order to reach quorum
        if (!fastPath && ok.witnessedAt.epoch > txnId.epoch && tracker.recordSupersedingEpoch(ok.witnessedAt.epoch))
        {
            node.configService().fetchTopologyForEpoch(ok.witnessedAt.epoch);
            node.topology().awaitEpoch(ok.witnessedAt.epoch).addListener(this::onEpochUpdate);
        }

        if (!tracker.hasSupersedingEpoch() && (tracker.hasMetFastPathCriteria() || tracker.shouldSlowPathAccept()))
            onPreAccepted(); // note, can already have invoked onPreAccepted in onEpochUpdate
    }

    private void onPreAccepted()
    {
        if (preAcceptIsDone)
            return;

        preAcceptIsDone = true;
        if (tracker.hasMetFastPathCriteria())
        {
            preAcceptIsDone = true;
            Deps deps = Deps.merge(preAcceptOks, ok -> ok.witnessedAt.equals(txnId) ? ok.deps : null);
            Execute.execute(node, txnId, txn, homeKey, txnId, deps, this);
        }
        else
        {
            Deps deps = Deps.merge(preAcceptOks, ok -> ok.deps);
            Timestamp executeAt; {
                Timestamp accumulate = Timestamp.NONE;
                for (PreAcceptOk preAcceptOk : preAcceptOks)
                    accumulate = Timestamp.max(accumulate, preAcceptOk.witnessedAt);
                executeAt = accumulate;
            }

            // TODO: perhaps don't submit Accept immediately if we almost have enough for fast-path,
            //       but by sending accept we rule out hybrid fast-path
            permitHybridFastPath = executeAt.compareTo(txnId) == 0;
            node.withEpoch(executeAt.epoch, () -> Propose.propose(node, tracker.topologies(), Ballot.ZERO, txnId, txn, homeKey, executeAt, deps, this));
        }
    }

    @Override
    public void accept(Result success, Throwable failure)
    {
        if (success != null) trySuccess(success);
        else tryFailure(failure);
    }
}
