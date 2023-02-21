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

package accord.topology;

import accord.api.ConfigurationService;
import accord.api.RoutingKey;
import accord.api.TopologySorter;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node.Id;
import accord.messages.Request;
import accord.primitives.*;
import accord.topology.Topologies.Single;
import com.google.common.annotations.VisibleForTesting;
import accord.utils.Invariants;
import accord.primitives.Timestamp;
import accord.utils.async.*;

import java.util.*;

import static accord.coordinate.tracking.RequestStatus.Success;

import static accord.utils.Invariants.checkArgument;

/**
 * Manages topology state changes and update bookkeeping
 *
 * Each time the topology changes we need to:
 * * confirm previous owners of ranges we replicate are aware of the new config
 * * learn of any outstanding operations for ranges we replicate
 * * clean up obsolete data
 *
 * Assumes a topology service that won't report epoch n without having n-1 etc also available
 *
 * TODO (desired, efficiency/clarity): make TopologyManager a Topologies and copy-on-write update to it,
 *  so we can always just take a reference for transactions instead of copying every time (and index into it by the txnId.epoch)
 */
public class TopologyManager implements ConfigurationService.Listener
{
    private static final AsyncResult<Void> SUCCESS = AsyncResults.success(null);
    static class EpochState
    {
        final Id self;
        private final Topology global;
        private final Topology local;
        private final QuorumTracker syncTracker;
        private boolean syncComplete = false;
        private boolean prevSynced;

        EpochState(Id node, Topology global, TopologySorter sorter, boolean prevSynced)
        {
            this.self = node;
            this.global = checkArgument(global, !global.isSubset());
            this.local = global.forNode(node).trim();
            Invariants.checkArgument(!global().isSubset());
            this.syncTracker = new QuorumTracker(new Single(sorter, global()));
            this.prevSynced = prevSynced;
        }

        void markPrevSynced()
        {
            prevSynced = true;
        }

        public void recordSyncComplete(Id node)
        {
            if (syncTracker.recordSuccess(node) == Success)
                syncComplete = true;
        }

        Topology global()
        {
            return global;
        }

        Topology local()
        {
            return local;
        }

        long epoch()
        {
            return global().epoch;
        }

        boolean syncComplete()
        {
            return prevSynced && syncComplete;
        }

        /**
         * determine if sync has completed for all shards intersecting with the given keys
         */
        boolean syncCompleteFor(Unseekables<?, ?> intersect)
        {
            if (!prevSynced)
                return false;
            if (syncComplete)
                return true;
            Boolean result = global().foldl(intersect, (shard, acc, i) -> {
                if (acc == Boolean.FALSE)
                    return acc;
                return syncTracker.get(i).hasReachedQuorum();
            }, Boolean.TRUE);
            return result == Boolean.TRUE;
        }

        boolean shardIsUnsynced(int idx)
        {
            return !prevSynced || !syncTracker.get(idx).hasReachedQuorum();
        }
    }

    private static class Epochs
    {
        private final long currentEpoch;
        private final EpochState[] epochs;
        // nodes we've received sync complete notifications from, for epochs we do not yet have topologies for.
        // Pending sync notifications are indexed by epoch, with the current epoch as index[0], and future epochs
        // as index[epoch - currentEpoch]. Sync complete notifications for the current epoch are marked pending
        // until the superseding epoch has been applied
        private final List<Set<Id>> pendingSyncComplete;

        // list of promises to be completed as newer epochs become active. This is to support processes that
        // are waiting on future epochs to begin (ie: txn requests from futures epochs). Index 0 is for
        // currentEpoch + 1
        private final List<AsyncResult.Settable<Void>> futureEpochFutures;

        private Epochs(EpochState[] epochs, List<Set<Id>> pendingSyncComplete, List<AsyncResult.Settable<Void>> futureEpochFutures)
        {
            this.currentEpoch = epochs.length > 0 ? epochs[0].epoch() : 0;
            this.pendingSyncComplete = pendingSyncComplete;
            this.futureEpochFutures = futureEpochFutures;
            for (int i=1; i<epochs.length; i++)
                checkArgument(epochs[i].epoch() == epochs[i-1].epoch() - 1);
            this.epochs = epochs;
        }

        private Epochs(EpochState[] epochs)
        {
            this(epochs, new ArrayList<>(), new ArrayList<>());
        }

        public AsyncResult<Void> awaitEpoch(long epoch)
        {
            if (epoch <= currentEpoch)
                return SUCCESS;

            int diff = (int) (epoch - currentEpoch);
            while (futureEpochFutures.size() < diff)
                futureEpochFutures.add(AsyncResults.settable());

            return futureEpochFutures.get(diff - 1);
        }

        public long nextEpoch()
        {
            return current().epoch + 1;
        }

        public Topology current()
        {
            return epochs.length > 0 ? epochs[0].global() : Topology.EMPTY;
        }

        /**
         * Mark sync complete for the given node/epoch, and if this epoch
         * is now synced, update the prevSynced flag on superseding epochs
         */
        public void syncComplete(Id node, long epoch)
        {
            checkArgument(epoch > 0);
            if (epoch > currentEpoch - 1)
            {
                int idx = (int) (epoch - currentEpoch);
                for (int i=pendingSyncComplete.size(); i<=idx; i++)
                    pendingSyncComplete.add(new HashSet<>());

                pendingSyncComplete.get(idx).add(node);
            }
            else
            {
                EpochState state = get(epoch);
                state.recordSyncComplete(node);
                for (epoch++ ; state.syncComplete() && epoch <= currentEpoch; epoch++)
                {
                    state = get(epoch);
                    state.markPrevSynced();
                }
            }
        }

        private EpochState get(long epoch)
        {
            if (epoch > currentEpoch || epoch <= currentEpoch - epochs.length)
                return null;

            return epochs[(int) (currentEpoch - epoch)];
        }

        boolean requiresHistoricalTopologiesFor(Unseekables<?, ?> intersect, long epoch)
        {
            Invariants.checkState(epoch <= currentEpoch);
            EpochState state = get(epoch - 1);
            return state != null && !state.syncCompleteFor(intersect);
        }
    }

    private final TopologySorter.Supplier sorter;
    private final Id node;
    private volatile Epochs epochs;

    public TopologyManager(TopologySorter.Supplier sorter, Id node)
    {
        this.sorter = sorter;
        this.node = node;
        this.epochs = new Epochs(new EpochState[0]);
    }

    @Override
    public synchronized void onTopologyUpdate(Topology topology)
    {
        Epochs current = epochs;

        checkArgument(topology.epoch == current.nextEpoch());
        EpochState[] nextEpochs = new EpochState[current.epochs.length + 1];
        List<Set<Id>> pendingSync = new ArrayList<>(current.pendingSyncComplete);
        if (!pendingSync.isEmpty())
        {
            EpochState currentEpoch = current.epochs[0];
            if (current.epochs.length <= 1 || current.epochs[1].syncComplete())
                currentEpoch.markPrevSynced();
            pendingSync.remove(0).forEach(currentEpoch::recordSyncComplete);
        }
        System.arraycopy(current.epochs, 0, nextEpochs, 1, current.epochs.length);

        boolean prevSynced = current.epochs.length == 0 || current.epochs[0].syncComplete();
        nextEpochs[0] = new EpochState(node, topology, sorter.get(topology), prevSynced);

        List<AsyncResult.Settable<Void>> futureEpochFutures = new ArrayList<>(current.futureEpochFutures);
        AsyncResult.Settable<Void> toComplete = !futureEpochFutures.isEmpty() ? futureEpochFutures.remove(0) : null;
        epochs = new Epochs(nextEpochs, pendingSync, futureEpochFutures);
        if (toComplete != null)
            toComplete.trySuccess(null);
    }

    public synchronized AsyncResult<Void> awaitEpoch(long epoch)
    {
        return epochs.awaitEpoch(epoch);
    }

    @Override
    public void onEpochSyncComplete(Id node, long epoch)
    {
        epochs.syncComplete(node, epoch);
    }

    public TopologySorter.Supplier sorter()
    {
        return sorter;
    }

    public Topology current()
    {
        return epochs.current();
    }

    public long epoch()
    {
        return current().epoch;
    }

    @VisibleForTesting
    EpochState getEpochStateUnsafe(long epoch)
    {
        return epochs.get(epoch);
    }

    public Topologies withUnsyncedEpochs(Unseekables<?, ?> select, Timestamp at)
    {
        return withUnsyncedEpochs(select, at.epoch());
    }

    public Topologies withUnsyncedEpochs(Unseekables<?, ?> select, long epoch)
    {
        return withUnsyncedEpochs(select, epoch, epoch);
    }

    public Topologies withUnsyncedEpochs(Unseekables<?, ?> select, Timestamp min, Timestamp max)
    {
        return withUnsyncedEpochs(select, min.epoch(), max.epoch());
    }

    public Topologies withUnsyncedEpochs(Unseekables<?, ?> select, long minEpoch, long maxEpoch)
    {
        Epochs snapshot = epochs;

        if (maxEpoch == Long.MAX_VALUE) maxEpoch = snapshot.currentEpoch;
        else Invariants.checkState(snapshot.currentEpoch >= maxEpoch);

        EpochState maxEpochState = snapshot.get(maxEpoch);
        if (minEpoch == maxEpoch && !snapshot.requiresHistoricalTopologiesFor(select, maxEpoch))
            return new Single(sorter, maxEpochState.global.forSelection(select));

        int start = (int)(snapshot.currentEpoch - maxEpoch);
        int limit = (int)(Math.min(1 + snapshot.currentEpoch - minEpoch, snapshot.epochs.length));
        int count = limit - start;
        while (limit < snapshot.epochs.length && !snapshot.epochs[limit].syncCompleteFor(select))
        {
            ++count;
            ++limit;
        }

        // We need to ensure we include ownership information in every epoch for all nodes we contact in any epoch
        // So we first collect the set of nodes we will contact, before selecting the affected shards and nodes in each epoch
        Set<Id> nodes = new LinkedHashSet<>();
        for (int i = start; i < limit ; ++i)
        {
            EpochState epochState = snapshot.epochs[i];
            if (epochState.epoch() < minEpoch)
                epochState.global.visitNodeForKeysOnceOrMore(select, EpochState::shardIsUnsynced, epochState, nodes::add);
            else
                epochState.global.visitNodeForKeysOnceOrMore(select, nodes::add);
        }

        Topologies.Multi topologies = new Topologies.Multi(sorter, count);
        for (int i = start; i < limit ; ++i)
        {
            EpochState epochState = snapshot.epochs[i];
            if (epochState.epoch() < minEpoch)
                topologies.add(epochState.global.forSelection(select, nodes, EpochState::shardIsUnsynced, epochState));
            else
                topologies.add(epochState.global.forSelection(select, nodes, (ignore, idx) -> true, null));
        }

        return topologies;
    }

    public Topologies preciseEpochs(Unseekables<?, ?> keys, long minEpoch, long maxEpoch)
    {
        Epochs snapshot = epochs;

        if (minEpoch == maxEpoch)
            return new Single(sorter, snapshot.get(minEpoch).global.forSelection(keys));

        Set<Id> nodes = new LinkedHashSet<>();
        int count = (int)(1 + maxEpoch - minEpoch);
        for (int i = count - 1 ; i >= 0 ; --i)
            snapshot.get(minEpoch + i).global().visitNodeForKeysOnceOrMore(keys, nodes::add);

        Topologies.Multi topologies = new Topologies.Multi(sorter, count);
        for (int i = count - 1 ; i >= 0 ; --i)
            topologies.add(snapshot.get(minEpoch + i).global.forSelection(keys, nodes));

        return topologies;
    }

    public Topologies forEpoch(Unseekables<?, ?> select, long epoch)
    {
        EpochState state = epochs.get(epoch);
        return new Single(sorter, state.global.forSelection(select));
    }

    public Shard forEpochIfKnown(RoutingKey key, long epoch)
    {
        EpochState epochState = epochs.get(epoch);
        if (epochState == null)
            return null;
        return epochState.global().forKey(key);
    }

    public Shard forEpoch(RoutingKey key, long epoch)
    {
        Shard ifKnown = forEpochIfKnown(key, epoch);
        if (ifKnown == null)
            throw new IndexOutOfBoundsException();
        return ifKnown;
    }

    public boolean hasEpoch(long epoch)
    {
        return epochs.get(epoch) != null;
    }

    public Topology localForEpoch(long epoch)
    {
        return epochs.get(epoch).local();
    }

    public Ranges localRangesForEpoch(long epoch)
    {
        return epochs.get(epoch).local().rangesForNode(node);
    }

    public Ranges localRangesForEpochs(long start, long end)
    {
        if (end < start) throw new IllegalArgumentException();
        Ranges ranges = localRangesForEpoch(start);
        for (long i = start + 1; i <= end ; ++i)
            ranges = ranges.with(localRangesForEpoch(i));
        return ranges;
    }

    public Topology globalForEpoch(long epoch)
    {
        return epochs.get(epoch).global();
    }

    public long maxUnknownEpoch(Request request)
    {
        long waitForEpoch = request.waitForEpoch();
        if (epochs.currentEpoch < waitForEpoch)
            return waitForEpoch;
        return 0;
    }
}
