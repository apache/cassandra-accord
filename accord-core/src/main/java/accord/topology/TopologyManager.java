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

import accord.api.RoutingKey;
import accord.api.TopologySorter;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.CommandStore;
import accord.local.Node.Id;
import accord.primitives.*;
import accord.topology.Topologies.Single;
import com.google.common.annotations.VisibleForTesting;
import accord.utils.Invariants;
import accord.utils.async.*;

import java.util.*;
import java.util.function.Function;

import static accord.coordinate.tracking.RequestStatus.Success;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.nonNull;

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
public class TopologyManager
{
    private static final AsyncResult<Void> SUCCESS = AsyncResults.success(null);
    static class EpochState
    {
        final Id self;
        private final Topology global;
        private final Topology local;
        private final QuorumTracker syncTracker;
        private final boolean[] curShardSyncComplete;
        private final Ranges newRanges;
        private Ranges curSyncComplete, prevSyncComplete, syncComplete;
        Ranges closed = Ranges.EMPTY, complete = Ranges.EMPTY;

        EpochState(Id node, Topology global, TopologySorter sorter, Ranges prevRanges, Ranges prevSyncComplete)
        {
            this.self = node;
            this.global = checkArgument(global, !global.isSubset());
            this.local = global.forNode(node).trim();
            Invariants.checkArgument(!global().isSubset());
            this.curShardSyncComplete = new boolean[global.shards.length];
            this.syncTracker = new QuorumTracker(new Single(sorter, global()));
            this.newRanges = global.ranges.subtract(prevRanges);
            this.prevSyncComplete = newRanges.with(prevSyncComplete);
            this.curSyncComplete = this.syncComplete = newRanges;
        }

        boolean markPrevSynced(Ranges newPrevSyncComplete)
        {
            if (prevSyncComplete.containsAll(newPrevSyncComplete))
                return false;
            Invariants.checkState(newPrevSyncComplete.containsAll(prevSyncComplete));
            prevSyncComplete = newPrevSyncComplete;
            syncComplete = curSyncComplete.slice(newPrevSyncComplete, Minimal).with(newRanges);
            return true;
        }

        public boolean recordSyncComplete(Id node)
        {
            if (syncTracker.recordSuccess(node) == Success)
            {
                curSyncComplete = global.ranges;
                syncComplete = prevSyncComplete;
                return true;
            }
            else
            {
                boolean updated = false;
                // loop over each current shard, and test if its ranges are complete
                for (int i = 0 ; i < global.shards.length ; ++i)
                {
                    if (syncTracker.get(i).hasReachedQuorum() && !curShardSyncComplete[i])
                    {
                        curSyncComplete = curSyncComplete.with(Ranges.of(global.shards[i].range));
                        syncComplete = curSyncComplete.slice(prevSyncComplete, Minimal);
                        curShardSyncComplete[i] = true;
                        updated = true;
                    }
                }
                return updated;
            }
        }

        boolean recordClosed(Ranges ranges)
        {
            if (closed.containsAll(ranges))
                return false;
            closed = closed.with(ranges);
            return true;
        }

        boolean recordComplete(Ranges ranges)
        {
            if (complete.containsAll(ranges))
                return false;
            closed = closed.with(ranges);
            complete = complete.with(ranges);
            return true;
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
            return syncComplete.containsAll(global.ranges);
        }

        /**
         * determine if sync has completed for all shards intersecting with the given keys
         */
        boolean syncCompleteFor(Unseekables<?, ?> intersect)
        {
            return syncComplete.containsAll(intersect);
        }
    }

    private static class Epochs
    {
        static class Notifications
        {
            final Set<Id> syncComplete = new TreeSet<>();
            Ranges closed = Ranges.EMPTY, complete = Ranges.EMPTY;
        }

        private static final Epochs EMPTY = new Epochs(new EpochState[0]);
        private final long currentEpoch;
        private final EpochState[] epochs;
        // nodes we've received sync complete notifications from, for epochs we do not yet have topologies for.
        // Pending sync notifications are indexed by epoch, with the current epoch as index[0], and future epochs
        // as index[epoch - currentEpoch]. Sync complete notifications for the current epoch are marked pending
        // until the superseding epoch has been applied
        private final List<Notifications> pending;

        // list of promises to be completed as newer epochs become active. This is to support processes that
        // are waiting on future epochs to begin (ie: txn requests from futures epochs). Index 0 is for
        // currentEpoch + 1
        private final List<AsyncResult.Settable<Void>> futureEpochFutures;

        private Epochs(EpochState[] epochs, List<Notifications> pending, List<AsyncResult.Settable<Void>> futureEpochFutures)
        {
            this.currentEpoch = epochs.length > 0 ? epochs[0].epoch() : 0;
            this.pending = pending;
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

        public long minEpoch()
        {
            if (currentEpoch == 0)
                return 0;
            return currentEpoch - epochs.length + 1;
        }

        public long epoch()
        {
            return currentEpoch;
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
            if (epoch > currentEpoch)
            {
                pending(epoch).syncComplete.add(node);
            }
            else
            {
                int i = indexOf(epoch);
                if (i < 0 || !epochs[i].recordSyncComplete(node))
                    return;

                while (--i >= 0 && epochs[i].markPrevSynced(epochs[i + 1].syncComplete)) {}
            }
        }

        /**
         * Mark sync complete for the given node/epoch, and if this epoch
         * is now synced, update the prevSynced flag on superseding epochs
         */
        public void epochClosed(Ranges ranges, long epoch)
        {
            checkArgument(epoch > 0);
            int i;
            if (epoch > currentEpoch)
            {
                Notifications notifications = pending(epoch);
                notifications.closed = notifications.closed.with(ranges);
                i = 0;
            }
            else
            {
                i = indexOf(epoch);
            }
            while (epochs[i].recordClosed(ranges) && ++i < epochs.length) {}
        }

        /**
         * Mark sync complete for the given node/epoch, and if this epoch
         * is now synced, update the prevSynced flag on superseding epochs
         */
        public void epochRedundant(Ranges ranges, long epoch)
        {
            checkArgument(epoch > 0);
            int i;
            if (epoch > currentEpoch)
            {
                Notifications notifications = pending(epoch);
                notifications.complete = notifications.complete.with(ranges);
                i = 0; // record these ranges as complete for all earlier epochs as well
            }
            else
            {
                i = indexOf(epoch);
                if (i < 0)
                    return;
            }
            while (epochs[i].recordComplete(ranges) && ++i < epochs.length) {}
        }

        private Notifications pending(long epoch)
        {
            int idx = (int) (epoch - (1 + currentEpoch));
            for (int i = pending.size(); i <= idx; i++)
                pending.add(new Notifications());

            return pending.get(idx);
        }

        private EpochState get(long epoch)
        {
            int index = indexOf(epoch);
            if (index < 0)
                return null;

            return epochs[index];
        }

        private int indexOf(long epoch)
        {
            if (epoch > currentEpoch || epoch <= currentEpoch - epochs.length)
                return -1;

            return (int) (currentEpoch - epoch);
        }
    }

    private final TopologySorter.Supplier sorter;
    private final Id node;
    private volatile Epochs epochs;

    public TopologyManager(TopologySorter.Supplier sorter, Id node)
    {
        this.sorter = sorter;
        this.node = node;
        this.epochs = Epochs.EMPTY;
    }

    public synchronized void onTopologyUpdate(Topology topology)
    {
        Epochs current = epochs;

        checkArgument(topology.epoch == current.nextEpoch() || epochs == Epochs.EMPTY,
                      "Expected topology update %d to be %d", topology.epoch, current.nextEpoch());
        EpochState[] nextEpochs = new EpochState[current.epochs.length + 1];
        List<Epochs.Notifications> pending = new ArrayList<>(current.pending);
        Epochs.Notifications notifications = pending.isEmpty() ? new Epochs.Notifications() : pending.remove(0);

        System.arraycopy(current.epochs, 0, nextEpochs, 1, current.epochs.length);

        Ranges prevSynced, prevAll;
        if (current.epochs.length == 0) prevSynced = prevAll = Ranges.EMPTY;
        else
        {
            prevSynced = current.epochs[0].syncComplete;
            prevAll = current.epochs[0].global.ranges;
        }
        nextEpochs[0] = new EpochState(node, topology, sorter.get(topology), prevAll, prevSynced);
        notifications.syncComplete.forEach(nextEpochs[0]::recordSyncComplete);
        nextEpochs[0].recordClosed(notifications.closed);
        nextEpochs[0].recordComplete(notifications.complete);

        List<AsyncResult.Settable<Void>> futureEpochFutures = new ArrayList<>(current.futureEpochFutures);
        AsyncResult.Settable<Void> toComplete = !futureEpochFutures.isEmpty() ? futureEpochFutures.remove(0) : null;
        epochs = new Epochs(nextEpochs, pending, futureEpochFutures);
        if (toComplete != null)
            toComplete.trySuccess(null);
    }

    public AsyncChain<Void> awaitEpoch(long epoch)
    {
        AsyncResult<Void> result;
        synchronized (this)
        {
            result = epochs.awaitEpoch(epoch);
        }
        CommandStore current = CommandStore.maybeCurrent();
        return current == null || result.isDone() ? result : result.withExecutor(current);
    }

    public void onEpochSyncComplete(Id node, long epoch)
    {
        epochs.syncComplete(node, epoch);
    }

    public synchronized void truncateTopologyUntil(long epoch)
    {
        Epochs current = epochs;
        checkArgument(current.epoch() >= epoch, "Unable to truncate; epoch %d is > current epoch %d", epoch , current.epoch());

        if (current.minEpoch() >= epoch)
            return;

        int newLen = current.epochs.length - (int) (epoch - current.minEpoch());
        Invariants.checkState(current.epochs[newLen - 1].syncComplete(), "Epoch %d's sync is not complete", current.epochs[newLen - 1].epoch());

        EpochState[] nextEpochs = new EpochState[newLen];
        System.arraycopy(current.epochs, 0, nextEpochs, 0, newLen);
        epochs = new Epochs(nextEpochs, current.pending, current.futureEpochFutures);
    }

    public void onEpochClosed(Ranges ranges, long epoch)
    {
        epochs.epochClosed(ranges, epoch);
    }

    public void onEpochRedundant(Ranges ranges, long epoch)
    {
        epochs.epochRedundant(ranges, epoch);
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

    public Topologies preciseEpochs(long epoch)
    {
        return new Single(sorter, epochs.get(epoch).global);
    }

    public Topologies withUnsyncedEpochs(Unseekables<?, ?> select, Timestamp min, Timestamp max)
    {
        return withUnsyncedEpochs(select, min.epoch(), max.epoch());
    }

    public Topologies withUnsyncedEpochs(Unseekables<?, ?> select, long minEpoch, long maxEpoch)
    {
        Invariants.checkArgument(minEpoch <= maxEpoch, "min epoch %d > max %d", minEpoch, maxEpoch);
        return withSufficientEpochs(select, minEpoch, maxEpoch, epochState -> epochState.syncComplete);
    }

    public Topologies withOpenEpochs(Unseekables<?, ?> select, Timestamp min, Timestamp max)
    {
        return withSufficientEpochs(select, min.epoch(), max.epoch(), epochState -> epochState.closed);
    }

    private Topologies withSufficientEpochs(Unseekables<?, ?> select, long minEpoch, long maxEpoch, Function<EpochState, Ranges> isSufficientFor)
    {
        Invariants.checkArgument(minEpoch <= maxEpoch);
        Epochs snapshot = epochs;

        if (maxEpoch == Long.MAX_VALUE) maxEpoch = snapshot.currentEpoch;
        else Invariants.checkState(snapshot.currentEpoch >= maxEpoch, "current epoch %d < max %d", snapshot.currentEpoch, maxEpoch);

        EpochState maxEpochState = nonNull(snapshot.get(maxEpoch));
        if (minEpoch == maxEpoch && isSufficientFor.apply(maxEpochState).containsAll(select))
            return new Single(sorter, maxEpochState.global.forSelection(select));

        int i = (int)(snapshot.currentEpoch - maxEpoch);
        int maxi = (int)(Math.min(1 + snapshot.currentEpoch - minEpoch, snapshot.epochs.length));
        Topologies.Multi topologies = new Topologies.Multi(sorter, maxi - i);

        Unseekables<?, ?> remaining = select;
        while (i < maxi)
        {
            EpochState epochState = snapshot.epochs[i++];
            topologies.add(epochState.global.forSelection(select));
            remaining = remaining.subtract(epochState.newRanges);
        }

        if (i == snapshot.epochs.length)
            return topologies;

        // include any additional epochs to reach sufficiency
        EpochState prev = snapshot.epochs[maxi - 1];
        do
        {
            Ranges sufficient = isSufficientFor.apply(prev);
            remaining = remaining.subtract(sufficient);
            if (remaining.isEmpty())
                return topologies;

            EpochState next = snapshot.epochs[i++];
            topologies.add(next.global.forSelection(remaining));
            prev = next;
        } while (i < snapshot.epochs.length);

        return topologies;
    }

    public Topologies preciseEpochs(Unseekables<?, ?> select, long minEpoch, long maxEpoch)
    {
        Epochs snapshot = epochs;

        if (minEpoch == maxEpoch)
            return new Single(sorter, snapshot.get(minEpoch).global.forSelection(select));

        int count = (int)(1 + maxEpoch - minEpoch);
        Topologies.Multi topologies = new Topologies.Multi(sorter, count);
        for (int i = count - 1 ; i >= 0 ; --i)
        {
            EpochState epochState = snapshot.get(minEpoch + i);
            topologies.add(epochState.global.forSelection(select));
            select = select.subtract(epochState.newRanges);
        }

        for (int i = count - 1 ; i >= 0 ; --i)
        Invariants.checkState(!topologies.isEmpty(), "Unable to find an epoch that contained %s", select);

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
        EpochState epochState = epochs.get(epoch);
        if (epochState == null)
            throw new IllegalStateException("Unknown epoch " + epoch);
        return epochState.local();
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
}
