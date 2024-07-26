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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import accord.api.ConfigurationService.EpochReady;
import accord.api.RoutingKey;
import accord.api.TopologySorter;
import accord.coordinate.TopologyMismatch;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.CommandStore;
import accord.local.Node.Id;
import accord.primitives.EpochSupplier;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Unseekables;
import accord.topology.Topologies.Single;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import javax.annotation.Nullable;

import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.topology.TopologyManager.EpochSufficiencyMode.AT_LEAST;
import static accord.topology.TopologyManager.EpochSufficiencyMode.AT_MOST;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.illegalState;
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
        private final BitSet curShardSyncComplete;
        private final Ranges addedRanges, removedRanges;
        private EpochReady ready;
        private Ranges curSyncComplete, prevSyncComplete, syncComplete;
        Ranges closed = Ranges.EMPTY, complete = Ranges.EMPTY;

        EpochState(Id node, Topology global, TopologySorter sorter, Ranges prevRanges, Ranges prevSyncComplete)
        {
            this.self = node;
            this.global = checkArgument(global, !global.isSubset());
            this.local = global.forNode(node).trim();
            Invariants.checkArgument(!global().isSubset());
            this.curShardSyncComplete = new BitSet(global.shards.length);
            if (global().size() > 0)
                this.syncTracker = new QuorumTracker(new Single(sorter, global()));
            else
                this.syncTracker = null;

            this.addedRanges = global.ranges.subtract(prevRanges).mergeTouching();
            this.removedRanges = prevRanges.mergeTouching().subtract(global.ranges);
            this.prevSyncComplete = addedRanges.union(MERGE_ADJACENT, prevSyncComplete.subtract(removedRanges));
            this.curSyncComplete = this.syncComplete = addedRanges;
        }

        boolean markPrevSynced(Ranges newPrevSyncComplete)
        {
            newPrevSyncComplete = newPrevSyncComplete.union(MERGE_ADJACENT, addedRanges).subtract(removedRanges);
            if (prevSyncComplete.containsAll(newPrevSyncComplete))
                return false;
            Invariants.checkState(newPrevSyncComplete.containsAll(prevSyncComplete), "Expected %s to contain all ranges in %s; but did not", newPrevSyncComplete, prevSyncComplete);
            prevSyncComplete = newPrevSyncComplete;
            syncComplete = curSyncComplete.slice(newPrevSyncComplete, Minimal).union(MERGE_ADJACENT, addedRanges);
            return true;
        }

        public boolean hasReachedQuorum()
        {
            return syncTracker == null || syncTracker.hasReachedQuorum();
        }

        public boolean recordSyncComplete(Id node)
        {
            if (syncTracker == null)
                return false;

            if (syncTracker.recordSuccess(node) == Success)
            {
                curSyncComplete = global.ranges.mergeTouching();
                syncComplete = prevSyncComplete;
                return true;
            }
            else
            {
                boolean updated = false;
                // loop over each current shard, and test if its ranges are complete
                for (int i = 0 ; i < global.shards.length ; ++i)
                {
                    if (syncTracker.get(i).hasReachedQuorum() && !curShardSyncComplete.get(i))
                    {
                        curSyncComplete = curSyncComplete.union(MERGE_ADJACENT, Ranges.of(global.shards[i].range));
                        syncComplete = curSyncComplete.slice(prevSyncComplete, Minimal);
                        curShardSyncComplete.set(i);
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
            closed = closed.union(MERGE_ADJACENT, ranges);
            return true;
        }

        boolean recordComplete(Ranges ranges)
        {
            if (complete.containsAll(ranges))
                return false;
            closed = closed.union(MERGE_ADJACENT, ranges);
            complete = complete.union(MERGE_ADJACENT, ranges);
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
        boolean syncCompleteFor(Unseekables<?> intersect)
        {
            return syncComplete.containsAll(intersect);
        }

        @Override
        public String toString()
        {
            return "EpochState{" +
                   "epoch=" + global.epoch() +
                   '}';
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

        public Topology currentLocal()
        {
            return epochs.length > 0 ? epochs[0].local() : Topology.EMPTY;
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
         * Mark the epoch as "closed" for the provided ranges; this means that no new transactions
         * that intersect with this range may be proposed in the epoch (they will be rejected).
         */
        public void epochClosed(Ranges ranges, long epoch)
        {
            checkArgument(epoch > 0);
            int i;
            if (epoch > currentEpoch)
            {
                Notifications notifications = pending(epoch);
                notifications.closed = notifications.closed.union(MERGE_ADJACENT, ranges);
                i = 0;
            }
            else
            {
                i = indexOf(epoch);
            }
            while (epochs[i].recordClosed(ranges) && ++i < epochs.length) {}
        }

        /**
         * Mark the epoch as "redundant" for the provided ranges; this means that all transactions that can be
         * proposed for this epoch have now been executed globally.
         */
        public void epochRedundant(Ranges ranges, long epoch)
        {
            checkArgument(epoch > 0);
            int i;
            if (epoch > currentEpoch)
            {
                Notifications notifications = pending(epoch);
                notifications.complete = notifications.complete.union(MERGE_ADJACENT, ranges);
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
            Invariants.checkArgument(epoch > currentEpoch);
            int idx = (int) (epoch - (1 + currentEpoch));
            for (int i = pending.size(); i <= idx; i++)
                pending.add(new Notifications());

            return pending.get(idx);
        }

        @Nullable
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

    public synchronized EpochReady onTopologyUpdate(Topology topology, Supplier<EpochReady> bootstrap)
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

        return nextEpochs[0].ready = bootstrap.get();
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

    public synchronized boolean hasReachedQuorum(long epoch)
    {
        EpochState state = epochs.get(epoch);
        return state != null && state.hasReachedQuorum();
    }

    @VisibleForTesting
    public EpochReady epochReady(long epoch)
    {
        Epochs epochs = this.epochs;

        if (epoch < epochs.minEpoch())
            return EpochReady.done(epoch);

        if (epoch > epochs.currentEpoch)
            throw new IllegalArgumentException(String.format("Epoch %d is larger than current epoch %d", epoch, epochs.currentEpoch));

        return epochs.get(epoch).ready;
    }

    public synchronized void onEpochSyncComplete(Id node, long epoch)
    {
        epochs.syncComplete(node, epoch);
    }

    public synchronized void onRemoveNodes(long removedIn, Collection<Id> removed)
    {
        for (long epoch = removedIn, min = minEpoch(); epoch >= min; epoch--)
        {
            EpochState state = epochs.get(epoch);
            if (state == null || state.hasReachedQuorum()) continue;
            for (Id node : removed)
                epochs.syncComplete(node, epoch);
        }
    }

    @VisibleForTesting
    public Ranges syncComplete(long epoch)
    {
        return epochs.get(epoch).syncComplete;
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

    public synchronized void onEpochClosed(Ranges ranges, long epoch)
    {
        epochs.epochClosed(ranges, epoch);
    }

    public synchronized void onEpochRedundant(Ranges ranges, long epoch)
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

    public Topology currentLocal()
    {
        return epochs.currentLocal();
    }

    public long epoch()
    {
        return current().epoch;
    }

    public long minEpoch()
    {
        return epochs.minEpoch();
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

    // TODO (required): test all of these methods when asking for epochs that have been cleaned up (and other code paths)
    public Topologies withUnsyncedEpochs(Unseekables<?> select, Timestamp min, Timestamp max)
    {
        return withUnsyncedEpochs(select, min.epoch(), max.epoch());
    }

    public Topologies withUnsyncedEpochs(Unseekables<?> select, long minEpoch, long maxEpoch)
    {
        Invariants.checkArgument(minEpoch <= maxEpoch, "min epoch %d > max %d", minEpoch, maxEpoch);
        return withSufficientEpochs(select, minEpoch, maxEpoch, epochState -> epochState.syncComplete, AT_LEAST);
    }

    public Topologies withOpenEpochs(Unseekables<?> select, EpochSupplier min, EpochSupplier max, EpochSufficiencyMode mode)
    {
        return withSufficientEpochs(select, min.epoch(), max.epoch(), epochState -> epochState.closed, mode);
    }

    public Topologies withUncompletedEpochs(Unseekables<?> select, EpochSupplier min, EpochSupplier max, EpochSufficiencyMode mode)
    {
        return withSufficientEpochs(select, min.epoch(), max.epoch(), epochState -> epochState.complete, mode);
    }

    public enum EpochSufficiencyMode { AT_LEAST, AT_MOST }

    private Topologies withSufficientEpochs(Unseekables<?> select, long minEpoch, long maxEpoch, Function<EpochState, Ranges> isSufficientFor, EpochSufficiencyMode mode)
    {
        Invariants.checkArgument(minEpoch <= maxEpoch);
        Epochs snapshot = epochs;

        if (mode == AT_LEAST)
        {
            TopologyMismatch tm = TopologyMismatch.checkForMismatch(snapshot.get(maxEpoch).global(), select);
            if (tm != null)
                throw tm;
        }

        if (maxEpoch == Long.MAX_VALUE) maxEpoch = snapshot.currentEpoch;
        else Invariants.checkState(snapshot.currentEpoch >= maxEpoch, "current epoch %d < max %d", snapshot.currentEpoch, maxEpoch);

        EpochState maxEpochState = nonNull(snapshot.get(maxEpoch));
        if (minEpoch == maxEpoch && (mode != AT_LEAST || isSufficientFor.apply(maxEpochState).containsAll(select)))
            return new Single(sorter, maxEpochState.global.forSelection(select));

        int i = (int)(snapshot.currentEpoch - maxEpoch);
        int maxi = (int)(Math.min(1 + snapshot.currentEpoch - minEpoch, snapshot.epochs.length));
        Topologies.Builder topologies = new Topologies.Builder(maxi - i);

        // Previous logic would exclude synced ranges, but this was removed as that makes min epoch selection harder.
        // An issue was found where a range was removed from a replica and min selection picked the epoch before that,
        // which caused a node to get included in the txn that actually lost the range
        // See CASSANDRA-18804
        while (i < maxi && !select.isEmpty())
        {
            EpochState epochState = snapshot.epochs[i++];
            topologies.add(epochState.global.forSelection(select));
            select = select.subtract(epochState.addedRanges);
            if (mode == AT_MOST)
                select = select.subtract(isSufficientFor.apply(epochState));
        }

        if (select.isEmpty() || mode == AT_MOST)
            return topologies.build(sorter);

        if (i == snapshot.epochs.length)
        {
            if (!select.isEmpty())
                throw new IllegalArgumentException("Ranges " + select + " could not be found");
            return topologies.build(sorter);
        }

        // remaining is updated based off isSufficientFor, but select is not
        Unseekables<?> remaining = select;

        // include any additional epochs to reach sufficiency
        EpochState prev = snapshot.epochs[maxi - 1];
        do
        {
            remaining = remaining.subtract(isSufficientFor.apply(prev));
            Unseekables<?> prevSelect = select;
            select = select.subtract(prev.addedRanges);
            if (prevSelect != select) // perf optimization; if select wasn't changed (it does not intersect addedRanges), then remaining won't
                remaining = remaining.subtract(prev.addedRanges);
            if (remaining.isEmpty())
                return topologies.build(sorter);

            EpochState next = snapshot.epochs[i++];
            topologies.add(next.global.forSelection(select));
            prev = next;
        } while (i < snapshot.epochs.length);
        // needd to remove sufficent / added else remaining may not be empty when the final matches are the last epoch
        remaining = remaining.subtract(isSufficientFor.apply(prev));
        remaining = remaining.subtract(prev.addedRanges);

        if (!remaining.isEmpty()) throw new IllegalArgumentException("Ranges " + remaining + " could not be found");

        return topologies.build(sorter);
    }

    public Topologies preciseEpochs(Unseekables<?> select, long minEpoch, long maxEpoch)
    {
        Epochs snapshot = epochs;

        EpochState maxState = snapshot.get(maxEpoch);
        Invariants.checkState(maxState != null, "Unable to find epoch %d; known epochs are %d -> %d", maxEpoch, snapshot.minEpoch(), snapshot.currentEpoch);
        TopologyMismatch tm = TopologyMismatch.checkForMismatch(maxState.global(), select);
        if (tm != null)
            throw tm;

        if (minEpoch == maxEpoch)
            return new Single(sorter, snapshot.get(minEpoch).global.forSelection(select));

        int count = (int)(1 + maxEpoch - minEpoch);
        Topologies.Builder topologies = new Topologies.Builder(count);
        for (int i = count - 1 ; i >= 0 ; --i)
        {
            EpochState epochState = snapshot.get(minEpoch + i);
            topologies.add(epochState.global.forSelection(select));
            select = select.subtract(epochState.addedRanges);
        }

        return topologies.build(sorter);
    }

    public Topologies forEpoch(Unseekables<?> select, long epoch)
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
            throw illegalState("Unknown epoch " + epoch);
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
        EpochState epochState = epochs.get(epoch);
        if (epochState == null)
            throw new IllegalArgumentException("Unknown epoch: " + epoch);
        return epochState.global();
    }
}
