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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.ConfigurationService;
import accord.api.ConfigurationService.EpochReady;
import accord.api.ProtocolModifiers.QuorumEpochIntersections.Include;
import accord.api.Timeouts;
import accord.api.Timeouts.RegisteredTimeout;
import accord.api.TopologySorter;
import accord.api.VisibleForImplementation;
import accord.coordinate.EpochTimeout;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node.Id;
import accord.local.TimeService;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId.FastPath;
import accord.primitives.Unseekables;
import accord.topology.Topologies.SelectNodeOwnership;
import accord.topology.Topologies.Single;
import accord.utils.IndexedBiFunction;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Owned;
import static accord.api.ProtocolModifiers.QuorumEpochIntersections.Include.Unsynced;
import static accord.coordinate.tracking.RequestStatus.Success;
import static accord.primitives.AbstractRanges.UnionMode.MERGE_ADJACENT;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithoutDeps;
import static accord.primitives.TxnId.FastPath.Unoptimised;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.nonNull;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.stream.Collectors.joining;

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
    private static final Logger logger = LoggerFactory.getLogger(TopologyManager.class);
    private static final FutureEpoch SUCCESS;

    static
    {
        SUCCESS = new FutureEpoch(-1L, null);
        SUCCESS.setDone();
    }

    static class EpochState
    {
        final Id self;
        private final Topology global;
        private final Topology local;
        private final QuorumTracker syncTracker;
        private final BitSet curShardSyncComplete;
        private final Ranges addedRanges, removedRanges;
        @GuardedBy("TopologyManager.this")
        private EpochReady ready;
        @GuardedBy("TopologyManager.this")
        private Ranges synced;
        @GuardedBy("TopologyManager.this")
        Ranges closed = Ranges.EMPTY, retired = Ranges.EMPTY;

        private volatile boolean allRetired;

        public boolean allRetired()
        {
            if (allRetired)
                return true;

            if (!retired.containsAll(global.ranges))
                return false;

            Invariants.require(closed.containsAll(global.ranges));
            allRetired = true;
            return true;
        }

        EpochState(Id node, Topology global, TopologySorter sorter, Ranges prevRanges)
        {
            this.self = node;
            this.global = Invariants.requireArgument(global, !global.isSubset());
            this.local = global.forNode(node).trim();
            Invariants.requireArgument(!global().isSubset());
            this.curShardSyncComplete = new BitSet(global.shards.length);
            if (!global().isEmpty())
                this.syncTracker = new QuorumTracker(new Single(sorter, global()));
            else
                this.syncTracker = null;

            this.addedRanges = global.ranges.without(prevRanges).mergeTouching();
            this.removedRanges = prevRanges.mergeTouching().without(global.ranges);
            this.synced = addedRanges;
        }

        public boolean hasReachedQuorum()
        {
            return syncTracker == null || syncTracker.hasReachedQuorum();
        }

        private boolean recordSyncCompleteFromFuture()
        {
            if (syncTracker == null || syncComplete())
                return false;
            synced = global.ranges.mergeTouching();
            return true;
        }

        enum NodeSyncStatus { Untracked, Complete, ShardUpdate, NoUpdate }

        NodeSyncStatus recordSyncComplete(Id node)
        {
            if (syncTracker == null)
                return NodeSyncStatus.Untracked;

            if (syncTracker.recordSuccess(node) == Success)
            {
                synced = global.ranges.mergeTouching();
                return NodeSyncStatus.Complete;
            }
            else
            {
                boolean updated = false;
                // loop over each current shard, and test if its ranges are complete
                for (int i = 0 ; i < global.shards.length ; ++i)
                {
                    if (syncTracker.get(i).hasReachedQuorum() && !curShardSyncComplete.get(i))
                    {
                        synced = synced.union(MERGE_ADJACENT, Ranges.of(global.shards[i].range));
                        curShardSyncComplete.set(i);
                        updated = true;
                    }
                }
                return updated ? NodeSyncStatus.ShardUpdate : NodeSyncStatus.NoUpdate;
            }
        }

        // returns those ranges that weren't already closed, so that they can be propagated to lower epochs
        Ranges recordClosed(Ranges ranges)
        {
            ranges = ranges.without(closed);
            if (ranges.isEmpty())
                return ranges;
            closed = closed.union(MERGE_ADJACENT, ranges);
            Invariants.require(closed.mergeTouching() == closed);
            return ranges.without(addedRanges);
        }

        // returns those ranges that weren't already retired, so that they can be propagated to lower epochs
        Ranges recordRetired(Ranges ranges)
        {
            ranges = ranges.without(retired);
            if (ranges.isEmpty())
                return ranges;
            closed = closed.union(MERGE_ADJACENT, ranges);
            retired = retired.union(MERGE_ADJACENT, ranges);
            Invariants.require(closed.mergeTouching() == closed);
            Invariants.require(retired.mergeTouching() == retired);
            return ranges.without(addedRanges);
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
            return synced.containsAll(global.ranges);
        }

        /**
         * determine if sync has completed for all shards intersecting with the given keys
         */
        boolean syncCompleteFor(Unseekables<?> intersect)
        {
            return synced.containsAll(intersect);
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
            Ranges closed = Ranges.EMPTY, retired = Ranges.EMPTY;
        }

        private static final Epochs EMPTY = new Epochs(new EpochState[0], Collections.emptyList(), Collections.emptyList(), -1);
        private final long currentEpoch;
        private final long firstNonEmptyEpoch;
        // Epochs are sorted in _descending_ order
        private final EpochState[] epochs;
        // nodes we've received sync complete notifications from, for epochs we do not yet have topologies for.
        // Pending sync notifications are indexed by epoch, with the current epoch as index[0], and future epochs
        // as index[epoch - currentEpoch]. Sync complete notifications for the current epoch are marked pending
        // until the superseding epoch has been applied
        private final List<Notifications> pending;

        // list of promises to be completed as newer epochs become active. This is to support processes that
        // are waiting on future epochs to begin (ie: txn requests from futures epochs). Index 0 is for
        // currentEpoch + 1
        // NOTE: this is NOT copy-on-write. This is mutated in place!
        private final List<FutureEpoch> futureEpochs;

        private Epochs(EpochState[] epochs, List<Notifications> pending, List<FutureEpoch> futureEpochs, long prevFirstNonEmptyEpoch)
        {
            this.currentEpoch = epochs.length > 0 ? epochs[0].epoch() : 0;
            if (prevFirstNonEmptyEpoch != -1)
                this.firstNonEmptyEpoch = prevFirstNonEmptyEpoch;
            else if (epochs.length > 0  && !epochs[0].global().isEmpty())
                this.firstNonEmptyEpoch = currentEpoch;
            else
                this.firstNonEmptyEpoch = prevFirstNonEmptyEpoch;

            this.pending = pending;
            this.futureEpochs = futureEpochs;
            if (!futureEpochs.isEmpty())
                Invariants.require(futureEpochs.get(0).epoch == currentEpoch + 1);

            for (int i = 1; i < futureEpochs.size(); i++)
                Invariants.requireArgument(futureEpochs.get(i).epoch == futureEpochs.get(i - 1).epoch + 1);
            for (int i = 1; i < epochs.length; i++)
                Invariants.requireArgument(epochs[i].epoch() == epochs[i - 1].epoch() - 1);
            int truncateFrom = -1;
            // > 0 because we do not want to be left without epochs in case they're all empty
            for (int i = epochs.length - 1; i > 0; i--)
            {
                EpochState epochState = epochs[i];
                if (epochState.allRetired() &&
                    (truncateFrom == -1 || truncateFrom == i + 1))
                {
                    Invariants.require(epochs[i].syncComplete());
                    truncateFrom = i;
                }
            }

            if (truncateFrom == -1)
            {
                this.epochs = epochs;
            }
            else
            {
                this.epochs = Arrays.copyOf(epochs, truncateFrom);
                for (int i = truncateFrom; i < epochs.length; i++)
                {
                    EpochState state = epochs[i];
                    Invariants.require(epochs[i].syncComplete());
                    logger.info("Retired epoch {} with added/removed ranges {}/{}. Topology: {}. Closed: {}", state.epoch(), state.addedRanges, state.removedRanges, state.global.ranges, state.closed);
                }
                if (logger.isTraceEnabled())
                {
                    for (int i = 0; i < truncateFrom; i++)
                    {
                        EpochState state = epochs[i];
                        Invariants.require(state.syncComplete());
                        logger.trace("Leaving epoch {} with added/removed ranges {}/{}", state.epoch(), state.addedRanges, state.removedRanges);
                    }
                }
            }
        }

        private FutureEpoch awaitEpoch(long epoch, TopologyManager manager)
        {
            if (epoch <= currentEpoch)
                return SUCCESS;

            int expectedIndex = (int) (epoch - (1 + currentEpoch));
            while (futureEpochs.size() <= expectedIndex)
            {
                long addEpoch = currentEpoch + futureEpochs.size() + 1;
                FutureEpoch futureEpoch = new FutureEpoch(addEpoch, manager);
                futureEpochs.add(futureEpoch);
            }

            return futureEpochs.get(expectedIndex);
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
            Invariants.requireArgument(epoch > 0);
            if (epoch > currentEpoch)
            {
                pending(epoch).syncComplete.add(node);
            }
            else
            {
                int i = indexOf(epoch);
                if (i < 0)
                    return;

                EpochState.NodeSyncStatus status = epochs[i].recordSyncComplete(node);
                switch (status)
                {
                    case Complete:
                        i++;
                        for (; i < epochs.length && epochs[i].recordSyncCompleteFromFuture(); i++) {}
                        break;
                    case Untracked:
                        // don't have access to TopologyManager.this.node to check if the nodes match... this state should not happen unless it is the same node
                    case NoUpdate:
                    case ShardUpdate:
                        break;
                    default:
                        throw new UnsupportedOperationException("Unknown status " + status);
                }
            }
        }

        /**
         * Mark the epoch as "closed" for the provided ranges; this means that no new transactions
         * that intersect with this range may be proposed in the epoch (they will be rejected).
         */
        public void epochClosed(Ranges ranges, long epoch)
        {
            Invariants.requireArgument(epoch > 0);
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

            if (i == -1)
            {
                Invariants.require(epoch < minEpoch(), "Could not find epoch %d. Min: %d, current: %d", epoch, minEpoch(), currentEpoch);
                return; // notification came for an already truncated epoch
            }

            while (!ranges.isEmpty() && i < epochs.length)
                ranges = epochs[i++].recordClosed(ranges);
        }

        /**
         * Mark the epoch as "retired" for the provided ranges; this means that all transactions that can be
         * proposed for this epoch have now been executed globally.
         */
        public void epochRetired(Ranges ranges, long epoch)
        {
            Invariants.requireArgument(epoch > 0);
            int retiredIdx;
            if (epoch > currentEpoch)
            {
                Notifications notifications = pending(epoch);
                notifications.retired = notifications.retired.union(MERGE_ADJACENT, ranges);
                retiredIdx = 0; // record these ranges as complete for all earlier epochs as well
            }
            else
            {
                retiredIdx = indexOf(epoch);
                if (retiredIdx < 0)
                    return;
            }

            for (int i = retiredIdx; !ranges.isEmpty() && i < epochs.length; i++)
                ranges = epochs[i].recordRetired(ranges);
        }

        private Notifications pending(long epoch)
        {
            Invariants.requireArgument(epoch > currentEpoch);
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

    static class WaitingForEpoch extends AsyncResults.SettableResult<Void>
    {
        final long deadlineMicros;
        WaitingForEpoch(long deadlineMicros)
        {
            this.deadlineMicros = deadlineMicros;
        }
    }

    private static class FutureEpoch implements Timeouts.Timeout
    {
        private final long epoch;
        private final TopologyManager manager;
        private boolean isDone;
        private final ArrayDeque<WaitingForEpoch> waiting = new ArrayDeque<>();
        private RegisteredTimeout timeout;

        public FutureEpoch(long epoch, TopologyManager manager)
        {
            this.epoch = epoch;
            this.manager = manager;
        }

        // TODO (expected): pass through request deadline
        AsyncResult<Void> waiting()
        {
            WaitingForEpoch result, last;
            synchronized (this)
            {
                if (isDone)
                    return AsyncResults.success(null);

                long timeoutMicros = manager.agent.expireEpochWait(MICROSECONDS);
                long deadlineMicros = manager.time.elapsed(MICROSECONDS) + timeoutMicros;
                result = last = waiting.peekLast();
                if (last == null || last.deadlineMicros < deadlineMicros)
                    waiting.add(result = new WaitingForEpoch(deadlineMicros + (timeoutMicros / 10)));
            }
            if (last == null)
                timeout = manager.timeouts.registerAt(this, result.deadlineMicros, MICROSECONDS);
            return result;
        }

        private void setDone()
        {
            synchronized (this)
            {
                isDone = true;
                WaitingForEpoch next;
                while (null != (next = waiting.poll()))
                    next.trySuccess(null);
            }
            RegisteredTimeout cancel = timeout;
            if (cancel != null)
                cancel.cancel();
        }

        @Override
        public void timeout()
        {
            long nextDeadlineMicros = 0;
            synchronized (this)
            {
                if (isDone)
                    return;

                long nowMicros = manager.time.elapsed(MICROSECONDS);
                WaitingForEpoch next;
                while (null != (next = waiting.peek()) && (nextDeadlineMicros = next.deadlineMicros) <= nowMicros)
                    waiting.poll().tryFailure(new EpochTimeout(epoch));
            }
            if (nextDeadlineMicros > 0)
                timeout = manager.timeouts.registerAt(this, nextDeadlineMicros, MICROSECONDS);
        }

        @Override
        public int stripe()
        {
            return (int) epoch;
        }
    }

    // this class could be just the list, but left it here in case we wish to expose "futureEpochs" and "pending" as well
    public static class EpochsSnapshot implements Iterable<EpochsSnapshot.Epoch>
    {
        public final ImmutableList<Epoch> epochs;

        public EpochsSnapshot(ImmutableList<Epoch> epochs)
        {
            this.epochs = epochs;
        }

        @Override
        public Iterator<Epoch> iterator()
        {
            return epochs.iterator();
        }

        public enum ResultStatus
        {
            PENDING("pending"),
            SUCCESS("success"),
            FAILURE("failure");

            public final String value;

            ResultStatus(String value)
            {
                this.value = value;
            }

            private static ResultStatus of(AsyncResult<?> result)
            {
                if (result == null || !result.isDone())
                    return PENDING;

                return result.isSuccess() ? SUCCESS : FAILURE;
            }
        }

        public static class EpochReady
        {
            public final ResultStatus metadata, coordinate, data, reads;

            public EpochReady(ResultStatus metadata, ResultStatus coordinate, ResultStatus data, ResultStatus reads)
            {
                this.metadata = metadata;
                this.coordinate = coordinate;
                this.data = data;
                this.reads = reads;
            }

            private static EpochReady of(ConfigurationService.EpochReady ready)
            {
                return new EpochReady(ResultStatus.of(ready.metadata),
                                      ResultStatus.of(ready.coordinate),
                                      ResultStatus.of(ready.data),
                                      ResultStatus.of(ready.reads));
            }
        }

        public static class Epoch
        {
            public final long epoch;
            public final EpochReady ready;
            public final Ranges global, addedRanges, removedRanges, synced, closed, retired;

            public Epoch(long epoch, EpochReady ready, Ranges global, Ranges addedRanges, Ranges removedRanges, Ranges synced, Ranges closed, Ranges retired)
            {
                this.epoch = epoch;
                this.ready = ready;
                this.global = global;
                this.addedRanges = addedRanges;
                this.removedRanges = removedRanges;
                this.synced = synced;
                this.closed = closed;
                this.retired = retired;
            }
        }
    }

    private final TopologySorter.Supplier sorter;
    private final TopologiesCollectors collector;
    private final BestFastPath bestFastPath;
    private final SupportsPrivilegedFastPath supportsPrivilegedFastPath;
    private final Agent agent;
    private final Id self;
    private final TimeService time;
    private final Timeouts timeouts;
    private volatile Epochs epochs;

    public TopologyManager(TopologySorter.Supplier sorter, Agent agent, Id self, TimeService time, Timeouts timeouts)
    {
        this.sorter = sorter;
        this.collector = new TopologiesCollectors(sorter, SelectNodeOwnership.SHARE);
        this.bestFastPath = new BestFastPath(self);
        this.supportsPrivilegedFastPath = new SupportsPrivilegedFastPath(self);
        this.agent = agent;
        this.self = self;
        this.time = time;
        this.timeouts = timeouts;
        this.epochs = Epochs.EMPTY;
    }

    public EpochsSnapshot epochsSnapshot()
    {
        // Write to this volatile variable is done via synchronized, so this is single-writer multi-consumer; safe to read without locks
        Epochs epochs = this.epochs;
        ImmutableList.Builder<EpochsSnapshot.Epoch> builder = ImmutableList.builderWithExpectedSize(epochs.epochs.length);
        for (int i = 0; i < epochs.epochs.length; i++)
        {
            // This class's state is mutable with regaurd to: ready, synced, closed, retired
            EpochState epoch = epochs.epochs[i];
            // Even though this field is populated with the same lock epochs is, it is done before publishing to epochs!
            // For this reason the field maybe null, in which case we need to use the lock to wait for the field.
            EpochReady ready;
            Ranges global, addedRanges, removedRanges, synced, closed, retired;
            global = epoch.global.ranges.mergeTouching();
            addedRanges = epoch.addedRanges;
            removedRanges = epoch.removedRanges;
            // ready, synced, closed, and retired all rely on TM's object lock
            synchronized (this)
            {
                ready = epoch.ready;
                synced = epoch.synced;
                closed = epoch.closed;
                retired = epoch.retired;
            }
            builder.add(new EpochsSnapshot.Epoch(epoch.epoch(), EpochsSnapshot.EpochReady.of(ready), global, addedRanges, removedRanges, synced, closed, retired));
        }
        return new EpochsSnapshot(builder.build());
    }

    public EpochReady onTopologyUpdate(Topology topology, Supplier<EpochReady> bootstrap, LongConsumer truncate)
    {
        FutureEpoch notifyDone;
        EpochReady ready;
        Epochs prev;
        Epochs next;
        synchronized (this)
        {
            prev = epochs;
            Invariants.requireArgument(topology.epoch == prev.nextEpoch() || epochs == Epochs.EMPTY,
                                       "Expected topology update %d to be %d", topology.epoch, prev.nextEpoch());
            EpochState[] nextEpochs = new EpochState[prev.epochs.length + 1];
            List<Epochs.Notifications> pending = new ArrayList<>(prev.pending);
            Epochs.Notifications notifications = pending.isEmpty() ? new Epochs.Notifications() : pending.remove(0);

            System.arraycopy(prev.epochs, 0, nextEpochs, 1, prev.epochs.length);

            Ranges prevAll = prev.epochs.length == 0 ? Ranges.EMPTY : prev.epochs[0].global.ranges;
            nextEpochs[0] = new EpochState(self, topology, sorter.get(topology), prevAll);
            notifications.syncComplete.forEach(nextEpochs[0]::recordSyncComplete);
            nextEpochs[0].recordClosed(notifications.closed);
            nextEpochs[0].recordRetired(notifications.retired);

            List<FutureEpoch> futureEpochs = new ArrayList<>(prev.futureEpochs);
            notifyDone = !futureEpochs.isEmpty() ? futureEpochs.remove(0) : null;
            next = new Epochs(nextEpochs, pending, futureEpochs, prev.firstNonEmptyEpoch);
            epochs = next;
            ready = nextEpochs[0].ready = bootstrap.get();
        }

        if (next.minEpoch() != prev.minEpoch())
            truncate.accept(epochs.minEpoch());
        
        if (notifyDone != null)
            notifyDone.setDone();

        return ready;
    }

    public AsyncChain<Void> awaitEpoch(long epoch, @Nullable Executor executor)
    {
        FutureEpoch futureEpoch;
        synchronized (this)
        {
            futureEpoch = epochs.awaitEpoch(epoch, this);
        }
        AsyncResult<Void> result = futureEpoch.waiting();
        return executor == null || result.isDone() ? result : result.withExecutor(executor);
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

    @VisibleForTesting
    public Ranges syncComplete(long epoch)
    {
        return epochs.get(epoch).synced;
    }

    public synchronized void truncateTopologiesUntil(long epoch)
    {
        Epochs current = epochs;
        Invariants.requireArgument(current.epoch() >= epoch, "Unable to truncate; epoch %d is > current epoch %d", epoch , current.epoch());

        if (current.minEpoch() >= epoch)
            return;

        int newLen = current.epochs.length - (int) (epoch - current.minEpoch());
        Invariants.require(current.epochs[newLen - 1].syncComplete(), "Epoch %d's sync is not complete", current.epochs[newLen - 1].epoch());

        EpochState[] nextEpochs = new EpochState[newLen];
        System.arraycopy(current.epochs, 0, nextEpochs, 0, newLen);
        epochs = new Epochs(nextEpochs, current.pending, current.futureEpochs, current.firstNonEmptyEpoch);
    }

    public synchronized void onEpochClosed(Ranges ranges, long epoch)
    {
        epochs.epochClosed(ranges, epoch);
    }

    /**
     * If ranges were added in epoch X, and are _not_ present in the current epoch, they
     * are purged and durability scheduling for them should be cancelled.
     */
    public synchronized boolean isFullyRetired(Ranges ranges)
    {
        Epochs epochs = this.epochs;
        EpochState current = epochs.get(epochs.currentEpoch);
        if (!current.addedRanges.containsAll(ranges))
            return false;

        long minEpoch = epochs.minEpoch();
        for (long i = minEpoch; i < epochs.currentEpoch; i++)
        {
            EpochState retiredIn = epochs.get(i);
            if (retiredIn.allRetired() && retiredIn.addedRanges.containsAll(ranges))
                return true;
        }

        return false;
    }

    public synchronized void onEpochRetired(Ranges ranges, long epoch)
    {
        epochs.epochRetired(ranges, epoch);
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

    public boolean isEmpty()
    {
        return epochs == Epochs.EMPTY;
    }

    public long epoch()
    {
        return current().epoch;
    }

    // TODO (desired): add tests for epoch GC and tracking
    @VisibleForImplementation
    public long firstNonEmpty()
    {
        return epochs.firstNonEmptyEpoch;
    }

    public long minEpoch()
    {
        Epochs epochs = this.epochs;
        return epochs.minEpoch();
    }

    @VisibleForTesting
    EpochState getEpochStateUnsafe(long epoch)
    {
        return epochs.get(epoch);
    }

    /**
     * Fetch topologies between {@param minEpoch} (inclusive), and {@param maxEpoch} (inclusive).
     */
    public TopologyRange between(long minEpoch, long maxEpoch)
    {
        Epochs epochs = this.epochs;
        // No epochs known to Accord
        if (epochs.firstNonEmptyEpoch == -1 || minEpoch > epochs.currentEpoch)
            return new TopologyRange(epochs.minEpoch(), epochs.currentEpoch, epochs.firstNonEmptyEpoch, Collections.emptyList());

        minEpoch = Math.max(minEpoch, epochs.minEpoch());
        int diff =  Math.toIntExact(epochs.currentEpoch - minEpoch + 1);
        List<Topology> topologies = new ArrayList<>(diff);
        for (int i = 0; epochs.minEpoch() + i <= maxEpoch && i < diff; i++)
            topologies.add(epochs.get(minEpoch + i).global);

        return new TopologyRange(epochs.minEpoch(), epochs.currentEpoch, epochs.firstNonEmptyEpoch, topologies);
    }

    public static class TopologyRange
    {
        public final long min;
        public final long current;
        public final long firstNonEmpty;
        public final List<Topology> topologies;

        public TopologyRange(long min, long current, long firstNonEmpty, List<Topology> topologies)
        {
            this.min = min;
            this.current = current;
            this.topologies = topologies;
            this.firstNonEmpty = firstNonEmpty;
        }

        public void forEach(Consumer<Topology> forEach, long minEpoch, int count)
        {
            if (minEpoch == 0) // Bootstrap
                minEpoch = this.min;

            long emptyUpTo = firstNonEmpty == -1 ? current : firstNonEmpty - 1;
            // Report empty epochs
            for (long epoch = minEpoch; epoch <= emptyUpTo && count > 0; epoch++, count--)
                forEach.accept(new Topology(epoch));

            // Report known non-empty epochs
            for (int i = 0; i < topologies.size() && count > 0; i++, count--)
            {
                Topology topology = topologies.get(i);
                Invariants.require(i > 0 || topology.epoch() == minEpoch || firstNonEmpty == topology.epoch(),
                                   "Min epoch: %d. Range: %s", minEpoch, this);
                forEach.accept(topology);
            }
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) return false;
            TopologyRange that = (TopologyRange) o;
            return min == that.min && current == that.current && firstNonEmpty == that.firstNonEmpty && Objects.equals(topologies, that.topologies);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(min, current, firstNonEmpty, topologies);
        }

        @Override
        public String toString()
        {
            return String.format("TopologyRange{min=%d, current=%d, firstNonEmpty=%d, topologies=[%s]}",
                                 min,
                                 current,
                                 firstNonEmpty,
                                 topologies.stream().map(t -> Long.toString(t.epoch())).collect(joining(",")));
        }
    }

    public Topologies preciseEpochs(long epoch)
    {
        return new Single(sorter, epochs.get(epoch).global);
    }

    // TODO (testing): test all of these methods when asking for epochs that have been cleaned up (and other code paths)

    /**
     * Returns topologies containing epochs where specified ranges haven't completed synchronization between min/max epochs.
     * Can be used during coordination operations to ensure they contact all relevant nodes across topology changes,
     * particularly when some ranges are still syncing after cluster membership changes.
     */
    public Topologies withUnsyncedEpochs(Unseekables<?> select, Timestamp min, Timestamp max)
    {
        return withUnsyncedEpochs(select, min.epoch(), max.epoch());
    }

    public Topologies select(Unseekables<?> select, Timestamp min, Timestamp max, SelectNodeOwnership selectNodeOwnership, Include include)
    {
        return select(select, min.epoch(), max.epoch(), selectNodeOwnership, include);
    }

    public Topologies select(Unseekables<?> select, long minEpoch, long maxEpoch, SelectNodeOwnership selectNodeOwnership, Include include)
    {
        switch (include)
        {
            default: throw new AssertionError("Unhandled Include: " +include);
            case Unsynced: return withUnsyncedEpochs(select, minEpoch, maxEpoch);
            case Owned: return preciseEpochs(select, minEpoch, maxEpoch, selectNodeOwnership);
        }
    }

    public Topologies reselect(@Nullable Topologies prev, @Nullable Include prevIncluded, Unseekables<?> select, Timestamp min, Timestamp max, SelectNodeOwnership selectNodeOwnership, Include include)
    {
        return reselect(prev, prevIncluded, select, min.epoch(), max.epoch(), selectNodeOwnership, include);
    }

    // prevIncluded may be null even when prev is not null, in cases where we do not know what prev was produced with
    public Topologies reselect(@Nullable Topologies prev, @Nullable Include prevIncluded, Unseekables<?> select, long minEpoch, long maxEpoch, SelectNodeOwnership selectNodeOwnership, Include include)
    {
        if (include == Owned)
        {
            if (prev != null && prev.currentEpoch() >= maxEpoch && prev.oldestEpoch() <= minEpoch)
                return prev.forEpochs(minEpoch, maxEpoch);
            else
                return preciseEpochs(select, minEpoch, maxEpoch, selectNodeOwnership);
        }
        else
        {
            if (prevIncluded == Unsynced && prev != null && prev.currentEpoch() == maxEpoch && prev.oldestEpoch() == minEpoch)
                return prev;
            else // TODO (desired): can we avoid recalculating when only minEpoch advances?
                return withUnsyncedEpochs(select, minEpoch, maxEpoch);
        }

    }

    public <U extends Participants<?>> @Nullable U unsyncedOnly(U select, long beforeEpoch)
    {
        return extra(select, 0, beforeEpoch, cur -> cur.synced, (UnsyncedSelector<U>)UnsyncedSelector.INSTANCE);
    }

    public Topologies withUnsyncedEpochs(Unseekables<?> select, long minEpoch, long maxEpoch)
    {
        Invariants.requireArgument(minEpoch <= maxEpoch, "min epoch %d > max %d", minEpoch, maxEpoch);
        return withSufficientEpochsAtLeast(select, minEpoch, maxEpoch, epochState -> epochState.synced);
    }

    public FastPath selectFastPath(Routables<?> select, long epoch)
    {
        return atLeast(select, epoch, epoch, epochState -> epochState.synced, bestFastPath);
    }

    public boolean supportsPrivilegedFastPath(Routables<?> select, long epoch)
    {
        return atLeast(select, epoch, epoch, epochState -> epochState.synced, supportsPrivilegedFastPath);
    }

    public Topologies withOpenEpochs(Routables<?> select, @Nullable EpochSupplier min, @Nullable EpochSupplier max)
    {
        return withSufficientEpochsAtMost(select,
                                          min == null ? Long.MIN_VALUE : min.epoch(),
                                          max == null ? Long.MAX_VALUE : max.epoch(),
                                          prev -> prev.closed);
    }

    public Topologies withUncompletedEpochs(Unseekables<?> select, @Nullable EpochSupplier min, EpochSupplier max)
    {
        return withSufficientEpochsAtLeast(select,
                                          min == null ? Long.MIN_VALUE : min.epoch(),
                                          max == null ? Long.MAX_VALUE : max.epoch(),
                                          prev -> prev.retired);
    }

    private Topologies withSufficientEpochsAtLeast(Unseekables<?> select, long minEpoch, long maxEpoch, Function<EpochState, Ranges> isSufficientFor)
    {
        return atLeast(select, minEpoch, maxEpoch, isSufficientFor, collector);
    }

    private <C, K extends Routables<?>, T> T atLeast(K select, long minEpoch, long maxEpoch, Function<EpochState, Ranges> isSufficientFor,
                                                     Collectors<C, K, T> collectors) throws IllegalArgumentException
    {
        Invariants.requireArgument(minEpoch <= maxEpoch);
        Epochs snapshot = epochs;
        if (maxEpoch < snapshot.minEpoch())
            throw new TopologyRetiredException(maxEpoch, snapshot.minEpoch());

        if (maxEpoch == Long.MAX_VALUE) maxEpoch = snapshot.currentEpoch;
        else Invariants.require(snapshot.currentEpoch >= maxEpoch, "current epoch %d < max %d", snapshot.currentEpoch, maxEpoch);

        EpochState maxEpochState = nonNull(snapshot.get(maxEpoch));
        if (minEpoch == maxEpoch && isSufficientFor.apply(maxEpochState).containsAll(select))
            return collectors.one(maxEpochState, select, false);

        int i = (int)(snapshot.currentEpoch - maxEpoch);
        int maxi = (int)(Math.min(1 + snapshot.currentEpoch - minEpoch, snapshot.epochs.length));
        C collector = collectors.allocate(maxi - i);

        // Previous logic would exclude synced ranges, but this was removed as that makes min epoch selection harder.
        // An issue was found where a range was removed from a replica and min selection picked the epoch before that,
        // which caused a node to get included in the txn that actually lost the range
        // See CASSANDRA-18804
        while (i < maxi)
        {
            EpochState epochState = snapshot.epochs[i++];
            collector = collectors.update(collector, epochState, select, false);
            select = (K)select.without(epochState.addedRanges);
        }

        if (select.isEmpty())
            return collectors.multi(collector);

        if (i == snapshot.epochs.length)
        {
            // now we GC epochs, we cannot rely on addedRanges to remove all ranges, so we also remove the ranges found in the earliest epoch we have
            select = (K)select.without(snapshot.epochs[snapshot.epochs.length - 1].global.ranges);
            if (!select.isEmpty())
                throw Invariants.illegalArgument("Ranges %s could not be found", select);
            return collectors.multi(collector);
        }

        // remaining is updated based off isSufficientFor, but select is not
        Routables<?> remaining = select;

        // include any additional epochs to reach sufficiency
        EpochState prev = snapshot.epochs[maxi - 1];
        do
        {
            remaining = remaining.without(isSufficientFor.apply(prev));
            Routables<?> prevSelect = select;
            select = (K)select.without(prev.addedRanges);
            if (prevSelect != select) // perf optimization; if select wasn't changed (it does not intersect addedRanges), then remaining won't
                remaining = remaining.without(prev.addedRanges);
            if (remaining.isEmpty())
                return collectors.multi(collector);

            EpochState next = snapshot.epochs[i++];
            collector = collectors.update(collector, next, select, false);
            prev = next;
        } while (i < snapshot.epochs.length);
        // need to remove sufficient / added else remaining may not be empty when the final matches are the last epoch
        remaining = remaining.without(isSufficientFor.apply(prev));
        remaining = remaining.without(prev.addedRanges);
        // TODO (desired): propagate addedRanges to the earliest epoch we retain for consistency
        // now we GC epochs, we cannot rely on addedRanges to remove all ranges, so we also remove the ranges found in the earliest epoch we have
        remaining = remaining.without(snapshot.epochs[snapshot.epochs.length - 1].global.ranges);
        if (!remaining.isEmpty())
            Invariants.illegalArgument("Ranges %s could not be found", remaining);

        return collectors.multi(collector);
    }

    private Topologies withSufficientEpochsAtMost(Routables<?> select, long minEpoch, long maxEpoch, Function<EpochState, Ranges> isSufficientFor)
    {
        return atMost(select, minEpoch, maxEpoch, isSufficientFor, collector);
    }

    private <C, K extends Routables<?>, T> T atMost(K select, long minEpoch, long maxEpoch, Function<EpochState, Ranges> isSufficientFor,
                                                    Collectors<C, K, T> collectors)
    {
        Invariants.requireArgument(minEpoch <= maxEpoch);
        Epochs snapshot = epochs;

        minEpoch = Math.max(snapshot.minEpoch(), minEpoch);
        maxEpoch = validateMax(maxEpoch, snapshot);

        EpochState cur = nonNull(snapshot.get(maxEpoch));
        if (minEpoch == maxEpoch)
        {
            // TODO (required): why are we testing isSufficientFor here? minEpoch == maxEpoch, we should always return.
            if (isSufficientFor.apply(cur).containsAll(select))
                return collectors.one(cur, select, true);
        }

        int i = (int)(snapshot.currentEpoch - maxEpoch);
        int maxi = (int)(Math.min(1 + snapshot.currentEpoch - minEpoch, snapshot.epochs.length));
        C collector = collectors.allocate(maxi - i);

        while (!select.isEmpty())
        {
            collector = collectors.update(collector, cur, select, true);
            select = (K)select.without(cur.addedRanges)
                              .without(isSufficientFor.apply(cur));

            if (++i == maxi)
                break;

            cur = snapshot.epochs[i];
        }

        return collectors.multi(collector);
    }

    private <C, K extends Routables<?>, T> T extra(K select, long minEpoch, long maxEpoch, Function<EpochState, Ranges> remove,
                                                   Collectors<C, K, T> collectors)
    {
        Invariants.requireArgument(minEpoch <= maxEpoch);
        Epochs snapshot = epochs;

        minEpoch = Math.max(snapshot.minEpoch(), minEpoch);
        maxEpoch = Math.min(maxEpoch, snapshot.currentEpoch);
        if (maxEpoch <= minEpoch)
            return collectors.none();

        EpochState cur = nonNull(snapshot.get(maxEpoch));
        select = (K) select.without(remove.apply(cur));
        if (select.isEmpty())
            return collectors.none();

        int i = (int)(snapshot.currentEpoch - maxEpoch);
        int maxi = (int)(Math.min(1 + snapshot.currentEpoch - minEpoch, snapshot.epochs.length));
        C collector = collectors.allocate(maxi - i);

        while (!select.isEmpty())
        {
            collector = collectors.update(collector, cur, select, true);
            select = (K)select.without(cur.addedRanges);

            if (++i == maxi)
                break;

            cur = snapshot.epochs[i];
            select = (K)select.without(remove.apply(cur));
        }

        return collectors.multi(collector);
    }

    private static long validateMax(long maxEpoch, Epochs snapshot)
    {
        if (maxEpoch == Long.MAX_VALUE)
            return snapshot.currentEpoch;

        Invariants.require(snapshot.currentEpoch >= maxEpoch, "current epoch %d < provided max %d", snapshot.currentEpoch, maxEpoch);
        if (maxEpoch < snapshot.minEpoch())
            throw new TopologyRetiredException(maxEpoch, snapshot.minEpoch());
        return maxEpoch;
    }

    public Topologies preciseEpochs(Unseekables<?> select, long minEpoch, long maxEpoch, SelectNodeOwnership selectNodeOwnership)
    {
        return preciseEpochs(select, minEpoch, maxEpoch, selectNodeOwnership, Topology::select);
    }

    public Topologies preciseEpochsIfExists(Unseekables<?> select, long minEpoch, long maxEpoch, SelectNodeOwnership selectNodeOwnership)
    {
        return preciseEpochs(select, minEpoch, maxEpoch, selectNodeOwnership, Topology::selectIfExists);
    }

    public interface SelectFunction
    {
        Topology apply(Topology topology, Unseekables<?> select, SelectNodeOwnership selectNodeOwnership);
    }

    public Topologies preciseEpochs(Unseekables<?> select, long minEpoch, long maxEpoch, SelectNodeOwnership selectNodeOwnership, SelectFunction selectFunction)
    {
        Epochs snapshot = epochs;

        // TODO (expected): we should disambiguate minEpoch we can bump (i.e. historical epochs) and those we cannot (i.e. txnId.epoch())
        minEpoch = Math.max(snapshot.minEpoch(), minEpoch);
        maxEpoch = validateMax(maxEpoch, snapshot);
        EpochState maxState = snapshot.get(maxEpoch);

        if (maxState == null)
            throw new TopologyRetiredException(maxEpoch, snapshot.minEpoch());

        if (minEpoch == maxEpoch)
            return new Single(sorter, selectFunction.apply(snapshot.get(minEpoch).global, select, selectNodeOwnership));

        int count = (int)(1 + maxEpoch - minEpoch);
        Topologies.Builder topologies = new Topologies.Builder(count);
        for (int i = count - 1 ; i >= 0 ; --i)
        {
            EpochState epochState = snapshot.get(minEpoch + i);
            topologies.add(selectFunction.apply(epochState.global, select, selectNodeOwnership));
            select = select.without(epochState.addedRanges);
        }
        Invariants.require(!topologies.isEmpty(), "Unable to find an epoch that contained %s", select);

        return topologies.build(sorter);
    }

    public Topologies forEpoch(Unseekables<?> select, long epoch, SelectNodeOwnership selectNodeOwnership)
    {
        Epochs snapshot = epochs;
        EpochState state = snapshot.get(epoch);
        if (state == null)
            throw new TopologyRetiredException(epoch, snapshot.minEpoch());
        return new Single(sorter, state.global.select(select, selectNodeOwnership));
    }

    public boolean hasReplicationMaybeChanged(Unseekables<?> select, long sinceEpoch)
    {
        Epochs snapshot = epochs;
        if (snapshot.minEpoch() > sinceEpoch)
            return true;

        return atLeast(select, sinceEpoch, Long.MAX_VALUE, ignore -> Ranges.EMPTY, HasChangedReplication.INSTANCE);
    }

    public Topologies forEpochAtLeast(Unseekables<?> select, long epoch, SelectNodeOwnership selectNodeOwnership)
    {
        Epochs snapshot = this.epochs;
        EpochState state = snapshot.get(epoch);
        if (state == null)
        {
            Invariants.require(snapshot.currentEpoch >= epoch, "current epoch %d < provided max %d", snapshot.currentEpoch, epoch);
            state = snapshot.get(snapshot.minEpoch());
        }
        return new Single(sorter, state.global.select(select, selectNodeOwnership));
    }

    public Shard forEpochIfKnown(RoutableKey key, long epoch)
    {
        EpochState epochState = epochs.get(epoch);
        if (epochState == null)
            return null;
        return epochState.global().forKey(key);
    }

    public Shard forEpoch(RoutableKey key, long epoch)
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

    public boolean hasAtLeastEpoch(long epoch)
    {
        return epochs.currentEpoch >= epoch;
    }

    public Topology localForEpoch(long epoch)
    {
        if (epoch < minEpoch())
            throw new TopologyRetiredException(epoch, minEpoch());
        EpochState epochState = epochs.get(epoch);
        if (epochState == null)
            throw illegalState("Unknown epoch " + epoch);
        return epochState.local();
    }

    public Ranges localRangesForEpoch(long epoch)
    {
        if (epoch < minEpoch())
            throw new TopologyRetiredException(epoch, minEpoch());
        return epochs.get(epoch).local().rangesForNode(self);
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

    public Topology maybeGlobalForEpoch(long epoch)
    {
        EpochState epochState = epochs.get(epoch);
        if (epochState == null)
            return null;
        return epochState.global();
    }

    static class TopologiesCollectors implements Collectors<Topologies.Builder, Routables<?>, Topologies>
    {
        final TopologySorter.Supplier sorter;
        final SelectNodeOwnership selectNodeOwnership;

        TopologiesCollectors(TopologySorter.Supplier sorter, SelectNodeOwnership selectNodeOwnership)
        {
            this.sorter = sorter;
            this.selectNodeOwnership = selectNodeOwnership;
        }

        @Override
        public Topologies.Builder update(Topologies.Builder collector, EpochState epoch, Routables<?> select, boolean permitMissing)
        {
            collector.add(epoch.global.select(select, permitMissing, selectNodeOwnership));
            return collector;
        }

        @Override
        public Topologies one(EpochState epoch, Routables<?> unseekables, boolean permitMissing)
        {
            return new Topologies.Single(sorter, epoch.global.select(unseekables, permitMissing, selectNodeOwnership));
        }

        @Override
        public Topologies multi(Topologies.Builder builder)
        {
            return builder.build(sorter);
        }

        @Override
        public Topologies.Builder allocate(int count)
        {
            return new Topologies.Builder(count);
        }
    }

    static class BestFastPath implements Collectors<FastPath, Routables<?>, FastPath>, IndexedBiFunction<Shard, Boolean, Boolean>
    {
        final Id self;

        BestFastPath(Id self)
        {
            this.self = self;
        }

        @Override
        public FastPath update(FastPath collector, EpochState epoch, Routables<?> select, boolean permitMissing)
        {
            return merge(collector, one(epoch, select, permitMissing));
        }

        @Override
        public FastPath one(EpochState epoch, Routables<?> routables, boolean permitMissing)
        {
            if (!epoch.local.ranges.containsAll(routables) || !epoch.local.foldl(routables, this, true))
                return Unoptimised;

            return epoch.local.foldl(routables, (s, v, i) -> merge(v, s.bestFastPath()), null);
        }

        @Override
        public FastPath multi(FastPath result)
        {
            return result;
        }

        @Override
        public FastPath allocate(int count)
        {
            return null;
        }

        private static FastPath merge(FastPath a, FastPath b)
        {
            if (a == null) return b;
            if (a == Unoptimised || b == Unoptimised) return Unoptimised;
            if (a == PrivilegedCoordinatorWithDeps || b == PrivilegedCoordinatorWithDeps) return PrivilegedCoordinatorWithDeps;
            return PrivilegedCoordinatorWithoutDeps;
        }

        @Override
        public Boolean apply(Shard shard, Boolean prev, int index)
        {
            return prev && shard.isInFastPath(self);
        }
    }

    static class SupportsPrivilegedFastPath implements Collectors<Boolean, Routables<?>, Boolean>, IndexedBiFunction<Shard, Boolean, Boolean>
    {
        final Id self;

        SupportsPrivilegedFastPath(Id self)
        {
            this.self = self;
        }

        @Override
        public Boolean update(Boolean collector, EpochState epoch, Routables<?> select, boolean permitMissing)
        {
            return collector && one(epoch, select, permitMissing);
        }

        @Override
        public Boolean one(EpochState epoch, Routables<?> routables, boolean permitMissing)
        {
            return epoch.local.ranges.containsAll(routables) && epoch.local.foldl(routables, this, true);
        }

        @Override
        public Boolean multi(Boolean result)
        {
            return result;
        }

        @Override
        public Boolean allocate(int count)
        {
            return true;
        }

        @Override
        public Boolean apply(Shard shard, Boolean prev, int index)
        {
            return prev && shard.isInFastPath(self);
        }
    }

    static class UnsyncedSelector<K extends Participants<?>> implements TopologyManager.Collectors<K, K, K>
    {
        static final UnsyncedSelector INSTANCE = new UnsyncedSelector();

        @Override
        public K allocate(int size)
        {
            return null;
        }

        @Override
        public K none()
        {
            return null;
        }

        @Override
        public K multi(K collector)
        {
            return collector;
        }

        @Override
        public K one(TopologyManager.EpochState epoch, K select, boolean permitMissing)
        {
            return (K) select.without(epoch.synced);
        }

        @Override
        public K update(K collector, TopologyManager.EpochState epoch, K select, boolean permitMissing)
        {
            select = (K)select.without(epoch.synced);
            return collector == null ? select : (K)collector.with((Participants) select);
        }
    }

    static class ReplicationChangeTracker
    {
        int rf = -1;
        boolean hasChanged;
    }
    static class HasChangedReplication implements TopologyManager.Collectors<ReplicationChangeTracker, Unseekables<?>, Boolean>
    {
        static final HasChangedReplication INSTANCE = new HasChangedReplication();

        @Override
        public ReplicationChangeTracker allocate(int size)
        {
            return new ReplicationChangeTracker();
        }

        @Override
        public Boolean none()
        {
            return false;
        }

        @Override
        public Boolean multi(ReplicationChangeTracker collector)
        {
            return collector.hasChanged;
        }

        @Override
        public Boolean one(TopologyManager.EpochState epoch, Unseekables<?> select, boolean permitMissing)
        {
            return false;
        }

        @Override
        public ReplicationChangeTracker update(ReplicationChangeTracker collector, TopologyManager.EpochState epoch, Unseekables<?> select, boolean permitMissing)
        {
            epoch.global.foldl(select, (shard, c, i1) -> {
                if (c.rf < 0) c.rf = shard.rf;
                else c.hasChanged |= c.rf != shard.rf;
                return c;
            }, collector);
            return collector;
        }
    }

    public interface Collectors<C, K, T>
    {
        C allocate(int size);
        C update(C collector, EpochState epoch, K select, boolean permitMissing);
        default T none() { throw new UnsupportedOperationException(); }
        T one(EpochState epoch, K select, boolean permitMissing);
        T multi(C collector);
    }

    public static class TopologyRetiredException extends RuntimeException
    {
        public TopologyRetiredException(long epoch, long minEpoch)
        {
            super(String.format("Topology %s retired. Min topology %d", epoch, minEpoch));
        }

        public TopologyRetiredException(String message, TopologyRetiredException t)
        {
            super(message, t);
        }
    }
}
