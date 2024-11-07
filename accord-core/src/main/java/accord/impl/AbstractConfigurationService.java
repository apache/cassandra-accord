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

package accord.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.ConfigurationService;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

public abstract class AbstractConfigurationService<EpochState extends AbstractConfigurationService.AbstractEpochState,
                                                   EpochHistory extends AbstractConfigurationService.AbstractEpochHistory<EpochState>>
                      implements ConfigurationService
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractConfigurationService.class);

    protected final Node.Id localId;

    protected final EpochHistory epochs = createEpochHistory();

    protected final List<Listener> listeners = new CopyOnWriteArrayList<>();

    public abstract static class AbstractEpochState
    {
        protected final long epoch;
        protected final AsyncResult.Settable<Topology> received = AsyncResults.settable();
        protected final AsyncResult.Settable<Void> acknowledged = AsyncResults.settable();
        @GuardedBy("AbstractEpochHistory.this")
        protected AsyncResult<Void> reads = null;
        @GuardedBy("AbstractEpochHistory.this")
        protected Topology topology = null;

        public AbstractEpochState(long epoch)
        {
            this.epoch = epoch;
        }

        public long epoch()
        {
            return epoch;
        }

        @Override
        public String toString()
        {
            return "EpochState{" + epoch + '}';
        }
    }

    /**
     * Thread safety is managed by a synchronized lock on this object, and extending classes can use the same lock when needed.
     *
     * There is a special case when the last recieved/acknowleged epochs are needed, they can be accessed without a lock
     * and provide a happens-before relationship (if lastReceived=42, epoch 42 exists and is set up)
     */
    @VisibleForTesting
    public abstract static class AbstractEpochHistory<EpochState extends AbstractEpochState>
    {
        // TODO (low priority): move pendingEpochs / FetchTopology into here?
        private List<EpochState> epochs = new ArrayList<>();

        /**
         * {@link #lastReceived} and {@link #lastAcknowledged} help define the visibility by making sure that the update is the very last thing done in their respected logic.
         * This means that the read of these fields can be done without a lock and can be used to tell the caller that the given epoch has reached a given step.
         *
         * The reason that these fields are private are to help enforce the thread safety of this class.
         */
        private volatile long lastReceived = 0;
        private volatile long lastAcknowledged = 0;

        protected abstract EpochState createEpochState(long epoch);

        public long lastReceived()
        {
            return lastReceived;
        }

        public long lastAcknowledged()
        {
            return lastAcknowledged;
        }

        public synchronized long minEpoch()
        {
            return epochs.isEmpty() ? 0L : epochs.get(0).epoch;
        }

        public synchronized long maxEpoch()
        {
            int size = epochs.size();
            return size == 0 ? 0L : epochs.get(size - 1).epoch;
        }

        @VisibleForTesting
        synchronized EpochState atIndex(int idx)
        {
            return epochs.get(idx);
        }

        @VisibleForTesting
        synchronized int size()
        {
            return epochs.size();
        }

        public boolean isEmpty()
        {
            return lastReceived == 0;
        }

        synchronized EpochState getOrCreate(long epoch)
        {
            Invariants.checkArgument(epoch > 0, "Epoch must be positive but given %d", epoch);
            if (epochs.isEmpty())
            {
                EpochState state = createEpochState(epoch);
                epochs.add(state);
                return state;
            }

            long minEpoch = minEpoch();
            if (epoch < minEpoch)
            {
                int prepend = Ints.checkedCast(minEpoch - epoch);
                List<EpochState> next = new ArrayList<>(epochs.size() + prepend);
                for (long addEpoch=epoch; addEpoch<minEpoch; addEpoch++)
                    next.add(createEpochState(addEpoch));
                next.addAll(epochs);
                epochs = next;
                minEpoch = minEpoch();
                Invariants.checkState(minEpoch == epoch, "Epoch %d != %d", epoch, minEpoch);
            }
            long maxEpoch = maxEpoch();
            int idx = Ints.checkedCast(epoch - minEpoch);

            // add any missing epochs
            for (long addEpoch = maxEpoch + 1; addEpoch <= epoch; addEpoch++)
                epochs.add(createEpochState(addEpoch));

            return epochs.get(idx);
        }

        public void receive(Topology topology)
        {
            EpochState state;
            synchronized (this)
            {
                long epoch = topology.epoch();
                logger.debug("Receiving epoch {}", epoch);
                Invariants.checkState(lastReceived == epoch - 1 || epoch == 0 || lastReceived == 0,
                                      "Epoch %d != %d + 1", epoch, lastReceived);
                state = getOrCreate(epoch);
                state.topology = topology;
                lastReceived = epoch;
            }
            // avoid resolving the future while holding the lock, as the callbacks get called in-line
            state.received.setSuccess(topology);
        }

        AsyncResult<Topology> receiveFuture(long epoch)
        {
            return getOrCreate(epoch).received;
        }

        Topology topologyFor(long epoch)
        {
            return getOrCreate(epoch).topology;
        }

        Topology topologyForLastReceived()
        {
            return getOrCreate(lastReceived).topology;
        }

        public void acknowledge(EpochReady ready)
        {
            EpochState state;
            synchronized (this)
            {
                long epoch = ready.epoch;
                logger.debug("Acknowledging epoch {}", epoch);
                Invariants.checkState(lastAcknowledged == epoch - 1 || epoch == 0 || lastAcknowledged == 0,
                                      "Epoch %d != %d + 1", epoch, lastAcknowledged);
                state = getOrCreate(epoch);
                Invariants.checkState(state.reads == null, "Reads result was already set for epoch", epoch);
                state.reads = ready.reads;
                lastAcknowledged = epoch;
            }
            // avoid resolving the future while holding the lock, as the callbacks get called in-line
            state.acknowledged.setSuccess(null);
        }

        public AsyncResult<Void> acknowledgeFuture(long epoch)
        {
            return getOrCreate(epoch).acknowledged;
        }

        synchronized void truncateUntil(long epoch)
        {
            Invariants.checkArgument(epoch <= maxEpoch(), "epoch %d > %d", epoch, maxEpoch());
            long minEpoch = minEpoch();
            int toTrim = Ints.checkedCast(epoch - minEpoch);
            if (toTrim <= 0)
                return;

            epochs = new ArrayList<>(epochs.subList(toTrim, epochs.size()));
        }
    }

    public AbstractConfigurationService(Node.Id localId)
    {
        this.localId = localId;
    }

    protected abstract EpochHistory createEpochHistory();

    protected EpochState getOrCreateEpochState(long epoch)
    {
        return epochs.getOrCreate(epoch);
    }

    @Override
    public void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    public boolean isEmpty()
    {
        return epochs.isEmpty();
    }

    @Override
    public Topology currentTopology()
    {
        return epochs.topologyForLastReceived();
    }

    @Override
    public Topology getTopologyForEpoch(long epoch)
    {
        return epochs.topologyFor(epoch);
    }

    protected abstract void fetchTopologyInternal(long epoch);

    @GuardedBy("this")
    private long maxRequestedEpoch;
    @Override
    public void fetchTopologyForEpoch(long epoch)
    {
        synchronized (this)
        {
            if (epoch <= maxRequestedEpoch)
                return;

            maxRequestedEpoch = epoch;
        }
        fetchTopologyInternal(epoch);
    }

    protected abstract void localSyncComplete(Topology topology, boolean startSync);

    @Override
    public void acknowledgeEpoch(EpochReady ready, boolean startSync)
    {
        ready.metadata.addCallback(() -> epochs.acknowledge(ready));
        ready.fastPath.addCallback(() ->  localSyncComplete(epochs.getOrCreate(ready.epoch).topology, startSync));
    }

    protected void topologyUpdatePreListenerNotify(Topology topology) {}
    protected void topologyUpdatePostListenerNotify(Topology topology) {}

    public void reportTopology(Topology topology, boolean isLoad, boolean startSync)
    {
        long lastReceived = epochs.lastReceived();
        if (topology.epoch() <= lastReceived)
            return;

        if (lastReceived > 0 && topology.epoch() > lastReceived + 1)
        {
            logger.debug("Epoch {} received; waiting to receive {} before reporting", topology.epoch(), lastReceived + 1);
            fetchTopologyForEpoch(lastReceived + 1);
            epochs.receiveFuture(lastReceived + 1).addCallback(() -> reportTopology(topology, startSync, isLoad));
            return;
        }

        long lastAcked = epochs.lastAcknowledged();
        if (lastAcked == 0 && lastReceived > 0)
        {
            logger.debug("Epoch {} received; waiting for {} to ack before reporting", topology.epoch(), epochs.minEpoch());
            epochs.acknowledgeFuture(epochs.minEpoch()).addCallback(() -> reportTopology(topology, startSync, isLoad));
            return;
        }
        if (lastAcked > 0 && topology.epoch() > lastAcked + 1)
        {
            logger.debug("Epoch {} received; waiting for {} to ack before reporting", topology.epoch(), lastAcked + 1);
            epochs.acknowledgeFuture(lastAcked + 1).addCallback(() -> reportTopology(topology, startSync, isLoad));
            return;
        }

        epochs.receive(topology);
        topologyUpdatePreListenerNotify(topology);
        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology, isLoad, startSync);
        topologyUpdatePostListenerNotify(topology);
    }

    public void reportTopology(Topology topology)
    {
        reportTopology(topology, false, true);
    }

    protected void receiveRemoteSyncCompletePreListenerNotify(Node.Id node, long epoch) {}

    public void receiveRemoteSyncComplete(Node.Id node, long epoch)
    {
        receiveRemoteSyncCompletePreListenerNotify(node, epoch);
        for (Listener listener : listeners)
            listener.onRemoteSyncComplete(node, epoch);
    }

    public void receiveClosed(Ranges ranges, long epoch)
    {
        for (Listener listener : listeners)
            listener.onEpochClosed(ranges, epoch);
    }

    public void receiveRedundant(Ranges ranges, long epoch)
    {
        for (Listener listener : listeners)
            listener.onEpochRedundant(ranges, epoch);
    }

    protected void truncateTopologiesPreListenerNotify(long epoch) {}
    protected void truncateTopologiesPostListenerNotify(long epoch) {}

    public void truncateTopologiesUntil(long epoch)
    {
        truncateTopologiesPreListenerNotify(epoch);
        for (Listener listener : listeners)
            listener.truncateTopologyUntil(epoch);
        truncateTopologiesPostListenerNotify(epoch);
        epochs.truncateUntil(epoch);
    }

    public AsyncChain<Void> epochReady(long epoch)
    {
        EpochState state = epochs.getOrCreate(epoch);
        if (state.reads != null)
            return state.reads;

        return state.acknowledged.flatMap(r -> state.reads);
    }

    public abstract static class Minimal extends AbstractConfigurationService<Minimal.EpochState, Minimal.EpochHistory>
    {
        static class EpochState extends AbstractEpochState
        {
            public EpochState(long epoch)
            {
                super(epoch);
            }
        }

        static class EpochHistory extends AbstractEpochHistory<EpochState>
        {
            @Override
            protected EpochState createEpochState(long epoch)
            {
                return new EpochState(epoch);
            }
        }

        public Minimal(Node.Id node)
        {
            super(node);
        }

        @Override
        protected EpochHistory createEpochHistory()
        {
            return new EpochHistory();
        }
    }
}
