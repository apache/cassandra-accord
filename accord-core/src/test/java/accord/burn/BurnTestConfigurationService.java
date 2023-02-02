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

package accord.burn;

import accord.api.ConfigurationService;
import accord.api.MessageSink;
import accord.api.TestableConfigurationService;
import accord.local.Node;
import accord.messages.*;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

public class BurnTestConfigurationService implements TestableConfigurationService
{
    private static final Logger logger = LoggerFactory.getLogger(BurnTestConfigurationService.class);

    private final Node.Id node;
    private final MessageSink messageSink;
    private final Function<Node.Id, Node> lookup;
    private final Supplier<Random> randomSupplier;
    private final Map<Long, FetchTopology> pendingEpochs = new HashMap<>();

    private final EpochHistory epochs = new EpochHistory();
    private final List<ConfigurationService.Listener> listeners = new ArrayList<>();
    private final TopologyUpdates topologyUpdates;

    private static class EpochState
    {
        private final long epoch;
        private final AsyncResult.Settable<Topology> received = AsyncResults.settable();
        private final AsyncResult.Settable<Void> acknowledged = AsyncResults.settable();
        private final AsyncResult.Settable<Void> synced = AsyncResults.settable();

        private Topology topology = null;

        public EpochState(long epoch)
        {
            this.epoch = epoch;
        }
    }

    private static class EpochHistory
    {
        // TODO (low priority): move pendingEpochs / FetchTopology into here?
        private final List<EpochState> epochs = new ArrayList<>();

        private long lastReceived = 0;
        private long lastAcknowledged = 0;
        private long lastSyncd = 0;

        private EpochState get(long epoch)
        {
            for (long addEpoch = epochs.size() - 1; addEpoch <= epoch; addEpoch++)
                epochs.add(new EpochState(addEpoch));
            return epochs.get((int) epoch);
        }

        EpochHistory receive(Topology topology)
        {
            long epoch = topology.epoch();
            Invariants.checkState(epoch == 0 || lastReceived == epoch - 1);
            lastReceived = epoch;
            EpochState state = get(epoch);
            state.topology = topology;
            state.received.setSuccess(topology);
            return this;
        }

        AsyncResult<Topology> receiveFuture(long epoch)
        {
            return get(epoch).received;
        }

        Topology topologyFor(long epoch)
        {
            return get(epoch).topology;
        }

        EpochHistory acknowledge(long epoch)
        {
            Invariants.checkState(epoch == 0 || lastAcknowledged == epoch - 1);
            lastAcknowledged = epoch;
            get(epoch).acknowledged.setSuccess(null);
            return this;
        }

        AsyncResult<Void> acknowledgeFuture(long epoch)
        {
            return get(epoch).acknowledged;
        }

        EpochHistory syncComplete(long epoch)
        {
            Invariants.checkState(epoch == 0 || lastSyncd == epoch - 1);
            EpochState state = get(epoch);
            Invariants.checkState(state.received.isDone());
            Invariants.checkState(state.acknowledged.isDone());
            lastSyncd = epoch;
            get(epoch).synced.setSuccess(null);
            return this;
        }
    }

    public BurnTestConfigurationService(Node.Id node, MessageSink messageSink, Supplier<Random> randomSupplier, Topology topology, Function<Node.Id, Node> lookup, TopologyUpdates topologyUpdates)
    {
        this.node = node;
        this.messageSink = messageSink;
        this.randomSupplier = randomSupplier;
        this.lookup = lookup;
        this.topologyUpdates = topologyUpdates;
        epochs.receive(Topology.EMPTY).acknowledge(0).syncComplete(0);
        epochs.receive(topology).acknowledge(1).syncComplete(1);
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    @Override
    public synchronized Topology currentTopology()
    {
        return epochs.topologyFor(epochs.lastReceived);
    }

    @Override
    public synchronized Topology getTopologyForEpoch(long epoch)
    {
        return epochs.topologyFor(epoch);
    }

    private static class FetchTopologyRequest implements Request
    {
        private final long epoch;

        public FetchTopologyRequest(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public void process(Node on, Node.Id from, ReplyContext replyContext)
        {
            Topology topology = on.configService().getTopologyForEpoch(epoch);
            on.reply(from, replyContext, new FetchTopologyReply(topology));
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "FetchTopologyRequest{" + epoch + '}';
        }
    }

    private static class FetchTopologyReply implements Reply
    {
        public final Topology topology;

        public FetchTopologyReply(Topology topology)
        {
            this.topology = topology;
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            String epoch = topology == null ? "null" : Long.toString(topology.epoch());
            return "FetchTopologyReply{" + epoch + '}';
        }
    }

    private class FetchTopology extends AsyncResults.Settable<Void> implements Callback<FetchTopologyReply>
    {
        private final FetchTopologyRequest request;
        private final List<Node.Id> candidates;

        public FetchTopology(long epoch)
        {
            this.request = new FetchTopologyRequest(epoch);
            this.candidates = new ArrayList<>();
            sendNext();
        }

        synchronized void sendNext()
        {
            if (candidates.isEmpty())
            {
                candidates.addAll(currentTopology().nodes());
                candidates.remove(node);
            }
            int idx = randomSupplier.get().nextInt(candidates.size());
            Node.Id node = candidates.remove(idx);
            messageSink.send(node, request, this);
        }

        @Override
        public void onSuccess(Node.Id from, FetchTopologyReply reply)
        {
            if (reply.topology != null)
                reportTopology(reply.topology);
            else
                sendNext();
        }

        @Override
        public synchronized void onFailure(Node.Id from, Throwable failure)
        {
            sendNext();
        }

        @Override
        public void onCallbackFailure(Node.Id from, Throwable failure)
        {
            tryFailure(failure);
        }
    }

    @Override
    public synchronized void fetchTopologyForEpoch(long epoch)
    {
        if (epoch <= epochs.lastReceived)
            return;

        pendingEpochs.computeIfAbsent(epoch, FetchTopology::new);
    }

    @Override
    public synchronized void acknowledgeEpoch(long epoch)
    {
        epochs.acknowledge(epoch);
        Topology topology = getTopologyForEpoch(epoch);
        Node originator = lookup.apply(node);
        topologyUpdates.syncEpoch(originator, epoch - 1, topology.nodes());
    }

    @Override
    public synchronized void reportTopology(Topology topology)
    {
        long lastReceived = epochs.lastReceived;
        if (topology.epoch() <= lastReceived)
            return;

        if (topology.epoch() > lastReceived + 1)
        {
            fetchTopologyForEpoch(lastReceived + 1);
            epochs.receiveFuture(lastReceived + 1).addListener(() -> reportTopology(topology));
            return;
        }

        long lastAcked = epochs.lastAcknowledged;
        if (topology.epoch() > lastAcked + 1)
        {
            epochs.acknowledgeFuture(lastAcked + 1).addListener(() -> reportTopology(topology));
            return;
        }
        logger.trace("Epoch {} received by {}", topology.epoch(), node);

        epochs.receive(topology);
        for (Listener listener : listeners)
            listener.onTopologyUpdate(topology);

        FetchTopology fetch = pendingEpochs.remove(topology.epoch());
        if (fetch == null)
            return;

        fetch.setSuccess(null);
    }
}
