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

package accord.impl.mock;

import accord.coordinate.FetchData;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.*;
import accord.messages.*;
import accord.primitives.KeyRoute;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topologies.Single;
import accord.topology.Topology;

import accord.utils.async.AsyncResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static accord.impl.InMemoryCommandStore.inMemory;
import static accord.impl.mock.MockCluster.configService;
import static accord.local.Status.Committed;
import static accord.messages.SimpleReply.Ok;
import static accord.utils.async.AsyncChains.getUninterruptibly;

public class EpochSync implements Runnable
{
    private final Logger logger = LoggerFactory.getLogger(EpochSync.class);

    private final Iterable<Node> cluster;
    private final long syncEpoch;
    private final long nextEpoch;

    public EpochSync(Iterable<Node> cluster, long syncEpoch)
    {
        this.cluster = cluster;
        this.syncEpoch = syncEpoch;
        this.nextEpoch = syncEpoch + 1;
    }

    private static class SyncCommitted implements Request
    {
        private final TxnId txnId;
        private Route<?> route;
        private final Timestamp executeAt;
        private final long epoch;

        public SyncCommitted(Command command, long epoch)
        {
            this.epoch = epoch;
            this.txnId = command.txnId();
            this.route = command.route();
            this.executeAt = command.executeAt();
        }

        void update(Command command)
        {
            route = Route.merge((Route)route, command.route());
        }

        @Override
        public void process(Node node, Node.Id from, ReplyContext replyContext)
        {
            FetchData.fetch(Committed.minKnown, node, txnId, route, executeAt, epoch, (outcome, fail) -> {
                if (fail != null) process(node, from, replyContext);
                else if (!Committed.minKnown.isSatisfiedBy(outcome)) throw new IllegalStateException();
                else node.reply(from, replyContext, Ok);
            });
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class CommandSync extends AsyncResults.Settable<Void> implements Callback<SimpleReply>
    {
        private final QuorumTracker tracker;

        public CommandSync(Node node, Route<?> route, SyncCommitted message, Topology topology)
        {
            this.tracker = new QuorumTracker(new Single(node.topology().sorter(), topology.forSelection(route)));
            node.send(tracker.nodes(), message, this);
        }

        @Override
        public synchronized void onSuccess(Node.Id from, SimpleReply reply)
        {
            tracker.recordSuccess(from);
            if (tracker.hasReachedQuorum())
                trySuccess(null);
        }

        @Override
        public synchronized void onFailure(Node.Id from, Throwable failure)
        {
            tracker.recordFailure(from);
            if (tracker.hasFailed())
                tryFailure(failure);
        }

        @Override
        public void onCallbackFailure(Node.Id from, Throwable failure)
        {
            tryFailure(failure);
        }

        public static void sync(Node node, Route<?> route, SyncCommitted message, Topology topology)
        {
            AsyncResults.getUninterruptibly(new CommandSync(node, route, message, topology));
        }
    }

    private static class SyncComplete implements Request
    {
        private final long epoch;

        public SyncComplete(long epoch)
        {
            this.epoch = epoch;
        }

        @Override
        public void process(Node on, Node.Id from, ReplyContext replyContext)
        {
            configService(on).reportSyncComplete(from, epoch);
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }
    }

    private class NodeSync implements Runnable
    {
        private final Node node;
        private final Topology syncTopology;
        private final Topology nextTopology;

        public NodeSync(Node node)
        {
            this.node = node;
            syncTopology = node.configService().getTopologyForEpoch(syncEpoch).forNode(node.id());
            nextTopology = node.configService().getTopologyForEpoch(nextEpoch);
        }

        @Override
        public void run()
        {
            Map<TxnId, SyncCommitted> syncMessages = new ConcurrentHashMap<>();
            Consumer<Command> commandConsumer = command -> syncMessages.computeIfAbsent(command.txnId(), id -> new SyncCommitted(command, syncEpoch))
                    .update(command);
            getUninterruptibly(node.commandStores().forEach(commandStore -> inMemory(commandStore).forCommittedInEpoch(syncTopology.ranges(), syncEpoch, commandConsumer)));

            for (SyncCommitted send : syncMessages.values())
                CommandSync.sync(node, send.route, send, nextTopology);

            SyncComplete syncComplete = new SyncComplete(syncEpoch);
            node.send(nextTopology.nodes(), syncComplete);
        }
    }

    private void syncNode(Node node)
    {
        new NodeSync(node).run();
    }

    @Override
    public void run()
    {
        logger.info("Beginning sync of epoch: {}", syncEpoch);
        cluster.forEach(this::syncNode);
    }

    public static void sync(MockCluster cluster, long epoch)
    {
        new EpochSync(cluster, epoch).run();
    }
}
