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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.NetworkFilter;
import accord.api.MessageSink;
import accord.api.LocalConfig;
import accord.coordinate.CoordinationAdapter;
import accord.impl.DefaultRequestTimeouts;
import accord.impl.InMemoryCommandStores;
import accord.impl.IntKey;
import accord.impl.DefaultLocalListeners;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.impl.DefaultRemoteListeners;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.NodeTimeService;
import accord.local.ShardDistributor;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;
import accord.messages.SafeCallback;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.topology.TopologyUtils;
import accord.utils.DefaultRandom;
import accord.utils.EpochFunction;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.ThreadPoolScheduler;

import static accord.Utils.id;
import static accord.Utils.idList;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.async.AsyncChains.awaitUninterruptibly;

public class MockCluster implements Network, AutoCloseable, Iterable<Node>
{
    private static final Logger logger = LoggerFactory.getLogger(MockCluster.class);

    private final RandomSource random;
    private final Config config;
    private final LongSupplier nowSupplier;
    private final Map<Id, Node> nodes = new ConcurrentHashMap<>();
    private final BiFunction<Id, Network, MessageSink> messageSinkFactory;
    private int nextNodeId = 1;
    public NetworkFilter networkFilter = new NetworkFilter();

    private long nextMessageId = 0;
    Map<Long, SafeCallback> callbacks = new ConcurrentHashMap<>();
    private final EpochFunction<MockConfigurationService> onFetchTopology;

    private MockCluster(Builder builder)
    {
        this.config = new Config(builder);
        this.random = new DefaultRandom(config.seed);
        this.nowSupplier = builder.nowSupplier;
        this.messageSinkFactory = builder.messageSinkFactory;
        this.onFetchTopology = builder.onFetchTopology;

        init(builder.topology);
    }

    @Override
    public Iterator<Node> iterator()
    {
        return nodes.values().iterator();
    }

    @Override
    public void close()
    {
        nodes.values().forEach(Node::shutdown);
    }

    private synchronized Id nextNodeId()
    {
        return id(nextNodeId++);
    }

    private synchronized long nextMessageId()
    {
        return nextMessageId++;
    }

    private Node createNode(Id id, Topology topology)
    {
        MockStore store = new MockStore();
        MessageSink messageSink = messageSinkFactory.apply(id, this);
        MockConfigurationService configurationService = new MockConfigurationService(messageSink, onFetchTopology, topology);
        LocalConfig localConfig = LocalConfig.DEFAULT;
        Node node = new Node(id,
                             messageSink,
                             configurationService,
                             nowSupplier,
                             NodeTimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MILLISECONDS, nowSupplier),
                             () -> store,
                             new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter()),
                             new TestAgent(),
                             random.fork(),
                             new ThreadPoolScheduler(),
                             SizeOfIntersectionSorter.SUPPLIER,
                             DefaultRemoteListeners::new,
                             DefaultRequestTimeouts::new,
                             DefaultProgressLogs::new,
                             DefaultLocalListeners.Factory::new,
                             InMemoryCommandStores.SingleThread::new,
                             new CoordinationAdapter.DefaultFactory(),
                             localConfig);
        awaitUninterruptibly(node.unsafeStart());
        node.onTopologyUpdate(topology, true);
        return node;
    }

    private void init(Topology topology)
    {
        List<Id> ids = new ArrayList<>(config.initialNodes);
        for (int i=0; i<config.initialNodes; i++)
        {
            Id nextId = nextNodeId();
            ids.add(nextId);
        }
        if (topology == null)
        {
            Ranges ranges = TopologyUtils.initialRanges(config.initialNodes, config.maxKey);
            topology = TopologyUtils.initialTopology(ids, ranges, config.replication);
        }
        for (int i=0; i<config.initialNodes; i++)
        {
            Id id = ids.get(i);
            Node node = createNode(id, topology);
            nodes.put(id, node);
        }
    }

    @Override
    public void send(Id from, Id to, Request request, AgentExecutor executor, Callback callback)
    {
        Node node = nodes.get(to);
        if (node == null)
        {
            logger.info("dropping message to unknown node {}: {} from {}", to, request, from);
            return;
        }

        if (networkFilter.shouldDiscard(from, to, request))
        {
            // TODO (desired, testing): more flexible timeouts
            if (callback != null)
                new SafeCallback(executor, callback).timeout(to);
            logger.info("discarding filtered message from {} to {}: {}", from, to, request);
            return;
        }

        long messageId = nextMessageId();
        if (callback != null)
        {
            SafeCallback sc = new SafeCallback(executor, callback);
            callbacks.put(messageId, sc);
            node.scheduler().once(() -> {
                if (callbacks.remove(messageId, sc))
                    sc.timeout(to);
                }, 2L, TimeUnit.SECONDS);
        }

        logger.info("processing message[{}] from {} to {}: {}", messageId, from, to, request);
        node.receive(request, from, Network.replyCtxFor(messageId));
    }

    @Override
    public void reply(Id from, Id replyingToNode, long replyingToMessage, Reply reply)
    {
        Node node = nodes.get(replyingToNode);
        if (node == null)
        {
            logger.info("dropping reply to unknown node {}: {} from {}", replyingToNode, reply, from);
            return;
        }

        SafeCallback sc = callbacks.remove(replyingToMessage);

        if (networkFilter.shouldDiscard(from, replyingToNode, reply))
        {
            logger.info("discarding filtered reply from {} to {}: {}", from, reply, reply);
            if (sc != null)
                sc.timeout(from);
            return;
        }

        if (sc == null)
        {
            logger.warn("Callback not found for reply from {} to {}: {} (msgid: {})", from, replyingToNode, reply, replyingToMessage);
            return;
        }

        logger.info("processing reply[{}] from {} to {}: {}", replyingToMessage, from, replyingToNode, reply);
        node.scheduler().now(() -> {
            try
            {
                if (reply instanceof Reply.FailureReply) sc.failure(from, ((Reply.FailureReply) reply).failure);
                else sc.success(from, reply);
            }
            catch (Throwable t)
            {
                sc.failure(from, t);
            }
        });
    }

    public Node get(Id id)
    {
        Node node = nodes.get(id);
        if (node == null)
            throw new NoSuchElementException("No node exists with id " + id);
        return node;
    }

    public Node get(int i)
    {
        return get(id(i));
    }

    public static MockConfigurationService configService(Node node)
    {
        return (MockConfigurationService) node.configService();
    }

    public MockConfigurationService configService(Id id)
    {
        Node node = nodes.get(id);
        if (node == null)
            throw new NoSuchElementException("No node exists with id " + id);
        return configService(node);
    }

    public MockConfigurationService configService(int i)
    {
        return configService(id(i));
    }

    public Iterable<MockConfigurationService> configServices(int... ids)
    {
        assert ids.length > 0;
        List<MockConfigurationService> result = new ArrayList<>(ids.length);
        for (int id : ids)
            result.add(configService(id));
        return result;
    }

    public List<Node> nodes(Iterable<Id> ids)
    {
        List<Node> rlist = new ArrayList<>();
        for (Id id : ids)
            rlist.add(get(id));
        return rlist;
    }

    public Iterable<Node> nodes(int... ids)
    {
        assert ids.length > 0;
        return nodes(idList(ids));
    }

    public static class Config
    {
        private final long seed;
        private final int initialNodes;
        private final int replication;
        private final int maxKey;

        private Config(MockCluster.Builder builder)
        {
            this.seed = builder.seed;
            this.initialNodes = builder.initialNodes;
            this.replication = builder.replication;
            this.maxKey = builder.maxKey;
        }
    }

    public static class Builder
    {
        private long seed = 0;
        private int initialNodes = 3;
        private int replication = 3;
        private int maxKey = 10000;
        private Topology topology = null;
        private LongSupplier nowSupplier = System::currentTimeMillis;
        private BiFunction<Id, Network, MessageSink> messageSinkFactory = SimpleMessageSink::new;
        private EpochFunction<MockConfigurationService> onFetchTopology = EpochFunction.noop();

        public Builder seed(long seed)
        {
            this.seed = seed;
            return this;
        }

        public Builder nodes(int initialNodes)
        {
            this.initialNodes = initialNodes;
            return this;
        }

        public Builder replication(int replication)
        {
            this.replication = replication;
            return this;
        }

        public Builder maxKey(int max)
        {
            this.maxKey = max;
            return this;
        }

        public Builder nowSupplier(LongSupplier supplier)
        {
            nowSupplier = supplier;
            return this;
        }

        public Builder topology(Topology topology)
        {
            this.topology = topology;
            return this;
        }

        public Builder messageSink(BiFunction<Id, Network, MessageSink> factory)
        {
            this.messageSinkFactory = factory;
            return this;
        }

        public Builder setOnFetchTopology(EpochFunction<MockConfigurationService> onFetchTopology)
        {
            this.onFetchTopology = onFetchTopology;
            return this;
        }

        public MockCluster build()
        {
            Invariants.checkArgument(initialNodes > 0);
            Invariants.checkArgument(replication > 0);
            Invariants.checkArgument(maxKey >= 0);
            return new MockCluster(this);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Clock implements LongSupplier
    {
        private final AtomicLong now;

        public Clock(long now)
        {
            this.now = new AtomicLong(now);
        }

        public long increment(long by)
        {
            return now.addAndGet(by);
        }

        public long increment()
        {
            return increment(1);
        }

        public long now()
        {
            return now.get();
        }

        @Override
        public long getAsLong()
        {
            return now();
        }

        public TxnId idForNode(long epoch, Id id)
        {
            return new TxnId(epoch, now.get(), Write, Key, id);
        }

        public TxnId idForNode(long epoch, int id)
        {
            return idForNode(epoch, new Id(id));
        }
    }
}
