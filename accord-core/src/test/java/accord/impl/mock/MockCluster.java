package accord.impl.mock;

import accord.NetworkFilter;
import accord.coordinate.Timeout;
import accord.impl.TopologyUtils;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.KeyRanges;
import accord.utils.ThreadPoolScheduler;
import accord.txn.TxnId;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;
import accord.impl.TestAgent;
import accord.topology.Topology;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static accord.Utils.id;

public class MockCluster implements Network, AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(MockCluster.class);

    private final Random random;
    private final Config config;
    private final LongSupplier nowSupplier;
    private final Map<Id, Node> nodes = new ConcurrentHashMap<>();
    private int nextNodeId = 1;
    public NetworkFilter networkFilter = new NetworkFilter();

    private long nextMessageId = 0;
    Map<Long, Callback> callbacks = new ConcurrentHashMap<>();

    private MockCluster(Builder builder)
    {
        this.config = new Config(builder);
        this.random = new Random(config.seed);
        this.nowSupplier = builder.nowSupplier;

        init(builder.topology);
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
        SimpleMessageSink messageSink = new SimpleMessageSink(id, this);
        MockConfigurationService configurationService = new MockConfigurationService(messageSink, topology);
        return new Node(id,
                        messageSink,
                        configurationService,
                        new Random(random.nextLong()),
                        nowSupplier,
                        () -> store,
                        new TestAgent(),
                        new ThreadPoolScheduler(),
                        CommandStore.Factory.SINGLE_THREAD);
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
            KeyRanges ranges = TopologyUtils.initialRanges(config.initialNodes, config.maxKey);
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
    public void send(Id from, Id to, Request request, Callback callback)
    {
        Node node = nodes.get(to);
        if (node == null)
        {
            logger.info("dropping message to unknown node {}: {} from {}", to, request, from);
            return;
        }

        if (networkFilter.shouldDiscard(from, to, request))
        {
            // TODO: more flexible timeouts
            if (callback != null)
                callback.onFailure(to, new Timeout());
            logger.info("discarding filtered message from {} to {}: {}", from, to, request);
            return;
        }

        long messageId = nextMessageId();
        if (callback != null)
        {
            callbacks.put(messageId, callback);
        }

        logger.info("processing message[{}] from {} to {}: {}", messageId, from, to, request);
        node.receive(request, from, messageId);
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

        Callback callback = reply.isFinal() ? callbacks.remove(replyingToMessage)
                                            : callbacks.get(replyingToMessage);

        if (networkFilter.shouldDiscard(from, replyingToNode, reply))
        {
            logger.info("discarding filtered reply from {} to {}: {}", from, reply, reply);
            if (callback != null)
                callback.onFailure(from, new Timeout());
            return;
        }

        if (callback == null)
        {
            logger.warn("Callback not found for reply from {} to {}: {} (msgid: {})", from, replyingToNode, reply, replyingToMessage);
            return;
        }

        logger.info("processing reply[{}] from {} to {}: {}", replyingToMessage, from, replyingToNode, reply);
        node.scheduler().now(() -> callback.onSuccess(from, reply));
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

    public MockConfigurationService configService(Id id)
    {
        Node node = nodes.get(id);
        if (node == null)
            throw new NoSuchElementException("No node exists with id " + id);
        return (MockConfigurationService) node.configurationService();
    }

    public MockConfigurationService configService(int i)
    {
        return configService(id(i));
    }

    public List<Node> nodes(Iterable<Id> ids)
    {
        List<Node> rlist = new ArrayList<>();
        for (Id id : ids)
            rlist.add(get(id));
        return rlist;
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

        public MockCluster build()
        {
            Preconditions.checkArgument(initialNodes > 0);
            Preconditions.checkArgument(replication > 0);
            Preconditions.checkArgument(maxKey >= 0);
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
            return new TxnId(epoch, now.get(), 0, id);
        }

        public TxnId idForNode(long epoch, int id)
        {
            return idForNode(epoch, new Id(id));
        }
    }
}
