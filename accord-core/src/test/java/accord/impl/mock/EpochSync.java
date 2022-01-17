package accord.impl.mock;

import accord.api.Key;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.*;
import accord.messages.*;
import accord.topology.Topologies.Single;
import accord.topology.Topology;
import accord.txn.*;
import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static accord.impl.mock.MockCluster.configService;

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
        private final Txn txn;
        private final Key homeKey;
        private final Timestamp executeAt;
        private final Dependencies deps;
        private final long epoch;

        public SyncCommitted(Command command, long epoch)
        {
            this.epoch = epoch;
            Preconditions.checkArgument(command.hasBeen(Status.Committed));
            this.txnId = command.txnId();
            this.txn = command.txn();
            this.homeKey = command.homeKey();
            this.executeAt = command.executeAt();
            this.deps = command.savedDeps();
        }

        @Override
        public void process(Node node, Node.Id from, ReplyContext replyContext)
        {
            Key progressKey = node.trySelectProgressKey(txnId, txn.keys, homeKey);
            node.forEachLocalSince(txn.keys, epoch, commandStore -> {
                Command command = commandStore.command(txnId);
                command.commit(txn, homeKey, progressKey, executeAt, deps);
            });
            node.reply(from, replyContext, SyncAck.INSTANCE);
        }

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class SyncAck implements Reply
    {
        private static final SyncAck INSTANCE = new SyncAck();
        private SyncAck() {}

        @Override
        public MessageType type()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class CommandSync extends AsyncPromise<Void> implements Callback<SyncAck>
    {
        private final QuorumTracker tracker;

        public CommandSync(Node node, SyncCommitted message, Topology topology)
        {
            Keys keys = message.txn.keys();
            this.tracker = new QuorumTracker(new Single(topology.forKeys(keys), false));
            node.send(tracker.nodes(), message, this);
        }

        @Override
        public synchronized void onSuccess(Node.Id from, SyncAck response)
        {
            tracker.success(from);
            if (tracker.hasReachedQuorum())
                setSuccess(null);
        }

        @Override
        public synchronized void onFailure(Node.Id from, Throwable throwable)
        {
            tracker.failure(from);
            if (tracker.hasFailed())
                tryFailure(throwable);
        }

        public static void sync(Node node, SyncCommitted message, Topology topology)
        {
            try
            {
                new CommandSync(node, message, topology).get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
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
            Consumer<Command> commandConsumer = command -> syncMessages.put(command.txnId(), new SyncCommitted(command, syncEpoch));
            node.forEachLocal(commandStore -> commandStore.forCommittedInEpoch(syncTopology.ranges(), syncEpoch, commandConsumer));

            for (SyncCommitted message : syncMessages.values())
                CommandSync.sync(node, message, nextTopology);

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
