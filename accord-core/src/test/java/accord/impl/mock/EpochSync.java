package accord.impl.mock;

import accord.api.Key;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Command;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.local.Status;
import accord.messages.Callback;
import accord.messages.Reply;
import accord.messages.Request;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.txn.*;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static accord.impl.mock.MockCluster.configService;

public class EpochSync implements Runnable
{
    private final Logger logger = LoggerFactory.getLogger(EpochSync.class);

    private final MockCluster cluster;
    private final long syncEpoch;
    private final long nextEpoch;
    private final Timestamp minTimestamp;
    private final Timestamp maxTimestamp;

    public EpochSync(MockCluster cluster, long syncEpoch)
    {
        this.cluster = cluster;
        this.syncEpoch = syncEpoch;
        this.nextEpoch = syncEpoch + 1;
        minTimestamp = new Timestamp(syncEpoch, Long.MIN_VALUE, Integer.MIN_VALUE, Node.Id.NONE);
        maxTimestamp = new Timestamp(syncEpoch, Long.MAX_VALUE, Integer.MAX_VALUE, Node.Id.MAX);
    }

    private static class SyncMessage implements Request
    {
        private final TxnId txnId;
        private final Txn txn;
        private final Timestamp executeAt;
        private final Dependencies deps;

        public SyncMessage(Command command)
        {
            Preconditions.checkArgument(command.hasBeen(Status.Committed));
            this.txnId = command.txnId();
            this.txn = command.txn();
            this.executeAt = command.executeAt();
            this.deps = command.savedDeps();
        }

        @Override
        public void process(Node node, Node.Id from, long messageId)
        {
            node.local().forEach(commandStore -> {
                Command command = commandStore.command(txnId);
                command.commit(txn, deps, executeAt);
            });
            node.reply(from, messageId, SyncAck.INSTANCE);
        }
    }

    private static class SyncAck implements Reply
    {
        private static final SyncAck INSTANCE = new SyncAck();
        private SyncAck() {}
    }

    private static class CommandSync extends CompletableFuture<Void> implements Callback<SyncAck>
    {
        private final QuorumTracker tracker;

        public CommandSync(Node node, SyncMessage message, Topology topology)
        {
            Keys keys = message.txn.keys();
            this.tracker = new QuorumTracker(new Topologies.Singleton(topology.forKeys(keys), false));
            node.send(tracker.nodes(), message, this);
        }

        @Override
        public synchronized void onSuccess(Node.Id from, SyncAck response)
        {
            tracker.recordSuccess(from);
            if (tracker.hasReachedQuorum())
                complete(null);
        }

        @Override
        public synchronized void onFailure(Node.Id from, Throwable throwable)
        {
            tracker.recordFailure(from);
            if (tracker.hasFailed())
                completeExceptionally(throwable);
        }

        public static void sync(Node node, SyncMessage message, Topology topology)
        {
            try
            {
                new CommandSync(node, message, topology).toCompletableFuture().get();
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
        public void process(Node on, Node.Id from, long messageId)
        {
            configService(on).reportSyncComplete(from, epoch);
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

        // TODO: clean this up
        private void syncKeyCommands(Key key, CommandsForKey commands, Map<TxnId, SyncMessage> destMap)
        {
            Collection<Command> committed = commands.committedByExecuteAt.subMap(minTimestamp,
                                                                                 true,
                                                                                 maxTimestamp,
                                                                                 true).values();
            for (Command command : committed)
                destMap.put(command.txnId(), new SyncMessage(command));
        }

        @Override
        public void run()
        {
            Map<TxnId, SyncMessage> syncMessages = new ConcurrentHashMap<>();
            // TODO: clean this up
            node.local().forEach(commandStore -> commandStore.commandsForRanges(
                    syncTopology.ranges(),
                    (key, commands) -> syncKeyCommands(key, commands, syncMessages)
            ));

            for (SyncMessage message : syncMessages.values())
                CommandSync.sync(node, message, nextTopology);

            // FIXME: which epoch to mark sync complete is confusing
            SyncComplete syncComplete = new SyncComplete(nextEpoch);
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
        cluster.forEachNode(this::syncNode);
    }

    public static void sync(MockCluster cluster, long epoch)
    {
        new EpochSync(cluster, epoch).run();
    }
}
