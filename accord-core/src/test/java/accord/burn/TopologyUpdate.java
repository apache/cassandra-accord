package accord.burn;

import accord.api.TestableConfigurationService;
import accord.coordinate.FetchData;
import accord.local.Command;
import accord.local.Node;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.primitives.AbstractRoute;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.MessageTask;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static accord.coordinate.FetchData.Outcome.NotFullyReplicated;
import static accord.coordinate.Invalidate.invalidate;

public class TopologyUpdate
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyUpdate.class);

    private static final Set<Long> pendingTopologies = Sets.newConcurrentHashSet();

    private static class CommandSync
    {
        final TxnId txnId;
        final Status status;
        final AbstractRoute route;
        final Timestamp executeAt;
        final long fromEpoch;
        final long toEpoch;

        public CommandSync(TxnId txnId, CheckStatusOk status, long fromEpoch)
        {
            Preconditions.checkArgument(status.status.hasBeen(Status.PreAccepted));
            Preconditions.checkState(status.route != null);
            this.txnId = txnId;
            this.status = status.status;
            this.route = status.route;
            this.executeAt = status.executeAt;
            this.fromEpoch = fromEpoch;
            this.toEpoch = fromEpoch + 1;
        }

        public void process(Node node, Consumer<Boolean> onDone)
        {
            if (!node.topology().hasEpoch(txnId.epoch))
            {
                node.configService().fetchTopologyForEpoch(txnId.epoch);
                node.topology().awaitEpoch(txnId.epoch).addListener(() -> process(node, onDone));
                return;
            }

            // first check if already applied locally, and respond immediately
            Status minStatus = node.mapReduceLocal(route, toEpoch, instance -> instance.command(txnId).status(), (a, b) -> a.compareTo(b) <= 0 ? a : b);
            if (minStatus == null || minStatus.logicalCompareTo(status) >= 0)
            {
                // TODO: minStatus == null means we're sending redundant messages
                onDone.accept(true);
                return;
            }

            BiConsumer<FetchData.Outcome, Throwable> callback = (outcome, fail) -> {
                if (fail != null)
                    process(node, onDone);
                else if (outcome == NotFullyReplicated)
                    invalidate(node, txnId, route.with(route.homeKey), route.homeKey, (i1, i2) -> process(node, onDone));
                else
                    onDone.accept(true);
            };
            switch (status)
            {
                case NotWitnessed:
                    onDone.accept(true);
                    break;
                case PreAccepted:
                case Accepted:
                case AcceptedInvalidate:
                    FetchData.fetchUncommitted(node, txnId, route, toEpoch, callback);
                    break;
                case Committed:
                case ReadyToExecute:
                case Executed:
                case Applied:
                case Invalidated:
                    node.withEpoch(Math.max(executeAt.epoch, toEpoch), () -> {
                        FetchData.fetchCommitted(node, txnId, route, executeAt, toEpoch, callback);
                    });
            }
        }
    }

    public static <T> BiConsumer<T, Throwable> dieOnException()
    {
        return (result, throwable) -> {
            if (throwable != null)
            {
                logger.error("", throwable);
                System.exit(1);
            }
        };
    }

    public static <T> Future<T> dieExceptionally(Future<T> stage)
    {
        return stage.addCallback(dieOnException());
    }

    public static MessageTask notify(Node originator, Collection<Node.Id> cluster, Topology update)
    {
        pendingTopologies.add(update.epoch());
        return MessageTask.begin(originator, cluster, "TopologyNotify:" + update.epoch(), (node, from, onDone) -> {
            long nodeEpoch = node.topology().epoch();
            if (nodeEpoch + 1 < update.epoch())
                onDone.accept(false);
            ((TestableConfigurationService) node.configService()).reportTopology(update);
            onDone.accept(true);
        });
    }

    private static Collection<Node.Id> allNodesFor(Txn txn, Topology... topologies)
    {
        Set<Node.Id> result = new HashSet<>();
        for (Topology topology : topologies)
            result.addAll(topology.forKeys(txn.keys()).nodes());
        return result;
    }

    private static Stream<MessageTask> syncEpochCommands(Node node, long epoch, KeyRanges ranges, Function<CommandSync, Collection<Node.Id>> recipients, long forEpoch, boolean committedOnly)
    {
        Map<TxnId, CheckStatusOk> syncMessages = new ConcurrentHashMap<>();
        Consumer<Command> commandConsumer = command -> syncMessages.merge(command.txnId(), new CheckStatusOk(node, command), CheckStatusOk::merge);
        if (committedOnly)
            node.forEachLocal(commandStore -> commandStore.forCommittedInEpoch(ranges, epoch, commandConsumer));
        else
            node.forEachLocal(commandStore -> commandStore.forEpochCommands(ranges, epoch, commandConsumer));

        return syncMessages.entrySet().stream().map(e -> {
            CommandSync sync = new CommandSync(e.getKey(), e.getValue(), epoch);
            return MessageTask.of(node, recipients.apply(sync), "Sync:" + e.getKey() + ':' + epoch + ':' + forEpoch, sync::process);
        });
    }

    private static final boolean COMMITTED_ONLY = true;

    /**
     * Syncs all replicated commands. Overkill, but useful for confirming issues in optimizedSync
     */
    private static Stream<MessageTask> thoroughSync(Node node, long syncEpoch)
    {
        Topology syncTopology = node.configService().getTopologyForEpoch(syncEpoch);
        Topology localTopology = syncTopology.forNode(node.id());
        Function<CommandSync, Collection<Node.Id>> allNodes = cmd -> node.topology().withUnsyncedEpochs(cmd.route, syncEpoch).nodes();

        KeyRanges ranges = localTopology.ranges();
        Stream<MessageTask> messageStream = Stream.empty();
        for (long epoch=1; epoch<=syncEpoch; epoch++)
        {
            messageStream = Stream.concat(messageStream, syncEpochCommands(node, epoch, ranges, allNodes, syncEpoch, COMMITTED_ONLY));
        }
        return messageStream;
    }

    /**
     * Syncs all newly replicated commands when nodes are gaining ranges and the current epoch
     */
    private static Stream<MessageTask> optimizedSync(Node node, long syncEpoch)
    {
        long nextEpoch = syncEpoch + 1;
        Topology syncTopology = node.configService().getTopologyForEpoch(syncEpoch);
        Topology localTopology = syncTopology.forNode(node.id());
        Topology nextTopology = node.configService().getTopologyForEpoch(nextEpoch);
        Function<CommandSync, Collection<Node.Id>> allNodes = cmd -> node.topology().withUnsyncedEpochs(cmd.route, syncEpoch).nodes();

        // backfill new replicas with operations from prior epochs
        Stream<MessageTask> messageStream = Stream.empty();
        for (Shard syncShard : localTopology.shards())
        {
            for (Shard nextShard : nextTopology.shards())
            {
                // do nothing if there's no change
                if (syncShard.range.equals(nextShard.range) && syncShard.nodeSet.equals(nextShard.nodeSet))
                    continue;

                KeyRange intersection = syncShard.range.intersection(nextShard.range);

                if (intersection == null)
                    continue;

                Set<Node.Id> newNodes = Sets.difference(nextShard.nodeSet, syncShard.nodeSet);

                if (newNodes.isEmpty())
                    continue;

                KeyRanges ranges = KeyRanges.single(intersection);
                for (long epoch=1; epoch<syncEpoch; epoch++)
                    messageStream = Stream.concat(messageStream, syncEpochCommands(node,
                                                                                   epoch,
                                                                                   ranges,
                                                                                   cmd -> newNodes,
                                                                                   syncEpoch, COMMITTED_ONLY));
            }
        }


        // update all current and future replicas with the contents of the sync epoch
        messageStream = Stream.concat(messageStream, syncEpochCommands(node,
                                                                       syncEpoch,
                                                                       localTopology.ranges(),
                                                                       allNodes,
                                                                       syncEpoch, COMMITTED_ONLY));
        return messageStream;
    }

    public static Future<Void> sync(Node node, long syncEpoch)
    {
        Stream<MessageTask> messageStream = optimizedSync(node, syncEpoch);

        Iterator<MessageTask> iter = messageStream.iterator();
        if (!iter.hasNext())
        {
            return ImmediateFuture.success(null);
        }

        MessageTask first = iter.next();
        MessageTask last = first;
        while (iter.hasNext())
        {
            MessageTask next = iter.next();
            last.addListener(next);
            last = next;
        }

        first.run();
        return dieExceptionally(last);
    }

    public static Future<Void> syncEpoch(Node originator, long epoch, Collection<Node.Id> cluster)
    {
        Future<Void> future = dieExceptionally(sync(originator, epoch)
                .flatMap(v -> MessageTask.apply(originator, cluster, "SyncComplete:" + epoch, (node, from, onDone) -> {
                    node.onEpochSyncComplete(originator.id(), epoch);
                    onDone.accept(true);
                })));
        future.addCallback((unused, throwable) -> pendingTopologies.remove(epoch));
        return future;
    }

    public static int pendingTopologies()
    {
        return pendingTopologies.size();
    }
}
