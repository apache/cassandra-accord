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

import accord.api.TestableConfigurationService;
import accord.impl.InMemoryCommandStore;
import accord.coordinate.FetchData;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Status;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.primitives.*;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.MapReduce;
import accord.utils.MessageTask;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static accord.coordinate.Invalidate.invalidate;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.*;
import static accord.local.Status.Known.*;

public class TopologyUpdates
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyUpdates.class);

    private static class CommandSync
    {
        final TxnId txnId;
        final Status status;
        final Route<?> route;
        final Timestamp executeAt;
        final long fromEpoch;
        final long toEpoch;

        public CommandSync(TxnId txnId, CheckStatusOk status, long srcEpoch, long trgEpoch)
        {
            Invariants.checkArgument(status.saveStatus.hasBeen(Status.PreAccepted));
            Invariants.checkState(status.route != null && !status.route.isEmpty());
            this.txnId = txnId;
            this.status = status.saveStatus.status;
            this.route = status.route;
            this.executeAt = status.executeAt;
            this.fromEpoch = srcEpoch;
            this.toEpoch = trgEpoch;
        }

        @Override
        public String toString()
        {
            return "CommandSync{" + "txnId:" + txnId + ", fromEpoch:" + fromEpoch + ", toEpoch:" + toEpoch + '}';
        }

        public void process(Node node, Consumer<Boolean> onDone)
        {
            if (!node.topology().hasEpoch(toEpoch))
            {
                node.configService().fetchTopologyForEpoch(toEpoch);
                node.topology().awaitEpoch(toEpoch).addCallback(() -> process(node, onDone)).begin(node.agent());
                return;
            }

            AsyncChain<Status> statusChain = node.commandStores().mapReduce(contextFor(txnId), route, toEpoch, toEpoch,
                                                                            MapReduce.of(safeStore -> safeStore.command(txnId).current().status(),
                                                                                         (a, b) -> a.compareTo(b) <= 0 ? a : b));
            AsyncResult<Object> sync = statusChain.map(minStatus -> {
                if (minStatus == null || minStatus.phase.compareTo(status.phase) >= 0)
                {
                    // TODO (low priority): minStatus == null means we're sending redundant messages
                    onDone.accept(true);
                    return null;
                }

                BiConsumer<Known, Throwable> callback = (outcome, fail) -> {
                    if (fail != null)
                        process(node, onDone);
                    else if (outcome == Nothing)
                        invalidate(node, txnId, route.with(route.homeKey()), (i1, i2) -> process(node, onDone));
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
                        FetchData.fetch(DefinitionOnly, node, txnId, route, toEpoch, callback);
                        break;
                    case Committed:
                    case ReadyToExecute:
                        FetchData.fetch(Committed.minKnown, node, txnId, route, toEpoch, callback);
                        break;
                    case PreApplied:
                    case Applied:
                        node.withEpoch(Math.max(executeAt.epoch(), toEpoch), () -> {
                            FetchData.fetch(PreApplied.minKnown, node, txnId, route, executeAt, toEpoch, callback);
                        });
                        break;
                    case Invalidated:
                        AsyncChain<Void> invalidate = node.forEachLocal(contextFor(txnId), route, txnId.epoch(), toEpoch, safeStore -> {
                            Commands.commitInvalidate(safeStore, txnId);
                        });

                        dieExceptionally(invalidate.addCallback(((unused, failure) -> onDone.accept(failure == null))).beginAsResult());
                }
                return null;
            }, node.commandStores().any()).beginAsResult();
            dieExceptionally(sync);
        }
    }

    private final Set<Long> pendingTopologies = Sets.newConcurrentHashSet();

    public static <T> BiConsumer<T, Throwable> dieOnException()
    {
        return (result, throwable) -> {
            if (throwable != null)
            {
                logger.error("Unexpected exception", throwable);
                logger.error("", new Throwable("Shutting down test"));
                System.exit(1);
            }
        };
    }

    public static <T> AsyncResult<T> dieExceptionally(AsyncResult<T> stage)
    {
        stage.addCallback(dieOnException());
        return stage;
    }

    public MessageTask notify(Node originator, Collection<Node.Id> cluster, Topology update)
    {
        pendingTopologies.add(update.epoch());
        CommandStore.checkNotInStore();
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
            result.addAll(topology.forSelection(txn.keys().toUnseekables()).nodes());
        return result;
    }

    private static AsyncChain<Stream<MessageTask>> syncEpochCommands(Node node, long srcEpoch, Ranges ranges, Function<CommandSync, Collection<Node.Id>> recipients, long trgEpoch, boolean committedOnly)
    {
        Map<TxnId, CheckStatusOk> syncMessages = new ConcurrentHashMap<>();
        Consumer<Command> commandConsumer = command -> syncMessages.merge(command.txnId(), new CheckStatusOk(node, command), CheckStatusOk::merge);
        AsyncChain<Void> start;
        if (committedOnly)
            start = node.commandStores().forEach(commandStore -> InMemoryCommandStore.inMemory(commandStore).forCommittedInEpoch(ranges, srcEpoch, commandConsumer));
        else
            start = node.commandStores().forEach(commandStore -> InMemoryCommandStore.inMemory(commandStore).forEpochCommands(ranges, srcEpoch, commandConsumer));

        return start.map(ignore -> syncMessages.entrySet().stream().map(e -> {
            CommandSync sync = new CommandSync(e.getKey(), e.getValue(), srcEpoch, trgEpoch);
            return MessageTask.of(node, recipients.apply(sync), sync.toString(), sync::process);
        }));
    }

    private static final boolean PREACCEPTED = false;
    private static final boolean COMMITTED_ONLY = true;

    /**
     * Syncs all replicated commands. Overkill, but useful for confirming issues in optimizedSync
     */
    private static AsyncChain<Stream<MessageTask>> thoroughSync(Node node, long syncEpoch)
    {
        Topology syncTopology = node.configService().getTopologyForEpoch(syncEpoch);
        Topology localTopology = syncTopology.forNode(node.id());
        Function<CommandSync, Collection<Node.Id>> allNodes = cmd -> node.topology().withUnsyncedEpochs(cmd.route, syncEpoch + 1).nodes();

        Ranges ranges = localTopology.ranges();

        List<AsyncChain<Stream<MessageTask>>> work = new ArrayList<>();
        for (long epoch=1; epoch<=syncEpoch; epoch++)
            work.add(syncEpochCommands(node, epoch, ranges, allNodes, syncEpoch, COMMITTED_ONLY));
        return AsyncChains.reduce(work, Stream.empty(), Stream::concat);
    }

    /**
     * Syncs all newly replicated commands when nodes are gaining ranges and the current epoch
     */
    private static AsyncChain<Stream<MessageTask>> optimizedSync(Node node, long srcEpoch)
    {
        long trgEpoch = srcEpoch + 1;
        Topology syncTopology = node.configService().getTopologyForEpoch(srcEpoch);
        Topology localTopology = syncTopology.forNode(node.id());
        Topology nextTopology = node.configService().getTopologyForEpoch(trgEpoch);
        Function<CommandSync, Collection<Node.Id>> allNodes = cmd -> node.topology().preciseEpochs(cmd.route, trgEpoch, trgEpoch).nodes();

        // backfill new replicas with operations from prior epochs
        List<AsyncChain<Stream<MessageTask>>> work = new ArrayList<>(localTopology.shards().size());
        for (Shard syncShard : localTopology.shards())
        {
            for (Shard nextShard : nextTopology.shards())
            {
                // do nothing if there's no change
                if (syncShard.range.equals(nextShard.range) && syncShard.nodeSet.equals(nextShard.nodeSet))
                    continue;

                Range intersection = syncShard.range.intersection(nextShard.range);

                if (intersection == null)
                    continue;

                Set<Node.Id> newNodes = Sets.difference(nextShard.nodeSet, syncShard.nodeSet);

                if (newNodes.isEmpty())
                    continue;

                Ranges ranges = Ranges.single(intersection);
                for (long epoch=1; epoch<srcEpoch; epoch++)
                    work.add(syncEpochCommands(node, epoch, ranges, cmd -> newNodes, trgEpoch, COMMITTED_ONLY));
            }
        }

        // update all current and future replicas with the contents of the sync epoch
        work.add(syncEpochCommands(node, srcEpoch, localTopology.ranges(), allNodes, trgEpoch, PREACCEPTED));

        return AsyncChains.reduce(work, Stream.empty(), Stream::concat);
    }

    private static AsyncChain<Void> sync(Node node, long syncEpoch)
    {
        return optimizedSync(node, syncEpoch)
                .flatMap(messageStream -> {
                    Iterator<MessageTask> iter = messageStream.iterator();
                    if (!iter.hasNext()) return AsyncResults.success(null);

                    MessageTask first = iter.next();
                    MessageTask last = first;
                    while (iter.hasNext())
                    {
                        MessageTask next = iter.next();
                        last.addCallback(next);
                        last = next;
                    }

                    first.run();
                    return dieExceptionally(last);
                });
    }

    public AsyncResult<Void> syncEpoch(Node originator, long epoch, Collection<Node.Id> cluster)
    {
        AsyncResult<Void> result = dieExceptionally(sync(originator, epoch)
                .flatMap(v -> MessageTask.apply(originator, cluster, "SyncComplete:" + epoch, (node, from, onDone) -> {
                    node.onEpochSyncComplete(originator.id(), epoch);
                    onDone.accept(true);
                })).beginAsResult());
        result.addCallback((unused, throwable) -> pendingTopologies.remove(epoch));
        return result;
    }

    public int pendingTopologies()
    {
        return pendingTopologies.size();
    }
}
