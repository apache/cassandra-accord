package accord.local;

import accord.api.Agent;
import accord.api.KeyRange;
import accord.api.Store;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Manages the single threaded metadata shards
 */
public class CommandStores
{
    private final Node.Id node;
    private final CommandStore[] commandStores;
    private volatile RangeMapping.Multi rangeMappings;

    public CommandStores(int num, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store, CommandStore.Factory shardFactory)
    {
        this.node = node;
        this.commandStores = new CommandStore[num];
        this.rangeMappings = RangeMapping.Multi.empty(num);
        for (int i=0; i<num; i++)
            commandStores[i] = shardFactory.create(i, node, uniqueNow, agent, store, this::getRangeMapping);
    }

    public Topology clusterTopology()
    {
        return rangeMappings.cluster;
    }

    private RangeMapping getRangeMapping(int idx)
    {
        return rangeMappings.mappings[idx];
    }

    public long epoch()
    {
        return rangeMappings.cluster.epoch();
    }

    public synchronized void shutdown()
    {
        for (CommandStore commandStore : commandStores)
            commandStore.shutdown();
    }

    public Stream<CommandStore> stream()
    {
        return StreamSupport.stream(new ShardSpliterator(), false);
    }

    public Stream<CommandStore> forKeys(Keys keys)
    {
        // TODO: filter shards before sending to their thread?
        return stream().filter(commandShard -> commandShard.intersects(keys));
    }

    static List<KeyRanges> shardRanges(KeyRanges ranges, int shards)
    {
        List<List<KeyRange>> sharded = new ArrayList<>(shards);
        for (int i=0; i<shards; i++)
            sharded.add(new ArrayList<>(ranges.size()));

        for (KeyRange range : ranges)
        {
            KeyRanges split = range.split(shards);
            Preconditions.checkState(split.size() <= shards);
            for (int i=0; i<split.size(); i++)
                sharded.get(i).add(split.get(i));
        }

        List<KeyRanges> result = new ArrayList<>(shards);
        for (int i=0; i<shards; i++)
        {
            result.add(new KeyRanges(sharded.get(i).toArray(KeyRange[]::new)));
        }

        return result;
    }

    public synchronized void updateTopology(Topology cluster)
    {
        Preconditions.checkArgument(!cluster.isSubset(), "Use full topology for CommandStores.updateTopology");

        // FIXME: if we miss a topology update, we may end up with stale command data for ranges that were owned by
        //  someone else during the period we were not receiving topology updates. Something like a ballot low bound
        //  may make this a non-issue, and would be preferable to having to chase down all historical topology changes.
        //  OTOH, we'll probably need a complete history of topology changes to do recovery properly
        if (cluster.epoch() <= rangeMappings.cluster.epoch())
            return;

        Topology local = cluster.forNode(node);
        KeyRanges removed = rangeMappings.local.ranges().difference(local.ranges());
        KeyRanges added = local.ranges().difference(rangeMappings.local.ranges());
        List<KeyRanges> sharded = shardRanges(added, commandStores.length);

        RangeMapping[] newMappings = new RangeMapping[rangeMappings.mappings.length];

        for (int i=0; i<rangeMappings.mappings.length; i++)
        {
            KeyRanges newRanges = rangeMappings.mappings[i].ranges.difference(removed).union(sharded.get(i)).mergeTouching();
            newMappings[i] = RangeMapping.mapRanges(newRanges, local);
        }

        RangeMapping.Multi previous = rangeMappings;
        rangeMappings = new RangeMapping.Multi(cluster, local, newMappings);
        stream().forEach(commands -> commands.onTopologyChange(previous.mappings[commands.index()],
                                                               rangeMappings.mappings[commands.index()]));
    }

    private class ShardSpliterator implements Spliterator<CommandStore>
    {
        int i = 0;

        @Override
        public boolean tryAdvance(Consumer<? super CommandStore> action)
        {
            if (i < commandStores.length)
            {
                CommandStore shard = commandStores[i++];
                try
                {
                    shard.process(action).toCompletableFuture().get();
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new RuntimeException(e);
                }

            }
            return i < commandStores.length;
        }

        @Override
        public void forEachRemaining(Consumer<? super CommandStore> action)
        {
            if (i >= commandStores.length)
                return;

            CompletableFuture<Void>[] futures = new CompletableFuture[commandStores.length - i];
            for (; i< commandStores.length; i++)
                futures[i] = commandStores[i].process(action).toCompletableFuture();

            try
            {
                for (CompletableFuture<Void> future : futures)
                    future.get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                Throwable cause = e.getCause();
                throw new RuntimeException(cause != null ? cause : e);
            }
        }

        @Override
        public Spliterator<CommandStore> trySplit()
        {
            return null;
        }

        @Override
        public long estimateSize()
        {
            return commandStores.length;
        }

        @Override
        public int characteristics()
        {
            return Spliterator.SIZED | Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.IMMUTABLE;
        }
    }
}
