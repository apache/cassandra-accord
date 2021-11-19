package accord.local;

import accord.api.Agent;
import accord.api.KeyRange;
import accord.api.Store;
import accord.local.CommandStore.Mapping;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Manages the single threaded metadata shards
 */
public class CommandStores
{
    static class Mappings
    {
        final Topology cluster;
        final Topology local;
        final Mapping[] mappings;

        public Mappings(Topology cluster, Topology local, Mapping[] mappings)
        {
            this.cluster = cluster;
            this.local = local;
            this.mappings = mappings;
        }

        static Mappings empty(int size)
        {
            Mapping[] mappings = new Mapping[size];
            for (int i=0; i<size; i++)
                mappings[i] = Mapping.EMPTY;

            return new Mappings(Topology.EMPTY, Topology.EMPTY, mappings);
        }
    }

    private final Node.Id node;
    private final CommandStore[] commandStores;
    private volatile Mappings rangeMappings;

    public CommandStores(int num, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store, CommandStore.Factory shardFactory)
    {
        this.node = node;
        this.commandStores = new CommandStore[num];
        this.rangeMappings = Mappings.empty(num);
        for (int i=0; i<num; i++)
            commandStores[i] = shardFactory.create(i, node, uniqueNow, agent, store, this::getRangeMapping);
    }

    private Mapping getRangeMapping(int idx)
    {
        return rangeMappings.mappings[idx];
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
        IntPredicate predicate = i -> rangeMappings.mappings[i].ranges.intersects(keys);
        return StreamSupport.stream(new ShardSpliterator(predicate), false);
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

        if (cluster.epoch() <= rangeMappings.cluster.epoch())
            return;

        Topology local = cluster.forNode(node);
        KeyRanges current = Arrays.stream(rangeMappings.mappings).map(mapping -> mapping.ranges).reduce(KeyRanges.EMPTY, (l, r) -> l.union(r)).mergeTouching();
        KeyRanges added = local.ranges().difference(current);
        List<KeyRanges> sharded = shardRanges(added, commandStores.length);

        Mapping[] newMappings = new Mapping[rangeMappings.mappings.length];

        for (int i=0; i<rangeMappings.mappings.length; i++)
        {
            KeyRanges newRanges = rangeMappings.mappings[i].ranges.union(sharded.get(i)).mergeTouching();
            newMappings[i] = new Mapping(newRanges, local);
        }

        rangeMappings = new Mappings(cluster, local, newMappings);
    }

    private class ShardSpliterator implements Spliterator<CommandStore>
    {
        int i = 0;
        final IntPredicate predicate;

        public ShardSpliterator(IntPredicate predicate)
        {
            this.predicate = predicate;
        }

        public ShardSpliterator()
        {
            this (i -> true);
        }

        @Override
        public boolean tryAdvance(Consumer<? super CommandStore> action)
        {
            while (i < commandStores.length)
            {
                int idx = i++;
                if (!predicate.test(idx))
                    continue;
                try
                {
                    commandStores[idx].process(action).toCompletableFuture().get();
                    break;
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

            List<CompletableFuture<Void>> futures = new ArrayList<>(commandStores.length - i);
            for (; i< commandStores.length; i++)
            {
                if (predicate.test(i))
                    futures.add(commandStores[i].process(action).toCompletableFuture());
            }

            try
            {
                for (int i=0, mi=futures.size(); i<mi; i++)
                    futures.get(i).get();
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
