package accord.local;

import accord.api.Agent;
import accord.api.KeyRange;
import accord.api.Store;
import accord.local.CommandStore.Mapping;
import accord.messages.TxnRequest;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Manages the single threaded metadata shards
 */
public class CommandStores
{
    static class Mappings
    {
        static final Mappings EMPTY = new Mappings(Topology.EMPTY, Topology.EMPTY, new CommandStore[0], new Mapping[0]);
        final Topology cluster;
        final CommandStore[] stores;
        final Topology local;
        final Mapping[] mappings;

        public Mappings(Topology cluster, Topology local, CommandStore[] stores, Mapping[] mappings)
        {
            Preconditions.checkArgument(stores.length == mappings.length);
            this.cluster = cluster;
            this.local = local;
            this.stores = stores;
            this.mappings = mappings;
        }

        int size()
        {
            return stores.length;
        }

        Mappings withNewTopology(Topology cluster, Topology local)
        {
            return new Mappings(cluster, local, stores, Mapping.withNewLocalTopology(mappings, local));
        }
    }

    private final Node.Id node;
    private final BiFunction<Integer, Mapping, CommandStore> shardFactory;
    private final int numShards;
    private volatile Mappings mappings = Mappings.EMPTY;

    public CommandStores(int num, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store, CommandStore.Factory shardFactory)
    {
        this.node = node;
        this.numShards = num;
        this.shardFactory = (idx, mapping) -> shardFactory.create(idx, node, uniqueNow, agent, store, mapping, this::getRangeMapping);
    }

    private Mapping getRangeMapping(int idx)
    {
        return mappings.mappings[idx];
    }

    public synchronized void shutdown()
    {
        for (CommandStore commandStore : mappings.stores)
            commandStore.shutdown();
    }

    public Stream<CommandStore> stream()
    {
        return StreamSupport.stream(new ShardSpliterator(mappings.stores), false);
    }

    public Stream<CommandStore> forKeys(Keys keys)
    {
        Mappings stores = mappings;
        IntPredicate predicate = i -> stores.mappings[i].ranges.intersects(keys);
        return StreamSupport.stream(new ShardSpliterator(stores.stores, predicate), false);
    }

    public Stream<CommandStore> forScope(TxnRequest.Scope scope)
    {
        Mappings stores = mappings;
        IntPredicate predicate = i ->  scope.intersects(stores.mappings[i].ranges);
        return StreamSupport.stream(new ShardSpliterator(stores.stores, predicate), false);
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

        Mappings current = mappings;
        if (cluster.epoch() <= current.cluster.epoch())
            return;

        Topology local = cluster.forNode(node);
        KeyRanges currentRanges = Arrays.stream(current.mappings).map(mapping -> mapping.ranges).reduce(KeyRanges.EMPTY, (l, r) -> l.union(r)).mergeTouching();
        KeyRanges added = local.ranges().difference(currentRanges);

        if (added.isEmpty())
        {
            mappings = mappings.withNewTopology(cluster, local);
            return;
        }


        List<KeyRanges> sharded = shardRanges(added, numShards);
        Mapping[] newMappings = new Mapping[current.size() + sharded.size()];
        CommandStore[] newStores = new CommandStore[current.size() + sharded.size()];
        Mapping.withNewLocalTopology(current.mappings, local, newMappings);
        System.arraycopy(current.stores, 0, newStores, 0, current.size());

        for (int i=0; i<sharded.size(); i++)
        {
            int idx = current.size() + i;
            Mapping mapping = new Mapping(sharded.get(i), local);
            newMappings[idx] = mapping;
            newStores[idx] = shardFactory.apply(idx, mapping);
        }

        mappings = new Mappings(cluster, local, newStores, newMappings);
    }

    private static class ShardSpliterator implements Spliterator<CommandStore>
    {
        int i = 0;
        final CommandStore[] commandStores;
        final IntPredicate predicate;

        public ShardSpliterator(CommandStore[] commandStores, IntPredicate predicate)
        {
            this.commandStores = commandStores;
            this.predicate = predicate;
        }

        public ShardSpliterator(CommandStore[] commandStores)
        {
            this (commandStores, i -> true);
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
