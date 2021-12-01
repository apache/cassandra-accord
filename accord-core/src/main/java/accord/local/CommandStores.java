package accord.local;

import accord.api.Agent;
import accord.api.Key;
import accord.api.Store;
import accord.messages.TxnRequest;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;
import com.google.common.base.Preconditions;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Manages the single threaded metadata shards
 */
public class CommandStores
{
    static class StoreGroup
    {
        final CommandStore[] stores;
        final KeyRanges ranges;

        public StoreGroup(CommandStore[] stores, KeyRanges ranges)
        {
            this.stores = stores;
            this.ranges = ranges;
        }

        private static class AccumulatingBitset extends BitSet implements Keys.KeyAccumulator<Keys>
        {
            final int numStores;
            boolean isDone = false;
            public AccumulatingBitset(int numStores)
            {
                super(numStores);
                this.numStores = numStores;
            }

            @Override
            public Keys accumulate(Key key, Keys keys)
            {
                int idx = keyIndex(key, numStores);
                if (get(idx))
                    return keys;
                set(idx);
                isDone = cardinality() == numStores;
                return keys;
            }

            @Override
            public boolean isDone()
            {
                return isDone;
            }
        }

        public Stream<CommandStore> stream()
        {
            return StreamSupport.stream(new ShardSpliterator(stores), false);
        }

        public Stream<CommandStore> stream(Keys keys)
        {
            AccumulatingBitset bitSet = new AccumulatingBitset(stores.length);
            keys.accumulate(ranges, bitSet, keys);
            if (bitSet.cardinality() == 0)
                return null;
            return StreamSupport.stream(new ShardSpliterator(stores, bitSet::get), false);
        }

        public Stream<CommandStore> stream(TxnRequest.Scope scope)
        {
            AccumulatingBitset bitSet = new AccumulatingBitset(stores.length);
            for (int i=0, mi=scope.size(); i<mi; i++)
            {
                Keys keys = scope.get(i).keys;
                keys.accumulate(ranges, bitSet, keys);
                if (bitSet.isDone)
                    break;
            }
            if (bitSet.cardinality() == 0)
                return null;
            return StreamSupport.stream(new ShardSpliterator(stores, bitSet::get), false);
        }
    }

    static int keyIndex(Key key, int numShards)
    {
        return (int) (Integer.toUnsignedLong(key.keyHash()) % numShards);
    }

    static class StoreGroups
    {
        static final StoreGroups EMPTY = new StoreGroups(new StoreGroup[0], Topology.EMPTY, Topology.EMPTY);
        final StoreGroup[] groups;
        final Topology global;
        final Topology local;

        public StoreGroups(StoreGroup[] groups, Topology global, Topology local)
        {
            this.groups = groups;
            this.global = global;
            this.local = local;
        }

        StoreGroups withNewTopology(Topology global, Topology local)
        {
            return new StoreGroups(groups, global, local);
        }

        public Stream<CommandStore> stream()
        {
            Stream<CommandStore> stream = null;
            for (StoreGroup group : groups)
            {
                Stream<CommandStore> nextStream = group.stream();
                if (nextStream == null) continue;
                stream = stream != null ? Stream.concat(stream, nextStream) : nextStream;
            }

            return stream != null ? stream : Stream.empty();
        }

        public Stream<CommandStore> stream(Keys keys)
        {
            Stream<CommandStore> stream = null;
            for (StoreGroup group : groups)
            {
                Stream<CommandStore> nextStream = group.stream(keys);
                if (nextStream == null) continue;
                stream = stream != null ? Stream.concat(stream, nextStream) : nextStream;
            }

            return stream != null ? stream : Stream.empty();
        }

        public Stream<CommandStore> stream(TxnRequest.Scope scope)
        {
            Stream<CommandStore> stream = null;
            for (StoreGroup group : groups)
            {
                Stream<CommandStore> nextStream = group.stream(scope);
                if (nextStream == null) continue;
                stream = stream != null ? Stream.concat(stream, nextStream) : nextStream;
            }

            return stream != null ? stream : Stream.empty();
        }
    }

    private final Node.Id node;
    private final Function<Timestamp, Timestamp> uniqueNow;
    private final Agent agent;
    private final Store store;
    private final CommandStore.Factory shardFactory;
    private final int numShards;
    private volatile StoreGroups groups = StoreGroups.EMPTY;

    public CommandStores(int num, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store, CommandStore.Factory shardFactory)
    {
        this.node = node;
        this.numShards = num;
        this.uniqueNow = uniqueNow;
        this.agent = agent;
        this.store = store;
        this.shardFactory = shardFactory;
    }

    private CommandStore createCommandStore(int generation, int index, KeyRanges ranges)
    {
        return shardFactory.create(generation, index, numShards, node, uniqueNow, agent, store, ranges, this::getLocalTopology);
    }

    private Topology getLocalTopology()
    {
        return groups.local;
    }

    public synchronized void shutdown()
    {
        for (StoreGroup group : groups.groups)
            for (CommandStore commandStore : group.stores)
                commandStore.shutdown();
    }

    public Stream<CommandStore> stream()
    {
        return groups.stream();
    }

    public Stream<CommandStore> forKeys(Keys keys)
    {
        return groups.stream(keys);
    }

    public Stream<CommandStore> forScope(TxnRequest.Scope scope)
    {
        return groups.stream(scope);
    }

    public synchronized void updateTopology(Topology cluster)
    {
        Preconditions.checkArgument(!cluster.isSubset(), "Use full topology for CommandStores.updateTopology");

        StoreGroups current = groups;
        if (cluster.epoch() <= current.global.epoch())
            return;

        Topology local = cluster.forNode(node);
        KeyRanges currentRanges = Arrays.stream(current.groups).map(group -> group.ranges).reduce(KeyRanges.EMPTY, (l, r) -> l.union(r)).mergeTouching();
        KeyRanges added = local.ranges().difference(currentRanges);

        if (added.isEmpty())
        {
            groups = groups.withNewTopology(cluster, local);
            return;
        }

        int newGeneration = current.groups.length;
        StoreGroup[] newGroups = new StoreGroup[current.groups.length + 1];
        CommandStore[] newStores = new CommandStore[numShards];
        System.arraycopy(current.groups, 0, newGroups, 0, current.groups.length);

        for (int i=0; i<numShards; i++)
            newStores[i] = createCommandStore(newGeneration, i, added);

        newGroups[current.groups.length] = new StoreGroup(newStores, added);

        groups = new StoreGroups(newGroups, cluster, local);
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
                    commandStores[idx].process(action).get();
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

            List<Future<Void>> futures = new ArrayList<>(commandStores.length - i);
            for (; i< commandStores.length; i++)
            {
                if (predicate.test(i))
                    futures.add(commandStores[i].process(action));
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
