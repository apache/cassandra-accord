package accord.local;

import accord.api.Agent;
import accord.api.Key;
import accord.api.Store;
import accord.local.CommandStore.SingleThread;
import accord.local.CommandStores.StoreGroups.Fold;
import accord.local.Node.Id;
import accord.messages.TxnRequest;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.*;

import static java.lang.Boolean.FALSE;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores
{
    public interface Factory
    {
        CommandStores create(int num, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store);
    }

    static class StoreGroup
    {
        final CommandStore[] stores;
        final KeyRanges ranges;

        public StoreGroup(CommandStore[] stores, KeyRanges ranges)
        {
            Preconditions.checkArgument(stores.length <= 64);
            this.stores = stores;
            this.ranges = ranges;
        }

        long all()
        {
            return -1L >>> (64 - stores.length);
        }

        long matches(Keys keys)
        {
            return keys.foldl(ranges, StoreGroup::addKeyIndex, stores.length, 0L, -1L);
        }

        long matches(TxnRequest.Scope scope)
        {
            return matches(scope.keys());
        }

        static long keyIndex(Key key, long numShards)
        {
            return Integer.toUnsignedLong(key.keyHash()) % numShards;
        }

        private static long addKeyIndex(Key key, long numShards, long accumulate)
        {
            return accumulate | (1L << keyIndex(key, numShards));
        }
    }

    static class StoreGroups
    {
        final StoreGroup[] groups;
        final Topology global;
        final Topology local;
        final int size;

        public StoreGroups(StoreGroup[] groups, Topology global, Topology local)
        {
            this.groups = groups;
            this.global = global;
            this.local = local;
            int size = 0;
            for (StoreGroup group : groups)
                size += group.stores.length;
            this.size = size;
        }

        StoreGroups withNewTopology(Topology global, Topology local)
        {
            return new StoreGroups(groups, global, local);
        }

        public int size()
        {
            return size;
        }

        private <I1, I2, O> O foldl(int startGroup, long bitset, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, O accumulator)
        {
            int groupIndex = startGroup;
            StoreGroup group = groups[groupIndex];
            int offset = 0;
            while (true)
            {
                int i = Long.numberOfTrailingZeros(bitset) - offset;
                while (i < group.stores.length)
                {
                    accumulator = fold.fold(group.stores[i], param1, param2, accumulator);
                    bitset ^= Long.lowestOneBit(bitset);
                    i = Long.numberOfTrailingZeros(bitset) - offset;
                }

                if (++groupIndex == groups.length)
                    break;

                if (bitset == 0)
                    break;

                offset += group.stores.length;
                group = groups[groupIndex];
                if (offset + group.stores.length > 64)
                    break;
            }
            return accumulator;
        }

        interface Fold<I1, I2, O>
        {
            O fold(CommandStore store, I1 i1, I2 i2, O accumulator);
        }

        <S, I1, I2, O> O foldl(ToLongBiFunction<StoreGroup, S> select, S scope, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, IntFunction<? extends O> factory)
        {
            O accumulator = null;
            int startGroup = 0;
            while (startGroup < groups.length)
            {
                long bits = select.applyAsLong(groups[startGroup], scope);
                if (bits == 0)
                {
                    ++startGroup;
                    continue;
                }

                int offset = groups[startGroup].stores.length;
                int endGroup = startGroup + 1;
                while (endGroup < groups.length)
                {
                    StoreGroup group = groups[endGroup];
                    if (offset + group.stores.length > 64)
                        break;

                    bits += select.applyAsLong(group, scope) << offset;
                    offset += group.stores.length;
                    ++endGroup;
                }

                if (accumulator == null)
                    accumulator = factory.apply(Long.bitCount(bits));

                accumulator = foldl(startGroup, bits, fold, param1, param2, accumulator);
                startGroup = endGroup;
            }

            return accumulator;
        }
    }

    private final Node.Id node;
    private final Function<Timestamp, Timestamp> uniqueNow;
    private final Agent agent;
    private final Store store;
    private final CommandStore.Factory shardFactory;
    private final int numShards;
    protected volatile StoreGroups groups = new StoreGroups(new StoreGroup[0], Topology.EMPTY, Topology.EMPTY);

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

    protected abstract <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach);
    protected abstract <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce);

    public void forEach(Consumer<CommandStore> forEach)
    {
        forEach((s, i) -> s.all(), null, forEach);
    }

    public void forEach(Keys keys, Consumer<CommandStore> forEach)
    {
        forEach(StoreGroup::matches, keys, forEach);
    }

    public void forEach(TxnRequest.Scope scope, Consumer<CommandStore> forEach)
    {
        forEach(StoreGroup::matches, scope, forEach);
    }

    public <T> T mapReduce(TxnRequest.Scope scope, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(StoreGroup::matches, scope, map, reduce);
    }

    public <T extends Collection<CommandStore>> T collect(Keys keys, IntFunction<T> factory)
    {
        return groups.foldl(StoreGroup::matches, keys, CommandStores::append, null, null, factory);
    }

    public <T extends Collection<CommandStore>> T collect(TxnRequest.Scope scope, IntFunction<T> factory)
    {
        return groups.foldl(StoreGroup::matches, scope, CommandStores::append, null, null, factory);
    }

    private static <T extends Collection<CommandStore>> T append(CommandStore store, Object ignore1, Object ignore2, T to)
    {
        to.add(store);
        return to;
    }

    public synchronized void updateTopology(Topology cluster)
    {
        Preconditions.checkArgument(!cluster.isSubset(), "Use full topology for CommandStores.updateTopology");

        StoreGroups current = groups;
        if (cluster.epoch() <= current.global.epoch())
            return;

        Topology local = cluster.forNode(node);
        KeyRanges added = local.ranges().difference(current.local.ranges());

        for (StoreGroup group : groups.groups)
        {
            // FIXME: remove this (and the corresponding check in TopologyRandomizer) once lower bounds are implemented.
            //  In the meantime, the logic needed to support acquiring ranges that we previously replicated is pretty
            //  convoluted without the ability to jettison epochs.
            Preconditions.checkState(!group.ranges.intersects(added));
        }

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

    @VisibleForTesting
    public CommandStore unsafeForKey(Key key)
    {
        for (StoreGroup group : groups.groups)
        {
            if (group.ranges.contains(key))
            {
                for (CommandStore store : group.stores)
                {
                    if (store.hashIntersects(key))
                        return store;
                }
            }
        }
        throw new IllegalArgumentException();
    }

    public static class Synchronized extends CommandStores
    {
        public Synchronized(int num, Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
        {
            super(num, node, uniqueNow, agent, store, CommandStore.Synchronized::new);
        }

        @Override
        protected <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return groups.foldl(select, scope, (store, f, r, t) -> t == null ? f.apply(store) : r.apply(t, f.apply(store)), map, reduce, ignore -> null);
        }

        @Override
        protected <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach)
        {
            groups.foldl(select, scope, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
        }
    }

    public static class SingleThread extends CommandStores
    {
        public SingleThread(int num, Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
        {
            this(num, node, uniqueNow, agent, store, CommandStore.SingleThread::new);
        }

        public SingleThread(int num, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store, CommandStore.Factory shardFactory)
        {
            super(num, node, uniqueNow, agent, store, shardFactory);
        }

        private <S, F, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, F f, Fold<F, ?, List<Future<T>>> fold, BiFunction<T, T, T> reduce)
        {
            List<Future<T>> futures = groups.foldl(select, scope, fold, f, null, ArrayList::new);
            T result = null;
            for (Future<T> future : futures)
            {
                try
                {
                    T next = future.get();
                    if (result == null) result = next;
                    else result = reduce.apply(result, next);
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e.getCause());
                }
            }
            return result;
        }

        @Override
        protected <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return mapReduce(select, scope, map, (store, f, i, t) -> { t.add(store.process(f)); return t; }, reduce);
        }

        protected <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach)
        {
            mapReduce(select, scope, forEach, (store, f, i, t) -> { t.add(store.process(f)); return t; }, (Void i1, Void i2) -> null);
        }
    }

    public static class Debug extends SingleThread
    {
        public Debug(int num, Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
        {
            super(num, node, uniqueNow, agent, store, CommandStore.Debug::new);
        }
    }

}
