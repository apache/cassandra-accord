package accord.impl;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.txn.Keys;

import java.util.function.Consumer;

import static java.lang.Boolean.FALSE;

public abstract class InMemoryCommandStores extends CommandStores
{
    public InMemoryCommandStores(int num, Node node, Agent agent, DataStore store,
                                 ProgressLog.Factory progressLogFactory)
    {
        super(num, node, agent, store, progressLogFactory);
    }

    public static InMemoryCommandStores inMemory(Node node)
    {
        return (InMemoryCommandStores) node.commandStores();
    }

    public void forEachLocal(Consumer<? super CommandStore> forEach)
    {
        foldl((ranges, o, minEpoch, maxEpoch) -> ranges.all(),
              null, Long.MIN_VALUE, Long.MAX_VALUE,
              (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
    }

    public void forEachLocal(Keys keys, long minEpoch, long maxEpoch, Consumer<? super CommandStore> forEach)
    {
        foldl(ShardedRanges::shards, keys, minEpoch, maxEpoch, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
    }

    public void forEachLocal(Keys keys, long epoch, Consumer<? super CommandStore> forEach)
    {
        forEachLocal(keys, epoch, epoch, forEach);
    }

    public void forEachLocalSince(Keys keys, long epoch, Consumer<? super CommandStore> forEach)
    {
        forEachLocal(keys, epoch, Long.MAX_VALUE, forEach);
    }

    public static class Synchronized extends InMemoryCommandStores
    {
        public Synchronized(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory);
        }

        @Override
        protected CommandStore createCommandStore(int generation, int index, int numShards, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.RangesForEpoch rangesForEpoch)
        {
            return new InMemoryCommandStore.Synchronized(generation, index, numShards, node, agent, store, progressLogFactory, rangesForEpoch);
        }
    }

    public static class SingleThread extends InMemoryCommandStores
    {
        public SingleThread(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory);
        }

        @Override
        protected CommandStore createCommandStore(int generation, int index, int numShards, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.RangesForEpoch rangesForEpoch)
        {
            return new InMemoryCommandStore.SingleThread(generation, index, numShards, node, agent, store, progressLogFactory, rangesForEpoch);
        }
    }

    public static class Debug extends InMemoryCommandStores.SingleThread
    {
        public Debug(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory);
        }

        @Override
        protected CommandStore createCommandStore(int generation, int index, int numShards, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.RangesForEpoch rangesForEpoch)
        {
            return new InMemoryCommandStore.SingleThreadDebug(generation, index, numShards, node, agent, store, progressLogFactory, rangesForEpoch);
        }
    }

}
