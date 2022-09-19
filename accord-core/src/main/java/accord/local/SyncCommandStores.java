package accord.local;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.primitives.Routables;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;

import java.util.function.Function;
import java.util.stream.IntStream;

// TODO (soon): introduce new CommandStores that mimics asynchrony by integrating with Cluster scheduling for List workload
public class SyncCommandStores extends CommandStores<SyncCommandStores.SyncCommandStore>
{
    public interface SafeSyncCommandStore extends SafeCommandStore
    {
    }

    public static abstract class SyncCommandStore extends CommandStore
    {
        public SyncCommandStore(int id, int generation, int shardIndex, int numShards)
        {
            super(id, generation, shardIndex, numShards);
        }
        protected abstract <T> T executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function);
    }

    public SyncCommandStores(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        super(num, node, agent, store, progressLogFactory, shardFactory);
    }

    protected static class SyncMapReduceAdapter<O> implements MapReduceAdapter<SyncCommandStore, O, O, O>
    {
        private static final SyncMapReduceAdapter INSTANCE = new SyncMapReduceAdapter<>();
        public static <O> SyncMapReduceAdapter<O> instance() { return INSTANCE; }
        private static final Object SENTINEL = new Object();

        @Override
        public O allocate()
        {
            return (O)SENTINEL;
        }

        @Override
        public O apply(MapReduce<? super SafeCommandStore, O> map, SyncCommandStore commandStore, PreLoadContext context)
        {
            return commandStore.executeSync(context, map);
        }

        @Override
        public O reduce(MapReduce<? super SafeCommandStore, O> reduce, O prev, O next)
        {
            return prev == SENTINEL ? next : reduce.reduce(prev, next);
        }

        @Override
        public void consume(MapReduceConsume<?, O> reduceAndConsume, O result)
        {
            reduceAndConsume.accept(result, null);
        }

        @Override
        public O reduce(MapReduce<?, O> reduce, O result)
        {
            return result == SENTINEL ? null : result;
        }
    }

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        try
        {
            mapReduceConsume(context, keys, minEpoch, maxEpoch, mapReduceConsume, SyncMapReduceAdapter.INSTANCE);
        }
        catch (Throwable t)
        {
            mapReduceConsume.accept(null, t);
        }
    }

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        try
        {
            mapReduceConsume(context, commandStoreIds, mapReduceConsume, SyncMapReduceAdapter.INSTANCE);
        }
        catch (Throwable t)
        {
            mapReduceConsume.accept(null, t);
        }
    }
}
