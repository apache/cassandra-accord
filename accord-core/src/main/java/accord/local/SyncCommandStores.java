package accord.local;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.primitives.Routables;
import accord.utils.*;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

// TODO (desired, testing): introduce new CommandStores that mimics asynchrony by integrating with Cluster scheduling for List workload
public class SyncCommandStores extends CommandStores<SyncCommandStores.SyncCommandStore>
{
    public interface SafeSyncCommandStore extends SafeCommandStore
    {
    }

    public static abstract class SyncCommandStore extends CommandStore
    {
        public SyncCommandStore(int id)
        {
            super(id);
        }
        protected abstract <T> T executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function);
    }

    public SyncCommandStores(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        super(time, agent, store, shardDistributor, progressLogFactory, shardFactory);
    }

    protected static class SyncMapReduceAdapter<O> implements MapReduceAdapter<SyncCommandStore, O, O, O, O>
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
        public O apply(Function<? super SafeCommandStore, O> map, SyncCommandStore commandStore, PreLoadContext context)
        {
            return commandStore.executeSync(context, map);
        }

        @Override
        public O reduce(Reduce<O> reduce, O prev, O next)
        {
            return prev == SENTINEL ? next : reduce.reduce(prev, next);
        }

        @Override
        public void consume(ReduceConsume<O> reduceAndConsume, O result)
        {
            reduceAndConsume.accept(result, null);
        }

        @Override
        public O reduce(Reduce<O> reduce, O result)
        {
            return result == SENTINEL ? null : result;
        }
    }

    protected static class SyncFutureMapReduceAdapter<O> implements MapReduceAdapter<SyncCommandStore, Future<O>, List<Future<O>>, Future<O>, O>
    {
        private static final SyncFutureMapReduceAdapter INSTANCE = new SyncFutureMapReduceAdapter<>();
        public static <O> SyncFutureMapReduceAdapter<O> instance() { return INSTANCE; }

        @Override
        public List<Future<O>> allocate()
        {
            return new ArrayList<>();
        }

        @Override
        public Future<O> apply(Function<? super SafeCommandStore, Future<O>> map, SyncCommandStore commandStore, PreLoadContext context)
        {
            return commandStore.executeSync(context, map);
        }

        @Override
        public List<Future<O>> reduce(Reduce<O> reduce, List<Future<O>> prev, Future<O> next)
        {
            prev.add(next);
            return prev;
        }

        @Override
        public void consume(ReduceConsume<O> consume, Future<O> reduced)
        {
            reduced.addCallback(consume);
        }

        @Override
        public Future<O> reduce(Reduce<O> reduce, List<Future<O>> futures)
        {
            if (futures.isEmpty())
                return ImmediateFuture.success(null);

            return ReducingFuture.reduce(futures, reduce);
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

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, AsyncMapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        try
        {
            mapReduceConsume(context, keys, minEpoch, maxEpoch, mapReduceConsume, SyncFutureMapReduceAdapter.INSTANCE);
        }
        catch (Throwable t)
        {
            mapReduceConsume.accept(null, t);
        }
    }
}
