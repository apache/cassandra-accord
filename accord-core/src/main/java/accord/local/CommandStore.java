package accord.local;

import accord.api.Agent;
import accord.api.Key;
import accord.local.CommandStores.ShardedRanges;
import accord.api.ProgressLog;
import accord.topology.KeyRange;
import accord.api.DataStore;
import accord.topology.KeyRanges;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;

import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore
{
    public interface Factory
    {
        CommandStore create(int generation,
                            int shardIndex,
                            int numShards,
                            Node node,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpoch rangesForEpoch);
    }

    public interface RangesForEpoch
    {
        KeyRanges at(long epoch);
        KeyRanges since(long epoch);
    }

    private final int generation;
    private final int shardIndex;
    private final int numShards;
    private final Node node;
    private final Agent agent;
    private final DataStore store;
    private final ProgressLog progressLog;
    private final RangesForEpoch rangesForEpoch;

    private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
    private final NavigableMap<Key, CommandsForKey> commandsForKey = new TreeMap<>();

    public CommandStore(int generation,
                        int shardIndex,
                        int numShards,
                        Node node,
                        Agent agent,
                        DataStore store,
                        ProgressLog.Factory progressLogFactory,
                        RangesForEpoch rangesForEpoch)
    {
        Preconditions.checkArgument(shardIndex < numShards);
        this.generation = generation;
        this.shardIndex = shardIndex;
        this.numShards = numShards;
        this.node = node;
        this.agent = agent;
        this.store = store;
        this.progressLog = progressLogFactory.create(this);
        this.rangesForEpoch = rangesForEpoch;
    }

    public Command ifPresent(TxnId txnId)
    {
        return commands.get(txnId);
    }

    public Command command(TxnId txnId)
    {
        return commands.computeIfAbsent(txnId, id -> new Command(this, id));
    }

    public boolean hasCommand(TxnId txnId)
    {
        return commands.containsKey(txnId);
    }

    public CommandsForKey commandsForKey(Key key)
    {
        return commandsForKey.computeIfAbsent(key, ignore -> new CommandsForKey());
    }

    public CommandsForKey maybeCommandsForKey(Key key)
    {
        return commandsForKey.get(key);
    }

    public boolean hasCommandsForKey(Key key)
    {
        return commandsForKey.containsKey(key);
    }

    public DataStore store()
    {
        return store;
    }

    public Timestamp uniqueNow(Timestamp atLeast)
    {
        return node.uniqueNow(atLeast);
    }

    public Agent agent()
    {
        return agent;
    }

    public ProgressLog progressLog()
    {
        return progressLog;
    }

    public Node node()
    {
        return node;
    }

    public RangesForEpoch ranges()
    {
        return rangesForEpoch;
    }

    public long latestEpoch()
    {
        // TODO: why not inject the epoch to each command store?
        return node.epoch();
    }

    protected Timestamp maxConflict(Keys keys)
    {
        return keys.stream()
                   .map(this::maybeCommandsForKey)
                   .filter(Objects::nonNull)
                   .map(CommandsForKey::max)
                   .max(Comparator.naturalOrder())
                   .orElse(Timestamp.NONE);
    }

    public void forEpochCommands(KeyRanges ranges, long epoch, Consumer<Command> consumer)
    {
        Timestamp minTimestamp = new Timestamp(epoch, Long.MIN_VALUE, Integer.MIN_VALUE, Node.Id.NONE);
        Timestamp maxTimestamp = new Timestamp(epoch, Long.MAX_VALUE, Integer.MAX_VALUE, Node.Id.MAX);
        for (KeyRange range : ranges)
        {
            Iterable<CommandsForKey> rangeCommands = commandsForKey.subMap(range.start(),
                                                                           range.startInclusive(),
                                                                           range.end(),
                                                                           range.endInclusive()).values();
            for (CommandsForKey commands : rangeCommands)
            {
                commands.forWitnessed(minTimestamp, maxTimestamp, consumer);
            }
        }
    }

    public void forCommittedInEpoch(KeyRanges ranges, long epoch, Consumer<Command> consumer)
    {
        Timestamp minTimestamp = new Timestamp(epoch, Long.MIN_VALUE, Integer.MIN_VALUE, Node.Id.NONE);
        Timestamp maxTimestamp = new Timestamp(epoch, Long.MAX_VALUE, Integer.MAX_VALUE, Node.Id.MAX);
        for (KeyRange range : ranges)
        {
            Iterable<CommandsForKey> rangeCommands = commandsForKey.subMap(range.start(),
                                                                           range.startInclusive(),
                                                                           range.end(),
                                                                           range.endInclusive()).values();
            for (CommandsForKey commands : rangeCommands)
            {

                Collection<Command> committed = commands.committedByExecuteAt.subMap(minTimestamp,
                                                                                     true,
                                                                                     maxTimestamp,
                                                                                     true).values();
                committed.forEach(consumer);
            }
        }
    }

    public boolean hashIntersects(Key key)
    {
        return ShardedRanges.keyIndex(key, numShards) == shardIndex;
    }

    public boolean intersects(Keys keys, KeyRanges ranges)
    {
        return keys.any(ranges, this::hashIntersects);
    }

    public static void onEach(Collection<CommandStore> stores, Consumer<? super CommandStore> consumer)
    {
        for (CommandStore store : stores)
            store.process(consumer);
    }

    <R> void processInternal(Function<? super CommandStore, R> function, Promise<R> promise)
    {
        try
        {
            promise.setSuccess(function.apply(this));
        }
        catch (Throwable e)
        {
            promise.tryFailure(e);
        }
    }

    void processInternal(Consumer<? super CommandStore> consumer, Promise<Void> promise)
    {
        try
        {
            consumer.accept(this);
            promise.setSuccess(null);
        }
        catch (Throwable e)
        {
            promise.tryFailure(e);
        }
    }

    public abstract Future<Void> process(Consumer<? super CommandStore> consumer);

    public abstract <T> Future<T> process(Function<? super CommandStore, T> function);

    public void processBlocking(Consumer<? super CommandStore> consumer)
    {
        try
        {
            process(consumer).get();
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

    public abstract void shutdown();

    public static class Synchronized extends CommandStore
    {
        public Synchronized(int generation, int index, int numShards, Node node,
                            Agent agent, DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpoch rangesForEpoch)
        {
            super(generation, index, numShards, node, agent, store, progressLogFactory, rangesForEpoch);
        }

        @Override
        public synchronized Future<Void> process(Consumer<? super CommandStore> consumer)
        {
            AsyncPromise<Void> promise = new AsyncPromise<>();
            processInternal(consumer, promise);
            return promise;
        }

        @Override
        public <T> Future<T> process(Function<? super CommandStore, T> function)
        {
            AsyncPromise<T> promise = new AsyncPromise<>();
            processInternal(function, promise);
            return promise;
        }

        @Override
        public void shutdown() {}
    }

    public static class SingleThread extends CommandStore
    {
        private final ExecutorService executor;

        private class ConsumerWrapper extends AsyncPromise<Void> implements Runnable
        {
            private final Consumer<? super CommandStore> consumer;

            public ConsumerWrapper(Consumer<? super CommandStore> consumer)
            {
                this.consumer = consumer;
            }

            @Override
            public void run()
            {
                processInternal(consumer, this);
            }
        }

        private class FunctionWrapper<T> extends AsyncPromise<T> implements Runnable
        {
            private final Function<? super CommandStore, T> function;

            public FunctionWrapper(Function<? super CommandStore, T> function)
            {
                this.function = function;
            }

            @Override
            public void run()
            {
                processInternal(function, this);
            }
        }

        public SingleThread(int generation, int index, int numShards, Node node,
                            Agent agent, DataStore store, ProgressLog.Factory progressLogFactory,
                            RangesForEpoch rangesForEpoch)
        {
            super(generation, index, numShards, node, agent, store, progressLogFactory, rangesForEpoch);
            executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + node.id() + ':' + index + ']');
                return thread;
            });
        }

        @Override
        public Future<Void> process(Consumer<? super CommandStore> consumer)
        {
            ConsumerWrapper future = new ConsumerWrapper(consumer);
            executor.execute(future);
            return future;
        }

        @Override
        public <T> Future<T> process(Function<? super CommandStore, T> function)
        {
            FunctionWrapper<T> future = new FunctionWrapper<>(function);
            executor.execute(future);
            return future;
        }

        @Override
        public void shutdown()
        {
            executor.shutdown();
        }
    }

    static class Debug extends SingleThread
    {
        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();

        public Debug(int generation, int index, int numShards, Node node,
                     Agent agent, DataStore store, ProgressLog.Factory progressLogFactory,
                     RangesForEpoch rangesForEpoch)
        {
            super(generation, index, numShards, node, agent, store, progressLogFactory, rangesForEpoch);
        }

        private void assertThread()
        {
            Thread current = Thread.currentThread();
            Thread expected;
            while (true)
            {
                expected = expectedThread.get();
                if (expected != null)
                    break;
                expectedThread.compareAndSet(null, Thread.currentThread());
            }
            Preconditions.checkState(expected == current);
        }

        @Override
        public Command command(TxnId txnId)
        {
            assertThread();
            return super.command(txnId);
        }

        @Override
        public boolean hasCommand(TxnId txnId)
        {
            assertThread();
            return super.hasCommand(txnId);
        }

        @Override
        public CommandsForKey commandsForKey(Key key)
        {
            assertThread();
            return super.commandsForKey(key);
        }

        @Override
        public boolean hasCommandsForKey(Key key)
        {
            assertThread();
            return super.hasCommandsForKey(key);
        }

        @Override
        <R> void processInternal(Function<? super CommandStore, R> function, Promise<R> future)
        {
            assertThread();
            super.processInternal(function, future);
        }

        @Override
        void processInternal(Consumer<? super CommandStore> consumer, Promise<Void> future)
        {
            assertThread();
            super.processInternal(consumer, future);
        }
    }

}
