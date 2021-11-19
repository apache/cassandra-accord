package accord.local;

import accord.api.Agent;
import accord.api.Key;
import accord.api.KeyRange;
import accord.api.Store;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore
{
    static class Mapping
    {
        static final Mapping EMPTY = new Mapping(KeyRanges.EMPTY, Topology.EMPTY);
        final KeyRanges ranges;
        final Topology topology;

        public Mapping(KeyRanges ranges, Topology topology)
        {
            this.ranges = ranges;
            this.topology = topology;
        }
    }

    public interface Factory
    {
        CommandStore create(int index,
                            Node.Id nodeId,
                            Function<Timestamp, Timestamp> uniqueNow,
                            Agent agent,
                            Store store,
                            IntFunction<Mapping> mappingSupplier);
        Factory SYNCHRONIZED = Synchronized::new;
        Factory SINGLE_THREAD = SingleThread::new;
        Factory SINGLE_THREAD_DEBUG = SingleThreadDebug::new;
    }

    private final int index;
    private final Node.Id nodeId;
    private final Function<Timestamp, Timestamp> uniqueNow;
    private final Agent agent;
    private final Store store;
    private final IntFunction<Mapping> mappingSupplier;
    private Mapping rangeMap;

    private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
    private final NavigableMap<Key, CommandsForKey> commandsForKey = new TreeMap<>();

    public CommandStore(int index,
                        Node.Id nodeId,
                        Function<Timestamp, Timestamp> uniqueNow,
                        Agent agent,
                        Store store,
                        IntFunction<Mapping> mappingSupplier)
    {
        this.index = index;
        this.nodeId = nodeId;
        this.uniqueNow = uniqueNow;
        this.agent = agent;
        this.store = store;
        this.mappingSupplier = mappingSupplier;
        rangeMap = mappingSupplier.apply(index);
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

    public boolean hasCommandsForKey(Key key)
    {
        return commandsForKey.containsKey(key);
    }

    public Store store()
    {
        return store;
    }

    public Timestamp uniqueNow(Timestamp atLeast)
    {
        return uniqueNow.apply(atLeast);
    }

    public Agent agent()
    {
        return agent;
    }

    public Node.Id nodeId()
    {
        return nodeId;
    }

    public long epoch()
    {
        return rangeMap.topology.epoch();
    }

    public KeyRanges ranges()
    {
        // TODO: check thread safety of callers
        return rangeMap.ranges;
    }

    void purgeRanges(KeyRanges removed)
    {
        for (KeyRange range : removed)
        {
            NavigableMap<Key, CommandsForKey> subMap = commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive());
            Iterator<Key> keyIterator = subMap.keySet().iterator();
            while (keyIterator.hasNext())
            {
                Key key = keyIterator.next();
                CommandsForKey forKey = commandsForKey.get(key);
                if (forKey != null)
                {
                    for (Command command : forKey)
                        if (command.txn() != null && !rangeMap.ranges.intersects(command.txn().keys))
                            commands.remove(command.txnId());
                }
                keyIterator.remove();
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

    public int index()
    {
        return index;
    }

    public boolean intersects(Keys keys)
    {
        return rangeMap.ranges.intersects(keys);
    }

    public static void onEach(Collection<CommandStore> stores, Consumer<? super CommandStore> consumer)
    {
        for (CommandStore store : stores)
            store.process(consumer);
    }

    private void refreshRangeMap()
    {
        rangeMap = mappingSupplier.apply(index);
    }

    <R> void processInternal(Function<? super CommandStore, R> function, CompletableFuture<R> future)
    {
        try
        {
            refreshRangeMap();
            future.complete(function.apply(this));
        }
        catch (Throwable e)
        {
            future.completeExceptionally(e);
        }
    }

    void processInternal(Consumer<? super CommandStore> consumer, CompletableFuture<Void> future)
    {
        try
        {
            refreshRangeMap();
            consumer.accept(this);
            future.complete(null);
        }
        catch (Throwable e)
        {
            future.completeExceptionally(e);
        }
    }

    public abstract <R> CompletionStage<R> process(Function<? super CommandStore, R> function);

    public abstract CompletionStage<Void> process(Consumer<? super CommandStore> consumer);

    public abstract void shutdown();

    public static class Synchronized extends CommandStore
    {
        public Synchronized(int index,
                            Node.Id nodeId,
                            Function<Timestamp, Timestamp> uniqueNow,
                            Agent agent,
                            Store store,
                            IntFunction<Mapping> mappingSupplier)
        {
            super(index, nodeId, uniqueNow, agent, store, mappingSupplier);
        }

        @Override
        public synchronized <R> CompletionStage<R> process(Function<? super CommandStore, R> func)
        {
            CompletableFuture<R> future = new CompletableFuture<>();
            processInternal(func, future);
            return future;
        }

        @Override
        public synchronized CompletionStage<Void> process(Consumer<? super CommandStore> consumer)
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            processInternal(consumer, future);
            return future;
        }

        @Override
        public void shutdown() {}
    }

    public static class SingleThread extends CommandStore
    {
        private final ExecutorService executor;

        private class FunctionWrapper<R> extends CompletableFuture<R> implements Runnable
        {
            private final Function<? super CommandStore, R> function;

            public FunctionWrapper(Function<? super CommandStore, R> function)
            {
                this.function = function;
            }

            @Override
            public void run()
            {
                processInternal(function, this);
            }
        }

        private class ConsumerWrapper extends CompletableFuture<Void> implements Runnable
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

        public SingleThread(int index,
                            Node.Id nodeId,
                            Function<Timestamp, Timestamp> uniqueNow,
                            Agent agent,
                            Store store,
                            IntFunction<Mapping> mappingSupplier)
        {
            super(index, nodeId, uniqueNow, agent, store, mappingSupplier);
            executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + nodeId + ':' + index + ']');
                return thread;
            });
        }

        @Override
        public <R> CompletionStage<R> process(Function<? super CommandStore, R> function)
        {
            FunctionWrapper<R> future = new FunctionWrapper<>(function);
            executor.execute(future);
            return future;
        }

        @Override
        public CompletionStage<Void> process(Consumer<? super CommandStore> consumer)
        {
            ConsumerWrapper future = new ConsumerWrapper(consumer);
            executor.execute(future);
            return future;
        }

        @Override
        public void shutdown()
        {
            executor.shutdown();
        }
    }

    public static class SingleThreadDebug extends SingleThread
    {
        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();

        public SingleThreadDebug(int index,
                                 Node.Id nodeId,
                                 Function<Timestamp, Timestamp> uniqueNow,
                                 Agent agent,
                                 Store store,
                                 IntFunction<Mapping> mappingSupplier)
        {
            super(index, nodeId, uniqueNow, agent, store, mappingSupplier);
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
        <R> void processInternal(Function<? super CommandStore, R> function, CompletableFuture<R> future)
        {
            assertThread();
            super.processInternal(function, future);
        }

        @Override
        void processInternal(Consumer<? super CommandStore> consumer, CompletableFuture<Void> future)
        {
            assertThread();
            super.processInternal(consumer, future);
        }
    }
}
