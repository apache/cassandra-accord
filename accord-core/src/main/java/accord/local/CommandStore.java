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
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore
{
    public interface Factory
    {
        CommandStore create(int generation,
                            int index,
                            int numShards,
                            Node.Id nodeId,
                            Function<Timestamp, Timestamp> uniqueNow,
                            Agent agent,
                            Store store,
                            KeyRanges ranges,
                            Supplier<Topology> localTopologySupplier);
        Factory SYNCHRONIZED = Synchronized::new;
        Factory SINGLE_THREAD = SingleThread::new;
        Factory SINGLE_THREAD_DEBUG = SingleThreadDebug::new;
    }

    private final int generation;
    private final int index;
    private final int numShards;
    private final Node.Id nodeId;
    private final Function<Timestamp, Timestamp> uniqueNow;
    private final Agent agent;
    private final Store store;
    private final KeyRanges ranges;
    private final Supplier<Topology> localTopologySupplier;

    private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
    private final NavigableMap<Key, CommandsForKey> commandsForKey = new TreeMap<>();

    public CommandStore(int generation,
                        int index,
                        int numShards,
                        Node.Id nodeId,
                        Function<Timestamp, Timestamp> uniqueNow,
                        Agent agent,
                        Store store,
                        KeyRanges ranges,
                        Supplier<Topology> localTopologySupplier)
    {
        this.generation = generation;
        this.index = index;
        this.numShards = numShards;
        this.nodeId = nodeId;
        this.uniqueNow = uniqueNow;
        this.agent = agent;
        this.store = store;
        this.ranges = ranges;
        this.localTopologySupplier = localTopologySupplier;
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
        return localTopologySupplier.get().epoch();
    }

    public KeyRanges ranges()
    {
        return ranges;
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
                        if (command.txn() != null && !ranges.intersects(command.txn().keys))
                            commands.remove(command.txnId());
                }
                keyIterator.remove();
            }
        }
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

    public int index()
    {
        return index;
    }

    public boolean hashIntersects(Key key)
    {
        return CommandStores.keyIndex(key, numShards) == index;
    }

    public boolean intersects(Keys keys)
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

    public void processBlocking(Consumer<? super CommandStore> consumer)
    {
        try
        {
            process(consumer).get();
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e.getCause());
        }
    }

    public abstract void shutdown();

    public static class Synchronized extends CommandStore
    {
        public Synchronized(int generation,
                            int index,
                            int numShards,
                            Node.Id nodeId,
                            Function<Timestamp, Timestamp> uniqueNow,
                            Agent agent,
                            Store store,
                            KeyRanges ranges,
                            Supplier<Topology> localTopologySupplier)
        {
            super(generation, index, numShards, nodeId, uniqueNow, agent, store, ranges, localTopologySupplier);
        }

        @Override
        public synchronized Future<Void> process(Consumer<? super CommandStore> consumer)
        {
            AsyncPromise<Void> promise = new AsyncPromise<>();
            processInternal(consumer, promise);
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

        public SingleThread(int generation,
                            int index,
                            int numShards,
                            Node.Id nodeId,
                            Function<Timestamp, Timestamp> uniqueNow,
                            Agent agent,
                            Store store,
                            KeyRanges ranges,
                            Supplier<Topology> localTopologySupplier)
        {
            super(generation, index, numShards, nodeId, uniqueNow, agent, store, ranges, localTopologySupplier);
            executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + nodeId + ':' + index + ']');
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
        public void shutdown()
        {
            executor.shutdown();
        }
    }

    public static class SingleThreadDebug extends SingleThread
    {
        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();

        public SingleThreadDebug(int generation,
                                 int index,
                                 int numShards,
                                 Node.Id nodeId,
                                 Function<Timestamp, Timestamp> uniqueNow,
                                 Agent agent,
                                 Store store,
                                 KeyRanges ranges,
                                 Supplier<Topology> localTopologySupplier)
        {
            super(generation, index, numShards, nodeId, uniqueNow, agent, store, ranges, localTopologySupplier);
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
