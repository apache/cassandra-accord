/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.impl;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.local.*;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.Promise;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public abstract class InMemoryCommandStore extends CommandStore
{
    private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
    private final NavigableMap<Key, CommandsForKey> commandsForKey = new TreeMap<>();

    public static InMemoryCommandStore inMemory(CommandStore commandStore)
    {
        return (InMemoryCommandStore) commandStore;
    }

    public InMemoryCommandStore(int generation, int index, int numShards, Function<Timestamp, Timestamp> uniqueNow, LongSupplier currentEpoch, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
    {
        super(generation, index, numShards, uniqueNow, currentEpoch, agent, store, progressLogFactory, rangesForEpoch);
    }

    @Override
    public Command ifPresent(TxnId txnId)
    {
        return commands.get(txnId);
    }

    @Override
    public Command command(TxnId txnId)
    {
        return commands.computeIfAbsent(txnId, id -> new InMemoryCommand(this, id));
    }

    public boolean hasCommand(TxnId txnId)
    {
        return commands.containsKey(txnId);
    }

    @Override
    public CommandsForKey commandsForKey(Key key)
    {
        return commandsForKey.computeIfAbsent(key, InMemoryCommandsForKey::new);
    }

    public boolean hasCommandsForKey(Key key)
    {
        return commandsForKey.containsKey(key);
    }

    @Override
    public CommandsForKey maybeCommandsForKey(Key key)
    {
        return commandsForKey.get(key);
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
                commands.forWitnessed(minTimestamp, maxTimestamp, cmd -> consumer.accept((Command) cmd));
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

                Collection<Command> committed = commands.committedByExecuteAt()
                        .between(minTimestamp, maxTimestamp).map(cmd -> (Command) cmd).collect(Collectors.toList());
                committed.forEach(consumer);
            }
        }
    }

    protected void processInternal(Consumer<? super CommandStore> consumer, Promise<Void> promise)
    {
        processInternal(cs -> {
            consumer.accept(cs);
            return null;
        }, promise);
    }

    protected <T> void processInternal(Function<? super CommandStore, T> function, Promise<T> promise)
    {
        try
        {
            T result = function.apply(this);
            promise.setSuccess(result);
        }
        catch (Throwable e)
        {
            promise.tryFailure(e);
        }
    }

    public static class Synchronized extends InMemoryCommandStore
    {
        public Synchronized(int generation,
                            int index,
                            int numShards,
                            Function<Timestamp, Timestamp> uniqueNow,
                            LongSupplier currentEpoch,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpoch rangesForEpoch)
        {
            super(generation, index, numShards, uniqueNow, currentEpoch, agent, store, progressLogFactory, rangesForEpoch);
        }

        @Override
        public synchronized Future<Void> processSetup(Consumer<? super CommandStore> function)
        {
            AsyncPromise<Void> promise = new AsyncPromise<>();
            processInternal(function, promise);
            return promise;
        }

        @Override
        public synchronized <T> Future<T> processSetup(Function<? super CommandStore, T> function)
        {
            AsyncPromise<T> promise = new AsyncPromise<>();
            processInternal(function, promise);
            return promise;
        }

        @Override
        public synchronized Future<Void> process(TxnOperation unused, Consumer<? super CommandStore> consumer)
        {
            Promise<Void> promise = new AsyncPromise<>();
            processInternal(consumer, promise);
            return promise;
        }

        @Override
        public synchronized <T> Future<T> process(TxnOperation unused, Function<? super CommandStore, T> function)
        {
            AsyncPromise<T> promise = new AsyncPromise<>();
            processInternal(function, promise);
            return promise;
        }

        @Override
        public synchronized void shutdown() {}
    }

    public static class SingleThread extends InMemoryCommandStore
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

        public SingleThread(int generation,
                            int index,
                            int numShards,
                            Node.Id nodeId,
                            Function<Timestamp, Timestamp> uniqueNow,
                            LongSupplier currentEpoch,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpoch rangesForEpoch)
        {
            super(generation, index, numShards, uniqueNow, currentEpoch, agent, store, progressLogFactory, rangesForEpoch);
            executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + nodeId + ':' + index + ']');
                return thread;
            });
        }

        @Override
        public Future<Void> processSetup(Consumer<? super CommandStore> function)
        {
            ConsumerWrapper future = new ConsumerWrapper(function);
            executor.execute(future);
            return future;
        }

        @Override
        public <T> Future<T> processSetup(Function<? super CommandStore, T> function)
        {
            FunctionWrapper<T> future = new FunctionWrapper<>(function);
            executor.execute(future);
            return future;
        }

        @Override
        public Future<Void> process(TxnOperation unused, Consumer<? super CommandStore> consumer)
        {
            ConsumerWrapper future = new ConsumerWrapper(consumer);
            executor.execute(future);
            return future;
        }

        @Override
        public <T> Future<T> process(TxnOperation unused, Function<? super CommandStore, T> function)
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

    public static class SingleThreadDebug extends SingleThread
    {
        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();

        public SingleThreadDebug(int generation,
                                 int index,
                                 int numShards,
                                 Node.Id nodeId,
                                 Function<Timestamp, Timestamp> uniqueNow,
                                 LongSupplier currentEpoch,
                                 Agent agent,
                                 DataStore store,
                                 ProgressLog.Factory progressLogFactory,
                                 RangesForEpoch rangesForEpoch)
        {
            super(generation, index, numShards, nodeId, uniqueNow, currentEpoch, agent, store, progressLogFactory, rangesForEpoch);
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
            if (expected != current)
                throw new IllegalStateException(String.format("Command store called from the wrong thread. Expected %s, got %s", expected, current));
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
        protected void processInternal(Consumer<? super CommandStore> consumer, Promise<Void> promise)
        {
            assertThread();
            super.processInternal(consumer, promise);
        }
    }
}
