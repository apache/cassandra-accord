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
import accord.local.CommandStore; // java8 fails compilation if this is in correct position
import accord.local.SyncCommandStores.SyncCommandStore; // java8 fails compilation if this is in correct position
import accord.impl.InMemoryCommandStore.SingleThread.AsyncState;
import accord.impl.InMemoryCommandStore.Synchronized.SynchronizedState;
import accord.local.Command;
import accord.local.CommandStore.RangesForEpoch;
import accord.local.CommandsForKey;
import accord.local.CommandListener;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.SyncCommandStores;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.*;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class InMemoryCommandStore
{
    public static abstract class State implements SafeCommandStore
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog progressLog;
        private final RangesForEpoch rangesForEpoch;

        private final CommandStore commandStore;
        private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
        private final NavigableMap<RoutableKey, InMemoryCommandsForKey> commandsForKey = new TreeMap<>();

        public State(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpoch rangesForEpoch, CommandStore commandStore)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.progressLog = progressLog;
            this.rangesForEpoch = rangesForEpoch;
            this.commandStore = commandStore;
        }

        public Command ifPresent(TxnId txnId)
        {
            return commands.get(txnId);
        }

        // TODO (soon): mimic caching to test C* behaviour
        public Command ifLoaded(TxnId txnId)
        {
            return commands.get(txnId);
        }

        public Command command(TxnId txnId)
        {
            return commands.computeIfAbsent(txnId, id -> new InMemoryCommand(commandStore, id));
        }

        public boolean hasCommand(TxnId txnId)
        {
            return commands.containsKey(txnId);
        }

        public CommandsForKey commandsForKey(Key key)
        {
            return commandsForKey.computeIfAbsent(key, k -> new InMemoryCommandsForKey((Key) k));
        }

        public boolean hasCommandsForKey(Key key)
        {
            return commandsForKey.containsKey(key);
        }

        public CommandsForKey maybeCommandsForKey(Key key)
        {
            return commandsForKey.get(key);
        }

        public void addAndInvokeListener(TxnId txnId, CommandListener listener)
        {
            command(txnId).addListener(listener);
        }

        @Override
        public DataStore dataStore()
        {
            return store;
        }

        @Override
        public CommandStore commandStore()
        {
            return commandStore;
        }

        @Override
        public Agent agent()
        {
            return agent;
        }

        @Override
        public ProgressLog progressLog()
        {
            return progressLog;
        }

        @Override
        public RangesForEpoch ranges()
        {
            return rangesForEpoch;
        }

        @Override
        public long latestEpoch()
        {
            return time.epoch();
        }

        @Override
        public Timestamp preaccept(TxnId txnId, Seekables<?, ?> keys)
        {
            Timestamp max = maxConflict(keys, ranges().at(txnId.epoch));
            long epoch = latestEpoch();
            if (txnId.compareTo(max) > 0 && txnId.epoch >= epoch && !agent.isExpired(txnId, time.now()))
                return txnId;

            return time.uniqueNow(max);
        }

        @Override
        public NodeTimeService time()
        {
            return time;
        }

        private Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
        {
            return mapReduce(keysOrRanges, slice, CommandsForKey::max, (a, b) -> a.compareTo(b) >= 0 ? a : b, Timestamp.NONE);
        }

        public void forEpochCommands(Ranges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = new Timestamp(epoch, Long.MIN_VALUE, Integer.MIN_VALUE, Node.Id.NONE);
            Timestamp maxTimestamp = new Timestamp(epoch, Long.MAX_VALUE, Integer.MAX_VALUE, Node.Id.MAX);
            for (Range range : ranges)
            {
                Iterable<InMemoryCommandsForKey> rangeCommands = commandsForKey.subMap(
                        range.start(), range.startInclusive(),
                        range.end(), range.endInclusive()
                ).values();
                for (InMemoryCommandsForKey commands : rangeCommands)
                {
                    commands.forWitnessed(minTimestamp, maxTimestamp, cmd -> consumer.accept((Command) cmd));
                }
            }
        }

        public void forCommittedInEpoch(Ranges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = new Timestamp(epoch, Long.MIN_VALUE, Integer.MIN_VALUE, Node.Id.NONE);
            Timestamp maxTimestamp = new Timestamp(epoch, Long.MAX_VALUE, Integer.MAX_VALUE, Node.Id.MAX);
            for (Range range : ranges)
            {
                Iterable<InMemoryCommandsForKey> rangeCommands = commandsForKey.subMap(range.start(),
                        range.startInclusive(),
                        range.end(),
                        range.endInclusive()).values();
                for (InMemoryCommandsForKey commands : rangeCommands)
                {
                    Collection<Command> committed = commands.committedByExecuteAt()
                            .between(minTimestamp, maxTimestamp).map(cmd -> (Command) cmd).collect(Collectors.toList());
                    committed.forEach(consumer);
                }
            }
        }

        public <T> T mapReduce(Routables<?, ?> keysOrRanges, Ranges slice, Function<CommandsForKey, T> map, BinaryOperator<T> reduce, T initialValue)
        {
            switch (keysOrRanges.kindOfContents()) {
                default:
                    throw new AssertionError();
                case Key:
                    // TODO: efficiency
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    return keys.stream()
                            .filter(slice::contains)
                            .filter(commandStore::hashIntersects)
                            .map(this::commandsForKey)
                            .map(map)
                            .reduce(initialValue, reduce);
                case Range:
                    // TODO: efficiency
                    Ranges ranges = (Ranges) keysOrRanges;
                    return ranges.slice(slice).stream().flatMap(range ->
                            commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive()).values().stream()
                    ).map(map).reduce(initialValue, reduce);
            }
        }

        public void forEach(Routables<?, ?> keysOrRanges, Ranges slice, Consumer<CommandsForKey> forEach)
        {
            switch (keysOrRanges.kindOfContents()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    keys.forEach(slice, key -> {
                        if (commandStore.hashIntersects(key))
                            forEach.accept(commandsForKey(key));
                    });
                    break;
                case Range:
                    Ranges ranges = (Ranges) keysOrRanges;
                    // TODO: zero allocation
                    ranges.slice(slice).forEach(range -> {
                        commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                                .values().forEach(forEach);
                    });
            }
        }

        public void forEach(Routable keyOrRange, Ranges slice, Consumer<CommandsForKey> forEach)
        {
            switch (keyOrRange.kind())
            {
                default: throw new AssertionError();
                case Key:
                    Key key = (Key) keyOrRange;
                    if (slice.contains(key))
                        forEach.accept(commandsForKey(key));
                    break;
                case Range:
                    Range range = (Range) keyOrRange;
                    // TODO: zero allocation
                    Ranges.of(range).slice(slice).forEach(r -> {
                        commandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive())
                                .values().forEach(forEach);
                    });
            }
        }
    }

    public static class Synchronized extends SyncCommandStore
    {
        public static class SynchronizedState extends State implements SyncCommandStores.SafeSyncCommandStore
        {
            public SynchronizedState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpoch rangesForEpoch, CommandStore commandStore)
            {
                super(time, agent, store, progressLog, rangesForEpoch, commandStore);
            }

            @Override
            public Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
            {
                return submit(context, i -> { consumer.accept(i); return null; });
            }

            public synchronized <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
            {
                AsyncPromise<T> promise = new AsyncPromise<>();
                try
                {
                    T result = function.apply(this);
                    promise.trySuccess(result);
                }
                catch (Throwable t)
                {
                    promise.tryFailure(t);
                }
                return promise;
            }

            public synchronized <T> T executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function)
            {
                return function.apply(this);
            }
        }

        final SynchronizedState state;

        public Synchronized(int id, int generation, int shardIndex, int numShards, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
        {
            super(id, generation, shardIndex, numShards);
            this.state = new SynchronizedState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
        }

        @Override
        public Agent agent()
        {
            return state.agent();
        }

        @Override
        public Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return state.execute(context, consumer);
        }

        @Override
        public <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return state.submit(context, function);
        }

        @Override
        public <T> T executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return state.executeSync(context, function);
        }

        @Override
        public void shutdown() {}
    }

    public static class SingleThread extends CommandStore
    {
        private class FunctionWrapper<T> extends AsyncPromise<T> implements Runnable
        {
            private final Function<? super SafeCommandStore, T> function;

            public FunctionWrapper(Function<? super SafeCommandStore, T> function)
            {
                this.function = function;
            }

            @Override
            public void run()
            {
                try
                {
                    trySuccess(function.apply(state));
                }
                catch (Throwable t)
                {
                    tryFailure(t);
                }
            }
        }

        class AsyncState extends State implements SafeCommandStore
        {
            public AsyncState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpoch rangesForEpoch, CommandStore commandStore)
            {
                super(time, agent, store, progressLog, rangesForEpoch, commandStore);
            }

            @Override
            public Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
            {
                return submit(context, i -> { consumer.accept(i); return null; });
            }

            @Override
            public <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
            {
                FunctionWrapper<T> future = new FunctionWrapper<>(function);
                executor.execute(future);
                return future;
            }
        }

        private final ExecutorService executor;
        private final AsyncState state;

        public SingleThread(int id, int generation, int shardIndex, int numShards, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
        {
            super(id, generation, shardIndex, numShards);
            executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + time.id() + ':' + shardIndex + ']');
                return thread;
            });
            state = newState(time, agent, store, progressLogFactory, rangesForEpoch);
        }

        AsyncState newState(NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
        {
            return new AsyncState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
        }

        @Override
        public Agent agent()
        {
            return state.agent();
        }

        @Override
        public Future<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return state.execute(context, consumer);
        }

        @Override
        public <T> Future<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return state.submit(context, function);
        }

        @Override
        public void shutdown()
        {
            executor.shutdown();
        }
    }

    public static class Debug extends SingleThread
    {
        class DebugState extends AsyncState
        {
            public DebugState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpoch rangesForEpoch, CommandStore commandStore)
            {
                super(time, agent, store, progressLog, rangesForEpoch, commandStore);
            }

            @Override
            public Command ifPresent(TxnId txnId)
            {
                assertThread();
                return super.ifPresent(txnId);
            }

            @Override
            public Command ifLoaded(TxnId txnId)
            {
                assertThread();
                return super.ifLoaded(txnId);
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
            public CommandsForKey maybeCommandsForKey(Key key)
            {
                assertThread();
                return super.maybeCommandsForKey(key);
            }

            @Override
            public void addAndInvokeListener(TxnId txnId, CommandListener listener)
            {
                assertThread();
                super.addAndInvokeListener(txnId, listener);
            }

            @Override
            public void forEpochCommands(Ranges ranges, long epoch, Consumer<Command> consumer)
            {
                assertThread();
                super.forEpochCommands(ranges, epoch, consumer);
            }

            @Override
            public void forCommittedInEpoch(Ranges ranges, long epoch, Consumer<Command> consumer)
            {
                assertThread();
                super.forCommittedInEpoch(ranges, epoch, consumer);
            }
        }

        private final AtomicReference<Thread> expectedThread = new AtomicReference<>();

        public Debug(int id, int generation, int shardIndex, int numShards, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
        {
            super(id, generation, shardIndex, numShards, time, agent, store, progressLogFactory, rangesForEpoch);
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
        AsyncState newState(NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpoch rangesForEpoch)
        {
            return new DebugState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
        }
    }

    public static State inMemory(CommandStore unsafeStore)
    {
        return (unsafeStore instanceof Synchronized) ? ((Synchronized) unsafeStore).state : ((SingleThread) unsafeStore).state;
    }

    public static State inMemory(SafeCommandStore safeStore)
    {
        return (safeStore instanceof SynchronizedState) ? ((SynchronizedState) safeStore) : ((AsyncState) safeStore);
    }
}
