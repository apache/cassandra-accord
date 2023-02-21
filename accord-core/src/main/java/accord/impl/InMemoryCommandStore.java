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

import accord.local.*;
import accord.local.SyncCommandStores.SyncCommandStore; // java8 fails compilation if this is in correct position
import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.InMemoryCommandStore.SingleThread.AsyncState;
import accord.impl.InMemoryCommandStore.Synchronized.SynchronizedState;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.local.CommandStores.RangesForEpoch;
import accord.impl.CommandsForKey.CommandTimeseries;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.local.SafeCommandStore.TestDep.*;
import static accord.local.SafeCommandStore.TestKind.Ws;
import static accord.local.Status.Committed;
import static accord.primitives.Routables.Slice.Minimal;

public class InMemoryCommandStore
{
    public static abstract class State implements SafeCommandStore
    {
        private final NodeTimeService time;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog progressLog;
        private final RangesForEpochHolder rangesForEpochHolder;
        private RangesForEpoch rangesForEpoch;

        private final CommandStore commandStore;
        private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
        private final NavigableMap<RoutableKey, InMemoryCommandsForKey> commandsForKey = new TreeMap<>();
        // TODO (find library, efficiency): this is obviously super inefficient, need some range map
        private final TreeMap<TxnId, RangeCommand> rangeCommands = new TreeMap<>();

        static class RangeCommand
        {
            final Command command;
            Ranges ranges;

            RangeCommand(Command command)
            {
                this.command = command;
            }

            void update(Ranges add)
            {
                if (ranges == null) ranges = add;
                else ranges = ranges.with(add);
            }
        }

        public State(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, CommandStore commandStore)
        {
            this.time = time;
            this.agent = agent;
            this.store = store;
            this.progressLog = progressLog;
            this.rangesForEpochHolder = rangesForEpoch;
            this.commandStore = commandStore;
        }

        @Override
        public Command ifPresent(TxnId txnId)
        {
            return commands.get(txnId);
        }

        // TODO (required): mimic caching to test C* behaviour
        @Override
        public Command ifLoaded(TxnId txnId)
        {
            return commands.get(txnId);
        }

        @Override
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

        @Override
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
            Invariants.checkState(rangesForEpoch != null);
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
            Timestamp max = maxConflict(keys, ranges().at(txnId.epoch()));
            long epoch = latestEpoch();
            if (txnId.compareTo(max) > 0 && txnId.epoch() >= epoch && !agent.isExpired(txnId, time.now()))
                return txnId;

            return time.uniqueNow(max);
        }

        void refreshRanges()
        {
            rangesForEpoch = rangesForEpochHolder.get();
        }

        @Override
        public NodeTimeService time()
        {
            return time;
        }

        private Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
        {
            Timestamp timestamp = mapReduceForKey(keysOrRanges, slice, (forKey, prev) -> Timestamp.max(forKey.max(), prev), Timestamp.NONE, null);
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            for (RangeCommand command : rangeCommands.values())
            {
                if (command.ranges.intersects(sliced))
                    timestamp = Timestamp.max(timestamp, command.command.executeAt());
            }
            return timestamp;
        }

        public void forEpochCommands(Ranges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = Timestamp.minForEpoch(epoch);
            Timestamp maxTimestamp = Timestamp.maxForEpoch(epoch);
            for (Range range : ranges)
            {
                Iterable<InMemoryCommandsForKey> rangeCommands = commandsForKey.subMap(
                        range.start(), range.startInclusive(),
                        range.end(), range.endInclusive()
                ).values();

                for (InMemoryCommandsForKey commands : rangeCommands)
                {
                    commands.forWitnessed(minTimestamp, maxTimestamp, consumer);
                }
            }
        }

        public void forCommittedInEpoch(Ranges ranges, long epoch, Consumer<Command> consumer)
        {
            Timestamp minTimestamp = Timestamp.minForEpoch(epoch);
            Timestamp maxTimestamp = Timestamp.maxForEpoch(epoch);
            for (Range range : ranges)
            {
                Iterable<InMemoryCommandsForKey> rangeCommands = commandsForKey.subMap(range.start(),
                        range.startInclusive(),
                        range.end(),
                        range.endInclusive()).values();
                for (InMemoryCommandsForKey commands : rangeCommands)
                {
                    commands.byExecuteAt()
                            .between(minTimestamp, maxTimestamp)
                            .filter(command -> command.hasBeen(Committed))
                            .forEach(consumer);
                }
            }
        }

        @Override
        public <T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<T, T> map, T accumulate, T terminalValue)
        {
            accumulate = mapReduceForKey(keysOrRanges, slice, (forKey, prev) -> {
                CommandTimeseries timeseries;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case STARTED_BEFORE:
                        timeseries = forKey.byId();
                        break;
                    case EXECUTES_AFTER:
                    case MAY_EXECUTE_BEFORE:
                        timeseries = forKey.byExecuteAt();
                }
                CommandTimeseries.TestTimestamp remapTestTimestamp;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case EXECUTES_AFTER:
                        remapTestTimestamp = CommandTimeseries.TestTimestamp.AFTER;
                        break;
                    case STARTED_BEFORE:
                    case MAY_EXECUTE_BEFORE:
                        remapTestTimestamp = CommandTimeseries.TestTimestamp.BEFORE;
                }
                return timeseries.mapReduce(testKind, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, prev, terminalValue);
            }, accumulate, terminalValue);

            if (accumulate.equals(terminalValue))
                return accumulate;

            // TODO (find lib, efficiency): this is super inefficient, need to store Command in something queryable
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            Map<Range, List<Command>> collect = new TreeMap<>(Range::compare);
            for (RangeCommand rangeCommand : rangeCommands.values())
            {
                Command command = rangeCommand.command;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                        if (command.txnId().compareTo(timestamp) < 0) continue;
                        else break;
                    case STARTED_BEFORE:
                        if (command.txnId().compareTo(timestamp) > 0) continue;
                        else break;
                    case EXECUTES_AFTER:
                        if (command.executeAt().compareTo(timestamp) < 0) continue;
                        else break;
                    case MAY_EXECUTE_BEFORE:
                        Timestamp compareTo = command.known().executeAt.hasDecidedExecuteAt() ? command.executeAt() : command.txnId();
                        if (compareTo.compareTo(timestamp) > 0) continue;
                        else break;
                }

                if (minStatus != null && command.status().compareTo(minStatus) < 0)
                    continue;

                if (maxStatus != null && command.status().compareTo(maxStatus) > 0)
                    continue;

                if (testKind == Ws && command.txnId().rw().isRead())
                    continue;

                if (testDep != ANY_DEPS)
                {
                    if (!command.known().deps.hasProposedOrDecidedDeps())
                        continue;

                    if ((testDep == WITH) == !command.partialDeps().contains(depId))
                        continue;
                }

                if (!rangeCommand.ranges.intersects(sliced))
                    continue;

                Routables.foldl(rangeCommand.ranges, sliced, (r, in, i) -> {
                    // TODO (easy, efficiency): pass command as a parameter to Fold
                    List<Command> list = in.computeIfAbsent(r, ignore -> new ArrayList<>());
                    if (list.isEmpty() || list.get(list.size() - 1) != command)
                            list.add(command);
                    return in;
                }, collect);
            }

            for (Map.Entry<Range, List<Command>> e : collect.entrySet())
            {
                for (Command command : e.getValue())
                    accumulate = map.apply(e.getKey(), command.txnId(), command.executeAt(), accumulate);
            }

            return accumulate;
        }

        @Override
        public void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
        {
            switch (keysOrRanges.domain())
            {
                default: throw new AssertionError();
                case Key:
                    forEach(keysOrRanges, slice, forKey -> forKey.register(command));
                    break;
                case Range:
                    rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(command))
                            .update((Ranges)keysOrRanges);
            }
        }

        @Override
        public void register(Seekable keyOrRange, Ranges slice, Command command)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    forEach(keyOrRange, slice, forKey -> forKey.register(command));
                    break;
                case Range:
                    rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(command))
                            .update(Ranges.of((Range)keyOrRange));
            }
        }

        private <O> O mapReduceForKey(Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<CommandsForKey, O, O> map, O accumulate, O terminalValue)
        {
            switch (keysOrRanges.domain()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    for (Key key : keys)
                    {
                        if (!slice.contains(key)) continue;
                        CommandsForKey forKey = commandsForKey(key);
                        accumulate = map.apply(forKey, accumulate);
                        if (accumulate.equals(terminalValue))
                            return accumulate;
                    }
                    break;
                case Range:
                    Ranges ranges = (Ranges) keysOrRanges;
                    Ranges sliced = ranges.slice(slice, Minimal);
                    for (Range range : sliced)
                    {
                        for (CommandsForKey forKey : commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive()).values())
                        {
                            accumulate = map.apply(forKey, accumulate);
                            if (accumulate.equals(terminalValue))
                                return accumulate;
                        }
                    }
            }
            return accumulate;
        }

        private void forEach(Seekables<?, ?> keysOrRanges, Ranges slice, Consumer<CommandsForKey> forEach)
        {
            switch (keysOrRanges.domain()) {
                default:
                    throw new AssertionError();
                case Key:
                    AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                    keys.forEach(slice, key -> forEach.accept(commandsForKey(key)));
                    break;
                case Range:
                    Ranges ranges = (Ranges) keysOrRanges;
                    ranges.slice(slice).forEach(range -> {
                        commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                                .values().forEach(forEach);
                    });
            }
        }

        private void forEach(Routable keyOrRange, Ranges slice, Consumer<CommandsForKey> forEach)
        {
            switch (keyOrRange.domain())
            {
                default: throw new AssertionError();
                case Key:
                    Key key = (Key) keyOrRange;
                    if (slice.contains(key))
                        forEach.accept(commandsForKey(key));
                    break;
                case Range:
                    Range range = (Range) keyOrRange;
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
            public SynchronizedState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, CommandStore commandStore)
            {
                super(time, agent, store, progressLog, rangesForEpoch, commandStore);
            }

            @Override
            public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
            {
                return submit(context, i -> { consumer.accept(i); return null; });
            }

            @Override
            public synchronized <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
            {
                return new AsyncChains.Head<T>()
                {
                    @Override
                    public void begin(BiConsumer<? super T, Throwable> callback)
                    {
                        T result = null;
                        Throwable failure = null;
                        synchronized (SynchronizedState.this)
                        {
                            try
                            {
                                result = function.apply(SynchronizedState.this);
                            }
                            catch (Throwable t)
                            {
                                failure = t;
                            }
                        }
                        callback.accept(result, failure);
                    }
                };
            }

            public synchronized <T> T executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function)
            {
                return function.apply(this);
            }
        }

        final SynchronizedState state;

        public Synchronized(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            super(id);
            this.state = new SynchronizedState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
        }

        @Override
        public Agent agent()
        {
            return state.agent();
        }

        private SynchronizedState safeStore()
        {
            state.refreshRanges();
            return state;
        }

        @Override
        public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return safeStore().execute(context, consumer);
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return safeStore().submit(context, function);
        }

        @Override
        public <T> T executeSync(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return safeStore().executeSync(context, function);
        }

        @Override
        public void shutdown() {}
    }

    public static class SingleThread extends CommandStore
    {
        class AsyncState extends State implements SafeCommandStore
        {
            public AsyncState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, CommandStore commandStore)
            {
                super(time, agent, store, progressLog, rangesForEpoch, commandStore);
            }

            @Override
            public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
            {
                return submit(context, i -> { consumer.accept(i); return null; });
            }

            @Override
            public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
            {
                return AsyncChains.ofCallable(executor, () -> function.apply(this));
            }
        }
        private final ExecutorService executor;
        private final AsyncState state;

        public SingleThread(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            super(id);
            executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + time.id() + ']');
                return thread;
            });
            state = newState(time, agent, store, progressLogFactory, rangesForEpoch);
        }

        AsyncState newState(NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            return new AsyncState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
        }

        @Override
        public Agent agent()
        {
            return state.agent();
        }

        private State safeStore()
        {
            state.refreshRanges();
            return state;
        }

        @Override
        public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return safeStore().execute(context, consumer);
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return safeStore().submit(context, function);
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
            public DebugState(NodeTimeService time, Agent agent, DataStore store, ProgressLog progressLog, RangesForEpochHolder rangesForEpoch, CommandStore commandStore)
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

        public Debug(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpoch);
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
        AsyncState newState(NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpoch)
        {
            return new DebugState(time, agent, store, progressLogFactory.create(this), rangesForEpoch, this);
        }
    }

    public static State inMemory(CommandStore unsafeStore)
    {
        return (unsafeStore instanceof Synchronized) ? ((Synchronized) unsafeStore).safeStore() : ((SingleThread) unsafeStore).safeStore();
    }

    public static State inMemory(SafeCommandStore safeStore)
    {
        return (safeStore instanceof SynchronizedState) ? ((SynchronizedState) safeStore) : ((AsyncState) safeStore);
    }
}
