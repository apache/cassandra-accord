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

import accord.api.*;
import accord.local.*;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.local.CommandStores.RangesForEpoch;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static accord.local.SafeCommandStore.TestDep.*;
import static accord.local.SafeCommandStore.TestKind.Ws;
import static accord.local.Status.*;
import static accord.primitives.Routables.Slice.Minimal;

public abstract class InMemoryCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryCommandStore.class);

    private final NavigableMap<TxnId, GlobalCommand> commands = new TreeMap<>();
    private final NavigableMap<RoutableKey, GlobalCommandsForKey> commandsForKey = new TreeMap<>();
    private final CFKLoader cfkLoader = new CFKLoader();
    // TODO (find library, efficiency): this is obviously super inefficient, need some range map

    private final TreeMap<TxnId, RangeCommand> rangeCommands = new TreeMap<>();

    private InMemorySafeStore current;

    public InMemoryCommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder)
    {
        super(id, time, agent, store, progressLogFactory, rangesForEpochHolder);
    }

    @Override
    public int id()
    {
        return id;
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    public GlobalCommand ifPresent(TxnId txnId)
    {
        return commands.get(txnId);
    }

    public GlobalCommand command(TxnId txnId)
    {
        return commands.computeIfAbsent(txnId, GlobalCommand::new);
    }

    public boolean hasCommand(TxnId txnId)
    {
        return commands.containsKey(txnId);
    }

    public GlobalCommandsForKey ifPresent(Key key)
    {
        return commandsForKey.get(key);
    }

    public GlobalCommandsForKey commandsForKey(Key key)
    {
        return commandsForKey.computeIfAbsent(key, GlobalCommandsForKey::new);
    }

    public boolean hasCommandsForKey(Key key)
    {
        return commandsForKey.containsKey(key);
    }

    public CFKLoader cfkLoader()
    {
        return cfkLoader;
    }

    public void forEpochCommands(Ranges ranges, long epoch, Consumer<Command> consumer)
    {
        Timestamp minTimestamp = Timestamp.minForEpoch(epoch);
        Timestamp maxTimestamp = Timestamp.maxForEpoch(epoch);
        for (Range range : ranges)
        {
            Iterable<GlobalCommandsForKey> keyCommands = commandsForKey.subMap(range.start(), range.startInclusive(),
                                                                                 range.end(), range.endInclusive()).values();

            for (GlobalCommandsForKey commands : keyCommands)
            {
                if (commands.isEmpty())
                    continue;
                commands.value().forWitnessed(minTimestamp, maxTimestamp, txnId -> consumer.accept(command(txnId).value()));
            }

            rangeCommands.forEach((txnId, rangeCommand) -> {
                if (!rangeCommand.ranges.intersects(range))
                    return;
                Command command = rangeCommand.command.value();
                Invariants.nonNull(command);
                if (!command.hasBeen(PreAccepted))
                    return;

                Timestamp timestamp = command.hasBeen(PreCommitted) ? command.executeAt() : txnId;
                if (timestamp.compareTo(maxTimestamp) > 0)
                    return;
                if (timestamp.compareTo(minTimestamp) < 0)
                    return;
                consumer.accept(command);
            });
        }
    }

    public void forCommittedInEpoch(Ranges ranges, long epoch, Consumer<Command> consumer)
    {
        Timestamp minTimestamp = Timestamp.minForEpoch(epoch);
        Timestamp maxTimestamp = Timestamp.maxForEpoch(epoch);
        for (Range range : ranges)
        {
            Iterable<GlobalCommandsForKey> keyCommands = commandsForKey.subMap(range.start(),
                                                                                 range.startInclusive(),
                                                                                 range.end(),
                                                                                 range.endInclusive()).values();
            for (GlobalCommandsForKey commands : keyCommands)
            {
                if (commands.isEmpty())
                    continue;
                commands.value().byExecuteAt()
                        .between(minTimestamp, maxTimestamp, status -> status.hasBeen(Committed))
                        .forEach(txnId -> consumer.accept(command(txnId).value()));
            }

            rangeCommands.forEach((txnId, rangeCommand) -> {
                if (!rangeCommand.ranges.intersects(range))
                    return;
                Command command = rangeCommand.command.value();
                if (command == null || !command.hasBeen(Committed))
                    return;
                Timestamp executeAt = command.executeAt();
                if (executeAt.compareTo(maxTimestamp) > 0)
                    return;
                if (executeAt.compareTo(minTimestamp) < 0)
                    return;
                consumer.accept(command);
            });
        }
    }

    public CommonAttributes register(InMemorySafeStore safeStore, Seekables<?, ?> keysOrRanges, Ranges slice, SafeCommand command, CommonAttributes attrs)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError();
            case Key:
                CommonAttributes.Mutable mutable = attrs.mutable();
                forEach(keysOrRanges, slice, key -> {
                    SafeCommandsForKey cfk = safeStore.commandsForKey(key);
                    CommandListener listener = cfk.register(command.current()).asListener();
                    mutable.addListener(listener);
                });
                return mutable;
            case Range:
                rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(commands.get(command.txnId())))
                        .update((Ranges)keysOrRanges);
        }
        return attrs;
    }

    public CommonAttributes register(InMemorySafeStore safeStore, Seekable keyOrRange, Ranges slice, SafeCommand command, CommonAttributes attrs)
    {
        switch (keyOrRange.domain())
        {
            default: throw new AssertionError();
            case Key:
                CommonAttributes.Mutable mutable = attrs.mutable();
                forEach(keyOrRange, slice, key -> {
                    SafeCommandsForKey cfk = safeStore.commandsForKey(key);
                    CommandListener listener = cfk.register(command.current()).asListener();
                    mutable.addListener(listener);
                });
                return mutable;
            case Range:
                rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(commands.get(command.txnId())))
                        .update(Ranges.of((Range)keyOrRange));
        }
        return attrs;
    }

    private <O> O mapReduceForKey(InMemorySafeStore safeStore, Routables<?, ?> keysOrRanges, Ranges slice, BiFunction<CommandsForKey, O, O> map, O accumulate, O terminalValue)
    {
        switch (keysOrRanges.domain()) {
            default:
                throw new AssertionError();
            case Key:
                AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                for (Key key : keys)
                {
                    if (!slice.contains(key)) continue;
                    SafeCommandsForKey forKey = safeStore.ifLoaded(key);
                    if (forKey.current() == null)
                        continue;
                    accumulate = map.apply(forKey.current(), accumulate);
                    if (accumulate.equals(terminalValue))
                        return accumulate;
                }
                break;
            case Range:
                Ranges ranges = (Ranges) keysOrRanges;
                Ranges sliced = ranges.slice(slice, Minimal);
                for (Range range : sliced)
                {
                    for (GlobalCommandsForKey forKey : commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive()).values())
                    {
                        if (forKey.value() == null)
                            continue;
                        accumulate = map.apply(forKey.value(), accumulate);
                        if (accumulate.equals(terminalValue))
                            return accumulate;
                    }
                }
        }
        return accumulate;
    }

    private void forEach(Seekables<?, ?> keysOrRanges, Ranges slice, Consumer<RoutableKey> forEach)
    {
        switch (keysOrRanges.domain()) {
            default:
                throw new AssertionError();
            case Key:
                AbstractKeys<Key, ?> keys = (AbstractKeys<Key, ?>) keysOrRanges;
                keys.forEach(slice, key -> forEach.accept(key));
                break;
            case Range:
                Ranges ranges = (Ranges) keysOrRanges;
                ranges.slice(slice).forEach(range -> {
                    commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                            .keySet().forEach(forEach);
                });
        }
    }

    private void forEach(Routable keyOrRange, Ranges slice, Consumer<RoutableKey> forEach)
    {
        switch (keyOrRange.domain())
        {
            default: throw new AssertionError();
            case Key:
                Key key = (Key) keyOrRange;
                if (slice.contains(key))
                    forEach.accept(key);
                break;
            case Range:
                Range range = (Range) keyOrRange;
                Ranges.of(range).slice(slice).forEach(r -> {
                    commandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive())
                            .keySet().forEach(forEach);
                });
        }
    }


    protected InMemorySafeStore createCommandStore(PreLoadContext context, RangesForEpoch ranges, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
    {
        return new InMemorySafeStore(this, cfkLoader, ranges, context, commands, commandsForKeys);
    }

    protected final InMemorySafeStore createCommandStore(PreLoadContext context, RangesForEpoch ranges)
    {
        Map<TxnId, InMemorySafeCommand> commands = new HashMap<>();
        Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys = new HashMap<>();
        for (TxnId txnId : context.txnIds())
            commands.put(txnId, command(txnId).createSafeReference());

        for (Seekable seekable : context.keys())
        {
            switch (seekable.domain())
            {
                case Key:
                    RoutableKey key = (RoutableKey) seekable;
                    commandsForKeys.put(key, commandsForKey((Key) key).createSafeReference());
                    break;
                case Range:
                    // load range cfks here
            }
        }
        return createCommandStore(context, ranges, commands, commandsForKeys);
    }

    public SafeCommandStore beginOperation(PreLoadContext context)
    {
        if (current != null)
            throw new IllegalStateException("Another operation is in progress or it's store was not cleared");
        current = createCommandStore(context, rangesForEpochHolder.get());
        return current;
    }

    public void completeOperation(SafeCommandStore store)
    {
        if (store != current)
            throw new IllegalStateException("This operation has already been cleared");
        try
        {
            current.complete();
        }
        catch (Throwable t)
        {
            logger.error("Exception completing operation", t);
            throw t;
        }
        finally
        {
            current = null;
        }
    }

    private <T> T executeInContext(InMemoryCommandStore commandStore, PreLoadContext preLoadContext, Function<? super SafeCommandStore, T> function, boolean isDirectCall)
    {

        SafeCommandStore safeStore = commandStore.beginOperation(preLoadContext);
        try
        {
            return function.apply(safeStore);
        }
        catch (Throwable t)
        {
            if (isDirectCall) logger.error("Uncaught exception", t);
            throw t;
        }
        finally
        {
            commandStore.completeOperation(safeStore);
        }
    }

    protected <T> T executeInContext(InMemoryCommandStore commandStore, PreLoadContext context, Function<? super SafeCommandStore, T> function)
    {
        return executeInContext(commandStore, context, function, true);
    }

    protected <T> void executeInContext(InMemoryCommandStore commandStore, PreLoadContext context, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
    {
        try
        {
            T result = executeInContext(commandStore, context, function, false);
            callback.accept(result, null);
        }
        catch (Throwable t)
        {
            logger.error("Uncaught exception", t);
            callback.accept(null, t);
        }
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" +
               "id=" + id +
               '}';
    }

    class RangeCommand
    {
        final GlobalCommand command;
        Ranges ranges;

        RangeCommand(GlobalCommand command)
        {
            this.command = command;
        }

        void update(Ranges add)
        {
            if (ranges == null) ranges = add;
            else ranges = ranges.with(add);
        }
    }

    class CFKLoader implements CommandsForKey.CommandLoader<TxnId>
    {
        private Command loadForCFK(TxnId data)
        {
            GlobalCommand globalCommand = ifPresent(data);
            if (globalCommand != null)
                return globalCommand.value();
            throw new IllegalStateException("Could not find command for CFK for " + data);
        }

        @Override
        public TxnId txnId(TxnId txnId)
        {
            return loadForCFK(txnId).txnId();
        }

        @Override
        public Timestamp executeAt(TxnId txnId)
        {
            return loadForCFK(txnId).executeAt();
        }

        @Override
        public SaveStatus saveStatus(TxnId txnId)
        {
            return loadForCFK(txnId).saveStatus();
        }

        @Override
        public List<TxnId> depsIds(TxnId data)
        {
            PartialDeps deps = loadForCFK(data).partialDeps();
            return deps != null ? deps.txnIds() : Collections.emptyList();
        }

        @Override
        public TxnId saveForCFK(Command command)
        {
            return command.txnId();
        }
    }

    public static abstract class GlobalState<V>
    {
        private V value;

        public V value()
        {
            return value;
        }

        boolean isEmpty()
        {
            return value == null;
        }

        public GlobalState<V> value(V value)
        {
            this.value = value;
            return this;
        }
    }

    public static class GlobalCommand extends GlobalState<Command>
    {
        private final TxnId txnId;

        public GlobalCommand(TxnId txnId)
        {
            this.txnId = txnId;
        }

        public InMemorySafeCommand createSafeReference()
        {
            return new InMemorySafeCommand(txnId, this);
        }
    }

    public static class GlobalCommandsForKey extends GlobalState<CommandsForKey>
    {
        private final Key key;

        public GlobalCommandsForKey(RoutableKey key)
        {
            this.key = (Key) key;
        }

        public InMemorySafeCommandsForKey createSafeReference()
        {
            return new InMemorySafeCommandsForKey(key, this);
        }
    }

    static class InMemorySafeStore extends AbstractSafeCommandStore<InMemorySafeCommand, InMemorySafeCommandsForKey>
    {
        private final InMemoryCommandStore commandStore;
        private final Map<TxnId, InMemorySafeCommand> commands;
        private final Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey;
        private final RangesForEpoch ranges;
        private final CFKLoader cfkLoader;

        public InMemorySafeStore(InMemoryCommandStore commandStore, CFKLoader cfkLoader, RangesForEpoch ranges, PreLoadContext context, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
        {
            super(context);
            this.commandStore = commandStore;
            this.commands = commands;
            this.commandsForKey = commandsForKey;
            this.ranges = Invariants.nonNull(ranges);
            this.cfkLoader = cfkLoader;
        }

        @Override
        protected InMemorySafeCommand getCommandInternal(TxnId txnId)
        {
            return commands.get(txnId);
        }

        @Override
        protected void addCommandInternal(InMemorySafeCommand command)
        {
            commands.put(command.txnId(), command);
        }

        @Override
        protected InMemorySafeCommandsForKey getCommandsForKeyInternal(RoutableKey key)
        {
            return commandsForKey.get(key);
        }

        @Override
        protected void addCommandsForKeyInternal(InMemorySafeCommandsForKey cfk)
        {
            commandsForKey.put(cfk.key(), (InMemorySafeCommandsForKey) cfk);
        }

        @Override
        protected InMemorySafeCommand getIfLoaded(TxnId txnId)
        {
            GlobalCommand global = commandStore.ifPresent(txnId);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        protected InMemorySafeCommandsForKey getIfLoaded(RoutableKey key)
        {
            GlobalCommandsForKey global = commandStore.ifPresent((Key) key);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        public CommandStore commandStore()
        {
            return commandStore;
        }

        @Override
        public DataStore dataStore()
        {
            return commandStore.store;
        }

        @Override
        public Agent agent()
        {
            return commandStore.agent;
        }

        @Override
        public ProgressLog progressLog()
        {
            return commandStore.progressLog;
        }

        @Override
        public RangesForEpoch ranges()
        {
            return ranges;
        }

        @Override
        public long latestEpoch()
        {
            return commandStore.time.epoch();

        }

        @Override
        public Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
        {
            Timestamp timestamp = commandStore.mapReduceForKey(this, keysOrRanges, slice, (forKey, prev) -> Timestamp.max(forKey.max(), prev), Timestamp.NONE, null);
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            for (RangeCommand command : commandStore.rangeCommands.values())
            {
                if (command.ranges.intersects(sliced))
                    timestamp = Timestamp.max(timestamp, command.command.value().executeAt());
            }
            return timestamp;
        }

        @Override
        public NodeTimeService time()
        {
            return commandStore.time;
        }

        @Override
        public <T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, TestKind testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<T, T> map, T accumulate, T terminalValue)
        {
            accumulate = commandStore.mapReduceForKey(this, keysOrRanges, slice, (forKey, prev) -> {
                CommandsForKey.CommandTimeseries<?> timeseries;
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
                CommandsForKey.CommandTimeseries.TestTimestamp remapTestTimestamp;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case EXECUTES_AFTER:
                        remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.AFTER;
                        break;
                    case STARTED_BEFORE:
                    case MAY_EXECUTE_BEFORE:
                        remapTestTimestamp = CommandsForKey.CommandTimeseries.TestTimestamp.BEFORE;
                }
                return timeseries.mapReduce(testKind, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, prev, terminalValue);
            }, accumulate, terminalValue);

            if (accumulate.equals(terminalValue))
                return accumulate;

            // TODO (find lib, efficiency): this is super inefficient, need to store Command in something queryable
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            Map<Range, List<Command>> collect = new TreeMap<>(Range::compare);
            commandStore.rangeCommands.forEach(((txnId, rangeCommand) -> {
                Command command = rangeCommand.command.value();
                Invariants.nonNull(command);
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                        if (command.txnId().compareTo(timestamp) < 0) return;
                        else break;
                    case STARTED_BEFORE:
                        if (command.txnId().compareTo(timestamp) > 0) return;
                        else break;
                    case EXECUTES_AFTER:
                        if (command.executeAt().compareTo(timestamp) < 0) return;
                        else break;
                    case MAY_EXECUTE_BEFORE:
                        Timestamp compareTo = command.known().executeAt.hasDecidedExecuteAt() ? command.executeAt() : command.txnId();
                        if (compareTo.compareTo(timestamp) > 0) return;
                        else break;
                }

                if (minStatus != null && command.status().compareTo(minStatus) < 0)
                    return;

                if (maxStatus != null && command.status().compareTo(maxStatus) > 0)
                    return;

                if (testKind == Ws && command.txnId().rw().isRead())
                    return;

                if (testDep != ANY_DEPS)
                {
                    if (!command.known().deps.hasProposedOrDecidedDeps())
                        return;

                    if ((testDep == WITH) == !command.partialDeps().contains(depId))
                        return;
                }

                if (!rangeCommand.ranges.intersects(sliced))
                    return;

                Routables.foldl(rangeCommand.ranges, sliced, (r, in, i) -> {
                    // TODO (easy, efficiency): pass command as a parameter to Fold
                    List<Command> list = in.computeIfAbsent(r, ignore -> new ArrayList<>());
                    if (list.isEmpty() || list.get(list.size() - 1) != command)
                        list.add(command);
                    return in;
                }, collect);
            }));
            for (Map.Entry<Range, List<Command>> e : collect.entrySet())
            {
                for (Command command : e.getValue())
                {
                    T initial = accumulate;
                    accumulate = map.apply(e.getKey(), command.txnId(), command.executeAt(), initial);
                }
            }

            return accumulate;
        }

        @Override
        public CommonAttributes completeRegistration(Seekables<?, ?> keysOrRanges, Ranges slice, InMemorySafeCommand command, CommonAttributes attrs)
        {
            return commandStore.register(this, keysOrRanges, slice, command, attrs);
        }

        @Override
        public CommonAttributes completeRegistration(Seekable keyOrRange, Ranges slice, InMemorySafeCommand command, CommonAttributes attrs)
        {
            return commandStore.register(this, keyOrRange, slice, command, attrs);
        }

        public CommandsForKey.CommandLoader<?> cfkLoader()
        {
            return cfkLoader;
        }

        @Override
        protected void invalidateSafeState()
        {
            commands.values().forEach(SafeState::invalidate);
            commandsForKey.values().forEach(SafeState::invalidate);
        }

        @Override
        public void complete()
        {
            postExecute();
            super.complete();
        }
    }

    public static class Synchronized extends InMemoryCommandStore
    {
        Runnable active = null;
        final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        public Synchronized(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpochHolder);
        }

        private synchronized void maybeRun()
        {
            if (active != null)
                return;

            active = queue.poll();
            while (active != null)
            {
                this.unsafeRunIn(() -> {
                    try
                    {
                        active.run();
                    }
                    catch (Throwable t)
                    {
                        logger.error("Uncaught exception", t);
                    }
                });
                active = queue.poll();
            }
        }

        private void enqueueAndRun(Runnable runnable)
        {
            boolean result = queue.add(runnable);
            if (!result)
                throw new IllegalStateException("could not add item to queue");
            maybeRun();
        }

        @Override
        public boolean inStore()
        {
            return CommandStore.maybeCurrent() == this;
        }

        @Override
        public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return submit(context, i -> { consumer.accept(i); return null; });
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return new AsyncChains.Head<T>()
            {
                @Override
                protected void start(BiConsumer<? super T, Throwable> callback)
                {
                    enqueueAndRun(() -> executeInContext(InMemoryCommandStore.Synchronized.this, context, function, callback));
                }
            };
        }

        @Override
        public <T> AsyncChain<T> submit(Callable<T> task)
        {
            return new AsyncChains.Head<T>()
            {
                @Override
                protected void start(BiConsumer<? super T, Throwable> callback)
                {
                    enqueueAndRun(() -> {
                        try
                        {
                            callback.accept(task.call(), null);
                        }
                        catch (Throwable t)
                        {
                            logger.error("Uncaught exception", t);
                            callback.accept(null, t);
                        }
                    });
                }
            };
        }

        @Override
        public void shutdown() {}
    }

    public static class SingleThread extends InMemoryCommandStore
    {
        private Thread thread; // when run in the executor this will be non-null, null implies not running in this store
        private final ExecutorService executor;

        private SingleThread(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpochHolder);
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + time.id() + ']');
                return thread;
            });
            executor.execute(() -> thread = Thread.currentThread());
        }

        public static SingleThread create(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder)
        {
            SingleThread st = new SingleThread(id, time, agent, store, progressLogFactory, rangesForEpochHolder);
            st.execute(() -> CommandStore.register(st));
            return st;
        }

        void assertThread()
        {
            Thread current = Thread.currentThread();
            Thread expected = thread;
            if (expected == null)
                throw new IllegalStateException(String.format("Command store called from wrong thread; unexpected %s", current));
            if (expected != current)
                throw new IllegalStateException(String.format("Command store called from the wrong thread. Expected %s, got %s", expected, current));
        }

        @Override
        public boolean inStore()
        {
            return thread == Thread.currentThread();
        }

        @Override
        public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return submit(context, i -> { consumer.accept(i); return null; });
        }

        @Override
        public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return AsyncChains.ofCallable(executor, () -> executeInContext(this, context, function));
        }

        @Override
        public <T> AsyncChain<T> submit(Callable<T> task)
        {
            return AsyncChains.ofCallable(executor, task);
        }

        @Override
        public void shutdown()
        {
            executor.shutdown();
        }
    }

    public static class Debug extends SingleThread
    {
        class DebugSafeStore extends InMemorySafeStore
        {
            public DebugSafeStore(InMemoryCommandStore commandStore, CFKLoader cfkLoader, RangesForEpoch ranges, PreLoadContext context, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
            {
                super(commandStore, cfkLoader, ranges, context, commands, commandsForKey);
            }

            @Override
            public InMemorySafeCommand ifPresent(TxnId txnId)
            {
                assertThread();
                return super.ifPresent(txnId);
            }

            @Override
            public InMemorySafeCommand ifLoaded(TxnId txnId)
            {
                assertThread();
                return super.ifLoaded(txnId);
            }

            @Override
            public InMemorySafeCommand command(TxnId txnId)
            {
                assertThread();
                return super.command(txnId);
            }

            @Override
            public void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command)
            {
                assertThread();
                super.register(keysOrRanges, slice, command);
            }

            @Override
            public void register(Seekable keyOrRange, Ranges slice, Command command)
            {
                assertThread();
                super.register(keyOrRange, slice, command);
            }
        }

        private Debug(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpochHolder);
        }

        public static Debug create(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder)
        {
            Debug debug = new Debug(id, time, agent, store, progressLogFactory, rangesForEpochHolder);
            debug.execute(() -> CommandStore.register(debug));
            return debug;
        }

        @Override
        protected InMemorySafeStore createCommandStore(PreLoadContext context, RangesForEpoch ranges, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
        {
            return new DebugSafeStore(this, cfkLoader(), ranges, context, commands, commandsForKeys);
        }

    }

    public static InMemoryCommandStore inMemory(CommandStore unsafeStore)
    {
        return (InMemoryCommandStore) unsafeStore;
    }

    public static InMemoryCommandStore inMemory(SafeCommandStore safeStore)
    {
        return inMemory(safeStore.commandStore());
    }

    /**
     * methods useful for troubleshooting burn test failures. Shouldn't be used anywhere
     */
    public static class Utils
    {
        private static String prefix(int level, boolean verbose)
        {
            if (level == 0 || !verbose)
                return "";

            StringBuilder prefix = new StringBuilder();
            for (int i=0; i<level; i++)
                prefix.append("-> ");
            prefix.append(' ');
            return prefix.toString();
        }

        private static String suffix(boolean blockingOnCommit, boolean blockingOnApply)
        {
            if (blockingOnApply)
                return " <Blocking On Apply>";
            if (blockingOnCommit)
                return " <Blocking On Commit>";
            return "";
        }

        private static void log(String prefix, String suffix, String msg, Object... args)
        {
            logger.info(prefix + msg + suffix, args);
        }

        private static void log(String prefix, String suffix, Command command)
        {
            log(prefix, suffix, "{} {}", command.txnId(), command.saveStatus());
        }

        private static void logDependencyGraph(InMemoryCommandStore commandStore, TxnId txnId, Set<TxnId> visited, boolean verbose, int level, boolean blockingOnCommit, boolean blockingOnApply)
        {
            String prefix = prefix(level, verbose);
            boolean previouslyVisited = !visited.add(txnId); // prevents infinite loops if command deps overlap
            String suffix = suffix(blockingOnCommit, blockingOnApply);
            if (previouslyVisited) suffix = suffix + " -- PREVIOUSLY VISITED";
            GlobalCommand global = commandStore.commands.get(txnId);
            if (global == null || global.isEmpty())
            {
                log(prefix, suffix, "{} NOT FOUND", txnId);
                return;
            }

            Command command = global.value();
            PartialDeps partialDeps = command.partialDeps();
            List<TxnId> deps = partialDeps != null ? partialDeps.txnIds() : Collections.emptyList();
            if (command.hasBeen(Committed))
            {
                Command.Committed committed = command.asCommitted();
                if (level == 0 || verbose || !committed.isWaitingOnDependency())
                    log(prefix, suffix, command);

                Set<TxnId> waitingOnCommit = committed.waitingOnCommit();
                Set<TxnId> waitingOnApply = new HashSet<>(committed.waitingOnApply().values());

                if (committed.isWaitingOnDependency() && !previouslyVisited)
                    deps.forEach(depId -> logDependencyGraph(commandStore, depId, visited, verbose, level+1, waitingOnCommit.contains(depId), waitingOnApply.contains(depId)));
            }
            else
            {
                log(prefix, suffix, command);
                if (!previouslyVisited)
                    deps.forEach(depId -> logDependencyGraph(commandStore, depId, visited, verbose, level+1, false, false));
            }
        }

        public static void logDependencyGraph(CommandStore commandStore, TxnId txnId, boolean verbose)
        {
            logger.info("Logging dependencies on for {}, verbose: {}", txnId, verbose);
            InMemoryCommandStore inMemoryCommandStore = (InMemoryCommandStore) commandStore;
            logger.info("Node: {}, CommandStore #{}", inMemoryCommandStore.time.id(), commandStore.id());
            Set<TxnId> visited = new HashSet<>();
            logDependencyGraph(inMemoryCommandStore, txnId, visited, verbose, 0, false, false);
        }

        /**
         * Recursively follows and prints dependencies starting from the given txnId. Useful in tracking down
         * the root causes of hung burn tests
         */
        public static void logDependencyGraph(CommandStore commandStore, TxnId txnId)
        {
            logDependencyGraph(commandStore, txnId, true);
        }
    }
}
