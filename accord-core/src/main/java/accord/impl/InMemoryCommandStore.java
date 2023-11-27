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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import accord.local.*;
import accord.utils.TriFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.CommandTimeseries.CommandLoader;
import accord.local.CommandStores.RangesForEpoch;
import accord.primitives.AbstractKeys;
import accord.primitives.Deps;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import static accord.local.Command.NotDefined.uninitialised;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SaveStatus.Applying;
import static accord.local.SaveStatus.ReadyToExecute;
import static accord.local.Status.Committed;
import static accord.local.Status.Truncated;
import static accord.local.Status.NotDefined;
import static accord.primitives.Routables.Slice.Minimal;

public abstract class InMemoryCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryCommandStore.class);
    private static final boolean CHECK_DEPENDENCY_INVARIANTS = false;

    final NavigableMap<TxnId, GlobalCommand> commands = new TreeMap<>();
    final NavigableMap<Timestamp, GlobalCommand> commandsByExecuteAt = new TreeMap<>();
    private final NavigableMap<RoutableKey, GlobalTimestampsForKey> timestampsForKey = new TreeMap<>();
    private final NavigableMap<RoutableKey, GlobalCommandsForKey> depsCommandsForKey = new TreeMap<>();
    private final NavigableMap<RoutableKey, GlobalCommandsForKey> allCommandsForKey = new TreeMap<>();
    // TODO (find library, efficiency): this is obviously super inefficient, need some range map

    private final TreeMap<TxnId, RangeCommand> rangeCommands = new TreeMap<>();
    private final TreeMap<TxnId, Ranges> historicalRangeCommands = new TreeMap<>();
    protected Timestamp maxRedundant = Timestamp.NONE;

    private InMemorySafeStore current;

    public InMemoryCommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, EpochUpdateHolder epochUpdateHolder)
    {
        super(id, time, agent, store, progressLogFactory, epochUpdateHolder);
    }

    protected boolean canExposeUnloaded()
    {
        return true;
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    TreeMap<TxnId, Ranges> historicalRangeCommands()
    {
        return historicalRangeCommands;
    }

    TreeMap<TxnId, RangeCommand> rangeCommands()
    {
        return rangeCommands;
    }

    public GlobalCommand commandIfPresent(TxnId txnId)
    {
        return commands.get(txnId);
    }

    public GlobalCommand command(TxnId txnId)
    {
        return commands.computeIfAbsent(txnId, this::newGlobalCommand);
    }

    public void onInitialise(GlobalCommand newGlobalCommand)
    {
        if (CHECK_DEPENDENCY_INVARIANTS)
        {

            newGlobalCommand.addListener(new Command.TransientListener()
            {
                @Override
                public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
                {
                    Command cur = safeCommand.current();
                    if (cur.saveStatus() == ReadyToExecute || cur.saveStatus() == Applying) // TODO (desired): only run the check once
                    {
                        long epoch = cur.executeAt().epoch();
                        Ranges ranges = safeStore.ranges().allAt(epoch);
                        Participants<?> participants = cur.route().participants(ranges, Minimal);
                        // TODO (required): look forwards also, but we only need to look at ?=ReadyToExecute transactions as they have already run their backwards checks
                        Iterator<GlobalCommand> iter = commandsByExecuteAt.descendingMap().tailMap(cur.executeAt(), false).values().iterator();
                        while (iter.hasNext())
                        {
                            GlobalCommand prevGlobal = iter.next();
                            Command prev = prevGlobal.value();
                            Timestamp prevExecuteAt = prev.executeAtIfKnown();
                            if (prevExecuteAt == null)
                            {
                                iter.remove();
                                continue;
                            }

                            if (prevExecuteAt.epoch() < epoch)
                            {
                                epoch = prevExecuteAt.epoch();
                                ranges = ranges.slice(safeStore.ranges().allAt(epoch), Minimal);
                                participants = participants.slice(ranges, Minimal);
                            }

                            if (participants.isEmpty())
                                break;

                            Participants participantsOfPrev = prev.route().participants(ranges, Minimal);
                            Participants intersectingParticipants = participants.intersect(participantsOfPrev);
                            if (intersectingParticipants.isEmpty())
                                continue;

                            if (!cur.txnId().rw().witnesses().test(prev.txnId().rw()) && !cur.partialDeps().contains(prev.txnId()))
                                continue;

                            Participants<?> depParticipants = cur.partialDeps().participants(prev.txnId());
                            if (!depParticipants.containsAll(intersectingParticipants))
                                Invariants.illegalState(cur.txnId() + " does not maintain dependency invariants with immediately preceding transaction " + prev.txnId() + "; intersecting participants: " + intersectingParticipants + "; dependency participants: " + depParticipants);

                            if (prev.txnId().isWrite())
                                participants = participants.subtract(intersectingParticipants);
                        };
                    }
                }

                @Override
                public PreLoadContext listenerPreLoadContext(TxnId caller)
                {
                    return PreLoadContext.contextFor(caller);
                }
            });
        }
    }

    private GlobalCommand newGlobalCommand(TxnId txnId)
    {
        GlobalCommand globalCommand = new GlobalCommand(txnId);
        onInitialise(globalCommand);
        return globalCommand;
    }

    public InMemorySafeCommand lazyReference(TxnId txnId)
    {
        GlobalCommand command = commands.get(txnId);
        return command != null ? new InMemorySafeCommand(txnId, command)
                               : new InMemorySafeCommand(txnId, () -> command(txnId));
    }

    public boolean hasCommand(TxnId txnId)
    {
        return commands.containsKey(txnId);
    }

    public GlobalCommandsForKey depsCommandsForKeyIfPresent(Key key)
    {
        return depsCommandsForKey.get(key);
    }

    public GlobalCommandsForKey depsCommandsForKey(Key key)
    {
        return depsCommandsForKey.computeIfAbsent(key, GlobalCommandsForKey::new);
    }

    public boolean hasDepsCommandsForKey(Key key)
    {
        return depsCommandsForKey.containsKey(key);
    }

    public GlobalCommandsForKey allCommandsForKeyIfPresent(Key key)
    {
        return allCommandsForKey.get(key);
    }

    public GlobalCommandsForKey allCommandsForKey(Key key)
    {
        return allCommandsForKey.computeIfAbsent(key, GlobalCommandsForKey::new);
    }

    public boolean hasAllCommandsForKey(Key key)
    {
        return allCommandsForKey.containsKey(key);
    }

    public GlobalTimestampsForKey timestampsForKey(Key key)
    {
        return timestampsForKey.computeIfAbsent(key, GlobalTimestampsForKey::new);
    }

    public GlobalTimestampsForKey timestampsForKeyIfPresent(Key key)
    {
        return timestampsForKey.get(key);
    }

    public CommonAttributes register(InMemorySafeStore safeStore, Seekables<?, ?> keysOrRanges, Ranges slice, SafeCommand command, CommonAttributes attrs)
    {
        switch (keysOrRanges.domain())
        {
            default: throw new AssertionError();
            case Key:
                CommonAttributes.Mutable mutable = attrs.mutable();
                forEach(keysOrRanges, slice, key -> {
                    Command.DurableAndIdempotentListener listener = CommandsForKeys.registerCommand(safeStore, key, command.current());
                    mutable.addListener(listener);
                });
                return mutable;
            case Range:
                // TODO (required): confirm we slice to owned ranges on C*
                rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(commands.get(command.txnId())))
                        .update(((Ranges)keysOrRanges).slice(slice, Minimal));
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
                    Command.DurableAndIdempotentListener listener = CommandsForKeys.registerCommand(safeStore, key, command.current());
                    mutable.addListener(listener);
                });
                return mutable;
            case Range:
                // TODO (required): confirm we slice to owned ranges on C*
                rangeCommands.computeIfAbsent(command.txnId(), ignore -> new RangeCommand(commands.get(command.txnId())))
                        .update(Ranges.of((Range)keyOrRange).slice(slice, Minimal));
        }
        return attrs;
    }

    private NavigableMap<RoutableKey, GlobalCommandsForKey> globalCommandsMapFor(KeyHistory keyHistory)
    {
        switch (keyHistory)
        {
            case DEPS:
                return depsCommandsForKey;
            case ALL:
                return allCommandsForKey;
            default:
                throw new IllegalStateException(keyHistory + " does not have a commands map");
        }
    }

    private <O> O mapReduceForKey(InMemorySafeStore safeStore, Routables<?> keysOrRanges, Ranges slice, KeyHistory keyHistory, TriFunction<TimestampsForKey, CommandsForKey, O, O> map, O accumulate, Predicate<? super O> terminate)
    {
        switch (keysOrRanges.domain()) {
            default:
                throw new AssertionError();
            case Key:
                AbstractKeys<Key> keys = (AbstractKeys<Key>) keysOrRanges;
                for (Key key : keys)
                {
                    if (!slice.contains(key)) continue;
                    SafeTimestampsForKey timestamps = safeStore.timestampsIfLoadedAndInitialised(key);
                    if (timestamps.current() == null)
                        continue;

                    CommandsForKey commands = null;
                    switch (keyHistory)
                    {
                        case DEPS:
                            commands = safeStore.depsCommandsIfLoadedAndInitialised(key).current();
                            break;
                        case ALL:
                            commands = safeStore.allCommandsIfLoadedAndInitialised(key).current();
                            break;
                    }

                    if (commands == null && keyHistory != KeyHistory.NONE)
                        continue;

                    accumulate = map.apply(timestamps.current(), commands, accumulate);
                    if (terminate.test(accumulate))
                        return accumulate;
                }
                break;
            case Range:
                Ranges ranges = (Ranges) keysOrRanges;
                Ranges sliced = ranges.slice(slice, Minimal);
                for (Range range : sliced)
                {
                    NavigableMap<RoutableKey, GlobalCommandsForKey> commandsMap = keyHistory.isNone() ? null : globalCommandsMapFor(keyHistory);
                    for (Map.Entry<RoutableKey, GlobalTimestampsForKey> entry : timestampsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive()).entrySet())
                    {
                        RoutableKey key = entry.getKey();
                        TimestampsForKey timestamps = entry.getValue().value();

                        GlobalCommandsForKey globalCommands = keyHistory.isNone() ? null : commandsMap.get(key);
                        if (timestamps == null)
                        {
                            Invariants.checkState(globalCommands == null || globalCommands.value() == null);
                            continue;
                        }

                        if (globalCommands == null)
                            continue;

                        CommandsForKey commands = keyHistory.isNone() ? null : Invariants.nonNull(globalCommands.value());

                        accumulate = map.apply(timestamps, commands, accumulate);
                        if (terminate.test(accumulate))
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
                AbstractKeys<Key> keys = (AbstractKeys<Key>) keysOrRanges;
                keys.forEach(slice, key -> forEach.accept(key));
                break;
            case Range:
                Ranges ranges = (Ranges) keysOrRanges;
                ranges.slice(slice).forEach(range -> {
                    depsCommandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
                            .keySet().forEach(forEach);
                    allCommandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive())
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
                    depsCommandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive())
                            .keySet().forEach(forEach);
                    allCommandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive())
                            .keySet().forEach(forEach);
                });
        }
    }

    @Override
    public void markShardDurable(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        super.markShardDurable(safeStore, syncId, ranges);

        if (!rangeCommands.containsKey(syncId))
            historicalRangeCommands.merge(syncId, ranges, Ranges::with);

        // TODO (now): apply on retrieval
        historicalRangeCommands.entrySet().removeIf(next -> next.getKey().compareTo(syncId) < 0 && next.getValue().intersects(ranges));
        rangeCommands.entrySet().removeIf(tx -> {
            if (tx.getKey().compareTo(syncId) >= 0)
                return false;
            Ranges newRanges = tx.getValue().ranges.subtract(ranges);
            if (!newRanges.isEmpty())
            {
                tx.getValue().ranges = newRanges;
                return false;
            }
            else
            {
                maxRedundant = Timestamp.nonNullOrMax(maxRedundant, tx.getValue().command.value().executeAt());
                return true;
            }
        });
        ranges.forEach(r -> {
            depsCommandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive()).values().forEach(forKey -> {
                if (!forKey.isEmpty())
                    forKey.value(forKey.value().withoutRedundant(syncId));
            });
            allCommandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive()).values().forEach(forKey -> {
                if (!forKey.isEmpty())
                    forKey.value(forKey.value().withoutRedundant(syncId));
            });
        });
    }

    protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges,
                                                Map<TxnId, InMemorySafeCommand> commands,
                                                Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey,
                                                Map<RoutableKey, InMemorySafeCommandsForKey> minimalCommandsForKeys,
                                                Map<RoutableKey, InMemorySafeCommandsForKey> completeCommandsForKeys)
    {
        return new InMemorySafeStore(this, ranges, context, commands, timestampsForKey, minimalCommandsForKeys, completeCommandsForKeys);
    }

    private void loadCommandsForKey(RoutableKey key,
                                    KeyHistory keyHistory,
                                    Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey,
                                    Map<RoutableKey, InMemorySafeCommandsForKey> minimalCommands,
                                    Map<RoutableKey, InMemorySafeCommandsForKey> completeCommands)
    {
        switch (keyHistory)
        {
            case ALL:
                // loading recovery commands implicitly loads deps
                completeCommands.put(key, allCommandsForKey((Key) key).createSafeReference());
            case DEPS:
                minimalCommands.put(key, depsCommandsForKey((Key) key).createSafeReference());
                break;
        }
        timestampsForKey.put(key, timestampsForKey((Key) key).createSafeReference());
    }

    protected final InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges)
    {
        Map<TxnId, InMemorySafeCommand> commands = new HashMap<>();
        Map<RoutableKey, InMemorySafeCommandsForKey> minimalCommands = new HashMap<>();
        Map<RoutableKey, InMemorySafeCommandsForKey> completeCommands = new HashMap<>();
        Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey = new HashMap<>();

        context.forEachId(txnId -> commands.put(txnId, lazyReference(txnId)));

        for (Seekable seekable : context.keys())
        {
            switch (seekable.domain())
            {
                case Key:
                    RoutableKey key = (RoutableKey) seekable;
                    switch (context.keyHistory())
                    {
                        case ALL:
                            // loading recovery commands implicitly loads deps
                            completeCommands.put(key, allCommandsForKey((Key) key).createSafeReference());
                        case DEPS:
                            minimalCommands.put(key, depsCommandsForKey((Key) key).createSafeReference());
                            break;
                    }
                    timestampsForKey.put(key, timestampsForKey((Key) key).createSafeReference());
                    break;
                case Range:
                    // load range cfks here
            }
        }
        return createSafeStore(context, ranges, commands, timestampsForKey, minimalCommands, completeCommands);
    }

    public SafeCommandStore beginOperation(PreLoadContext context)
    {
        if (current != null)
            throw new IllegalStateException("Another operation is in progress or it's store was not cleared");
        current = createSafeStore(context, updateRangesForEpoch());
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

    static class RangeCommand
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

    static class CFKEntry extends TxnId
    {
        final boolean uninitialised;
        public CFKEntry(TxnId copy, boolean uninitialised)
        {
            super(copy);
            this.uninitialised = uninitialised;
        }
    }

    class CFKLoader implements CommandLoader<CFKEntry>
    {
        final RoutableKey key;
        CFKLoader(RoutableKey key)
        {
            this.key = key;
        }

        private Command loadForCFK(CFKEntry entry)
        {
            GlobalCommand globalCommand = commandIfPresent(entry);
            if (globalCommand != null && globalCommand.value() != null)
                return globalCommand.value();
            if (entry.uninitialised)
                return uninitialised(entry);
            throw new IllegalStateException("Could not find command for CFK for " + entry);
        }

        @Override
        public TxnId txnId(CFKEntry txnId)
        {
            return loadForCFK(txnId).txnId();
        }

        @Override
        public Timestamp executeAt(CFKEntry txnId)
        {
            return loadForCFK(txnId).executeAt();
        }

        @Override
        public SaveStatus saveStatus(CFKEntry txnId)
        {
            return loadForCFK(txnId).saveStatus();
        }

        @Override
        public List<TxnId> depsIds(CFKEntry data)
        {
            PartialDeps deps = loadForCFK(data).partialDeps();
            return deps != null ? deps.txnIds() : Collections.emptyList();
        }

        @Override
        public CFKEntry saveForCFK(Command command)
        {
            return new CFKEntry(command.txnId(), command.saveStatus().isUninitialised());
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

        public String toString()
        {
            return value == null ? "null" : value.toString();
        }
    }

    public static class GlobalCommand extends GlobalState<Command>
    {
        private final TxnId txnId;
        private Listeners<Command.TransientListener> transientListeners = null;

        public GlobalCommand(TxnId txnId)
        {
            this.txnId = txnId;
        }

        public InMemorySafeCommand createSafeReference()
        {
            return new InMemorySafeCommand(txnId, this);
        }

        public void addListener(Command.TransientListener listener)
        {
            if (transientListeners == null) transientListeners = new Listeners<>();
            transientListeners.add(listener);
        }

        public boolean removeListener(Command.TransientListener listener)
        {
            if (transientListeners == null || !transientListeners.remove(listener))
                return false;

            if (transientListeners.isEmpty())
                transientListeners = null;

            return true;
        }

        @Override
        public GlobalState<Command> value(Command value)
        {
            return super.value(value);
        }

        public Listeners<Command.TransientListener> transientListeners()
        {
            return transientListeners == null ? Listeners.EMPTY : transientListeners;
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

    private static class TimestampAndStatus
    {
        public final Timestamp timestamp;
        public final Status status;

        public TimestampAndStatus(Timestamp timestamp, Status status)
        {
            this.timestamp = timestamp;
            this.status = status;
        }
    }

    public static class GlobalTimestampsForKey extends GlobalState<TimestampsForKey>
    {
        private final Key key;

        public GlobalTimestampsForKey(RoutableKey key)
        {
            this.key = (Key) key;
        }

        public InMemorySafeTimestampsForKey createSafeReference()
        {
            return new InMemorySafeTimestampsForKey(key, this);
        }
    }

    public static class InMemorySafeStore extends AbstractSafeCommandStore<InMemorySafeCommand, InMemorySafeTimestampsForKey, InMemorySafeCommandsForKey, InMemorySafeCommandsForKeyUpdate>
    {
        private final InMemoryCommandStore commandStore;
        private final Map<TxnId, InMemorySafeCommand> commands;
        private final Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey;
        private final Map<RoutableKey, InMemorySafeCommandsForKey> depsCommandsForKey;
        private final Map<RoutableKey, InMemorySafeCommandsForKey> allCommandsForKey;
        private final Map<RoutableKey, InMemorySafeCommandsForKeyUpdate> updatesForKey = new HashMap<>();
        private final RangesForEpoch ranges;

        public InMemorySafeStore(InMemoryCommandStore commandStore,
                                 RangesForEpoch ranges,
                                 PreLoadContext context,
                                 Map<TxnId, InMemorySafeCommand> commands,
                                 Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey,
                                 Map<RoutableKey, InMemorySafeCommandsForKey> depsCommandsForKey,
                                 Map<RoutableKey, InMemorySafeCommandsForKey> allCommandsForKey)
        {
            super(context);
            this.commandStore = commandStore;
            this.commands = commands;
            this.depsCommandsForKey = depsCommandsForKey;
            this.allCommandsForKey = allCommandsForKey;
            this.timestampsForKey = timestampsForKey;
            this.ranges = Invariants.nonNull(ranges);
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
        protected InMemorySafeTimestampsForKey getTimestampsForKeyInternal(RoutableKey key)
        {
            return timestampsForKey.get(key);
        }

        @Override
        protected void addTimestampsForKeyInternal(InMemorySafeTimestampsForKey tfk)
        {
            timestampsForKey.put(tfk.key(), tfk);
        }

        @Override
        protected InMemorySafeTimestampsForKey getTimestampsForKeyIfLoaded(RoutableKey key)
        {
            if (!commandStore.canExposeUnloaded())
                return null;
            GlobalTimestampsForKey global = commandStore.timestampsForKeyIfPresent((Key) key);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        protected InMemorySafeCommand getIfLoaded(TxnId txnId)
        {
            if (!commandStore.canExposeUnloaded())
                return null;
            GlobalCommand global = commandStore.commandIfPresent(txnId);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        protected InMemorySafeCommandsForKey getDepsCommandsForKeyInternal(RoutableKey key)
        {
            return depsCommandsForKey.get(key);
        }

        @Override
        protected void addDepsCommandsForKeyInternal(InMemorySafeCommandsForKey cfk)
        {
            depsCommandsForKey.put(cfk.key(), cfk);
        }

        @Override
        protected InMemorySafeCommandsForKey getDepsCommandsForKeyIfLoaded(RoutableKey key)
        {
            if (!commandStore.canExposeUnloaded())
                return null;
            GlobalCommandsForKey global = commandStore.depsCommandsForKeyIfPresent((Key) key);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        protected InMemorySafeCommandsForKey getAllCommandsForKeyInternal(RoutableKey key)
        {
            return allCommandsForKey.get(key);
        }

        @Override
        protected void addAllCommandsForKeyInternal(InMemorySafeCommandsForKey cfk)
        {
            allCommandsForKey.put(cfk.key(), cfk);
        }

        @Override
        protected InMemorySafeCommandsForKey getAllCommandsForKeyIfLoaded(RoutableKey key)
        {
            if (!commandStore.canExposeUnloaded())
                return null;
            GlobalCommandsForKey global = commandStore.allCommandsForKeyIfPresent((Key) key);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        protected InMemorySafeCommandsForKeyUpdate getCommandsForKeyUpdateInternal(RoutableKey key)
        {
            return updatesForKey.get(key);
        }

        @Override
        protected InMemorySafeCommandsForKeyUpdate createCommandsForKeyUpdateInternal(RoutableKey key)
        {
            return new InMemorySafeCommandsForKeyUpdate((Key) key, (CommandLoader<CFKEntry>) cfkLoader(key));
        }

        @Override
        protected void addCommandsForKeyUpdateInternal(InMemorySafeCommandsForKeyUpdate update)
        {
            updatesForKey.put(update.key(), update);
        }

        private CommandsForKey applyUpdate(CommandsForKey cfk, InMemorySafeCommandsForKeyUpdate update, KeyHistory keyHistory)
        {
            switch (keyHistory)
            {
                case DEPS:
                    return update.applyToDeps(cfk);
                case ALL:
                    return update.applyToAll(cfk);
                default:
                    throw new IllegalArgumentException("Unhandled KeyHistory " + keyHistory);
            }
        }

        private void applyCommandForKeyUpdate(InMemorySafeCommandsForKeyUpdate update, KeyHistory kind, Map<RoutableKey, InMemorySafeCommandsForKey> commandsMap, Function<Key, GlobalCommandsForKey> getter)
        {
            Key key = update.key();
            InMemorySafeCommandsForKey safeCFK = commandsMap.get(key);
            if (safeCFK != null)
            {
                if (safeCFK.isEmpty()) safeCFK.initialize(cfkLoader(key));
                safeCFK.update(applyUpdate(safeCFK.current(), update, kind));
            }
            else
            {
                GlobalCommandsForKey globalCFK = getter.apply(key);
                CommandsForKey cfk = globalCFK.value();
                if (cfk == null)
                    cfk = new CommandsForKey(key, cfkLoader(key));
                globalCFK.value(applyUpdate(cfk, update, kind));
            }
        }

        private void applyCommandForKeyUpdate(InMemorySafeCommandsForKeyUpdate update)
        {
            applyCommandForKeyUpdate(update, KeyHistory.DEPS, depsCommandsForKey, commandStore::depsCommandsForKey);
            applyCommandForKeyUpdate(update, KeyHistory.ALL, allCommandsForKey, commandStore::allCommandsForKey);
        }

        @Override
        protected void applyCommandForKeyUpdates()
        {
            updatesForKey.values().forEach(this::applyCommandForKeyUpdate);
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
        public Timestamp maxConflict(Seekables<?, ?> keysOrRanges, Ranges slice)
        {
            Timestamp timestamp = commandStore.mapReduceForKey(this, keysOrRanges, slice, KeyHistory.NONE, (timestamps, commands, prev) -> Timestamp.nonNullOrMax(timestamps.max(), prev), Timestamp.NONE, Objects::isNull);
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            for (RangeCommand command : commandStore.rangeCommands.values())
            {
                if (command.ranges.intersects(sliced))
                    timestamp = Timestamp.nonNullOrMax(timestamp, command.command.value().executeAt());
            }
            // TODO (required): check we have this on C*-side; accept TxnId instead of Ranges when updating API, to avoid slicing
            return Timestamp.nonNullOrMax(timestamp, commandStore.redundantBefore().minConflict(keysOrRanges.slice(slice)));
        }

        @Override
        public void erase(SafeCommand command)
        {
            commands.remove(command.txnId());
        }

        // TODO (preferable): this can have protected visibility if under CommandStore, and this is perhaps a better place to put it also
        @Override
        public void registerHistoricalTransactions(Deps deps)
        {
            RangesForEpoch rangesForEpoch = commandStore.rangesForEpoch;
            Ranges allRanges = rangesForEpoch.all();
            deps.keyDeps.keys().forEach(allRanges, key -> {
                deps.keyDeps.forEach(key, txnId -> {
                    // TODO (desired, efficiency): this can be made more efficient by batching by epoch
                    if (rangesForEpoch.coordinates(txnId).contains(key))
                        return; // already coordinates, no need to replicate
                    if (!rangesForEpoch.allBefore(txnId.epoch()).contains(key))
                        return;

                    CommandsForKeys.registerNotWitnessed(this, key, txnId);
                });

            });
            TreeMap<TxnId, RangeCommand> rangeCommands = commandStore.rangeCommands();
            TreeMap<TxnId, Ranges> historicalRangeCommands = commandStore.historicalRangeCommands();
            deps.rangeDeps.forEachUniqueTxnId(allRanges, null, (ignore, txnId) -> {

                if (rangeCommands.containsKey(txnId))
                    return;

                Ranges ranges = deps.rangeDeps.ranges(txnId);
                if (rangesForEpoch.coordinates(txnId).intersects(ranges))
                    return; // already coordinates, no need to replicate
                if (!rangesForEpoch.allBefore(txnId.epoch()).intersects(ranges))
                    return;

                historicalRangeCommands.merge(txnId, ranges.slice(allRanges), Ranges::with);
            });
        }

        @Override
        public NodeTimeService time()
        {
            return commandStore.time;
        }

        private static class TxnInfo
        {
            private final TxnId txnId;
            private final Timestamp executeAt;
            private final Status status;
            private final List<TxnId> deps;

                public TxnInfo(TxnId txnId, Timestamp executeAt, Status status, List<TxnId> deps)
            {
                this.txnId = txnId;
                this.executeAt = executeAt;
                this.status = status;
                this.deps = deps;
            }

                public TxnInfo(Command command)
            {
                this.txnId = command.txnId();
                this.executeAt = command.executeAt();
                this.status = command.status();
                PartialDeps deps = command.partialDeps();
                this.deps = deps != null ? deps.txnIds() : Collections.emptyList();
            }
        }
        // TODO (expected): instead of accepting a slice, accept the min/max epoch and let implementation handle it
        @Override
        public <P1, T> T mapReduce(Seekables<?, ?> keysOrRanges, Ranges slice, KeyHistory keyHistory, Kinds testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate, Predicate<? super T> terminate)
        {
            accumulate = commandStore.mapReduceForKey(this, keysOrRanges, slice, keyHistory, (timestamps, commands, prev) -> {
                CommandTimeseries.TimestampType timestampType;
                switch (testTimestamp)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                    case STARTED_BEFORE:
                        timestampType = CommandTimeseries.TimestampType.TXN_ID;
                        break;
                    case EXECUTES_AFTER:
                    case MAY_EXECUTE_BEFORE:
                        timestampType = CommandTimeseries.TimestampType.EXECUTE_AT;
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
                return commands.commands().mapReduce(testKind, timestampType, remapTestTimestamp, timestamp, testDep, depId, minStatus, maxStatus, map, p1, prev, terminate);
            }, accumulate, terminate);

            if (terminate.test(accumulate))
                return accumulate;

            // TODO (find lib, efficiency): this is super inefficient, need to store Command in something queryable
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            Map<Range, List<TxnInfo>> collect = new TreeMap<>(Range::compare);
            commandStore.rangeCommands.forEach(((txnId, rangeCommand) -> {
                Command command = rangeCommand.command.value();
                // TODO (now): probably this isn't safe - want to ensure we take dependency on any relevant syncId
                if (command.saveStatus().compareTo(SaveStatus.Erased) >= 0)
                    return;

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
                        Timestamp compareTo = command.executeAtIfKnownElseTxnId();
                        if (compareTo.compareTo(timestamp) > 0) return;
                        else break;
                }

                if (minStatus != null && command.status().compareTo(minStatus) < 0)
                    return;

                if (maxStatus != null && command.status().compareTo(maxStatus) > 0)
                    return;

                if (!testKind.test(command.txnId().rw()))
                    return;

                if (testDep != ANY_DEPS)
                {
                    if (!command.known().deps.hasProposedOrDecidedDeps())
                        return;

                    // TODO (required): ensure C* matches this behaviour
                    // We are looking for transactions A that have (or have not) B as a dependency.
                    // If B covers ranges [1..3] and A covers [2..3], but the command store only covers ranges [1..2],
                    // we could have A adopt B as a dependency on [3..3] only, and have that A intersects B on this
                    // command store, but also that there is no dependency relation between them on the overlapping
                    // key range [2..2].

                    // This can lead to problems on recovery, where we believe a transaction is a dependency
                    // and so it is safe to execute, when in fact it is only a dependency on a different shard
                    // (and that other shard, perhaps, does not know that it is a dependency - and so it is not durably known)
                    // TODO (required): consider this some more
                    if ((testDep == WITH) == !command.partialDeps().intersects(depId, rangeCommand.ranges))
                        return;
                }

                if (!rangeCommand.ranges.intersects(sliced))
                    return;

                Routables.foldl(rangeCommand.ranges, sliced, (r, in, i) -> {
                    // TODO (easy, efficiency): pass command as a parameter to Fold
                    List<TxnInfo> list = in.computeIfAbsent(r, ignore -> new ArrayList<>());
                    if (list.isEmpty() || !list.get(list.size() - 1).txnId.equals(command.txnId()))
                        list.add(new TxnInfo(command));
                    return in;
                }, collect);
            }));

            if (minStatus == null && testDep == ANY_DEPS)
            {
                commandStore.historicalRangeCommands.forEach(((txnId, ranges) -> {
                    switch (testTimestamp)
                    {
                        default: throw new AssertionError();
                        case STARTED_AFTER:
                        case EXECUTES_AFTER:
                            if (txnId.compareTo(timestamp) < 0) return;
                            else break;
                        case STARTED_BEFORE:
                        case MAY_EXECUTE_BEFORE:
                            if (txnId.compareTo(timestamp) > 0) return;
                            else break;
                    }

                    if (!testKind.test(txnId.rw()))
                        return;

                    if (!ranges.intersects(sliced))
                        return;

                    Routables.foldl(ranges, sliced, (r, in, i) -> {
                        // TODO (easy, efficiency): pass command as a parameter to Fold
                        List<TxnInfo> list = in.computeIfAbsent(r, ignore -> new ArrayList<>());
                        if (list.isEmpty() || !list.get(list.size() - 1).txnId.equals(txnId))
                        {
                            GlobalCommand global = commandStore.commands.get(txnId);
                            if (global != null && global.value() != null)
                            {
                                Command command = global.value();
                                PartialDeps deps = command.partialDeps();
                                List<TxnId> depsIds = deps != null ? deps.txnIds() : Collections.emptyList();
                                list.add(new TxnInfo(txnId, txnId, command.status(), depsIds));
                            }
                            else
                            {
                                list.add(new TxnInfo(txnId, txnId, NotDefined, Collections.emptyList()));
                            }
                        }
                        return in;
                    }, collect);
                }));
            }

            for (Map.Entry<Range, List<TxnInfo>> e : collect.entrySet())
            {
                for (TxnInfo command : e.getValue())
                {
                    T initial = accumulate;
                    accumulate = map.apply(p1, e.getKey(), command.txnId, command.executeAt, command.status, () -> command.deps, initial);
                    if (terminate.test(accumulate))
                        return accumulate;
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

        @Override
        public CommandLoader<?> cfkLoader(RoutableKey key)
        {
            return commandStore.new CFKLoader(key);
        }

        @Override
        protected void invalidateSafeState()
        {
            commands.values().forEach(c -> {
                if (c != null && c.current() != null)
                {
                    Timestamp executeAt = c.current().executeAtIfKnown();
                    if (executeAt == null)
                        return;

                    if (c.current().hasBeen(Truncated)) commandStore.commandsByExecuteAt.remove(executeAt);
                    else commandStore.commandsByExecuteAt.put(executeAt, commandStore.command(c.txnId()));
                }
            });
            commands.values().forEach(SafeState::invalidate);
            timestampsForKey.values().forEach(SafeState::invalidate);
            depsCommandsForKey.values().forEach(SafeState::invalidate);
            allCommandsForKey.values().forEach(SafeState::invalidate);
            updatesForKey.values().forEach(SafeState::invalidate);
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

        public Synchronized(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, EpochUpdateHolder epochUpdateHolder)
        {
            super(id, time, agent, store, progressLogFactory, epochUpdateHolder);
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

        public SingleThread(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, EpochUpdateHolder epochUpdateHolder)
        {
            super(id, time, agent, store, progressLogFactory, epochUpdateHolder);
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + time.id() + ']');
                return thread;
            });
            // "this" is leaked before constructor is completed, but since all fields are "final" and set before "this"
            // is leaked, then visibility should not be an issue.
            executor.execute(() -> thread = Thread.currentThread());
            executor.execute(() -> CommandStore.register(this));
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
            public DebugSafeStore(InMemoryCommandStore commandStore,
                                  RangesForEpoch ranges,
                                  PreLoadContext context,
                                  Map<TxnId, InMemorySafeCommand> commands,
                                  Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey,
                                  Map<RoutableKey, InMemorySafeCommandsForKey> minimalCommandsForKey,
                                  Map<RoutableKey, InMemorySafeCommandsForKey> completeCommandsForKey)
            {
                super(commandStore, ranges, context, commands, timestampsForKey, minimalCommandsForKey, completeCommandsForKey);
            }

            @Override
            public InMemorySafeCommand getInternalIfLoadedAndInitialised(TxnId txnId)
            {
                assertThread();
                return super.getInternalIfLoadedAndInitialised(txnId);
            }

            @Override
            public InMemorySafeCommand getInternal(TxnId txnId)
            {
                assertThread();
                return super.getInternal(txnId);
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

        public Debug(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, EpochUpdateHolder epochUpdateHolder)
        {
            super(id, time, agent, store, progressLogFactory, epochUpdateHolder);
        }

        @Override
        protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKeyMap, Map<RoutableKey, InMemorySafeCommandsForKey> minimalCommandsForKeys, Map<RoutableKey, InMemorySafeCommandsForKey> completeCommandsForKeys)
        {
            return new DebugSafeStore(this, ranges, context, commands, timestampsForKeyMap, minimalCommandsForKeys, completeCommandsForKeys);
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

                Set<TxnId> waitingOnCommit = committed.waitingOn.computeWaitingOnCommit();
                Set<TxnId> waitingOnApply = committed.waitingOn.computeWaitingOnApply();

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
