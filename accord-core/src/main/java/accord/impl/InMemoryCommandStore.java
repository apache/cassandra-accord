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
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.impl.progresslog.DefaultProgressLog;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.RedundantStatus;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractKeys;
import accord.primitives.Deps;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestStartedAt.STARTED_BEFORE;
import static accord.local.SafeCommandStore.TestStatus.ANY_STATUS;
import static accord.local.SaveStatus.Applying;
import static accord.local.SaveStatus.Erased;
import static accord.local.SaveStatus.ErasedOrInvalidOrVestigial;
import static accord.local.SaveStatus.ReadyToExecute;
import static accord.local.Status.Applied;
import static accord.local.Status.Invalidated;
import static accord.local.Status.NotDefined;
import static accord.local.Status.PreCommitted;
import static accord.local.Status.Stable;
import static accord.local.Status.Truncated;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;
import static java.lang.String.format;

public abstract class InMemoryCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryCommandStore.class);
    private static final boolean CHECK_DEPENDENCY_INVARIANTS = false;

    final NavigableMap<TxnId, GlobalCommand> commands = new TreeMap<>();
    final NavigableMap<Timestamp, GlobalCommand> commandsByExecuteAt = new TreeMap<>();
    private final NavigableMap<RoutableKey, GlobalTimestampsForKey> timestampsForKey = new TreeMap<>();
    private final NavigableMap<RoutableKey, GlobalCommandsForKey> commandsForKey = new TreeMap<>();

    // TODO (find library, efficiency): this is obviously super inefficient, need some range map
    private final TreeMap<TxnId, RangeCommand> rangeCommands = new TreeMap<>();
    private final TreeMap<TxnId, Ranges> historicalRangeCommands = new TreeMap<>();
    // TODO (desired): use `redundantBefore` information instead
    protected Timestamp maxRedundant = Timestamp.NONE;

    private InMemorySafeStore current;

    public InMemoryCommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder)
    {
        super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder);
    }

    protected boolean canExposeUnloaded()
    {
        return true;
    }

    @VisibleForTesting
    public NavigableMap<TxnId, GlobalCommand> unsafeCommands()
    {
        return commands;
    }

    @VisibleForTesting
    public NavigableMap<Timestamp, GlobalCommand> unsafeCommandsByExecuteAt()
    {
        return commandsByExecuteAt;
    }

    @VisibleForTesting
    public NavigableMap<RoutableKey, GlobalCommandsForKey> unsafeCommandsForKey()
    {
        return commandsForKey;
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
            listeners.register(newGlobalCommand.txnId, new LocalListeners.ComplexListener()
            {
                @Override
                public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
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
                            Participants intersectingParticipants = participants.intersecting(participantsOfPrev, Minimal);
                            if (intersectingParticipants.isEmpty())
                                continue;

                            if (!cur.txnId().kind().witnesses().test(prev.txnId().kind()) && !cur.partialDeps().contains(prev.txnId()))
                                continue;

                            Participants<?> depParticipants = cur.partialDeps().participants(prev.txnId());
                            if (!depParticipants.containsAll(intersectingParticipants))
                                Invariants.illegalState(cur.txnId() + " does not maintain dependency invariants with immediately preceding transaction " + prev.txnId() + "; intersecting participants: " + intersectingParticipants + "; dependency participants: " + depParticipants);

                            if (prev.txnId().isWrite())
                                participants = participants.without(intersectingParticipants);
                        }
                    }
                    return !cur.hasBeen(Status.Applied);
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

    public GlobalCommandsForKey commandsForKeyIfPresent(Key key)
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

    public GlobalTimestampsForKey timestampsForKey(Key key)
    {
        return timestampsForKey.computeIfAbsent(key, GlobalTimestampsForKey::new);
    }

    public GlobalTimestampsForKey timestampsForKeyIfPresent(Key key)
    {
        return timestampsForKey.get(key);
    }

    private <O> O mapReduceForKey(InMemorySafeStore safeStore, Routables<?> keysOrRanges, Ranges slice, BiFunction<CommandsForKey, O, O> map, O accumulate)
    {
        switch (keysOrRanges.domain()) {
            default:
                throw new AssertionError();
            case Key:
                AbstractKeys<Key> keys = (AbstractKeys<Key>) keysOrRanges;
                for (Key key : keys)
                {
                    if (!slice.contains(key)) continue;
                    CommandsForKey commands = safeStore.ifLoadedAndInitialised(key).current();
                    if (commands == null)
                        continue;

                    accumulate = map.apply(commands, accumulate);
                }
                break;
            case Range:
                Ranges ranges = (Ranges) keysOrRanges;
                Ranges sliced = ranges.slice(slice, Minimal);
                for (Range range : sliced)
                {
                    // TODO (required): this method should fail if it requires more info than available
                    // TODO (required): I don't think this can possibly work in C*, as we don't know which timestampsForKey we need
                    for (Map.Entry<RoutableKey, GlobalCommandsForKey> entry : commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive()).entrySet())
                    {
                        GlobalCommandsForKey globalCommands = entry.getValue();
                        CommandsForKey commands = globalCommands.value();
                        if (commands == null)
                            continue;
                        accumulate = map.apply(commands, accumulate);
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

    @Override
    protected void updatedRedundantBefore(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        InMemorySafeStore inMemorySafeStore = (InMemorySafeStore) safeStore;
        ranges.forEach(r -> {
            commandsForKey.subMap(r.start(), r.startInclusive(), r.end(), r.endInclusive()).forEach((forKey, forValue) -> {
                if (!forValue.isEmpty())
                {
                    InMemorySafeCommandsForKey safeCfk = forValue.createSafeReference();
                    inMemorySafeStore.commandsForKey.put(forKey, safeCfk);
                    safeCfk.refresh(safeStore);
                }
            });
        });
    }

    @Override
    public void markShardDurable(SafeCommandStore safeStore, TxnId syncId, Ranges ranges)
    {
        super.markShardDurable(safeStore, syncId, ranges);
        markShardDurable(syncId, ranges);
    }

    private void markShardDurable(TxnId syncId, Ranges ranges)
    {
        if (!rangeCommands.containsKey(syncId))
            historicalRangeCommands.merge(syncId, ranges, Ranges::with);

        // TODO (now): apply on retrieval
        historicalRangeCommands.entrySet().removeIf(next -> next.getKey().compareTo(syncId) < 0 && next.getValue().intersects(ranges));
        rangeCommands.entrySet().removeIf(tx -> {
            if (tx.getKey().compareTo(syncId) >= 0)
                return false;
            Ranges newRanges = tx.getValue().ranges.without(ranges);
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

        // verify we're clearing the progress log
        ((Node)time).scheduler().once(() -> {
            DefaultProgressLog progressLog = (DefaultProgressLog) this.progressLog;
            commands.headMap(syncId, false).forEach((id, cmd) -> {
                Command command = cmd.value();
                if (!command.hasBeen(PreCommitted)) return;
                if (!command.txnId().kind().isGloballyVisible()) return;

                Ranges allRanges = unsafeRangesForEpoch().allBetween(id.epoch(), command.executeAtOrTxnId().epoch());
                boolean done = command.hasBeen(Truncated);
                if (!done)
                {
                    if (redundantBefore().status(cmd.txnId, command.executeAtOrTxnId(), command.route()) == RedundantStatus.PRE_BOOTSTRAP_OR_STALE)
                        return;

                    Route<?> route = cmd.value().route().slice(allRanges);
                    done = !route.isEmpty() && ranges.containsAll(route);
                }

                if (done) Invariants.checkState(progressLog.get(id) == null);
            });
        }, 5L, TimeUnit.SECONDS);
    }

    protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges,
                                                Map<TxnId, InMemorySafeCommand> commands,
                                                Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey,
                                                Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
    {
        return new InMemorySafeStore(this, ranges, context, commands, timestampsForKey, commandsForKeys);
    }

    protected void validateRead(Command current) {}

    protected final InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges)
    {
        Map<TxnId, InMemorySafeCommand> commands = new HashMap<>();
        Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey = new HashMap<>();
        Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey = new HashMap<>();

        context.forEachId(txnId -> commands.put(txnId, lazyReference(txnId)));
        for (InMemorySafeCommand safe : commands.values())
        {
            GlobalCommand global = safe.unsafeGlobal();
            if (global == null) continue;
            Command current = global.value();
            if (current == null) continue;
            validateRead(current);
        }

        for (Seekable seekable : context.keys())
        {
            switch (seekable.domain())
            {
                case Key:
                    RoutableKey key = (RoutableKey) seekable;
                    switch (context.keyHistory())
                    {
                        case NONE:
                            continue;
                        case COMMANDS:
                            commandsForKey.put(key, commandsForKey((Key) key).createSafeReference());
                            break;
                        case TIMESTAMPS:
                            timestampsForKey.put(key, timestampsForKey((Key) key).createSafeReference());
                            break;
                        default: throw new UnsupportedOperationException("Unknown key history: " + context.keyHistory());
                    }
                    break;
                case Range:
                    // load range cfks here
            }
        }
        return createSafeStore(context, ranges, commands, timestampsForKey, commandsForKey);
    }

    public SafeCommandStore beginOperation(PreLoadContext context)
    {
        if (current != null)
            throw illegalState("Another operation is in progress or it's store was not cleared");
        current = createSafeStore(context, rangesForEpoch);
        updateRangesForEpoch(current);
        return current;
    }

    public void completeOperation(SafeCommandStore store)
    {
        if (store != current)
            throw illegalState("This operation has already been cleared");

        try
        {
            current.postExecute();
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
            throw new RuntimeException("Caught exception in command store " + this, t);
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

    private static Timestamp maxApplied(SafeCommandStore safeStore, Seekables<?, ?> keysOrRanges, Ranges slice)
    {
        Timestamp max = ((InMemoryCommandStore)safeStore.commandStore()).maxRedundant;
        for (GlobalCommand command : ((InMemoryCommandStore) safeStore.commandStore()).commands.values())
        {
            if (command.value().hasBeen(Applied))
                max = Timestamp.max(command.value().executeAt(), max);
        }
        return max;
    }

    public AsyncChain<Timestamp> maxAppliedFor(Seekables<?, ?> keysOrRanges, Ranges slice)
    {
        return submit(PreLoadContext.contextFor(keysOrRanges), safeStore -> maxApplied(safeStore, keysOrRanges, slice));
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" + "id=" + id + ",node=" + time.id().id + '}';
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

        public GlobalCommand(TxnId txnId)
        {
            this.txnId = txnId;
        }

        public InMemorySafeCommand createSafeReference()
        {
            return new InMemorySafeCommand(txnId, this);
        }

        @Override
        public GlobalState<Command> value(Command value)
        {
            return super.value(value);
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

    public static class InMemorySafeStore extends AbstractSafeCommandStore<InMemorySafeCommand, InMemorySafeTimestampsForKey, InMemorySafeCommandsForKey>
    {
        private final InMemoryCommandStore commandStore;
        protected final Map<TxnId, InMemorySafeCommand> commands;
        private final Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey;
        private final Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey;
        private RangesForEpoch ranges;

        public InMemorySafeStore(InMemoryCommandStore commandStore,
                                 RangesForEpoch ranges,
                                 PreLoadContext context,
                                 Map<TxnId, InMemorySafeCommand> commands,
                                 Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey,
                                 Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
        {
            super(context);
            this.commandStore = commandStore;
            this.commands = commands;
            this.commandsForKey = commandsForKey;
            this.timestampsForKey = timestampsForKey;
            this.ranges = ranges;
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
        protected InMemorySafeTimestampsForKey getTimestampsForKeyInternal(Key key)
        {
            return timestampsForKey.get(key);
        }

        @Override
        protected void addTimestampsForKeyInternal(InMemorySafeTimestampsForKey tfk)
        {
            timestampsForKey.put(tfk.key(), tfk);
        }

        @Override
        protected InMemorySafeTimestampsForKey getTimestampsForKeyIfLoaded(Key key)
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
        protected InMemorySafeCommandsForKey getCommandsForKeyInternal(Key key)
        {
            return commandsForKey.get(key);
        }

        @Override
        protected void addCommandsForKeyInternal(InMemorySafeCommandsForKey cfk)
        {
            commandsForKey.put(cfk.key(), cfk);
        }

        @Override
        protected InMemorySafeCommandsForKey getCommandsForKeyIfLoaded(Key key)
        {
            if (!commandStore.canExposeUnloaded())
                return null;
            GlobalCommandsForKey global = commandStore.commandsForKeyIfPresent(key);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        protected void update(Command prev, Command updated)
        {
            super.update(prev, updated);

            TxnId txnId = updated.txnId();
            if (txnId.domain() != Domain.Range)
                return;

            // TODO (expected): consider removing if erased
            if (updated.saveStatus() == Erased || updated.saveStatus() == ErasedOrInvalidOrVestigial)
                return;

            Seekables<?, ?> keysOrRanges = updated.keysOrRanges();
            if (keysOrRanges == null) keysOrRanges = prev.keysOrRanges();
            if (keysOrRanges == null)
                return;

            Ranges slice = ranges().allBetween(txnId, updated.executeAtOrTxnId());
            slice = commandStore.redundantBefore().removeShardRedundant(txnId, updated.executeAtOrTxnId(), slice);
            commandStore.rangeCommands.computeIfAbsent(txnId, ignore -> new RangeCommand(commandStore.commands.get(txnId)))
                         .update(((Ranges)keysOrRanges).slice(slice, Minimal));
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
        public void setRangesForEpoch(RangesForEpoch rangesForEpoch)
        {
            super.setRangesForEpoch(rangesForEpoch);
            ranges = rangesForEpoch;
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

        @Override
        public <P1, T> T mapReduceActive(Seekables<?, ?> keysOrRanges, Ranges slice, Timestamp startedBefore, Kinds testKind, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            accumulate = commandStore.mapReduceForKey(this, keysOrRanges, slice, (commands, prev) -> {
                return commands.mapReduceActive(startedBefore, testKind, map, p1, prev);
            }, accumulate);

            return mapReduceRangesInternal(keysOrRanges, slice, startedBefore, null, testKind, STARTED_BEFORE, ANY_DEPS, ANY_STATUS, map, p1, accumulate);
        }

        // TODO (expected): instead of accepting a slice, accept the min/max epoch and let implementation handle it
        @Override
        public <P1, T> T mapReduceFull(Seekables<?, ?> keysOrRanges, Ranges slice, TxnId testTxnId, Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            accumulate = commandStore.mapReduceForKey(this, keysOrRanges, slice, (commands, prev) -> {
                return commands.mapReduceFull(testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, prev);
            }, accumulate);

            return mapReduceRangesInternal(keysOrRanges, slice, testTxnId, testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, accumulate);
        }

        private <P1, T> T mapReduceRangesInternal(Seekables<?, ?> keysOrRanges, Ranges slice, @Nonnull Timestamp testTimestamp, @Nullable TxnId testTxnId, Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            // TODO (find lib, efficiency): this is super inefficient, need to store Command in something queryable
            Seekables<?, ?> sliced = keysOrRanges.slice(slice, Minimal);
            Map<Range, List<TxnInfo>> collect = new TreeMap<>(Range::compare);
            commandStore.rangeCommands.forEach(((txnId, rangeCommand) -> {
                Command command = rangeCommand.command.value();
                // TODO (now): probably this isn't safe - want to ensure we take dependency on any relevant syncId
                if (command.saveStatus().compareTo(SaveStatus.Erased) >= 0)
                    return;

                Invariants.nonNull(command);
                switch (testStartedAt)
                {
                    default: throw new AssertionError();
                    case STARTED_AFTER:
                        if (command.txnId().compareTo(testTimestamp) <= 0) return;
                        else break;
                    case STARTED_BEFORE:
                        if (command.txnId().compareTo(testTimestamp) >= 0) return;
                    case ANY:
                        if (testDep != ANY_DEPS && command.executeAtOrTxnId().compareTo(testTxnId) < 0)
                            return;
                }

                switch (testStatus)
                {
                    default: throw new AssertionError("Unhandled TestStatus: " + testStatus);
                    case ANY_STATUS:
                        break;
                    case IS_PROPOSED:
                        switch (command.status())
                        {
                            default: return;
                            case PreCommitted:
                            case Committed:
                            case Accepted:
                        }
                        break;
                    case IS_STABLE:
                        if (command.status().compareTo(Stable) < 0 || command.status().compareTo(Truncated) >= 0)
                            return;
                }

                if (!testKind.test(command.txnId().kind()))
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
                    if ((testDep == WITH) == !command.partialDeps().intersects(testTxnId, rangeCommand.ranges))
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

            if (testStatus == ANY_STATUS && testDep == ANY_DEPS)
            {
                commandStore.historicalRangeCommands.forEach(((txnId, ranges) -> {
                    switch (testStartedAt)
                    {
                        default: throw new AssertionError();
                        case STARTED_AFTER:
                            if (txnId.compareTo(testTimestamp) <= 0) return;
                            else break;
                        case STARTED_BEFORE:
                            if (txnId.compareTo(testTimestamp) >= 0) return;
                            else break;
                        case ANY:
                    }

                    if (!testKind.test(txnId.kind()))
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
                    accumulate = map.apply(p1, e.getKey(), command.txnId, command.executeAt, initial);
                }
            }

            return accumulate;
        }

        @Override
        public void postExecute()
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

            commands.values().forEach(c -> {
                if (c.isUnset())
                    commandStore.commands.remove(c.txnId());
                c.invalidate();
            });
            timestampsForKey.values().forEach(tfk -> {
                if (tfk.isUnset())
                    commandStore.timestampsForKey.remove(tfk.key());
                tfk.invalidate();
            });
            commandsForKey.values().forEach(cfk -> {
                if (cfk.isUnset())
                    commandStore.commandsForKey.remove(cfk.key());
                cfk.invalidate();
            });
        }
    }

    public static class Synchronized extends InMemoryCommandStore
    {
        Runnable active = null;
        final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        public Synchronized(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder);
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
                throw illegalState("could not add item to queue");
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

        public SingleThread(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder);
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
                throw illegalState(format("Command store called from wrong thread; unexpected %s", current));
            if (expected != current)
                throw illegalState(format("Command store called from the wrong thread. Expected %s, got %s", expected, current));
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
                                  Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
            {
                super(commandStore, ranges, context, commands, timestampsForKey, commandsForKey);
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
        }

        public Debug(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder);
        }

        @Override
        protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKeyMap, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
        {
            return new DebugSafeStore(this, ranges, context, commands, timestampsForKeyMap, commandsForKeys);
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

        private static String suffix(boolean blocking)
        {
            if (blocking)
                return " <Blocking>";
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

        private static void logDependencyGraph(InMemoryCommandStore commandStore, TxnId txnId, Set<TxnId> visited, boolean verbose, int level, boolean blocking)
        {
            String prefix = prefix(level, verbose);
            boolean previouslyVisited = !visited.add(txnId); // prevents infinite loops if command deps overlap
            String suffix = suffix(blocking);
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
            if (command.hasBeen(Stable))
            {
                Command.Committed committed = command.asCommitted();
                if (level == 0 || verbose || !committed.isWaitingOnDependency())
                    log(prefix, suffix, command);

                if (committed.isWaitingOnDependency() && !previouslyVisited)
                    deps.forEach(depId -> logDependencyGraph(commandStore, depId, visited, verbose, level+1, committed.waitingOn.isWaitingOn(depId)));
            }
            else
            {
                log(prefix, suffix, command);
                if (!previouslyVisited)
                    deps.forEach(depId -> logDependencyGraph(commandStore, depId, visited, verbose, level+1, false));
            }
        }

        public static void logDependencyGraph(CommandStore commandStore, TxnId txnId, boolean verbose)
        {
            logger.info("Logging dependencies on for {}, verbose: {}", txnId, verbose);
            InMemoryCommandStore inMemoryCommandStore = (InMemoryCommandStore) commandStore;
            logger.info("Node: {}, CommandStore #{}", inMemoryCommandStore.time.id(), commandStore.id());
            Set<TxnId> visited = new HashSet<>();
            logDependencyGraph(inMemoryCommandStore, txnId, visited, verbose, 0, false);
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

    /**
     * Replay and loading logic
     */

    // redundantBefore, durableBefore, newBootstrapBeganAt, safeToRead, rangesForEpoch are
    // not replayed here. It is assumed that persistence on the application side will ensure
    // they are brought up to latest values _before_ replay.
    public void clearForTesting()
    {
        Invariants.checkState(current == null);
        progressLog.clear();
        commands.clear();
        commandsByExecuteAt.clear();
        timestampsForKey.clear();
        commandsForKey.clear();
        rangeCommands.clear();
        historicalRangeCommands.clear();
        unsafeSetRejectBefore(new ReducingRangeMap<>());
    }

    public interface Loader
    {
        void load(Command next);
        void apply(Command next);
    }

    public Loader loader()
    {
        return new Loader()
        {
            private PreLoadContext context(Command command, KeyHistory keyHistory)
            {
                TxnId txnId = command.txnId();
                Keys keys = null;

                if (CommandsForKey.manages(txnId))
                    keys = (Keys) command.keysOrRanges();
                else if (!CommandsForKey.managesExecution(txnId) && command.hasBeen(Status.Stable) && !command.hasBeen(Status.Truncated))
                    keys = command.asCommitted().waitingOn.keys;

                if (keys != null)
                {
                    return PreLoadContext.contextFor(txnId, keys, keyHistory);
                }

                return PreLoadContext.contextFor(txnId);
            }

            public void load(Command command)
            {
                TxnId txnId = command.txnId();

                executeInContext(InMemoryCommandStore.this,
                                 context(command, KeyHistory.COMMANDS),
                                 safeStore -> {
                                     Command local = command;
                                     if (local.status() != Truncated && local.status() != Invalidated)
                                     {
                                         Cleanup cleanup = Cleanup.shouldCleanup(InMemoryCommandStore.this, local, null, local.route(), false);
                                         switch (cleanup)
                                         {
                                             case NO:
                                                 break;
                                             case INVALIDATE:
                                             case TRUNCATE_WITH_OUTCOME:
                                             case TRUNCATE:
                                             case ERASE:
                                                 local = Commands.purge(local, local.route(), cleanup);
                                         }
                                     }

                                     local = safeStore.unsafeGet(txnId).update(safeStore, local);
                                     if (local.status() == Truncated)
                                         safeStore.progressLog().clear(local.txnId());
                                     return local;
                                 });


            }

            public void apply(Command command)
            {
                TxnId txnId = command.txnId();

                PreLoadContext context = context(command, KeyHistory.TIMESTAMPS);
                executeInContext(InMemoryCommandStore.this,
                                 context,
                                 safeStore -> {
                                     SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                                     Command local = safeCommand.current();
                                     if (local.is(Stable) && !local.hasBeen(Applied))
                                         Commands.maybeExecute(safeStore, safeCommand, local, true, true);
                                     else if (local.saveStatus().compareTo(Applying) >= 0 && !local.is(Invalidated) && !local.is(Truncated))
                                         Commands.applyWrites(safeStore, context, local).begin(agent);
                                     return null;
                                 });
            }
        };
    }

    @VisibleForTesting
    public void load(Deps loading)
    {
        registerHistoricalTransactions(loading,
                                       ((key, txnId) -> {
                                           GlobalCommandsForKey globalCfk = commandsForKey(key);
                                           if (globalCfk.isEmpty())
                                               globalCfk.value(new CommandsForKey(key));
                                           CommandsForKey cfk = globalCfk.createSafeReference().current();
                                           Invariants.checkState(cfk != null);
                                           globalCfk.value(cfk.registerHistorical(txnId).cfk());
                                       }));
    }

    @Override
    protected void registerHistoricalTransactions(Deps deps, SafeCommandStore safeStore)
    {
        registerHistoricalTransactions(deps, (key, txnId) -> safeStore.get(key).registerHistorical(safeStore, txnId));
    }

    private void registerHistoricalTransactions(Deps deps, BiConsumer<Key, TxnId> registerHistorical)
    {
        RangesForEpoch rangesForEpoch = this.rangesForEpoch;
        Ranges allRanges = rangesForEpoch.all();
        deps.keyDeps.keys().forEach(allRanges, key -> {
            deps.keyDeps.forEach(key, (txnId, txnIdx) -> {
                // TODO (desired, efficiency): this can be made more efficient by batching by epoch
                if (rangesForEpoch.coordinates(txnId).contains(key))
                    return; // already coordinates, no need to replicate
                // TODO (required): check this logic, esp. next line, matches C*
                if (!rangesForEpoch.allAfter(txnId.epoch()).contains(key))
                    return;

                registerHistorical.accept(key, txnId);
            });

        });

        TreeMap<TxnId, RangeCommand> rangeCommands = this.rangeCommands;
        TreeMap<TxnId, Ranges> historicalRangeCommands = historicalRangeCommands();
        deps.rangeDeps.forEachUniqueTxnId(allRanges, null, (ignore, txnId) -> {

            if (rangeCommands.containsKey(txnId))
                return;

            Ranges ranges = deps.rangeDeps.ranges(txnId);
            if (rangesForEpoch.coordinates(txnId).intersects(ranges))
                return; // already coordinates, no need to replicate
            // TODO (required): check this logic, esp. next line, matches C*
            if (!rangesForEpoch.allAfter(txnId.epoch()).intersects(ranges))
                return;

            historicalRangeCommands.merge(txnId, ranges.slice(allRanges), Ranges::with);
        });
    }
}
