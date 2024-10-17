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
import java.util.Arrays;
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

import accord.api.LocalListeners;
import accord.api.RoutingKey;
import accord.impl.progresslog.DefaultProgressLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.RedundantStatus;
import accord.local.RejectBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.cfk.CommandsForKey;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.local.KeyHistory.ASYNC;
import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SafeCommandStore.TestStartedAt.STARTED_BEFORE;
import static accord.local.SafeCommandStore.TestStatus.ANY_STATUS;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.ErasedOrVestigial;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Durability.Local;
import static accord.primitives.Status.Invalidated;
import static accord.primitives.Status.PreApplied;
import static accord.primitives.Status.PreCommitted;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Status.NotDefined;
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

    public InMemoryCommandStore(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder)
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

                            if (!cur.txnId().witnesses().test(prev.txnId()) && !cur.partialDeps().contains(prev.txnId()))
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

    public GlobalCommandsForKey commandsForKeyIfPresent(RoutingKey key)
    {
        return commandsForKey.get(key);
    }

    public GlobalCommandsForKey commandsForKey(RoutingKey key)
    {
        return commandsForKey.computeIfAbsent(key, GlobalCommandsForKey::new);
    }

    public boolean hasCommandsForKey(RoutingKey key)
    {
        return commandsForKey.containsKey(key);
    }

    public GlobalTimestampsForKey timestampsForKey(RoutingKey key)
    {
        return timestampsForKey.computeIfAbsent(key, GlobalTimestampsForKey::new);
    }

    public GlobalTimestampsForKey timestampsForKeyIfPresent(RoutingKey key)
    {
        return timestampsForKey.get(key);
    }

    private <O> O mapReduceForKey(InMemorySafeStore safeStore, Unseekables<?> keysOrRanges, BiFunction<CommandsForKey, O, O> map, O accumulate)
    {
        switch (keysOrRanges.domain()) {
            default:
                throw new AssertionError();
            case Key:
                AbstractUnseekableKeys keys = (AbstractUnseekableKeys) keysOrRanges;
                for (RoutingKey key : keys)
                {
                    CommandsForKey commands = safeStore.ifLoadedAndInitialised(key).current();
                    if (commands == null)
                        continue;

                    accumulate = map.apply(commands, accumulate);
                }
                break;
            case Range:
                AbstractRanges ranges = (AbstractRanges) keysOrRanges;
                for (Range range : ranges)
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
        new VerifyProgressLogCleared(((Node) node), ranges, Arrays.asList(commands.headMap(syncId, false).keySet().toArray(TxnId[]::new))).run();
    }

    private class VerifyProgressLogCleared implements Runnable
    {
        final Node node;
        final Ranges ranges;
        List<TxnId> txnIds;
        int rounds = 0;

        private VerifyProgressLogCleared(Node node, Ranges ranges, List<TxnId> txnIds)
        {
            this.node = node;
            this.ranges = ranges;
            this.txnIds = txnIds;
        }

        @Override
        public void run()
        {
            ++rounds;
            DefaultProgressLog progressLog = (DefaultProgressLog) InMemoryCommandStore.this.progressLog;
            List<TxnId> resubmit = new ArrayList<>();
            for (TxnId txnId : txnIds)
            {
                Command command = commands.get(txnId).value();
                if (!command.hasBeen(PreCommitted)) return;
                if (!command.txnId().isVisible()) return;

                Ranges allRanges = unsafeRangesForEpoch().allBetween(txnId.epoch(), command.executeAtOrTxnId().epoch());
                boolean done = command.hasBeen(Truncated);
                if (!done)
                {
                    if (unsafeGetRedundantBefore().status(txnId, command.route()) == RedundantStatus.PRE_BOOTSTRAP_OR_STALE)
                        return;

                    Route<?> route = command.route().slice(allRanges);
                    done = !route.isEmpty() && ranges.containsAll(route);
                }

                if (done)
                {
                    if (progressLog.isWaitingStateActive(txnId))
                    {
                        Invariants.checkState(rounds < 4);
                        resubmit.add(txnId);
                    }
                    else if (progressLog.isHomeStateActive(txnId))
                    {
                        Invariants.checkState(command.durability() == Local);
                        resubmit.add(txnId);
                    }
                }
            }
            if (!resubmit.isEmpty())
            {
                txnIds = resubmit;
                node.scheduler().once(this, 5L, TimeUnit.SECONDS);
            }
        }
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

        for (Unseekable unseekable : context.keys())
        {
            switch (unseekable.domain())
            {
                case Key:
                    RoutableKey key = (RoutableKey) unseekable;
                    switch (context.keyHistory())
                    {
                        case NONE:
                            continue;
                        case INCR:
                        case SYNC:
                        case ASYNC:
                        case RECOVER:
                            commandsForKey.put(key, commandsForKey((RoutingKey) key).createSafeReference());
                            break;
                        case TIMESTAMPS:
                            timestampsForKey.put(key, timestampsForKey((RoutingKey) key).createSafeReference());
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

    private static Timestamp maxApplied(SafeCommandStore safeStore, Unseekables<?> keysOrRanges, Ranges slice)
    {
        Timestamp max = ((InMemoryCommandStore)safeStore.commandStore()).maxRedundant;
        for (GlobalCommand command : ((InMemoryCommandStore) safeStore.commandStore()).commands.values())
        {
            if (command.value().hasBeen(Applied))
                max = Timestamp.max(command.value().executeAt(), max);
        }
        return max;
    }

    public AsyncChain<Timestamp> maxAppliedFor(Unseekables<?> keysOrRanges, Ranges slice)
    {
        return submit(PreLoadContext.contextFor(keysOrRanges), safeStore -> maxApplied(safeStore, keysOrRanges, slice));
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{id=" + id + ",node=" + node.id().id + '}';
    }

    static class RangeCommand
    {
        final GlobalCommand command;
        Ranges ranges;

        RangeCommand(GlobalCommand command)
        {
            this.command = command;
        }

        void update(Ranges set)
        {
            ranges = set;
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
        private final RoutingKey key;

        public GlobalCommandsForKey(RoutableKey key)
        {
            this.key = (RoutingKey) key;
        }

        public InMemorySafeCommandsForKey createSafeReference()
        {
            return new InMemorySafeCommandsForKey(key, this);
        }
    }

    public static class GlobalTimestampsForKey extends GlobalState<TimestampsForKey>
    {
        private final RoutingKey key;

        public GlobalTimestampsForKey(RoutableKey key)
        {
            this.key = (RoutingKey) key;
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
            for (InMemorySafeCommand cmd : commands.values())
            {
                if (cmd.isUnset()) cmd.uninitialised();
            }
            for (InMemorySafeTimestampsForKey tfk : timestampsForKey.values())
            {
                if (tfk.isUnset()) tfk.initialize();
            }
            for (InMemorySafeCommandsForKey cfk : commandsForKey.values())
            {
                if (cfk.isUnset()) cfk.initialize();
            }
        }

        @Override
        protected InMemorySafeCommand getCommandUnsafe(TxnId txnId)
        {
            return commands.get(txnId);
        }

        @Override
        protected void addCommandUnsafe(InMemorySafeCommand command)
        {
            if (command.isUnset()) command.uninitialised();
            commands.put(command.txnId(), command);
        }

        @Override
        protected InMemorySafeTimestampsForKey getTimestampsForKeyUnsafe(RoutingKey key)
        {
            return timestampsForKey.get(key);
        }

        @Override
        protected void addTimestampsForKeyUnsafe(InMemorySafeTimestampsForKey tfk)
        {
            if (tfk.isUnset()) tfk.initialize();
            timestampsForKey.put(tfk.key(), tfk);
        }

        @Override
        protected InMemorySafeTimestampsForKey getTimestampsForKeyIfUnsafe(RoutingKey key)
        {
            if (!commandStore.canExposeUnloaded())
                return null;
            GlobalTimestampsForKey global = commandStore.timestampsForKeyIfPresent(key);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        protected InMemorySafeCommand getIfLoadedUnsafe(TxnId txnId)
        {
            if (!commandStore.canExposeUnloaded())
                return null;
            GlobalCommand global = commandStore.commandIfPresent(txnId);
            return global != null ? global.createSafeReference() : null;
        }

        @Override
        protected InMemorySafeCommandsForKey getCommandsForKeyUnsafe(RoutingKey key)
        {
            return commandsForKey.get(key);
        }

        @Override
        protected void addCommandsForKeyUnsafe(InMemorySafeCommandsForKey cfk)
        {
            if (cfk.isUnset()) cfk.initialize();
            commandsForKey.put(cfk.key(), cfk);
        }

        @Override
        protected InMemorySafeCommandsForKey getCommandsForKeyIfUnsafe(RoutingKey key)
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
            if (updated.saveStatus() == Erased || updated.saveStatus() == ErasedOrVestigial)
                return;

            Ranges slice = ranges(txnId, updated.executeAtOrTxnId());
            slice = commandStore.unsafeGetRedundantBefore().removeShardRedundant(txnId, updated.executeAtOrTxnId(), slice);
            commandStore.rangeCommands.computeIfAbsent(txnId, ignore -> new RangeCommand(commandStore.commands.get(txnId)))
                         .update(((AbstractRanges)updated.participants().touches()).toRanges().slice(slice, Minimal));
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
        public void upsertRedundantBefore(RedundantBefore addRedundantBefore)
        {
            unsafeUpsertRedundantBefore(addRedundantBefore);
        }

        @Override
        public NodeCommandStoreService node()
        {
            return commandStore.node;
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
        public <P1, T> T mapReduceActive(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            accumulate = commandStore.mapReduceForKey(this, keysOrRanges, (commands, prev) -> {
                return commands.mapReduceActive(startedBefore, testKind, map, p1, prev);
            }, accumulate);

            return mapReduceRangesInternal(keysOrRanges, startedBefore, null, testKind, STARTED_BEFORE, ANY_DEPS, ANY_STATUS, map, p1, accumulate);
        }

        // TODO (expected): instead of accepting a slice, accept the min/max epoch and let implementation handle it
        @Override
        public <P1, T> T mapReduceFull(Unseekables<?> keysOrRanges, TxnId testTxnId, Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            accumulate = commandStore.mapReduceForKey(this, keysOrRanges, (commands, prev) -> {
                return commands.mapReduceFull(testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, prev);
            }, accumulate);

            return mapReduceRangesInternal(keysOrRanges, testTxnId, testTxnId, testKind, testStartedAt, testDep, testStatus, map, p1, accumulate);
        }

        private <P1, T> T mapReduceRangesInternal(Unseekables<?> keysOrRanges, @Nonnull Timestamp testTimestamp, @Nullable TxnId testTxnId, Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T accumulate)
        {
            // TODO (find lib, efficiency): this is super inefficient, need to store Command in something queryable
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

                if (!testKind.test(command.txnId()))
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

                if (!rangeCommand.ranges.intersects(keysOrRanges))
                    return;

                Routables.foldl(rangeCommand.ranges, keysOrRanges, (r, in, i) -> {
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

                    if (!testKind.test(txnId))
                        return;

                    if (!ranges.intersects(keysOrRanges))
                        return;

                    Routables.foldl(ranges, keysOrRanges, (r, in, i) -> {
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

        public Synchronized(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder)
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

        private Cancellable enqueueAndRun(Runnable runnable)
        {
            boolean result = queue.add(runnable);
            if (!result)
                throw illegalState("could not add item to queue");
            maybeRun();
            return () -> queue.remove(runnable);
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
                protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                {
                    return enqueueAndRun(() -> executeInContext(InMemoryCommandStore.Synchronized.this, context, function, callback));
                }
            };
        }

        @Override
        public <T> AsyncChain<T> submit(Callable<T> task)
        {
            return new AsyncChains.Head<T>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                {
                    return enqueueAndRun(() -> {
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

        public SingleThread(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder)
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
            public InMemorySafeCommand ifLoadedAndInitialisedAndNotErasedInternal(TxnId txnId)
            {
                assertThread();
                return super.ifLoadedAndInitialisedAndNotErasedInternal(txnId);
            }

            @Override
            public InMemorySafeCommand getInternal(TxnId txnId)
            {
                assertThread();
                return super.getInternal(txnId);
            }
        }

        public Debug(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder)
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
            logger.info("Node: {}, CommandStore #{}", inMemoryCommandStore.node.id(), commandStore.id());
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
        unsafeSetRejectBefore(new RejectBefore());
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
                AbstractUnseekableKeys keys = null;

                if (CommandsForKey.manages(txnId))
                    keys = (AbstractUnseekableKeys) command.participants().hasTouched();
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
                                 context(command, ASYNC),
                                 safeStore -> {
                                     Command local = command;
                                     if (local.status() != Truncated && local.status() != Invalidated)
                                     {
                                         Cleanup cleanup = Cleanup.shouldCleanup(safeStore, local, local.participants());
                                         switch (cleanup)
                                         {
                                             case NO:
                                                 break;
                                             case INVALIDATE:
                                             case TRUNCATE_WITH_OUTCOME:
                                             case TRUNCATE:
                                             case ERASE:
                                                 local = Commands.purge(local, local.participants(), cleanup);
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
                                     if (local.is(Stable) || local.is(PreApplied))
                                     {
                                         Commands.maybeExecute(safeStore, safeCommand, local, true, true);
                                     }
                                     else if (local.saveStatus().compareTo(Applying) >= 0 && !local.hasBeen(Truncated))
                                     {
                                         unsafeApplyWrites(safeStore, safeCommand, local);
                                     }
                                     return null;
                                 });
            }
        };
    }

    public static void unsafeApplyWrites(SafeCommandStore safeStore, SafeCommand safeCommand, Command command)
    {
        Command.Executed executed = command.asExecuted();
        Participants<?> executes = executed.participants().executes(safeStore, command.txnId(), command.executeAt());
        if (!executes.isEmpty())
        {
            command.writes().applyUnsafe(safeStore, Commands.applyRanges(safeStore, command.executeAt()), command.partialTxn());
            safeCommand.applied(safeStore);
            safeStore.notifyListeners(safeCommand, command);
        }
    }

    @Override
    protected void registerTransitive(SafeCommandStore safeStore, RangeDeps rangeDeps)
    {
        RangesForEpoch rangesForEpoch = this.rangesForEpoch;
        Ranges allRanges = rangesForEpoch.all();

        TreeMap<TxnId, RangeCommand> rangeCommands = this.rangeCommands;
        TreeMap<TxnId, Ranges> historicalRangeCommands = historicalRangeCommands();
        rangeDeps.forEachUniqueTxnId(allRanges, null, (ignore, txnId) -> {
            if (rangeCommands.containsKey(txnId))
                return;

            Ranges ranges = rangeDeps.ranges(txnId);
            if (rangesForEpoch.coordinates(txnId).intersects(ranges))
                return; // already coordinates, no need to replicate
            // TODO (required): check this logic, esp. next line, matches C*
            if (!rangesForEpoch.allSince(txnId.epoch()).intersects(ranges))
                return;

            historicalRangeCommands.merge(txnId, ranges.slice(allRanges), Ranges::with);
        });
    }
}
