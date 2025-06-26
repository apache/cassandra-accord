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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.progresslog.DefaultProgressLog;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommandSummaries;
import accord.local.Commands;
import accord.local.KeyHistory;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RejectBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutableKey;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;
import org.agrona.collections.ObjectHashSet;

import static accord.local.Cleanup.Input.FULL;
import static accord.local.KeyHistory.ASYNC;
import static accord.local.KeyHistory.NONE;
import static accord.local.RedundantStatus.Coverage.ALL;
import static accord.local.StoreParticipants.Filter.LOAD;
import static accord.primitives.Known.KnownRoute.MaybeRoute;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.Erased;
import static accord.primitives.SaveStatus.NotDefined;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.SaveStatus.Vestigial;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Committed;
import static accord.primitives.Status.Durability.UniversalOrInvalidated;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.ExclusiveSyncPoint;
import static accord.primitives.Txn.Kind.Read;
import static accord.utils.Invariants.illegalState;
import static java.lang.String.format;

public abstract class InMemoryCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryCommandStore.class);
    private static final boolean CHECK_DEPENDENCY_INVARIANTS = false;

    final NavigableMap<TxnId, GlobalCommand> commands = new TreeMap<>();
    final NavigableMap<Timestamp, GlobalCommand> commandsByExecuteAt = new TreeMap<>();
    private final NavigableMap<RoutableKey, GlobalCommandsForKey> commandsForKey = new TreeMap<>();

    private final TreeMap<TxnId, RangeCommand> rangeCommands = new TreeMap<>();
    protected Timestamp maxRedundant = Timestamp.NONE;

    private InMemorySafeStore current;
    private final Journal journal;

    public InMemoryCommandStore(int id, NodeCommandStoreService node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder, Journal journal)
    {
        super(id, node, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder);
        this.journal = journal;
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
                        // TODO (testing): look forwards also, but we only need to look at ?=ReadyToExecute transactions as they have already run their backwards checks
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

                            if (!cur.txnId().witnesses(prev.txnId()) && !cur.partialDeps().contains(prev.txnId()))
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
        TxnId clearProgressLogBefore = unsafeGetRedundantBefore().minShardAndLocallyAppliedBefore();
        List<TxnId> clearing = ((DefaultProgressLog) progressLog).activeBefore(clearProgressLogBefore);
        for (TxnId txnId : clearing)
        {
            GlobalCommand globalCommand = commands.get(txnId);
            Invariants.require(globalCommand != null && !globalCommand.isEmpty());
            Command command = globalCommand.value();
            StoreParticipants participants = command.participants().filter(LOAD, safeStore, txnId, command.executeAtIfKnown());
            Cleanup cleanup = Cleanup.shouldCleanup(FULL, txnId, command.executeAtIfKnown(), command.saveStatus(), command.durability(), participants, unsafeGetRedundantBefore(), durableBefore());
            Invariants.require(command.hasBeen(Applied)
                               || cleanup.compareTo(Cleanup.TRUNCATE) >= 0
                               || (durableBefore().min(txnId) != UniversalOrInvalidated &&
                                      ((command.participants().stillExecutes() != null && command.participants().stillExecutes().isEmpty())
                                      || !Route.isFullRoute(command.route()))));
        }
        super.updatedRedundantBefore(safeStore, syncId, ranges);
    }

    @Override
    public void markShardDurable(SafeCommandStore safeStore, TxnId syncId, Ranges ranges, Status.Durability durability)
    {
        super.markShardDurable(safeStore, syncId, ranges, durability);
        if (durability.compareTo(UniversalOrInvalidated) >= 0)
            markShardDurable(syncId, ranges);
    }

    private void markShardDurable(TxnId syncId, Ranges ranges)
    {
        rangeCommands.computeIfAbsent(syncId, RangeCommand::new).add(ranges);
        rangeCommands.headMap(syncId, false).entrySet().removeIf(tx -> {
            Ranges newRanges = tx.getValue().ranges.without(ranges);
            if (!newRanges.isEmpty())
            {
                tx.getValue().ranges = newRanges;
                return false;
            }
            else
            {
                GlobalCommand global = tx.getValue().command;
                if (global != null)
                    maxRedundant = Timestamp.nonNullOrMax(maxRedundant, global.value().executeAt());
                return true;
            }
        });
    }

    protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges,
                                                Map<TxnId, InMemorySafeCommand> commands,
                                                Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
    {
        return new InMemorySafeStore(this, ranges, context, commands, commandsForKeys);
    }

    protected void onRead(Command current) {}
    protected void onWrite(Command current) {}
    protected void onRead(CommandsForKey current) {}

    protected final InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges)
    {
        Map<TxnId, InMemorySafeCommand> commands = new HashMap<>();
        Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey = new HashMap<>();

        context.forEachId(txnId -> commands.put(txnId, lazyReference(txnId)));

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
                        default: throw new UnsupportedOperationException("Unknown key history: " + context.keyHistory());
                    }
                    break;
                case Range:
                    // load range cfks here
                    break;
            }
        }
        return createSafeStore(context, ranges, commands, commandsForKey);
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

    protected <T> T executeInContext(InMemoryCommandStore commandStore, PreLoadContext preLoadContext, Function<? super SafeCommandStore, T> function)
    {
        SafeCommandStore safeStore = commandStore.beginOperation(preLoadContext);
        try
        {
            return function.apply(safeStore);
        }
        // for history: we don't want to wrap exceptions here as we can't then handle the specific exception effectively
        finally
        {
            commandStore.completeOperation(safeStore);
        }
    }

    protected <T> void executeInContext(InMemoryCommandStore commandStore, PreLoadContext context, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
    {
        try
        {
            T result = executeInContext(commandStore, context, function);
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
        return getClass().getSimpleName() + "{id=" + id + ",node=" + node.id().id + '}';
    }

    static class RangeCommand
    {
        final TxnId txnId;
        @Nullable GlobalCommand command;
        Ranges ranges;

        RangeCommand(GlobalCommand command)
        {
            this.txnId = command.txnId;
            this.command = command;
        }

        RangeCommand(TxnId txnId)
        {
            this.txnId = txnId;
        }

        void update(Ranges set)
        {
            ranges = set;
        }

        void add(Ranges add)
        {
            if (ranges == null) ranges = add;
            else ranges = ranges.with(add);
        }

        void update(GlobalCommand set)
        {
            command = set;
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

    public class InMemoryCommandStoreCaches implements AbstractSafeCommandStore.CommandStoreCaches<InMemorySafeCommand, InMemorySafeCommandsForKey>
    {
        @Override
        public void close() {}

        @Override
        public InMemorySafeCommand acquireIfLoaded(TxnId txnId)
        {
            GlobalCommand command = commands.get(txnId);
            if (command == null)
                return null;
            return command.createSafeReference();
        }

        @Override
        public InMemorySafeCommandsForKey acquireIfLoaded(RoutingKey key)
        {
            GlobalCommandsForKey cfk = commandsForKey.get(key);
            if (cfk == null)
                return null;
            return cfk.createSafeReference();
        }
    }

    public static class InMemorySafeStore extends AbstractSafeCommandStore<InMemorySafeCommand, InMemorySafeCommandsForKey, InMemoryCommandStoreCaches>
    {
        protected final Map<TxnId, InMemorySafeCommand> commands;
        private final Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey;
        private final Set<Object> hasLoaded = new ObjectHashSet<>();
        private ByTxnIdSnapshot commandsForRanges;

        public InMemorySafeStore(InMemoryCommandStore commandStore,
                                 RangesForEpoch ranges,
                                 PreLoadContext context,
                                 Map<TxnId, InMemorySafeCommand> commands,
                                 Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
        {
            super(context, commandStore);

            super.setRangesForEpoch(ranges);
            this.commands = commands;
            this.commandsForKey = commandsForKey;
            for (InMemorySafeCommand cmd : commands.values())
            {
                if (cmd.isUnset()) cmd.uninitialised();
            }
            for (InMemorySafeCommandsForKey cfk : commandsForKey.values())
            {
                if (cfk.isUnset()) cfk.initialize();
            }
        }

        @Override
        protected InMemorySafeCommand getInternal(TxnId txnId)
        {
            return commands.get(txnId);
        }

        @Override
        public InMemoryCommandStore commandStore()
        {
            return (InMemoryCommandStore) super.commandStore();
        }

        @Override
        protected InMemoryCommandStoreCaches tryGetCaches()
        {
            if (commandStore().canExposeUnloaded())
                return commandStore().new InMemoryCommandStoreCaches();
            return null;
        }

        @Override
        protected SafeCommand maybeCleanup(SafeCommand safeCommand)
        {
            SafeCommand result = super.maybeCleanup(safeCommand);
            if (!((InMemorySafeCommand)result).isModified() && hasLoaded.add(safeCommand.txnId()))
                commandStore().onRead(result.current());
            return result;
        }

        @Override
        protected SafeCommand maybeCleanup(SafeCommand safeCommand, @Nonnull StoreParticipants supplemental)
        {
            SafeCommand result = super.maybeCleanup(safeCommand, supplemental);
            if (!((InMemorySafeCommand)result).isModified() && hasLoaded.add(safeCommand.txnId()))
                commandStore().onRead(result.current());
            return result;
        }

        @Override
        protected SafeCommandsForKey maybeCleanup(SafeCommandsForKey safeCfk)
        {
            safeCfk = super.maybeCleanup(safeCfk);
            if (hasLoaded.add(safeCfk.key()))
                commandStore().onRead(safeCfk.current());
            return safeCfk;
        }

        @Override
        protected InMemorySafeCommand add(InMemorySafeCommand command, InMemoryCommandStoreCaches caches)
        {
            if (command.isUnset()) command.uninitialised();
            commands.put(command.txnId(), command);
            return command;
        }

        @Override
        protected InMemorySafeCommandsForKey ifLoadedInternal(RoutingKey key)
        {
            if (context.keyHistory() != NONE && context.keys().domain() == Range && context.keys().contains(key))
            {
                GlobalCommandsForKey globalCfk = commandStore().commandsForKey.get(key);
                if (globalCfk == null)
                    return null;

                InMemorySafeCommandsForKey safeCfk = globalCfk.createSafeReference();
                commandsForKey.put(key, safeCfk);
                return safeCfk;
            }

            return super.ifLoadedInternal(key);
        }

        @Override
        protected InMemorySafeCommandsForKey getInternal(RoutingKey key)
        {
            return commandsForKey.get(key);
        }

        @Override
        protected InMemorySafeCommandsForKey add(InMemorySafeCommandsForKey cfk, InMemoryCommandStoreCaches caches)
        {
            if (cfk.isUnset()) cfk.initialize();
            commandsForKey.put(cfk.key(), cfk);
            return cfk;
        }

        @Override
        protected void update(Command prev, Command updated, boolean force)
        {
            super.update(prev, updated, force);

            TxnId txnId = updated.txnId();
            if (txnId.domain() != Domain.Range)
                return;

            // TODO (testing): consider removing if erased
            if (updated.saveStatus() == Erased || updated.saveStatus() == Vestigial)
                return;

            commandStore().rangeCommands.computeIfAbsent(txnId, ignore -> new RangeCommand(commandStore().commands.get(txnId)))
                         .update(((AbstractRanges)updated.participants().stillTouches()).toRanges());
        }

        @Override
        public DataStore dataStore()
        {
            return commandStore().dataStore;
        }

        @Override
        public Agent agent()
        {
            return commandStore().agent;
        }

        @Override
        public ProgressLog progressLog()
        {
            return commandStore().progressLog;
        }

        @Override
        public NodeCommandStoreService node()
        {
            return commandStore().node;
        }

        public void postExecute()
        {
            super.postExecute();
            commands.values().forEach(c -> {
                if (c == null || c.current() == null)
                    return;

                Timestamp executeAt = c.current().executeAtIfKnown();
                if (executeAt != null)
                {
                    if (c.current().hasBeen(Truncated)) commandStore().commandsByExecuteAt.remove(executeAt);
                    else commandStore().commandsByExecuteAt.put(executeAt, commandStore().command(c.txnId()));
                }

                if (c.isUnset() || c.current().saveStatus().isUninitialised())
                    commandStore().commands.remove(c.txnId());

                c.invalidate();
            });
            commandsForKey.values().forEach(cfk -> {
                if (cfk.isUnset())
                    commandStore().commandsForKey.remove(cfk.key());
                cfk.invalidate();
            });
        }

        CommandSummaries commandsForRanges()
        {
            if (commandsForRanges != null)
                return commandsForRanges;

            Summary.Loader loader = Summary.Loader.loader(redundantBefore(), context.primaryTxnId(), context.keyHistory(), context.keys());
            TreeMap<Timestamp, Summary> summaries = new TreeMap<>();
            for (RangeCommand rangeCommand : commandStore().rangeCommands.values())
            {
                GlobalCommand global = rangeCommand.command;
                Command command = global == null ? null : global.value();
                Summary summary;
                if (command == null)
                {
                    summary = loader.ifRelevant(rangeCommand.txnId, rangeCommand.txnId, NotDefined, rangeCommand.ranges, null);
                }
                else
                {
                    summary = loader.ifRelevant(command);
                }
                if (summary != null)
                    summaries.put(summary.txnId, summary);
            }

            final Kinds kinds = new Kinds(Read, ExclusiveSyncPoint);
            return commandsForRanges = new ByTxnIdSnapshot()
            {
                @Override public NavigableMap<Timestamp, Summary> byTxnId() { return summaries; }
            };
        }

        private boolean visitForKey(Unseekables<?> keysOrRanges, Predicate<CommandsForKey> forEach)
        {
            for (GlobalCommandsForKey global : commandStore().commandsForKey.values())
            {
                if (!keysOrRanges.contains(global.key))
                    continue;

                InMemorySafeCommandsForKey safeCfk = commandsForKey.get(global.key);
                if (safeCfk == null)
                    commandsForKey.put(global.key, safeCfk = global.createSafeReference());

                if (!forEach.test(safeCfk.current()))
                    return false;
            }
            return true;
        }

        @Override
        public <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visitor, P1 p1, P2 p2)
        {
            visitForKey(keysOrRanges, cfk -> { cfk.visit(startedBefore, testKind, visitor, p1, p2); return true; });
            commandsForRanges().visit(keysOrRanges, startedBefore, testKind, visitor, p1, p2);
        }

        @Override
        public boolean visit(Unseekables<?> keysOrRanges, TxnId testTxnId, Kinds testKind, TestStartedAt testStartedAt, Timestamp testStartedAtTimestamp, ComputeIsDep computeIsDep, AllCommandVisitor visit)
        {
            return visitForKey(keysOrRanges, cfk -> cfk.visit(testTxnId, testKind, testStartedAt, testStartedAtTimestamp, computeIsDep, null, visit))
                   && commandsForRanges().visit(keysOrRanges, testTxnId, testKind, testStartedAt, testStartedAtTimestamp, computeIsDep, visit);
        }

        @Override
        public void updateExclusiveSyncPoint(Command prev, Command updated, boolean force)
        {
            super.updateExclusiveSyncPoint(prev, updated, force);
            if (updated.txnId().kind() != Txn.Kind.ExclusiveSyncPoint || updated.txnId().domain() != Range || !updated.hasBeen(Applied) || (prev.hasBeen(Applied) && !force) || updated.hasBeen(Truncated)) return;

            Participants<?> covering = updated.participants().touches();
            for (Map.Entry<TxnId, GlobalCommand> entry : commandStore().commands.headMap(updated.txnId(), false).entrySet())
            {
                Command command = entry.getValue().value();
                TxnId txnId = command.txnId();
                if (!command.hasBeen(Committed)) continue;
                if (command.hasBeen(Applied)) continue;
                if (txnId.is(EphemeralRead)) continue;
                Participants<?> intersecting = (txnId.is(ExclusiveSyncPoint) ? command.participants().owns(): command.participants().stillWaitsOn()).intersecting(covering, Minimal);
                if (intersecting.isEmpty()) continue;
                if (commandStore().unsafeGetRedundantBefore().locallyDefunct(command.txnId(), intersecting) == ALL) continue;
                if (txnId.is(Key))
                {
                    // TODO (required): document our invariants around this scenario, where a transaction with a higher txnId
                    //  but lower executeAt than a transaction that is pre-bootstrap is ignore by the CFK (but cannot be validated to be pre-bootstrap independently).
                    //  This invariant requires a timestamp store for correctness at least (as do other protocol assumptions).
                    //  Make sure these invariants are validated in all relevant locations in the burn test.
                    boolean isShadowedByPreBootstrap = true;
                    for (RoutingKey key : (AbstractUnseekableKeys)command.participants().executes())
                    {
                        SafeCommandsForKey safeCfk = commandsForKey.get(key);
                        CommandsForKey cfk = safeCfk.current();
                        int i = cfk.indexOf(cfk.bounds().bootstrappedAt);
                        if (i < 0) i = -1 - i;
                        while (--i >= 0)
                        {
                            CommandsForKey.TxnInfo txn = cfk.get(i);
                            if (txn.isCommittedToExecute() && txn.executeAt.compareTo(cfk.bootstrappedAt()) > 0)
                                break;
                        }
                        isShadowedByPreBootstrap &= i >= 0;
                    }
                    if (isShadowedByPreBootstrap) continue;
                }
                illegalState(String.format("Prev: %s, updated: %s, command: %s", prev, updated, command));
            }
        }
    }

    public static class Synchronized extends InMemoryCommandStore
    {
        Runnable active;
        Thread activeThread;
        final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        public Synchronized(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder, journal);
        }

        private synchronized void maybeRun()
        {
            if (active != null)
                return;

            active = queue.poll();
            activeThread = Thread.currentThread();
            while (active != null)
            {
                try { active.run(); }
                catch (Throwable t) { logger.error("Uncaught exception", t); }
                active = queue.poll();
            }
            activeThread = null;
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
            return activeThread == Thread.currentThread();
        }

        @Override
        public AsyncChain<Void> build(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return build(context, i -> { consumer.accept(i); return null; });
        }

        @Override
        public <T> AsyncChain<T> build(PreLoadContext context, Function<? super SafeCommandStore, T> function)
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
        public <T> AsyncChain<T> build(Callable<T> task)
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

        public SingleThread(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder, journal);
            this.executor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + time.id() + ']');
                return thread;
            });
            // "this" is leaked before constructor is completed, but since all fields are "final" and set before "this"
            // is leaked, then visibility should not be an issue.
            executor.execute(() -> thread = Thread.currentThread());
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
        public AsyncChain<Void> build(PreLoadContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return build(context, i -> { consumer.accept(i); return null; });
        }

        @Override
        public <T> AsyncChain<T> build(PreLoadContext context, Function<? super SafeCommandStore, T> function)
        {
            return AsyncChains.ofCallable(executor, () -> executeInContext(this, context, function));
        }

        @Override
        public <T> AsyncChain<T> build(Callable<T> task)
        {
            return AsyncChains.ofCallable(executor, task);
        }

        @Override
        public void shutdown()
        {
            executor.shutdownNow();
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
                                  Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
            {
                super(commandStore, ranges, context, commands, commandsForKey);
            }

            @Override
            public InMemorySafeCommand ifLoadedInternal(TxnId txnId)
            {
                assertThread();
                return super.ifLoadedInternal(txnId);
            }

            @Override
            public InMemorySafeCommand getInternal(TxnId txnId)
            {
                assertThread();
                return super.getInternal(txnId);
            }
        }

        public Debug(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder, journal);
        }

        @Override
        protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
        {
            return new DebugSafeStore(this, ranges, context, commands, commandsForKeys);
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

    @Override
    public void unsafeClearForTesting()
    {
        super.unsafeClearForTesting();
        Invariants.require(current == null);
        progressLog.clear();
        commands.clear();
        commandsByExecuteAt.clear();
        commandsForKey.clear();
        rangeCommands.clear();
        unsafeSetRejectBefore(new RejectBefore());
    }

    public Journal.Loader loader()
    {
        return new CommandLoader(this);
    }

    private static class CommandLoader extends AbstractLoader
    {
        private final InMemoryCommandStore commandStore;

        private CommandLoader(InMemoryCommandStore commandStore)
        {
            this.commandStore = commandStore;
        }

        private PreLoadContext context(Command command, KeyHistory keyHistory)
        {
            TxnId txnId = command.txnId();
            AbstractUnseekableKeys keys = null;

            if (CommandsForKey.manages(txnId))
                keys = (AbstractUnseekableKeys) command.participants().hasTouched();
            else if (!CommandsForKey.managesExecution(txnId) && command.hasBeen(Status.Stable) && !command.hasBeen(Status.Truncated))
                keys = command.asCommitted().waitingOn.keys;

            if (keys != null)
                return PreLoadContext.contextFor(txnId, keys, keyHistory);

            return txnId;
        }

        protected TxnId loadInternal(Command command, SafeCommandStore safeStore)
        {
            TxnId txnId = command.txnId();
            Cleanup cleanup = Cleanup.shouldCleanup(FULL, safeStore, command, command.participants());
            if (cleanup != Cleanup.NO)
                command = Commands.purge(safeStore, command, cleanup);

            safeStore.unsafeGetNoCleanup(txnId).update(safeStore, command);
            return txnId;
        }

        private AsyncChain<TxnId> load(Command command)
        {
            return AsyncChains.success(commandStore.executeInContext(commandStore,
                                                                     context(command, ASYNC),
                                                                     (SafeCommandStore safeStore) -> loadInternal(command, safeStore)));
        }

        private AsyncChain<Void> apply(TxnId txnId)
        {
            return AsyncChains.success(commandStore.executeInContext(commandStore,
                                                                     txnId,
                                                                     (SafeCommandStore safeStore) -> {
                                                                         maybeApplyWrites(safeStore, txnId);
                                                                         return null;
                                                                     }));
        }

        @Override
        public AsyncChain<Void> load(TxnId txnId)
        {
            // TODO (required): consider this race condition some more:
            //      - can we avoid double-applying?
            //      - is this definitely safe?
            if (commandStore.hasCommand(txnId))
                return apply(txnId);

            Command command = commandStore.journal.loadCommand(commandStore.id, txnId, commandStore.unsafeGetRedundantBefore(), commandStore.durableBefore());
            if (command == null)
                return AsyncChains.success(null);
            return load(command).flatMap(this::apply);
        }
    }

    @Override
    protected void registerTransitive(SafeCommandStore safeStore, RangeDeps rangeDeps)
    {
        RangesForEpoch rangesForEpoch = this.rangesForEpoch;
        Ranges allRanges = rangesForEpoch.all();

        TreeMap<TxnId, RangeCommand> rangeCommands = this.rangeCommands;
        rangeDeps.forEachUniqueTxnId(allRanges, null, (ignore, txnId) -> {
            GlobalCommand global = commands.get(txnId);
            if (global != null && global.value().known().has(MaybeRoute))
                return;

            Ranges ranges = rangeDeps.ranges(txnId);
            ranges = ranges.without(rangesForEpoch.coordinates(txnId));  // already coordinates, no need to replicate
            if (ranges.isEmpty())
                return;

            ranges = ranges.slice(rangesForEpoch.allSince(txnId.epoch()), Minimal); // never coordinated, no need to replicate for dependency or recovery calculations
            if (ranges.isEmpty())
                return;

            rangeCommands.computeIfAbsent(txnId, RangeCommand::new).add(ranges);
        });
    }
}
