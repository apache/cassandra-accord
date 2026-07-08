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

import java.nio.ByteBuffer;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.local.CommandStores.RangesForEpoch;
import accord.local.cfk.NotifySink;
import accord.primitives.*;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.LocalListeners.TxnListener;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.cfr.IdEntry;
import accord.impl.cfr.InMemoryRangeSummaryIndex;
import accord.impl.cfr.LoadListener;
import accord.impl.progresslog.DefaultProgressLog;
import accord.impl.progresslog.TxnState;
import accord.local.Cleanup;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandSummaries;
import accord.local.CommandSummaries.Summary;
import accord.local.CommandSummaries.SummaryLoader;
import accord.local.Commands;
import accord.local.LoadKeysFor;
import accord.local.MaxDecidedRX;
import accord.local.NodeCommandStoreService;
import accord.local.ExecutionContext;
import accord.local.ExecutionContext.Empty;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.StoreParticipants;
import accord.local.cfk.CommandsForKey;
import accord.local.cfk.SafeCommandsForKey;
import accord.local.cfk.Serialize;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.Txn.Kind.Kinds;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;

import static accord.api.ProtocolModifiers.isRangeEndInclusive;
import static accord.api.ProtocolModifiers.isRangeStartInclusive;
import static accord.local.Cleanup.Input.FULL;
import static accord.local.LoadKeys.NONE;
import static accord.local.LoadKeysFor.RECOVERY;
import static accord.local.LoadKeysFor.WRITE;
import static accord.local.RedundantStatus.Coverage.ALL;
import static accord.local.StoreParticipants.Filter.LOAD;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.SaveStatus.Applying;
import static accord.primitives.SaveStatus.Invalidated;
import static accord.primitives.SaveStatus.ReadyToExecute;
import static accord.primitives.Status.Applied;
import static accord.primitives.Status.Committed;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.utils.Invariants.illegalState;
import static java.lang.String.format;

public abstract class InMemoryCommandStore extends CommandStore
{
    private static final Logger logger = LoggerFactory.getLogger(InMemoryCommandStore.class);
    private static final boolean CHECK_DEPENDENCY_INVARIANTS = false;

    public static class Snapshot extends AsyncResults.SettableResult<Snapshot>
    {
        private static final AtomicLong nextId = new AtomicLong();

        private final long id = nextId.incrementAndGet();
        private final NavigableMap<RoutingKey, ByteBuffer> commandsForKey = new TreeMap<>();
        private List<IdEntry> commandsForRanges;
        private List<TxnListener> listeners;
        private List<TxnState> progressLog;
        private int waitingForCfk;

        private Snapshot(){}

        public void restore(InMemoryCommandStore commandStore)
        {
            for (Map.Entry<RoutingKey, ByteBuffer> e : commandsForKey.entrySet())
                commandStore.commandsForKey.computeIfAbsent(e.getKey(), GlobalCommandsForKey::new).value(Serialize.fromBytes(e.getKey(), e.getValue()));

            commandStore.progressLog.clear();
            ((DefaultProgressLog)commandStore.progressLog).restore(null, progressLog);
            ((DefaultLocalListeners)commandStore.listeners).restore(listeners);
            commandStore.commandsForRanges.restore(commandsForRanges);
            commandStore.commandsForRanges.prune(commandStore);
        }

        void saveCallback(CommandsForKey cfk)
        {
            save(cfk);
            if (--waitingForCfk == 0)
                trySuccess(this);
        }

        private void save(CommandsForKey cfk)
        {
            cfk = cfk.maximalPrune();
            ByteBuffer serialized = Serialize.toBytesWithoutKey(cfk);
            CommandsForKey roundTrip = Serialize.fromBytes(cfk.key(), serialized);
            if (roundTrip != null)
            {
                Invariants.require(cfk.equalContents(roundTrip));
                commandsForKey.put(cfk.key(), serialized);
            }
        }

        public static AsyncResult<Snapshot> snapshot(InMemoryCommandStore commandStore)
        {
            Snapshot snapshot = new Snapshot();

            snapshot.listeners = ((DefaultLocalListeners)commandStore.listeners).snapshot();
            snapshot.progressLog = ((DefaultProgressLog)commandStore.progressLog).snapshot();
            snapshot.commandsForRanges = commandStore.commandsForRanges.snapshot();
            for (Map.Entry<RoutableKey, GlobalCommandsForKey> e : commandStore.commandsForKey.entrySet())
            {
                GlobalCommandsForKey global = e.getValue();
                CommandsForKey cfk = global.value();
                if (cfk.isLoadingPruned())
                {
                    ++snapshot.waitingForCfk;
                    global.pendingSnapshots.add(snapshot);
                }
                else
                {
                    snapshot.save(cfk);
                }
            }
            if (snapshot.waitingForCfk == 0)
                snapshot.trySuccess(snapshot);
            return snapshot;
        }
    }

    public static class CommandsForRangeLoad implements Cancellable
    {
        public final SummaryLoader loader;
        public final TreeMap<Timestamp, Summary> loaded;
        public final Cancellable unregister;

        public CommandsForRangeLoad(SummaryLoader loader, TreeMap<Timestamp, Summary> loaded, Cancellable unregister)
        {
            this.loader = loader;
            this.loaded = loaded;
            this.unregister = unregister;
        }

        @Override
        public void cancel()
        {
            unregister.cancel();
        }
    }

    protected final NavigableMap<TxnId, GlobalCommand> commands = new TreeMap<>();
    final NavigableMap<Timestamp, GlobalCommand> commandsByExecuteAt = new TreeMap<>();
    final NavigableMap<RoutableKey, GlobalCommandsForKey> commandsForKey = new TreeMap<>();

    protected final InMemoryRangeSummaryIndex commandsForRanges;

    private InMemorySafeStore current;
    private final Journal journal;

    public InMemoryCommandStore(int id, NodeCommandStoreService node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, RangesForEpoch rangesForEpoch, Journal journal)
    {
        super(id, node, agent, store, progressLogFactory, listenersFactory, rangesForEpoch);
        this.journal = journal;
        this.commandsForRanges = new InMemoryRangeSummaryIndex();
        progressLog.unsafeStart();
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

    @Override
    public AsyncChain<Void> continuationChain(ExecutionContext context, Consumer<? super SafeCommandStore> consumer)
    {
        return chain(context, consumer);
    }

    @Override
    public <T> AsyncChain<T> continuationChain(ExecutionContext context, Function<? super SafeCommandStore, T> function)
    {
        return chain(context, function);
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

    public boolean hasCommand(TxnId txnId)
    {
        return commands.containsKey(txnId);
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
    protected void ensureDurable(Ranges ranges, RedundantBefore onCommandStoreDurable)
    {
        if (node().isReplaying())
            return;

        if (agent instanceof InMemoryAgent)
        {
            ((InMemoryAgent)agent).snapshot(this).invoke((success, fail) -> {
                if (fail == null)
                {
                    execute((Empty)() -> "Report CommandStore Durable", safeStore -> {
                        safeStore.reportDurable(onCommandStoreDurable, 0);
                    });
                }
            });
        }
    }

    @Override
    protected void upsertedRedundantBefore(SafeCommandStore safeStore, RedundantBefore added)
    {
        safeStore = safeStore;
        execute((Empty)() -> "Upsert RedundantBefore", safeStore0 -> {
            for (int i = 0 ; i < added.size() ; ++i)
            {
                if (added.valueAt(i) != null)
                {
                    commandsForKey.subMap(added.startAt(i), isRangeStartInclusive(), added.startAt(i + 1), isRangeEndInclusive()).forEach((forKey, forValue) -> {
                        if (!forValue.isEmpty())
                        {
                            InMemorySafeCommandsForKey safeCfk = forValue.createSafeReference();
                            safeCfk.preExecute();
                            ((InMemorySafeStore)safeStore0).commandsForKey.put(forKey, safeCfk);
                            safeCfk.refresh(safeStore0);
                        }
                    });
                }
            }
            TxnId clearProgressLogBefore = unsafeGetRedundantBefore().minShardAndLocallyAppliedBefore();
            if (progressLog instanceof DefaultProgressLog)
            {
                List<TxnId> clearing = ((DefaultProgressLog) progressLog).activeBefore(clearProgressLogBefore);
                for (TxnId txnId : clearing)
                {
                    GlobalCommand globalCommand = commands.get(txnId);
                    if (globalCommand == null)
                        continue; // now we restore contents from snapshot, we might repopulate older items
                    Command command = globalCommand.value();
                    StoreParticipants participants = command.participants().filter(LOAD, safeStore0, txnId, command.executeAtIfKnown());
                    Cleanup cleanup = Cleanup.shouldCleanup(FULL, txnId, command.executeAtIfKnown(), command.saveStatus(), command.durability(), participants, unsafeGetRedundantBefore(), durableBefore());
                    Invariants.require(command.hasBeen(Applied)
                                       || cleanup.compareTo(Cleanup.TRUNCATE) >= 0
                                       || (durableBefore().min(txnId) != Universal &&
                                              ((command.participants().stillExecutes() != null && command.participants().stillExecutes().isEmpty())
                                              || !Route.isFullRoute(command.route()))));
                }
            }
        });
        super.upsertedRedundantBefore(safeStore, added);
    }

    @Override
    public void markShardDurable(SafeCommandStore safeStore, TxnId syncId, Ranges ranges, HasOutcome level)
    {
        super.markShardDurable(safeStore, syncId, ranges, level);
        if (level == Universal)
            commandsForRanges.prune(syncId, ranges, safeStore.redundantBefore());
    }

    @Override
    protected void markExclusiveSyncPointLocallyApplied(SafeCommandStore safeStore, TxnId txnId, TxnId txnIdWithFlags, RangeRoute route, Ranges ranges, SaveStatus prevStatus)
    {
        super.markExclusiveSyncPointLocallyApplied(safeStore, txnId, txnIdWithFlags, route, ranges, prevStatus);
        commandsForRanges.prune(txnIdWithFlags, ranges, safeStore.redundantBefore());
    }

    protected InMemorySafeStore createSafeStore(ExecutionContext context, CommandsForRangeLoad cfrLoad,
                                                Map<TxnId, InMemorySafeCommand> commands,
                                                Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
    {
        return new InMemorySafeStore(this, context, cfrLoad, commands, commandsForKeys);
    }

    protected void onRead(Command current) {}
    protected void onWrite(Command current) {}
    protected void onRead(CommandsForKey current) {}

    protected final InMemorySafeStore createSafeStore(ExecutionContext context, CommandsForRangeLoad cfrLoad)
    {
        Map<TxnId, InMemorySafeCommand> commands = new HashMap<>();
        Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey = new HashMap<>();

        context.forEachId(txnId -> commands.put(txnId, new InMemorySafeCommand(txnId, command(txnId))));
        if (context.loadKeys() != NONE)
        {
            Unseekables unseekables = context.keys();
            if (unseekables.domain() == Key)
            {
                for (RoutingKey key : (AbstractUnseekableKeys)unseekables)
                {
                    InMemorySafeCommandsForKey safeCfk = commandsForKey(key).createSafeReference();
                    commandsForKey.put(key, safeCfk);
                }
            }
            else if (context.loadKeysFor() != WRITE)
            {
                SummaryLoader loader = SummaryLoader.loader(unsafeGetRedundantBefore(), unsafeGetMaxDecidedRX(), context);
                for (GlobalCommandsForKey global : this.commandsForKey.values())
                {
                    if (!unseekables.contains(global.key))
                        continue;

                    if (global.value() == null || !loader.isRelevant(global.value()))
                        continue;

                    InMemorySafeCommandsForKey safeCfk = commandsForKey.get(global.key);
                    if (safeCfk == null)
                    {
                        safeCfk = global.createSafeReference();
                        commandsForKey.put(global.key, safeCfk);
                    }
                }
            }
        }

        return createSafeStore(context, cfrLoad, commands, commandsForKey);
    }

    public SafeCommandStore beginOperation(ExecutionContext context, @Nullable CommandsForRangeLoad cfrLoad)
    {
        if (current != null)
            throw illegalState("Another operation is in progress or it's store was not cleared");
        return current = createSafeStore(context, cfrLoad);
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

    protected <T> T executeInContext(InMemoryCommandStore commandStore, ExecutionContext executionContext, @Nullable CommandsForRangeLoad cfrLoad, Function<? super SafeCommandStore, T> function)
    {
        SafeCommandStore safeStore = commandStore.beginOperation(executionContext, cfrLoad);
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

    protected <T> void executeInContext(InMemoryCommandStore commandStore, ExecutionContext context, @Nullable CommandsForRangeLoad cfrLoad, Function<? super SafeCommandStore, T> function, BiConsumer<? super T, Throwable> callback)
    {
        try
        {
            T result = executeInContext(commandStore, context, cfrLoad, function);
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
        Ranges ranges;

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
    }

    public static abstract class GlobalState<V>
    {
        private V value;
        Object lockedBy;

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

        public void lock(Object lockedBy)
        {
            Invariants.require(lockedBy != null);
            Invariants.require(this.lockedBy == null);
            this.lockedBy = lockedBy;
        }

        public void unlock(Object lockedBy)
        {
            Invariants.require(this.lockedBy == lockedBy);
            this.lockedBy = null;
        }

        public boolean isLocked()
        {
            return lockedBy != null;
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
        private final RoutingKey key;
        private final List<Snapshot> pendingSnapshots = new ArrayList<>();
        NotifySink overrideSink;

        public GlobalCommandsForKey(RoutableKey key)
        {
            this.key = (RoutingKey) key;
        }

        @Override
        public GlobalState<CommandsForKey> value(CommandsForKey value)
        {
            if (!pendingSnapshots.isEmpty() && !value.isLoadingPruned())
                pendingSnapshots.forEach(snapshot -> snapshot.saveCallback(value));
            return super.value(value);
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
            if (command == null || command.isLocked())
                return null;
            return command.createSafeReference();
        }

        @Override
        public InMemorySafeCommandsForKey acquireIfLoaded(RoutingKey key)
        {
            GlobalCommandsForKey cfk = commandsForKey.get(key);
            if (cfk == null || cfk.isLocked())
                return null;
            return cfk.createSafeReference();
        }
    }

    public static class InMemorySafeStore extends AbstractSafeCommandStore<InMemorySafeCommand, InMemorySafeCommandsForKey, InMemoryCommandStoreCaches>
    {
        final InMemoryCommandStore commandStore;
        protected final Map<TxnId, InMemorySafeCommand> commands;
        private final Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey;
        private final CommandsForRangeLoad cfrLoad;
        private ByTxnIdSnapshot commandsForRanges;

        public InMemorySafeStore(InMemoryCommandStore commandStore,
                                 ExecutionContext context,
                                 CommandsForRangeLoad cfrLoad,
                                 Map<TxnId, InMemorySafeCommand> commands,
                                 Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
        {
            super(context);
            this.commandStore = commandStore;
            this.commands = commands;
            this.commandsForKey = commandsForKey;
            this.cfrLoad = cfrLoad;
            commands.values().forEach(InMemorySafeCommand::preExecute);
            commandsForKey.values().forEach(InMemorySafeCommandsForKey::preExecute);
            if (cfrLoad != null)
                cfrLoad.cancel();
        }

        @Override
        protected InMemorySafeCommand getInternal(TxnId txnId)
        {
            return commands.get(txnId);
        }

        @Override
        public InMemoryCommandStore commandStore()
        {
            return commandStore;
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
            if (((InMemorySafeCommand)result).touch())
                commandStore().onRead(result.current());
            return result;
        }

        @Override
        protected SafeCommand maybeCleanup(SafeCommand safeCommand, @Nonnull StoreParticipants supplemental)
        {
            if (((InMemorySafeCommand)safeCommand).touch())
            {
                SafeCommand result = super.maybeCleanup(safeCommand, safeCommand.current().participants());
                commandStore().onRead(result.current());
                safeCommand = result;
            }
            return super.maybeCleanup(safeCommand, supplemental);
        }

        @Override
        protected SafeCommandsForKey maybeCleanup(SafeCommandsForKey safeCfk)
        {
            safeCfk = super.maybeCleanup(safeCfk);
            if (((InMemorySafeCommandsForKey)safeCfk).touch())
                commandStore().onRead(safeCfk.current());
            return safeCfk;
        }

        @Override
        protected InMemorySafeCommand add(InMemorySafeCommand command, InMemoryCommandStoreCaches caches)
        {
            command.preExecute();
            commands.put(command.txnId(), command);
            return command;
        }

        @Override
        protected InMemorySafeCommandsForKey ifLoadedInternal(RoutingKey key)
        {
            if (context.loadKeys() != NONE && context.keys().domain() == Range && context.keys().contains(key))
            {
                GlobalCommandsForKey globalCfk = commandStore().commandsForKey.get(key);
                if (globalCfk == null || globalCfk.isLocked())
                    return null;

                InMemorySafeCommandsForKey safeCfk = globalCfk.createSafeReference();
                safeCfk.preExecute();
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
            cfk.preExecute();
            commandsForKey.put(cfk.key(), cfk);
            return cfk;
        }

        @Override
        public void updateCommandsForRanges(Command prev, Command updated, boolean force)
        {
            commandStore().commandsForRanges.update(prev, updated, force);
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
            commandStore().commandsForRanges.tryDrainPendingEdits();
            super.postExecute();
            commands.values().forEach(c -> {
                Timestamp executeAt = c.current().executeAtIfKnown();
                if (executeAt != null)
                {
                    if (c.current().hasBeen(Truncated)) commandStore().commandsByExecuteAt.remove(executeAt);
                    else commandStore().commandsByExecuteAt.put(executeAt, commandStore().command(c.txnId()));
                }

                c.postExecute(commandStore);
            });
            commandsForKey.values().forEach(safeCfk -> safeCfk.postExecute(commandStore));
        }

        CommandSummaries commandsForRanges()
        {
            if (commandsForRanges != null)
                return commandsForRanges;

            Invariants.require(context.loadKeysFor() != WRITE);
            MaxDecidedRX maxDecidedRX = commandStore().unsafeGetMaxDecidedRX();
            SummaryLoader loader = cfrLoad != null ? cfrLoad.loader
                                                   : SummaryLoader.loader(redundantBefore(), maxDecidedRX, context);

            TreeMap<Timestamp, Summary> loaded = new TreeMap<>();
            commandStore().commandsForRanges.populateMinFutureRx(loader);
            commandStore().commandsForRanges.search(loader, loaded::put, null);
            if (cfrLoad != null)
                loaded.putAll(cfrLoad.loaded);
            return commandsForRanges = () -> loaded;
        }

        private boolean visitForKey(Unseekables<?> keysOrRanges, Predicate<CommandsForKey> forEach)
        {
            for (SafeCommandsForKey safeCfk : commandsForKey.values())
            {
                if (!keysOrRanges.contains(safeCfk.key()))
                    continue;

                if (!forEach.test(safeCfk.current()))
                    return false;
            }
            return true;
        }

        private <P1, P2> void visitForKey(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visitor, P1 p1, P2 p2)
        {
            visitForKey(keysOrRanges, cfk -> { cfk.visit(startedBefore, testKind, visitor, p1, p2); return true; });
        }

        public boolean visitForKey(Unseekables<?> keysOrRanges, TxnId testTxnId, Kinds testKind, SupersedingCommandVisitor visit)
        {
            return visitForKey(keysOrRanges, cfk -> cfk.visit(testTxnId, testKind, visit));
        }

        @Override
        public <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visitor, P1 p1, P2 p2)
        {
            visitForKey(keysOrRanges, startedBefore, testKind, visitor, p1, p2);
            commandsForRanges().visit(keysOrRanges, startedBefore, testKind, visitor, p1, p2);
        }

        @Override
        public boolean visit(Unseekables<?> keysOrRanges, TxnId testTxnId, Kinds testKind, SupersedingCommandVisitor visit)
        {
            return visitForKey(keysOrRanges, testTxnId, testKind, visit)
                   && commandsForRanges().visit(keysOrRanges, testTxnId, testKind, visit);
        }

        @Override
        public void updateExclusiveSyncPoint(Command prev, Command updated, boolean force)
        {
            super.updateExclusiveSyncPoint(prev, updated, force);
            if (!updated.txnId().isSyncPoint() || updated.txnId().domain() != Range || !updated.hasBeen(Applied) || (prev.hasBeen(Applied) && !force) || updated.hasBeen(Truncated)) return;

            Participants<?> covering = updated.participants().touches();
            for (Map.Entry<TxnId, GlobalCommand> entry : commandStore().commands.headMap(updated.txnId(), false).entrySet())
            {
                GlobalCommand global = entry.getValue();
                Command command = global.isLocked() ? ((InMemorySafeCommand)global.lockedBy).unsafeCurrent() : global.value();
                TxnId txnId = command.txnId();
                if (!command.hasBeen(Committed)) continue;
                if (command.hasBeen(Applied)) continue;
                if (txnId.is(EphemeralRead)) continue;
                Participants<?> intersecting = (txnId.isSyncPoint() ? command.participants().owns(): command.participants().stillWaitsOn()).intersecting(covering, Minimal);
                if (intersecting.isEmpty()) continue;
                if (commandStore().unsafeGetRedundantBefore().isLocallyDefunct(command.txnId(), intersecting, ALL)) continue;
                if (txnId.is(Key))
                {
                    // TODO (required): document our invariants around this scenario, where a transaction with a higher txnId
                    //  but lower executeAt than a transaction that is pre-bootstrap is ignore by the CFK (but cannot be validated to be pre-bootstrap independently).
                    //  This invariant requires a timestamp store for correctness at least (as do other protocol assumptions).
                    //  Make sure these invariants are validated in all relevant locations in the burn test.
                    boolean isShadowedByUnready = true;
                    for (RoutingKey key : (AbstractUnseekableKeys)command.participants().executes())
                    {
                        SafeCommandsForKey safeCfk = commandsForKey.get(key);
                        CommandsForKey cfk = safeCfk.current();
                        int i = cfk.indexOf(cfk.bounds().readyAt);
                        if (i < 0) i = -1 - i;
                        while (--i >= 0)
                        {
                            CommandsForKey.TxnInfo txn = cfk.get(i);
                            if (txn.isCommittedToExecute() && txn.executeAt.compareTo(cfk.readyAt()) > 0)
                                break;
                        }
                        isShadowedByUnready &= i >= 0;
                    }
                    if (isShadowedByUnready) continue;
                }
                illegalState("Prev: %s, updated: %s, command: %s", prev, updated, command);
            }
        }
    }

    protected CommandsForRangeLoad cfrLoad(ExecutionContext context)
    {
        if (context.loadKeysFor() != LoadKeysFor.RECOVERY)
            return null;

        SummaryLoader loader = SummaryLoader.loader(unsafeGetRedundantBefore(), unsafeGetMaxDecidedRX(), context);
        commandsForRanges.populateMinFutureRx(loader);
        TreeMap<Timestamp, Summary> loaded = new TreeMap<>();
        commandsForRanges.search(loader, null, txnId -> {
            Invariants.require(loader.loadKeysFor() == RECOVERY);
            Command command = commands.get(txnId).value();
            Summary summary = loader.ifRelevant(command);
            // TODO (expected): prune implied invalidations from index, so no need to special case
            if (summary == null) Invariants.require(command.saveStatus() == Invalidated);
            else loaded.put(summary.plainTxnId(), summary);
        });
        return new CommandsForRangeLoad(loader, loaded, commandsForRanges.registerListener(new LoadListener(loader, loaded)));
    }

    public static class Synchronized extends InMemoryCommandStore
    {
        Runnable active;
        Thread activeThread;
        final Queue<Runnable> queue = new ConcurrentLinkedQueue<>();

        public Synchronized(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, RangesForEpoch rangesForEpoch, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, rangesForEpoch, journal);
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

        private Cancellable enqueueAndRun(Runnable runnable, @Nullable Cancellable ifCancelled)
        {
            boolean result = queue.add(runnable);
            if (!result)
                throw illegalState("could not add item to queue");
            maybeRun();
            return () -> {
                queue.remove(runnable);
                if (ifCancelled != null)
                    ifCancelled.cancel();
            };
        }

        @Override
        public boolean inStore()
        {
            return activeThread == Thread.currentThread();
        }

        @Override
        public AsyncChain<Void> chain(ExecutionContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return chain(context, i -> { consumer.accept(i); return null; });
        }

        @Override
        public <T> AsyncChain<T> chain(ExecutionContext context, Function<? super SafeCommandStore, T> function)
        {
            return new AsyncChains.Head<T>()
            {
                @Override
                protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                {
                    CommandsForRangeLoad cfrLoad = cfrLoad(context);
                    return enqueueAndRun(() -> executeInContext(InMemoryCommandStore.Synchronized.this, context, cfrLoad, function, callback), cfrLoad);
                }
            };
        }

        @Override
        public void shutdown() {}

        @Override
        public void execute(Runnable run)
        {
            enqueueAndRun(run, null);
        }
    }

    public static class SingleThread extends InMemoryCommandStore
    {
        private Thread thread; // when run in the executor this will be non-null, null implies not running in this store
        private final ExecutorService executor;

        public SingleThread(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, RangesForEpoch rangesForEpoch, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, rangesForEpoch, journal);
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
        public AsyncChain<Void> chain(ExecutionContext context, Consumer<? super SafeCommandStore> consumer)
        {
            return chain(context, i -> { consumer.accept(i); return null; });
        }

        @Override
        public <T> AsyncChain<T> chain(ExecutionContext context, Function<? super SafeCommandStore, T> function)
        {
            // TODO (expected): must unregister if chain is cancelled; should also only register when start() called
            CommandsForRangeLoad cfrLoad = cfrLoad(context);
            return chain(() -> executeInContext(SingleThread.this, context, cfrLoad, function));
        }

        @Override
        public void shutdown()
        {
            executor.shutdownNow();
        }

        @Override
        public void execute(Runnable run)
        {
            executor.execute(run);
        }
    }

    public static class Debug extends SingleThread
    {
        class DebugSafeStore extends InMemorySafeStore
        {
            public DebugSafeStore(InMemoryCommandStore commandStore,
                                  ExecutionContext context,
                                  CommandsForRangeLoad cfrLoad,
                                  Map<TxnId, InMemorySafeCommand> commands,
                                  Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
            {
                super(commandStore, context, cfrLoad, commands, commandsForKey);
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

        public Debug(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, RangesForEpoch rangesForEpoch, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, rangesForEpoch, journal);
        }

        @Override
        protected InMemorySafeStore createSafeStore(ExecutionContext context, CommandsForRangeLoad cfrLoad, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
        {
            return new DebugSafeStore(this, context, cfrLoad, commands, commandsForKeys);
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

    @VisibleForTesting
    public void unsafeClearForTesting()
    {
        super.unsafeClearForTesting();
        commands.clear();
        commandsByExecuteAt.clear();
        commandsForKey.clear();
        commandsForRanges.clear();
        progressLog.clear();
        hasResumedBootstraps = false;
    }

    public Journal.Replayer replayer(AbstractReplayer.Mode mode)
    {
        return new CommandReplayer(this);
    }

    private static class CommandReplayer extends AbstractReplayer
    {
        private final InMemoryCommandStore commandStore;

        private CommandReplayer(InMemoryCommandStore commandStore)
        {
            // TODO (required): we shouldn't be providing TxnId.NONE here, we need to standardise on querying journal for data missing from InMemoryCommandStore
            super(commandStore, Mode.PART_NON_DURABLE, TxnId.NONE);
            this.commandStore = commandStore;
        }

        private AsyncChain<Void> apply(Command command, Replay replay)
        {
            return AsyncChains.success(commandStore.executeInContext(commandStore,
                                                                     ExecutionContext.unsequenced(command.txnId(), "Replay"),
                                                                     null,
                                                                     (SafeCommandStore safeStore) -> {
                                                                         super.replay(safeStore, command.txnId(), replay);
                                                                         return null;
                                                                     }));
        }

        @Override
        public AsyncChain<Void> replay(TxnId txnId)
        {
            // TODO (required): consider this race condition some more:
            //      - can we avoid double-applying?
            //      - is this definitely safe?
            Command command = null;
            if (commandStore.hasCommand(txnId))
                command = commandStore.commands.get(txnId).value();

            if (command == null)
            {
                command = commandStore.journal.loadCommand(commandStore.id, txnId, commandStore.unsafeGetRedundantBefore(), commandStore.durableBefore());
                if (command != null)
                {
                    Cleanup cleanup = Cleanup.shouldCleanup(FULL, command, commandStore.unsafeGetRedundantBefore(), commandStore.durableBefore());
                    if (cleanup != Cleanup.NO)
                        command = Commands.purgeUnsafe(commandStore, command, cleanup);

                    // initialise basic state, but don't call safeStore.update so we don't initialise listeners etc
                    GlobalCommand global = commandStore.commands.computeIfAbsent(txnId, GlobalCommand::new);
                    global.value(command);
                    Timestamp executeAt = command.executeAtIfKnown();
                    if (executeAt != null)
                        commandStore.commandsByExecuteAt.put(executeAt, global);
                }
            }

            if (command == null || !maybeShouldReplay(txnId))
                return AsyncChains.success(null);

            Replay replay = shouldReplay(txnId, command.participants());
            if (replay == Replay.NONE)
                return AsyncChains.success(null);

            return apply(command, replay);
        }
    }
}
