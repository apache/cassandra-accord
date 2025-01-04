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

package accord.impl.basic;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableSortedMap;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStores;
import accord.impl.InMemorySafeCommand;
import accord.impl.InMemorySafeCommandsForKey;
import accord.impl.PrefixedIntHashKey;
import accord.impl.basic.TaskExecutorService.Task;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.local.ShardDistributor;
import accord.local.cfk.CommandsForKey;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.api.Journal.CommandUpdate;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.HIGH;

public class DelayedCommandStores extends InMemoryCommandStores.SingleThread
{
    private DelayedCommandStores(NodeCommandStoreService time, Agent agent, DataStore store, RandomSource random, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, SimulatedDelayedExecutorService executorService, CacheLoading isLoadedCheck, Journal journal)
    {
        super(time, agent, store, random, journal, shardDistributor, progressLogFactory, listenersFactory, DelayedCommandStore.factory(executorService, isLoadedCheck, journal));
    }

    public static CommandStores.Factory factory(PendingQueue pending, CacheLoading isLoadedCheck)
    {
        return (time, agent, store, random, journal, shardDistributor, progressLogFactory, listenersFactory) ->
               new DelayedCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, listenersFactory, new SimulatedDelayedExecutorService(pending, agent), isLoadedCheck, journal);
    }

    @Override
    protected boolean shouldBootstrap(Node node, Topology previous, Topology updated, Range range)
    {
        if (!super.shouldBootstrap(node, previous, updated, range))
            return false;
        if (!(range.start() instanceof PrefixedIntHashKey)) return true;
        int prefix = ((PrefixedIntHashKey) range.start()).prefix;
        // we see new prefix when a new prefix is added, so avoid bootstrap in these cases
        return contains(previous, prefix);
    }

    protected void loadSnapshot(Snapshot nextSnapshot)
    {
        Snapshot current = current();
        // These checks are only applicable to delayed command stores.
        for (Integer id : current.byId.keySet())
        {
            CommandStore prev = current.byId.get(id);
            CommandStore next = nextSnapshot.byId.get(id);
            {
                RedundantBefore orig = prev.unsafeGetRedundantBefore();
                RedundantBefore loaded = next.unsafeGetRedundantBefore();
                Invariants.checkState(orig.equals(loaded), "%s should equal %s", loaded, orig);
            }

            {
                NavigableMap<TxnId, Ranges> orig = prev.unsafeGetBootstrapBeganAt();
                NavigableMap<TxnId, Ranges> loaded = next.unsafeGetBootstrapBeganAt();
                Invariants.checkState(orig.equals(loaded), "%s should equal %s", loaded, orig);
            }

            {
                NavigableMap<Timestamp, Ranges> orig = prev.unsafeGetSafeToRead();
                NavigableMap<Timestamp, Ranges> loaded = next.unsafeGetSafeToRead();
                Invariants.checkState(orig.equals(loaded), "%s should equal %s", loaded, orig);
            }

            {
                RangesForEpoch orig = prev.unsafeGetRangesForEpoch();
                RangesForEpoch loaded = next.unsafeGetRangesForEpoch();
                Invariants.checkState(orig.equals(loaded), "%s should equal %s", loaded, orig);
            }
        }

        super.loadSnapshot(nextSnapshot);
    }

    private static boolean contains(Topology previous, int searchPrefix)
    {
        for (Range range : previous.ranges())
        {
            int prefix = ((PrefixedIntHashKey) range.start()).prefix;
            if (prefix == searchPrefix)
                return true;
        }
        return false;
    }

    public static class DelayedCommandStore extends InMemoryCommandStore
    {
        public class DelayedTask<T> extends Task<T>
        {
            private DelayedTask(Callable<T> fn)
            {
                super(fn);
            }

            public DelayedCommandStore parent()
            {
                return DelayedCommandStore.this;
            }

            @Override
            public void run()
            {
                unsafeRunIn(super::run);
            }

            public Callable<T> callable()
            {
                return callable;
            }
        }

        private final SimulatedDelayedExecutorService executor;
        private final Queue<Task<?>> pending = new LinkedList<>();
        private final CacheLoading cacheLoading;
        private final Journal journal;

        public DelayedCommandStore(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder, SimulatedDelayedExecutorService executor, CacheLoading cacheLoading, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder);
            this.executor = executor;
            this.cacheLoading = cacheLoading;
            this.journal = journal;
            restore();
        }

        protected void loadRedundantBefore(RedundantBefore redundantBefore)
        {
            if (redundantBefore == null)
            {
                Invariants.checkState(unsafeGetRedundantBefore().size() == 0);
            }
            else
            {
                unsafeClearRedundantBefore();
                super.loadRedundantBefore(redundantBefore);
            }
        }

        protected void loadBootstrapBeganAt(NavigableMap<TxnId, Ranges> bootstrapBeganAt)
        {
            unsafeClearBootstrapBeganAt();
            if (bootstrapBeganAt == null) bootstrapBeganAt = ImmutableSortedMap.of(TxnId.NONE, Ranges.EMPTY);
            super.loadBootstrapBeganAt(bootstrapBeganAt);
        }

        protected void loadSafeToRead(NavigableMap<Timestamp, Ranges> safeToRead)
        {
            unsafeClearSafeToRead();
            if (safeToRead == null) safeToRead = ImmutableSortedMap.of(Timestamp.NONE, Ranges.EMPTY);
            super.loadSafeToRead(safeToRead);
        }

        @Override
        protected void loadRangesForEpoch(RangesForEpoch newRangesForEpoch)
        {
            if (newRangesForEpoch == null) Invariants.checkState(super.rangesForEpoch == null);
            else
            {
                unsafeClearRangesForEpoch();
                super.loadRangesForEpoch(newRangesForEpoch);
            }
        }

        public void restore()
        {
            loadRedundantBefore(journal.loadRedundantBefore(id()));
            loadBootstrapBeganAt(journal.loadBootstrapBeganAt(id()));
            loadSafeToRead(journal.loadSafeToRead(id()));
            loadRangesForEpoch(journal.loadRangesForEpoch(id()));
        }

        @Override
        public void onRead(Command current)
        {
            cacheLoading.validate(this, current);
        }

        @Override
        public void onRead(CommandsForKey current)
        {
            cacheLoading.validate(this, current);
        }

        @Override
        protected boolean canExposeUnloaded()
        {
            return !cacheLoading.cacheEmpty();
        }

        private static CommandStore.Factory factory(SimulatedDelayedExecutorService executor, CacheLoading isLoadedCheck, Journal journal)
        {
            return (id, node, agent, store, progressLogFactory, listenersFactory, rangesForEpoch) -> new DelayedCommandStore(id, node, agent, store, progressLogFactory, listenersFactory, rangesForEpoch, executor, isLoadedCheck, journal);
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
            return submit(() -> executeInContext(this, context, function));
        }

        @Override
        public <T> AsyncChain<T> submit(Callable<T> fn)
        {
            Task<T> task = new DelayedTask<>(fn);
            if (Invariants.testParanoia(LINEAR, LINEAR, HIGH))
            {
                return AsyncChains.detectLeak(agent::onUncaughtException, () -> {
                    boolean wasEmpty = pending.isEmpty();
                    pending.add(task);
                    if (wasEmpty)
                        runNextTask();
                }).flatMap(ignore -> task);
            }
            else
            {
                return new AsyncChains.Head<T>()
                {
                    @Override
                    protected Cancellable start(BiConsumer<? super T, Throwable> callback)
                    {
                        boolean wasEmpty = pending.isEmpty();
                        pending.add(task);
                        if (wasEmpty)
                            runNextTask();
                        task.begin(callback);
                        return () -> {
                            if (pending.peek() != task)
                                pending.remove(task);
                        };
                    }
                };
            }
        }

        private void runNextTask()
        {
            Task<?> next = pending.peek();
            if (next == null)
                return;

            next.addCallback(agent()); // used to track unexpected exceptions and notify simulations
            next.addCallback(this::afterExecution);
            executor.execute(next);
        }

        private void afterExecution()
        {
            pending.poll();
            runNextTask();
        }

        @Override
        public void shutdown()
        {

        }

        @Override
        protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
        {
            return new DelayedSafeStore(this, ranges, context, commands, commandsForKeys, cacheLoading);
        }

        @Override
        public void unsafeRunIn(Runnable fn)
        {
            super.unsafeRunIn(fn);
        }
    }

    public static class DelayedSafeStore extends InMemoryCommandStore.InMemorySafeStore
    {
        private final DelayedCommandStore commandStore;
        private final CacheLoading cacheLoading;

        public DelayedSafeStore(DelayedCommandStore commandStore,
                                RangesForEpoch ranges,
                                PreLoadContext context,
                                Map<TxnId, InMemorySafeCommand> commands,
                                Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey,
                                CacheLoading cacheLoading)
        {
            super(commandStore, ranges, context, commands, commandsForKey);
            this.commandStore = commandStore;
            this.cacheLoading = cacheLoading;
        }

        @Override
        public void postExecute()
        {
            Journal.FieldUpdates fieldUpdates = fieldUpdates();
            if (fieldUpdates != null)
                commandStore.journal.saveStoreState(commandStore.id(), fieldUpdates, () -> {});

            commands.entrySet().forEach(e -> {
                InMemorySafeCommand safe = e.getValue();
                if (!safe.isModified()) return;

                Command before = safe.original();
                Command after = safe.current();
                commandStore.journal.saveCommand(commandStore.id(), new CommandUpdate(before, after), () -> {});
                commandStore.onRead(safe.current());
            });
            super.postExecute();
        }

        @Override
        protected InMemoryCommandStore.InMemoryCommandStoreCaches tryGetCaches()
        {
            if (!cacheLoading.cacheEmpty())
            {
                boolean cacheFull = cacheLoading.cacheFull();
                return commandStore.new InMemoryCommandStoreCaches() {
                    @Override
                    public InMemorySafeCommand acquireIfLoaded(TxnId txnId)
                    {
                        if (cacheFull || cacheLoading.isLoaded(txnId))
                            return super.acquireIfLoaded(txnId);
                        return null;
                    }

                    @Override
                    public InMemorySafeCommandsForKey acquireIfLoaded(RoutingKey key)
                    {
                        if (cacheFull || cacheLoading.isLoaded(key))
                            return super.acquireIfLoaded(key);
                        return null;
                    }
                };
            }
            return null;

        }
    }

    public List<DelayedCommandStore> unsafeStores()
    {
        List<DelayedCommandStore> stores = new ArrayList<>();
        for (ShardHolder holder : current().shards)
            stores.add((DelayedCommandStore) holder.store);
        return stores;
    }

    public interface CacheLoading
    {
        boolean cacheEmpty();
        boolean cacheFull();
        boolean isLoaded(TxnId txnId);
        boolean isLoaded(RoutingKey key);
        boolean tfkLoaded();
        void validate(CommandStore commandStore, Command command);
        void validate(CommandStore commandStore, CommandsForKey cfk);
    }
}
