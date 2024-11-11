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
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.Iterables;

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
import accord.impl.InMemorySafeTimestampsForKey;
import accord.impl.PrefixedIntHashKey;
import accord.impl.basic.TaskExecutorService.Task;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommandStore;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.LazyToString;
import accord.utils.RandomSource;
import accord.utils.ReflectionUtils;
import accord.utils.ReflectionUtils.Difference;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.Cancellable;

import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.HIGH;

public class DelayedCommandStores extends InMemoryCommandStores.SingleThread
{
    private DelayedCommandStores(NodeCommandStoreService time, Agent agent, DataStore store, RandomSource random, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, SimulatedDelayedExecutorService executorService, CacheLoadingChance isLoadedCheck, Journal journal)
    {
        super(time, agent, store, random, shardDistributor, progressLogFactory, listenersFactory, DelayedCommandStore.factory(executorService, isLoadedCheck, journal));
    }

    public static CommandStores.Factory factory(PendingQueue pending, CacheLoadingChance isLoadedCheck, Journal journal)
    {
        return (time, agent, store, random, shardDistributor, progressLogFactory, listenersFactory) ->
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
        private final CacheLoadingChance cacheLoadingChance;
        private final Journal journal;

        public DelayedCommandStore(int id, NodeCommandStoreService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory, EpochUpdateHolder epochUpdateHolder, SimulatedDelayedExecutorService executor, CacheLoadingChance cacheLoadingChance, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, listenersFactory, epochUpdateHolder);
            this.executor = executor;
            this.cacheLoadingChance = cacheLoadingChance;
            this.journal = journal;
        }

        @Override
        public void validateRead(Command current)
        {
            if (!Invariants.testParanoia(LINEAR, LINEAR, HIGH))
                return;

            // "loading" the command doesn't make sense as we don't "store" the command...
            if (current.txnId().kind() == Txn.Kind.EphemeralRead)
                return;

            // Journal will not have result persisted. This part is here for test purposes and ensuring that we have strict object equality.
            // TODO: redundant/durable before
            Command reconstructed = journal.loadCommand(id, current.txnId(), RedundantBefore.EMPTY, DurableBefore.EMPTY);
            List<Difference<?>> diff = ReflectionUtils.recursiveEquals(current, reconstructed);
            Invariants.checkState(diff.isEmpty(), "Commands did not match: expected %s, given %s, node %s, store %d, diff %s", current, reconstructed, node, id(), new LazyToString(() -> String.join("\n", Iterables.transform(diff, Object::toString))));
        }

        @Override
        protected boolean canExposeUnloaded()
        {
            return !cacheLoadingChance.cacheEmpty();
        }

        private static CommandStore.Factory factory(SimulatedDelayedExecutorService executor, CacheLoadingChance isLoadedCheck, Journal journal)
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
        protected InMemorySafeStore createSafeStore(PreLoadContext context, RangesForEpoch ranges, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKeys)
        {
            return new DelayedSafeStore(this, ranges, context, commands, timestampsForKey, commandsForKeys, cacheLoadingChance);
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
        private final CacheLoadingChance cacheLoadingChance;
        public DelayedSafeStore(DelayedCommandStore commandStore,
                                RangesForEpoch ranges,
                                PreLoadContext context,
                                Map<TxnId, InMemorySafeCommand> commands,
                                Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey,
                                Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey,
                                CacheLoadingChance cacheLoadingChance)
        {
            super(commandStore, ranges, context, commands, timestampsForKey, commandsForKey);
            this.commandStore = commandStore;
            this.cacheLoadingChance = cacheLoadingChance;
        }

        @Override
        public void postExecute()
        {
            commands.entrySet().forEach(e -> {
                InMemorySafeCommand safe = e.getValue();
                if (!safe.isModified()) return;

                Command before = safe.original();
                Command after = safe.current();
                // TODO: do we want to use AccordCommandStore, since it handles caches?
                commandStore.journal.saveCommand(commandStore.id(), new Journal.CommandUpdate(before, after), () -> {});
                commandStore.validateRead(safe.current());
            });
            super.postExecute();
        }

        @Override
        protected InMemoryCommandStore.InMemoryCommandStoreCaches tryGetCaches()
        {
            if (!cacheLoadingChance.cacheEmpty())
            {
                boolean cacheFull = cacheLoadingChance.cacheFull();
                return commandStore.new InMemoryCommandStoreCaches() {
                    @Override
                    public InMemorySafeCommand acquireIfLoaded(TxnId txnId)
                    {
                        if (cacheFull || cacheLoadingChance.commandLoaded())
                            return super.acquireIfLoaded(txnId);
                        return null;
                    }

                    @Override
                    public InMemorySafeTimestampsForKey acquireTfkIfLoaded(RoutingKey key)
                    {
                        if (cacheFull || cacheLoadingChance.tfkLoaded())
                            return super.acquireTfkIfLoaded(key);
                        return null;
                    }

                    @Override
                    public InMemorySafeCommandsForKey acquireIfLoaded(RoutingKey key)
                    {
                        if (cacheFull || cacheLoadingChance.cfkLoaded())
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

    public interface CacheLoadingChance
    {
        boolean cacheEmpty();
        boolean cacheFull();
        boolean commandLoaded();
        boolean cfkLoaded();
        boolean tfkLoaded();
    }
}
