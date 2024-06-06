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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStores;
import accord.impl.InMemorySafeCommand;
import accord.impl.InMemorySafeCommandsForKey;
import accord.impl.InMemorySafeTimestampsForKey;
import accord.impl.PrefixedIntHashKey;
import accord.impl.basic.TaskExecutorService.Task;
import accord.impl.mock.MockStore;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
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

public class DelayedCommandStores extends InMemoryCommandStores.SingleThread
{
    //TODO (correctness): remove once we have a Journal integration that does not change the values of a command...
    // There are known issues due to Journal, so exclude known problamtic fields
    private static class ValidationHack
    {
        private static Pattern field(String path)
        {
            path = path.replace(".", "\\.");
            path += ".*";
            return Pattern.compile(path);
        }

        private static final List<Pattern> KNOWN_ISSUES = List.of(
                                                                  // when a new epoch is detected and the execute ranges have more than the coordinating ranges,
                                                                  // and the coordinating ranges doesn't include the home key... we drop the query...
                                                                  // The logic to stitch messages together is not able to handle this as it doesn't know the original Topologies
                                                                  // TODO (required): test int if this is still true
//                                                                  field(".partialTxn.query."),
                                                                  // cmd.mutable().build() != cmd.  This is due to Command.durability changing NotDurable to Local depending on the status
                                                                  field(".durability."));
    }

    private static boolean hasKnownIssue(String path)
    {
        for (Pattern p : ValidationHack.KNOWN_ISSUES)
        {
            Matcher m = p.matcher(path);
            if (m.find())
                return true;
        }
        return false;
    }

    private DelayedCommandStores(NodeTimeService time, Agent agent, DataStore store, RandomSource random, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, SimulatedDelayedExecutorService executorService, BooleanSupplier isLoadedCheck, Journal journal)
    {
        super(time, agent, store, random, shardDistributor, progressLogFactory, DelayedCommandStore.factory(executorService, isLoadedCheck, journal));
    }

    public static CommandStores.Factory factory(PendingQueue pending, BooleanSupplier isLoadedCheck, Journal journal)
    {
        return (time, agent, store, random, shardDistributor, progressLogFactory) ->
               new DelayedCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, new SimulatedDelayedExecutorService(pending, agent), isLoadedCheck, journal);
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
        private class DelayedTask<T> extends Task<T>
        {
            private DelayedTask(Callable<T> fn)
            {
                super(fn);
            }

            @Override
            public void run()
            {
                unsafeRunIn(super::run);
            }
        }

        private final SimulatedDelayedExecutorService executor;
        private final Queue<Task<?>> pending = new LinkedList<>();
        private final BooleanSupplier isLoadedCheck;
        private final Journal journal;

        public DelayedCommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, EpochUpdateHolder epochUpdateHolder, SimulatedDelayedExecutorService executor, BooleanSupplier isLoadedCheck, Journal journal)
        {
            super(id, time, agent, store, progressLogFactory, epochUpdateHolder);
            this.executor = executor;
            this.isLoadedCheck = isLoadedCheck;
            this.journal = journal;
        }

        @Override
        public void validateRead(Command current)
        {
            if (Invariants.paranoia() < 3)
                return;

            // "loading" the command doesn't make sense as we don't "store" the command...
            if (current.txnId().kind() == Txn.Kind.EphemeralRead)
                return;

            Result result = current.result();
            if (result == null)
                result = MockStore.RESULT;
            // Journal will not have result persisted. This part is here for test purposes and ensuring that we have strict object equality.
            Command reconstructed = journal.reconstruct(id, current.txnId(), result);
            List<Difference<?>> diff = ReflectionUtils.recursiveEquals(current, reconstructed);
            List<String> filteredDiff = diff.stream().filter(d -> !DelayedCommandStores.hasKnownIssue(d.path)).map(Object::toString).collect(Collectors.toList());
            Invariants.checkState(filteredDiff.isEmpty(), "Commands did not match: expected %s, given %s, node %s, store %d, diff %s", current, reconstructed, time, id(), new LazyToString(() -> String.join("\n", filteredDiff)));
        }

        @Override
        protected boolean canExposeUnloaded()
        {
            return isLoadedCheck.getAsBoolean();
        }

        private static CommandStore.Factory factory(SimulatedDelayedExecutorService executor, BooleanSupplier isLoadedCheck, Journal journal)
        {
            return (id, time, agent, store, progressLogFactory, rangesForEpoch) -> new DelayedCommandStore(id, time, agent, store, progressLogFactory, rangesForEpoch, executor, isLoadedCheck, journal);
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
            if (Invariants.paranoia() >= 3)
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
                    protected void start(BiConsumer<? super T, Throwable> callback)
                    {
                        boolean wasEmpty = pending.isEmpty();
                        pending.add(task);
                        if (wasEmpty)
                            runNextTask();
                        task.begin(callback);
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
            return new DelayedSafeStore(this, ranges, context, commands, timestampsForKey, commandsForKeys);
        }
    }

    public static class DelayedSafeStore extends InMemoryCommandStore.InMemorySafeStore
    {
        private final DelayedCommandStore commandStore;
        public DelayedSafeStore(DelayedCommandStore commandStore, RangesForEpoch ranges, PreLoadContext context, Map<TxnId, InMemorySafeCommand> commands, Map<RoutableKey, InMemorySafeTimestampsForKey> timestampsForKey, Map<RoutableKey, InMemorySafeCommandsForKey> commandsForKey)
        {
            super(commandStore, ranges, context, commands, timestampsForKey, commandsForKey);
            this.commandStore = commandStore;
        }

        @Override
        public void postExecute()
        {
            super.postExecute();
            commands.entrySet().forEach(e -> {
                InMemorySafeCommand safe = e.getValue();
                if (!safe.isModified()) return;

                Command before = safe.original();
                Command after = safe.current();
                commandStore.journal.onExecute(commandStore.id(), before, after, context.primaryTxnId().equals(after.txnId()));
                commandStore.validateRead(safe.current());
            });
        }
    }
}
