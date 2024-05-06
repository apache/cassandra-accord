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
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.BooleanSupplier;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStores;
import accord.impl.PrefixedIntHashKey;
import accord.impl.basic.TaskExecutorService.Task;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.CommonAttributes;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.SerializerSupport;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.Txn;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

public class DelayedCommandStores extends InMemoryCommandStores.SingleThread
{
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
        protected void validateRead(Command current)
        {
            // "loading" the command doesn't make sense as we don't "store" the command...
            if (current.txnId().kind() == Txn.Kind.EphemeralRead)
                return;
            //TODO (correctness): these type of txn must be durable but currently they are not... should make sure this is plugged into the C* journal properly for reply
            if (current.txnId().kind() == Txn.Kind.LocalOnly)
                return;
            Command.WaitingOn waitingOn = null;
            if (current.isStable() && !current.isTruncated())
                waitingOn = current.asCommitted().waitingOn;
            SerializerSupport.MessageProvider messages = journal.makeMessageProvider(current.txnId());
            Command.WaitingOn finalWaitingOn = waitingOn;
            CommonAttributes.Mutable mutable = current.mutable();
            mutable.partialDeps(null).removePartialTxn();
            Command reconstructed;
            try
            {
                reconstructed = SerializerSupport.reconstruct(unsafeRangesForEpoch(), mutable, current.saveStatus(), current.executeAt(), current.txnId().kind().awaitsOnlyDeps() ? current.executesAtLeast() : null, current.promised(), current.acceptedOrCommitted(), ignore -> finalWaitingOn, messages);
            }
            catch (IllegalStateException t)
            {
                //TODO (correctness): journal doesn’t guarantee we pick the same records we used to state transition
                // Journal stores a list of messages it saw in some order it defines, but when reconstructing a command we don't actually know what messages were used, this could
                // lead to a case where deps mismatch, so ignoring this for now
                if (t.getMessage() != null && t.getMessage().startsWith("Deps do not match; expected"))
                    return;
                // here for debugging
                SerializerSupport.reconstruct(unsafeRangesForEpoch(), mutable, current.saveStatus(), current.executeAt(), current.txnId().kind().awaitsOnlyDeps() ? current.executesAtLeast() : null, current.promised(), current.acceptedOrCommitted(), ignore -> finalWaitingOn, messages);
                throw t;
            }
            //TODO (correctness): journal doesn’t guarantee we pick the same records we used to state transition
            if (current.partialDeps() != null && !current.partialDeps().rangeDeps.equals(reconstructed.partialDeps().rangeDeps))
                return;
            // for some reasons scope doesn't alaways match, this might be due to journal... what sucks is that this can also be a bug in the extract, so its
            // hard to figure out what happened.
            if (current.partialDeps() != null && !current.partialDeps().equals(reconstructed.partialDeps()))
                return;
            if (current.isCommitted() && !current.isTruncated() && !Objects.equals(current.asCommitted().waitingOn(), reconstructed.asCommitted().waitingOn()))
                return;
//            Invariants.checkState(current.equals(reconstructed), "Commands did not match: expected %s, given %s", current, reconstructed);
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
            if (Invariants.isParanoid())
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
    }
}
