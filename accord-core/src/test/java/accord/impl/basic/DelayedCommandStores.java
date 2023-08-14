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
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStores;
import accord.impl.basic.TaskExecutorService.Task;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.local.ShardDistributor;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChain;

public class DelayedCommandStores extends InMemoryCommandStores.SingleThread
{
    private DelayedCommandStores(NodeTimeService time, Agent agent, DataStore store, RandomSource random, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, SimulatedDelayedExecutorService executorService)
    {
        super(time, agent, store, random, shardDistributor, progressLogFactory, DelayedCommandStore.factory(executorService));
    }

    public static CommandStores.Factory factory(PendingQueue pending)
    {
        return (time, agent, store, random, shardDistributor, progressLogFactory) ->
               new DelayedCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, new SimulatedDelayedExecutorService(pending, agent));
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

        public DelayedCommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, EpochUpdateHolder epochUpdateHolder, SimulatedDelayedExecutorService executor)
        {
            super(id, time, agent, store, progressLogFactory, epochUpdateHolder);
            this.executor = executor;
        }

        private static CommandStore.Factory factory(SimulatedDelayedExecutorService executor)
        {
            return (id, time, agent, store, progressLogFactory, rangesForEpoch) -> new DelayedCommandStore(id, time, agent, store, progressLogFactory, rangesForEpoch, executor);
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
            boolean wasEmpty = pending.isEmpty();
            pending.add(task);
            if (wasEmpty)
                runNextTask();
            return task;
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
