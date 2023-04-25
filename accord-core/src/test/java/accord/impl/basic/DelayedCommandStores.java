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
import java.util.concurrent.TimeUnit;
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
               new DelayedCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, new SimulatedDelayedExecutorService(pending, random));
    }

    private static class Pending
    {
        final Task<?> task;
        final long nowMillis;

        private Pending(Task<?> task, long nowMillis)
        {
            this.task = task;
            this.nowMillis = nowMillis;
        }
    }

    public static class DelayedCommandStore extends InMemoryCommandStore
    {
        private final SimulatedDelayedExecutorService executor;
        private final Queue<Pending> pending = new LinkedList<>();
        private Task<?> scheduled = null;

        public DelayedCommandStore(int id, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, RangesForEpochHolder rangesForEpochHolder, SimulatedDelayedExecutorService executor)
        {
            super(id, time, agent, store, progressLogFactory, rangesForEpochHolder);
            this.executor = executor;
        }

        private static CommandStore.Factory factory(SimulatedDelayedExecutorService executor)
        {
            return (id, time, agent, store, progressLogFactory, rangesForEpoch) -> new DelayedCommandStore(id, time, agent, store, progressLogFactory, rangesForEpoch, executor);
        }

        @Override
        public boolean inStore()
        {
            return CommandStore.Unsafe.maybeCurrent() == this;
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
            Task<T> task = new Task<>(() -> Unsafe.runWith(this, fn));
            task.addCallback(agent()); // used to track unexpected exceptions and notify simulations
            // add continuation
            task.addCallback(() -> {
                scheduled = null;
                Pending next = pending.poll();
                if (next == null)
                    return;
                scheduled = next.task;
                executor.executeWithObservedDelay(next.task, executor.nowMillis() - next.nowMillis, TimeUnit.MILLISECONDS);
            });

            if (scheduled == null)
            {
                scheduled = task;
                executor.execute(task);
            }
            else
            {
                // SimulatedDelayedExecutorService can interleave tasks and is global; this violates a requirement for
                // CommandStore; single threaded with ordered execution!  To simulate this behavior, add the task to the
                // FIFO queue
                pending.add(new Pending(task, executor.nowMillis()));
            }
            return task;
        }

        @Override
        public void shutdown()
        {

        }
    }
}
