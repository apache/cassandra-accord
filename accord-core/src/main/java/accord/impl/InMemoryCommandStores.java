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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.primitives.Keys;

import java.util.function.Consumer;

import static java.lang.Boolean.FALSE;

public abstract class InMemoryCommandStores extends CommandStores
{
    public InMemoryCommandStores(int num, Node node, Agent agent, DataStore store,
                                 ProgressLog.Factory progressLogFactory)
    {
        super(num, node, agent, store, progressLogFactory);
    }

    public static InMemoryCommandStores inMemory(Node node)
    {
        return (InMemoryCommandStores) node.commandStores();
    }

    public void forEachLocal(Consumer<? super CommandStore> forEach)
    {
        foldl((ranges, o, minEpoch, maxEpoch) -> ranges.all(),
              null, Long.MIN_VALUE, Long.MAX_VALUE,
              (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
    }

    public void forEachLocal(Keys keys, long minEpoch, long maxEpoch, Consumer<? super CommandStore> forEach)
    {
        foldl(ShardedRanges::shards, keys, minEpoch, maxEpoch, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
    }

    public void forEachLocal(Keys keys, long epoch, Consumer<? super CommandStore> forEach)
    {
        forEachLocal(keys, epoch, epoch, forEach);
    }

    public void forEachLocalSince(Keys keys, long epoch, Consumer<? super CommandStore> forEach)
    {
        forEachLocal(keys, epoch, Long.MAX_VALUE, forEach);
    }

    public static class Synchronized extends InMemoryCommandStores
    {
        public Synchronized(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory);
        }

        @Override
        protected CommandStore createCommandStore(int generation, int index, int numShards, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.RangesForEpoch rangesForEpoch)
        {
            return new InMemoryCommandStore.Synchronized(generation, index, numShards, node::uniqueNow, node.topology()::epoch, agent, store, progressLogFactory, rangesForEpoch);
        }
    }

    public static class SingleThread extends InMemoryCommandStores
    {
        public SingleThread(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory);
        }

        @Override
        protected CommandStore createCommandStore(int generation, int index, int numShards, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.RangesForEpoch rangesForEpoch)
        {
            return new InMemoryCommandStore.SingleThread(generation, index, numShards, node.id(), node::uniqueNow, node.topology()::epoch, agent, store, progressLogFactory, rangesForEpoch);
        }
    }

    public static class Debug extends InMemoryCommandStores.SingleThread
    {
        public Debug(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory);
        }

        @Override
        protected CommandStore createCommandStore(int generation, int index, int numShards, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.RangesForEpoch rangesForEpoch)
        {
            return new InMemoryCommandStore.SingleThreadDebug(generation, index, numShards, node.id(), node::uniqueNow, node.topology()::epoch, agent, store, progressLogFactory, rangesForEpoch);
        }
    }

}
