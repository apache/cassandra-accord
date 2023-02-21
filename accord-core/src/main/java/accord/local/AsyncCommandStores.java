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

package accord.local;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.primitives.Routables;
import accord.utils.MapReduce;
import accord.utils.MapReduceConsume;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class AsyncCommandStores extends CommandStores<CommandStore>
{
    static class AsyncMapReduceAdapter<O> implements MapReduceAdapter<CommandStore, AsyncChain<O>, List<AsyncChain<O>>, O>
    {
        private static final AsyncChain<?> SUCCESS = AsyncChains.success(null);
        private static final AsyncMapReduceAdapter INSTANCE = new AsyncMapReduceAdapter<>();
        public static <O> AsyncMapReduceAdapter<O> instance() { return INSTANCE; }

        @Override
        public List<AsyncChain<O>> allocate()
        {
            return new ArrayList<>();
        }

        @Override
        public AsyncChain<O> apply(MapReduce<? super SafeCommandStore, O> map, CommandStore commandStore, PreLoadContext context)
        {
            return commandStore.submit(context, map);
        }

        @Override
        public List<AsyncChain<O>> reduce(MapReduce<? super SafeCommandStore, O> reduce, List<AsyncChain<O>> chains, AsyncChain<O> next)
        {
            chains.add(next);
            return chains;
        }

        @Override
        public void consume(MapReduceConsume<?, O> reduceAndConsume, AsyncChain<O> chain)
        {
            chain.begin(reduceAndConsume);
        }

        @Override
        public AsyncChain<O> reduce(MapReduce<?, O> reduce, List<AsyncChain<O>> futures)
        {
            if (futures.isEmpty())
                return (AsyncChain<O>) SUCCESS;
            return AsyncChains.reduce(futures, reduce::reduce);
        }
    }

    public AsyncCommandStores(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        super(time, agent, store, shardDistributor, progressLogFactory, shardFactory);
    }

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, Routables<?, ?> keys, long minEpoch, long maxEpoch, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        mapReduceConsume(context, keys, minEpoch, maxEpoch, mapReduceConsume, AsyncMapReduceAdapter.INSTANCE);
    }

    @Override
    public <O> void mapReduceConsume(PreLoadContext context, IntStream commandStoreIds, MapReduceConsume<? super SafeCommandStore, O> mapReduceConsume)
    {
        mapReduceConsume(context, commandStoreIds, mapReduceConsume, AsyncMapReduceAdapter.INSTANCE);
    }
}
