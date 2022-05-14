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

import accord.api.*;
import accord.local.*;
import accord.primitives.AbstractKeys;
import accord.utils.MapReduce;

import java.util.function.BiFunction;
import java.util.function.Function;

public class InMemoryCommandStores
{
    public static class Synchronized extends SyncCommandStores
    {
        public Synchronized(int num, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, agent, store, progressLogFactory, InMemoryCommandStore.Synchronized::new);
        }

        public <T> T mapReduce(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, MapReduce<? super SafeCommandStore, T> map)
        {
            return super.mapReduce(context, keys, minEpoch, maxEpoch, map, SyncMapReduceAdapter.instance());
        }

        public <T> T mapReduce(PreLoadContext context, AbstractKeys<?, ?> keys, long minEpoch, long maxEpoch, Function<? super SafeCommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return mapReduce(context, keys, minEpoch, maxEpoch, new MapReduce<SafeCommandStore, T>() {
                @Override
                public T apply(SafeCommandStore in)
                {
                    return map.apply(in);
                }

                @Override
                public T reduce(T o1, T o2)
                {
                    return reduce.apply(o1, o2);
                }
            });
        }
    }

    public static class SingleThread extends AsyncCommandStores
    {
        public SingleThread(int num, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, time, agent, store, progressLogFactory, InMemoryCommandStore.SingleThread::new);
        }

        public SingleThread(int num, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
        {
            super(num, time, agent, store, progressLogFactory, shardFactory);
        }
    }

    public static class Debug extends InMemoryCommandStores.SingleThread
    {
        public Debug(int num, NodeTimeService time, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, time, agent, store, progressLogFactory, InMemoryCommandStore.Debug::new);
        }
    }
}
