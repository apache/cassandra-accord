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

import accord.api.*;
import accord.api.ProgressLog;
import accord.api.DataStore;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncExecutor;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public interface CommandStore extends AsyncExecutor
{
    interface Factory
    {
        CommandStore create(int id,
                            NodeTimeService time,
                            Agent agent,
                            DataStore store,
                            ProgressLog.Factory progressLogFactory,
                            RangesForEpochHolder rangesForEpoch);
    }

    int id();
    Agent agent();
    AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer);
    <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply);

    @Override
    default void execute(Runnable command)
    {
        submit(command).begin(agent());
    }

    void shutdown();
}
