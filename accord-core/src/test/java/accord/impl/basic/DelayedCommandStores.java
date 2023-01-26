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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStores;
import accord.local.CommandStores;
import accord.local.NodeTimeService;
import accord.local.ShardDistributor;

import java.util.Random;

public class DelayedCommandStores extends InMemoryCommandStores.SingleThread
{
    private DelayedCommandStores(NodeTimeService time, Agent agent, DataStore store, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, SimulatedDelayedExecutorService executorService)
    {
        super(time, agent, store, shardDistributor, progressLogFactory, InMemoryCommandStore.SingleThread.factory(executorService));
    }

    public static CommandStores.Factory factory(PendingQueue pending, Random random)
    {
        SimulatedDelayedExecutorService executorService = new SimulatedDelayedExecutorService(pending, random);
        return (time, agent, store, shardDistributor, progressLogFactory) -> new DelayedCommandStores(time, agent, store, shardDistributor, progressLogFactory, executorService);
    }
}
