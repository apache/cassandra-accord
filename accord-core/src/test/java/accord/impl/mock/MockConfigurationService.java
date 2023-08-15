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

package accord.impl.mock;

import accord.api.MessageSink;
import accord.api.TestableConfigurationService;
import accord.local.Node;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.EpochFunction;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import org.junit.jupiter.api.Assertions;

import java.util.*;

public class MockConfigurationService implements TestableConfigurationService
{
    private final MessageSink messageSink;
    private final List<Topology> epochs = new ArrayList<>();
    private final Map<Long, EpochReady> acks = new HashMap<>();
    private final List<AsyncResult<Void>> syncs = new ArrayList<>();
    private final List<Listener> listeners = new ArrayList<>();
    private final EpochFunction<MockConfigurationService> fetchTopologyHandler;

    public MockConfigurationService(MessageSink messageSink, EpochFunction<MockConfigurationService> fetchTopologyHandler)
    {
        this.messageSink = messageSink;
        this.fetchTopologyHandler = fetchTopologyHandler;
        epochs.add(Topology.EMPTY);
    }

    public MockConfigurationService(MessageSink messageSink, EpochFunction<MockConfigurationService> fetchTopologyHandler, Topology initialTopology)
    {
        this(messageSink, fetchTopologyHandler);
        reportTopology(initialTopology);
    }

    @Override
    public synchronized void registerListener(Listener listener)
    {
        listeners.add(listener);
    }

    @Override
    public synchronized Topology currentTopology()
    {
        return epochs.get(epochs.size() - 1);
    }

    @Override
    public synchronized Topology getTopologyForEpoch(long epoch)
    {
        return epoch >= epochs.size() ? null : epochs.get((int) epoch);
    }

    @Override
    public synchronized void fetchTopologyForEpoch(long epoch)
    {
        if (epoch < epochs.size())
            return;

        fetchTopologyHandler.apply(epoch, this);
        return;
    }

    @Override
    public synchronized void acknowledgeEpoch(EpochReady epoch, boolean startSync)
    {
        Assertions.assertFalse(acks.containsKey(epoch.epoch));
        acks.put(epoch.epoch, epoch);
    }

    public synchronized EpochReady ackFor(long epoch)
    {
        return acks.get(epoch);
    }

    @Override
    public void reportEpochClosed(Ranges ranges, long epoch)
    {
    }

    @Override
    public void reportEpochRedundant(Ranges ranges, long epoch)
    {
    }

    @Override
    public synchronized void reportTopology(Topology topology)
    {
        if (topology.epoch() > epochs.size())
            return;

        Assertions.assertEquals(topology.epoch(), epochs.size());
        epochs.add(topology);

        List<AsyncResult<Void>> futures = new ArrayList<>();
        for (Listener listener : listeners)
            futures.add(listener.onTopologyUpdate(topology, true));

        AsyncResult<Void> result = futures.isEmpty()
           ? AsyncResults.success(null)
           : AsyncChains.reduce(futures, (a, b) -> null).beginAsResult();

        syncs.add(result);
    }

    public synchronized void reportSyncComplete(Node.Id node, long epoch)
    {
        for (Listener listener : listeners)
            listener.onRemoteSyncComplete(node, epoch);
    }
}
