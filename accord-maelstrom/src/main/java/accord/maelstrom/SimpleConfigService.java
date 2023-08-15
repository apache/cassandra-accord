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

package accord.maelstrom;

import accord.api.ConfigurationService;
import accord.primitives.Ranges;
import accord.topology.Topology;

public class SimpleConfigService implements ConfigurationService
{
    private final Topology topology;

    public SimpleConfigService(Topology topology)
    {
        this.topology = topology;
    }

    @Override
    public void registerListener(Listener listener)
    {

    }

    @Override
    public Topology currentTopology()
    {
        return topology;
    }

    @Override
    public Topology getTopologyForEpoch(long epoch)
    {
        assert epoch == topology.epoch();
        return topology;
    }

    @Override
    public void fetchTopologyForEpoch(long epoch)
    {
    }

    @Override
    public void acknowledgeEpoch(EpochReady ready, boolean startSync)
    {
    }

    @Override
    public void reportEpochClosed(Ranges ranges, long epoch)
    {
    }

    @Override
    public void reportEpochRedundant(Ranges ranges, long epoch)
    {
    }
}

