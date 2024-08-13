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

package accord.burn;

import accord.api.TestableConfigurationService;
import accord.local.AgentExecutor;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.utils.MessageTask;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.*;
import java.util.function.Function;

public class TopologyUpdates
{
    private final Long2ObjectHashMap<Map<Node.Id, Ranges>> pendingTopologies = new Long2ObjectHashMap<>();

    Function<Node.Id, AgentExecutor> executors;
    public TopologyUpdates(Function<Node.Id, AgentExecutor> executors)
    {
        this.executors = executors;
    }

    public synchronized MessageTask notify(Node originator, Topology prev, Topology update)
    {
        Set<Node.Id> nodes = new TreeSet<>(prev.nodes());
        nodes.addAll(update.nodes());
        Map<Node.Id, Ranges> nodeToNewRanges = new HashMap<>();
        for (Node.Id node : nodes)
        {
            Ranges newRanges = update.rangesForNode(node).without(prev.rangesForNode(node));
            nodeToNewRanges.put(node, newRanges);
        }
        pendingTopologies.put(update.epoch(), nodeToNewRanges);
        return MessageTask.begin(originator, nodes, executors.apply(originator.id()), "TopologyNotify:" + update.epoch(), (node, from, onDone) -> {
            long nodeEpoch = node.epoch();
            if (nodeEpoch + 1 < update.epoch())
                onDone.accept(false);
            ((TestableConfigurationService) node.configService()).reportTopology(update);
            onDone.accept(true);
        });
    }

    public synchronized void syncComplete(Node originator, Collection<Node.Id> cluster, long epoch)
    {
        // topology is init topology
        if (pendingTopologies.isEmpty())
            return;
        Map<Node.Id, Ranges> pending = pendingTopologies.get(epoch);
        if (pending == null || null == pending.remove(originator.id()))
            throw new AssertionError();

        if (pending.isEmpty())
            pendingTopologies.remove(epoch);

        MessageTask.begin(originator, cluster, executors.apply(originator.id()), "SyncComplete:" + epoch, (node, from, onDone) -> {
            node.onRemoteSyncComplete(originator.id(), epoch);
            onDone.accept(true);
        });
    }

    public synchronized void epochClosed(Node originator, Collection<Node.Id> cluster, Ranges ranges, long epoch)
    {
        executors.apply(originator.id()).execute(() -> {
            MessageTask.begin(originator, cluster, executors.apply(originator.id()), "EpochClosed:" + epoch, (node, from, onDone) -> {
                node.onEpochClosed(ranges, epoch);
                onDone.accept(true);
            });
        });
    }

    public synchronized void epochRedundant(Node originator, Collection<Node.Id> cluster, Ranges ranges, long epoch)
    {
        executors.apply(originator.id()).execute(() -> {
            MessageTask.begin(originator, cluster, executors.apply(originator.id()), "EpochComplete:" + epoch, (node, from, onDone) -> {
                node.onEpochRedundant(ranges, epoch);
                onDone.accept(true);
            });
        });
    }

    public boolean isPending(Range range, Node.Id id)
    {
        return pendingTopologies.entrySet().stream().anyMatch(e -> {
            Ranges ranges = e.getValue().get(id);
            return ranges != null && ranges.intersects(range);
        });
    }

    public int pendingTopologies()
    {
        return pendingTopologies.size();
    }
}
