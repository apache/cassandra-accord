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

import accord.api.TopologySorter;
import accord.local.Node;
import accord.topology.ShardSelection;
import accord.topology.Topologies;
import accord.topology.Topology;

// TODO (required): a variant that sorts by distance
public class SizeOfIntersectionSorter implements TopologySorter
{
    public static final TopologySorter.Supplier SUPPLIER = new Supplier() {
        @Override
        public TopologySorter get(Topology topology)
        {
            return new SizeOfIntersectionSorter(new Topologies.Single(this, topology));
        }

        @Override
        public TopologySorter get(Topologies topologies)
        {
            return new SizeOfIntersectionSorter(topologies);
        }
    };

    final Topologies topologies;
    SizeOfIntersectionSorter(Topologies topologies)
    {
        this.topologies = topologies;
    }

    @Override
    public int compare(Node.Id node1, Node.Id node2, ShardSelection shards)
    {
        int maxShardsPerEpoch = topologies.maxShardsPerEpoch();
        int count1 = 0, count2 = 0;
        for (int i = 0, mi = topologies.size() ; i < mi ; ++i)
        {
            Topology topology = topologies.get(i);
            count1 += count(node1, shards, i * maxShardsPerEpoch, topology);
            count2 += count(node2, shards, i * maxShardsPerEpoch, topology);
        }

        // sort more intersections later
        return count1 - count2;
    }

    private static int count(Node.Id node, ShardSelection shards, int offset, Topology topology)
    {
        return topology.foldlIntOn(node, (shard, v, index) -> shard.get(index) > 0 ? v + 1 : v, shards, offset, 0, 0);
    }
}
