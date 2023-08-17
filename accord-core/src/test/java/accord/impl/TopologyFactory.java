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


import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.topology.Topology;
import accord.topology.TopologyUtils;

import java.util.*;

import static accord.utils.Utils.toArray;

public class TopologyFactory
{
    public final int rf;
    public final Range[] shardRanges;

    public TopologyFactory(int rf, Range... shardRanges)
    {
        this.rf = rf;
        this.shardRanges = shardRanges;
    }

    public Topology toTopology(Node.Id[] cluster)
    {
        return TopologyUtils.initialTopology(cluster, Ranges.ofSortedAndDeoverlapped(shardRanges), rf);
    }

    public Topology toTopology(List<Node.Id> cluster)
    {
        return toTopology(toArray(cluster, Node.Id[]::new));
    }

    public static Topology toTopology(List<Node.Id> cluster, int rf, Range... ranges)
    {
        return new TopologyFactory(rf, ranges).toTopology(cluster);
    }
}
