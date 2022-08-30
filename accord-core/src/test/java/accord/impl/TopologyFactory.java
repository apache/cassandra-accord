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
import accord.api.Key;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.topology.Topology;

import java.util.*;

public class TopologyFactory
{
    public final int rf;
    // TODO: convert to KeyRanges
    final KeyRange[] ranges;

    public TopologyFactory(int rf, KeyRange... ranges)
    {
        this.rf = rf;
        this.ranges = ranges;
    }

    public Topology toTopology(Node.Id[] cluster)
    {
        return TopologyUtils.initialTopology(cluster, KeyRanges.ofSortedAndDeoverlapped(ranges), rf);
    }

    public Topology toTopology(List<Node.Id> cluster)
    {
        return toTopology(cluster.toArray(Node.Id[]::new));
    }

    public static Topology toTopology(List<Node.Id> cluster, int rf, KeyRange... ranges)
    {
        return new TopologyFactory(rf, ranges).toTopology(cluster);
    }
}
