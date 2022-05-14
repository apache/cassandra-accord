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

package accord.coordinate.tracking;

import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.topology.Topologies;
import accord.topology.Topologies.Single;
import accord.topology.Topology;

public class QuorumTracker extends AbstractQuorumTracker<QuorumShardTracker>
{
    public QuorumTracker(Topologies topologies)
    {
        super(topologies, QuorumShardTracker[]::new, QuorumShardTracker::new);
    }

    public QuorumTracker(Topology topology)
    {
        super(new Single(topology, false), QuorumShardTracker[]::new, QuorumShardTracker::new);
    }

    // return true iff hasReachedQuorum()
    public boolean success(Node.Id node)
    {
        return allForNode(node, QuorumShardTracker::success) && hasReachedQuorum();
    }
}
