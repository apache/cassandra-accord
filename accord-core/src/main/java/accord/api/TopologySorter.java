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

package accord.api;

import accord.local.Node;
import accord.topology.ShardSelection;
import accord.topology.Topologies;
import accord.topology.Topology;

// TODO (desired): introduce abstract sorter that is aware of bootstrap time and deprioritises replicas that may not
//  have the transaction data, or may not be able to serve the read
public interface TopologySorter
{
    enum NodeStatus
    {
        UNAVAILABLE, UNABLE_TO_CALCULATE_DEPS, UNREADABLE,
        /* Some ranges may be unable to participate in some way */
        DEGRADED,
        HEALTHY;

        public final boolean isAtLeast(NodeStatus greaterThan)
        {
            return this.compareTo(greaterThan) >= 0;
        }

        public final boolean isHealthy()
        {
            return this == HEALTHY;
        }

        public final boolean isFaulty()
        {
            return this != HEALTHY;
        }

        public final boolean isUnreadable()
        {
            return compareTo(UNREADABLE) <= 0;
        }

        public final boolean isUnableToCalculateDeps()
        {
            return compareTo(UNABLE_TO_CALCULATE_DEPS) <= 0;
        }

        public final boolean isUnavailable()
        {
            return this == UNAVAILABLE;
        }
    }

    interface Supplier
    {
        TopologySorter get(Topology topologies);
        TopologySorter get(Topologies topologies);
    }

    interface StaticSorter extends Supplier, TopologySorter
    {
        @Override
        default TopologySorter get(Topology topologies) { return this; }
        @Override
        default TopologySorter get(Topologies topologies) { return this; }

        @Override
        default NodeStatus status(Node.Id node) { return NodeStatus.HEALTHY; }
    }

    /**
     * Compare two nodes that occur in some topologies, so that the one that is *least* preferable sorts first
     */
    int compare(Node.Id node1, Node.Id node2, ShardSelection shards);

    NodeStatus status(Node.Id node);
}
