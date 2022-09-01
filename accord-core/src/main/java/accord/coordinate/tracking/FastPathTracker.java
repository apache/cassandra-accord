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

import java.util.function.Function;
import java.util.function.IntFunction;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;

public class FastPathTracker<T extends FastPathTracker.FastPathShardTracker> extends AbstractQuorumTracker<T>
{
    public abstract static class FastPathShardTracker extends QuorumTracker.QuorumShardTracker
    {
        protected int fastPathAccepts = 0;

        public FastPathShardTracker(Shard shard)
        {
            super(shard);
        }

        public abstract boolean includeInFastPath(Node.Id node, boolean withFastPathTimestamp);

        public void onSuccess(Node.Id node, boolean withFastPathTimestamp)
        {
            if (oneSuccess(node) && includeInFastPath(node, withFastPathTimestamp))
                fastPathAccepts++;
        }

        public abstract boolean hasMetFastPathCriteria();
    }

    public FastPathTracker(Topologies topologies, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        super(topologies, arrayFactory, trackerFactory);
    }

    public void recordSuccess(Node.Id node, boolean withFastPathTimestamp)
    {
        forEachTrackerForNode(node, (tracker, n) -> tracker.onSuccess(n, withFastPathTimestamp));
    }

    public boolean hasMetFastPathCriteria()
    {
        return all(FastPathShardTracker::hasMetFastPathCriteria);
    }
}
