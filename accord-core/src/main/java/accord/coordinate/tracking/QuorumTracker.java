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

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.*;

public class QuorumTracker extends AbstractTracker<QuorumTracker.QuorumShardTracker> implements ResponseTracker
{
    public static class QuorumShardTracker extends ShardTracker
    {
        protected int successes;
        protected int failures;

        public QuorumShardTracker(Shard shard)
        {
            super(shard);
        }

        public ShardOutcomes onSuccess(Object ignore)
        {
            return ++successes == shard.slowPathQuorumSize ? Success : NoChange;
        }

        // return true iff hasFailed()
        public ShardOutcomes onFailure(Object ignore)
        {
            return ++failures > shard.maxFailures ? Fail : NoChange;
        }

        public boolean hasReachedQuorum()
        {
            return successes >= shard.slowPathQuorumSize;
        }

        boolean hasInFlight()
        {
            return successes + failures < shard.rf();
        }

        boolean hasFailures()
        {
            return failures > 0;
        }

        boolean hasFailed()
        {
            return failures > shard.maxFailures;
        }
    }

    public QuorumTracker(Topologies topologies)
    {
        super(topologies, QuorumShardTracker[]::new, QuorumShardTracker::new);
    }

    public RequestStatus recordSuccess(Node.Id node)
    {
        return recordResponse(this, node, QuorumShardTracker::onSuccess, null);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id node)
    {
        return recordResponse(this, node, QuorumShardTracker::onFailure, null);
    }

    public boolean hasFailures()
    {
        return any(QuorumShardTracker::hasFailures);
    }

    public boolean hasFailed()
    {
        return any(QuorumShardTracker::hasFailed);
    }

    public boolean hasInFlight()
    {
        return any(QuorumShardTracker::hasInFlight);
    }

    public boolean hasReachedQuorum()
    {
        return all(QuorumShardTracker::hasReachedQuorum);
    }
}
