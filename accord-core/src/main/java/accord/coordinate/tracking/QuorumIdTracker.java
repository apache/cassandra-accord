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

import java.util.Set;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;
import org.agrona.collections.ObjectHashSet;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;

public class QuorumIdTracker extends SimpleTracker<QuorumIdTracker.QuorumIdShardTracker> implements ResponseTracker
{
    public static class QuorumIdShardTracker extends ShardTracker
    {
        protected final Set<Node.Id> successes = new ObjectHashSet<>();
        protected Set<Node.Id> failures;

        public QuorumIdShardTracker(Shard shard)
        {
            super(shard);
        }

        public ShardOutcomes onSuccess(Node.Id from)
        {
            return successes.add(from) && successes.size() == shard.slowPathQuorumSize ? Success : NoChange;
        }

        // return true iff hasFailed()
        public ShardOutcomes onFailure(Node.Id from)
        {
            if (failures == null)
                failures = new ObjectHashSet<>();
            return failures.add(from) && failures.size() == 1 + shard.maxFailures ? Fail : NoChange;
        }

        public boolean hasReachedQuorum()
        {
            return successes.size() >= shard.slowPathQuorumSize;
        }

        boolean hasInFlight()
        {
            return successes.size() + (failures == null ? 0 : failures.size()) < shard.rf();
        }

        boolean hasFailures()
        {
            return failures != null;
        }

        boolean hasFailed()
        {
            return failures != null && failures.size() > shard.maxFailures;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "{successes: "+successes+"," +
                   "failures: "+failures+"," +
                   "quorum?: "+hasReachedQuorum()+"," +
                   "shard:"+shard+"}";
        }
    }

    public QuorumIdTracker(Topologies topologies)
    {
        super(topologies, QuorumIdShardTracker[]::new, QuorumIdShardTracker::new);
    }

    public RequestStatus recordSuccess(Node.Id node)
    {
        return recordResponse(this, node, QuorumIdShardTracker::onSuccess, node);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id node)
    {
        return recordResponse(this, node, QuorumIdShardTracker::onFailure, node);
    }

    public boolean hasFailures()
    {
        return any(QuorumIdShardTracker::hasFailures);
    }

    public boolean hasFailed()
    {
        return any(QuorumIdShardTracker::hasFailed);
    }

    public boolean hasInFlight()
    {
        return any(QuorumIdShardTracker::hasInFlight);
    }

    public boolean hasReachedQuorum()
    {
        return all(QuorumIdShardTracker::hasReachedQuorum);
    }
}
