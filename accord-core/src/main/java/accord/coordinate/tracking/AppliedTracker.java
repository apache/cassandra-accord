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

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;

public class AppliedTracker extends AbstractTracker<AppliedTracker.AppliedShardTracker> implements ResponseTracker
{
    public static class AppliedShardTracker extends ShardTracker
    {
        protected int waitingOn;

        public AppliedShardTracker(Shard shard)
        {
            super(shard);
            waitingOn = shard.rf();
        }

        public ShardOutcomes onSuccess(Object ignore)
        {
            return --waitingOn == 0 ? Success : NoChange;
        }

        // return true iff hasFailed()
        public ShardOutcomes onFailure(Object ignore)
        {
            if (waitingOn < 0)
                return NoChange;
            waitingOn = -1;
            return Fail;
        }

        public boolean hasReachedQuorum()
        {
            return waitingOn == 0;
        }

        boolean hasInFlight()
        {
            return waitingOn > 0;
        }

        boolean hasFailed()
        {
            return waitingOn < 0;
        }
    }

    public AppliedTracker(Topologies topologies)
    {
        super(topologies, AppliedShardTracker[]::new, shard -> new AppliedShardTracker(shard));
    }

    public RequestStatus recordSuccess(Node.Id node)
    {
        return recordResponse(this, node, AppliedShardTracker::onSuccess, null);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id node)
    {
        return recordResponse(this, node, AppliedShardTracker::onFailure, null);
    }

    public boolean hasFailed()
    {
        return any(AppliedShardTracker::hasFailed);
    }

    public boolean hasInFlight()
    {
        return any(AppliedShardTracker::hasInFlight);
    }

    public boolean hasReachedQuorum()
    {
        return all(AppliedShardTracker::hasReachedQuorum);
    }
}
