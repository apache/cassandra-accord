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

import accord.coordinate.tracking.QuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.primitives.DataConsistencyLevel;
import accord.topology.Shard;
import accord.topology.Topologies;

import static accord.primitives.DataConsistencyLevel.INVALID;
import static accord.utils.Invariants.checkArgument;

public class RecoveryTracker extends AbstractTracker<RecoveryTracker.RecoveryShardTracker, Node.Id>
{
    public static class RecoveryShardTracker extends QuorumShardTracker
    {
        protected int fastPathRejects = 0;

        private RecoveryShardTracker(Shard shard, DataConsistencyLevel dataCL)
        {
            super(shard, dataCL);
            checkArgument(dataCL == INVALID);
        }

        private ShardOutcomes onSuccessRejectFastPath(Node.Id from)
        {
            if (shard.fastPathElectorate.contains(from))
                ++fastPathRejects;
            return onSuccess(from);
        }

        private boolean rejectsFastPath()
        {
            return fastPathRejects > shard.fastPathElectorate.size() - shard.fastPathQuorumSize;
        }
    }

    public RecoveryTracker(Topologies topologies)
    {
        super(topologies, INVALID, RecoveryShardTracker[]::new, RecoveryShardTracker::new);
    }

    public RequestStatus recordSuccess(Node.Id node, boolean acceptsFastPath)
    {
        if (acceptsFastPath)
            return recordResponse(this, node, RecoveryShardTracker::onSuccess, node);

        return recordResponse(this, node, RecoveryShardTracker::onSuccessRejectFastPath, node);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id from)
    {
        return recordResponse(this, from, RecoveryShardTracker::onFailure, from);
    }

    public boolean rejectsFastPath()
    {
        return any(RecoveryShardTracker::rejectsFastPath);
    }
}
