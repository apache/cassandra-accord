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

import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.DataConsistencyLevel;
import accord.topology.Shard;
import accord.topology.Topologies;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;
import static accord.primitives.DataConsistencyLevel.INVALID;
import static accord.utils.Invariants.checkArgument;

public class InvalidationTracker extends AbstractTracker<InvalidationTracker.InvalidationShardTracker, Node.Id>
{
    public static class InvalidationShardTracker extends ShardTracker implements ShardOutcome<InvalidationTracker>
    {
        private int fastPathRejects;
        private int fastPathInflight;
        private int promises;
        private int inflight;
        private boolean isDecided;

        private InvalidationShardTracker(Shard shard, DataConsistencyLevel dataCL)
        {
            super(shard, dataCL);
            checkArgument(dataCL == INVALID);
            inflight = shard.rf();
            fastPathInflight = shard.fastPathElectorate.size();
        }

        public InvalidationShardTracker onSuccess(Node.Id from, boolean isPromised, boolean withFastPath)
        {
            if (shard.fastPathElectorate.contains(from))
            {
                --fastPathInflight;
                if (!withFastPath) ++fastPathRejects;
            }
            if (isPromised) ++promises;
            --inflight;
            return this;
        }

        public ShardOutcome<? super InvalidationTracker> onFailure(Node.Id from)
        {
            if (shard.fastPathElectorate.contains(from))
                --fastPathInflight;
            --inflight;
            return this;
        }

        public boolean isDecided()
        {
            return isFastPathDecided() && isPromiseDecided();
        }

        private boolean isFastPathDecided()
        {
            return isFastPathRejected() || !canFastPathBeRejected();
        }

        public boolean isFastPathRejected()
        {
            return fastPathRejects > shard.fastPathElectorate.size() - shard.fastPathQuorumSize;
        }

        public boolean canFastPathBeRejected()
        {
            return fastPathRejects + fastPathInflight > shard.fastPathElectorate.size() - shard.fastPathQuorumSize;
        }

        private boolean isPromiseDecided()
        {
            return isPromised() || isPromiseRejected();
        }

        public boolean isPromiseRejected()
        {
            return promises + inflight < shard.slowPathQuorumSize;
        }

        public boolean isPromised()
        {
            return promises >= shard.slowPathQuorumSize;
        }

        @Override
        public ShardOutcomes apply(InvalidationTracker tracker, int shardIndex)
        {
            if (isDecided)
                return NoChange;

            if (isFastPathRejected()) tracker.rejectsFastPath = true;
            if (isPromised() && tracker.promisedShard < 0) tracker.promisedShard = shardIndex;
            if (isDecided()) isDecided = true;

            if (tracker.rejectsFastPath && tracker.promisedShard >= 0)
            {
                tracker.waitingOnShards = 0;
                return Success;
            }

            if (isDecided && --tracker.waitingOnShards == 0)
                return tracker.all(InvalidationShardTracker::isPromised) ? Success : Fail;

            return NoChange;
        }
    }

    private int promisedShard = -1;
    private boolean rejectsFastPath;
    public InvalidationTracker(Topologies topologies)
    {
        super(topologies, INVALID, InvalidationShardTracker[]::new, InvalidationShardTracker::new);
    }

    public Shard promisedShard()
    {
        return get(promisedShard).shard;
    }

    public boolean isPromised()
    {
        return promisedShard >= 0;
    }

    public boolean isSafeToInvalidate()
    {
        return rejectsFastPath;
    }

    public boolean isPromisedForKey(RoutingKey key, long epoch)
    {
        int shardIndex = (int) (topologies.get(0).epoch() - epoch);
        int withinShardIndex = topologies.get(shardIndex).indexForKey(key);
        return get(shardIndex, withinShardIndex).isPromised();
    }

    public RequestStatus recordSuccess(Node.Id from, boolean isPromised, boolean acceptedFastPath)
    {
        return recordResponse(this, from, (shard, node) -> shard.onSuccess(node, isPromised, acceptedFastPath), from);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id from)
    {
        return recordResponse(this, from, InvalidationShardTracker::onFailure, from);
    }
}
