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

import java.util.function.BiFunction;
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import accord.local.Node;
import accord.primitives.DataConsistencyLevel;
import accord.topology.Shard;
import accord.topology.Topologies;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;
import static accord.primitives.DataConsistencyLevel.INVALID;
import static accord.utils.Invariants.checkArgument;

// TODO (desired, efficiency): if any shard *cannot* take the fast path, and all shards have accepted, terminate
public class FastPathTracker extends AbstractTracker<FastPathTracker.FastPathShardTracker, Node.Id>
{
    private static final ShardOutcome<FastPathTracker> NewFastPathSuccess = (tracker, shardIndex) -> {
        --tracker.waitingOnFastPathSuccess;
        return --tracker.waitingOnShards == 0 ? Success : NoChange;
    };

    public static class FastPathShardTracker extends ShardTracker
    {
        protected int fastPathAccepts, accepts;
        protected int fastPathFailures, failures;

        public FastPathShardTracker(Shard shard, DataConsistencyLevel dataCL)
        {
            super(shard, dataCL);
            checkArgument(dataCL == INVALID);
        }

        // return NewQuorumSuccess ONLY once fast path is rejected
        public ShardOutcome<? super FastPathTracker> onQuorumSuccess(Node.Id node)
        {
            ++accepts;
            if (!shard.fastPathElectorate.contains(node))
                return quorumIfRejectsFastPath();

            ++fastPathFailures;
            if (isNewFastPathReject() && hasReachedQuorum())
                return Success;

            if (isNewSlowPathSuccess() && hasRejectedFastPath())
                return Success;

            return NoChange;
        }

        public ShardOutcome<? super FastPathTracker> onMaybeFastPathSuccess(Node.Id node)
        {
            ++accepts;
            if (shard.fastPathElectorate.contains(node))
            {
                ++fastPathAccepts;
                if (isNewFastPathSuccess())
                    return NewFastPathSuccess;
            }
            return quorumIfRejectsFastPath();
        }

        public ShardOutcome<? super FastPathTracker> onFailure(@Nonnull Node.Id from)
        {
            if (++failures > shard.maxFailures)
                return Fail;

            if (shard.fastPathElectorate.contains(from)) {
                ++fastPathFailures;

                if (isNewFastPathReject() && accepts >= shard.slowPathQuorumSize)
                    return Success;
            }

            return NoChange;
        }

        private ShardOutcome<? super FastPathTracker> quorumIfRejectsFastPath()
        {
            return isNewSlowPathSuccess() && hasRejectedFastPath() ? Success : NoChange;
        }

        private boolean isNewSlowPathSuccess()
        {
            return accepts == shard.slowPathQuorumSize;
        }

        private boolean isNewFastPathReject()
        {
            return fastPathFailures == 1 + shard.fastPathElectorate.size() - shard.fastPathQuorumSize;
        }

        private boolean isNewFastPathSuccess()
        {
            return fastPathAccepts == shard.fastPathQuorumSize;
        }

        @VisibleForTesting
        public boolean hasMetFastPathCriteria()
        {
            return fastPathAccepts >= shard.fastPathQuorumSize;
        }

        @VisibleForTesting
        public boolean hasRejectedFastPath()
        {
            return shard.rejectsFastPath(fastPathFailures);
        }

        boolean hasInFlight()
        {
            return accepts + failures < shard.rf();
        }

        boolean hasReachedQuorum()
        {
            return accepts >= shard.slowPathQuorumSize;
        }

        boolean hasFailed()
        {
            return failures > shard.maxFailures;
        }
    }

    int waitingOnFastPathSuccess; // if we reach zero, we have determined the fast path outcome of every shard
    public FastPathTracker(Topologies topologies)
    {
        super(topologies, INVALID, FastPathShardTracker[]::new, FastPathShardTracker::new);
        this.waitingOnFastPathSuccess = super.waitingOnShards;
    }

    public RequestStatus recordSuccess(Node.Id from, boolean withFastPathTimestamp)
    {
        if (withFastPathTimestamp)
            return recordResponse(from, FastPathShardTracker::onMaybeFastPathSuccess);

        return recordResponse(from, FastPathShardTracker::onQuorumSuccess);
    }

    public RequestStatus recordFailure(Node.Id from)
    {
        return recordResponse(from, FastPathShardTracker::onFailure);
    }

    protected RequestStatus recordResponse(Node.Id node, BiFunction<? super FastPathShardTracker, Node.Id, ? extends ShardOutcome<? super FastPathTracker>> function)
    {
        return recordResponse(this, node, function, node);
    }

    public boolean hasFastPathAccepted()
    {
        return waitingOnFastPathSuccess == 0;
    }

    public boolean hasFailed()
    {
        return any(FastPathShardTracker::hasFailed);
    }

    public boolean hasInFlight()
    {
        return any(FastPathShardTracker::hasInFlight);
    }

    public boolean hasReachedQuorum()
    {
        return all(FastPathShardTracker::hasReachedQuorum);
    }
}
