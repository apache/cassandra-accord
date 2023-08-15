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
import accord.topology.Shard;
import accord.topology.Topologies;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;

import java.util.function.BiFunction;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.*;

// TODO (desired, efficiency): if any shard *cannot* take the fast path, and all shards have accepted, terminate
public class FastPathTracker extends AbstractTracker<FastPathTracker.FastPathQuorumShardTracker>
{
    private static final ShardOutcome<FastPathTracker> NewFastPathSuccess = (tracker, shardIndex) -> {
        --tracker.waitingOnFastPathSuccess;
        return --tracker.waitingOnShards == 0 ? Success : NoChange;
    };

    public static class FastPathQuorumShardTracker extends QuorumShardTracker
    {
        public FastPathQuorumShardTracker(Shard shard)
        {
            super(shard);
        }

        public ShardOutcome<? super FastPathTracker> onQuorumSuccess(Node.Id node)
        {
            return super.onSuccess(node);
        }

        public ShardOutcome<? super FastPathTracker> onMaybeFastPathSuccess(Node.Id node)
        {
            return super.onSuccess(node);
        }

        public ShardOutcome<? super FastPathTracker> onFailure(@Nonnull Node.Id from)
        {
            return super.onFailure(from);
        }

        public boolean hasRejectedFastPath()
        {
            return false;
        }

        public boolean hasMetFastPathCriteria()
        {
            return true;
        }
    }

    public static class FastPathShardTracker extends FastPathQuorumShardTracker
    {
        protected int fastPathAccepts;
        protected int fastPathFailures;

        public FastPathShardTracker(Shard shard)
        {
            super(shard);
        }

        // return NewQuorumSuccess ONLY once fast path is rejected
        @Override
        public ShardOutcome<? super FastPathTracker> onQuorumSuccess(Node.Id node)
        {
            ++successes;
            if (!shard.fastPathElectorate.contains(node))
                return quorumIfRejectsFastPath();

            ++fastPathFailures;
            if (isNewFastPathReject() && hasReachedQuorum())
                return Success;

            if (isNewSlowPathSuccess() && hasRejectedFastPath())
                return Success;

            return NoChange;
        }

        @Override
        public ShardOutcome<? super FastPathTracker> onMaybeFastPathSuccess(Node.Id node)
        {
            ++successes;
            if (shard.fastPathElectorate.contains(node))
            {
                ++fastPathAccepts;
                if (isNewFastPathSuccess())
                    return NewFastPathSuccess;
            }
            return quorumIfRejectsFastPath();
        }

        @Override
        public ShardOutcome<? super FastPathTracker> onFailure(@Nonnull Node.Id from)
        {
            if (++failures > shard.maxFailures)
                return Fail;

            if (shard.fastPathElectorate.contains(from)) {
                ++fastPathFailures;

                if (isNewFastPathReject() && successes >= shard.slowPathQuorumSize)
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
            return successes == shard.slowPathQuorumSize;
        }

        private boolean isNewFastPathReject()
        {
            return fastPathFailures == 1 + shard.fastPathElectorate.size() - shard.fastPathQuorumSize;
        }

        private boolean isNewFastPathSuccess()
        {
            return fastPathAccepts == shard.fastPathQuorumSize;
        }

        @Override
        @VisibleForTesting
        public boolean hasMetFastPathCriteria()
        {
            return fastPathAccepts >= shard.fastPathQuorumSize;
        }

        @Override
        @VisibleForTesting
        public boolean hasRejectedFastPath()
        {
            return shard.rejectsFastPath(fastPathFailures);
        }
    }

    int waitingOnFastPathSuccess; // if we reach zero, we have succeeded on the fast path outcome for every shard
    public FastPathTracker(Topologies topologies)
    {
        super(topologies, FastPathQuorumShardTracker[]::new, (i, shard) -> i == 0 ? new FastPathShardTracker(shard) : new FastPathQuorumShardTracker(shard));
        this.waitingOnFastPathSuccess = topologies.current().size();
    }

    public RequestStatus recordSuccess(Node.Id from, boolean withFastPathTimestamp)
    {
        if (withFastPathTimestamp)
            return recordResponse(from, FastPathQuorumShardTracker::onMaybeFastPathSuccess);

        return recordResponse(from, FastPathQuorumShardTracker::onQuorumSuccess);
    }

    public RequestStatus recordFailure(Node.Id from)
    {
        return recordResponse(from, FastPathQuorumShardTracker::onFailure);
    }

    protected RequestStatus recordResponse(Node.Id node, BiFunction<? super FastPathQuorumShardTracker, Node.Id, ? extends ShardOutcome<? super FastPathTracker>> function)
    {
        return recordResponse(this, node, function, node);
    }

    public boolean hasFastPathAccepted()
    {
        return waitingOnFastPathSuccess == 0;
    }

    public boolean hasFailed()
    {
        return any(FastPathQuorumShardTracker::hasFailed);
    }

    public boolean hasInFlight()
    {
        return any(FastPathQuorumShardTracker::hasInFlight);
    }

    public boolean hasReachedQuorum()
    {
        return all(FastPathQuorumShardTracker::hasReachedQuorum);
    }
}
