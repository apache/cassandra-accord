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

import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

public abstract class AbstractTracker<ST extends ShardTracker, P>
{
    /**
     * Represents the logical result of a ShardFunction applied to a ShardTracker,
     * encapsulating also any modification it should make to the AbstractTracker
     * containing the shard.
     */
    public interface ShardOutcome<T extends AbstractTracker<?, ?>>
    {
        ShardOutcomes apply(T tracker);
    }

    public enum ShardOutcomes implements ShardOutcome<AbstractTracker<?, ?>>
    {
        Fail, SendMore, Success, NoChange;

        static ShardOutcomes min(ShardOutcomes a, ShardOutcomes b)
        {
            return a.compareTo(b) <= 0 ? a : b;
        }

        @Override
        public ShardOutcomes apply(AbstractTracker<?, ?> tracker)
        {
            if (this == Success)
                --tracker.waitingOnShards;
            return this;
        }
    }

    final Topologies topologies;
    protected final ST[] trackers;
    protected final int maxShardsPerEpoch;
    protected int waitingOnShards;

    AbstractTracker(Topologies topologies, IntFunction<ST[]> arrayFactory, Function<Shard, ST> trackerFactory)
    {
        Preconditions.checkArgument(topologies.totalShards() > 0);
        int topologyCount = topologies.size();
        int maxShardsPerEpoch = topologies.get(0).size();
        int shardCount = maxShardsPerEpoch;
        for (int i = 1 ; i < topologyCount ; ++i)
        {
            int size = topologies.get(i).size();
            maxShardsPerEpoch = Math.max(maxShardsPerEpoch, size);
            shardCount += size;
        }
        this.topologies = topologies;
        this.trackers = arrayFactory.apply(topologyCount * maxShardsPerEpoch);
        for (int i = 0 ; i < topologyCount ; ++i)
        {
            Topology topology = topologies.get(i);
            int size = topology.size();
            for (int j = 0; j < size; ++j)
                trackers[i * maxShardsPerEpoch + j] = trackerFactory.apply(topology.get(j));
        }
        this.maxShardsPerEpoch = maxShardsPerEpoch;
        this.waitingOnShards = shardCount;
    }

    protected int topologyOffset(int topologyIdx)
    {
        return topologyIdx * maxShardsPerEpoch();
    }

    public Topologies topologies()
    {
        return topologies;
    }

    protected RequestStatus trySendMore() { throw new UnsupportedOperationException(); }

    <T extends AbstractTracker<ST, P>>
    RequestStatus recordResponse(T self, Id node, BiFunction<? super ST, P, ? extends ShardOutcome<? super T>> function, P param)
    {
        return recordResponse(self, node, function, param, topologies.size());
    }

    <T extends AbstractTracker<ST, P>>
    RequestStatus recordResponse(T self, Id node, BiFunction<? super ST, P, ? extends ShardOutcome<? super T>> function, P param, int topologyLimit)
    {
        Preconditions.checkState(self == this); // we just accept self as parameter for type safety
        ShardOutcomes minShardStatus = ShardOutcomes.NoChange;
        int maxShards = maxShardsPerEpoch();
        for (int i = 0; i < topologyLimit && minShardStatus != ShardOutcomes.Fail; ++i)
        {
            minShardStatus = topologies.get(i).mapReduceOn(node, i * maxShards, AbstractTracker::apply, self, function, param, ShardOutcomes::min, minShardStatus);
        }

        switch (minShardStatus)
        {
            default: throw new AssertionError();
            case SendMore:
                return trySendMore();
            case Success:
                if (waitingOnShards == 0)
                    return RequestStatus.Success;
            case NoChange:
                return RequestStatus.NoChange;
            case Fail:
                return RequestStatus.Failed;
        }
    }

    static <ST extends ShardTracker, P, T extends AbstractTracker<ST, P>>
    ShardOutcomes apply(int trackerIndex, T tracker, BiFunction<? super ST, P, ? extends ShardOutcome<? super T>> function, P param)
    {
        return function.apply(tracker.trackers[trackerIndex], param).apply(tracker);
    }

    public boolean any(Predicate<ST> test)
    {
        for (ST tracker : trackers)
        {
            if (test.test(tracker))
                return true;
        }
        return false;
    }

    public boolean all(Predicate<ST> test)
    {
        for (ST tracker : trackers)
        {
            if (!test.test(tracker))
                return false;
        }
        return true;
    }

    public boolean hasFailed()
    {
        return any(ShardTracker::hasFailed);
    }

    public boolean hasInFlight()
    {
        return any(ShardTracker::hasInFlight);
    }

    public boolean hasReachedQuorum()
    {
        return all(ShardTracker::hasReachedQuorum);
    }

    public Set<Id> nodes()
    {
        return topologies.nodes();
    }

    @VisibleForTesting
    public ST unsafeGet(int topologyIdx, int shardIdx)
    {
        if (shardIdx >= maxShardsPerEpoch())
            throw new IndexOutOfBoundsException();
        return trackers[topologyOffset(topologyIdx) + shardIdx];
    }

    protected int maxShardsPerEpoch()
    {
        return maxShardsPerEpoch;
    }

    public ST unsafeGet(int i)
    {
        Preconditions.checkArgument(topologies.size() == 1);
        return unsafeGet(0, i);
    }
}
