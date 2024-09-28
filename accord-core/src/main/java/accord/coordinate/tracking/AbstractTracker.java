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

import accord.utils.Invariants;
import com.google.common.annotations.VisibleForTesting;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;

public abstract class AbstractTracker<ST extends ShardTracker>
{
    public enum ShardOutcomes implements ShardOutcome<AbstractTracker<?>>
    {
        Fail(RequestStatus.Failed),
        Success(RequestStatus.Success),
        SendMore(null),
        NoChange(RequestStatus.NoChange);

        final RequestStatus result;

        ShardOutcomes(RequestStatus result) {
            this.result = result;
        }

        private boolean isTerminalState()
        {
            return compareTo(Success) <= 0;
        }

        private static ShardOutcomes min(ShardOutcomes a, ShardOutcomes b)
        {
            return a.compareTo(b) <= 0 ? a : b;
        }

        @Override
        public ShardOutcomes apply(AbstractTracker<?> tracker, int shardIndex)
        {
            if (this == Success)
                return --tracker.waitingOnShards == 0 ? Success : NoChange;
            return this;
        }

        private RequestStatus toRequestStatus(AbstractTracker<?> tracker)
        {
            if (result != null)
                return result;
            return tracker.trySendMore();
        }
    }

    public interface ShardFactory<ST extends ShardTracker>
    {
        ST apply(int epochIndex, Shard shard);
    }

    protected final Topologies topologies;
    protected final ST[] trackers;
    protected final int maxShardsPerEpoch;
    protected int waitingOnShards;

    public AbstractTracker(Topologies topologies, IntFunction<ST[]> arrayFactory, Function<Shard, ST> trackerFactory)
    {
        this(topologies, arrayFactory, (ignore, shard) -> trackerFactory.apply(shard));
    }

    public AbstractTracker(Topologies topologies, IntFunction<ST[]> arrayFactory, ShardFactory<ST> trackerFactory)
    {
        Invariants.checkArgument(topologies.totalShards() > 0);
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
                trackers[i * maxShardsPerEpoch + j] = trackerFactory.apply(i, topology.get(j));
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

    protected <T extends AbstractTracker<ST>, P>
    RequestStatus recordResponse(T self, Id node, BiFunction<? super ST, P, ? extends ShardOutcome<? super T>> function, P param)
    {
        return recordResponse(self, node, function, param, topologies.size());
    }

    protected <T extends AbstractTracker<ST>, P>
    RequestStatus recordResponse(T self, Id node, BiFunction<? super ST, P, ? extends ShardOutcome<? super T>> function, P param, int topologyLimit)
    {
        Invariants.checkState(self == this); // we just accept self as parameter for type safety
        ShardOutcomes status = NoChange;
        int maxShards = maxShardsPerEpoch();
        for (int i = 0; i < topologyLimit && !status.isTerminalState() ; ++i)
        {
            status = topologies.get(i).mapReduceOn(node, i * maxShards, AbstractTracker::apply, self, function, param, ShardOutcomes::min, status);
        }

        return status.toRequestStatus(this);
    }

    static <ST extends ShardTracker, P, T extends AbstractTracker<ST>>
    ShardOutcomes apply(T tracker, BiFunction<? super ST, P, ? extends ShardOutcome<? super T>> function, P param, int trackerIndex)
    {
        return function.apply(tracker.trackers[trackerIndex], param).apply(tracker, trackerIndex);
    }

    public boolean any(Predicate<ST> test)
    {
        for (ST tracker : trackers)
        {
            if (tracker == null) continue;
            if (test.test(tracker))
                return true;
        }
        return false;
    }

    public boolean all(Predicate<ST> test)
    {
        for (ST tracker : trackers)
        {
            if (tracker == null) continue;
            if (!test.test(tracker))
                return false;
        }
        return true;
    }

    public Collection<Id> nodes()
    {
        return topologies.nodes();
    }

    public Collection<Id> nonStaleNodes()
    {
        return topologies.nonStaleNodes();
    }

    public ST get(int shardIndex)
    {
        int maxShardsPerEpoch = maxShardsPerEpoch();
        return get(shardIndex / maxShardsPerEpoch, shardIndex % maxShardsPerEpoch);
    }

    @VisibleForTesting
    public ST get(int topologyIdx, int shardIdx)
    {
        if (shardIdx >= maxShardsPerEpoch())
            throw new IndexOutOfBoundsException();
        return trackers[topologyOffset(topologyIdx) + shardIdx];
    }

    protected int maxShardsPerEpoch()
    {
        return maxShardsPerEpoch;
    }
}
