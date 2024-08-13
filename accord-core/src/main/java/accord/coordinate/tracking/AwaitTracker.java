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

import java.util.function.Predicate;

import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Unseekables;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.Invariants;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;
import static accord.utils.ArrayBuffers.cachedRanges;

public class AwaitTracker extends AbstractTracker<AwaitTracker.AwaitShardTracker>
{
    public static class AwaitShardTracker extends ShardTracker
    {
        protected int inFlight;
        protected int notReady;
        protected int ready;

        public AwaitShardTracker(Shard shard)
        {
            super(shard);
            inFlight = shard.rf();
        }

        public ShardOutcomes onSuccess(Boolean isReady)
        {
            if (isReady)
            {
                --inFlight;
                return ready++ == 0 ? Success : NoChange;
            }
            else
            {
                ++notReady;
                return --inFlight == 0 && ready == 0 ? Success : NoChange;
            }
        }

        // return true iff hasFailed()
        public ShardOutcomes onFailure(Object ignore)
        {
            if (--inFlight > 0)
                return NoChange;
            return hasFailed() ? Fail : Success;
        }

        public boolean hasSucceeded()
        {
            return notReady > 0;
        }

        boolean hasInFlight()
        {
            return inFlight > 0;
        }

        boolean hasFailed()
        {
            return inFlight == 0 && notReady == 0;
        }
    }

    public AwaitTracker(Topologies topologies)
    {
        super(topologies, AwaitShardTracker[]::new, AwaitShardTracker::new);
        Invariants.checkArgument(topologies.size() == 1, "AwaitTracker does not currently support multiple epochs; logic to compute ready keys assumes a single epoch");
    }

    public RequestStatus recordSuccess(Node.Id node, boolean isReady)
    {
        return recordResponse(this, node, AwaitShardTracker::onSuccess, isReady);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id node)
    {
        return recordResponse(this, node, AwaitShardTracker::onFailure, null);
    }

    public boolean hasFailed()
    {
        return any(AwaitShardTracker::hasFailed);
    }

    public Unseekables<?> ready(Unseekables<?> select)
    {
        return select(select, t -> t.ready > 0);
    }

    public Unseekables<?> notReady(Unseekables<?> select)
    {
        return select(select, t -> t.ready == 0);
    }

    private Unseekables<?> select(Unseekables<?> select, Predicate<AwaitShardTracker> include)
    {
        // this logic assumes a single epoch
        Range[] rangeBuffer = cachedRanges().get(trackers.length);
        int rangeCount = 0;
        for (int i = 0 ; i < trackers.length ; ++i)
        {
            if (trackers[i] == null) continue;
            if (!include.test(trackers[i])) continue;
            rangeBuffer[rangeCount++] = trackers[i].shard.range;
        }

        if (rangeCount == 0)
            return select.slice(0, 0);
        Ranges slice = Ranges.ofSortedAndDeoverlapped(cachedRanges().completeAndDiscard(rangeBuffer, rangeCount));
        return select.slice(slice);
    }
}
