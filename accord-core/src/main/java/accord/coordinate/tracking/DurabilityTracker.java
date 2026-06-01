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
import accord.local.durability.DurabilityLevel;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncReadable;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.SortedListSet;
import org.agrona.collections.IntHashSet;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;

public class DurabilityTracker extends SimpleTracker<DurabilityTracker.DurabilityShardTracker> implements ResponseTracker
{
    private static final ShardOutcome<DurabilityTracker> NewQuorum = (t, i) -> {
        --t.waitingOnQuorum;
        return NoChange;
    };

    private static final ShardOutcome<DurabilityTracker> NewSuccessAndQuorum = (t, i) -> {
        --t.waitingOnQuorum;
        return Success.apply(t, i);
    };

    private static final ShardOutcome<DurabilityTracker> NewFailAndQuorum = (t, i) -> {
        --t.waitingOnQuorum;
        return Fail.apply(t, i);
    };

    public static class DurabilityShardTracker extends ShardTracker
    {
        static final IntHashSet EMPTY_SET = new IntHashSet();

        // TODO (desired): support partial success (but perhaps requires support also from execution)
        protected final SortedListSet<Node.Id> including;
        protected final SortedArrayList<Node.Id> ineligible;
        protected final IntHashSet ineligibleSet;
        protected int waitingOnSuccess;
        protected int waitingOn;

        public DurabilityShardTracker(SortedArrayList<Node.Id> staleOrRemoved, Shard shard)
        {
            super(shard);
            this.including = SortedListSet.noneOf(shard.nodes);
            this.ineligible = staleOrRemoved.with(shard.hardRemoved);
            if (!ineligible.isEmpty())
            {
                this.ineligibleSet = new IntHashSet();
                for (Node.Id id : shard.hardRemoved)
                {
                    this.ineligibleSet.add(id.id);
                }
                for (Node.Id id : shard.nodes)
                {
                    if (staleOrRemoved.contains(id))
                        this.ineligibleSet.add(id.id);
                }
            }
            else this.ineligibleSet = EMPTY_SET;
            this.waitingOn = shard.rf();
            this.waitingOnSuccess = waitingOn - this.ineligibleSet.size();
            Invariants.require(this.ineligibleSet.size() <= shard.maxFailures);
        }

        public ShardOutcome<? super DurabilityTracker> onSuccess(Node.Id from)
        {
            including.add(from);
            if (!ineligibleSet.contains(from.id))
                --waitingOnSuccess;
            return onResponse(including.size() == shard.slowQuorumSize);
        }

        // return true iff hasFailed()
        public ShardOutcome<? super DurabilityTracker> onFailure(Node.Id from)
        {
            return onResponse(false);
        }

        private ShardOutcome<? super DurabilityTracker> onResponse(boolean newQuorum)
        {
            if (--waitingOn > 0)
                return newQuorum ? NewQuorum : NoChange;
            return hasSucceeded() ? newQuorum ? NewSuccessAndQuorum : Success
                                  : newQuorum ? NewFailAndQuorum : Fail;
        }

        public boolean hasSucceeded()
        {
            return waitingOnSuccess == 0;
        }

        public boolean hasQuorumSuccess()
        {
            return including.size() >= Math.min(shard.slowQuorumSize, eligible());
        }

        public boolean hasMinorityQuorumSuccess()
        {
            return including.size() >= Math.min(shard.minorityQuorumSize(), eligible());
        }

        private int eligible()
        {
            return shard.rf - ineligibleSet.size();
        }

        boolean hasFailed()
        {
            return waitingOn == 0 && !hasSucceeded();
        }

        @Override
        public String summarise()
        {
            if (ineligibleSet == null)
                return including.size() + "/" + shard.rf;

            return including.size() + "/" + (shard.rf - ineligibleSet.size()) + '(' + shard.rf + ')';
        }

        DurabilityLevel result(Node.Id self, boolean knownToSelf, boolean hasInFlight, SortedArrayList<Node.Id> readable)
        {
            SyncLocal local;
            if (including.contains(self) || !shard.nodes.contains(self)) local = SyncLocal.Self;
            else if (knownToSelf) local = SyncLocal.KnownToSelf;
            else local = SyncLocal.NoLocal;

            SyncRemote remote;
            if (hasSucceeded()) remote = SyncRemote.All;
            else if (hasQuorumSuccess()) remote = hasInFlight ? SyncRemote.Quorum : SyncRemote.QuorumAndWaitedForAll;
            else if (hasMinorityQuorumSuccess()) remote = hasInFlight ? SyncRemote.MinorityQuorum : SyncRemote.MinorityQuorumAndWaitedForAll;
            else remote = SyncRemote.NoRemote;

            SortedArrayList<Node.Id> including = SortedArrayList.copySorted(this.including, Node.Id[]::new);
            SortedArrayList<Node.Id> excluding = shard.nodes.without(including);

            return new DurabilityLevel(local, remote, SyncReadable.known(readable.intersects(including)), including, excluding, ineligible);
        }
    }

    private int waitingOnQuorum;

    public DurabilityTracker(Topologies topologies)
    {
        super(topologies, DurabilityShardTracker[]::new, topologies.staleOrRemovedIds(), (p, i, s) -> new DurabilityShardTracker(p, s));
        waitingOnQuorum = waitingOnShards;
    }

    public RequestStatus recordSuccess(Node.Id node)
    {
        return recordResponse(this, node, DurabilityShardTracker::onSuccess, node);
    }

    // return true iff hasFailed()
    public RequestStatus recordFailure(Node.Id node)
    {
        return recordResponse(this, node, DurabilityShardTracker::onFailure, node);
    }

    public boolean hasFailed()
    {
        return any(DurabilityShardTracker::hasFailed);
    }

    public boolean hasSucceeded()
    {
        return all(DurabilityShardTracker::hasSucceeded);
    }

    public ReducingRangeMap<DurabilityLevel> results(Node.Id self, boolean knownToSelf, boolean hasInFlight, SortedArrayList<Node.Id> readable)
    {
        ReducingRangeMap<DurabilityLevel> result = null;
        ReducingRangeMap.Builder<DurabilityLevel> builder = new ReducingRangeMap.Builder<>(trackers.length);
        for (int topologyIndex = 0 ; topologyIndex < topologies.size() ; ++topologyIndex)
        {
            for (int i = topologyOffset(topologyIndex), max = topologyOffset(topologyIndex + 1); i < max ; ++i)
            {
                DurabilityShardTracker tracker = trackers[i];
                if (tracker == null)
                    continue;

                builder.appendNoOverlap(tracker.shard.range.start(), tracker.result(self, knownToSelf, hasInFlight, readable));
                builder.appendNoOverlap(tracker.shard.range.end(), null);
            }

            ReducingRangeMap<DurabilityLevel> add = builder.build();
            if (result == null) result = add;
            else result = ReducingRangeMap.merge(result, add, DurabilityLevel::min);
            builder.clear();
        }
        return result;
    }

    public boolean hasQuorumSuccess()
    {
        Invariants.require((waitingOnQuorum == 0) == all(DurabilityShardTracker::hasQuorumSuccess));
        return waitingOnQuorum == 0;
    }

    public boolean hasMinorityQuorumSuccess()
    {
        return all(DurabilityShardTracker::hasMinorityQuorumSuccess);
    }
}
