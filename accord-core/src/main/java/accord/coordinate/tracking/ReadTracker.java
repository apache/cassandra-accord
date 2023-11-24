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

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import accord.coordinate.tracking.ReadTracker.ReadShardTracker.PartialReadSuccess;
import accord.primitives.Ranges;
import accord.topology.Shard;
import accord.topology.ShardSelection;
import com.google.common.annotations.VisibleForTesting;
import accord.utils.Invariants;

import accord.local.Node.Id;
import accord.topology.Topologies;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.*;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Invariants.illegalState;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

public class ReadTracker extends AbstractTracker<ReadTracker.ReadShardTracker>
{
    private static final ShardOutcome<ReadTracker> DataSuccess = (tracker, shardIndex) -> {
        --tracker.waitingOnData;
        return --tracker.waitingOnShards == 0 ? Success : NoChange;
    };

    public static class ReadShardTracker extends ShardTracker
    {
        protected boolean hasData = false;
        protected int quorum; // if !hasData, a slowPathQuorum will trigger success
        protected int inflight;
        protected int contacted;
        protected int slow;
        protected Ranges unavailable;

        public ReadShardTracker(Shard shard)
        {
            super(shard);
        }

        public ShardOutcome<? super ReadTracker> recordInFlightRead(boolean ignore)
        {
            ++contacted;
            ++inflight;
            return NoChange;
        }

        public ShardOutcome<? super ReadTracker> recordSlowResponse(boolean ignore)
        {
            Invariants.checkState(!hasFailed());
            ++slow;

            if (shouldRead() && canRead())
                return SendMore;

            return NoChange;
        }

        /**
         * Have received a requested data payload with desired contents
         */
        public ShardOutcome<? super ReadTracker> recordReadSuccess(boolean isSlow)
        {
            Invariants.checkState(inflight > 0);
            boolean hadSucceeded = hasSucceeded();
            --inflight;
            if (isSlow) --slow;
            hasData = true;
            if (unavailable != null) unavailable = Ranges.EMPTY;
            return hadSucceeded ? NoChange : DataSuccess;
        }

        static class PartialReadSuccess
        {
            final boolean isSlow;
            final Ranges unavailable;

            PartialReadSuccess(boolean isSlow, Ranges unavailable)
            {
                this.isSlow = isSlow;
                this.unavailable = unavailable;
            }
        }
        /**
         * Have received a requested data payload with desired contents
         */
        public ShardOutcome<? super ReadTracker> recordPartialReadSuccess(PartialReadSuccess partialSuccess)
        {
            Invariants.checkState(inflight > 0);
            boolean hadSucceeded = hasSucceeded();
            --inflight;
            if (partialSuccess.isSlow) --slow;
            if (hadSucceeded)
                return NoChange;

            // TODO (low priority, efficiency): support slice method accepting a single Range
            if (unavailable == null) unavailable = partialSuccess.unavailable.slice(Ranges.of(shard.range));
            else unavailable = unavailable.slice(partialSuccess.unavailable, Minimal);
            if (!unavailable.isEmpty())
                return ensureProgressOrFail();

            hasData = true;
            return DataSuccess;
        }

        public ShardOutcome<? super ReadTracker> recordQuorumReadSuccess(boolean isSlow)
        {
            Invariants.checkState(inflight > 0);
            boolean hadSucceeded = hasSucceeded();
            --inflight;
            ++quorum;
            if (isSlow) --slow;

            if (hadSucceeded)
                return NoChange;

            if (quorum == shard.slowPathQuorumSize)
                return Success;

            return ensureProgressOrFail();
        }

        public ShardOutcomes recordReadFailure(boolean isSlow)
        {
            Invariants.checkState(inflight > 0);
            --inflight;
            if (isSlow) --slow;

            return ensureProgressOrFail();
        }

        private ShardOutcomes ensureProgressOrFail()
        {
            if (!shouldRead())
                return NoChange;

            if (canRead())
                return SendMore;

            return hasInFlight() ? NoChange : Fail;
        }

        public boolean hasReachedQuorum()
        {
            return quorum >= shard.slowPathQuorumSize;
        }

        public boolean hasSucceeded()
        {
            return hasData() || hasReachedQuorum();
        }

        boolean hasInFlight()
        {
            return inflight > 0;
        }

        public boolean shouldRead()
        {
            return !hasSucceeded() && inflight == slow;
        }

        public boolean canRead()
        {
            return contacted < shard.rf();
        }

        public boolean hasFailed()
        {
            return !hasData && inflight == 0 && contacted == shard.nodes.size() && !hasReachedQuorum();
        }

        public boolean hasData()
        {
            return hasData;
        }

        public Ranges unavailable()
        {
            return unavailable;
        }
    }

    final Set<Id> inflight;    // TODO (easy, efficiency): use Agrona's IntHashSet as soon as Node.Id switches from long to int
    final List<Id> candidates; // TODO (easy, efficiency): use Agrona's IntArrayList as soon as Node.Id switches from long to int
    private Set<Id> slow;      // TODO (easy, efficiency): use Agrona's IntHashSet as soon as Node.Id switches from long to int
    protected int waitingOnData;

    public ReadTracker(Topologies topologies)
    {
        super(topologies, ReadShardTracker[]::new, ReadShardTracker::new);
        this.candidates = new ArrayList<>(topologies.nodes()); // TODO (low priority, efficiency): copyOfNodesAsList to avoid unnecessary copies
        this.inflight = newHashSetWithExpectedSize(maxShardsPerEpoch());
        this.waitingOnData = waitingOnShards;
    }

    @VisibleForTesting
    protected void recordInFlightRead(Id node)
    {
        if (!inflight.add(node))
            throw illegalState(node + " already in flight");

        recordResponse(this, node, ReadShardTracker::recordInFlightRead, false);
    }

    private boolean receiveResponseIsSlow(Id node)
    {
        if (!inflight.remove(node))
            throw illegalState("Nothing in flight for " + node);

        return slow != null && slow.remove(node);
    }

    /**
     * Record a response that immediately satisfies the criteria for the shards the node participates in
     */
    protected RequestStatus recordSlowResponse(Id from)
    {
        if (!inflight.contains(from))
            throw new IllegalStateException();

        if (slow == null)
            slow = newHashSetWithExpectedSize(maxShardsPerEpoch());

        if (!slow.add(from)) // we can mark slow responses due to ReadCoordinator.TryAlternative OR onSlowResponse
            return RequestStatus.NoChange;

        return recordResponse(this, from, ReadShardTracker::recordSlowResponse, true);
    }

    /**
     * Record a response that immediately satisfies the criteria for the shards the node participates in
     */
    protected RequestStatus recordReadSuccess(Id from)
    {
        return recordResponse(from, ReadShardTracker::recordReadSuccess);
    }

    /**
     * Record a response that immediately satisfies the criteria for the shards the node participates in
     */
    protected RequestStatus recordPartialReadSuccess(Id from, Ranges unavailable)
    {
        boolean isSlow = receiveResponseIsSlow(from);
        return recordResponse(this, from, ReadShardTracker::recordPartialReadSuccess, new PartialReadSuccess(isSlow, unavailable));
    }

    /**
     * Record a response that contributes to a potential quorum decision (i.e. accept once we have such a quorum)
     */
    protected RequestStatus recordQuorumReadSuccess(Id from)
    {
        return recordResponse(from, ReadShardTracker::recordQuorumReadSuccess);
    }

    /**
     * Record a failure response
     */
    protected RequestStatus recordReadFailure(Id from)
    {
        return recordResponse(from, ReadShardTracker::recordReadFailure);
    }

    protected RequestStatus recordResponse(Id from, BiFunction<? super ReadShardTracker, Boolean, ? extends ShardOutcome<? super ReadTracker>> function)
    {
        boolean isSlow = receiveResponseIsSlow(from);
        return recordResponse(this, from, function, isSlow);
    }

    public <T1> RequestStatus trySendMore(BiConsumer<T1, Id> contact, T1 with)
    {
        ShardSelection toRead;
        {
            ShardSelection tmp = null;
            for (int i = 0 ; i < trackers.length ; ++i)
            {
                ReadShardTracker tracker = trackers[i];
                if (tracker == null || !tracker.shouldRead() || !tracker.canRead())
                    continue;

                if (tmp == null)
                    tmp = new ShardSelection(); // determinism

                tmp.set(i);
            }
            toRead = tmp;
        }

        Invariants.checkState(toRead != null, "We were asked to read more, but found no shards in need of reading more");

        // TODO (desired, consider): maybe for each additional candidate do one linear compare run to find better secondary match
        //       OR at least discount candidates that do not contribute additional knowledge beyond those additional
        //       candidates already contacted, since implementations are likely to sort primarily by health
        candidates.sort((a, b) -> topologies().compare(a, b, toRead));
        int i = candidates.size() - 1;
        while (i >= 0)
        {
            Id candidate = candidates.get(i);
            topologies().forEach((topology, ti) -> {
                int offset = topologyOffset(ti);
                topology.forEachOn(candidate, (s, si) -> toRead.clear(offset + si));
            });

            if (toRead.isEmpty())
                break;

            --i;
        }

        if (!toRead.isEmpty())
            return RequestStatus.NoChange;

        for (int j = candidates.size() - 1; j >= i; --j)
        {
            Id candidate = candidates.get(j);
            recordInFlightRead(candidate);
            contact.accept(with, candidate);
            candidates.remove(j);
        }
        return RequestStatus.NoChange;
    }

    public boolean hasData()
    {
        return all(ReadShardTracker::hasData);
    }

    public boolean hasFailed()
    {
        return any(ReadShardTracker::hasFailed);
    }
}
