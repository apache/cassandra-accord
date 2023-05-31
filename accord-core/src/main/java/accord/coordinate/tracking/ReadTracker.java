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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;

import accord.api.RoutingKey;
import accord.local.Node.Id;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.RoutingKeys;
import accord.topology.Shard;
import accord.topology.ShardSelection;
import accord.topology.Topologies;
import accord.utils.TriConsumer;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.SendMore;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;
import static accord.primitives.DataConsistencyLevel.INVALID;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.checkState;
import static accord.utils.Invariants.nonNull;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;

public class ReadTracker extends AbstractTracker<ReadTracker.ReadShardTracker, Boolean>
{
    private static final ShardOutcome<ReadTracker> DataSuccess = (tracker, shardIndex) -> {
        --tracker.waitingOnData;
        return --tracker.waitingOnShards == 0 ? Success : NoChange;
    };

    public static class ReadShardTracker extends ShardTracker
    {
        /**
         * There are basically two use cases for ReadShardTracker which is reading Accord metadata
         * and reading actual data. Where they differ is how for a given shard you know if you
         * have gotten the information you need.
         *
         * ApproveIfQuorum indicates that the current response is not sufficient for this shard, but if a quorum of similar
         * responses are received it will be enough. For data reads a single data response
         * might trigger Approve if the consistency level only requires one data read, or a data read + some number of digest reads if the CL
         * requires it (triggering multiple ApproveIfQuorum and never Approve).
         *
         * For Accord metadata reads it's usually a quorum or getting a response back that meets
         * some sufficiency criteria in which cases more responses from the shard aren't necessary.
         *
         * These two pretty orthogonal concepts (sufficiency vs (data and digest)) are mixed throughout ReadTracker
         * and ReadCoordinator. There are no digest reads for Accord metadata it's really all about sufficiency XOR hitting a quorum,
         * and even when reading data it's not a given that a quorum is required or that there will be digest
         * reads sent since that is based on consistency level.
         */
        protected boolean hasDataOrSufficientResponse = false;
        protected int quorum; // if !hasDataOrSufficientResponse, a slowPathQuorum will trigger success
        protected int inflight;
        protected int contacted;
        protected int slow;
        private boolean succeeded = false;

        public ReadShardTracker(Shard shard, DataConsistencyLevel dataCL)
        {
            super(shard, dataCL);
        }

        public ShardOutcome<? super ReadTracker> recordInFlightRead(boolean ignore)
        {
            ++contacted;
            ++inflight;
            return NoChange;
        }

        public ShardOutcome<? super ReadTracker> recordSlowResponse(boolean ignore)
        {
            checkState(!hasFailed());
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
            checkState(!dataCL.requiresDigestReads, "Don't use recordReadSuccess to track data responses with digest reads");
            checkState(inflight > 0);
            --inflight;
            if (isSlow) --slow;

            hasDataOrSufficientResponse = true;

            if (succeeded)
                return NoChange;

            succeeded = true;
            return DataSuccess;
        }

        public ShardOutcome<? super ReadTracker> recordQuorumReadSuccess(boolean isSlow, boolean isDigestReadOrInsufficient)
        {
            checkState(inflight > 0);
            --inflight;
            ++quorum;
            if (isSlow) --slow;

            if (!isDigestReadOrInsufficient)
                hasDataOrSufficientResponse = true;

            if (succeeded)
                return NoChange;

            if (hasSucceeded())
            {
                succeeded = true;
                return Success;
            }

            return ensureProgressOrFail();
        }

        public ShardOutcomes recordReadFailure(boolean isSlow)
        {
            checkState(inflight > 0);
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
            return quorum >= quorumSize();
        }
        
        public int quorumSize()
        {
            switch (dataCL)
            {
                // Accord only needs to read data from a single replica.
                // UNSPECIFIED shouldn't be used for reading Accord metadata
                case UNSPECIFIED:
                    return 1;
                case QUORUM:
                    // TODO What about pending nodes? Will this come with bootstrap?
                    // We write to them, but they don't actually change read quorum size
                    // and it is important that they don't for availabiility
                    return (shard.nodes.size() / 2) + 1;
                // For reading Accord metadata, this is always accordSlowPathQuorumSize
                case INVALID:
                    return shard.slowPathQuorumSize;
                default:
                    throw new IllegalStateException("Unhandled CL " + dataCL);
            }
        }

        public boolean hasSucceeded()
        {
            if (dataCL.requiresDigestReads)
                // Both are required when sending digest reads
                return hasDataOrSufficientResponse() && hasReachedQuorum();
            else
                return hasDataOrSufficientResponse() || hasReachedQuorum();
        }

        boolean hasInFlight()
        {
            return inflight > 0;
        }

        public boolean shouldRead()
        {
            return !hasSucceeded()
                    && (dataCL.requiresDigestReads
                        ? (quorumSize() - quorum) > (inflight - slow)
                        : inflight == slow);
        }

        public boolean canRead()
        {
            return contacted < shard.rf();
        }

        public boolean hasFailed()
        {
            return !hasDataOrSufficientResponse() && inflight == 0 && contacted == shard.nodes.size() && !hasReachedQuorum();
        }

        public boolean hasDataOrSufficientResponse()
        {
            return hasDataOrSufficientResponse;
        }
    }

    // TODO (required): abstract the candidate selection process so the implementation may prioritise based on distance/health etc
    final Set<Id> inflight;    // TODO (easy, efficiency): use Agrona's IntHashSet as soon as Node.Id switches from long to int
    final List<Id> candidates; // TODO (easy, efficiency): use Agrona's IntArrayList as soon as Node.Id switches from long to int
    private Set<Id> slow;      // TODO (easy, efficiency): use Agrona's IntHashSet as soon as Node.Id switches from long to int

    /*
     * Initialized when sending the initial requests and used to track which nodes got data reads and digest reads
     * No entry means it was all data reads, because all followup reads are data reads
     * Empty list means it was all digest reads
     * Any key that is present was read as a data read
     */
    private @Nonnull Map<Id, List<RoutingKey>> contactedToDataReadKeys = ImmutableMap.of();

    protected final DataConsistencyLevel dataCL;
    protected int waitingOnData;

    public ReadTracker(Topologies topologies, DataConsistencyLevel dataCL)
    {
        super(topologies, dataCL, ReadShardTracker[]::new, ReadShardTracker::new);
        checkState(topologies.size() == 1 || dataCL == INVALID, "Data reads should only have a single topology");
        checkArgument(dataCL != DataConsistencyLevel.ALL, "ALL is not supported yet as a read DataConsistencyLevel");
        this.candidates = new ArrayList<>(topologies.nodes()); // TODO (low priority, efficiency): copyOfNodesAsList to avoid unnecessary copies
        this.dataCL = dataCL;
        this.inflight = newHashSetWithExpectedSize(maxShardsPerEpoch());
        this.waitingOnData = waitingOnShards;
    }

    public ReadTracker(Topologies topologies)
    {
        this(topologies, INVALID);
    }

    @VisibleForTesting
    protected void recordInFlightRead(Id node)
    {
        if (!inflight.add(node))
            throw new IllegalStateException(node + " already in flight");

        recordResponse(this, node, ReadShardTracker::recordInFlightRead, false);
    }

    private boolean receiveResponseIsSlow(Id node)
    {
        if (!inflight.remove(node))
            throw new IllegalStateException("Nothing in flight for " + node);

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
     * Record a response that contributes to a potential quorum decision (i.e. accept once we have such a quorum)
     */
    protected RequestStatus recordQuorumReadSuccess(Id from)
    {
        // TODO this lambda is just binding from, maybe add from to the signature (TriFunction instead of BiFunction) so it can be a member
        // of ReadTracker?
        return recordResponse(from, (readShardTracker, isSlow) -> {
            // If the readCL doesn't do digest reads then ApproveIfQuorum
            // will never "haveData" mixed in with some of the responses
            // It might not actually be a digest read, but a read of Accord metadata
            // that is "insufficient"
            boolean isDigestReadOrInsufficient = true;
            if (readShardTracker.dataCL.requiresDigestReads)
            {
                nonNull(contactedToDataReadKeys, "contactedToDataReadKeys should have been initialized in trySendMore if the dataCL requires digest reads");
                // Null is all data reads, empty list is all digest, if any key intersects the shard
                // then all keys for the shard would be data reads
                List<RoutingKey> contactedDataReadKeys = contactedToDataReadKeys.get(from);
                if (contactedDataReadKeys != null)
                {
                    for (RoutingKey k : contactedDataReadKeys)
                        if (readShardTracker.shard.range.contains(k))
                        {
                            isDigestReadOrInsufficient = false;
                            break;
                        }
                }
                else
                {
                    // Reads after the first round are all data reads and
                    // won't put a list of keys into contactedToDataReadKeys
                    isDigestReadOrInsufficient = false;
                }
            }
            return readShardTracker.recordQuorumReadSuccess(isSlow, isDigestReadOrInsufficient);
        });
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

    public <T1> RequestStatus trySendMore(TriConsumer<T1, Id, RoutingKeys> contact, T1 with)
    {
        return trySendMore(contact, with, ImmutableListMultimap.of());
    }

    protected <T1> RequestStatus trySendMore(TriConsumer<T1, Id, RoutingKeys> contact, T1 with, @Nonnull ListMultimap<Shard, RoutingKey> shardToDataReadKeys)
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

                int numToContact = 1;
                if (!shardToDataReadKeys.isEmpty())
                    numToContact = tracker.quorumSize();
                tmp.set(i, numToContact);
            }
            toRead = tmp;
        }

        checkState(toRead != null, "We were asked to read more, but found no shards in need of reading more");

        // TODO (desired, consider): maybe for each additional candidate do one linear compare run to find better secondary match
        //       OR at least discount candidates that do not contribute additional knowledge beyond those additional
        //       candidates already contacted, since implementations are likely to sort primarily by health
        candidates.sort((a, b) -> topologies().compare(a, b, toRead));
        boolean isInitialRequestsWithDigestReads = !shardToDataReadKeys.isEmpty();
        if (isInitialRequestsWithDigestReads)
            contactedToDataReadKeys = new HashMap<>();
        int i = candidates.size() - 1;
        while (i >= 0)
        {
            Id candidate = candidates.get(i);

            topologies().forEach((topology, ti) -> {
                int offset = topologyOffset(ti);
                // TODO does this nested lambda allocate for each topology even though what it binds doesn't change?
                topology.forEachOn(candidate, (shard, shardIndex) -> {
                    if (!shardToDataReadKeys.isEmpty())
                        contactedToDataReadKeys.computeIfAbsent(candidate, id -> new ArrayList<>(3)).addAll(shardToDataReadKeys.removeAll(shard));
                    toRead.decrementSkipZero(offset + shardIndex);
                });
            });

            // Need to put an empty list here to indicate the reads were
            // digest reads, because no entry means all data reads
            if (isInitialRequestsWithDigestReads)
                contactedToDataReadKeys.computeIfAbsent(candidate, id -> ImmutableList.of());

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
            contact.consume(with, candidate, RoutingKeys.of(contactedToDataReadKeys.getOrDefault(candidate, ImmutableList.of())));
            candidates.remove(j);
        }
        return RequestStatus.NoChange;
    }

    public boolean hasData()
    {
        return all(ReadShardTracker::hasDataOrSufficientResponse);
    }

    public boolean hasFailed()
    {
        return any(ReadShardTracker::hasFailed);
    }
}
