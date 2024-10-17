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

package accord.messages;

import accord.api.RoutingKey;
import accord.local.*;
import accord.local.Node.Id;
import accord.primitives.*;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.SortedList;
import accord.utils.async.Cancellable;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

import static accord.primitives.Route.castToFullRoute;
import static accord.primitives.Route.isFullRoute;
import static accord.utils.Functions.mapReduceNonNull;

public class BeginInvalidation extends AbstractRequest<BeginInvalidation.InvalidateReply> implements Request, PreLoadContext
{
    public final Ballot ballot;
    public final Participants<?> participants;

    public BeginInvalidation(Id to, Topologies topologies, TxnId txnId, Participants<?> participants, Ballot ballot)
    {
        super(txnId);
        this.participants = participants.slice(topologies.computeRangesForNode(to));
        this.ballot = ballot;
    }

    public BeginInvalidation(TxnId txnId, Participants<?> participants, Ballot ballot)
    {
        super(txnId);
        this.participants = participants;
        this.ballot = ballot;
    }

    @Override
    public Cancellable submit()
    {
        return node.mapReduceConsumeLocal(this, participants, txnId.epoch(), txnId.epoch(), this);
    }

    @Override
    public InvalidateReply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.invalidate(safeStore, this.participants, txnId);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();
        boolean acceptedFastPath;
        Ballot supersededBy;
        Participants<?> truncated;
        if (command.is(Status.Truncated))
        {
            acceptedFastPath = false;
            supersededBy = null;
            truncated = participants.owns();
        }
        else
        {
            boolean promised = Commands.preacceptInvalidate(safeCommand, ballot);
            acceptedFastPath = command.executeAt() != null && command.executeAt().equals(command.txnId());
            supersededBy = promised ? null : safeCommand.current().promised();
            truncated = null;
        }
        return new InvalidateReply(supersededBy, command.acceptedOrCommitted(), command.saveStatus(), acceptedFastPath, command.route(), command.homeKey(), truncated);
    }

    @Override
    public InvalidateReply reduce(InvalidateReply o1, InvalidateReply o2)
    {
        // since the coordinator treats a node's response as a collective answer for the keys it owns
        // we can safely take any reject from one key as a reject for the whole node
        // unfortunately we must also treat the promise rejection as pan-node, even though we only need
        // a single key to accept a promise globally for the invalidation to be able to succeed
        Ballot supersededBy = Ballot.nonNullOrMax(o1.supersededBy, o2.supersededBy);
        boolean acceptedFastPath = o1.acceptedFastPath && o2.acceptedFastPath;
        Route<?> route =  Route.merge((Route)o1.route, o2.route);
        Participants<?> truncated =  Participants.merge((Participants) o1.truncated, o2.truncated);
        RoutingKey homeKey = o1.homeKey != null ? o1.homeKey : o2.homeKey != null ? o2.homeKey : null;
        InvalidateReply maxStatus = SaveStatus.max(o1, o1.maxStatus, o1.accepted, o2, o2.maxStatus, o2.accepted, false);
        InvalidateReply maxKnowledgeStatus = SaveStatus.max(o1, o1.maxKnowledgeStatus, o1.accepted, o2, o2.maxKnowledgeStatus, o2.accepted, true);
        return new InvalidateReply(supersededBy, maxStatus.accepted, maxStatus.maxStatus, maxKnowledgeStatus.maxKnowledgeStatus, acceptedFastPath, truncated, route, homeKey);
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    @Override
    public long waitForEpoch()
    {
        return txnId.epoch();
    }

    @Override
    public MessageType type()
    {
        return MessageType.BEGIN_INVALIDATE_REQ;
    }

    @Override
    public String toString()
    {
        return "BeginInvalidate{" +
               "txnId:" + txnId +
               ", ballot:" + ballot +
               '}';
    }

    public static class InvalidateReply implements Reply
    {
        public final @Nullable Ballot supersededBy;
        public final Ballot accepted;
        public final SaveStatus maxStatus, maxKnowledgeStatus;
        public final boolean acceptedFastPath;
        public final @Nullable Participants<?> truncated;
        public final @Nullable Route<?> route;
        public final @Nullable RoutingKey homeKey;

        public InvalidateReply(@Nullable Ballot supersededBy, Ballot accepted, SaveStatus status, boolean acceptedFastPath, @Nullable Route<?> route, @Nullable RoutingKey homeKey, Participants<?> truncated)
        {
            this(supersededBy, accepted, status, status, acceptedFastPath, truncated, route, homeKey);
        }

        public InvalidateReply(@Nullable Ballot supersededBy, Ballot accepted, SaveStatus maxStatus, SaveStatus maxKnowledgeStatus, boolean acceptedFastPath, @Nullable Participants<?> truncated, @Nullable Route<?> route, @Nullable RoutingKey homeKey)
        {
            this.supersededBy = supersededBy;
            this.accepted = accepted;
            this.maxStatus = maxStatus;
            this.maxKnowledgeStatus = maxKnowledgeStatus;
            this.acceptedFastPath = acceptedFastPath;
            this.truncated = truncated;
            this.route = route;
            this.homeKey = homeKey;
        }

        public boolean hasDecision()
        {
            return maxKnowledgeStatus.known.executeAt.hasDecision();
        }

        public boolean isPromiseRejected()
        {
            return supersededBy != null;
        }

        public boolean isTruncated()
        {
            return truncated != null;
        }

        public boolean isPromisedOrPartPromised()
        {
            return supersededBy == null;
        }

        public boolean isPartPromised()
        {
            return isPromisedOrPartPromised() && isTruncated();
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            InvalidateReply that = (InvalidateReply) o;
            return acceptedFastPath == that.acceptedFastPath && Objects.equals(supersededBy, that.supersededBy) && Objects.equals(accepted, that.accepted) && maxStatus == that.maxStatus && maxKnowledgeStatus == that.maxKnowledgeStatus && Objects.equals(route, that.route) && Objects.equals(homeKey, that.homeKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(supersededBy, accepted, maxStatus, acceptedFastPath, route, homeKey);
        }

        @Override
        public String toString()
        {
            String description = isPromiseRejected() ? "Rejected{" + supersededBy + ","
                                                     : isPartPromised() ? "PartPromised"
                                                                        : isTruncated() ? "Truncated"
                                                                                        : "Promised";
            return "Invalidate" + description + maxStatus + ',' + maxKnowledgeStatus + ',' + (route != null ? route: homeKey) + '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.BEGIN_INVALIDATE_RSP;
        }

        public static FullRoute<?> findRoute(InvalidateReply[] invalidateOks)
        {
            for (InvalidateReply ok : invalidateOks)
            {
                if (ok != null && isFullRoute(ok.route))
                    return castToFullRoute(ok.route);
            }
            return null;
        }

        public static Route<?> mergeRoutes(InvalidateReply[] invalidateOks)
        {
            return mapReduceNonNull(ok -> (Route)ok.route, Route::with, invalidateOks);
        }

        public static InvalidateReply max(InvalidateReply[] invalidateReplies, Shard shard, SortedList<Id> nodeIds)
        {
            return SaveStatus.max(nodeIds.select(invalidateReplies, shard.nodes), r -> r.maxStatus, r -> r.accepted, Objects::nonNull, false);
        }

        public static InvalidateReply max(InvalidateReply[] invalidateReplies)
        {
            return SaveStatus.max(Arrays.asList(invalidateReplies),r -> r.maxStatus, r -> r.accepted, Objects::nonNull, false);
        }

        public static InvalidateReply maxNotTruncated(InvalidateReply[] invalidateReplies)
        {
            return SaveStatus.max(Arrays.asList(invalidateReplies),r -> r.maxKnowledgeStatus, r -> r.accepted, r -> r != null && !r.maxKnowledgeStatus.is(Status.Truncated), true);
        }

        public static int countTruncated(InvalidateReply[] invalidateReplies, Shard shard, SortedList<Id> nodeIds)
        {
            int count = 0;
            for (InvalidateReply reply : nodeIds.select(invalidateReplies, shard.nodes))
            {
                if (reply != null && reply.maxStatus.is(Status.Truncated))
                    ++count;
            }
            return count;
        }
    }
}
