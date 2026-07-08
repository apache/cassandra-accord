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
import java.util.List;
import java.util.Objects;

import static accord.messages.MessageType.StandardMessage.BEGIN_INVALIDATE_REQ;
import static accord.messages.MessageType.StandardMessage.BEGIN_INVALIDATE_RSP;
import static accord.primitives.Route.castToFullRoute;
import static accord.primitives.Route.isFullRoute;
import static accord.utils.Functions.mapReduceNonNull;

public class BeginInvalidation extends ParticipantsRequest<Participants<?>, BeginInvalidation.InvalidateReply> implements Request, ExecutionContext
{
    public final Ballot ballot;

    public BeginInvalidation(Id to, Topologies topologies, TxnId txnId, Participants<?> participants, Ballot ballot)
    {
        super(txnId, participants.overlapping(topologies.computeRangesForNode(to)), txnId.epoch());
        this.ballot = ballot;
    }

    public BeginInvalidation(TxnId txnId, Participants<?> participants, Ballot ballot)
    {
        super(txnId, participants, txnId.epoch());
        this.ballot = ballot;
    }

    @Override
    public Cancellable submit()
    {
        return node.commandStores().mapReduceConsume(txnId.epoch(), txnId.epoch(), this);
    }

    @Override
    public InvalidateReply applyInternal(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.notAccept(safeStore, scope, txnId);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();
        Ballot supersededBy;
        Participants<?> truncated;
        if (command.is(Status.Truncated))
        {
            supersededBy = null;
            truncated = participants.owns();
        }
        else
        {
            boolean promised = Commands.preacceptInvalidate(safeStore, safeCommand, ballot);
            supersededBy = promised ? null : safeCommand.current().promised();
            truncated = null;
        }

        SaveStatus saveStatus = command.saveStatus();
        // We don't respond with Vestigial as it breaks the logic (as of this commit) that aborts an invalidate if there exists a truncated reply
        //  but vestigial truncations don't imply anything about a decision of the command, so we can encounter a fully undecided command
        //  with some shard that doesn't actually own the command (but participates in the invalidation) and incorrectly determine it's unsafe to invalidate
        //  because the decision may have been erased.
        // TODO (required): move to KnownMap so we can simply check if any key we consulted is undecided.
        if (saveStatus == SaveStatus.Vestigial)
            saveStatus = SaveStatus.NotDefined;
        boolean acceptedFastPath = BeginRecovery.acceptsFastPath(txnId, participants, saveStatus, command.executeAt());
        return new InvalidateReply(supersededBy, command.acceptedOrCommitted(), saveStatus, acceptedFastPath, command.route(), command.homeKey(), truncated);
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
    public MessageType type()
    {
        return BEGIN_INVALIDATE_REQ;
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
            return maxKnowledgeStatus.known.executeAt().hasDecision();
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
            return BEGIN_INVALIDATE_RSP;
        }

        public static FullRoute<?> findRoute(List<InvalidateReply> invalidateOks)
        {
            for (InvalidateReply ok : invalidateOks)
            {
                if (ok != null && isFullRoute(ok.route))
                    return castToFullRoute(ok.route);
            }
            return null;
        }

        public static Route<?> mergeRoutes(List<InvalidateReply> invalidateOks)
        {
            return mapReduceNonNull(ok -> (Route)ok.route, Route::with, invalidateOks);
        }

        public static InvalidateReply max(List<InvalidateReply> invalidateReplies, Shard shard, SortedList<Id> nodeIds)
        {
            return SaveStatus.maxOfList(nodeIds.lazySelect(invalidateReplies, shard.nodes), r -> r.maxStatus, r -> r.accepted, Objects::nonNull);
        }

        public static InvalidateReply max(List<InvalidateReply> invalidateReplies)
        {
            return SaveStatus.maxOfList(invalidateReplies, r -> r.maxStatus, r -> r.accepted, Objects::nonNull);
        }

        public static InvalidateReply maxNotTruncated(List<InvalidateReply> invalidateReplies)
        {
            return SaveStatus.maxOfList(invalidateReplies, r -> r.maxKnowledgeStatus, r -> r.accepted, r -> r != null && !r.maxKnowledgeStatus.is(Status.Truncated));
        }
    }
}
