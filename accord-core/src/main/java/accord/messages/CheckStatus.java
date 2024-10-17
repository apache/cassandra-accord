/*
 * Licensed to the Apache Software ation (ASF) under one
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

import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Infer.InvalidIf;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.KnownMap;
import accord.primitives.WithQuorum;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import javax.annotation.Nonnull;

import static accord.coordinate.Infer.InvalidIf.IfUncommitted;
import static accord.coordinate.Infer.InvalidIf.IsNotInvalid;
import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;
import static accord.primitives.Status.Durability;
import static accord.primitives.Status.Durability.Local;
import static accord.primitives.Status.Durability.Majority;
import static accord.primitives.Status.Durability.ShardUniversal;
import static accord.primitives.Status.Durability.Universal;

import accord.primitives.Known;
import accord.utils.async.Cancellable;

import static accord.primitives.Status.NotDefined;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.messages.TxnRequest.computeScope;
import static accord.primitives.WithQuorum.HasQuorum;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

public class CheckStatus extends AbstractRequest<CheckStatus.CheckStatusReply>
        implements Request, PreLoadContext, MapReduceConsume<SafeCommandStore, CheckStatus.CheckStatusReply>
{
    public static class SerializationSupport
    {
        public static CheckStatusOk createOk(KnownMap map, SaveStatus maxKnowledgeStatus, SaveStatus maxStatus,
                                             Ballot promised, Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted,
                                             @Nullable Timestamp executeAt, boolean isCoordinating, Durability durability,
                                             @Nullable Route<?> route, @Nullable RoutingKey homeKey, InvalidIf invalidIf)
        {
            return new CheckStatusOk(map, maxKnowledgeStatus, maxStatus, promised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, route, homeKey, invalidIf);
        }
        public static CheckStatusOk createOk(KnownMap map, SaveStatus maxKnowledgeStatus, SaveStatus maxStatus,
                                             Ballot promised, Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted,
                                             @Nullable Timestamp executeAt, boolean isCoordinating, Durability durability,
                                             @Nullable Route<?> route, @Nullable RoutingKey homeKey, InvalidIf invalidIf,
                                             PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            return new CheckStatusOkFull(map, maxKnowledgeStatus, maxStatus, promised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                         executeAt, isCoordinating, durability, route, homeKey, invalidIf, partialTxn, committedDeps, writes, result);
        }
    }

    // order is important
    public enum IncludeInfo
    {
        No, Route, All
    }

    // query is usually a Route
    public final Participants<?> query;
    public final long sourceEpoch;
    public final IncludeInfo includeInfo;

    public CheckStatus(TxnId txnId, Participants<?> query, long sourceEpoch, IncludeInfo includeInfo)
    {
        super(txnId);
        Invariants.checkState(txnId.is(query.domain()));
        this.query = query;
        this.sourceEpoch = sourceEpoch;
        this.includeInfo = includeInfo;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    public CheckStatus(Id to, Topologies topologies, TxnId txnId, Participants<?> query, long sourceEpoch, IncludeInfo includeInfo)
    {
        super(txnId);
        if (isRoute(query)) this.query = computeScope(to, topologies, castToRoute(query), 0, Route::slice, Route::with);
        else this.query = computeScope(to, topologies, (Participants) query, 0, Participants::slice, Participants::with);
        this.sourceEpoch = sourceEpoch;
        this.includeInfo = includeInfo;
    }

    @Override
    public Cancellable submit()
    {
        // TODO (expected): only contact sourceEpoch
        return node.mapReduceConsumeLocal(this, query, txnId.epoch(), sourceEpoch, this);
    }

    @Override
    public CheckStatusReply apply(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.read(safeStore, query, txnId, sourceEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();

        Commands.updateParticipants(safeStore, safeCommand, participants);

        boolean isCoordinating = isCoordinating(node, command);
        Durability durability = command.durability();
        // unsafe to augment durability using DurableBefore, as DurableBefore can in theory get ahead of RedundantBefore
        Route<?> route = command.route();
        KnownMap map = foundKnown(command, participants);
        InvalidIf invalidIf = invalidIf(command, participants);

        switch (includeInfo)
        {
            default: throw new IllegalStateException();
            case No:
            case Route:
                Route<?> respondWithRoute = includeInfo == IncludeInfo.No ? null : route;
                return new CheckStatusOk(map, isCoordinating, durability, respondWithRoute, invalidIf, command);
            case All:
                return new CheckStatusOkFull(map, isCoordinating, durability, invalidIf, command);
        }
    }

    private KnownMap foundKnown(Command command, StoreParticipants participants)
    {
        SaveStatus saveStatus = command.saveStatus();
        return KnownMap.create(participants.owns(), saveStatus.known);
    }

    private InvalidIf invalidIf(Command command, StoreParticipants participants)
    {
        SaveStatus saveStatus = command.saveStatus();
        InvalidIf invalidIf = NotKnownToBeInvalid;
        if (command.known().isDecidedToExecute())
            invalidIf = IsNotInvalid;
        else if (saveStatus == SaveStatus.Erased && !participants.owns().isEmpty())
            invalidIf = IfUncommitted;
        return invalidIf;
    }

    private static boolean isCoordinating(Node node, Command command)
    {
        return node.isCoordinating(command.txnId(), command.promised());
    }

    @Override
    public CheckStatusReply reduce(CheckStatusReply r1, CheckStatusReply r2)
    {
        if (r1.isOk() && r2.isOk())
            return ((CheckStatusOk)r1).merge((CheckStatusOk) r2);
        if (r1.isOk() != r2.isOk())
            return r1.isOk() ? r2 : r1;
        CheckStatusNack nack1 = (CheckStatusNack) r1;
        CheckStatusNack nack2 = (CheckStatusNack) r2;
        return nack1.compareTo(nack2) <= 0 ? nack1 : nack2;
    }

    @Override
    protected void acceptInternal(CheckStatusReply ok, Throwable failure)
    {
        if (failure != null) node.reply(replyTo, replyContext, ok, failure);
        else if (ok == null) node.reply(replyTo, replyContext, CheckStatusNack.NotOwned, null);
        else node.reply(replyTo, replyContext, ok, null);
    }

    public interface CheckStatusReply extends Reply
    {
        boolean isOk();
    }
    
    public static class CheckStatusOk implements CheckStatusReply
    {
        public final KnownMap map;
        // TODO (required): tighten up constraints here to ensure we only report truncated when the range is Durable
        // TODO (expected, cleanup): stop using saveStatus and maxSaveStatus - move to only Known
        //   care needed when merging Accepted and AcceptedInvalidate; might be easier to retain saveStatus only for merging these cases
        public final SaveStatus maxKnowledgeSaveStatus, maxSaveStatus;
        public final Ballot maxPromised;
        public final Ballot acceptedOrCommitted;
        /**
         * The maximum accepted or committed ballot.
         * Note that this is NOT safe to combine with maxSaveStatus or maxKnowledgeSaveStatus.
         * This is because we might see a higher accepted ballot on one shard, and a lower committed (but therefore higher status)
         * on another shard, and the combined maxKnowledgeSaveStatus,maxAcceptedOrCommitted will order itself incorrectly
         * with other commit records that in fact supersede the data we have.
         */
        public final Ballot maxAcceptedOrCommitted;
        // TODO (expected, cleanup): try convert to committedExecuteAt, so null if not 'known'
        public final @Nullable Timestamp executeAt; // not set if invalidating or invalidated
        public final boolean isCoordinating;
        public final Durability durability;
        public final @Nullable Route<?> route;
        public final @Nullable RoutingKey homeKey;
        public final InvalidIf invalidIf;

        public CheckStatusOk(KnownMap map, boolean isCoordinating, Durability durability, InvalidIf invalidIf, Command command)
        {
            this(map, isCoordinating, durability, command.route(), invalidIf, command);
        }

        public CheckStatusOk(KnownMap map, boolean isCoordinating, Durability durability, Route<?> route, InvalidIf invalidIf, Command command)
        {
            this(map, command.saveStatus(), command.promised(), command.acceptedOrCommitted(), command.acceptedOrCommitted(),
                 command.executeAt(), isCoordinating, durability, route, command.homeKey(), invalidIf);
        }

        private CheckStatusOk(KnownMap map, SaveStatus saveStatus, Ballot maxPromised,
                              Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted, @Nullable Timestamp executeAt,
                              boolean isCoordinating, Durability durability,
                              @Nullable Route<?> route, @Nullable RoutingKey homeKey, InvalidIf invalidIf)
        {
            this(map, saveStatus, saveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted, executeAt, isCoordinating, durability, route, homeKey, invalidIf);
        }

        private CheckStatusOk(KnownMap map, SaveStatus maxKnowledgeSaveStatus, SaveStatus maxSaveStatus, Ballot maxPromised, Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted,
                              @Nullable Timestamp executeAt, boolean isCoordinating, Durability durability,
                              @Nullable Route<?> route, @Nullable RoutingKey homeKey, InvalidIf invalidIf)
        {
            this.map = map;
            this.maxSaveStatus = maxSaveStatus;
            this.maxKnowledgeSaveStatus = maxKnowledgeSaveStatus;
            this.maxPromised = maxPromised;
            this.maxAcceptedOrCommitted = maxAcceptedOrCommitted;
            this.acceptedOrCommitted = acceptedOrCommitted;
            this.executeAt = executeAt;
            this.isCoordinating = isCoordinating;
            this.durability = durability;
            this.route = route;
            this.homeKey = homeKey;
            this.invalidIf = invalidIf;
        }

        public ProgressToken toProgressToken()
        {
            Status status = maxSaveStatus.status;
            return new ProgressToken(durability, status, maxPromised, maxAcceptedOrCommitted);
        }

        public Timestamp executeAtIfKnown()
        {
            if (maxKnown().executeAt.isDecidedAndKnownToExecute())
                return executeAt;
            return null;
        }

        public CheckStatusOk finish(Unseekables<?> routeOrParticipants, WithQuorum withQuorum)
        {
            CheckStatusOk finished = this;
            if (withQuorum == HasQuorum)
            {
                Durability durability = this.durability;
                if (durability == Local) durability = Majority;
                else if (durability == ShardUniversal) durability = Universal;
                finished = finished.merge(durability);
            }

            if (Route.isRoute(routeOrParticipants))
            {
                finished = finished.merge(Route.castToRoute(routeOrParticipants));
                routeOrParticipants = finished.route;
            }
            else
            {
                routeOrParticipants = Unseekables.merge(routeOrParticipants, (Unseekables) finished.route);
            }

            Known validForAll = map.computeValidForAll(routeOrParticipants);
            if (withQuorum == HasQuorum && invalidIf.inferInvalidWithQuorum(finished.maxKnown()))
                validForAll = validForAll.atLeast(Known.Invalidated);

            return finished.with(map.with(validForAll));
        }

        /**
         * NOTE: if the response is *incomplete* this does not detect possible truncation, it only indicates if the
         * combination of the responses we received represents truncation
         */
        public boolean isTruncatedResponse()
        {
            return map.hasTruncated();
        }

        public Ranges truncatedResponse()
        {
            return map.matchingRanges(Known::isTruncated);
        }

        public CheckStatusOk merge(@Nonnull Route<?> route)
        {
            Route<?> mergedRoute = Route.merge((Route)this.route, route);
            if (mergedRoute == this.route)
                return this;
            return new CheckStatusOk(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, mergedRoute, homeKey, invalidIf);
        }

        public CheckStatusOk merge(@Nonnull Durability durability)
        {
            durability = Durability.merge(durability, this.durability);
            if (durability == this.durability)
                return this;
            return new CheckStatusOk(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, route, homeKey, invalidIf);
        }

        CheckStatusOk with(@Nonnull KnownMap newMap)
        {
            if (newMap == this.map)
                return this;
            return new CheckStatusOk(newMap, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, route, homeKey, invalidIf);
        }

        // TODO (required): harden markShardStale against unnecessary actions by utilising inferInvalidated==MAYBE and performing a global query
        public Known knownFor(TxnId txnId, Unseekables<?> owns, Unseekables<?> touches)
        {
            Known known = map.knownFor(owns, touches);
            Invariants.checkState(!known.hasFullRoute() || Route.isFullRoute(route));
            Invariants.checkState(!known.outcome.isInvalidated() || (!maxKnowledgeSaveStatus.known.isDecidedToExecute() && !maxSaveStatus.known.isDecidedToExecute()));
            Invariants.checkState(!(maxSaveStatus.known.outcome.isInvalidated() || maxKnowledgeSaveStatus.known.outcome.isInvalidated()) || !known.isDecidedToExecute());
            // TODO (desired): make sure these match identically, rather than only ensuring Route.isFullRoute (either by coercing it here or by ensuring it at callers)
            return known;
        }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "map:" + map +
                   "maxNotTruncatedSaveStatus:" + maxKnowledgeSaveStatus +
                   "maxSaveStatus:" + maxSaveStatus +
                   ", promised:" + maxPromised +
                   ", accepted:" + maxAcceptedOrCommitted +
                   ", executeAt:" + executeAt +
                   ", durability:" + durability +
                   ", isCoordinating:" + isCoordinating +
                   ", route:" + route +
                   ", homeKey:" + homeKey +
                   '}';
        }

        boolean preferSelf(CheckStatusOk that)
        {
            if ((this.maxKnowledgeSaveStatus.is(Truncated) && !this.maxKnowledgeSaveStatus.is(NotDefined)) || (that.maxKnowledgeSaveStatus.is(Truncated) && !that.maxKnowledgeSaveStatus.is(NotDefined)))
                return this.maxKnowledgeSaveStatus.compareTo(that.maxKnowledgeSaveStatus) <= 0;

            return this.maxKnowledgeSaveStatus.compareTo(that.maxKnowledgeSaveStatus) >= 0;
        }

        public CheckStatusOk merge(CheckStatusOk that)
        {
            if (!preferSelf(that))
            {
                Invariants.checkState(that.preferSelf(this));
                return that.merge(this);
            }

            // preferentially select the one that is coordinating, if any
            CheckStatusOk prefer = this.isCoordinating ? this : that;
            CheckStatusOk defer = prefer == this ? that : this;

            // then select the max along each criteria, preferring the coordinator
            KnownMap mergeMap = KnownMap.merge(prefer.map, defer.map);
            CheckStatusOk maxStatus = SaveStatus.max(prefer, prefer.maxKnowledgeSaveStatus, prefer.acceptedOrCommitted, defer, defer.maxKnowledgeSaveStatus, defer.acceptedOrCommitted, true);
            SaveStatus mergeMaxKnowledgeStatus = SaveStatus.merge(prefer.maxKnowledgeSaveStatus, prefer.acceptedOrCommitted, defer.maxKnowledgeSaveStatus, defer.acceptedOrCommitted, true);
            SaveStatus mergeMaxStatus = SaveStatus.merge(prefer.maxSaveStatus, prefer.acceptedOrCommitted, defer.maxSaveStatus, defer.acceptedOrCommitted, false);
            CheckStatusOk maxPromised = prefer.maxPromised.compareTo(defer.maxPromised) >= 0 ? prefer : defer;
            CheckStatusOk maxAccepted = prefer.maxAcceptedOrCommitted.compareTo(defer.maxAcceptedOrCommitted) >= 0 ? prefer : defer;
            CheckStatusOk maxHomeKey = prefer.homeKey != null || defer.homeKey == null ? prefer : defer;
            CheckStatusOk maxExecuteAt = prefer.maxKnown().executeAt.compareTo(defer.maxKnown().executeAt) >= 0 ? prefer : defer;
            Route<?> mergedRoute = Route.merge(prefer.route, (Route)defer.route);
            Durability mergedDurability = Durability.merge(prefer.durability, defer.durability);
            InvalidIf invalidIf = prefer.invalidIf.atLeast(defer.invalidIf);

            // if the maximum (or preferred equal) is the same on all dimensions, return it
            if (mergeMaxKnowledgeStatus == maxStatus.maxKnowledgeSaveStatus
                && mergeMaxStatus == maxStatus.maxSaveStatus
                && maxStatus == maxPromised && maxStatus == maxAccepted
                && maxStatus == maxHomeKey && maxStatus == maxExecuteAt
                && maxStatus.route == mergedRoute
                && maxStatus.map.equals(mergeMap)
                && maxStatus.durability == mergedDurability)
            {
                return maxStatus;
            }

            // otherwise assemble the maximum of each, and propagate isCoordinating from the origin we selected the promise from
            boolean isCoordinating = maxPromised == prefer ? prefer.isCoordinating : defer.isCoordinating;
            return new CheckStatusOk(mergeMap, mergeMaxKnowledgeStatus, mergeMaxStatus,
                                     maxPromised.maxPromised, maxAccepted.maxAcceptedOrCommitted, maxStatus.acceptedOrCommitted,
                                     maxExecuteAt.executeAt, isCoordinating, mergedDurability, mergedRoute, maxHomeKey.homeKey, invalidIf);
        }

        public Known maxKnown()
        {
            return map.foldl(Known::atLeast, Known.Nothing, i -> false);
        }

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }
    }

    public static class CheckStatusOkFull extends CheckStatusOk
    {
        public final PartialTxn partialTxn;
        public final PartialDeps stableDeps; // only set if status >= Committed, so safe to merge
        public final Writes writes;
        public final Result result;

        public CheckStatusOkFull(KnownMap map, boolean isCoordinating, Durability durability, InvalidIf invalidIf, Command command)
        {
            super(map, isCoordinating, durability, invalidIf, command);
            this.partialTxn = command.partialTxn();
            this.stableDeps = command.status().compareTo(Stable) >= 0 ? command.partialDeps() : null;
            this.writes = command.writes();
            this.result = command.result();
        }

        protected CheckStatusOkFull(KnownMap map, SaveStatus maxNotTruncatedSaveStatus, SaveStatus maxSaveStatus, Ballot promised, Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted,
                                    Timestamp executeAt, boolean isCoordinating, Durability durability, Route<?> route,
                                    RoutingKey homeKey, InvalidIf invalidIf, PartialTxn partialTxn, PartialDeps stableDeps, Writes writes, Result result)
        {
            super(map, maxNotTruncatedSaveStatus, maxSaveStatus, promised, maxAcceptedOrCommitted, acceptedOrCommitted,
                  executeAt, isCoordinating, durability, route, homeKey, invalidIf);
            this.partialTxn = partialTxn;
            this.stableDeps = stableDeps;
            this.writes = writes;
            this.result = result;
        }

        public CheckStatusOkFull finish(Unseekables<?> unseekables, WithQuorum withQuorum)
        {
            return (CheckStatusOkFull) super.finish(unseekables, withQuorum);
        }

        public CheckStatusOkFull merge(@Nonnull Route<?> route)
        {
            Route<?> mergedRoute = Route.merge((Route)this.route, route);
            if (mergedRoute == this.route)
                return this;
            return new CheckStatusOkFull(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                         executeAt, isCoordinating, durability, mergedRoute, homeKey, invalidIf, partialTxn, stableDeps, writes, result);
        }

        public CheckStatusOkFull merge(@Nonnull Durability durability)
        {
            durability = Durability.merge(durability, this.durability);
            if (durability == this.durability)
                return this;
            return new CheckStatusOkFull(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                         executeAt, isCoordinating, durability, route, homeKey, invalidIf, partialTxn, stableDeps, writes, result);
        }

        CheckStatusOk with(@Nonnull KnownMap newMap)
        {
            if (newMap == this.map)
                return this;
            return new CheckStatusOkFull(newMap, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                         executeAt, isCoordinating, durability, route, homeKey, invalidIf, partialTxn, stableDeps, writes, result);
        }

        /**
         * This method assumes parameter is of the same type and has the same additional info (modulo partial replication).
         * If parameters have different info, it is undefined which properties will be returned.
         *
         * This method is NOT guaranteed to return CheckStatusOkFull unless the parameter is also CheckStatusOkFull.
         * This method is NOT guaranteed to return either parameter: it may merge the two to represent the maximum
         * combined info, (and in this case if the parameter were not CheckStatusOkFull, and were the higher status
         * reply, the info would potentially be unsafe to act upon when given a higher status
         * (e.g. Accepted executeAt is very different to Committed executeAt))
         */
        @Override
        public CheckStatusOk merge(CheckStatusOk that)
        {
            CheckStatusOk max = super.merge(that);
            CheckStatusOk maxSrc = preferSelf(that) ? this : that;
            if (!(maxSrc instanceof CheckStatusOkFull))
                return max;

            CheckStatusOkFull fullMax = (CheckStatusOkFull) maxSrc;
            CheckStatusOk minSrc = maxSrc == this ? that : this;
            if (!(minSrc instanceof CheckStatusOkFull))
            {
                return new CheckStatusOkFull(max.map, max.maxKnowledgeSaveStatus, max.maxSaveStatus,
                                             max.maxPromised, max.maxAcceptedOrCommitted, max.acceptedOrCommitted,
                                             fullMax.executeAt, max.isCoordinating, max.durability, max.route,
                                             max.homeKey, max.invalidIf, fullMax.partialTxn, fullMax.stableDeps, fullMax.writes, fullMax.result);
            }

            CheckStatusOkFull fullMin = (CheckStatusOkFull) minSrc;

            PartialTxn partialTxn = PartialTxn.merge(fullMax.partialTxn, fullMin.partialTxn);
            PartialDeps committedDeps;
            if (fullMax.stableDeps == null) committedDeps = fullMin.stableDeps;
            else if (fullMin.stableDeps == null) committedDeps = fullMax.stableDeps;
            else committedDeps = fullMax.stableDeps.with(fullMin.stableDeps);
            Writes writes = (fullMax.writes != null ? fullMax : fullMin).writes;
            Result result = (fullMax.result != null ? fullMax : fullMin).result;

            return new CheckStatusOkFull(max.map, max.maxKnowledgeSaveStatus, max.maxSaveStatus,
                                         max.maxPromised, max.maxAcceptedOrCommitted, max.acceptedOrCommitted,
                                         max.executeAt, max.isCoordinating, max.durability, max.route,
                                         max.homeKey, max.invalidIf, partialTxn, committedDeps, writes, result);
        }

        /**
         * Reduce what is Known about all intersecting shards into a summary Known. This will be the maximal knowledge
         * we have, i.e. if we have some outcome/decision on one shard but it is truncated on another intersecting shard,
         * we will get the outcome/decision; if we only have it truncated on one shard and unknown on another, it will
         * be shown as truncated.
         *
         * If a non-intersecting shard has information that can be propagated to this shard, i.e. the executeAt or outcome,
         * then this will be merged as though it were an intersecting shard, however no record of truncation will be so propagated,
         * nor any knowledge that does not transfer (i.e. Definition or Deps).
         */
        @Override
        public Known knownFor(TxnId txnId, Unseekables<?> owns, Unseekables<?> touches)
        {
            Known known = super.knownFor(txnId, owns, touches);
            Invariants.checkState(!known.hasDefinition() || txnId.isSystemTxn() || (partialTxn != null && partialTxn.covers(owns)));
            Invariants.checkState(!known.hasDecidedDeps() || (stableDeps != null && stableDeps.covers(touches)));
            return known;
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "map:" + map +
                   ", maxSaveStatus:" + maxSaveStatus +
                   ", promised:" + maxPromised +
                   ", accepted:" + maxAcceptedOrCommitted +
                   ", executeAt:" + executeAt +
                   ", durability:" + durability +
                   ", isCoordinating:" + isCoordinating +
                   ", deps:" + stableDeps +
                   ", writes:" + writes +
                   ", result:" + result +
                   '}';
        }
    }

    public enum CheckStatusNack implements CheckStatusReply
    {
        NotOwned;

        @Override
        public MessageType type()
        {
            return MessageType.CHECK_STATUS_RSP;
        }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "CheckStatusNack{" + name() + '}';
        }
    }

    @Override
    public String toString()
    {
        return "CheckStatus{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.CHECK_STATUS_REQ;
    }

    @Override
    public long waitForEpoch()
    {
        return sourceEpoch;
    }
}
