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

import accord.api.Result.PersistableResult;
import accord.api.RoutingKey;
import accord.coordinate.Infer.InvalidIf;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node.Id;
import accord.local.ExecutionContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.KnownMap.MinAndMaxKnown;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.KnownMap;
import accord.primitives.Status.Durability.HasDecision;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.WithQuorum;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
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
import static accord.coordinate.Infer.InvalidIf.IsInvalid;
import static accord.coordinate.Infer.InvalidIf.IsNotInvalid;
import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;
import static accord.messages.MessageType.StandardMessage.CHECK_STATUS_REQ;
import static accord.messages.MessageType.StandardMessage.CHECK_STATUS_RSP;
import static accord.primitives.Known.Definition.DefinitionKnown;
import static accord.primitives.Known.Definition.DefinitionUnknown;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownDeps.DepsUnknown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtKnown;
import static accord.primitives.Known.KnownExecuteAt.ExecuteAtProposed;
import static accord.primitives.Known.Nothing;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.Durability;

import accord.primitives.Known;
import accord.utils.async.Cancellable;

import static accord.primitives.Status.Durability.HasOutcome.None;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.NotDefined;
import static accord.primitives.Status.Durability.HasDecision.DurablyCommitted;
import static accord.primitives.Status.Durability.HasDecision.DurablyStable;
import static accord.primitives.Status.Durability.HasDecision.FastPathDecided;
import static accord.primitives.Status.Stable;
import static accord.primitives.Status.Truncated;
import static accord.primitives.WithQuorum.HasQuorum;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

public class CheckStatus extends ParticipantsRequest<Participants<?>, CheckStatus.CheckStatusReply>
        implements Request, ExecutionContext, MapReduceConsume<SafeCommandStore, CheckStatus.CheckStatusReply>
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
                                             PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, PersistableResult result)
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
    public final Participants<?> scope;
    public final IncludeInfo includeInfo;
    // if set, simply ensure the ballot on the command is equal or greater to this ballot
    public final @Nullable Ballot bumpBallot;

    public CheckStatus(TxnId txnId, Participants<?> scope, long sourceEpoch, IncludeInfo includeInfo, @Nullable Ballot bumpBallot)
    {
        super(txnId, scope, sourceEpoch);
        this.bumpBallot = bumpBallot;
        Invariants.require(txnId.is(scope.domain()));
        this.scope = scope;
        this.includeInfo = includeInfo;
    }

    public CheckStatus(Id to, Topologies topologies, TxnId txnId, Participants<?> scope, long sourceEpoch, IncludeInfo includeInfo, Ballot bumpBallot)
    {
        super(txnId, scope, sourceEpoch);
        this.bumpBallot = bumpBallot;
        if (isRoute(scope)) this.scope = computeScope(to, topologies, castToRoute(scope), 0, Route::overlapping, Route::with);
        else this.scope = computeScope(to, topologies, (Participants) scope, 0, Participants::overlapping, Participants::with);
        this.includeInfo = includeInfo;
    }

    @Override
    public Cancellable submit()
    {
        // TODO (expected): only contact sourceEpoch
        return node.commandStores().mapReduceConsume(txnId.epoch(), waitForEpoch, this);
    }

    @Override
    public CheckStatusReply applyInternal(SafeCommandStore safeStore)
    {
        StoreParticipants participants = StoreParticipants.read(safeStore, scope, txnId, waitForEpoch);
        SafeCommand safeCommand = safeStore.get(txnId, participants);
        Command command = safeCommand.current();

        Commands.supplementParticipants(safeStore, safeCommand, participants);
        if (bumpBallot != null && bumpBallot.compareTo(command.promised()) > 0)
            safeCommand.updatePromised(bumpBallot);

        // for the moment we use this flag only to report that the initial coordinator is still active since we cannot infer progress by ballot;
        // for all future attempts we rely on witnessing a new ballot and using its age to decide when we should attempt to take over
        boolean isCoordinating = txnId.node.equals(node.id()) && command.promised().equals(Ballot.ZERO) && node.isCoordinatingWithBallot(txnId, Ballot.ZERO);
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

    private KnownMap foundKnown(Command command, StoreParticipants query)
    {
        SaveStatus saveStatus = command.saveStatus();
        if (command.participants().isPureOwns() || (!saveStatus.known.deps().hasProposedOrDecidedDeps() && saveStatus.known.definition() != DefinitionKnown))
        {
            Participants<?> validFor = query.owns();
            if (saveStatus != SaveStatus.Erased) // no StoreParticipants for Erased commands
                validFor = validFor.intersecting(command.participants().owns(), Minimal);

            KnownMap result = KnownMap.create(validFor, saveStatus.known);
            // TODO (formalise): consider and test this case more carefully - should we reply null for minOwned? Should we explicitly handle truncated states?
            if (validFor != query.owns())
                result = KnownMap.merge(result, KnownMap.create(query.owns().without(validFor), saveStatus.known.validForAll()));
            return result;
        }

        Known known = saveStatus.known;
        KnownMap result = KnownMap.EMPTY;
        if (known.deps().hasProposedOrDecidedDeps())
        {
            Invariants.require(command.participants().touches().containsAll(command.partialDeps().covering));
            result = KnownMap.create(command.partialDeps().covering, new MinAndMaxKnown(null, Nothing.with(saveStatus.known.deps())));
            known = known.with(DepsUnknown);
        }
        if (known.definition() == DefinitionKnown && !txnId.isSystemTxn())
        {
            if (command.partialTxn() != null)
            {
                Participants<?> participants = command.partialTxn().keys().toParticipants();
                Invariants.require(command.participants().owns().containsAll(participants));
                result = KnownMap.merge(result, KnownMap.create(participants, new MinAndMaxKnown(null, Known.DefinitionOnly)));
            }
            else Invariants.require(command.participants().stillOwns().isEmpty());
            known = known.with(DefinitionUnknown);
        }
        // TODO (expected): consider this case more carefully - should we reply null for minOwned? Should we explicitly handle truncated states?
        result = KnownMap.merge(result, KnownMap.create(query.owns(), known));
        return result;
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
        if (failure != null) node.reply(replyTo, replyContext, ok, failure, tracing());
        else if (ok == null) node.reply(replyTo, replyContext, CheckStatusNack.NotOwned, null, tracing());
        else node.reply(replyTo, replyContext, ok, null, tracing());
    }

    public interface CheckStatusReply extends Reply
    {
        boolean isOk();
    }
    
    public static class CheckStatusOk implements CheckStatusReply
    {
        public final KnownMap map;
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
        // TODO (expected): convert to committedExecuteAt if safe to do so; then null if not 'known'
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
            return new ProgressToken(durability.allShardsOrInvalidated(), status, maxPromised, maxAcceptedOrCommitted);
        }

        public Timestamp executeAtIfKnown()
        {
            if (maxKnown().isExecuteAtKnown())
                return executeAt;
            return null;
        }

        public CheckStatusOk finish(Unseekables<?> queried, Unseekables<?> requestedFor, Unseekables<?> routeOrParticipants, WithQuorum withQuorum, InvalidIf previouslyKnownToBeInvalidIf)
        {
            CheckStatusOk finished = this;

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
            if (withQuorum == HasQuorum)
            {
                Known minKnown = finished.minKnown(queried),
                      minMaxKnown = finished.minMaxKnown(queried),
                      maxKnown = finished.maxKnown(queried);
                {
                    HasOutcome addShard = HasOutcome.max(minKnown.outcome().isOrWasApply() ? Quorum : None, finished.durability.shard());
                    boolean upgradeAll = Route.isFullRoute(finished.route) && queried.containsAll(finished.route);
                    HasOutcome addAllShards = None;
                    HasDecision addDecision = HasDecision.None;
                    if (upgradeAll)
                    {
                        addAllShards = addShard;
                        if (minKnown.is(DepsKnown)) addDecision = DurablyStable;
                        else if (minKnown.is(ExecuteAtKnown)) addDecision = DurablyCommitted;
                        else if (minKnown.is(ExecuteAtProposed)) addDecision = FastPathDecided;
                    }
                    finished = finished.merge((Durability.get(addDecision, addShard, addAllShards, minKnown.isInvalidated())));
                }
                // TODO (required): should we require that we contacted the coordination epoch?
                if (invalidIf == IfUncommitted || previouslyKnownToBeInvalidIf == IfUncommitted)
                {
                    InvalidIf invalidIf = this.invalidIf.inferWithQuorum(minMaxKnown, maxKnown);
                    invalidIf = invalidIf.inferWithNewQuorum(previouslyKnownToBeInvalidIf, minMaxKnown);
                    if (invalidIf == IsInvalid)
                        validForAll = validForAll.atLeast(Known.Invalidated);
                    finished = finished.with(invalidIf);
                }
            }

            return finished.with(map.with(validForAll));
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
            durability = durability.mergeMax(this.durability);
            if (durability == this.durability)
                return this;
            return new CheckStatusOk(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, route, homeKey, invalidIf);
        }

        CheckStatusOk with(InvalidIf invalidIf)
        {
            if (invalidIf == this.invalidIf)
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
            Invariants.require(!known.hasFullRoute() || Route.isFullRoute(route));
            Invariants.require(!known.outcome().isInvalidated() || (!maxKnowledgeSaveStatus.known.isDecidedToExecute() && !maxSaveStatus.known.isDecidedToExecute()));
            Invariants.require(!(maxSaveStatus.known.outcome().isInvalidated() || maxKnowledgeSaveStatus.known.outcome().isInvalidated()) || !known.isDecidedToExecute());
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
                   ", maxNotTruncatedSaveStatus:" + maxKnowledgeSaveStatus +
                   ", maxSaveStatus:" + maxSaveStatus +
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
                Invariants.require(that.preferSelf(this));
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
            CheckStatusOk maxExecuteAt = prefer.maxKnown().executeAt().compareTo(defer.maxKnown().executeAt()) >= 0 ? prefer : defer;
            Route<?> mergedRoute = Route.merge(prefer.route, (Route)defer.route);
            Durability mergedDurability = prefer.durability.mergeShardsOrReplicas(defer.durability);
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
            return map.foldl(MinAndMaxKnown::nonNullOrMax, Nothing);
        }

        public Known maxKnown(Unseekables<?> query)
        {
            return map.foldl(query, MinAndMaxKnown::nonNullOrMax, Nothing);
        }

        /**
         * The minimum of all maximum knowns, i.e. what is the least state we are able to reach for the intersecting shards
         */
        public Known minMaxKnown(Unseekables<?> query)
        {
            return map.foldlWithDefault(query, MinAndMaxKnown::nonNullOrMinMax, MinAndMaxKnown.Nothing, null);
        }

        /**
         * The minimum of all minimum knowns, i.e. what is the least state we are guaranteed to reach for the
         * intersecting shards if we have a quorum both times
         */
        public Known minKnown(Unseekables<?> query)
        {
            Known known = map.foldlWithDefault(query, MinAndMaxKnown::nonNullOrMin, MinAndMaxKnown.Nothing, null);
            return known == null ? Nothing : known;
        }

        public Known minMaxKnown(RoutingKey key)
        {
            return map.getOrDefault(key, MinAndMaxKnown.Nothing).max;
        }

        @Override
        public MessageType type()
        {
            return CHECK_STATUS_RSP;
        }
    }

    public static class CheckStatusOkFull extends CheckStatusOk
    {
        public final PartialTxn partialTxn;
        public final PartialDeps stableDeps; // only set if status >= Committed, so safe to merge
        public final Writes writes;
        public final PersistableResult result;

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
                                    RoutingKey homeKey, InvalidIf invalidIf, PartialTxn partialTxn, PartialDeps stableDeps, Writes writes, PersistableResult result)
        {
            super(map, maxNotTruncatedSaveStatus, maxSaveStatus, promised, maxAcceptedOrCommitted, acceptedOrCommitted,
                  executeAt, isCoordinating, durability, route, homeKey, invalidIf);
            this.partialTxn = partialTxn;
            this.stableDeps = stableDeps;
            this.writes = writes;
            this.result = result;
        }

        public CheckStatusOkFull finish(Unseekables<?> queried, Unseekables<?> requestedFor, Unseekables<?> routeOrParticipants, WithQuorum withQuorum, InvalidIf previouslyKnownToBeInvalidIf)
        {
            return (CheckStatusOkFull) super.finish(queried, requestedFor, routeOrParticipants, withQuorum, previouslyKnownToBeInvalidIf);
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
            durability = durability.mergeMax(this.durability);
            if (durability == this.durability)
                return this;
            return new CheckStatusOkFull(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                         executeAt, isCoordinating, durability, route, homeKey, invalidIf, partialTxn, stableDeps, writes, result);
        }

        CheckStatusOkFull with(InvalidIf invalidIf)
        {
            if (invalidIf == this.invalidIf)
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
            PersistableResult result = (fullMax.result != null ? fullMax : fullMin).result;

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
            Invariants.require(!known.hasDefinition() || txnId.isSystemTxn() || owns.isEmpty() || (partialTxn != null && partialTxn.covers(owns)));
            Invariants.require(!known.hasDecidedDeps() || (stableDeps != null && stableDeps.covers(touches)));
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
            return CHECK_STATUS_RSP;
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
        return CHECK_STATUS_REQ;
    }
}
