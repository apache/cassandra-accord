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

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Infer;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status;
import accord.primitives.Ballot;
import accord.primitives.EpochSupplier;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
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
import javax.annotation.Nullable;

import static accord.local.Status.Committed;
import static accord.local.Status.Definition;
import static accord.local.Status.Durability;
import static accord.local.Status.Known;
import static accord.local.Status.KnownDeps;
import static accord.local.Status.KnownExecuteAt;
import static accord.local.Status.NotDefined;
import static accord.local.Status.Outcome;
import static accord.local.Status.Phase;
import static accord.local.Status.Truncated;
import static accord.messages.TxnRequest.computeScope;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

public class CheckStatus extends AbstractEpochRequest<CheckStatus.CheckStatusReply>
        implements Request, PreLoadContext, MapReduceConsume<SafeCommandStore, CheckStatus.CheckStatusReply>, EpochSupplier
{
    public enum WithQuorum { HasQuorum, NoQuorum }

    public static class SerializationSupport
    {
        public static CheckStatusOk createOk(Status invalidIfNotAtLeast, SaveStatus status, SaveStatus maxStatus, Ballot promised, Ballot accepted, @Nullable Timestamp executeAt,
                                             boolean isCoordinating, Durability durability,
                                             @Nullable Route<?> route, @Nullable RoutingKey homeKey)
        {
            return new CheckStatusOk(invalidIfNotAtLeast, status, maxStatus, promised, accepted, executeAt, isCoordinating, durability, route, homeKey);
        }
        public static CheckStatusOk createOk(Status invalidIfNotAtLeast, SaveStatus status, SaveStatus maxStatus, Ballot promised, Ballot accepted, @Nullable Timestamp executeAt,
                                             boolean isCoordinating, Durability durability,
                                             @Nullable Route<?> route, @Nullable RoutingKey homeKey,
                                             PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            return new CheckStatusOkFull(invalidIfNotAtLeast, status, maxStatus, promised, accepted, executeAt, isCoordinating, durability, route, homeKey,
                                         partialTxn, committedDeps, writes, result);
        }
    }

    // order is important
    public enum IncludeInfo
    {
        No, Route, All
    }

    // query is usually a Route
    public final Unseekables<?> query;
    public final long sourceEpoch;
    public final IncludeInfo includeInfo;

    public CheckStatus(TxnId txnId, Unseekables<?> query, long sourceEpoch, IncludeInfo includeInfo)
    {
        super(txnId);
        this.query = query;
        this.sourceEpoch = sourceEpoch;
        this.includeInfo = includeInfo;
    }

    @Override
    public TxnId primaryTxnId()
    {
        return txnId;
    }

    public CheckStatus(Id to, Topologies topologies, TxnId txnId, Unseekables<?> query, long sourceEpoch, IncludeInfo includeInfo)
    {
        super(txnId);
        if (isRoute(query)) this.query = computeScope(to, topologies, castToRoute(query), 0, Route::slice, PartialRoute::union);
        else this.query = computeScope(to, topologies, (Unseekables) query, 0, Unseekables::slice, Unseekables::with);
        this.sourceEpoch = sourceEpoch;
        this.includeInfo = includeInfo;
    }

    @Override
    public void process()
    {
        // TODO (expected): only contact sourceEpoch
        node.mapReduceConsumeLocal(this, query, txnId.epoch(), sourceEpoch, this);
    }

    @Override
    public long epoch()
    {
        return sourceEpoch;
    }

    @Override
    public CheckStatusReply apply(SafeCommandStore safeStore)
    {
        SafeCommand safeCommand = safeStore.get(txnId, this, query);
        Command command = safeCommand.current();
        // TODO (expected): do we want to force ourselves to serialise these?
        if (!command.has(Known.DefinitionOnly) && Route.isRoute(query) && safeStore.ranges().allAt(txnId.epoch()).contains(Route.castToRoute(query).homeKey()))
            Commands.informHome(safeStore, safeCommand, Route.castToRoute(query));

        switch (includeInfo)
        {
            default: throw new IllegalStateException();
            case No:
            case Route:
                return new CheckStatusOk(invalidIfNotAtLeast(safeStore), command.saveStatus(), command.saveStatus(),
                                         command.promised(), command.accepted(), command.executeAt(),
                                         node.isCoordinating(txnId, command.promised()),
                                         command.durability(), includeInfo == IncludeInfo.No ? null : command.route(), command.homeKey());
            case All:
                return new CheckStatusOkFull(isCoordinating(node, command), invalidIfNotAtLeast(safeStore), command);
        }
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
    public void accept(CheckStatusReply ok, Throwable failure)
    {
        if (ok == null) node.reply(replyTo, replyContext, CheckStatusNack.NotOwned);
        else node.reply(replyTo, replyContext, ok);
    }

    private Status invalidIfNotAtLeast(SafeCommandStore safeStore)
    {
        return Infer.invalidIfNotAtLeast(safeStore, txnId, query);
    }

    public interface CheckStatusReply extends Reply
    {
        boolean isOk();
    }

    public static class CheckStatusOk implements CheckStatusReply
    {
        public final Status invalidIfNotAtLeast;
        public final SaveStatus saveStatus; // the maximum non-truncated status; or truncated if all responses are truncated
        public final SaveStatus maxSaveStatus;
        public final Ballot promised;
        public final Ballot accepted;
        public final @Nullable Timestamp executeAt; // not set if invalidating or invalidated
        public final boolean isCoordinating;
        public final Durability durability; // i.e. on all shards
        public final @Nullable Route<?> route;
        public final @Nullable RoutingKey homeKey;

        public CheckStatusOk(boolean isCoordinating, Status invalidIfNotAtLeast, Command command)
        {
            this(invalidIfNotAtLeast, command.saveStatus(), command.saveStatus(), command.promised(), command.accepted(), command.executeAt(),
                 isCoordinating, command.durability(), command.route(), command.homeKey());
        }

        private CheckStatusOk(Status invalidIfNotAtLeast, SaveStatus saveStatus, SaveStatus maxSaveStatus,
                              Ballot promised, Ballot accepted, @Nullable Timestamp executeAt,
                              boolean isCoordinating, Durability durability,
                              @Nullable Route<?> route, @Nullable RoutingKey homeKey)
        {
            this.invalidIfNotAtLeast = invalidIfNotAtLeast;
            this.saveStatus = saveStatus;
            this.maxSaveStatus = maxSaveStatus;
            this.promised = promised;
            this.accepted = accepted;
            this.executeAt = executeAt;
            this.isCoordinating = isCoordinating;
            this.durability = durability;
            this.route = route;
            this.homeKey = homeKey;
        }

        public ProgressToken toProgressToken()
        {
            Status status = maxSaveStatus.status;
            return new ProgressToken(durability, status, promised, accepted);
        }

        public boolean isTruncated()
        {
            return maxSaveStatus.phase == Phase.Cleanup;
        }

        public boolean inferInvalidated(WithQuorum withQuorum)
        {
            return Infer.inferInvalidated(withQuorum, invalidIfNotAtLeast, saveStatus, maxSaveStatus);
        }

        public Known inferredOrKnown(WithQuorum withQuorum)
        {
            if (inferInvalidated(withQuorum))
                return Known.Invalidated;

            return saveStatus.known;
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
                   "status:" + saveStatus +
                   ", promised:" + promised +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", durability:" + durability +
                   ", isCoordinating:" + isCoordinating +
                   ", route:" + route +
                   ", homeKey:" + homeKey +
                   '}';
        }

        boolean preferSelf(CheckStatusOk that)
        {
            if ((this.saveStatus.is(Truncated) && !this.saveStatus.is(NotDefined)) || (that.saveStatus.is(Truncated) && !that.saveStatus.is(NotDefined)))
                return this.saveStatus.compareTo(that.saveStatus) <= 0;

            return this.saveStatus.compareTo(that.saveStatus) >= 0;
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
            CheckStatusOk maxStatus = SaveStatus.max(prefer, prefer.saveStatus, prefer.accepted, defer, defer.saveStatus, defer.accepted, true);
            SaveStatus mergeStatus = SaveStatus.merge(prefer.saveStatus, prefer.accepted, defer.saveStatus, defer.accepted, true);
            SaveStatus mergeMaxStatus = SaveStatus.merge(prefer.saveStatus, prefer.accepted, defer.saveStatus, defer.accepted, false);
            CheckStatusOk maxPromised = prefer.promised.compareTo(defer.promised) >= 0 ? prefer : defer;
            CheckStatusOk maxAccepted = prefer.accepted.compareTo(defer.accepted) >= 0 ? prefer : defer;
            CheckStatusOk maxDurability = prefer.durability.compareTo(defer.durability) >= 0 ? prefer : defer;
            CheckStatusOk maxHomeKey = prefer.homeKey != null || defer.homeKey == null ? prefer : defer;
            CheckStatusOk maxExecuteAt = prefer.saveStatus.known.executeAt.compareTo(defer.saveStatus.known.executeAt) >= 0 ? prefer : defer;
            Route<?> mergedRoute = Route.merge(prefer.route, (Route)defer.route);
            Status mergedInvalidIfNotAtLeast = Status.simpleMax(prefer.invalidIfNotAtLeast, defer.invalidIfNotAtLeast);

            // if the maximum (or preferred equal) is the same on all dimensions, return it
            if (mergeStatus == maxStatus.saveStatus && maxStatus == maxPromised && maxStatus == maxAccepted && maxStatus == maxDurability
                && maxStatus.route == mergedRoute && maxStatus == maxHomeKey && maxStatus == maxExecuteAt)
            {
                return maxStatus;
            }

            // otherwise assemble the maximum of each, and propagate isCoordinating from the origin we selected the promise from
            boolean isCoordinating = maxPromised == prefer ? prefer.isCoordinating : defer.isCoordinating;
            return new CheckStatusOk(mergedInvalidIfNotAtLeast, mergeStatus, mergeMaxStatus,
                                     maxPromised.promised, maxAccepted.accepted, maxExecuteAt.executeAt,
                                     isCoordinating, maxDurability.durability, mergedRoute, maxHomeKey.homeKey);
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
        public final PartialDeps committedDeps; // only set if status >= Committed, so safe to merge
        public final Writes writes;
        public final Result result;

        public CheckStatusOkFull(boolean isCoordinating, Status invalidIfNotAtLeast, Command command)
        {
            super(isCoordinating, invalidIfNotAtLeast, command);
            this.partialTxn = command.partialTxn();
            this.committedDeps = command.status().compareTo(Committed) >= 0 ? command.partialDeps() : null;
            this.writes = command.writes();
            this.result = command.result();
        }

        protected CheckStatusOkFull(Status invalidIfNotCommitted, SaveStatus status, SaveStatus maxStatus, Ballot promised, Ballot accepted, Timestamp executeAt,
                                  boolean isCoordinating, Durability durability, Route<?> route,
                                  RoutingKey homeKey, PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            super(invalidIfNotCommitted, status, maxStatus, promised, accepted, executeAt, isCoordinating, durability, route, homeKey);
            this.partialTxn = partialTxn;
            this.committedDeps = committedDeps;
            this.writes = writes;
            this.result = result;
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
                return new CheckStatusOkFull(max.invalidIfNotAtLeast, max.saveStatus, max.maxSaveStatus, max.promised, max.accepted, fullMax.executeAt, max.isCoordinating, max.durability, max.route,
                                             max.homeKey, fullMax.partialTxn, fullMax.committedDeps, fullMax.writes, fullMax.result);
            }

            CheckStatusOkFull fullMin = (CheckStatusOkFull) minSrc;

            PartialTxn partialTxn = PartialTxn.merge(fullMax.partialTxn, fullMin.partialTxn);
            PartialDeps committedDeps;
            if (fullMax.committedDeps == null) committedDeps = fullMin.committedDeps;
            else if (fullMin.committedDeps == null) committedDeps = fullMax.committedDeps;
            else committedDeps = fullMax.committedDeps.with(fullMin.committedDeps);
            Writes writes = (fullMax.writes != null ? fullMax : fullMin).writes;
            Result result = (fullMax.result != null ? fullMax : fullMin).result;

            return new CheckStatusOkFull(max.invalidIfNotAtLeast, max.saveStatus, max.maxSaveStatus, max.promised, max.accepted, max.executeAt, max.isCoordinating, max.durability, max.route,
                                         max.homeKey, partialTxn, committedDeps, writes, result);
        }

        public Known sufficientFor(Participants<?> participants, WithQuorum withQuorum)
        {
            if (inferInvalidated(withQuorum))
                return Known.Invalidated;

            return sufficientFor(participants, saveStatus, route, partialTxn, committedDeps, writes, result);
        }

        private static Known sufficientFor(Participants<?> participants, SaveStatus saveStatus, Route<?> route, PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            Status.Definition definition = saveStatus.known.definition;
            switch (definition)
            {
                default: throw new AssertionError();
                case DefinitionKnown:
                    if (partialTxn != null && partialTxn.covers(participants) && route.kind().isFullRoute())
                        break;
                    definition = Definition.DefinitionUnknown;
                case DefinitionUnknown:
                case NoOp:
            }

            KnownExecuteAt executeAt = saveStatus.known.executeAt;
            KnownDeps deps = saveStatus.known.deps;
            switch (deps)
            {
                default: throw new AssertionError();
                case DepsKnown:
                    if (committedDeps != null && committedDeps.covers(participants))
                        break;
                case DepsProposed:
                case NoDeps:
                    deps = KnownDeps.DepsUnknown;
                case DepsUnknown:
            }

            Status.Outcome outcome = saveStatus.known.outcome;
            switch (outcome)
            {
                default: throw new AssertionError();
                case WasApply:
                    if (writes != null && result != null)
                        outcome = Outcome.Apply;

                case Apply:
                    if (writes == null || result == null)
                        outcome = Outcome.WasApply;

                case Invalidated:
                case Unknown:
                case Erased:
            }

            return new Known(definition, executeAt, deps, outcome);
        }

        @Override
        public String toString()
        {
            return "CheckStatusOk{" +
                   "status:" + saveStatus +
                   ", promised:" + promised +
                   ", accepted:" + accepted +
                   ", executeAt:" + executeAt +
                   ", durability:" + durability +
                   ", isCoordinating:" + isCoordinating +
                   ", deps:" + committedDeps +
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
