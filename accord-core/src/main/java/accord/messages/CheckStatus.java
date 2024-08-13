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

import java.util.Arrays;
import java.util.Objects;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.coordinate.Infer;
import accord.coordinate.Infer.InvalidIfNot;
import accord.coordinate.Infer.IsPreempted;
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
import accord.primitives.PartialTxn;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.topology.Topologies;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;
import accord.utils.ReducingRangeMap;

import javax.annotation.Nonnull;

import static accord.coordinate.Infer.InvalidIfNot.IsNotInvalid;
import static accord.coordinate.Infer.InvalidIfNot.NotKnownToBeInvalid;
import static accord.coordinate.Infer.IsPreempted.NotPreempted;
import static accord.coordinate.Infer.IsPreempted.Preempted;
import static accord.local.Status.Durability;
import static accord.local.Status.Durability.Local;
import static accord.local.Status.Durability.Majority;
import static accord.local.Status.Durability.ShardUniversal;
import static accord.local.Status.Durability.Universal;
import static accord.local.Status.Known;
import static accord.local.Status.KnownDeps.DepsErased;
import static accord.local.Status.NotDefined;
import static accord.local.Status.Stable;
import static accord.local.Status.Truncated;
import static accord.messages.CheckStatus.WithQuorum.HasQuorum;
import static accord.messages.TxnRequest.computeScope;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

public class CheckStatus extends AbstractEpochRequest<CheckStatus.CheckStatusReply>
        implements Request, PreLoadContext, MapReduceConsume<SafeCommandStore, CheckStatus.CheckStatusReply>, EpochSupplier
{
    public enum WithQuorum { HasQuorum, NoQuorum }

    public static class SerializationSupport
    {
        public static CheckStatusOk createOk(FoundKnownMap map, SaveStatus maxKnowledgeStatus, SaveStatus maxStatus,
                                             Ballot promised, Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted,
                                             @Nullable Timestamp executeAt, boolean isCoordinating, Durability durability,
                                             @Nullable Route<?> route, @Nullable RoutingKey homeKey)
        {
            return new CheckStatusOk(map, maxKnowledgeStatus, maxStatus, promised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, route, homeKey);
        }
        public static CheckStatusOk createOk(FoundKnownMap map, SaveStatus maxKnowledgeStatus, SaveStatus maxStatus,
                                             Ballot promised, Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted,
                                             @Nullable Timestamp executeAt, boolean isCoordinating, Durability durability,
                                             @Nullable Route<?> route, @Nullable RoutingKey homeKey,
                                             PartialTxn partialTxn, PartialDeps committedDeps, Writes writes, Result result)
        {
            return new CheckStatusOkFull(map, maxKnowledgeStatus, maxStatus, promised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                         executeAt, isCoordinating, durability, route, homeKey, partialTxn, committedDeps, writes, result);
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
        if (isRoute(query)) this.query = computeScope(to, topologies, castToRoute(query), 0, Route::slice, Route::with);
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

        Commands.updateRouteOrParticipants(safeStore, safeCommand, query);

        boolean isCoordinating = isCoordinating(node, command);
        Durability durability = command.durability();
        Route<?> route = command.route();
        if (Route.isFullRoute(route))
            durability = Durability.mergeAtLeast(durability, safeStore.commandStore().durableBefore().min(txnId, route));
        FoundKnownMap map = foundKnown(safeStore, command, route);

        switch (includeInfo)
        {
            default: throw new IllegalStateException();
            case No:
            case Route:
                Route<?> respondWithRoute = includeInfo == IncludeInfo.No ? null : route;
                return new CheckStatusOk(map, isCoordinating, durability, respondWithRoute, command);
            case All:
                return new CheckStatusOkFull(map, isCoordinating, durability, command);
        }
    }

    private FoundKnownMap foundKnown(SafeCommandStore safeStore, Command command, @Nullable Route<?> route)
    {
        Unseekables<?> max = Route.isFullRoute(route) ? route : route != null ? Unseekables.merge((Unseekables)query, route.withHomeKey()) : query;
        Unseekables<?> local = max.slice(safeStore.ranges().allAt(command.txnId()), Minimal);
        FoundKnown known = new FoundKnown(command.saveStatus().known, NotKnownToBeInvalid, Timestamp.max(command.promised(), command.acceptedOrCommitted()).equals(Ballot.ZERO) ? NotPreempted : Preempted);

        if (command.known().isDecidedToExecute())
            return FoundKnownMap.create(local, known.withAtLeast(IsNotInvalid));

        return Infer.withInvalidIfNot(safeStore, command.txnId(), local, max, known);
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
        if (failure != null) node.reply(replyTo, replyContext, ok, failure);
        else if (ok == null) node.reply(replyTo, replyContext, CheckStatusNack.NotOwned, null);
        else node.reply(replyTo, replyContext, ok, null);
    }

    public interface CheckStatusReply extends Reply
    {
        boolean isOk();
    }

    /**
     * This is slightly different to Known, in that it represents the knowledge we have obtained from any shard that
     * can be applied to any other shard, along with any information we might use to infer invalidation.
     */
    public static class FoundKnown extends Known
    {
        public static final FoundKnown Nothing = new FoundKnown(Known.Nothing, NotKnownToBeInvalid, NotPreempted);
        public static final FoundKnown Invalidated = new FoundKnown(Known.Invalidated, NotKnownToBeInvalid, NotPreempted);

        public final InvalidIfNot invalidIfNot;
        public final IsPreempted isPreempted;

        public FoundKnown(Known known, InvalidIfNot invalidIfNot, IsPreempted isPreempted)
        {
            super(known);
            this.invalidIfNot = invalidIfNot;
            this.isPreempted = isPreempted;
        }

        public FoundKnown atLeast(FoundKnown with)
        {
            Known known = super.atLeast(with);
            if (known == this)
                return this;
            return new FoundKnown(known, invalidIfNot.atLeast(with.invalidIfNot), isPreempted.merge(with.isPreempted));
        }

        public FoundKnown reduce(FoundKnown with)
        {
            Known known = super.reduce(with);
            if (known == this)
                return this;
            return new FoundKnown(known, invalidIfNot.reduce(with.invalidIfNot), isPreempted.validForBoth(with.isPreempted));
        }

        public FoundKnown withAtLeast(InvalidIfNot invalidIfNot)
        {
            invalidIfNot = this.invalidIfNot.atLeast(invalidIfNot);
            if (this.invalidIfNot == invalidIfNot)
                return this;
            return new FoundKnown(this, invalidIfNot, isPreempted);
        }

        public FoundKnown validForAll()
        {
            Known known = super.validForAll();
            if (known == this)
                return this;
            return new FoundKnown(known, NotKnownToBeInvalid, NotPreempted);
        }

        public boolean isTruncated()
        {
            switch (outcome)
            {
                default: throw new AssertionError("Unhandled outcome: " + outcome);
                case Invalidated:
                case Unknown:
                    return false;
                case Apply:
                    // since Apply is universal, we can
                    return deps == DepsErased;
                case Erased:
                case WasApply:
                    return true;
            }
        }

        public boolean canProposeInvalidation()
        {
            return deps.canProposeInvalidation() && executeAt.canProposeInvalidation() && outcome.canProposeInvalidation();
        }

        public boolean inferInvalidWithQuorum()
        {
            return invalidIfNot.inferInvalidWithQuorum(isPreempted, this);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FoundKnown that = (FoundKnown) o;
            return route == that.route && definition == that.definition && executeAt == that.executeAt && deps == that.deps && outcome == that.outcome && invalidIfNot == that.invalidIfNot && isPreempted == that.isPreempted;
        }
    }

    // TODO (expected, consider): build only with keys/ranges found in command stores, not the covering ranges of command stores?
    public static class FoundKnownMap extends ReducingRangeMap<FoundKnown>
    {
        public static class SerializerSupport
        {
            public static FoundKnownMap create(boolean inclusiveEnds, RoutingKey[] ends, FoundKnown[] values)
            {
                return new FoundKnownMap(inclusiveEnds, ends, values);
            }
        }

        private transient final FoundKnown validForAll;

        private FoundKnownMap()
        {
            this.validForAll = FoundKnown.Nothing;
        }

        public FoundKnownMap(boolean inclusiveEnds, RoutingKey[] starts, FoundKnown[] values)
        {
            this(inclusiveEnds, starts, values, FoundKnown.Nothing);
        }

        private FoundKnownMap(boolean inclusiveEnds, RoutingKey[] starts, FoundKnown[] values, FoundKnown validForAll)
        {
            super(inclusiveEnds, starts, values);
            this.validForAll = validForAll;
        }

        public static FoundKnownMap create(Unseekables<?> keysOrRanges, SaveStatus saveStatus, InvalidIfNot invalidIfNot, Ballot promised)
        {
            FoundKnown known = new FoundKnown(saveStatus.known, invalidIfNot, promised.equals(Ballot.ZERO) ? NotPreempted : Preempted);
            if (keysOrRanges.isEmpty())
                return new FoundKnownMap();

            return create(keysOrRanges, known, Builder::new);
        }

        public static FoundKnownMap create(Unseekables<?> keysOrRanges, FoundKnown known)
        {
            if (keysOrRanges.isEmpty())
                return new FoundKnownMap();

            return create(keysOrRanges, known, Builder::new);
        }

        public static FoundKnownMap merge(FoundKnownMap a, FoundKnownMap b)
        {
            return ReducingRangeMap.merge(a, b, FoundKnown::atLeast, Builder::new);
        }

        private FoundKnownMap finish(Unseekables<?> routeOrParticipants, WithQuorum withQuorum)
        {
            FoundKnown validForAll;
            switch (withQuorum)
            {
                default: throw new AssertionError("Unhandled WithQuorum: " + withQuorum);
                case HasQuorum: validForAll = foldlWithDefault(routeOrParticipants, FoundKnownMap::reduceInferredOrKnownForWithQuorum, FoundKnown.Nothing, null, i -> false); break;
                case NoQuorum: validForAll = foldlWithDefault(routeOrParticipants, FoundKnownMap::reduceKnownFor, FoundKnown.Nothing, null, i -> false); break;
            }
            validForAll = this.validForAll.atLeast(validForAll).validForAll();
            return with(validForAll);
        }

        private FoundKnownMap with(FoundKnown validForAll)
        {
            if (validForAll.equals(this.validForAll))
                return this;

            int i = 0;
            for (; i < size(); ++i)
            {
                FoundKnown pre = values[i];
                if (pre == null)
                    continue;

                FoundKnown post = pre.atLeast(validForAll);
                if (!pre.equals(post))
                    break;
            }

            if (i == size())
                return new FoundKnownMap(inclusiveEnds(), starts, values, validForAll);

            RoutingKey[] newStarts = new RoutingKey[size() + 1];
            FoundKnown[] newValues = new FoundKnown[size()];
            System.arraycopy(starts, 0, newStarts, 0, i);
            System.arraycopy(values, 0, newValues, 0, i);
            int count = i;
            while (i < size())
            {
                FoundKnown pre = values[i++];
                FoundKnown post = pre == null ? null : pre.atLeast(validForAll);
                if (count == 0 || !Objects.equals(post, newValues[count - 1]))
                {
                    newStarts[count] = starts[i-1];
                    newValues[count++] = post;
                }
            }
            newStarts[count] = starts[size()];
            if (count != newValues.length)
            {
                newValues = Arrays.copyOf(newValues, count);
                newStarts = Arrays.copyOf(newStarts, count + 1);
            }
            return new FoundKnownMap(inclusiveEnds(), newStarts, newValues, validForAll);
        }

        public boolean hasTruncated(Routables<?> routables)
        {
            return foldlWithDefault(routables, (known, prev) -> known.isTruncated(), FoundKnown.Nothing, false, i -> i);
        }

        public boolean hasTruncated()
        {
            return foldl((known, prev) -> known.isTruncated(), false, i -> i);
        }

        public boolean hasInvalidated()
        {
            return foldl((known, prev) -> known.isInvalidated(), false, i -> i);
        }

        public Known knownFor(Routables<?> routables)
        {
            return validForAll.atLeast(foldlWithDefault(routables, FoundKnownMap::reduceKnownFor, FoundKnown.Nothing, null, i -> false));
        }

        public Known knownForAny()
        {
            return validForAll.atLeast(foldl(FoundKnown::atLeast, FoundKnown.Nothing, i -> false));
        }

        public Ranges matchingRanges(Predicate<FoundKnown> match)
        {
            return foldlWithBounds((known, ranges, start, end) -> match.test(known) ? ranges.with(Ranges.of(start.rangeFactory().newRange(start, end))) : ranges, Ranges.EMPTY, i -> false);
        }

        private static FoundKnown reduceInferredOrKnownForWithQuorum(FoundKnown foundKnown, @Nullable FoundKnown prev)
        {
            if (foundKnown.inferInvalidWithQuorum())
                foundKnown = foundKnown.atLeast(FoundKnown.Invalidated);

            if (prev == null)
                return foundKnown;

            return prev.reduce(foundKnown);
        }

        private static Known reduceKnownFor(FoundKnown foundKnown, @Nullable Known prev)
        {
            if (prev == null)
                return foundKnown;

            return prev.reduce(foundKnown);
        }

        private static FoundKnown reduceKnownFor(FoundKnown foundKnown, @Nullable FoundKnown prev)
        {
            if (prev == null)
                return foundKnown;

            return prev.reduce(foundKnown);
        }

        public Ranges knownForRanges(Known required, Ranges expect)
        {
            // TODO (desired): implement and use foldlWithDefaultAndBounds so can subtract rather than add
            return foldlWithBounds(expect, (known, prev, start, end) -> {
                if (!required.isSatisfiedBy(known))
                    return prev;

                return prev.with(Ranges.of(start.rangeFactory().newRange(start, end)));
            }, Ranges.EMPTY, i -> false);
        }

        public static class Builder extends AbstractBoundariesBuilder<RoutingKey, FoundKnown, FoundKnownMap>
        {
            public Builder(boolean inclusiveEnds, int capacity)
            {
                super(inclusiveEnds, capacity);
            }

            @Override
            protected FoundKnownMap buildInternal()
            {
                return new FoundKnownMap(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new FoundKnown[0]));
            }
        }
    }

    public static class CheckStatusOk implements CheckStatusReply
    {
        public final FoundKnownMap map;
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

        public CheckStatusOk(FoundKnownMap map, boolean isCoordinating, Durability durability, Command command)
        {
            this(map, isCoordinating, durability, command.route(), command);
        }

        public CheckStatusOk(FoundKnownMap map, boolean isCoordinating, Durability durability, Route<?> route, Command command)
        {
            this(map, command.saveStatus(), command.promised(), command.acceptedOrCommitted(), command.acceptedOrCommitted(),
                 command.executeAt(), isCoordinating, durability, route, command.homeKey());
        }

        private CheckStatusOk(FoundKnownMap map, SaveStatus saveStatus, Ballot maxPromised,
                              Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted, @Nullable Timestamp executeAt,
                              boolean isCoordinating, Durability durability,
                              @Nullable Route<?> route, @Nullable RoutingKey homeKey)
        {
            this(map, saveStatus, saveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted, executeAt, isCoordinating, durability, route, homeKey);
        }

        private CheckStatusOk(FoundKnownMap map, SaveStatus maxKnowledgeSaveStatus, SaveStatus maxSaveStatus, Ballot maxPromised, Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted,
                              @Nullable Timestamp executeAt, boolean isCoordinating, Durability durability,
                              @Nullable Route<?> route, @Nullable RoutingKey homeKey)
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
                finished = merge(durability);
            }
            if (Route.isRoute(routeOrParticipants))
            {
                finished = finished.merge(Route.castToRoute(routeOrParticipants));
                return finished.with(finished.map.finish(finished.route, withQuorum));
            }
            else
            {
                return finished.with(finished.map.finish(Unseekables.merge(routeOrParticipants, (Unseekables) finished.route), withQuorum));
            }
        }

        /**
         * NOTE: if the response is *incomplete* this does not detect possible truncation, it only indicates if the
         * combination of the responses we received represents truncation
         */
        public boolean isTruncatedResponse()
        {
            return map.hasTruncated();
        }

        public boolean isTruncatedResponse(Routables<?> routables)
        {
            return map.hasTruncated(routables);
        }

        public Ranges truncatedResponse()
        {
            return map.matchingRanges(FoundKnown::isTruncated);
        }

        public CheckStatusOk merge(@Nonnull Route<?> route)
        {
            Route<?> mergedRoute = Route.merge((Route)this.route, route);
            if (mergedRoute == this.route)
                return this;
            return new CheckStatusOk(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, mergedRoute, homeKey);
        }

        public CheckStatusOk merge(@Nonnull Durability durability)
        {
            durability = Durability.merge(durability, this.durability);
            if (durability == this.durability)
                return this;
            return new CheckStatusOk(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, route, homeKey);
        }

        CheckStatusOk with(@Nonnull FoundKnownMap newMap)
        {
            if (newMap == this.map)
                return this;
            return new CheckStatusOk(newMap, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                     executeAt, isCoordinating, durability, route, homeKey);
        }

        // TODO (required): harden markShardStale against unnecessary actions by utilising inferInvalidated==MAYBE and performing a global query
        public Known knownFor(Unseekables<?> participants)
        {
            Known known = map.knownFor(participants);
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
            FoundKnownMap mergeMap = FoundKnownMap.merge(prefer.map, defer.map);
            CheckStatusOk maxStatus = SaveStatus.max(prefer, prefer.maxKnowledgeSaveStatus, prefer.acceptedOrCommitted, defer, defer.maxKnowledgeSaveStatus, defer.acceptedOrCommitted, true);
            SaveStatus mergeMaxKnowledgeStatus = SaveStatus.merge(prefer.maxKnowledgeSaveStatus, prefer.acceptedOrCommitted, defer.maxKnowledgeSaveStatus, defer.acceptedOrCommitted, true);
            SaveStatus mergeMaxStatus = SaveStatus.merge(prefer.maxSaveStatus, prefer.acceptedOrCommitted, defer.maxSaveStatus, defer.acceptedOrCommitted, false);
            CheckStatusOk maxPromised = prefer.maxPromised.compareTo(defer.maxPromised) >= 0 ? prefer : defer;
            CheckStatusOk maxAccepted = prefer.maxAcceptedOrCommitted.compareTo(defer.maxAcceptedOrCommitted) >= 0 ? prefer : defer;
            CheckStatusOk maxHomeKey = prefer.homeKey != null || defer.homeKey == null ? prefer : defer;
            CheckStatusOk maxExecuteAt = prefer.maxKnown().executeAt.compareTo(defer.maxKnown().executeAt) >= 0 ? prefer : defer;
            Route<?> mergedRoute = Route.merge(prefer.route, (Route)defer.route);
            Durability mergedDurability = Durability.merge(prefer.durability, defer.durability);

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
                                     maxExecuteAt.executeAt, isCoordinating, mergedDurability, mergedRoute, maxHomeKey.homeKey);
        }

        public Known maxKnown()
        {
            return map.foldl(Known::atLeast, Known.Nothing, i -> false);
        }

        public InvalidIfNot maxInvalidIfNot()
        {
            return map.foldl((known, prev) -> known.invalidIfNot.atLeast(prev), NotKnownToBeInvalid, InvalidIfNot::isMax);
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

        public CheckStatusOkFull(FoundKnownMap map, boolean isCoordinating, Durability durability, Command command)
        {
            super(map, isCoordinating, durability, command);
            this.partialTxn = command.partialTxn();
            this.stableDeps = command.status().compareTo(Stable) >= 0 ? command.partialDeps() : null;
            this.writes = command.writes();
            this.result = command.result();
        }

        protected CheckStatusOkFull(FoundKnownMap map, SaveStatus maxNotTruncatedSaveStatus, SaveStatus maxSaveStatus, Ballot promised, Ballot maxAcceptedOrCommitted, Ballot acceptedOrCommitted,
                                    Timestamp executeAt, boolean isCoordinating, Durability durability, Route<?> route,
                                    RoutingKey homeKey, PartialTxn partialTxn, PartialDeps stableDeps, Writes writes, Result result)
        {
            super(map, maxNotTruncatedSaveStatus, maxSaveStatus, promised, maxAcceptedOrCommitted, acceptedOrCommitted,
                  executeAt, isCoordinating, durability, route, homeKey);
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
                                         executeAt, isCoordinating, durability, mergedRoute, homeKey, partialTxn, stableDeps, writes, result);
        }

        public CheckStatusOkFull merge(@Nonnull Durability durability)
        {
            durability = Durability.merge(durability, this.durability);
            if (durability == this.durability)
                return this;
            return new CheckStatusOkFull(map, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                         executeAt, isCoordinating, durability, route, homeKey, partialTxn, stableDeps, writes, result);
        }

        CheckStatusOk with(@Nonnull FoundKnownMap newMap)
        {
            if (newMap == this.map)
                return this;
            return new CheckStatusOkFull(newMap, maxKnowledgeSaveStatus, maxSaveStatus, maxPromised, maxAcceptedOrCommitted, acceptedOrCommitted,
                                         executeAt, isCoordinating, durability, route, homeKey, partialTxn, stableDeps, writes, result);
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
                                             max.homeKey, fullMax.partialTxn, fullMax.stableDeps, fullMax.writes, fullMax.result);
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
                                         max.homeKey, partialTxn, committedDeps, writes, result);
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
        public Known knownFor(Unseekables<?> participants)
        {
            Known known = super.knownFor(participants);
            Invariants.checkState(!known.hasDefinition() || (partialTxn != null && partialTxn.covers(participants)));
            Invariants.checkState(!known.hasDecidedDeps() || (stableDeps != null && stableDeps.covers(participants)));
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
