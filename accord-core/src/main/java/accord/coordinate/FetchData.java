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

package accord.coordinate;

import java.util.function.BiConsumer;

import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.Status;
import accord.local.Status.Known;
import accord.local.Status.Phase;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.WithQuorum;
import accord.primitives.*;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;

import javax.annotation.Nullable;

import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.local.PreLoadContext.contextFor;
import static accord.local.Status.NotDefined;
import static accord.local.Status.Phase.Cleanup;
import static accord.local.Status.PreApplied;
import static accord.messages.CheckStatus.WithQuorum.NoQuorum;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isRoute;

/**
 * Find data and persist locally
 *
 * TODO (expected): avoid multiple command stores performing duplicate queries to other shards
 */
public class FetchData extends CheckShards<Route<?>>
{
    public static Object fetch(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, BiConsumer<? super Known, Throwable> callback)
    {
        return fetch(fetch, node, txnId, someUnseekables, null, callback);
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        if (someUnseekables.kind().isRoute()) return fetch(fetch, node, txnId, castToRoute(someUnseekables), executeAt, callback);
        else return fetchViaSomeRoute(fetch, node, txnId, someUnseekables, executeAt, callback);
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, Route<?> route, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        if (!node.topology().hasEpoch(srcEpoch))
            return node.topology().awaitEpoch(srcEpoch).map(ignore -> fetch(fetch, node, txnId, route, executeAt, callback)).beginAsResult();

        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        Ranges ranges = node.topology().localRangesForEpochs(txnId.epoch(), srcEpoch);
        if (!route.covers(ranges))
        {
            return fetchWithIncompleteRoute(fetch, node, txnId, route, executeAt, callback);
        }
        else
        {
            return fetchInternal(ranges, fetch, node, txnId, route.sliceStrict(ranges), executeAt, callback);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object fetchViaSomeRoute(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        return FindSomeRoute.findSomeRoute(node, txnId, someUnseekables, (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute.route == null)
            {
                reportRouteNotFound(node, txnId, someUnseekables, executeAt, foundRoute.known, callback);
            }
            else if (isRoute(someUnseekables) && someUnseekables.containsAll(foundRoute.route))
            {
                // this is essentially a reentrancy check; we can only reach this point if we have already tried once to fetchSomeRoute
                // (as a user-provided Route is used to fetchRoute, not fetchSomeRoute)
                reportRouteNotFound(node, txnId, someUnseekables, executeAt, foundRoute.known, callback);
            }
            else
            {
                Route<?> route = foundRoute.route;
                if (isRoute(someUnseekables))
                    route = Route.merge(route, (Route)someUnseekables);
                fetch(fetch, node, txnId, route, executeAt, callback);
            }
        });
    }

    private static void reportRouteNotFound(Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable Timestamp executeAt, Known found, BiConsumer<? super Known, Throwable> callback)
    {
        Invariants.checkState(executeAt == null);
        switch (found.outcome)
        {
            default: throw new AssertionError("Unknown outcome: " + found.outcome);
            case Invalidated:
                locallyInvalidateAndCallback(node, txnId, someUnseekables, found, callback);
                break;

            case Unknown:
                if (found.canProposeInvalidation())
                {
                    Invalidate.invalidate(node, txnId, someUnseekables, (outcome, throwable) -> {
                        Known known = throwable != null ? null : outcome.asProgressToken().status == Status.Invalidated ? Known.Invalidated : Known.Nothing;
                        callback.accept(known, throwable);
                    });
                    break;
                }
            case Erased:
            case WasApply:
            case Apply:
                callback.accept(found, null);
        }
    }

    private static Object fetchWithIncompleteRoute(Known fetch, Node node, TxnId txnId, Route<?> someRoute, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        return FindRoute.findRoute(node, txnId, someRoute.withHomeKey(), (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute == null) fetchViaSomeRoute(fetch, node, txnId, someRoute, executeAt, callback);
            else fetch(fetch, node, txnId, foundRoute.route, foundRoute.executeAt, callback);
        });
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, FullRoute<?> route, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        return node.awaitEpoch(executeAt).map(ignore -> {
            Ranges ranges = node.topology().localRangesForEpochs(txnId.epoch(), fetch.fetchEpoch(txnId, executeAt));
            return fetchInternal(ranges, fetch, node, txnId, route.sliceStrict(ranges), executeAt, callback);
        }).beginAsResult();
    }

    private static Object fetchInternal(Ranges ranges, Known target, Node node, TxnId txnId, PartialRoute<?> route, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        long srcEpoch = target.fetchEpoch(txnId, executeAt);
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        PartialRoute<?> fetch = route.sliceStrict(ranges);
        return fetchData(target, node, txnId, fetch, srcEpoch, (sufficientFor, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else callback.accept(sufficientFor, null);
        });
    }

    final BiConsumer<Known, Throwable> callback;
    /**
     * The epoch until which we want to persist any response for locally
     */
    final Known target;

    private FetchData(Node node, Known target, TxnId txnId, Route<?> route, long sourceEpoch, BiConsumer<Known, Throwable> callback)
    {
        this(node, target, txnId, route, route.withHomeKey(), sourceEpoch, callback);
    }

    private FetchData(Node node, Known target, TxnId txnId, Route<?> route, Route<?> routeWithHomeKey, long sourceEpoch, BiConsumer<Known, Throwable> callback)
    {
        // TODO (desired, efficiency): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, routeWithHomeKey, sourceEpoch, CheckStatus.IncludeInfo.All);
        Invariants.checkArgument(routeWithHomeKey.contains(route.homeKey()), "route %s does not contain %s", routeWithHomeKey, route.homeKey());
        this.target = target;
        this.callback = callback;
    }

    private static FetchData fetchData(Known sufficientStatus, Node node, TxnId txnId, Route<?> route, long epoch, BiConsumer<Known, Throwable> callback)
    {
        FetchData fetch = new FetchData(node, sufficientStatus, txnId, route, epoch, callback);
        fetch.start();
        return fetch;
    }

    protected Route<?> route()
    {
        return route;
    }

    @Override
    protected boolean isSufficient(Node.Id from, CheckStatus.CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().computeRangesForNode(from);
        PartialRoute<?> scope = this.route.slice(rangesForNode);
        return isSufficient(scope, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatus.CheckStatusOk ok)
    {
        return isSufficient(route, ok);
    }

    protected boolean isSufficient(Route<?> scope, CheckStatus.CheckStatusOk ok)
    {
        return target.isSatisfiedBy(((CheckStatusOkFull)ok).sufficientFor(scope.participants(), NoQuorum));
    }

    @Override
    protected void onDone(ReadCoordinator.Success success, Throwable failure)
    {
        Invariants.checkState((success == null) != (failure == null));
        if (failure != null)
        {
            callback.accept(null, failure);
        }
        else
        {
            if (success == ReadCoordinator.Success.Success)
                Invariants.checkState(isSufficient(merged), "Status %s is not sufficient", merged);

            OnDone.propagate(node, txnId, sourceEpoch, success.withQuorum, route(), target, (CheckStatusOkFull) merged, callback);
        }
    }

    static class OnDone implements MapReduceConsume<SafeCommandStore, Void>, EpochSupplier
    {
        final Node node;
        final TxnId txnId;
        final Route<?> route;
        final RoutingKey progressKey;
        final CheckStatusOkFull full;
        // this is a WHOLE NODE measure, so if commit epoch has more ranges we do not count as committed if we can only commit in coordination epoch
        final Known achieved;
        final PartialTxn partialTxn;
        final PartialDeps partialDeps;
        final long toEpoch;
        final BiConsumer<Known, Throwable> callback;

        OnDone(Node node, TxnId txnId, Route<?> route, RoutingKey progressKey, CheckStatusOkFull full, Known achieved, PartialTxn partialTxn, PartialDeps partialDeps, long toEpoch, BiConsumer<Known, Throwable> callback)
        {
            this.node = node;
            this.txnId = txnId;
            this.route = route;
            this.progressKey = progressKey;
            this.full = full;
            this.achieved = achieved;
            this.partialTxn = partialTxn;
            this.partialDeps = partialDeps;
            this.toEpoch = toEpoch;
            this.callback = callback;
        }

        @SuppressWarnings({"rawtypes", "unchecked"})
        public static void propagate(Node node, TxnId txnId, long sourceEpoch, WithQuorum withQuorum, Route route, @Nullable Known target, CheckStatusOkFull full, BiConsumer<Known, Throwable> callback)
        {
            if (full.saveStatus.status == NotDefined && full.invalidIfNotAtLeast == NotDefined)
            {
                callback.accept(Known.Nothing, null);
                return;
            }

            Invariants.checkState(sourceEpoch == txnId.epoch() || (full.executeAt != null && sourceEpoch == full.executeAt.epoch()));
            Route<?> maxRoute = Route.merge(route, full.route);

            // TODO (required): permit individual shards that are behind to catch up by themselves
            long toEpoch = sourceEpoch;
            Ranges sliceRanges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);
            if (!maxRoute.covers(sliceRanges))
            {
                callback.accept(Known.Nothing, null);
                return;
            }

            RoutingKey progressKey = node.trySelectProgressKey(txnId, maxRoute);

            Ranges covering = maxRoute.sliceCovering(sliceRanges, Minimal);
            Participants<?> participatingKeys = maxRoute.participants().slice(covering, Minimal);
            Known achieved = full.sufficientFor(participatingKeys, withQuorum);
            if (achieved.executeAt.hasDecidedExecuteAt() && full.executeAt.epoch() > toEpoch)
            {
                Ranges acceptRanges;
                if (!node.topology().hasEpoch(full.executeAt.epoch()) ||
                    (!maxRoute.covers(acceptRanges = node.topology().localRangesForEpochs(txnId.epoch(), full.executeAt.epoch()))))
                {
                    // we don't know what the execution epoch requires, so we cannot be sure we can replicate it locally
                    // we *could* wait until we have the local epoch before running this
                    Status.Outcome outcome = achieved.outcome.propagatesBetweenShards() ? achieved.outcome : Status.Outcome.Unknown;
                    achieved = new Known(achieved.definition, achieved.executeAt, Status.KnownDeps.DepsUnknown, outcome);
                }
                else
                {
                    // TODO (expected): this should only be the two precise epochs, not the full range of epochs
                    sliceRanges = acceptRanges;
                    covering = maxRoute.sliceCovering(sliceRanges, Minimal);
                    participatingKeys = maxRoute.participants().slice(covering, Minimal);
                    Known knownForExecution = full.sufficientFor(participatingKeys, withQuorum);
                    if ((target != null && target.isSatisfiedBy(knownForExecution)) || knownForExecution.isSatisfiedBy(achieved))
                    {
                        achieved = knownForExecution;
                        toEpoch = full.executeAt.epoch();
                    }
                    else
                    {
                        Invariants.checkState(sourceEpoch == txnId.epoch(), "%d != %d", sourceEpoch, txnId.epoch());
                        achieved = new Known(achieved.definition, achieved.executeAt, knownForExecution.deps, knownForExecution.outcome);
                    }
                }
            }

            PartialTxn partialTxn = null;
            if (achieved.definition.isKnown())
                partialTxn = full.partialTxn.slice(sliceRanges, true).reconstitutePartial(covering);

            PartialDeps partialDeps = null;
            if (achieved.deps.hasDecidedDeps())
                partialDeps = full.committedDeps.slice(sliceRanges).reconstitutePartial(covering);

            new OnDone(node, txnId, maxRoute, progressKey, full, achieved, partialTxn, partialDeps, toEpoch, callback).start();
        }

        void start()
        {
            Seekables<?, ?> keys = Keys.EMPTY;
            if (achieved.definition.isKnown())
                keys = partialTxn.keys();
            else if (achieved.deps.hasProposedOrDecidedDeps())
                keys = partialDeps.keyDeps.keys();

            PreLoadContext loadContext = contextFor(txnId, keys);
            node.mapReduceConsumeLocal(loadContext, route, txnId.epoch(), toEpoch, this);
        }

        @Override
        public Void apply(SafeCommandStore safeStore)
        {
            SafeCommand safeCommand = safeStore.get(txnId, this, route);
            Command command = safeCommand.current();
            if (command.saveStatus().phase.compareTo(Phase.Persist) >= 0)
                return null;

            Status propagate = achieved.propagate();
            if (command.hasBeen(propagate))
            {
                if (full.maxSaveStatus.phase == Cleanup && full.durability.isDurableOrInvalidated() && Infer.safeToCleanup(safeStore, command, route, full.executeAt))
                    Commands.setTruncatedApply(safeStore, safeCommand);
                return null;
            }

            switch (propagate)
            {
                default: throw new IllegalStateException("Unexpected status: " + propagate);
                case Accepted:
                case AcceptedInvalidate:
                    // we never "propagate" accepted statuses as these are essentially votes,
                    // and contribute nothing to our local state machine
                    throw new IllegalStateException("Invalid states to propagate: " + achieved.propagate());

                case Truncated:
                    // if our peers have truncated this command, then either:
                    // 1) we have already applied it locally; 2) the command doesn't apply locally; 3) we are stale; or 4) the command is invalidated
                    if (command.hasBeen(PreApplied) || command.saveStatus().isUninitialised())
                        break;

                    if (Infer.safeToCleanup(safeStore, command, route, full.executeAt))
                    {
                        Commands.setErased(safeStore, safeCommand);
                        break;
                    }

                    // TODO (required): check if we are stale
                    // otherwise we are either stale, or the command didn't reach consensus

                case Invalidated:
                    Commands.commitInvalidate(safeStore, safeCommand, route);
                    break;

                case Applied:
                case PreApplied:
                    Invariants.checkState(full.executeAt != null);
                    if (toEpoch >= full.executeAt.epoch())
                    {
                        confirm(Commands.apply(safeStore, safeCommand, txnId, route, progressKey, full.executeAt, partialDeps, partialTxn, full.writes, full.result));
                        break;
                    }

                case Committed:
                case ReadyToExecute:
                    confirm(Commands.commit(safeStore, safeCommand, txnId, route, progressKey, partialTxn, full.executeAt, partialDeps));
                    break;

                case PreCommitted:
                    Commands.precommit(safeStore, safeCommand, txnId, full.executeAt, route);
                    if (!achieved.definition.isKnown())
                        break;

                case PreAccepted:
                    // only preaccept if we coordinate the transaction
                    if (safeStore.ranges().coordinates(txnId).intersects(route) && Route.isFullRoute(route))
                        Commands.preaccept(safeStore, safeCommand, txnId, txnId.epoch(), partialTxn, Route.castToFullRoute(route), progressKey);
                    break;

                case NotDefined:
                    break;
            }

            RoutingKey homeKey = full.homeKey;
            if (!full.durability.isDurable() || homeKey == null)
                return null;

            if (!safeStore.ranges().coordinates(txnId).contains(homeKey))
                return null;

            Timestamp executeAt = full.saveStatus.known.executeAt.hasDecidedExecuteAt() ? full.executeAt : null;
            Commands.setDurability(safeStore, safeCommand, full.durability, route, executeAt);
            return null;
        }

        @Override
        public Void reduce(Void o1, Void o2)
        {
            return null;
        }

        @Override
        public void accept(Void result, Throwable failure)
        {
            callback.accept(failure  == null ? achieved : null, failure);
        }

        @Override
        public long epoch()
        {
            return toEpoch;
        }
    }

    private static void confirm(Commands.CommitOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }

    private static void confirm(Commands.ApplyOutcome outcome)
    {
        switch (outcome)
        {
            default: throw new IllegalStateException("Unknown outcome: " + outcome);
            case Redundant:
            case Success:
                return;
            case Insufficient: throw new IllegalStateException("Should have enough information");
        }
    }

}
