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
import javax.annotation.Nullable;

import accord.local.Node;
import accord.local.Status;
import accord.local.Status.Known;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.Propagate;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import javax.annotation.Nonnull;

import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.primitives.Route.castToRoute;
import static accord.primitives.Route.isFullRoute;
import static accord.primitives.Route.isRoute;

/**
 * Find data and persist locally
 *
 * TODO (expected): avoid multiple command stores performing duplicate queries to other shards
 */
public class FetchData extends CheckShards<Route<?>>
{
    public static class FetchResult
    {
        public final @Nonnull Known achieved;
        public final Unseekables<?> achievedTarget;
        public final @Nullable Unseekables<?> didNotAchieveTarget;

        public FetchResult(Known achieved, Unseekables<?> achievedTarget, @Nullable Unseekables<?> didNotAchieveTarget)
        {
            this.achieved = Invariants.nonNull(achieved);
            this.achievedTarget = achievedTarget;
            this.didNotAchieveTarget = didNotAchieveTarget == null || didNotAchieveTarget.isEmpty() ? null : didNotAchieveTarget;
        }
    }

    public static void fetch(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super FetchResult, Throwable> callback)
    {
        if (someUnseekables.kind().isRoute()) fetch(fetch, node, txnId, castToRoute(someUnseekables), forLocalEpoch, executeAt, callback);
        else fetchViaSomeRoute(fetch, node, txnId, someUnseekables, forLocalEpoch, executeAt, callback);
    }

    public static void fetch(Known fetch, Node node, TxnId txnId, Route<?> route, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super FetchResult, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        if (!node.topology().hasEpoch(srcEpoch))
        {
            node.withEpoch(srcEpoch, callback, () -> fetch(fetch, node, txnId, route, forLocalEpoch, executeAt, callback));
            return;
        }

        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        long toEpoch = Math.max(srcEpoch, forLocalEpoch == null ? 0 : forLocalEpoch.epoch());
        Ranges ranges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);
        if (!Route.isFullRoute(route))
        {
            fetchWithIncompleteRoute(fetch, node, txnId, route, forLocalEpoch, executeAt, callback);
        }
        else
        {
            Route<?> slicedRoute = route.slice(ranges);
            fetchInternal(ranges, fetch, node, txnId, slicedRoute, slicedRoute, executeAt, toEpoch, callback);
        }
    }

    /**
     * Do not make an attempt to discern what keys need to be contacted; fetch from only the specific remote keys that were requested.
     */
    public static void fetchSpecific(Known fetch, Node node, TxnId txnId, Route<?> query, Unseekables<?> propagateTo, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super FetchResult, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        if (!node.topology().hasEpoch(srcEpoch))
        {
            node.withEpoch(srcEpoch, callback, () -> fetchSpecific(fetch, node, txnId, query, propagateTo, forLocalEpoch, executeAt, callback));
            return;
        }

        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        long toEpoch = Math.max(srcEpoch, forLocalEpoch == null ? 0 : forLocalEpoch.epoch());
        fetchData(fetch, node, txnId, query, propagateTo, srcEpoch, toEpoch, callback);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void fetchViaSomeRoute(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super FetchResult, Throwable> callback)
    {
        FindSomeRoute.findSomeRoute(node, txnId, someUnseekables, (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute.route == null)
            {
                reportRouteNotFound(node, txnId, executeAt, forLocalEpoch, someUnseekables, foundRoute.known, callback);
            }
            else if (isFullRoute(foundRoute.route))
            {
                fetch(fetch, node, txnId, Route.castToFullRoute(foundRoute.route), forLocalEpoch, executeAt, callback);
            }
            else if (isRoute(someUnseekables) && someUnseekables.containsAll(foundRoute.route))
            {
                // this is essentially a reentrancy check; we can only reach this point if we have already tried once to fetchSomeRoute
                // (as a user-provided Route is used to fetchRoute, not fetchSomeRoute)
                reportRouteNotFound(node, txnId, executeAt, forLocalEpoch, someUnseekables, foundRoute.known, callback);
            }
            else
            {
                Route<?> route = foundRoute.route;
                if (isRoute(someUnseekables))
                    route = Route.merge(route, (Route)someUnseekables);
                fetch(fetch, node, txnId, route, forLocalEpoch, executeAt, callback);
            }
        });
    }

    private static void reportRouteNotFound(Node node, TxnId txnId, @Nullable Timestamp executeAt, @Nullable EpochSupplier forLocalEpoch, Unseekables<?> fetchKeys, Known found, BiConsumer<? super FetchResult, Throwable> callback)
    {
        Invariants.checkState(executeAt == null);
        switch (found.outcome)
        {
            default: throw new AssertionError("Unknown outcome: " + found.outcome);
            case Invalidated:
                if (forLocalEpoch == null) forLocalEpoch = txnId;
                locallyInvalidateAndCallback(node, txnId, forLocalEpoch, fetchKeys, new FetchResult(found, fetchKeys, null), callback);
                break;

            case Unknown:
                if (found.canProposeInvalidation())
                {
                    Invalidate.invalidate(node, txnId, fetchKeys, (outcome, throwable) -> {
                        FetchResult result = null;
                        if (throwable == null)
                        {
                            Known achieved = Known.Invalidated;
                            Unseekables<?> achievedTarget = fetchKeys, didNotAchieveTarget = null;
                            if (outcome.asProgressToken().status != Status.Invalidated)
                            {
                                achievedTarget = fetchKeys.slice(0, 0);
                                didNotAchieveTarget = fetchKeys;
                                achieved = Known.Nothing;
                            }
                            result = new FetchResult(achieved, achievedTarget, didNotAchieveTarget);
                        }
                        callback.accept(result, throwable);
                    });
                    break;
                }
            case Erased:
            case WasApply:
            case Apply:
                // TODO (expected): we may be stale
                callback.accept(new FetchResult(found, fetchKeys.slice(0, 0), fetchKeys), null);
        }
    }

    private static void fetchWithIncompleteRoute(Known fetch, Node node, TxnId txnId, Route<?> someRoute, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super FetchResult, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        FindRoute.findRoute(node, txnId, someRoute.withHomeKey(), (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute == null) fetchViaSomeRoute(fetch, node, txnId, someRoute, forLocalEpoch, executeAt, callback);
            else fetch(fetch, node, txnId, foundRoute.route, forLocalEpoch, foundRoute.executeAt, callback);
        });
    }

    public static void fetch(Known fetch, Node node, TxnId txnId, FullRoute<?> route, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super FetchResult, Throwable> callback)
    {
        node.withEpoch(executeAt, (ignore, withEpochFailure) -> {
            if (withEpochFailure != null)
            {
                callback.accept(null, CoordinationFailed.wrap(withEpochFailure));
                return;
            }
            long toEpoch = Math.max(fetch.fetchEpoch(txnId, executeAt), forLocalEpoch == null ? 0 : forLocalEpoch.epoch());
            Ranges ranges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);

            Route<?> slicedRoute = route.slice(ranges);
            fetchInternal(ranges, fetch, node, txnId, slicedRoute, slicedRoute, executeAt, toEpoch, callback);
        });
    }

    private static Object fetchInternal(Ranges ranges, Known target, Node node, TxnId txnId, Route<?> route, Unseekables<?> propagateTo, @Nullable Timestamp executeAt, long forLocalEpoch, BiConsumer<? super FetchResult, Throwable> callback)
    {
        long srcEpoch = target.fetchEpoch(txnId, executeAt);
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        Route<?> fetch = route.slice(ranges);
        return fetchData(target, node, txnId, fetch, propagateTo, srcEpoch, forLocalEpoch, (result, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else callback.accept(result, null);
        });
    }

    final BiConsumer<? super FetchResult, Throwable> callback;
    /**
     * The epoch until which we want to persist any response for locally
     */
    final Known target;

    // to support cases where a later epoch that ultimately does not participate in execution has a vestigial entry
    // (i.e. if preaccept/accept contact a later epoch than execution is decided for)
    final long toEpoch;

    final Unseekables<?> propagateTo;

    private FetchData(Node node, Known target, TxnId txnId, Route<?> route, Unseekables<?> propagateTo, long sourceEpoch, long toEpoch, BiConsumer<? super FetchResult, Throwable> callback)
    {
        this(node, target, txnId, route, route.withHomeKey(), propagateTo, sourceEpoch, toEpoch, callback);
    }

    private FetchData(Node node, Known target, TxnId txnId, Route<?> route, Route<?> routeWithHomeKey, Unseekables<?> propagateTo, long sourceEpoch, long toEpoch, BiConsumer<? super FetchResult, Throwable> callback)
    {
        // TODO (desired, efficiency): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, routeWithHomeKey, sourceEpoch, CheckStatus.IncludeInfo.All);
        this.propagateTo = propagateTo;
        Invariants.checkArgument(routeWithHomeKey.contains(route.homeKey()), "route %s does not contain %s", routeWithHomeKey, route.homeKey());
        this.target = target;
        this.toEpoch = toEpoch;
        this.callback = callback;
    }

    private static FetchData fetchData(Known sufficientStatus, Node node, TxnId txnId, Route<?> route, Unseekables<?> propagateTo, long sourceEpoch, long toEpoch, BiConsumer<? super FetchResult, Throwable> callback)
    {
        Invariants.checkState(!propagateTo.isEmpty());
        FetchData fetch = new FetchData(node, sufficientStatus, txnId, route, propagateTo, sourceEpoch, toEpoch, callback);
        fetch.start();
        return fetch;
    }

    protected Route<?> query()
    {
        return route;
    }

    @Override
    protected boolean isSufficient(Node.Id from, CheckStatus.CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().computeRangesForNode(from);
        Route<?> scope = this.route.slice(rangesForNode);
        return isSufficient(scope, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatus.CheckStatusOk ok)
    {
        return isSufficient(route, ok);
    }

    protected boolean isSufficient(Route<?> scope, CheckStatus.CheckStatusOk ok)
    {
        return target.isSatisfiedBy(ok.knownFor(scope.participants()));
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
            {
                if (!isSufficient(merged))
                    Invariants.checkState(isSufficient(merged), "Status %s is not sufficient", merged);
            }

            // TODO (expected): should we automatically trigger a new fetch if we find executeAt but did not request enough information? would be more rob ust
            Propagate.propagate(node, txnId, sourceEpoch, toEpoch, success.withQuorum, query(), propagateTo, target, (CheckStatusOkFull) merged, callback);
        }
    }
}
