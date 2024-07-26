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
import accord.primitives.Status;
import accord.primitives.Known;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.Propagate;
import accord.primitives.EpochSupplier;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import javax.annotation.Nonnull;

import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.primitives.EpochSupplier.constant;
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

    private static class FetchRequest
    {
        final Known fetch;
        final TxnId txnId;
        final @Nullable Timestamp executeAt;
        final long srcEpoch;
        final Participants<?> fetchKeys;
        final long lowEpoch, highEpoch;
        final BiConsumer<? super FetchResult, Throwable> callback;

        public FetchRequest(Known fetch, TxnId txnId, @Nullable Timestamp executeAt, Participants<?> fetchKeys, EpochSupplier lowEpoch, EpochSupplier highEpoch, BiConsumer<? super FetchResult, Throwable> callback)
        {
            this.fetch = fetch;
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.callback = callback;
            this.srcEpoch = fetch.fetchEpoch(txnId, executeAt);
            this.fetchKeys = fetchKeys;
            this.lowEpoch = lowEpoch == null ? txnId.epoch() : lowEpoch.epoch();
            this.highEpoch = highEpoch == null ? srcEpoch : highEpoch.epoch();
        }

        Ranges localRanges(Node node)
        {
            return node.topology().localRangesForEpochs(lowEpoch, highEpoch);
        }
    }

    public static void fetch(Known fetch, Node node, TxnId txnId, @Nullable Timestamp executeAt, Participants<?> someKeys, @Nullable EpochSupplier localLowEpoch, @Nullable EpochSupplier localHighEpoch, BiConsumer<? super FetchResult, Throwable> callback)
    {
        FetchRequest request = new FetchRequest(fetch, txnId, executeAt, someKeys, localLowEpoch, localHighEpoch, callback);
        if (someKeys.kind().isRoute()) fetch(node, castToRoute(someKeys), request);
        else fetchViaSomeRoute(node, someKeys, request);
    }

    public static void fetch(Node node, Route<?> route, FetchRequest request)
    {
        long srcEpoch = request.srcEpoch;
        if (!node.topology().hasEpoch(srcEpoch))
        {
            node.withEpoch(srcEpoch, request.callback, () -> fetch(node, route, request));
            return;
        }

        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        Ranges ranges = request.localRanges(node);
        if (!Route.isFullRoute(route))
        {
            fetchWithIncompleteRoute(node, route, request);
        }
        else
        {
            Route<?> slicedRoute = route.slice(ranges);
            fetchInternal(node, ranges, slicedRoute, request);
        }
    }

    /**
     * Do not make an attempt to discern what keys need to be contacted; fetch from only the specific remote keys that were requested.
     */
    public static void fetchSpecific(Known fetch, Node node, TxnId txnId, Route<?> query, Route<?> maxRoute, Participants<?> localKeys, @Nullable EpochSupplier lowEpoch, @Nullable EpochSupplier highEpoch, @Nullable Timestamp executeAt, BiConsumer<? super FetchResult, Throwable> callback)
    {
        fetchSpecific(node, query, maxRoute, new FetchRequest(fetch, txnId, executeAt, localKeys, lowEpoch, highEpoch, callback));
    }

    public static void fetchSpecific(Node node, Route<?> query, Route<?> maxRoute, FetchRequest request)
    {
        long srcEpoch = request.srcEpoch;
        if (!node.topology().hasEpoch(srcEpoch))
        {
            node.withEpoch(srcEpoch, request.callback, () -> fetchSpecific(node, query, maxRoute, request));
            return;
        }

        fetchData(node, query, maxRoute, request);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void fetchViaSomeRoute(Node node, Participants<?> someUnseekables, FetchRequest request)
    {
        TxnId txnId = request.txnId;
        FindSomeRoute.findSomeRoute(node, request.txnId, someUnseekables, (foundRoute, fail) -> {
            if (fail != null) request.callback.accept(null, fail);
            else if (foundRoute.route == null)
            {
                reportRouteNotFound(node, foundRoute.known, request);
            }
            else if (isFullRoute(foundRoute.route))
            {
                fetch(node, Route.castToFullRoute(foundRoute.route), request);
            }
            else if (isRoute(someUnseekables) && someUnseekables.containsAll(foundRoute.route))
            {
                // this is essentially a reentrancy check; we can only reach this point if we have already tried once to fetchSomeRoute
                // (as a user-provided Route is used to fetchRoute, not fetchSomeRoute)
                reportRouteNotFound(node, foundRoute.known, request);
            }
            else
            {
                Route<?> route = foundRoute.route;
                if (isRoute(someUnseekables))
                    route = Route.merge(route, (Route)someUnseekables);
                fetch(node, route, request);
            }
        });
    }

    private static void reportRouteNotFound(Node node, Known found, FetchRequest req)
    {
        Invariants.checkState(req.executeAt == null);
        TxnId txnId = req.txnId;
        switch (found.outcome)
        {
            default: throw new AssertionError("Unknown outcome: " + found.outcome);
            case Invalidated:
                locallyInvalidateAndCallback(node, txnId, constant(req.lowEpoch), constant(req.highEpoch), req.fetchKeys, new FetchResult(found, req.fetchKeys, null), req.callback);
                break;

            case Unknown:
                if (found.canProposeInvalidation())
                {
                    Invalidate.invalidate(node, txnId, req.fetchKeys, (outcome, throwable) -> {
                        FetchResult result = null;
                        if (throwable == null)
                        {
                            Known achieved = Known.Invalidated;
                            Unseekables<?> achievedTarget = req.fetchKeys, didNotAchieveTarget = null;
                            if (outcome.asProgressToken().status != Status.Invalidated)
                            {
                                achievedTarget = req.fetchKeys.slice(0, 0);
                                didNotAchieveTarget = req.fetchKeys;
                                achieved = Known.Nothing;
                            }
                            result = new FetchResult(achieved, achievedTarget, didNotAchieveTarget);
                        }
                        req.callback.accept(result, throwable);
                    });
                    break;
                }
            case Erased:
            case WasApply:
            case Apply:
                // TODO (expected): we may be stale
                req.callback.accept(new FetchResult(found, req.fetchKeys.slice(0, 0), req.fetchKeys), null);
        }
    }

    private static void fetchWithIncompleteRoute(Node node, Route<?> someRoute, FetchRequest request)
    {
        long srcEpoch = request.srcEpoch;
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        FindRoute.findRoute(node, request.txnId, someRoute.withHomeKey(), (foundRoute, fail) -> {
            if (fail != null) request.callback.accept(null, fail);
            else if (foundRoute == null) fetchViaSomeRoute(node, someRoute, request);
            else fetch(node, foundRoute.route, request);
        });
    }

    public static void fetch(Node node, FullRoute<?> route, FetchRequest request)
    {
        node.withEpoch(request.srcEpoch, request.callback, () -> {
            Ranges ranges = request.localRanges(node);
            fetchInternal(node, ranges, route, request);
        });
    }

    private static Object fetchInternal(Node node, Ranges ranges, Route<?> route, FetchRequest request)
    {
        long srcEpoch = request.srcEpoch;
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        Route<?> slicedRoute = route.slice(ranges);
        return fetchData(node, slicedRoute, route, request);
    }

    final BiConsumer<? super FetchResult, Throwable> callback;
    /**
     * The epoch until which we want to persist any response for locally
     */
    final Known target;
    final Route<?> maxRoute;

    // to support cases where a later epoch that ultimately does not participate in execution has a vestigial entry
    // (i.e. if preaccept/accept contact a later epoch than execution is decided for)
    final long lowEpoch, highEpoch;

    final Unseekables<?> propagateTo;

    private FetchData(Node node, Known target, TxnId txnId, Route<?> route, Route<?> maxRoute, Unseekables<?> propagateTo, long sourceEpoch, long lowEpoch, long highEpoch, BiConsumer<? super FetchResult, Throwable> callback)
    {
        this(node, target, txnId, route, route.withHomeKey(), maxRoute, propagateTo, sourceEpoch, lowEpoch, highEpoch, callback);
    }

    private FetchData(Node node, Known target, TxnId txnId, Route<?> route, Route<?> routeWithHomeKey, Route<?> maxRoute, Unseekables<?> propagateTo, long sourceEpoch, long lowEpoch, long highEpoch, BiConsumer<? super FetchResult, Throwable> callback)
    {
        // TODO (desired, efficiency): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, txnId, routeWithHomeKey, sourceEpoch, CheckStatus.IncludeInfo.All);
        this.propagateTo = propagateTo;
        this.lowEpoch = lowEpoch;
        this.maxRoute = maxRoute;
        Invariants.checkArgument(routeWithHomeKey.contains(route.homeKey()), "route %s does not contain %s", routeWithHomeKey, route.homeKey());
        this.target = target;
        this.highEpoch = highEpoch;
        this.callback = callback;
    }

    private static FetchData fetchData(Node node, Route<?> route, Route<?> maxRoute, FetchRequest req)
    {
        Invariants.checkState(!req.fetchKeys.isEmpty());
        FetchData fetch = new FetchData(node, req.fetch, req.txnId, route, maxRoute, req.fetchKeys, req.srcEpoch, req.lowEpoch, req.highEpoch, req.callback);
        fetch.start();
        return fetch;
    }

    private static FetchData fetchData(Node node, Known fetch, TxnId txnId, Route<?> route, Route<?> maxRoute, Unseekables<?> localKeys, long sourceEpoch, long lowEpoch, long highEpoch, BiConsumer<? super FetchResult, Throwable> callback)
    {
        Invariants.checkState(!localKeys.isEmpty());
        FetchData fetchData = new FetchData(node, fetch, txnId, route, maxRoute, localKeys, sourceEpoch, lowEpoch, highEpoch, callback);
        fetchData.start();
        return fetchData;
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
        return target.isSatisfiedBy(ok.knownFor(txnId, scope.participants()));
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
            Propagate.propagate(node, txnId, sourceEpoch, lowEpoch, highEpoch, success.withQuorum, query(), propagateTo, target, (CheckStatusOkFull) merged, callback);
        }
    }
}
