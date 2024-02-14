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

import accord.local.Node;
import accord.local.Status;
import accord.local.Status.Known;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.Propagate;
import accord.primitives.*;
import accord.utils.Invariants;

import javax.annotation.Nullable;

import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
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
        return fetch(fetch, node, txnId, someUnseekables, null, null, callback);
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        if (someUnseekables.kind().isRoute()) return fetch(fetch, node, txnId, castToRoute(someUnseekables), forLocalEpoch, executeAt, callback);
        else return fetchViaSomeRoute(fetch, node, txnId, someUnseekables, forLocalEpoch, executeAt, callback);
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, Route<?> route, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        if (!node.topology().hasEpoch(srcEpoch))
            return node.topology().awaitEpoch(srcEpoch).map(ignore -> fetch(fetch, node, txnId, route, forLocalEpoch, executeAt, callback)).beginAsResult();

        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        long toLocalEpoch = Math.max(srcEpoch, forLocalEpoch == null ? 0 : forLocalEpoch.epoch());
        Ranges ranges = node.topology().localRangesForEpochs(txnId.epoch(), toLocalEpoch);
        if (!route.covers(ranges))
        {
            return fetchWithIncompleteRoute(fetch, node, txnId, route, forLocalEpoch, executeAt, callback);
        }
        else
        {
            return fetchInternal(ranges, fetch, node, txnId, route.sliceStrict(ranges), executeAt, callback);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object fetchViaSomeRoute(Known fetch, Node node, TxnId txnId, Unseekables<?> someUnseekables, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
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
                fetch(fetch, node, txnId, route, forLocalEpoch, executeAt, callback);
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
                // TODO (required): we may now be stale
                callback.accept(found, null);
        }
    }

    private static Object fetchWithIncompleteRoute(Known fetch, Node node, TxnId txnId, Route<?> someRoute, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        long srcEpoch = fetch.fetchEpoch(txnId, executeAt);
        Invariants.checkArgument(node.topology().hasEpoch(srcEpoch), "Unknown epoch %d, latest known is %d", srcEpoch, node.epoch());
        return FindRoute.findRoute(node, txnId, someRoute.withHomeKey(), (foundRoute, fail) -> {
            if (fail != null) callback.accept(null, fail);
            else if (foundRoute == null) fetchViaSomeRoute(fetch, node, txnId, someRoute, forLocalEpoch, executeAt, callback);
            else fetch(fetch, node, txnId, foundRoute.route, forLocalEpoch, foundRoute.executeAt, callback);
        });
    }

    public static Object fetch(Known fetch, Node node, TxnId txnId, FullRoute<?> route, @Nullable EpochSupplier forLocalEpoch, @Nullable Timestamp executeAt, BiConsumer<? super Known, Throwable> callback)
    {
        return node.awaitEpoch(executeAt).map(ignore -> {
            long toEpoch = Math.max(fetch.fetchEpoch(txnId, executeAt), forLocalEpoch == null ? 0 : forLocalEpoch.epoch());
            Ranges ranges = node.topology().localRangesForEpochs(txnId.epoch(), toEpoch);
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
            Propagate.propagate(node, txnId, sourceEpoch, success.withQuorum, route(), target, (CheckStatusOkFull) merged, callback);
        }
    }
}
