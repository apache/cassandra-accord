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

import accord.api.Tracing;
import accord.coordinate.Infer.InvalidIf;
import accord.local.CommandStores.LatentStoreSelector;
import accord.local.CommandStores.StoreSelector;
import accord.local.Node;
import accord.local.SequentialAsyncExecutor;
import accord.primitives.Known;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.Propagate;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;

import javax.annotation.Nonnull;

import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;

/**
 * Find data and persist locally
 *
 * TODO (expected): avoid multiple command stores performing duplicate queries to other shards
 */
public class FetchData extends CheckShards<FetchData.FetchResult, Route<?>>
{
    public static class FetchResult
    {
        public final @Nonnull Known target;
        public final Unseekables<?> achievedTarget;
        /**
         * Report the minimum knowledge we found for the keys we queried OR for all the local keys we are updating (if greater)
         */
        public final Known foundAtLeast;

        public FetchResult(@Nonnull Known target, Unseekables<?> achievedTarget, Known foundAtLeast)
        {
            this.target = target;
            this.achievedTarget = achievedTarget;
            this.foundAtLeast = foundAtLeast;
        }
    }

    // TODO (expected): separate keys we fetch deps and txns for
    public static class FetchRequest implements BiConsumer<FetchResult, Throwable>
    {
        final SequentialAsyncExecutor executor;
        final Known fetch;
        final TxnId txnId;
        final InvalidIf invalidIf;
        final @Nullable Timestamp executeAt;
        final long srcEpoch;
        // known participants, a subset of which we may fetch from
        final Participants<?> contactable;
        final LatentStoreSelector reportTo;
        final BiConsumer<? super FetchResult, Throwable> callback;
        final @Nullable Tracing tracing;

        public FetchRequest(SequentialAsyncExecutor executor, Known fetch, TxnId txnId, InvalidIf invalidIf, @Nullable Timestamp executeAt, Participants<?> contactable, LatentStoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback, @Nullable Tracing tracing)
        {
            this.executor = executor;
            this.fetch = fetch;
            this.invalidIf = invalidIf;
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.callback = callback;
            this.srcEpoch = fetch.fetchEpoch(txnId, executeAt);
            this.contactable = contactable;
            this.reportTo = reportTo;
            this.tracing = tracing;
        }

        @Override
        public void accept(FetchResult success, Throwable fail)
        {
            callback.accept(success, fail);
        }
    }

    /**
     * Do not make an attempt to discern what keys need to be contacted; fetch from only the specific remote keys that were requested.
     */
    public static Object fetchSpecific(Known fetch, Node node, TxnId txnId, @Nullable Timestamp executeAt, Route<?> query, Route<?> maxRoute, LatentStoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        return fetchSpecific(fetch, node, txnId, NotKnownToBeInvalid, executeAt, query, maxRoute, reportTo, callback);
    }

    /**
     * Do not make an attempt to discern what keys need to be contacted; fetch from only the specific remote keys that were requested.
     */
    public static Object fetchSpecific(Known fetch, Node node, TxnId txnId, InvalidIf invalidIf, @Nullable Timestamp executeAt, Route<?> query, Route<?> maxRoute, LatentStoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        return fetchSpecific(node, query, maxRoute, new FetchRequest(node.someSequentialExecutor(), fetch, txnId, invalidIf, executeAt, maxRoute, reportTo, callback, null));
    }

    public static Object fetchSpecific(Node node, Route<?> query, Route<?> maxRoute, FetchRequest request)
    {
        long srcEpoch = request.srcEpoch;
        if (!node.topology().hasAtLeastEpoch(srcEpoch))
        {
            if (request.tracing != null)
                request.tracing.trace(null, "Epoch %d not ready; waiting", srcEpoch);
            return node.withEpochAtLeast(srcEpoch, request.executor, request, () -> fetchSpecific(node, query, maxRoute, request));
        }

        return fetchData(node, query, maxRoute, request);
    }

    /**
     * The epoch until which we want to persist any response for locally
     */
    final Known target;
    final Route<?> maxRoute;

    // to support cases where a later epoch that ultimately does not participate in execution has a vestigial entry
    // (i.e. if preaccept/accept contact a later epoch than execution is decided for)
    final LatentStoreSelector reportTo;

    private FetchData(Node node, Known target, TxnId txnId, InvalidIf invalidIf, Route<?> route, Route<?> maxRoute, long sourceEpoch, LatentStoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        this(node, target, txnId, invalidIf, route, route.withHomeKey(), maxRoute, sourceEpoch, reportTo, callback);
    }

    private FetchData(Node node, Known target, TxnId txnId, InvalidIf invalidIf, Route<?> route, Route<?> routeWithHomeKey, Route<?> maxRoute, long sourceEpoch, LatentStoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        // TODO (desired, efficiency): restore behaviour of only collecting info if e.g. Committed or Executed
        super(node, node.someSequentialExecutor(), txnId, routeWithHomeKey, sourceEpoch, CheckStatus.IncludeInfo.All, null, invalidIf, callback);
        this.reportTo = reportTo;
        this.maxRoute = maxRoute;
        Invariants.requireArgument(routeWithHomeKey.contains(route.homeKey()), "route %s does not contain %s", routeWithHomeKey, route.homeKey());
        this.target = target;
    }

    private static FetchData fetchData(Node node, Route<?> route, Route<?> maxRoute, FetchRequest req)
    {
        Invariants.require(!req.contactable.isEmpty());
        FetchData fetch = new FetchData(node, req.fetch, req.txnId, req.invalidIf, route, maxRoute, req.srcEpoch, req.reportTo, req.callback);
        fetch.start();
        return fetch;
    }

    private static FetchData fetchData(Node node, Known fetch, TxnId txnId, InvalidIf invalidIf, Route<?> route, Route<?> maxRoute, long sourceEpoch, StoreSelector reportTo, BiConsumer<? super FetchResult, Throwable> callback)
    {
        FetchData fetchData = new FetchData(node, fetch, txnId, invalidIf, route, maxRoute, sourceEpoch, reportTo, callback);
        fetchData.start();
        return fetchData;
    }

    protected Route<?> query()
    {
        return query;
    }

    @Override
    protected boolean isSufficient(Node.Id from, CheckStatus.CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().computeRangesForNode(from);
        Route<?> scope = this.query.slice(rangesForNode);
        return isSufficient(scope, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatus.CheckStatusOk ok)
    {
        return isSufficient(query, ok);
    }

    protected boolean isSufficient(Route<?> scope, CheckStatus.CheckStatusOk ok)
    {
        return target.isSatisfiedBy(ok.knownFor(txnId, scope.participants(), scope.participants()));
    }

    @Override
    protected void onDone(ReadCoordinator.Success success, Throwable failure)
    {
        Invariants.require((success == null) != (failure == null));
        if (failure != null)
        {
            invokeCallback(null, failure);
        }
        else
        {
            if (success == ReadCoordinator.Success.Success)
                Invariants.require(isSufficient(merged), "Status %s is not sufficient", merged);

            // TODO (expected): should we automatically trigger a new fetch if we find executeAt but did not request enough information? would be more robust
            Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, success.withQuorum, query(), maxRoute, reportTo, target, (CheckStatusOkFull) merged, takeCallback(), tracing);
        }
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.Fetch;
    }

    @Override
    public String describe()
    {
        return super.describe() + ", target=" + target;
    }
}
