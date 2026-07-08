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
import accord.local.Commands;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.Known;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.primitives.RoutingKeys;
import accord.primitives.WithQuorum;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.TxnId;
import accord.topology.TopologyException;

import static accord.coordinate.Infer.InvalidIf.NotKnownToBeInvalid;
import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.local.CommandStores.*;
import static accord.primitives.Known.Nothing;
import static accord.primitives.WithQuorum.HasQuorum;

/**
 * Find some Route for a txnId using some known participants
 */
public class FetchRoute extends CheckShards<Route<?>, Participants<?>>
{
    final LatentStoreSelector reportTo;

    FetchRoute(Node node, TxnId txnId, Infer.InvalidIf invalidIf, Participants<?> contactable, LatentStoreSelector reportTo, BiConsumer<Route<?>, Throwable> callback) throws TopologyException
    {
        super(node, node.someExclusiveExecutor(), txnId, contactable, txnId.epoch(), IncludeInfo.Route, null, invalidIf, callback);
        this.reportTo = reportTo;
    }

    public static Object fetchRoute(Node node, TxnId txnId, Infer.InvalidIf invalidIf, Participants<?> participants, LatentStoreSelector reportTo, BiConsumer<Route<?>, Throwable> callback)
    {
        Tracing tracing = node.agent().trace(txnId, participants, CoordinationKind.FetchRoute);
        return fetchRoute(node, txnId, invalidIf, participants, reportTo, callback, tracing);
    }

    private static Object fetchRoute(Node node, TxnId txnId, Infer.InvalidIf invalidIf, Participants<?> participants, LatentStoreSelector reportTo, BiConsumer<Route<?>, Throwable> callback, @Nullable Tracing tracing)
    {
        if (!node.topology().active().hasAtLeastEpoch(txnId.epoch()))
        {
            if (tracing != null)
                tracing.trace(null, "Waiting for epoch %d", txnId.epoch());
            return node.withEpochAtLeast(txnId.epoch(), null, callback, () -> fetchRoute(node, txnId, invalidIf, participants, reportTo, callback, tracing));
        }

        FetchRoute fetchRoute;
        try
        {
            fetchRoute = new FetchRoute(node, txnId, invalidIf, participants, reportTo, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return null;
        }
        fetchRoute.start();
        return fetchRoute;
    }

    public static Object fetchRoute(Node node, TxnId txnId, Participants<?> contactable, LatentStoreSelector reportTo, BiConsumer<Route<?>, Throwable> callback)
    {
        return fetchRoute(node, txnId, NotKnownToBeInvalid, contactable, reportTo, callback, null);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return Route.isFullRoute(ok.route);
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure != null) invokeCallback(null, failure);
        else
        {
            final Route<?> route = merged == null ? null : merged.route;
            if (route == null)
            {
                Known known = Nothing;
                if (merged != null)
                    known = merged.finish(query, query, query, success.withQuorum, previouslyKnownToBeInvalidIf).knownFor(txnId, query, query);
                reportRouteNotFound(node, success.withQuorum, known, txnId, query, reportTo, takeCallback(), tracing);
            }
            else
            {
                StoreSelector selector = reportTo.refine(txnId, null, query);
                node.commandStores().mapReduceConsume(selector, new MapReduceConsumeCommandStores<>(RoutingKeys.EMPTY)
                {
                    @Override
                    public TxnId primaryTxnId()
                    {
                        return txnId;
                    }

                    @Override
                    public String reason()
                    {
                        return "Report Route";
                    }

                    @Override
                    public void accept(Object result, Throwable failure)
                    {
                        invokeCallback(route, null);
                    }

                    @Override
                    public Object applyInternal(SafeCommandStore safeStore)
                    {
                        SafeCommand safeCommand = safeStore.ifInitialised(txnId);
                        if (safeCommand != null)
                            Commands.updateRoute(safeStore, safeCommand, route);
                        return null;
                    }

                    @Override
                    public Object reduce(Object o1, Object o2)
                    {
                        return null;
                    }
                });
            }
        }
    }

    private static void reportRouteNotFound(Node node, WithQuorum withQuorum, Known found, TxnId txnId, Participants<?> participants, LatentStoreSelector reportTo, BiConsumer<? super Route<?>, Throwable> callback, @Nullable Tracing tracing)
    {
        switch (found.outcome())
        {
            default: throw new AssertionError("Unknown outcome: " + found.outcome());
            case Abort:
                if (tracing != null)
                    tracing.trace(null, "No Route. Found %s; invalidating", found);

                locallyInvalidateAndCallback(node, txnId, reportTo.refine(txnId, null, participants), participants, null, callback, tracing);
                break;

            case Unknown:
                if (withQuorum == HasQuorum && found.canProposeInvalidation())
                {
                    Invalidate.invalidate(node, txnId, participants, false, reportTo, (outcome, throwable) -> callback.accept(null, throwable));
                    break;
                }
                else if (tracing != null)
                {
                    tracing.trace(null, "No Route. Found %s; cannot invalidate (%s, %s)", found, withQuorum, found.canProposeInvalidation());
                }
            case Erased:
            case WasApply:
            case Apply:
                callback.accept(null, null);
        }
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.FetchRoute;
    }
}
