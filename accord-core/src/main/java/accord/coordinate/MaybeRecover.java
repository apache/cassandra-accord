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

import accord.local.Status.Known;
import accord.primitives.*;
import accord.utils.Invariants;

import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;

import static accord.primitives.ProgressToken.TRUNCATED;
import static accord.utils.Functions.reduceNonNull;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class MaybeRecover extends CheckShards<Route<?>>
{
    final ProgressToken prevProgress;
    final BiConsumer<Outcome, Throwable> callback;

    MaybeRecover(Node node, TxnId txnId, Route<?> someRoute, ProgressToken prevProgress, BiConsumer<Outcome, Throwable> callback)
    {
        // we only want to enquire with the home shard, but we prefer maximal route information for running Invalidation against, if necessary
        super(node, txnId, someRoute.withHomeKey(), IncludeInfo.Route);
        this.prevProgress = prevProgress;
        this.callback = callback;
    }

    public static void maybeRecover(Node node, TxnId txnId, Route<?> someRoute, ProgressToken prevProgress, BiConsumer<Outcome, Throwable> callback)
    {
        MaybeRecover maybeRecover = new MaybeRecover(node, txnId, someRoute, prevProgress, callback);
        maybeRecover.start();
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return hasMadeProgress(ok) || ok.truncated;
    }

    public boolean hasMadeProgress(CheckStatusOk ok)
    {
        return ok != null && (ok.isCoordinating // TODO (required, liveness): make this coordinatingSince so can be pre-empted by others if stuck
                              || ok.toProgressToken().compareTo(prevProgress) > 0);
    }

    @Override
    protected void onDone(Success success, Throwable fail)
    {
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else if (!truncated)
        {
            Invariants.checkState(merged != null);
            Known known = merged.saveStatus.known;

            switch (known.outcome)
            {
                default: throw new AssertionError();
                case Unknown:
                    if (known.canProposeInvalidation() && !Route.isFullRoute(merged.route))
                    {
                        // order important, as route could be a Route which does not implement RoutingKeys.union
                        Route<?> someRoute = reduceNonNull(Route::union, (Route)this.route, merged.route);
                        // for correctness reasons, we have not necessarily preempted the initial pre-accept round and
                        // may have raced with it, so we must attempt to recover anything we see pre-accepted.
                        Invalidate.invalidate(node, txnId, someRoute.withHomeKey(), callback);
                        break;
                    }

                case Applying:
                case Applied:
                    // we have included the home key, and one that witnessed the definition has responded, so it should also know the full route
                    if (hasMadeProgress(merged))
                    {
                        callback.accept(merged.toProgressToken(), null);
                    }
                    else
                    {
                        Invariants.checkState(Route.isFullRoute(merged.route));
                        node.recover(txnId, Route.castToFullRoute(merged.route)).addCallback(callback);
                    }
                    break;

                case Invalidate:
                    // TODO (easy, efficiency): we should simply invoke commitInvalidate
                    Route<?> someRoute = reduceNonNull(Route::union, (Route)this.route, merged.route);
                    Invalidate.invalidate(node, txnId, someRoute.withHomeKey(), callback);
                    break;

                case Truncated:
                case TruncatedApply:
                    callback.accept(merged.toProgressToken(), null);
            }
        }
        else
        {
            callback.accept(TRUNCATED, null);
        }
    }
}
