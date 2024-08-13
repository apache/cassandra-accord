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
import accord.messages.InformDurable;
import accord.primitives.*;
import accord.utils.Invariants;

import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;

import static accord.coordinate.Infer.SafeEraseAndCallback.safeEraseAndCallback;
import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;

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
        return hasMadeProgress(ok) || ok.durability.isDurableOrInvalidated();
    }

    public boolean hasMadeProgress(CheckStatusOk ok)
    {
        // TODO (required, liveness): if Ballot.hlc is stale enough then preempt; also do not query isCoordinating, query directly the node that owns the ballot (or TxnId if Ballot is ZERO)
        return ok != null && (ok.isCoordinating
                              || ok.toProgressToken().compareTo(prevProgress) > 0);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected void onDone(Success success, Throwable fail)
    {
        if (fail != null)
        {
            callback.accept(null, fail);
        }
        else
        {
            Invariants.checkState(merged != null);
            CheckStatusOk full = merged.finish(this.route, success.withQuorum);
            Known known = full.maxKnown();
            Route<?> someRoute = full.route;

            switch (known.outcome)
            {
                default: throw new AssertionError();
                case Unknown:
                    // TODO (required): ErasedOrInvalidated takes Unknown here. This is probably wrong, consider it more carefully.
                    //     Specifically, we should probably not propose invalidation if it's possible the command has been Erased.
                    //     We should probably introduce a MaybeErased Outcome.
                    //     If we only have a partial route, and all end-points we contact are MaybeErased, we are either stale or
                    //     we have enough local information to know the command's outcome is durable.
                    //     If any shard is not MaybeErased but is also Unknown, then we are safe to Invalidate.
                    if (known.canProposeInvalidation() && !Route.isFullRoute(full.route))
                    {
                        // for correctness reasons, we have not necessarily preempted the initial pre-accept round and
                        // may have raced with it, so we must attempt to recover anything we see pre-accepted.
                        Invalidate.invalidate(node, txnId, someRoute, callback);
                        break;
                    }
                    // fall through otherwise to recovery

                case Apply:
                    // we have included the home key, and one that witnessed the definition has responded, so it should also know the full route
                    if (hasMadeProgress(full))
                    {
                        if (full.durability.isDurable())
                            node.send(topologies.forEpoch(txnId.epoch()).forKey(route.homeKey()).nodes, to -> new InformDurable(to, topologies, route, txnId, full.executeAtIfKnown(), full.durability));
                        callback.accept(full.toProgressToken(), null);
                    }
                    else
                    {
                        Invariants.checkState(Route.isFullRoute(someRoute), "Require a full route but given %s", full.route);
                        node.recover(txnId, Route.castToFullRoute(someRoute)).addCallback(callback);
                    }
                    break;

                case WasApply:
                    callback.accept(full.toProgressToken(), null);
                    break;

                case Invalidated:
                    locallyInvalidateAndCallback(node, txnId, txnId, someRoute, full.toProgressToken(), callback);
                    break;

                case Erased:
                    // TODO (required): this isn't valid. This is either an invalidated command or we're stale. Most likely the latter.
                    //   this is because Erased is only permitted to be adopted when every shard has made the command durable.
                    Invariants.checkState(!full.knownFor(route.participants()).isInvalidated());
                    safeEraseAndCallback(node, txnId, txnId, someRoute, full.toProgressToken(), callback);
            }
        }
    }
}
