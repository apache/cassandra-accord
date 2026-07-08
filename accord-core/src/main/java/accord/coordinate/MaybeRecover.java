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

import accord.local.CommandStores.LatentStoreSelector;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.InformDurable;
import accord.primitives.*;
import accord.topology.TopologyException;
import accord.utils.Invariants;

import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.IncludeInfo;
import accord.utils.UnhandledEnum;

import static accord.coordinate.Infer.InvalidateAndCallback.locallyInvalidateAndCallback;
import static accord.messages.Commit.Invalidate.commitInvalidate;
import static accord.primitives.Known.Outcome.Unknown;
import static accord.primitives.WithQuorum.HasQuorum;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class MaybeRecover extends CheckShards<Outcome, Route<?>>
{
    final ProgressToken prevProgress;
    final boolean recoverIfAlreadyDurable;
    final LatentStoreSelector reportTo;

    MaybeRecover(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, Infer.InvalidIf invalidIf, Route<?> someRoute, ProgressToken prevProgress, boolean recoverIfAlreadyDurable, LatentStoreSelector reportTo, BiConsumer<? super Outcome, Throwable> callback) throws TopologyException
    {
        // we only want to enquire with the home shard, but we prefer maximal route information for running Invalidation against, if necessary
        super(node, executor, txnId, someRoute.withHomeKey(), IncludeInfo.Route, null, invalidIf, callback);
        this.prevProgress = prevProgress;
        this.recoverIfAlreadyDurable = recoverIfAlreadyDurable;
        this.reportTo = reportTo;
    }
    public static Object maybeRecover(Node node, TxnId txnId, Infer.InvalidIf invalidIf, Route<?> someRoute, ProgressToken prevProgress, boolean recoverIfAlreadyDurable, LatentStoreSelector reportTo, BiConsumer<? super Outcome, Throwable> callback)
    {
        MaybeRecover maybeRecover;
        try
        {
            maybeRecover = new MaybeRecover(node, node.someExclusiveExecutor(), txnId, invalidIf, someRoute, prevProgress, recoverIfAlreadyDurable, reportTo, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return null;
        }
        maybeRecover.start();
        return maybeRecover;
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        // We don't accept a single response if any are truncated - must have a quorum so we can make inferences about invalidation
        return !merged.map.hasTruncated() && hasMadeProgress(ok);
    }

    public boolean hasMadeProgress(CheckStatusOk ok)
    {
        return (ok.durability.isDurable() && !recoverIfAlreadyDurable && !prevProgress.outcome.isDurableOrInvalidated())
               || ok.isCoordinating || ok.toProgressToken().compareTo(prevProgress) > 0;
    }

    @Override
    protected void onDone(Success success, Throwable fail)
    {
        // TODO (desired): we don't need a full quorum to proceed, just a quorum that intersects a full quorum (i.e. when rf=2, we need only 1 reply)
        //  this can be helpful in mitigating flakiness and helping forward progress for large transactions spanning many shards
        if (fail != null)
        {
            invokeCallback(null, fail);
        }
        else
        {
            Invariants.require(merged != null);
            CheckStatusOk full = merged.finish(this.query, this.query, this.query, success.withQuorum, previouslyKnownToBeInvalidIf);
            if (tracing != null)
                tracing.trace(null, "merged: " + full);

            Known known = full.maxKnown();
            Route<?> someRoute = full.route;

            switch (known.outcome())
            {
                default: throw new UnhandledEnum(known.outcome());
                case Unknown:
                {
                    // ErasedOrInvalidated takes Unknown, and so permits invalidation to be initiated.
                    // This might prima facie seem unsafe, as ErasedOrInvalidated might mean the command
                    // has been executed and erased, in which case it is not safe to invalidate.
                    // However, replicas that have erased the history for these commands also cannot vote to proceed with
                    // invalidation, so the Invalidation state machine must special-case this scenario.
                    // If there exists a shard that has not been decided, then the outcome must be invalidated and it
                    // may be disseminated globally. However, if all shards are erased then the outcome must be
                    // decided locally by the application of GC points.
                    // TODO (expected): replicas may be stale in this case, and should detect this and stop attempting to coordinate/invalidate.
                    if (success.withQuorum == HasQuorum && known.canProposeInvalidation() && !Route.isFullRoute(full.route))
                    {
                        if (tracing != null)
                            tracing.trace(null, "found quorum permitting invalidation: " + known);

                        // for correctness reasons, we have not necessarily preempted the initial pre-accept round and
                        // may have raced with it, so we must attempt to recover anything we see pre-accepted.
                        Invalidate.invalidate(node, txnId, someRoute, takeCallback());
                        break;
                    }
                    if (tracing != null)
                        tracing.trace(null, "found quorum of Unknown that did not permit invalidation; falling through.");
                    // fall through otherwise to recovery
                }
                case WasApply:
                {
                    if (merged.durability.isDurable() && full.minMaxKnown(someRoute.homeKey()).outcome().isOrWasApply())
                    {
                        ProgressToken progressToken = full.toProgressToken();
                        if (tracing != null)
                            tracing.trace(null, "found %s which need not be recovered; reporting %s", known.outcome(), progressToken);
                        invokeCallback(progressToken, null);
                        break;
                    }
                }
                case Apply:
                {
                    // we have included the home key, and one that witnessed the definition has responded, so it should also know the full route
                    if (hasMadeProgress(full) || !Route.isFullRoute(someRoute))
                    {
                        ProgressToken progressToken = full.toProgressToken();
                        if (tracing != null)
                            tracing.trace(null, "found %s; reporting progress token %s", hasMadeProgress(full) ? "progress" : "no route", progressToken);
                        if (full.durability.isDurable())
                            InformDurable.informDefault(node, topologies, txnId, query, bumpBallot, full.executeAtIfKnown(), null, full.durability, tracing);
                        invokeCallback(full.toProgressToken(), null);
                    }
                    else
                    {
                        Invariants.expect(!full.durability.isDurableOrInvalidated() || recoverIfAlreadyDurable);
                        if (tracing != null)
                            tracing.trace(null, "invoking RecoverWithRoute");
                        node.recover(txnId, full.invalidIf, Route.castToFullRoute(someRoute), reportTo).begin(takeCallback());
                    }
                    break;
                }
                case Erased:
                {
                    if (full.minMaxKnown(someRoute.homeKey()).outcome() == Unknown)
                    {
                        if (previouslyKnownToBeInvalidIf != full.invalidIf)
                        {
                            MaybeRecover.maybeRecover(node, txnId, full.invalidIf, someRoute, full.toProgressToken(), recoverIfAlreadyDurable, reportTo, takeCallback());
                            break;
                        }
                        else Invariants.expect(false, "Expect home shard to know outcome before completing recovery");
                    }
                    ProgressToken progressToken = full.toProgressToken();
                    if (tracing != null)
                        tracing.trace(null, "found %s which cannot be recovered; reporting %s", known.outcome(), progressToken);
                    invokeCallback(progressToken, null);
                    break;
                }
                case Abort:
                {
                    if (tracing != null)
                        tracing.trace(null, "found Abort; invalidating locally");

                    commitInvalidate(node, txnId, Route.merge(full.route, (Route) query), txnId.epoch(), tracing);
                    locallyInvalidateAndCallback(node, txnId, txnId.epoch(), txnId.epoch(), someRoute, full.toProgressToken(), takeCallback(), null);
                    break;
                }
            }
        }
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.MaybeRecover;
    }

    @Override
    public String describe()
    {
        return super.describe() + ", prevProgress=" + prevProgress;
    }
}
