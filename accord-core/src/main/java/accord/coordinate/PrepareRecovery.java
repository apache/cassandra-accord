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

import accord.local.CommandStores.LatentStoreSelector;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.ExclusiveAsyncExecutor;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.Commit;
import accord.messages.Propagate;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Known;
import accord.primitives.LatestDeps;
import accord.primitives.Participants;
import accord.primitives.ProgressToken;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topologies;
import accord.topology.TopologyException;
import accord.utils.Invariants;
import accord.utils.Rethrowable;

import static accord.coordinate.CoordinationAdapter.Factory.Kind.Recovery;
import static accord.coordinate.ReadCoordinator.Success.Quorum;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.Outcome.Apply;
import static accord.primitives.ProgressToken.APPLIED;
import static accord.primitives.ProgressToken.INVALIDATED;
import static accord.primitives.ProgressToken.TRUNCATED_DURABLE_OR_INVALIDATED;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.Durability.AllQuorums;
import static accord.topology.SelectShards.ALL;
import static accord.utils.Invariants.illegalState;

public class PrepareRecovery extends CheckShards<Outcome, FullRoute<?>>
{
    final Status witnessedByInvalidation;
    final LatentStoreSelector reportTo;

    private PrepareRecovery(Node node, ExclusiveAsyncExecutor executor, Topologies topologies, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, Status witnessedByInvalidation, LatentStoreSelector reportTo, BiConsumer<? super Outcome, Throwable> callback) throws TopologyException
    {
        super(node, executor, txnId, route, IncludeInfo.All, node.uniqueTimestamp(Ballot::fromValues), invalidIf, callback);
        this.reportTo = reportTo;
        // if witnessedByInvalidation == AcceptedInvalidate then we cannot assume its definition was known, and our comparison with the status is invalid
        Invariants.require(witnessedByInvalidation != Status.AcceptedInvalidate);
        // if witnessedByInvalidation == Invalidated we should anyway not be recovering
        Invariants.require(witnessedByInvalidation != Status.Invalidated);
        this.witnessedByInvalidation = witnessedByInvalidation;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch();
    }

    public static void recover(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, @Nullable Status witnessedByInvalidation, LatentStoreSelector reportTo, BiConsumer<? super Outcome, Throwable> callback)
    {
        PrepareRecovery recover;
        try
        {
            Topologies topologies = node.topology().active().forEpoch(route, txnId.epoch(), ALL);
            recover = new PrepareRecovery(node, executor, topologies, txnId, invalidIf, route, witnessedByInvalidation, reportTo, callback);
        }
        catch (Throwable t)
        {
            callback.accept(null, t);
            return;
        }
        recover.start();
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new CheckStatus(to, topologies(), txnId, query, sourceEpoch, IncludeInfo.All, bumpBallot), executor, this, tracing);
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().getEpoch(txnId.epoch()).rangesForNode(from);
        Route<?> route = this.query.slice(rangesForNode, Minimal);
        return isSufficient(route, ok);
    }

    @Override
    protected boolean isSufficient(CheckStatusOk ok)
    {
        return isSufficient(query, merged);
    }

    protected boolean isSufficient(Route<?> route, CheckStatusOk ok)
    {
        CheckStatusOkFull full = (CheckStatusOkFull)ok;
        Known sufficientTo = full.knownFor(txnId, route, route);
        if (!sufficientTo.isDefinitionKnown())
            return false;

        if (sufficientTo.outcome().isInvalidated())
            return true;

        Invariants.require(full.partialTxn.covers(route));
        return true;
    }

    @Override
    protected void onDone(Success success, Throwable failure)
    {
        if (failure != null)
        {
            invokeCallback(null, failure);
            return;
        }

        CheckStatusOkFull full = ((CheckStatusOkFull) this.merged).finish(query, query, query, success.withQuorum, previouslyKnownToBeInvalidIf);
        Known known = full.knownFor(txnId, query, query);
        if (tracing != null)
            tracing.trace(null, "merged: " + full);

        // TODO (required): audit this logic, and centralise with e.g. FetchData inferences
        // TODO (expected): skip straight to ExecuteTxn if we have a Stable reply from each shard
        switch (known.outcome())
        {
            default: throw new AssertionError();
            case Unknown:
            {
                if (known.definition().isKnown())
                {
                    if (tracing != null)
                        tracing.trace(null, "found definition; invoking Recover.");

                    Txn txn = full.partialTxn.reconstitute(query);
                    Recover.recover(node, txnId, txn, query, full.durability.isFastPathDurablyDecided(), reportTo, takeCallback());
                }
                else if (!known.definition().isOrWasKnown())
                {
                    if (tracing != null)
                        tracing.trace(null, "found no current or erased transaction; invoking Invalidate.");

                    if (witnessedByInvalidation != null && witnessedByInvalidation.compareTo(Status.PreAccepted) > 0)
                        throw illegalState("We previously invalidated, finding a status that should be recoverable");

                    Invalidate.invalidate(node, txnId, query, witnessedByInvalidation != null, reportTo, takeCallback());
                }
                else
                {
                    ProgressToken progressToken = full.toProgressToken();
                    if (tracing != null)
                        tracing.trace(null, "found insufficient information to Recover or Invalidate; calling back with %s.", progressToken);
                    invokeCallback(progressToken, null);
                }
                break;
            }
            case WasApply:
            case Apply:
            {
                if (!known.isDefinitionKnown())
                {
                    if (!known.isTruncated() && !known.isInvalidated())
                    {
                        if (tracing != null)
                            tracing.trace(null, "found Apply/WasApply, but no definition, truncation or invalidation; must have raced with Apply, reporting no progress in expectation next attempt is successful.");

                        // we must have raced with a successful apply, so should simply abort
                        invokeCallback(ProgressToken.NONE, null);
                        return;
                    }

                    // TODO (expected): if we determine new durability, propagate it
                    CheckStatusOkFull propagate;
                    if (!full.map.hasFullyTruncated(query))
                    {
                        // we might have only part of the full transaction, and a shard may have truncated;
                        // in this case we want to skip straight to apply, but only for the shards that haven't truncated
                        Route<?> trySendTo = query.without(full.map.matchingRanges(minMax -> minMax.minOwnedElse(Known.Nothing).isTruncated()));
                        if (!trySendTo.isEmpty())
                        {
                            if (known.isInvalidated())
                            {
                                if (tracing != null)
                                    tracing.trace(null, "found partially truncated Invalidate; committing to shards " + trySendTo);
                                Commit.Invalidate.commitInvalidate(node, txnId, trySendTo, txnId, tracing);
                            }
                            else
                            {
                                known = full.knownFor(txnId, trySendTo, trySendTo);
                                if (known.isDefinitionKnown() && known.is(ApplyAtKnown) && known.outcome() == Apply)
                                {
                                    boolean informDurableOnDone = success == Quorum; // if we have a quorum of truncated responses for the part of the remote we have removed, it is safe to consider this part durable
                                    if (!known.is(DepsKnown))
                                    {
                                        if (tracing != null)
                                            tracing.trace(null, "found partially truncated Apply with incomplete Deps; advancing state machine for shards " + trySendTo);

                                        Invariants.require(txnId.isSystemTxn() || full.partialTxn.covers(trySendTo));
                                        Participants<?> haveStable = full.map.knownFor(Known.DepsOnly, trySendTo);
                                        Route<?> haveUnstable = trySendTo.without(haveStable);
                                        Deps stable = haveStable.isEmpty() ? Deps.NONE : full.stableDeps.reconstitutePartial(haveStable).asFullUnsafe();

                                        LatestDeps.withStable(node.coordinationAdapter(txnId, Recovery), node, executor, txnId, full.executeAt, full.partialTxn, stable, haveUnstable, trySendTo, ALL, query, node.agent(), deps -> {
                                            Deps stableDeps = deps.intersecting(trySendTo);
                                            node.coordinationAdapter(txnId, Recovery).persist(node, executor, null, trySendTo, trySendTo, query, bumpBallot, CoordinationFlags.none(), txnId, full.partialTxn, full.executeAt, stableDeps, full.writes, full.result, informDurableOnDone, null);
                                        });
                                    }
                                    else
                                    {
                                        if (tracing != null)
                                            tracing.trace(null, "found partially truncated Apply; persisting to shards " + trySendTo);

                                        Invariants.require(full.stableDeps.covers(trySendTo));
                                        Invariants.require(txnId.isSystemTxn() || full.partialTxn.covers(trySendTo));
                                        node.coordinationAdapter(txnId, Recovery).persist(node, executor, null, trySendTo, trySendTo, query, bumpBallot, CoordinationFlags.none(), txnId, full.partialTxn, full.executeAt, full.stableDeps, full.writes, full.result, informDurableOnDone, null);
                                    }
                                }
                            }
                            propagate = full;
                        }
                        else
                        {
                            if (tracing != null)
                                tracing.trace(null, "found Apply truncated at all shards; advancing Durability to at least Majority");

                            propagate = full.merge(AllQuorums);
                        }
                    }
                    else
                    {
                        propagate = full;
                    }

                    Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, success.withQuorum, query, query, reportTo, null, propagate, (s, f) -> invokeCallback(f == null ? propagate.toProgressToken() : null, f), tracing);
                    break;
                }

                Txn txn = full.partialTxn.reconstitute(query);
                if (known.is(ApplyAtKnown) && known.outcome() == Apply)
                {
                    Deps deps;
                    Route<?> missingDeps;
                    if (known.is(DepsKnown))
                    {
                        if (tracing != null)
                            tracing.trace(null, "found Apply with DepsKnown; persisting to all shards.");
                        deps = full.stableDeps.reconstitute(query);
                        missingDeps = query.slice(0, 0);
                    }
                    else
                    {
                        Participants<?> hasDeps = full.map.knownFor(Known.DepsOnly, query);
                        missingDeps = query.without(hasDeps);
                        if (tracing != null)
                            tracing.trace(null, "found Apply, with deps missing for %s; advancing state machine for these shards.", missingDeps);

                        if (full.stableDeps == null)
                        {
                            Invariants.require(hasDeps.isEmpty());
                            deps = Deps.NONE;
                        }
                        else
                        {
                            // convert to plain Deps as when we merge with latest deps we may erroneously keep the
                            // PartialDeps if e.g. an empty range of deps is found
                            deps = new Deps(full.stableDeps.reconstitutePartial(hasDeps));
                        }
                    }
                    LatestDeps.withStable(node.coordinationAdapter(txnId, Recovery), node, executor, txnId, full.executeAt, full.partialTxn, deps, missingDeps, missingDeps, ALL, query, (s, f) -> invokeCallback(null, f), mergedDeps -> {
                        node.withEpochAtLeast(full.executeAt.epoch(), executor, node.agent(), t -> Rethrowable.rethrowable(t), () -> {
                            node.coordinationAdapter(txnId, Recovery).persist(node, executor, topologies, query, bumpBallot, CoordinationFlags.none(), txnId, txn, full.executeAt, mergedDeps, full.writes, full.result, (s, f) -> {
                                invokeCallback(f == null ? APPLIED : null, f);
                            });
                        });
                    });
                }
                else
                {
                    if (tracing != null)
                        tracing.trace(null, "found %s; invoking Recover", known);

                    Recover.recover(node, txnId, txn, query, full.durability.isFastPathDurablyDecided(), takeCallback());
                }
                break;
            }
            case Abort:
            {
                if (tracing != null)
                    tracing.trace(null, "Found Abort; propagating locally.");

                if (witnessedByInvalidation != null && witnessedByInvalidation.hasBeen(Status.PreCommitted))
                    throw illegalState("We previously invalidated, finding a status that should be recoverable");

                Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, success.withQuorum, query, query, reportTo, null, full, (s, f) -> invokeCallback(f == null ? INVALIDATED : null, f), tracing);
                break;
            }
            case Erased:
            {
                if (tracing != null)
                    tracing.trace(null, "found Erased; propagating locally.");

                // we should only be able to hit the Erased case if every participating shard has advanced past this TxnId, so we don't need to recover it
                Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, success.withQuorum, query, query, reportTo, null, full, (s, f) -> invokeCallback(f == null ? TRUNCATED_DURABLE_OR_INVALIDATED : null, f), tracing);
                break;
            }
        }
    }

    @Override
    public CoordinationKind kind()
    {
        return CoordinationKind.PrepareRecovery;
    }

    @Override
    public String describe()
    {
        return super.describe() + ", witnessedByInvalidation=" + witnessedByInvalidation;
    }
}
