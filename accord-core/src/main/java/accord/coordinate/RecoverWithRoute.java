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
import accord.local.SequentialAsyncExecutor;
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
import accord.utils.Invariants;
import accord.utils.WrappableException;

import static accord.coordinate.CoordinationAdapter.Factory.Kind.Recovery;
import static accord.coordinate.ReadCoordinator.Success.Quorum;
import static accord.primitives.Known.KnownDeps.DepsKnown;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Known.Outcome.Apply;
import static accord.primitives.ProgressToken.APPLIED;
import static accord.primitives.ProgressToken.INVALIDATED;
import static accord.primitives.ProgressToken.TRUNCATED_DURABLE_OR_INVALIDATED;
import static accord.primitives.Status.Durability.Majority;
import static accord.topology.Topologies.SelectNodeOwnership.SLICE;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;
import static accord.utils.Invariants.illegalState;

public class RecoverWithRoute extends CheckShards<FullRoute<?>>
{
    final BiConsumer<Outcome, Throwable> callback;
    final Status witnessedByInvalidation;
    final LatentStoreSelector reportTo;

    private RecoverWithRoute(Node node, SequentialAsyncExecutor executor, Topologies topologies, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, Status witnessedByInvalidation, LatentStoreSelector reportTo, BiConsumer<Outcome, Throwable> callback)
    {
        super(node, executor, txnId, route, IncludeInfo.All, node.uniqueTimestamp(Ballot::fromValues), invalidIf);
        this.reportTo = reportTo;
        // if witnessedByInvalidation == AcceptedInvalidate then we cannot assume its definition was known, and our comparison with the status is invalid
        Invariants.require(witnessedByInvalidation != Status.AcceptedInvalidate);
        // if witnessedByInvalidation == Invalidated we should anyway not be recovering
        Invariants.require(witnessedByInvalidation != Status.Invalidated);
        this.callback = callback;
        this.witnessedByInvalidation = witnessedByInvalidation;
        assert topologies.oldestEpoch() == topologies.currentEpoch() && topologies.currentEpoch() == txnId.epoch();
    }

    public static RecoverWithRoute recover(Node node, SequentialAsyncExecutor executor, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, @Nullable Status witnessedByInvalidation, LatentStoreSelector reportTo, BiConsumer<Outcome, Throwable> callback)
    {
        return recover(node, executor, node.topology().forEpoch(route, txnId.epoch(), SHARE), txnId, invalidIf, route, witnessedByInvalidation, reportTo, callback);
    }

    private static RecoverWithRoute recover(Node node, SequentialAsyncExecutor executor, Topologies topologies, TxnId txnId, Infer.InvalidIf invalidIf, FullRoute<?> route, @Nullable Status witnessedByInvalidation, LatentStoreSelector reportTo, BiConsumer<Outcome, Throwable> callback)
    {
        RecoverWithRoute recover = new RecoverWithRoute(node, executor, topologies, txnId, invalidIf, route, witnessedByInvalidation, reportTo, callback);
        recover.start();
        return recover;
    }

    @Override
    public void contact(Id to)
    {
        node.send(to, new CheckStatus(to, topologies(), txnId, query, sourceEpoch, IncludeInfo.All, bumpBallot), executor, this);
    }

    @Override
    protected boolean isSufficient(Id from, CheckStatusOk ok)
    {
        Ranges rangesForNode = topologies().getEpoch(txnId.epoch()).rangesForNode(from);
        Route<?> route = this.query.slice(rangesForNode);
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
            callback.accept(null, failure);
            return;
        }

        CheckStatusOkFull full = ((CheckStatusOkFull) this.merged).finish(query, query, query, success.withQuorum, previouslyKnownToBeInvalidIf);
        Known known = full.knownFor(txnId, query, query);

        // TODO (required): audit this logic, and centralise with e.g. FetchData inferences
        // TODO (expected): skip straight to ExecuteTxn if we have a Stable reply from each shard
        switch (known.outcome())
        {
            default: throw new AssertionError();
            case Unknown:
                if (known.definition().isKnown())
                {
                    Txn txn = full.partialTxn.reconstitute(query);
                    Recover.recover(node, txnId, txn, query, reportTo, callback);
                }
                else if (!known.definition().isOrWasKnown())
                {
                    if (witnessedByInvalidation != null && witnessedByInvalidation.compareTo(Status.PreAccepted) > 0)
                        throw illegalState("We previously invalidated %s, finding a status %s that should be recoverable", txnId, witnessedByInvalidation);
                    Invalidate.invalidate(node, txnId, query, witnessedByInvalidation != null, reportTo, callback);
                }
                else
                {
                    callback.accept(full.toProgressToken(), null);
                }
                break;

            case WasApply:
            case Apply:
                if (!known.isDefinitionKnown())
                {
                    if (!known.isTruncated() && !known.isInvalidated())
                    {
                        // we must have raced with a successful apply, so should simply abort
                        callback.accept(ProgressToken.NONE, null);
                        return;
                    }

                    // TODO (expected): if we determine new durability, propagate it
                    CheckStatusOkFull propagate;
                    if (!full.map.hasFullyTruncated(query))
                    {
                        // we might have only part of the full transaction, and a shard may have truncated;
                        // in this case we want to skip straight to apply, but only for the shards that haven't truncated
                        Route<?> trySendTo = query.without(full.map.matchingRanges(minMax -> minMax.min.isTruncated()));
                        if (!trySendTo.isEmpty())
                        {
                            if (known.isInvalidated())
                            {
                                Commit.Invalidate.commitInvalidate(node, txnId, trySendTo, txnId);
                            }
                            else
                            {
                                boolean informDurableOnDone = success == Quorum; // if we have a quorum of truncated responses for the part of the remote we have removed, it is safe to consider this part durable
                                known = full.knownFor(txnId, trySendTo, trySendTo);
                                if (known.isDefinitionKnown() && known.is(ApplyAtKnown) && known.outcome() == Apply)
                                {
                                    if (!known.is(DepsKnown))
                                    {
                                        Invariants.require(txnId.isSystemTxn() || full.partialTxn.covers(trySendTo));
                                        Participants<?> haveStable = full.map.knownFor(Known.DepsOnly, trySendTo);
                                        Route<?> haveUnstable = trySendTo.without(haveStable);
                                        Deps stable = haveStable.isEmpty() ? Deps.NONE : full.stableDeps.reconstitutePartial(haveStable).asFullUnsafe();

                                        LatestDeps.withStable(node.coordinationAdapter(txnId, Recovery), node, executor, txnId, full.executeAt, full.partialTxn, stable, haveUnstable, trySendTo, SLICE, query, callback, deps -> {
                                            Deps stableDeps = deps.intersecting(trySendTo);
                                            node.coordinationAdapter(txnId, Recovery).persist(node, executor, null, trySendTo, trySendTo, SLICE, query, bumpBallot, CoordinationFlags.none(), txnId, full.partialTxn, full.executeAt, stableDeps, full.writes, full.result, informDurableOnDone, null);
                                        });
                                    }
                                    else
                                    {
                                        Invariants.require(full.stableDeps.covers(trySendTo));
                                        Invariants.require(txnId.isSystemTxn() || full.partialTxn.covers(trySendTo));
                                        node.coordinationAdapter(txnId, Recovery).persist(node, executor, null, trySendTo, trySendTo, SLICE, query, bumpBallot, CoordinationFlags.none(), txnId, full.partialTxn, full.executeAt, full.stableDeps, full.writes, full.result, informDurableOnDone, null);
                                    }
                                }
                            }
                            propagate = full;
                        }
                        else
                        {
                            propagate = full.merge(Majority);
                        }
                    }
                    else
                    {
                        propagate = full;
                    }

                    Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, success.withQuorum, query, query, reportTo, null, propagate, (s, f) -> callback.accept(f == null ? propagate.toProgressToken() : null, f), null);
                    break;
                }

                Txn txn = full.partialTxn.reconstitute(query);
                if (known.is(ApplyAtKnown) && known.outcome() == Apply)
                {
                    Deps deps;
                    Route<?> missingDeps;
                    if (known.is(DepsKnown))
                    {
                        deps = full.stableDeps.reconstitute(query);
                        missingDeps = query.slice(0, 0);
                    }
                    else
                    {
                        Participants<?> hasDeps = full.map.knownFor(Known.DepsOnly, query);
                        missingDeps = query.without(hasDeps);
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
                    LatestDeps.withStable(node.coordinationAdapter(txnId, Recovery), node, executor, txnId, full.executeAt, full.partialTxn, deps, missingDeps, missingDeps, SHARE, query, callback, mergedDeps -> {
                        node.withEpochAtLeast(full.executeAt.epoch(), executor, node.agent(), t -> WrappableException.wrap(t), () -> {
                            node.coordinationAdapter(txnId, Recovery).persist(node, executor, topologies, query, bumpBallot, CoordinationFlags.none(), txnId, txn, full.executeAt, mergedDeps, full.writes, full.result, (s, f) -> {
                                callback.accept(f == null ? APPLIED : null, f);
                            });
                        });
                    });
                }
                else
                {
                    Recover.recover(node, txnId, txn, query, callback);
                }
                break;

            case Abort:
                if (witnessedByInvalidation != null && witnessedByInvalidation.hasBeen(Status.PreCommitted))
                    throw illegalState("We previously invalidated, finding a status that should be recoverable");

                Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, success.withQuorum, query, query, reportTo, null, full, (s, f) -> callback.accept(f == null ? INVALIDATED : null, f), null);
                break;

            case Erased:
                // we should only be able to hit the Erased case if every participating shard has advanced past this TxnId, so we don't need to recover it
                Propagate.propagate(node, txnId, previouslyKnownToBeInvalidIf, sourceEpoch, success.withQuorum, query, query, reportTo, null, full, (s, f) -> callback.accept(f == null ? TRUNCATED_DURABLE_OR_INVALIDATED : null, f), null);
                break;
        }
    }
}
