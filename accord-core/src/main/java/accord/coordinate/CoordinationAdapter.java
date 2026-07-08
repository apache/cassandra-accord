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
import java.util.function.BiFunction;
import javax.annotation.Nullable;

import accord.api.ProtocolModifiers;
import accord.api.Result;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.tracking.PreAcceptExclusiveSyncPointTracker;
import accord.coordinate.tracking.PreAcceptTracker;
import accord.local.Node;
import accord.api.ExclusiveAsyncExecutor;
import accord.local.durability.DurabilityResult;
import accord.local.durability.DurabilityLevel;
import accord.messages.Accept;
import accord.messages.Apply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.SyncPoint;
import accord.primitives.RangeRoute;
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.MinimalSyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.topology.ActiveEpochs;
import accord.topology.Topologies;
import accord.topology.TopologyException;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.QuorumEpochIntersections;
import static accord.api.ProtocolModifiers.dataStoreRequiresUniqueHlcs;
import static accord.coordinate.CoordinationAdapter.Factory.Kind.Recovery;
import static accord.coordinate.ExecuteFlag.HAS_UNIQUE_HLC;
import static accord.coordinate.ExecutePath.FAST;
import static accord.coordinate.ExecutePath.SLOW;
import static accord.messages.Apply.Kind.Maximal;
import static accord.messages.Apply.Kind.Minimal;
import static accord.topology.SelectShards.ALL;

public interface CoordinationAdapter<R>
{
    interface Factory
    {
        enum Kind { Standard, Recovery }
        <R> CoordinationAdapter<R> get(TxnId txnId, Kind kind);
    }

    void propose(Node node, ExclusiveAsyncExecutor executor, @Nullable Topologies preaccept, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void proposeOnly(Node node, ExclusiveAsyncExecutor executor, Route<?> require, Route<?> sendTo, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Deps, Throwable> callback);
    void stabilise(Node node, ExclusiveAsyncExecutor executor, @Nullable Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback);
    void stabiliseOnly(Node node, ExclusiveAsyncExecutor executor, Route<?> require, Route<?> sendTo, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Deps, Throwable> callback);
    void execute(Node node, ExclusiveAsyncExecutor executor, @Nullable Topologies any, FullRoute<?> route, Ballot ballot, ExecutePath path, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps stableDeps, Deps sendDeps, BiConsumer<? super R, Throwable> callback);
    void persist(Node node, ExclusiveAsyncExecutor executor, @Nullable Topologies any, Route<?> require, Route<?> sendTo, FullRoute<?> route, Ballot ballot, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, boolean informDurableOnDone, BiConsumer<? super R, Throwable> callback);
    default void persist(Node node, ExclusiveAsyncExecutor executor, @Nullable Topologies any, FullRoute<?> route, Ballot ballot, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, BiConsumer<? super R, Throwable> callback)
    {
        persist(node, executor, any, route, route, route, ballot, flags, txnId, txn, executeAt, deps, writes, result, true, callback);
    }

    class DefaultFactory implements Factory
    {
        @Override
        public <R> CoordinationAdapter<R> get(TxnId txnId, Kind kind)
        {
            if (txnId.isSyncPoint())
            {
                // callback types are different, and we pass through the recovery adapter for sync points so should not invoke Continue
                Invariants.require(kind == Recovery);
                return (CoordinationAdapter<R>) Adapters.recoverExclusiveSyncPoint();
            }
            switch (kind)
            {
                default: throw new UnhandledEnum(kind);
                case Standard: return (CoordinationAdapter<R>) Adapters.standard();
                case Recovery: return (CoordinationAdapter<R>) Adapters.recover();
            }
        }
    }

    class Adapters
    {
        public static TxnAdapter standard()
        {
            return ProtocolModifiers.sendMinimal() ? TxnAdapter.MINIMAL : TxnAdapter.MAXIMAL;
        }

        // note that by default the recovery adapter is only used for the initial recovery decision - if e.g. propose is initiated
        // then we revert back to standard adapter behaviour for later steps
        public static CoordinationAdapter<Result> recover()
        {
            return TxnAdapter.MAXIMAL;
        }

        public static SyncPointAdapter<SyncPoint> exclusiveSyncPoint()
        {
            return ExclusiveSyncPointAdapter.INSTANCE;
        }

        public static CoordinationAdapter<Result> recoverExclusiveSyncPoint()
        {
            return RecoverExclusiveSyncPointAdapter.INSTANCE;
        }

        public static class TxnAdapter implements CoordinationAdapter<Result>
        {
            static final TxnAdapter MINIMAL = new TxnAdapter(Minimal);
            static final TxnAdapter MAXIMAL = new TxnAdapter(Maximal);

            final Apply.Kind applyKind;
            public TxnAdapter(Apply.Kind applyKind)
            {
                this.applyKind = applyKind;
            }

            @Override
            public void propose(Node node, ExclusiveAsyncExecutor executor, @Nullable Topologies preacceptOrRecovery, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                ProposeTxn propose;
                try
                {
                    ActiveEpochs epochs = node.topology().active();
                    Topologies accept = epochs.reselect(preacceptOrRecovery, QuorumEpochIntersections.preacceptOrRecover,
                                                        route, txnId, executeAt, txnId.selectsShards(), QuorumEpochIntersections.accept);
                    propose = new ProposeTxn(node, executor, accept, route, kind, ballot, txnId, txn, executeAt, deps, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                propose.start();
            }

            @Override
            public void proposeOnly(Node node, ExclusiveAsyncExecutor executor, Route<?> require, Route<?> sendTo, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Deps, Throwable> callback)
            {
                ProposeOnly propose;
                try
                {
                    ActiveEpochs epochs = node.topology().active();
                    Topologies accept = epochs.reselect(null, QuorumEpochIntersections.preacceptOrRecover,
                                                        sendTo, txnId, executeAt, txnId.selectsShards(), QuorumEpochIntersections.accept);
                    propose = new ProposeOnly(node, executor, accept, sendTo, route, kind, ballot, txnId, txn, executeAt, deps, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                propose.start();
            }

            @Override
            public void stabilise(Node node, ExclusiveAsyncExecutor executor, Topologies accept, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                ActiveEpochs epochs = node.topology().active();
                if (!epochs.hasAtLeastEpoch(executeAt.epoch()))
                {
                    node.withEpochAtLeast(executeAt.epoch(), executor, (success, fail) -> {
                        if (fail != null) callback.accept(null, fail);
                        else stabilise(node, executor, accept, route, ballot, txnId, txn, executeAt, deps, callback);
                    });
                    return;
                }

                StabiliseTxn stabilise;
                try
                {
                    Topologies all = epochs.reselect(accept, QuorumEpochIntersections.accept,
                                                     route, txnId, executeAt, txnId.selectsShards(), QuorumEpochIntersections.commit);
                    Topologies coordinates = all.size() == 1 ? all : accept.forEpoch(txnId.epoch());

                    if (ProtocolModifiers.Faults.txnInstability)
                    {
                        execute(node, executor, all, route, ballot, SLOW, CoordinationFlags.none(), txnId, txn, executeAt, deps, deps, callback);
                        return;
                    }

                    stabilise = new StabiliseTxn(node, executor, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                stabilise.start();
            }

            @Override
            public void stabiliseOnly(Node node, ExclusiveAsyncExecutor executor, Route<?> require, Route<?> sendTo, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Deps, Throwable> callback)
            {
                ActiveEpochs epochs = node.topology().active();
                if (!epochs.hasAtLeastEpoch(executeAt.epoch()))
                {
                    node.withEpochAtLeast(executeAt.epoch(), executor, (success, fail) -> {
                        if (fail != null) callback.accept(null, fail);
                        else stabiliseOnly(node, executor, require, sendTo, route, ballot, txnId, txn, executeAt, deps, callback);
                    });
                    return;
                }

                StabiliseOnly stabilise;
                try
                {
                    Topologies all = epochs.reselect(null, QuorumEpochIntersections.accept,
                                                     sendTo, txnId, executeAt, txnId.selectsShards(), QuorumEpochIntersections.commit);
                    Topologies coordinates = all.size() == 1 ? all : all.forEpoch(txnId.epoch());

                    stabilise = new StabiliseOnly(node, executor, coordinates, all, require, route, ballot, txnId, txn, executeAt, deps, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                stabilise.start();
            }

            @Override
            public void execute(Node node, ExclusiveAsyncExecutor executor, Topologies any, FullRoute<?> route, Ballot ballot, ExecutePath path, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps stableDeps, Deps sendDeps, BiConsumer<? super Result, Throwable> callback)
            {
                ExecuteTxn execute;
                try
                {
                    Topologies all = execution(node, any, route, route, txnId, executeAt);

                    if ((flags.all().contains(HAS_UNIQUE_HLC) || !dataStoreRequiresUniqueHlcs()) && txn.read().keys().isEmpty() && (path != FAST || !txnId.hasPrivilegedCoordinator()))
                    {
                        // TODO (expected): enable this optimisation with privileged coordinator to support faster blind writes
                        //   (only unsafe because we don't guarantee the stable record goes to the coordinator first)

                        /*
                          This optimisation is currently safe only because HAS_UNIQUE_HLC implies its dependencies are all applied.

                          From Fedor:
                            - c1 writes to key k1 and k2, it is not stable yet but eventually gets stable with t1
                            - c2 writes to key k1, stabilizes with {c1} and a timestamp t2 > t1 and returns to client
                            - c3 (submitted after c2 is returned) reads k2, gets ordered before c1 (t3 < t1)
                            - c4 reads k1 and k2, gets ordered after c2 (t4 > t2)

                          If we want to enable it for other systems, we need to give it a lot more thought.
                          For instance, it might be that we need to be transtively Stable; or it might be sufficient
                          to witness only >= Accepted dependencies and for Recovery to either not contact clients or
                          to have some
                         */
                        Writes writes = txnId.is(Txn.Kind.Write) ? txn.execute(txnId, executeAt, null) : null;
                        Result result = txn.result(txnId, executeAt, null);
                        persist(node, executor, all, route, ballot, flags, txnId, txn, executeAt, stableDeps, writes, result, callback);
                        return;
                    }
                    else
                    {
                        execute = new ExecuteTxn(node, executor, all, route, ballot, path, flags, txnId, txn, executeAt, stableDeps, sendDeps, callback);
                    }
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                execute.start();
            }

            @Override
            public void persist(Node node, ExclusiveAsyncExecutor executor, Topologies any, Route<?> require, Route<?> sendTo, FullRoute<?> route, Ballot ballot, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, boolean informDurableOnDone, BiConsumer<? super Result, Throwable> callback)
            {
                if (callback != null) callback.accept(result, null);

                try
                {
                    Topologies all = execution(node, any, sendTo, route, txnId, executeAt);
                    new PersistTxn(node, executor, all, txnId, ballot, require, txn, executeAt, deps, writes, result == null ? null : result.toPersistable(), route, flags, informDurableOnDone, Apply.FACTORY, applyKind)
                    .start();
                }
                catch (TopologyException e)
                {
                    node.agent().onException(e, "Unable to persist " + txnId);
                }
            }

            protected Topologies execution(Node node, @Nullable Topologies preacceptOrCommit, Route<?> sendTo, FullRoute<?> route, TxnId txnId, Timestamp executeAt) throws TopologyException
            {
                ActiveEpochs epochs = node.topology().active();
                if (route != sendTo) preacceptOrCommit = null;
                return epochs.reselect(preacceptOrCommit, QuorumEpochIntersections.preacceptOrCommit,
                                       sendTo, txnId, executeAt, txnId.selectsShards(), QuorumEpochIntersections.stable);
            }
        }

        public static abstract class SyncPointAdapter<R> implements CoordinationAdapter<R>
        {
            final BiFunction<Topologies, TxnId, PreAcceptTracker<?>> preacceptTrackerFactory;

            protected SyncPointAdapter(BiFunction<Topologies, TxnId, PreAcceptTracker<?>> preacceptTrackerFactory)
            {
                this.preacceptTrackerFactory = preacceptTrackerFactory;
            }

            abstract Topologies forDecision(Node node, Route<?> route, TxnId txnId, Timestamp executeAt) throws TopologyException;
            abstract Topologies forExecution(Node node, Route<?> route, TxnId txnId, Timestamp executeAt, Deps deps) throws TopologyException;
            abstract void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Deps deps, BiConsumer<? super R, Throwable> callback);

            @Override
            public void propose(Node node, ExclusiveAsyncExecutor executor, Topologies any, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
            {
                ProposeSyncPoint<R> propose;
                try
                {
                    Topologies all = forDecision(node, route, txnId, executeAt);
                    propose = new ProposeSyncPoint<>(this, node, executor, all, route, kind, ballot, txnId, txn, executeAt, deps, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                propose.start();
            }

            @Override
            public void proposeOnly(Node node, ExclusiveAsyncExecutor executor, Route<?> require, Route<?> sendTo, FullRoute<?> route, Accept.Kind kind, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Deps, Throwable> callback)
            {
                ProposeOnly propose;
                try
                {
                    Topologies all = forDecision(node, sendTo, txnId, executeAt);
                    propose = new ProposeOnly(node, executor, all, sendTo, route, kind, ballot, txnId, txn, executeAt, deps, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                propose.start();
            }

            @Override
            public void stabilise(Node node, ExclusiveAsyncExecutor executor, Topologies any, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super R, Throwable> callback)
            {
                StabiliseSyncPoint<R> stabilise;
                try
                {
                    Topologies all = forExecution(node, route, txnId, executeAt, deps);
                    Topologies coordinates = all.forEpoch(txnId.epoch());
                    stabilise = new StabiliseSyncPoint<>(this, node, executor, coordinates, all, route, ballot, txnId, txn, executeAt, deps, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                stabilise.start();
            }

            @Override
            public void stabiliseOnly(Node node, ExclusiveAsyncExecutor executor, Route<?> require, Route<?> sendTo, FullRoute<?> route, Ballot ballot, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, BiConsumer<? super Deps, Throwable> callback)
            {
                StabiliseOnly stabilise;
                try
                {
                    Topologies all = forExecution(node, route, txnId, executeAt, deps);
                    Topologies coordinates = all.forEpoch(txnId.epoch());
                    stabilise = new StabiliseOnly(node, executor, coordinates, all, sendTo, route, ballot, txnId, txn, executeAt, deps, callback);
                }
                catch (Throwable t)
                {
                    callback.accept(null, t);
                    return;
                }
                stabilise.start();
            }

            @Override
            public void execute(Node node, ExclusiveAsyncExecutor executor, Topologies any, FullRoute<?> route, Ballot ballot, ExecutePath path, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps stableDeps, Deps sendDeps, BiConsumer<? super R, Throwable> callback)
            {
                persist(node, executor, null, route, ballot, flags, txnId, txn, executeAt, stableDeps, null, txn.result(txnId, executeAt, null), callback);
            }

            @Override
            public void persist(Node node, ExclusiveAsyncExecutor executor, Topologies ignore, Route<?> require, Route<?> sendTo, FullRoute<?> route, Ballot ballot, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps deps, Writes writes, Result result, boolean informDurableOnDone, BiConsumer<? super R, Throwable> callback)
            {
                invokeSuccess(node, route, txnId, executeAt, txn, deps, callback);

                try
                {
                    Topologies all = forExecution(node, sendTo, txnId, executeAt, deps);
                    new PersistSyncPoint(node, executor, all, txnId, ballot, sendTo, txn, executeAt, deps, writes, result.toPersistable(), informDurableOnDone, route, Maximal)
                    .start();
                }
                catch (Throwable t)
                {
                    node.agent().onException(t);
                }
            }
        }

        private static abstract class AbstractExclusiveSyncPointAdapter<R> extends SyncPointAdapter<R>
        {
            public AbstractExclusiveSyncPointAdapter()
            {
                super(PreAcceptExclusiveSyncPointTracker::new);
            }

            @Override
            Topologies forDecision(Node node, Route<?> route, TxnId txnId, Timestamp executeAt) throws TopologyException
            {
                return node.topology().active().withOpenEpochs(route, null, txnId, ALL);
            }

            @Override
            Topologies forExecution(Node node, Route<?> route, TxnId txnId, Timestamp executeAt, Deps deps) throws TopologyException
            {
                return node.topology().active().withUncompletedEpochs(route, txnId, txnId, ALL);
            }

            @Override
            public void execute(Node node, ExclusiveAsyncExecutor executor, Topologies any, FullRoute<?> route, Ballot ballot, ExecutePath path, CoordinationFlags flags, TxnId txnId, Txn txn, Timestamp executeAt, Deps stableDeps, Deps sendDeps, BiConsumer<? super R, Throwable> callback)
            {
                // We cannot use the fast path for sync points as their visibility is asymmetric wrt other transactions,
                // so we could recover to include different transactions than those we fast path committed with.
                Invariants.require(path != FAST);
                super.execute(node, executor, any, route, ballot, path, flags, txnId, txn, executeAt, stableDeps, sendDeps, callback);
            }
        }

        static class RecoverExclusiveSyncPointAdapter extends AbstractExclusiveSyncPointAdapter<Result>
        {
            static final RecoverExclusiveSyncPointAdapter INSTANCE = new RecoverExclusiveSyncPointAdapter();

            @Override
            void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Deps deps, BiConsumer<? super Result, Throwable> callback)
            {
                if (callback != null)
                {
                    callback.accept(txn.result(txnId, executeAt, null), null);
                }
                if (txnId.is(Routable.Domain.Range))
                {
                    MinimalSyncPoint syncPoint = new MinimalSyncPoint(txnId, executeAt, (RangeRoute) route);
                    node.topology().onEpochClosed(syncPoint.route.toRanges(), syncPoint.syncId);
                    node.durability().report(new DurabilityResult(syncPoint, DurabilityLevel.NONE, null));
                }
            }
        }

        static class ExclusiveSyncPointAdapter extends AbstractExclusiveSyncPointAdapter<SyncPoint>
        {
            static final ExclusiveSyncPointAdapter INSTANCE = new ExclusiveSyncPointAdapter();

            @Override
            void invokeSuccess(Node node, FullRoute<?> route, TxnId txnId, Timestamp executeAt, Txn txn, Deps deps, BiConsumer<? super SyncPoint, Throwable> callback)
            {
                SyncPoint syncPoint = new SyncPoint(txnId, executeAt, (FullRangeRoute)route, deps);
                if (callback != null)
                {
                    callback.accept(syncPoint, null);
                }
                if (txnId.is(Routable.Domain.Range))
                {
                    node.topology().onEpochClosed(syncPoint.route.toRanges(), syncPoint.syncId);
                    node.durability().report(new DurabilityResult(syncPoint, DurabilityLevel.NONE, null));
                }
            }
        }
    }

}
