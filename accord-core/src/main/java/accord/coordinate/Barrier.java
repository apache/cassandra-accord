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

import java.util.function.BiFunction;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.LocalListeners;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.KeyHistory;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.FullRoute;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutableKey;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.MapReduceConsume;
import accord.utils.TriFunction;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import javax.annotation.Nonnull;

import static accord.local.PreLoadContext.contextFor;
import static accord.local.SafeCommandStore.TestStatus.IS_STABLE;
import static accord.primitives.Txn.Kind.Kinds.AnyGloballyVisible;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.checkState;
import static accord.utils.Invariants.illegalState;

/**
 * Local or global barriers that return a result once all transactions have their side effects visible.
 *
 * For local barriers the epoch is the only guarantee, but for global barriers a new transaction is created so it will
 * be as of the timestamp of the created transaction.
 *
 * Note that reads might still order after, but side effect bearing transactions should not.
 */
public class Barrier extends AsyncResults.AbstractResult<TxnId>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(Barrier.class);

    private final Node node;

    private final FullRoute<?> route;
    private final Seekables<?, ?> keysOrRanges;

    private final long minEpoch;
    private final BarrierType barrierType;

    @VisibleForTesting
    AsyncResult<? extends SyncPoint<?>> coordinateSyncPoint;
    @VisibleForTesting
    ExistingTransactionCheck existingTransactionCheck;

    public static class AsyncSyncPoint
    {
        final TxnId txnId;
        final AsyncResult<? extends SyncPoint<?>> async;

        public AsyncSyncPoint(TxnId txnId, AsyncResult<? extends SyncPoint<?>> async)
        {
            this.txnId = txnId;
            this.async = async;
        }
    }

    private final BiFunction<Node, FullRoute<?>, AsyncSyncPoint> syncPoint;

    Barrier(Node node, Seekables<?, ?> keysOrRanges, FullRoute<?> route, long minEpoch, BarrierType barrierType, BiFunction<Node, FullRoute<?>, AsyncSyncPoint> syncPoint)
    {
        this.syncPoint = syncPoint;
        this.keysOrRanges = keysOrRanges;
        checkArgument(route.domain() == Domain.Key || barrierType.global, "Ranges are only supported with global barriers");
        checkArgument(route.size() == 1 || barrierType.global, "Only a single key is supported with local barriers");
        this.node = node;
        this.minEpoch = minEpoch;
        this.route = route;
        this.barrierType = barrierType;
    }


    /**
     * Trigger one of several different kinds of barrier transactions on a key or range with different properties. Barriers ensure that all prior transactions
     * have their side effects visible up to some point.
     *
     * Local barriers will look for a local transaction that was applied in minEpoch or later and returns when one exists or completes.
     * It may, but it is not guaranteed to, trigger a global barrier transaction that effects the barrier at all replicas.
     *
     * A global barrier is guaranteed to create a distributed barrier transaction, and if it is synchronous will not return until the
     * transaction has applied at a quorum globally (meaning all dependencies and their side effects are already visible). If it is asynchronous
     * it will return once the barrier has been applied locally.
     *
     * Ranges are only supported for global barriers.
     *
     * Returns the Timestamp the barrier actually ended up occurring at. Keep in mind for local barriers it doesn't mean a new transaction was created.
     */
    public static Barrier barrier(Node node, Seekables<?, ?> keysOrRanges, FullRoute<?> route, long minEpoch, BarrierType barrierType, BiFunction<Node, FullRoute<?>, AsyncSyncPoint> syncPoint)
    {
        Barrier barrier = new Barrier(node, keysOrRanges, route, minEpoch, barrierType, syncPoint);
        node.topology().awaitEpoch(minEpoch).begin((ignored, failure) -> {
            if (failure != null)
            {
                barrier.tryFailure(failure);
                return;
            }
            barrier.start();
        });
        return barrier;
    }

    public static Barrier barrier(Node node, Seekables<?, ?> keysOrRanges, FullRoute route, long minEpoch, BarrierType barrierType)
    {
        return barrier(node, keysOrRanges, route, minEpoch, barrierType, wrap(barrierType.async ? CoordinateSyncPoint::inclusive : CoordinateSyncPoint::inclusiveAndAwaitQuorum));
    }

    private static BiFunction<Node, FullRoute<?>, AsyncSyncPoint> wrap(TriFunction<Node, TxnId, FullRoute<?>, AsyncResult<? extends SyncPoint<?>>> syncPointFactory)
    {
        return (node, route) -> {
            TxnId txnId = node.nextTxnId(Txn.Kind.SyncPoint, route.domain());
            return new AsyncSyncPoint(txnId, syncPointFactory.apply(node, txnId, route));
        };
    }

    private void start()
    {
        // It may be possible to use local state to determine that the barrier is already satisfied or
        // there is an existing transaction we can wait on
        if (!barrierType.global)
        {
            existingTransactionCheck = checkForExistingTransaction();
            existingTransactionCheck.addCallback((barrierTxn, existingTransactionCheckFailure) -> {
                if (existingTransactionCheckFailure != null)
                {
                    Barrier.this.tryFailure(existingTransactionCheckFailure);
                    return;
                }

                if (barrierTxn == null)
                {
                    createSyncPoint();
                }
            });
        }
        else
        {
            createSyncPoint();
        }
    }

    private void createSyncPoint()
    {
        AsyncSyncPoint async = syncPoint.apply(node, route);
        coordinateSyncPoint = async.async;
        if (barrierType.async)
        {
            Invariants.checkState(barrierType.async);
            TxnId txnId = async.txnId;
            long epoch = txnId.epoch();
            RoutingKey homeKey = route.homeKey();
            node.commandStores().ifLocal(contextFor(txnId), homeKey, epoch, epoch, safeStore -> register(safeStore, txnId, homeKey))
                .begin(node.agent());
        }

        coordinateSyncPoint.addCallback((syncPoint, syncPointFailure) -> {
            if (syncPointFailure != null)
            {
                Barrier.this.tryFailure(syncPointFailure);
                return;
            }

            if (!barrierType.async)
                Barrier.this.trySuccess(syncPoint.syncId);
        });
    }

    private class BarrierCommandListener implements LocalListeners.ComplexListener
    {
        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            Command command = safeCommand.current();
            if (command.is(Status.Applied))
            {
                // In all the cases where we add a listener (listening to existing command, async completion of CoordinateSyncPoint)
                // we want to notify the agent
                Barrier.this.trySuccess(command.txnId());
                return false;
            }
            else if (command.hasBeen(Status.Truncated))
            {
                // Surface the invalidation/truncation and the barrier can be retried by the caller
                Barrier.this.tryFailure(new Timeout(command.txnId(), command.homeKey()));
                return false;
            }
            return true;
        }
    }

    private ExistingTransactionCheck checkForExistingTransaction()
    {
        checkState(route.size() == 1 && route.domain() == Domain.Key);
        ExistingTransactionCheck check = new ExistingTransactionCheck();
        RoutingKey k = route.get(0).asRoutingKey();
        node.commandStores().mapReduceConsume(
                contextFor(k, KeyHistory.SYNC),
                k.toUnseekable(),
                minEpoch,
                Long.MAX_VALUE,
                check);
        return check;
    }

    private void register(SafeCommandStore safeStoreWithTxn, TxnId txnId, RoutableKey key)
    {
        BarrierCommandListener listener = new BarrierCommandListener();
        safeStoreWithTxn.registerAndInvoke(txnId, key.toUnseekable(), listener);
    }

    // Hold result of looking for a transaction to act as a barrier for an Epoch
    static class BarrierTxn
    {
        @Nonnull
        public final TxnId txnId;
        @Nonnull
        public final Timestamp executeAt;
        @Nonnull
        public final RoutingKey key;
        public BarrierTxn(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, RoutingKey key)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.key = key;
        }
    }

    /*
     * Check for an existing transaction that is either already Applied (dependencies were applied)
     * or PreApplied (outcome fixed, dependencies not yet applied).
     *
     * For Applied we can return success immediately with the executeAt epoch. For PreApplied we can add
     * a listener for when it transitions to Applied and then return success.
     */
    class ExistingTransactionCheck extends AsyncResults.AbstractResult<BarrierTxn> implements MapReduceConsume<SafeCommandStore, BarrierTxn>
    {
        @Override
        public BarrierTxn apply(SafeCommandStore safeStore)
        {
            // TODO (required, consider): consider these semantics
            BarrierTxn found = safeStore.mapReduceFull(
            route,
            // Barriers are trying to establish that committed transactions are applied before the barrier (or in this case just minEpoch)
            // so all existing transaction types should ensure that at this point. An earlier txnid may have an executeAt that is after
            // this barrier or the transaction we listen on and that is fine
            TxnId.minForEpoch(minEpoch),
            AnyGloballyVisible,
            TestStartedAt.STARTED_AFTER,
            TestDep.ANY_DEPS,
            IS_STABLE,
            (p1, keyOrRange, txnId, executeAt, barrierTxn) -> {
                if (barrierTxn != null)
                    return barrierTxn;
                if (keyOrRange.domain() == Domain.Key)
                    return new BarrierTxn(txnId, executeAt, keyOrRange.asRoutingKey());
                return null;
            },
            null,
            null);
            // It's not applied so add a listener to find out when it is applied
            if (found != null)
            {
                //noinspection SillyAssignment,ConstantConditions
                safeStore = safeStore; // prevent use in lambda
                safeStore.commandStore()
                         .execute(contextFor(found.txnId), safeStoreWithTxn -> register(safeStoreWithTxn, found.txnId, found.key))
                         .begin(node.agent());
            }
            return found;
        }

        @Override
        public BarrierTxn reduce(BarrierTxn o1, BarrierTxn o2)
        {
            throw illegalState("Should not be possible to find multiple transactions");
        }

        @Override
        public void accept(BarrierTxn result, Throwable failure)
        {
            if (failure != null)
            {
                ExistingTransactionCheck.this.tryFailure(failure);
                return;
            }

            // Will need to create a transaction
            ExistingTransactionCheck.this.trySuccess(result);
        }
    }
}
