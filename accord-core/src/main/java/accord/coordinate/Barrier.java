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
import javax.annotation.Nonnull;

import accord.local.*;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.primitives.Routable.Domain;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.MapReduceConsume;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import static accord.local.PreLoadContext.contextFor;
import static accord.primitives.Txn.Kind.Kinds.AnyGloballyVisible;
import static accord.local.SafeCommandStore.TestStatus.IS_STABLE;
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
public class Barrier<S extends Seekables<?, ?>> extends AsyncResults.AbstractResult<Timestamp>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(Barrier.class);

    private final Node node;

    private final S seekables;

    private final long minEpoch;
    private final BarrierType barrierType;

    @VisibleForTesting
    AsyncResult<SyncPoint<S>> coordinateSyncPoint;
    @VisibleForTesting
    ExistingTransactionCheck existingTransactionCheck;

    private final BiFunction<Node, S, AsyncResult<SyncPoint<S>>> syncPoint;

    Barrier(Node node, S keysOrRanges, long minEpoch, BarrierType barrierType, BiFunction<Node, S, AsyncResult<SyncPoint<S>>> syncPoint)
    {
        this.syncPoint = syncPoint;
        checkArgument(keysOrRanges.domain() == Domain.Key || barrierType.global, "Ranges are only supported with global barriers");
        checkArgument(keysOrRanges.size() == 1 || barrierType.global, "Only a single key is supported with local barriers");
        this.node = node;
        this.minEpoch = minEpoch;
        this.seekables = keysOrRanges;
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
    public static <S extends Seekables<?, ?>> Barrier<S> barrier(Node node, S keysOrRanges, long minEpoch, BarrierType barrierType, BiFunction<Node, S, AsyncResult<SyncPoint<S>>> syncPoint)
    {
        Barrier<S> barrier = new Barrier(node, keysOrRanges, minEpoch, barrierType, syncPoint);
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

    public static <S extends Seekables<?, ?>> Barrier<S> barrier(Node node, S keysOrRanges, long minEpoch, BarrierType barrierType)
    {
        return barrier(node, keysOrRanges, minEpoch, barrierType, barrierType.async ? CoordinateSyncPoint::inclusive : CoordinateSyncPoint::inclusiveAndAwaitQuorum);
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

    private void doBarrierSuccess(Timestamp executeAt)
    {
        // The transaction we wait on might have more keys, but it's not
        // guaranteed they were wanted and we don't want to force the agent to filter them
        // so provide the seekables we were given.
        // We also don't notify the agent for range barriers since that makes sense as a local concept
        // since ranges can easily span multiple nodes
        if (seekables != null)
            node.agent().onLocalBarrier(seekables, executeAt);
        Barrier.this.trySuccess(executeAt);
    }

    private void createSyncPoint()
    {
        coordinateSyncPoint = syncPoint.apply(node, seekables);
        coordinateSyncPoint.addCallback((syncPoint, syncPointFailure) -> {
            if (syncPointFailure != null)
            {
                Barrier.this.tryFailure(syncPointFailure);
                return;
            }

            // Need to wait for the local transaction to finish since coordinate sync point won't wait on anything
            // if async was requested or there were no deps found
            if (barrierType.async)
            {
                TxnId txnId = syncPoint.syncId;
                long epoch = txnId.epoch();
                RoutingKey homeKey = syncPoint.homeKey;
                node.commandStores().ifLocal(contextFor(txnId), homeKey, epoch, epoch,
                                             safeStore -> safeStore.get(txnId, homeKey).addAndInvokeListener(safeStore, new BarrierCommandListener()))
                    .begin(node.agent());
            }
            else
            {
                doBarrierSuccess(syncPoint.syncId);
            }
        });
    }

    private class BarrierCommandListener implements Command.TransientListener
    {
        @Override
        public void onChange(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            Command command = safeCommand.current();
            if (command.is(Status.Applied))
            {
                Timestamp executeAt = command.executeAt();
                // In all the cases where we add a listener (listening to existing command, async completion of CoordinateSyncPoint)
                // we want to notify the agent
                doBarrierSuccess(executeAt);
            }
            else if (command.hasBeen(Status.Truncated))
                // Surface the invalidation/truncation and the barrier can be retried by the caller
                Barrier.this.tryFailure(new Timeout(command.txnId(), command.homeKey()));

        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            return PreLoadContext.contextFor(caller);
        }
    }

    private ExistingTransactionCheck checkForExistingTransaction()
    {
        checkState(seekables.size() == 1 && seekables.domain() == Domain.Key);
        ExistingTransactionCheck check = new ExistingTransactionCheck();
        Key k = seekables.get(0).asKey();
        node.commandStores().mapReduceConsume(
                contextFor(k, KeyHistory.COMMANDS),
                k.toUnseekable(),
                minEpoch,
                Long.MAX_VALUE,
                check);
        return check;
    }

    // Hold result of looking for a transaction to act as a barrier for an Epoch
    static class BarrierTxn
    {
        @Nonnull
        public final TxnId txnId;
        @Nonnull
        public final Timestamp executeAt;
        @Nonnull
        public final Key key;
        public BarrierTxn(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, Key key)
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
            // TODO (required): consider these semantics
            BarrierTxn found = safeStore.mapReduceFull(
            seekables,
            safeStore.ranges().allAfter(minEpoch),
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
                    return new BarrierTxn(txnId, executeAt, keyOrRange.asKey());
                return null;
            },
            null,
            null);
            // It's not applied so add a listener to find out when it is applied
            if (found != null)
            {
                safeStore.commandStore().execute(
                        contextFor(found.txnId),
                        safeStoreWithTxn -> safeStoreWithTxn.get(found.txnId, found.key.toUnseekable()).addAndInvokeListener(safeStore, new BarrierCommandListener())
                ).begin(node.agent());
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
