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

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.Key;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.CommandListener;
import accord.local.Node;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestKind;
import accord.local.SafeCommandStore.TestTimestamp;
import accord.local.Status;
import accord.primitives.Routable.Domain;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.MapReduceConsume;
import accord.utils.async.AsyncResults;

import static accord.local.PreLoadContext.EMPTY_PRELOADCONTEXT;
import static accord.local.PreLoadContext.contextFor;
import static accord.utils.Invariants.checkArgument;
import static accord.utils.Invariants.checkState;

/**
 * Local or global barriers that return a result once all transactions have their side effects visible.
 *
 * For local barriers the epoch is the only guarantee, but for global barriers a new transaction is created so it will
 * be as of the timestamp of the created transaction.
 *
 * Note that reads might still order after, but side effect bearing transactions should not.
 */
public class Barrier extends AsyncResults.AbstractResult<Timestamp>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(Barrier.class);

    private final Node node;

    private final Seekables<?, ?> seekables;

    private final long minEpoch;
    private final BarrierType barrierType;

    @VisibleForTesting
    CoordinateSyncPoint coordinateSyncPoint;
    @VisibleForTesting
    ExistingTransactionCheck existingTransactionCheck;

    Barrier(Node node, Seekables keysOrRanges, long minEpoch, BarrierType barrierType)
    {
        checkArgument(keysOrRanges.domain() == Domain.Key || barrierType.global, "Ranges are only supported with global barriers");
        checkArgument(keysOrRanges.size() == 1 || barrierType.global, "Only a single key is supported with local barriers");
        this.node = node;
        this.minEpoch = minEpoch;
        this.seekables = keysOrRanges;
        this.barrierType = barrierType;
    }

    public static Barrier barrier(Node node, Seekables keysOrRanges, long minEpoch, BarrierType barrierType)
    {
        Barrier barrier = new Barrier(node, keysOrRanges, minEpoch, barrierType);
        barrier.start();
        return barrier;
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

                if (barrierTxn != null)
                {
                    if (barrierTxn.status.equals(Status.Applied))
                    {
                        doBarrierSuccess(barrierTxn.executeAt);
                    }
                    // A listener was added to the transaction already
                }
                else
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
        coordinateSyncPoint = CoordinateSyncPoint.inclusive(node, seekables, barrierType.async);
        coordinateSyncPoint.addCallback((syncPoint, syncPointFailure) -> {
            if (syncPointFailure != null)
            {
                Barrier.this.tryFailure(syncPointFailure);
                return;
            }

            // Need to wait for the local transaction to finish since coordinate sync point won't wait on anything
            // if async was requested or there were no deps found
            if (syncPoint.finishedAsync)
            {
                TxnId txnId = syncPoint.txnId;
                long epoch = txnId.epoch();
                RoutingKey homeKey = syncPoint.homeKey;
                node.commandStores().ifLocal(contextFor(syncPoint.txnId), homeKey, epoch, epoch, safeStore -> {
                    CommandListener listener = new BarrierCommandListener();
                    SafeCommand safeCommand = safeStore.command(txnId);
                    listener.onChange(safeStore, safeCommand);
                    if (!isDone())
                        safeCommand.addListener(listener);
                }).begin(node.agent());
            }
            else
            {
                doBarrierSuccess(syncPoint.txnId);
            }
        });
    }

    private class BarrierCommandListener implements CommandListener
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
            else if (command.is(Status.Invalidated))
                // Surface the invalidation and the barrier can be retried by the caller
                Barrier.this.tryFailure(new Timeout(command.txnId(), command.homeKey()));

        }

        @Override
        public PreLoadContext listenerPreLoadContext(TxnId caller)
        {
            // TODO Surely the command passed to `onChange` is loaded?
            return EMPTY_PRELOADCONTEXT;
        }

        @Override
        public boolean isTransient()
        {
            return true;
        }
    }

    private ExistingTransactionCheck checkForExistingTransaction()
    {
        checkState(seekables.size() == 1 && seekables.domain() == Domain.Key);
        ExistingTransactionCheck check = new ExistingTransactionCheck();
        Key k = seekables.get(0).asKey();
        node.commandStores().mapReduceConsume(
                contextFor(k),
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
        public final Status status;
        public BarrierTxn(@Nonnull TxnId txnId, @Nonnull Timestamp executeAt, @Nonnull Status status)
        {
            this.txnId = txnId;
            this.executeAt = executeAt;
            this.status = status;
        }

        public BarrierTxn max(BarrierTxn other)
        {
            if (other == null)
                return this;
            return status.compareTo(other.status) >= 0 ? this : other;
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
            BarrierTxn found = safeStore.mapReduceWithTerminate(
                    seekables,
                    safeStore.ranges().since(minEpoch),
                    // TODO review this with Benedict, but I don't believe an exclusive sync point works here
                    TestKind.Any,
                    TestTimestamp.EXECUTES_AFTER,
                    TxnId.minForEpoch(minEpoch),
                    TestDep.ANY_DEPS,
                    null,
                    // TODO is PreApplied right? It's the first OutcomeKnown state, but why not Committed?
                    Status.PreApplied,
                    Status.Applied,
                    (keyOrRange, txnId, executeAt, status, barrierTxn) -> new BarrierTxn(txnId, executeAt, status),
                    null,
                    // Take the first one we find, and call it good enough to wait on
                    barrierTxn -> true);
            // It's not applied so add a listener to find out when it is applied
            if (found != null && !found.status.equals(Status.Applied))
            {
                safeStore.commandStore().execute(contextFor(found.txnId), safeStoreWithTxn -> safeStoreWithTxn.command(found.txnId).addListener(new BarrierCommandListener())).begin(node.agent());
            }
            return found;
        }

        @Override
        public BarrierTxn reduce(BarrierTxn o1, BarrierTxn o2)
        {
            throw new IllegalStateException("Should not be possible to find multiple transactions");
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
