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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.local.CommandSummaries;
import accord.local.MapReduceConsumeCommandStores;
import accord.local.Node;
import accord.local.SafeCommandStore;
import accord.api.ExclusiveAsyncExecutor;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.messages.Await;
import accord.primitives.RoutingKeys;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.async.Cancellable;

import javax.annotation.Nullable;

import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.CommandSummaries.SummaryStatus.COMMITTED;
import static accord.local.CommandSummaries.SummaryStatus.INVALIDATED;
import static accord.local.durability.DurabilityService.SyncLocal.NoLocal;
import static accord.local.durability.DurabilityService.SyncLocal.Self;
import static accord.local.durability.DurabilityService.SyncRemote.NoRemote;
import static accord.local.durability.DurabilityService.SyncRemote.Quorum;
import static accord.messages.Await.Until.IsApplied;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Ws;
import static accord.utils.Invariants.illegalState;

/**
 * Facility for finding existing key transactions that can serve as a barrier transaction,
 * ensuring all reads/writes after some point in the txn log have been executed.
 */
@VisibleForImplementation
public class KeyBarriers
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KeyBarriers.class);

    public static class Found
    {
        public final TxnId txnId;
        public final RoutingKey key;
        public final SyncLocal knownLocal;
        public final SyncRemote knownRemote;

        public Found(TxnId txnId, RoutingKey key, SyncLocal knownLocal, SyncRemote knownRemote)
        {
            this.txnId = txnId;
            this.key = key;
            this.knownLocal = knownLocal;
            this.knownRemote = knownRemote;
        }
    }

    public static AsyncResult<Found> find(Node node, TxnId min, RoutingKey key, SyncLocal syncLocal, SyncRemote syncRemote)
    {
        Find find = new Find(min, key, syncLocal, syncRemote);
        node.commandStores().mapReduceConsume(min.epoch(), Long.MAX_VALUE, find);
        return find.result;
    }

    /*
     * Check for an existing transaction that is either already Applied (dependencies were applied)
     * or Committed (outcome fixed, dependencies not yet applied).
     *
     * For Applied we can return success immediately with the executeAt epoch. For PreApplied we can add
     * a listener for when it transitions to Applied and then return success.
     */
    static class Find extends MapReduceConsumeCommandStores<RoutingKeys, Found> implements CommandSummaries.SupersedingCommandVisitor
    {
        final AsyncResults.SettableByCallback<Found> result = new AsyncResults.SettableByCallback<>();
        final TxnId min;
        final RoutingKey find;
        final SyncLocal syncLocal;
        final SyncRemote syncRemote;
        Found found;

        Find(TxnId min, RoutingKey find, SyncLocal syncLocal, SyncRemote syncRemote)
        {
            super(RoutingKeys.of(find));
            this.min = min;
            this.find = find;
            this.syncLocal = syncLocal;
            this.syncRemote = syncRemote;
        }

        @Override
        public Found applyInternal(SafeCommandStore safeStore)
        {
            // Barriers are trying to establish that committed transactions are applied before the barrier (or in this case just minEpoch)
            // so all existing transaction types should ensure that at this point. An earlier txnid may have an executeAt that is after
            // this barrier or the transaction we listen on and that is fine
            safeStore.visit(scope, min, Ws, this);
            return found;
        }

        @Override
        protected Found refuseInternal(SafeCommandStore safeStore)
        {
            return null;
        }

        @Override
        public boolean visit(Unseekable keyOrRange, TxnId txnId, Timestamp executeAt, CommandSummaries.SummaryStatus status, @Nullable CommandSummaries.IsDep dep, Status.Durability minDurability)
        {
            if (status.compareTo(COMMITTED) < 0 || status == INVALIDATED)
                return true;

            if (keyOrRange.domain() != Key)
                return true;

            SyncLocal knownLocal = status.compareTo(APPLIED) >= 0 ? Self : NoLocal;
            SyncRemote knownRemote = minDurability.isDurable() ? Quorum : NoRemote;
            if (found != null && (found.knownRemote.compareTo(knownRemote) > 0 || found.knownRemote == knownRemote && found.knownLocal.compareTo(knownLocal) >= 0))
                return true;

            found = new Found(txnId, find, knownLocal, knownRemote);
            return !(knownLocal.compareTo(syncLocal) >= 0 && knownRemote.compareTo(syncRemote) >= 0);
        }

        @Override
        public Found reduce(Found o1, Found o2)
        {
            throw illegalState("Should not be possible to find multiple transactions");
        }

        @Nullable
        @Override
        public TxnId primaryTxnId()
        {
            return null;
        }

        @Override
        public String reason()
        {
            return "Find existing transaction";
        }

        @Override
        public void accept(Found result, Throwable failure)
        {
            this.result.accept(result, failure);
        }
    }

    public static AsyncChain<Void> awaitLocal(Node node, TxnId txnId, RoutingKey key)
    {
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super Void, Throwable> callback)
            {
                Await await = new Await(txnId, RoutingKeys.of(key), IsApplied, txnId.epoch(), txnId.epoch(), -1, true)
                {
                    @Override
                    protected void reply(AwaitOk reply, Throwable failure)
                    {
                        callback.accept(null, failure);
                    }
                };
                await.process(node, null, null);
                return null;
            }
        };
    }

    public static AsyncChain<Boolean> await(Node node, ExclusiveAsyncExecutor executor, Found found, SyncLocal syncLocal, SyncRemote syncRemote)
    {
        if (found == null)
            return AsyncChains.success(false);

        if (found.knownLocal.compareTo(syncLocal) < 0)
            return awaitLocal(node, found.txnId, found.key)
                   .flatMap(ignore -> awaitRemote(node, executor, found, syncRemote));

        if (found.knownRemote.compareTo(syncRemote) < 0)
            return awaitRemote(node, executor, found.txnId, found.key);

        return AsyncChains.success(true);
    }

    public static AsyncChain<Boolean> awaitRemote(Node node, ExclusiveAsyncExecutor executor, Found found, SyncRemote syncRemote)
    {
        if (found.knownRemote.compareTo(syncRemote) >= 0)
            return AsyncChains.success(true);

        return awaitRemote(node, executor, found.txnId, found.key);
    }

    public static AsyncChain<Boolean> awaitRemote(Node node, ExclusiveAsyncExecutor executor, TxnId txnId, RoutingKey key)
    {
        RoutingKeys keys = RoutingKeys.of(key);
        return SynchronousAwait.awaitQuorum(node, executor, txnId, keys, IsApplied, true);
    }
}
