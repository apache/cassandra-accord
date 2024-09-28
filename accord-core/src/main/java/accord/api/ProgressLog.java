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

package accord.api;

import javax.annotation.Nullable;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SaveStatus;
import accord.local.Status.Durability;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.SortedArrays;

import static accord.api.ProgressLog.BlockedUntil.Query.HOME;
import static accord.api.ProgressLog.BlockedUntil.Query.SHARD;
import static accord.local.Status.Durability.Majority;
import static accord.local.Status.Durability.NotDurable;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * This interface is responsible for managing incomplete transactions and ensuring they complete.
 * This includes both ensuring a transaction is coordinated (normally handled by the home shard only)
 * and ensuring the local command store executes the transaction.
 *
 * The basic logical flow for ensuring a transaction is committed and applied at all replicas is as follows:
 *
 *  - Members of the home shard will be informed of a transaction to monitor. This happens as soon as the
 *    home shard receives any Route. If this is not followed closely by {@link SaveStatus#Stable},
 *    {@link accord.coordinate.MaybeRecover} should be invoked to ensure the transaction is decided and executed.
 *
 * TODO (now): finish rewriting description and port to DefaultProgressLog
 *
 *  - Non-home shards that have not witnessed an Accept phase or later should inform the home shard of the transaction.
 *    This can be done at any time. The default implementation does this only when the transaction is blocking the
 *    progress of some other transaction.
 *
 *  - If a {@code blockedBy} transaction is uncommitted it is required that the progress log invoke
 *    {@link accord.coordinate.FetchData#fetch} for the transaction if no {@link #stable} is witnessed.
 *
 *  - Members of the home shard will later be informed that the transaction is {@link #readyToExecute}.
 *    If this is not followed closely by {@link #preapplied}, {@link accord.coordinate.MaybeRecover} should be invoked.
 *
 *  - Finally, it is up to each shard to independently coordinate disseminating the write to every replica.
 */
public interface ProgressLog
{
    interface Factory
    {
        ProgressLog create(CommandStore store);
    }

    enum BlockedUntil
    {
        /**
         * Wait for the transaction to decide its executeAt (or else decide to be invalidated).
         *
         * This also waits for a FullRoute to be known.
         */
        HasDecidedExecuteAt(HOME, HOME, SaveStatus.PreCommitted, NotDurable),

        /**
         * Wait for the transaction to be Committed.
         *
         * Note that we only set Committed during coordination, we do not propagate Committed directly between replicas,
         * so for local progress it only makes sense to request HasStableDeps.
         *
         * This BlockedUntil is useful for remote listeners performing recovery that are waiting for transactions in
         * the Accept phase that need to reach Committed to advance the recovery machine.
         */
        HasCommittedDeps(SHARD, SHARD, SaveStatus.Committed, NotDurable),

        /**
         * Wait for the transaction's dependencies to stabilise. This provides enough information
         * to locally execute a transaction (if all the dependencies have applied).
         */
        HasStableDeps(SHARD, SHARD, SaveStatus.Stable, NotDurable),

        /**
         * Wait for all shards to be ReadyToExecute so that a recovery coordinator may make progress
         */
        CanCoordinateExecution(SHARD, SHARD, SaveStatus.ReadyToExecute, NotDurable),

        /**
         * Wait for the transaction to have enough information to apply.
         * It does not need to be ready to apply yet.
         */
        CanApply(HOME, SHARD, SaveStatus.PreApplied, Majority);

        public enum Query { HOME, SHARD }

        private static final BlockedUntil[] lookup = values();
        public final Query waitsOn, fetchFrom;
        public final SaveStatus minSaveStatus;
        // have remote listeners wait for the Durability before responding
        // this permits us to wait for only the home shard for CanApply
        public final Durability remoteDurability;

        BlockedUntil(Query waitsOn, Query fetchFrom, SaveStatus minSaveStatus, Durability remoteDurability)
        {
            this.waitsOn = waitsOn;
            this.fetchFrom = fetchFrom;
            this.minSaveStatus = minSaveStatus;
            this.remoteDurability = remoteDurability;
        }

        public long fetchEpoch(TxnId txnId, Timestamp executeAt)
        {
            if (this != CanApply || executeAt == null || executeAt.equals(Timestamp.NONE))
                return txnId.epoch();

            return executeAt.epoch();
        }

        public static BlockedUntil forOrdinal(int ordinal)
        {
            if (ordinal < 0 || ordinal > lookup.length)
                throw new IndexOutOfBoundsException(ordinal);
            return lookup[ordinal];
        }

        public static BlockedUntil forSaveStatus(SaveStatus saveStatus)
        {
            int i = SortedArrays.binarySearch(lookup, 0, lookup.length, saveStatus, (s, w) -> s.compareTo(w.minSaveStatus), FAST);
            if (i < 0) i = Math.max(0, -2 - i);
            return lookup[i];
        }
    }

    /**
     * Record an updated local status for the transaction, to clear any waiting state it satisfies.
     */
    void update(SafeCommandStore safeStore, TxnId txnId, Command before, Command after);

    void remoteCallback(SafeCommandStore safeStore, SafeCommand safeCommand, SaveStatus remoteStatus, int callbackId, Node.Id from);

    /**
     * The parameter is a command that some other command's execution is most proximally blocked by.
     * This may be invoked by either the home or non-home command store.
     * <p>
     * If invoked by the non-home command store for a {@code blockedBy} transaction that has not yet been committed, this
     * must eventually trigger contact with the home shard of this {@code blockedBy} transaction in order to check on the
     * transaction's progress (unless the transaction is committed first). This is to avoid unnecessary additional messages
     * being exchanged in the common case, where a transaction may be committed successfully to members of its home shard,
     * but not to all non-home shards. In such a case the transaction may be a false-dependency of another transaction that
     * needs to perform a read, and all nodes which may do so are waiting for the commit record to arrive.
     * <p>
     * If a quorum of the home shard does not know of the transaction, then we can ask the home shard to perform recovery
     * to either complete or invalidate it, so that we may make progress.
     * <p>
     * In all other scenarios, the implementation is free to choose its course of action.
     * <p>
     * Either blockedOnRoute or blockedOnParticipants should be non-null.
     *
     * @param blockedUntil          what we are waiting for
     * @param blockedBy             is the transaction id that is blocking progress
     * @param blockedOnRoute        the route (if any) we are blocked on execution for
     * @param blockedOnParticipants the participating keys on which we are blocked for execution
     */
    void waiting(BlockedUntil blockedUntil, SafeCommandStore safeStore, SafeCommand blockedBy, @Nullable Route<?> blockedOnRoute, @Nullable Participants<?> blockedOnParticipants);

    /**
     * We have finished processing this transaction; ensure its state is cleared
     */
    void clear(TxnId txnId);

    void clear();

    class NoOpProgressLog implements ProgressLog
    {
        @Override public void update(SafeCommandStore safeStore, TxnId txnId, Command before, Command after) {}
        @Override public void remoteCallback(SafeCommandStore safeStore, SafeCommand safeCommand, SaveStatus remoteStatus, int callbackId, Node.Id from) {}
        @Override public void waiting(BlockedUntil blockedUntil, SafeCommandStore safeStore, SafeCommand blockedBy, Route<?> blockedOnRoute, Participants<?> blockedOnParticipants) {}
        @Override public void clear(TxnId txnId) {}
        @Override public void clear() {}
    }
}
