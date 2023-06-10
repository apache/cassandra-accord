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

import accord.coordinate.InformHomeOfTxn;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.SafeCommand;
import accord.local.Status.Known;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.TxnId;

/**
 * This interface is responsible for managing incomplete transactions *and retrying them*.
 * Each stage is fenced by two methods, one entry and one exit. The entry method notifies the implementation
 * that it should soon be notified of the exit method, and if it is not that it should invoke some
 * pre-specified recovery mechanism.
 *
 * This is a per-CommandStore structure, with transactions primarily being managed by their home shard,
 * except during PreAccept as a transaction may not yet have been durably recorded by the home shard.
 *
 * The basic logical flow for ensuring a transaction is committed and applied at all replicas is as follows:
 *
 *  - First, ensure a quorum of the home shard is aware of the transaction by invoking {@link InformHomeOfTxn}.
 *    Entry by {@link #preaccepted} Non-home shards may now forget this transaction for replay purposes.
 *
 *  - Non-home shards may also be informed of transactions that are blocking the progress of other transactions.
 *    If the {@code waitingOn} transaction that is blocking progress is uncommitted it is required that the progress
 *    log invoke {@link accord.coordinate.FetchData#fetch} for the transaction if no {@link #committed} is witnessed.
 *
 *  - Members of the home shard will be informed of a transaction to monitor by the invocation of {@link #preaccepted} or
 *    {@link #accepted}. If this is not followed closely by {@link #committed}, {@link accord.coordinate.MaybeRecover} should
 *    be invoked.
 *
 *  - Members of the home shard will later be informed that the transaction is {@link #readyToExecute}.
 *    If this is not followed closely by {@link #executed}, {@link accord.coordinate.MaybeRecover} should be invoked.
 *
 *  - Finally, it is up to each shard to independently coordinate disseminating the write to every replica.
 */
public interface ProgressLog
{
    interface Factory
    {
        ProgressLog create(CommandStore store);
    }

    enum ProgressShard
    {
        /* We do not have enough information to say whether the shard is a progress shard or not */
        Unsure,

        /**
         * This shard is not a progress shard
         */
        No,

        /* Adhoc Local Progress Shard, i.e. where the local node is not a replica for the coordination epoch */
        Adhoc,

        /* Designated Local Progress Shard (selected from keys replicated locally at coordination epoch) */
        Local,

        /* Designated Home (Global Progress) Shard (if local node is a replica of home key on coordination epoch) */
        Home,

        /** A special variant of Home that is not managed by the progress log */
        UnmanagedHome;

        public boolean isHome() { return this.compareTo(Home) >= 0; }
        public boolean isProgress() { return this.compareTo(Local) >= 0 && this.compareTo(Home) <= 0; }
    }

    /**
     * Has not been pre-accepted, but has been witnessed by ourselves (only partially) or another node that has informed us
     *
     * A home shard should monitor this transaction for global progress.
     * A non-home shard should not receive this message.
     */
    void unwitnessed(TxnId txnId, ProgressShard shard);

    /**
     * Has been pre-accepted.
     *
     * A home shard should monitor this transaction for global progress.
     * A non-home shard should begin monitoring this transaction only to ensure it reaches the Accept phase, or is
     * witnessed by a majority of the home shard.
     */
    void preaccepted(Command command, ProgressShard shard);

    /**
     * Has been accepted
     *
     * A home shard should monitor this transaction for global progress.
     * A non-home shard can safely ignore this transaction, as it has been witnessed by a majority of the home shard.
     */
    void accepted(Command command, ProgressShard shard);

    /**
     * Has committed
     *
     * A home shard should monitor this transaction for global progress.
     * A non-home shard can safely ignore this transaction, as it has been witnessed by a majority of the home shard.
     */
    void committed(Command command, ProgressShard shard);

    /**
     * The transaction is waiting to make progress, as all local dependencies have applied.
     *
     * A home shard should monitor this transaction for global progress.
     * A non-home shard can safely ignore this transaction, as it has been witnessed by a majority of the home shard.
     */
    void readyToExecute(Command command, ProgressShard shard);

    /**
     * The transaction's outcome has been durably recorded (but not necessarily applied) locally.
     * It will be applied once all local dependencies have been.
     *
     * Invoked on both home and non-home command stores, and is required to trigger per-shard processes
     * that ensure the transaction's outcome is durably persisted on all replicas of the shard.
     *
     * May also permit aborting a pending waitingOn-triggered event.
     */
    void executed(Command command, ProgressShard shard);

    /**
     * The transaction's outcome has been durably recorded (but not necessarily applied) at a quorum of all shards,
     * including at least those node's ids that are provided.
     *
     * If this replica has not witnessed the outcome of the transaction, it should poll a majority of each shard
     * for its outcome.
     *
     * Otherwise, this transaction no longer needs to be monitored, but implementations may wish to ensure that
     * the result is propagated to every live replica.
     */
    void durable(Command command);

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
     *
     * Either blockedOnRoute or blockedOnParticipants should be non-null.
     *
     * @param blockedBy             is the transaction id that is blocking progress
     * @param blockedUntil          either Committed or Executed; the state we are waiting for
     * @param blockedOnRoute        the route (if any) we are blocked on execution for
     * @param blockedOnParticipants the participating keys on which we are blocked for execution
     */
    void waiting(SafeCommand blockedBy, Known blockedUntil, @Nullable Route<?> blockedOnRoute, @Nullable Participants<?> blockedOnParticipants);

    /**
     * We have finished processing this transaction; ensure its state is cleared
     */
    void clear(TxnId txnId);
}
