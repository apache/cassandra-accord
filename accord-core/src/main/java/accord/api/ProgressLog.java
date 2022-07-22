package accord.api;

import java.util.Set;

import javax.annotation.Nullable;

import accord.coordinate.CheckOnUncommitted;
import accord.coordinate.InformHomeOfTxn;
import accord.local.CommandStore;
import accord.local.Node.Id;
import accord.local.Status;
import accord.primitives.AbstractRoute;
import accord.primitives.RoutingKeys;
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
 *    Entry by {@link #preaccept} Non-home shards may now forget this transaction for replay purposes.
 *
 *  - Non-home shards may also be informed of transactions that are blocking the progress of other transactions.
 *    If the {@code waitingOn} transaction that is blocking progress is uncommitted it is required that the progress
 *    log invoke {@link CheckOnUncommitted} for the transaction if no {@link #commit} is witnessed.
 *
 *  - Members of the home shard will be informed of a transaction to monitor by the invocation of {@link #preaccept} or
 *    {@link #accept}. If this is not followed closely by {@link #commit}, {@link accord.coordinate.MaybeRecover} should
 *    be invoked.
 *
 *  - Members of the home shard will later be informed that the transaction is {@link #readyToExecute}.
 *    If this is not followed closely by {@link #execute}, {@link accord.coordinate.MaybeRecover} should be invoked.
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
        Home;

        public boolean isHome() { return this == Home; }
        public boolean isProgress() { return this.compareTo(Local) >= 0; }
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
    void preaccept(TxnId txnId, ProgressShard shard);

    /**
     * Has been accepted
     *
     * A home shard should monitor this transaction for global progress.
     * A non-home shard can safely ignore this transaction, as it has been witnessed by a majority of the home shard.
     */
    void accept(TxnId txnId, ProgressShard shard);

    /**
     * Has committed
     *
     * A home shard should monitor this transaction for global progress.
     * A non-home shard can safely ignore this transaction, as it has been witnessed by a majority of the home shard.
     */
    void commit(TxnId txnId, ProgressShard shard);

    /**
     * The transaction is waiting to make progress, as all local dependencies have applied.
     *
     * A home shard should monitor this transaction for global progress.
     * A non-home shard can safely ignore this transaction, as it has been witnessed by a majority of the home shard.
     */
    void readyToExecute(TxnId txnId, ProgressShard shard);

    /**
     * The transaction's outcome has been durably recorded (but not necessarily applied) locally.
     * It will be applied once all local dependencies have been.
     *
     * Invoked on both home and non-home command stores, and is required to trigger per-shard processes
     * that ensure the transaction's outcome is durably persisted on all replicas of the shard.
     *
     * May also permit aborting a pending waitingOn-triggered event.
     */
    void execute(TxnId txnId, ProgressShard shard);

    /**
     * The transaction has been durably invalidated
     */
    void invalidate(TxnId txnId, ProgressShard shard);

    /**
     * The transaction's outcome has been durably recorded (but not necessarily applied) locally at all shards.
     *
     * This is only invoked on the home shard, once all local shards have successfully applied.
     */
    void durableLocal(TxnId txnId);

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
    void durable(TxnId txnId, RoutingKey homeKey, @Nullable Set<Id> persistedOn);

    /**
     * The transaction's outcome has been durably recorded (but not necessarily applied) at a quorum of all shards.
     *
     * If this replica has not witnessed the outcome of the transaction, it should poll a majority of each shard
     * for its outcome, using the provided route (if any).
     */
    void durable(TxnId txnId, @Nullable AbstractRoute route, ProgressShard shard);

    /**
     * The parameter is a command that some other command's execution is most proximally blocked by.
     * This may be invoked by either the home or non-home command store.
     *
     * If invoked by the non-home command store for a {@code blockedBy} transaction that has not yet been committed, this
     * must eventually trigger contact with the home shard of this {@code blockedBy} transaction in order to check on the
     * transaction's progress (unless the transaction is committed first). This is to avoid unnecessary additional messages
     * being exchanged in the common case, where a transaction may be committed successfully to members of its home shard,
     * but not to all non-home shards. In such a case the transaction may be a false-dependency of another transaction that
     * needs to perform a read, and all nodes which may do so are waiting for the commit record to arrive.
     *
     * If a quorum of the home shard does not know of the transaction, then we can ask the home shard to perform recovery
     * to either complete or invalidate it, so that we may make progress.
     *
     * In all other scenarios, the implementation is free to choose its course of action.
     */
    void waiting(TxnId blockedBy, Status blockedUntil, RoutingKeys someKeys);
}
