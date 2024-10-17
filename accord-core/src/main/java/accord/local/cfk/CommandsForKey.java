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

package accord.local.cfk;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.impl.CommandsSummary;
import accord.local.Command;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.SafeCommandStore.CommandFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.local.SafeCommandStore.TestStatus;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.local.cfk.PostProcess.NotifyUnmanagedResult;
import accord.local.cfk.Pruning.LoadingPruned;
import accord.primitives.Ballot;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.btree.BTree;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasStableDeps;
import static accord.local.cfk.CommandsForKey.InternalStatus.ACCEPTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.APPLIED;
import static accord.local.cfk.CommandsForKey.InternalStatus.PREACCEPTED_OR_ACCEPTED_INVALIDATE;
import static accord.local.cfk.CommandsForKey.InternalStatus.STABLE;
import static accord.local.cfk.CommandsForKey.InternalStatus.HISTORICAL;
import static accord.local.cfk.CommandsForKey.InternalStatus.INVALID_OR_TRUNCATED_OR_PRUNED;
import static accord.local.cfk.CommandsForKey.InternalStatus.TRANSITIVELY_KNOWN;
import static accord.local.cfk.PostProcess.notifyManagedPreBootstrap;
import static accord.local.cfk.Pruning.isAnyPredecessorWaitingOnPruned;
import static accord.local.cfk.Pruning.isWaitingOnPruned;
import static accord.local.cfk.Pruning.loadingPrunedFor;
import static accord.local.cfk.Pruning.pruneById;
import static accord.local.cfk.Pruning.prunedBeforeId;
import static accord.local.cfk.UpdateUnmanagedMode.UPDATE;
import static accord.local.cfk.Updating.insertOrUpdate;
import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Kinds.AnyGloballyVisible;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.Paranoia.NONE;
import static accord.utils.Invariants.Paranoia.SUPERLINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.LOW;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.isParanoid;
import static accord.utils.Invariants.testParanoia;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * REMEMBER AT ALL TIMES WHEN MODIFYING.
 * This class does THREE things:
 *  - Calculates execution dependencies. For this we must witness all transactions that may execute before
 *    (at least via some transitive relation). We cannot remove transactions we have executed locally unless
 *    they are represented by some other transaction we return (such as an exclusive sync point or a later
 *    transaction that has it durably as a dependency).
 *    This includes transactions we have imported from earlier epochs, either directly or transitively
 *    via the dependencies of other transactions we know of.
 *  - Computes recovery decisions.
 *      - This missing collection is involved here, to decide if the transaction we are recovering
 *        has been witnessed and therefore may have taken the fast path.
 *      - This must include any transactions from future epochs, for which we must maintain accurate deps
 *        (but do not execute, and should never set to APPLIED)
 *  - Ensures transactions execute locally in the correct order.
 *      - The missing collection is involved in this, but can ignore any transactions that are pre-bootstrap,
 *        or execute outside of our epoch ownership information.
 *
 *  Each of these features has different constraints and requirements. Because a change satisfies one or two of the three
 *  does not mean it is safe!
 *
 * <h2>Introduction</h2>
 * A specialised collection for efficiently representing and querying everything we need for making coordination
 * and recovery decisions about a key's command conflicts, and for managing execution order.
 *
 * Every command we know about that is not shard-redundant is listed in the {@code byId} collection, which is sorted by {@code TxnId}.
 * This includes all transitive dependencies we have witnessed via other transactions, but not witnessed directly.
 *
 * <h2>Contents</h2>
 * This collection tracks kinds of transactions differently:
 * - Range transactions are tracked ONLY as dependencies; if no managed key transactions witness a range transaction
 *   it will not be tracked here. The dependencies of range transactions are not themselves tracked at all.
 *   A range transaction that depends on some key for execution will be registered as an unmanaged transaction
 *   to track when it may be executed.
 * - Key (Exclusive)?SyncPoints are tracked fully until execution; we fully encode their dependencies, and track their lifecycle.
 *   This permits them to be consulted for recovery. Once they are stable, they will be registered as unmanaged transactions for execution.
 * - Key Reads and Writes are first class citizens. We fully encode their dependencies, track their lifecycle and also
 *   directly manage their execution.
 *
 * <h2>Dependency Encoding</h2>
 * The byId list implies the contents of the deps of all commands in the collection - that is, it is assumed that in
 * the normal course of events every transaction will include the full set of {@code TxnId} we know that could be
 * witnessed by the command. We only encode divergences from this, stored in each command's {@code missing} collection.
 *
 * We then go one step further, exploiting the fact that the missing collection exists solely to implement recovery,
 * and so we elide from this missing collection any {@code TxnId} we have recorded as {@code Committed} or higher.
 * Any recovery coordinator that contacts this replica will report that the command has been agreed to execute,
 * and so will not need to decipher any fast-path decisions. So the missing collection is redundant, as no command's deps
 * will need to be queried for this TxnId's presence/absence.
 * TODO (expected) this logic applies equally well to Accepted
 *
 * The goal with these behaviours is that this missing collection will ordinarily be empty, occupying no space.
 *
 * <h2>Garbage Collection</h2>
 * This collection is trimmed by two mechanisms: pruning applied transactions and removing redundant transactions.
 *
 * 1) redundantBefore represents the local lower bound covering the key for transactions having all been applied or
 * invalidated. This is not the global lower bound, but we know that anything with a lower TxnId is either committed
 * locally and we will report this decision to any distributed recovery OR it will not execute and so we may safely
 * report that it is unknown to us in this collection. So we may simply erase these.
 *
 * 2) prunedBefore represents a local bound that permits us to optimistically remove data from the CommandsForKey
 * that may need to be loaded again later. Specifically, we pick an applied {@code TxnId} that we will retain, and we
 * remove from the {@code CommandsForKey} any transaction with a lower {@code TxnId} and {@code executeAt} that is also
 * applied or invalidated.
 *
 * [We only do this if there also exists some later transactions we are not pruning that collectively have a superset of
 * its {@code missing} collection, so that recovery decisions will be unaffected by the removal of the transaction.]
 *
 * The complexity here is that, by virtue of being a local decision point, we cannot guarantee that no coordinator will
 * contact us in future with either a new TxnId that is lower than this, or a dependency collection containing a TxnId
 * we have already processed.
 *
 * [We pick a TxnId stale by some time bound so that we can expect that any earlier already-applied TxnId will
 * not be included in a future set of dependencies - we expect that "transitive dependency elision" will ordinarily filter
 * it; but it might not on all replicas.]
 *
 * The difficulty is that we cannot immediately distinguish these two cases, and so on encountering a TxnId that is
 * less than our {@code prunedBefore} we must load the local command state for the TxnId. If we have not witnessed the
 * TxnId then we know it is a new transitive dependency. If we have witnessed it, and it is applied, then we load it
 * into the {@code CommandsForKey} until we next prune to avoid reloading it repeatedly. This is managed with the
 * {@link #loadingPruned} btree collection.
 *
 * <h2>Transitive Dependency Elision</h2>
 * {@code CommandsForKey} also implements transitive dependency elision.
 * When evaluating {@code mapReduceActive}, we first establish the last-executing Stable write command (i.e. those whose deps
 * are considered durably decided, and so must wait for all commands {@code Committed} with a lower {@code executeAt}).
 * We then elide any {@code Committed} command that has a lower executeAt than this command that would be witnessed by that command.
 *
 * Both commands must be known at a majority, but neither might be {@code Committed} at any other replica.
 * Either command may therefore be recovered.
 * If the later command is recovered, this replica will report its Stable deps, thereby recovering them.
 * If this replica is not contacted, some other replica must participate that either has taken the same action as this replica,
 * or else does not know the later command is Stable, and so will report the earlier command as a dependency again.
 * If the earlier command is recovered, this replica will report that it is {@code Committed}, and so will not consult
 * this replica's collection to decipher any fast path decision. Any other replica must either do the same, or else
 * will correctly record this transaction as present in any relevant deps of later transactions.
 *
 * TODO (required): tighten semantics around transactions that are not owned by this CFK, which occur in two to three cases:
 *      1) when a dependency calculation must report dependencies imported from a prior epoch.
 *         in this case we do not need to manage dependencies for the transaction, only report the transaction itself.
 *      2) when a transaction proposed in a future epoch visits an earlier epoch, it is registered here for recovery
 *         decisions, so that recovery does not need to contact future epochs to find any superseding transactions
 *      3) when an accept round visits a later epoch than the one in which it is agreed.
 * TODO (desired):  track whether a TxnId is a write on this key only for execution (rather than globally)
 * TODO (expected): merge with TimestampsForKey
 * TODO (desired):  save space by encoding InternalStatus in TxnId.flags(), so that when executeAt==txnId we can save 8 bytes per entry
 * TODO (expected): remove a command that is committed to not intersect with the key for this store (i.e. if accepted in a later epoch than committed on, so ownership changes)
 * TODO (expected): avoid updating transactions we don't manage the execution of - perhaps have a dedicated InternalStatus
 * TODO (expected): minimise repeated notification, either by logic or marking a command as notified once ready-to-execute
 * TODO (required): better linearizability violation detection
 * TODO (expected): cleanup unmanaged transitively known transactions
 * TODO (desired): introduce a new status or other fast and simple mechanism for filtering treatment of range or unmanaged transactions
 * TODO (desired): store missing transactions against the highest known transaction only (this should also permit us to prune better by ignoring the missing collection contents)
 */
public class CommandsForKey extends CommandsForKeyUpdate implements CommandsSummary
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);

    private static boolean reportLinearizabilityViolations = true;
    private static final boolean ELIDE_TRANSITIVE_DEPENDENCIES = true;

    public static final RedundantBefore.Entry NO_BOUNDS_INFO = new RedundantBefore.Entry(null, Long.MIN_VALUE, Long.MAX_VALUE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE, null);
    public static final TxnInfo NO_INFO = TxnInfo.create(TxnId.NONE, HISTORICAL, false, TxnId.NONE, Ballot.ZERO);
    public static final TxnInfo[] NO_INFOS = new TxnInfo[0];
    public static final Unmanaged[] NO_PENDING_UNMANAGED = new Unmanaged[0];

    /**
     * Transactions that are witnessed by {@code CommandsForKey} for dependency management
     * (essentially all globally visible key transactions).
     */
    public static boolean manages(TxnId txnId)
    {
        return txnId.is(Key) && txnId.isVisible();
    }

    /**
     * Transactions whose execution will be wholly managed by {@code CommandsForKey} (essentially reads and writes).
     *
     * Other transactions that depend on these transactions need only adopt a dependency on the {@code Key} to represent
     * all of these transactions; the {@code CommandsForKey} will then notify when they have executed.
     */
    public static boolean managesExecution(TxnId txnId)
    {
        return Write.witnesses(txnId.kind()) && txnId.is(Key);
    }

    public boolean executes(TxnId txnId, Timestamp executeAt)
    {
        return executes(boundsInfo, txnId, executeAt);
    }

    public boolean executes(Timestamp executeAt)
    {
        return executes(boundsInfo, executeAt);
    }

    public boolean mayExecute(TxnId txnId)
    {
        return mayExecute(boundsInfo, txnId);
    }

    public boolean mayExecute(TxnInfo txn)
    {
        return mayExecute(txn, txn.isCommittedToExecute() ? txn.executeAt : null);
    }

    public boolean mayExecute(TxnId txnId, InternalStatus status, Command command)
    {
        return mayExecute(txnId, status.isCommittedToExecute() ? command.executeAt() : null);
    }

    public boolean mayExecute(TxnId txnId, @Nullable Timestamp committedToExecuteAt)
    {
        if (!mayExecute(boundsInfo, txnId)) return false;
        return committedToExecuteAt == null || executes(boundsInfo, committedToExecuteAt);
    }

    public static boolean executes(RedundantBefore.Entry boundsInfo, TxnId txnId, Timestamp executeAt)
    {
        return managesExecution(txnId)
               && boundsInfo.bootstrappedAt.compareTo(txnId) < 0
               && boundsInfo.endOwnershipEpoch > executeAt.epoch()
               && boundsInfo.startOwnershipEpoch <= executeAt.epoch();
    }

    private static boolean executes(RedundantBefore.Entry boundsInfo, Timestamp executeAt)
    {
        return boundsInfo.endOwnershipEpoch > executeAt.epoch() && boundsInfo.startOwnershipEpoch <= executeAt.epoch();
    }

    public static boolean mayExecute(RedundantBefore.Entry boundsInfo, TxnId txnId)
    {
        return managesExecution(txnId)
               && boundsInfo.endOwnershipEpoch > txnId.epoch()
               && boundsInfo.bootstrappedAt.compareTo(txnId) < 0;
    }

    public static boolean needsUpdate(Command prev, Command updated)
    {
        SaveStatus prevStatus;
        Ballot prevAcceptedOrCommitted;
        if (prev == null)
        {
            prevStatus = SaveStatus.NotDefined;
            prevAcceptedOrCommitted = Ballot.ZERO;
        }
        else
        {
            prevStatus = prev.saveStatus();
            prevAcceptedOrCommitted = prev.acceptedOrCommitted();
        }

        return needsUpdate(prevStatus, prevAcceptedOrCommitted, updated.saveStatus(), updated.acceptedOrCommitted());
    }

    public static boolean needsUpdate(SaveStatus prevStatus, Ballot prevAcceptedOrCommitted, SaveStatus updatedStatus, Ballot updatedAcceptedOrCommitted)
    {
        InternalStatus prev = InternalStatus.from(prevStatus);
        InternalStatus updated = InternalStatus.from(updatedStatus);
        return updated != prev || (updated != null && updated.hasExecuteAtOrDeps && !prevAcceptedOrCommitted.equals(updatedAcceptedOrCommitted));
    }

    public static class SerializerSupport
    {
        public static CommandsForKey create(RoutingKey key, TxnInfo[] txns, Unmanaged[] unmanageds, TxnId prunedBefore, RedundantBefore.Entry boundsInfo)
        {
            return reconstruct(key, boundsInfo, txns, prunedBefore, unmanageds);
        }
    }

    interface Updater<O>
    {
        O update(RoutingKey key, RedundantBefore.Entry boundsInfo, TxnInfo[] byId, TxnInfo[] committedByExecuteAt, int minUndecidedById, int maxAppliedWriteByExecuteAt, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds);
    }

    /**
     * An object representing the basic CommandsForKey state, extending TxnId to save memory and improve locality.
     */
    public static class TxnInfo extends TxnId
    {
        // TODO (desired): consider saving in TxnId flag bits (plenty of room); can then store just a TxnId in most cases
        static final int MAY_EXECUTE = 0x10;
        static final int MANAGED = 0x20;
        static final int COMMITTED_TO_EXECUTE = 0x40;
        static final int HAS_DEPS = 0x80;
        static final int DEPS_KNOWN_UNTIL_EXECUTE_AT = 0x100;
        static final int COMMITTED_AND_EXECUTES = MAY_EXECUTE | COMMITTED_TO_EXECUTE;
        static final int NOTIFIED_READY = 0x1000;
        static final int NOTIFIED_WAITING = 0x2000;

        final int encodedStatus;
        public final Timestamp executeAt;

        private TxnInfo(TxnId txnId, int encodedStatus, Timestamp executeAt)
        {
            super(txnId);
            Invariants.checkState(executeAt == txnId || !executeAt.equals(txnId));
            this.encodedStatus = encodedStatus;
            this.executeAt = executeAt == txnId ? this : executeAt;
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, Command command)
        {
            Timestamp executeAt = txnId;
            if (status.hasExecuteAt()) executeAt = command.executeAt();
            if (executeAt.equals(txnId)) executeAt = txnId;
            Ballot ballot;
            int encodedStatus = encode(txnId, status, mayExecute);
            if (!status.hasBallot || (ballot = command.acceptedOrCommitted()).equals(Ballot.ZERO))
                return new TxnInfo(txnId, encodedStatus, executeAt);
            return new TxnInfoExtra(txnId, encodedStatus, executeAt, NO_TXNIDS, ballot);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, @Nonnull Timestamp executeAt, @Nonnull Ballot ballot)
        {
            return create(txnId, status, mayExecute, 0, executeAt, ballot);
        }
        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, int overrideFlags, @Nonnull Timestamp executeAt, @Nonnull Ballot ballot)
        {
            return create(txnId, status, mayExecute, overrideFlags, executeAt, NO_TXNIDS, ballot);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing, @Nonnull Ballot ballot)
        {
            return create(txnId, status, mayExecute, 0, executeAt, missing, ballot);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, int overrideFlags, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing, @Nonnull Ballot ballot)
        {
            Invariants.checkState(executeAt == txnId || !executeAt.equals(txnId));
            Invariants.checkState(status.hasExecuteAtOrDeps || executeAt == txnId);
            Invariants.checkState(status.hasBallot || ballot == Ballot.ZERO);
            Invariants.checkState(status.hasExecuteAtOrDeps || missing == NO_TXNIDS);
            int encodedStatus = encode(txnId, status, mayExecute, ((overrideFlags & 1) != 0) ^ status.hasDeps(), ((overrideFlags & 2) != 0) ^ status.depsKnownUntilExecuteAt());
            if (missing == NO_TXNIDS && (!status.hasBallot || ballot == Ballot.ZERO))
                return new TxnInfo(txnId, encodedStatus, executeAt);
            Invariants.checkState(missing.length > 0 || missing == NO_TXNIDS);
            return new TxnInfoExtra(txnId, encodedStatus, executeAt, missing, ballot);
        }

        boolean is(InternalStatus status)
        {
            return (encodedStatus & 0xf) == status.ordinal();
        }

        boolean is(TestStatus testStatus)
        {
            InternalStatus status = status();
            switch (testStatus)
            {
                default: throw new AssertionError("Unhandled TestStatus: " + testStatus);
                case IS_PROPOSED:
                    return status.isProposed();
                case IS_STABLE:
                    return status.isStable();
                case ANY_STATUS:
                    return status != TRANSITIVELY_KNOWN;
            }
        }

        // TODO (expected): document the reason for this, and the justification for why it is safe
        //      At least, ensure and explain how we guarantee that we do not break the calculation of the W set from the protocol.
        //      (Since we only compute W on the coordination epoch, and we should never have a statusOverride for that epoch, this should be safe)
        // bits 0 or 1 may be set.
        public int statusOverrides()
        {
            return   (hasDeps() != status().hasDeps() ? 1 : 0)
                   | (depsKnownUntilExecuteAt() != status().depsKnownUntilExecuteAt() ? 2 : 0);
        }

        // this field may differ from status().hasDeps()
        public boolean hasDeps()
        {
            return 0 != (encodedStatus & HAS_DEPS);
        }

        public boolean isManaged()
        {
            return 0 != (encodedStatus & MANAGED);
        }

        public boolean mayExecute()
        {
            return 0 != (encodedStatus & MAY_EXECUTE);
        }

        public boolean isCommittedToExecute()
        {
            return 0 != (encodedStatus & COMMITTED_TO_EXECUTE);
        }

        // this field may differ from status().depsKnownUntilExecuteAt()
        public boolean depsKnownUntilExecuteAt()
        {
            return 0 != (encodedStatus & DEPS_KNOWN_UNTIL_EXECUTE_AT);
        }

        public boolean isCommittedAndExecutes()
        {
            return (encodedStatus & COMMITTED_AND_EXECUTES) == COMMITTED_AND_EXECUTES;
        }

        public InternalStatus status()
        {
            return InternalStatus.get(encodedStatus & 0xf);
        }

        Timestamp depsKnownBefore()
        {
            return depsKnownUntilExecuteAt() ? executeAt : this;
        }

        public TxnInfo withMissing(TxnId[] newMissing)
        {
            Invariants.checkState(status().hasExecuteAtOrDeps);
            return newMissing == NO_TXNIDS
                   ? new TxnInfo(this, encodedStatus, executeAt)
                   : new TxnInfoExtra(this, encodedStatus, executeAt, newMissing, Ballot.ZERO);
        }

        TxnInfo withEncodedStatus(int encodedStatus)
        {
            return new TxnInfo(this, encodedStatus, executeAt);
        }

        public TxnInfo withMayExecute(boolean mayExecute)
        {
            if (mayExecute() == mayExecute)
                return this;

            return withEncodedStatus((encodedStatus & ~MAY_EXECUTE) | (mayExecute ? MAY_EXECUTE : 0));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TxnInfo info = (TxnInfo) o;
            return status() == info.status()
                   && (executeAt == this ? info.executeAt == info : Objects.equals(executeAt, info.executeAt))
                   && Arrays.equals(missing(), info.missing());
        }

        public TxnId plainTxnId()
        {
            return new TxnId(this);
        }

        public Timestamp plainExecuteAt()
        {
            return executeAt == this ? plainTxnId() : executeAt;
        }

        /**
         * Any uncommitted transactions the owning CommandsForKey is aware of, that could have been included in our
         * dependencies but weren't.
         * <p>
         * That is to say, any TxnId < depsKnownBefore() we have otherwise witnessed that were not witnessed by this transaction.
         */
        public TxnId[] missing()
        {
            return NO_TXNIDS;
        }

        public Ballot ballot()
        {
            return Ballot.ZERO;
        }

        @Override
        public int hashCode()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return "Info{" +
                   "txnId=" + toPlainString() +
                   ", status=" + status() +
                   ", executeAt=" + plainExecuteAt() +
                   '}';
        }

        public String toPlainString()
        {
            return super.toString();
        }

        public int compareExecuteAt(TxnInfo that)
        {
            return this.executeAt.compareTo(that.executeAt);
        }

        public int compareExecuteAtEpoch(TxnInfo that)
        {
            return Long.compare(this.executeAt.epoch(), that.executeAt.epoch());
        }

        Timestamp executeAtIfKnownElseTxnId()
        {
            return status() == INVALID_OR_TRUNCATED_OR_PRUNED ? this : executeAt;
        }

        private static int encode(TxnId txnId, InternalStatus internalStatus, boolean mayExecute)
        {
            int encoded = internalStatus.ordinal() | (mayExecute ? MAY_EXECUTE : 0);
            if (txnId.is(Key)) encoded |= MANAGED;
            if (internalStatus.isCommittedToExecute()) encoded |= COMMITTED_TO_EXECUTE;
            if (internalStatus.hasExecuteAtOrDeps) encoded |= HAS_DEPS;
            if (internalStatus.depsKnownUntilExecuteAt()) encoded |= DEPS_KNOWN_UNTIL_EXECUTE_AT;
            return encoded;
        }

        private static int encode(TxnId txnId, InternalStatus internalStatus, boolean mayExecute, boolean hasDeps, boolean depsKnownUntilExecuteAt)
        {
            int encoded = internalStatus.ordinal() | (mayExecute ? MAY_EXECUTE : 0);
            if (txnId.is(Key)) encoded |= MANAGED;
            if (internalStatus.isCommittedToExecute()) encoded |= COMMITTED_TO_EXECUTE;
            if (hasDeps) encoded |= HAS_DEPS;
            if (depsKnownUntilExecuteAt) encoded |= DEPS_KNOWN_UNTIL_EXECUTE_AT;
            return encoded;
        }

        public boolean hasNotifiedReady()
        {
            return 0 != (encodedStatus & NOTIFIED_READY);
        }

        public boolean hasNotifiedWaiting()
        {
            return 0 != (encodedStatus & NOTIFIED_WAITING);
        }

        public TxnInfo asNotifiedReady()
        {
            return withEncodedStatus(encodedStatus | NOTIFIED_READY);
        }

        public TxnInfo asNotifiedWaiting()
        {
            return withEncodedStatus(encodedStatus | NOTIFIED_WAITING);
        }
    }

    public static class TxnInfoExtra extends TxnInfo
    {
        /**
         * {@link TxnInfo#missing()}
         */
        public final TxnId[] missing;
        public final Ballot ballot;

        TxnInfoExtra(TxnId txnId, int encodedStatus, Timestamp executeAt, TxnId[] missing, Ballot ballot)
        {
            super(txnId, encodedStatus, executeAt);
            this.missing = missing;
            this.ballot = ballot;
        }

        /**
         * {@link TxnInfo#missing()}
         */
        @Override
        public TxnId[] missing()
        {
            return missing;
        }

        @Override
        public Ballot ballot()
        {
            return ballot;
        }

        public TxnInfo withMissing(TxnId[] newMissing)
        {
            if (newMissing == missing)
                return this;

            return newMissing == NO_TXNIDS && ballot == Ballot.ZERO
                   ? new TxnInfo(this, encodedStatus, executeAt)
                   : new TxnInfoExtra(this, encodedStatus, executeAt, newMissing, ballot);
        }

        TxnInfo withEncodedStatus(int encodedStatus)
        {
            return new TxnInfoExtra(this, encodedStatus, executeAt, missing, ballot);
        }

        @Override
        public String toString()
        {
            return "Info{" +
                   "txnId=" + toPlainString() +
                   ", status=" + status() +
                   ", executeAt=" + plainExecuteAt() +
                   (ballot != Ballot.ZERO ? ", ballot=" + ballot : "") +
                   ", missing=" + Arrays.toString(missing) +
                   '}';
        }
    }

    /**
     * A transaction whose key-dependencies for execution are not natively managed by this class.
     * This essentially supports all range commands and key sync points for managing their key execution dependencies.
     * <p>
     * We maintain a sorted list of waiting transactions; we gate this by simple TxnId and executeAt bounds in two phases:
     * 1) pick the highest dependency TxnId on the key; wait for all <= TxnId to commit
     * 2) pick the highest executeAt dependency on the key that executes before the Unmanaged txn, and wait for it and all earlier txn to Apply
     *
     * NOTE: this can create false dependencies on commit phase for earlier TxnId, which could delay transaction execution.
     * This is probably an acceptable trade-off given this vastly simplifies execution and reduces the state we have to manage;
     * we mostly use this for sync point execution; and because Commit should always complete promptly.
     * However, we could improve this in future if we want to, by integrating this execution logic with the execution
     * of regular reads and writes now we anyway often represent these transactions in the TxnInfo collections.
     */
    public static class Unmanaged implements Comparable<Unmanaged>
    {
        public enum Pending { COMMIT, APPLY }

        public final Pending pending;
        public final Timestamp waitingUntil;
        public final TxnId txnId;

        public Unmanaged(Pending pending, TxnId txnId, Timestamp waitingUntil)
        {
            this.pending = pending;
            this.txnId = txnId;
            this.waitingUntil = waitingUntil;
        }

        @Override
        public int compareTo(Unmanaged that)
        {
            if (this.pending != that.pending) return this.pending.compareTo(that.pending);
            int c = this.waitingUntil.compareTo(that.waitingUntil);
            if (c == 0) c = this.txnId.compareTo(that.txnId);
            return c;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Unmanaged unmanaged = (Unmanaged) o;
            return pending == unmanaged.pending && waitingUntil.equals(unmanaged.waitingUntil) && txnId.equals(unmanaged.txnId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(pending, waitingUntil, txnId);
        }

        @Override
        public String toString()
        {
            return "Pending{" + txnId + " until:" + waitingUntil + " " + pending + "}";
        }
    }

    public enum InternalStatus
    {
        // TODO (expected): collapse historical and transitively known into a single concept;
        //  no reason to not witness transitive deps, and then registerHistorical can rely on ExclusiveSyncPoints.
        TRANSITIVELY_KNOWN(false, false),
        HISTORICAL(false, false),
        PREACCEPTED_OR_ACCEPTED_INVALIDATE(false, true),
        ACCEPTED(true, true),
        COMMITTED(true, true),
        STABLE(true, false),
        APPLIED(true, false),
        INVALID_OR_TRUNCATED_OR_PRUNED(false, false);

        static final EnumMap<SaveStatus, InternalStatus> convert = new EnumMap<>(SaveStatus.class);
        static final InternalStatus[] VALUES = values();

        static
        {
            convert.put(SaveStatus.PreAccepted, PREACCEPTED_OR_ACCEPTED_INVALIDATE);
            convert.put(SaveStatus.AcceptedInvalidateWithDefinition, PREACCEPTED_OR_ACCEPTED_INVALIDATE);
            convert.put(SaveStatus.Accepted, ACCEPTED);
            convert.put(SaveStatus.AcceptedWithDefinition, ACCEPTED);
            convert.put(SaveStatus.PreCommittedWithDefinition, PREACCEPTED_OR_ACCEPTED_INVALIDATE);
            convert.put(SaveStatus.PreCommittedWithAcceptedDeps, ACCEPTED);
            convert.put(SaveStatus.PreCommittedWithDefinitionAndAcceptedDeps, ACCEPTED);
            convert.put(SaveStatus.Committed, COMMITTED);
            convert.put(SaveStatus.Stable, STABLE);
            convert.put(SaveStatus.ReadyToExecute, STABLE);
            convert.put(SaveStatus.PreApplied, STABLE);
            convert.put(SaveStatus.Applying, STABLE);
            convert.put(SaveStatus.Applied, APPLIED);
            // TODO (expected): can we simply delete in the following cases?
            convert.put(SaveStatus.TruncatedApplyWithDeps, INVALID_OR_TRUNCATED_OR_PRUNED);
            convert.put(SaveStatus.TruncatedApplyWithOutcome, INVALID_OR_TRUNCATED_OR_PRUNED);
            convert.put(SaveStatus.TruncatedApply, INVALID_OR_TRUNCATED_OR_PRUNED);
            convert.put(SaveStatus.Erased, INVALID_OR_TRUNCATED_OR_PRUNED);
            convert.put(SaveStatus.ErasedOrVestigial, INVALID_OR_TRUNCATED_OR_PRUNED);
            convert.put(SaveStatus.Invalidated, INVALID_OR_TRUNCATED_OR_PRUNED);
        }

        public final boolean hasInfo;
        public final boolean hasExecuteAtOrDeps;
        public final boolean hasBallot;

        InternalStatus(boolean hasExecuteAtOrDeps, boolean hasBallot)
        {
            this.hasExecuteAtOrDeps = hasExecuteAtOrDeps;
            this.hasBallot = hasBallot;
            this.hasInfo = hasExecuteAtOrDeps | hasBallot;
        }

        boolean hasExecuteAt()
        {
            return hasExecuteAtOrDeps;
        }

        private boolean hasDeps()
        {
            return hasExecuteAtOrDeps;
        }

        public boolean isProposed()
        {
            return this == ACCEPTED | this == COMMITTED;
        }

        public boolean isStable()
        {
            return this == STABLE | this == APPLIED;
        }

        public boolean isCommittedToExecute()
        {
            return this == COMMITTED | this == STABLE | this == APPLIED;
        }

        public Timestamp depsKnownBefore(TxnId txnId, Timestamp executeAt)
        {
            return depsKnownUntilExecuteAt() ? executeAt : txnId;
        }

        boolean depsKnownUntilExecuteAt()
        {
            return isCommittedToExecute();
        }

        @VisibleForTesting
        public static InternalStatus from(SaveStatus status)
        {
            return convert.get(status);
        }

        public static InternalStatus get(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    final RoutingKey key;
    final RedundantBefore.Entry boundsInfo;

    // all transactions, sorted by TxnId
    final TxnInfo[] byId;
    final int minUndecidedById;

    // managed commands that are committed, stable or applied; sorted by executeAt
    // TODO (required): validate that it is always a prefix that is Applied (i.e. never a gap)
    // TODO (desired): filter transactions whose execution we don't manage
    final TxnInfo[] committedByExecuteAt;
    final int maxAppliedWriteByExecuteAt; // applied OR prebootstrap

    // a btree keyed by TxnId we have encountered since pruning that occur before prunedBefore;
    // mapping to those TxnId that had witnessed this potentially-pruned TxnId.
    final Object[] loadingPruned;
    final int prunedBeforeById;

    final Unmanaged[] unmanageds;

    CommandsForKey(RoutingKey key, RedundantBefore.Entry boundsInfo, TxnInfo[] byId, TxnInfo[] committedByExecuteAt, int minUndecidedById, int maxAppliedWriteByExecuteAt, Object[] loadingPruned, int prunedBeforeById, Unmanaged[] unmanageds)
    {
        this.key = key;
        this.boundsInfo = boundsInfo;
        this.byId = byId;
        this.committedByExecuteAt = committedByExecuteAt;
        this.minUndecidedById = minUndecidedById;
        this.maxAppliedWriteByExecuteAt = maxAppliedWriteByExecuteAt;
        this.loadingPruned = loadingPruned;
        this.prunedBeforeById = prunedBeforeById;
        this.unmanageds = unmanageds;
        checkIntegrity();
    }

    CommandsForKey(CommandsForKey copy, Object[] loadingPruned, Unmanaged[] unmanageds)
    {
        this.key = copy.key;
        this.boundsInfo = copy.boundsInfo;
        this.byId = copy.byId;
        this.committedByExecuteAt = copy.committedByExecuteAt;
        this.minUndecidedById = copy.minUndecidedById;
        this.maxAppliedWriteByExecuteAt = copy.maxAppliedWriteByExecuteAt;
        this.loadingPruned = loadingPruned;
        this.prunedBeforeById = copy.prunedBeforeById;
        this.unmanageds = unmanageds;
        checkIntegrity();
    }

    public CommandsForKey(RoutingKey key)
    {
        this.key = key;
        this.boundsInfo = NO_BOUNDS_INFO;
        this.byId = NO_INFOS;
        this.committedByExecuteAt = NO_INFOS;
        this.minUndecidedById = this.maxAppliedWriteByExecuteAt = -1;
        this.loadingPruned = LoadingPruned.empty();
        this.prunedBeforeById = -1;
        this.unmanageds = NO_PENDING_UNMANAGED;
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    public RoutingKey key()
    {
        return key;
    }

    public int size()
    {
        return byId.length;
    }

    public boolean isLoadingPruned()
    {
        return !BTree.isEmpty(loadingPruned);
    }

    @VisibleForImplementation
    public int unmanagedCount()
    {
        return unmanageds.length;
    }

    public int indexOf(TxnId txnId)
    {
        return Arrays.binarySearch(byId, txnId);
    }

    public TxnId txnId(int i)
    {
        return byId[i];
    }

    public TxnInfo get(int i)
    {
        return byId[i];
    }

    @VisibleForImplementation
    public Unmanaged getUnmanaged(int i)
    {
        return unmanageds[i];
    }

    public RedundantBefore.Entry boundsInfo()
    {
        return boundsInfo;
    }

    public TxnInfo get(TxnId txnId)
    {
        int i = indexOf(txnId);
        return i < 0 ? null : byId[i];
    }

    private static TxnInfo get(TxnId txnId, TxnInfo[] txns)
    {
        int i = Arrays.binarySearch(txns, txnId);
        return i < 0 ? null : txns[i];
    }

    // TODO (expected): why do we require prunedBefore to have executeAt == txnId? I don't think this is a correctness requirement, and it is no longer simpler.
    public TxnInfo prunedBefore()
    {
        return prunedBeforeById < 0 ? NO_INFO : byId[prunedBeforeById];
    }

    public TxnId safelyPrunedBefore()
    {
        return safelyPrunedBefore(boundsInfo);
    }

    static TxnId safelyPrunedBefore(RedundantBefore.Entry boundsInfo)
    {
        /*
         * We cannot use locallyDecidedAndAppliedOrInvalidatedBefore to GC because until it has been applied everywhere
         * it cannot safely e substituted for earlier transactions as a dependency.
         *
         * However, it can be safely used as a prune lower bound that we know we do not need to go to disk to load.
         */
        return boundsInfo.locallyDecidedAndAppliedOrInvalidatedBefore;
    }

    public TxnId redundantOrBootstrappedBefore()
    {
        return TxnId.nonNullOrMax(redundantBefore(), boundsInfo.bootstrappedAt);
    }

    public TxnId bootstrappedAt()
    {
        return bootstrappedAt(boundsInfo);
    }

    static TxnId bootstrappedAt(RedundantBefore.Entry boundsInfo)
    {
        TxnId bootstrappedAt = boundsInfo.bootstrappedAt;
        if (bootstrappedAt.compareTo(boundsInfo.gcBefore) <= 0)
            bootstrappedAt = null;
        return bootstrappedAt;
    }

    public TxnId redundantBefore()
    {
        return redundantBefore(boundsInfo);
    }

    static TxnId redundantBefore(RedundantBefore.Entry boundsInfo)
    {
        // TODO (expected): this can be weakened to shardAppliedOrInvalidatedBefore
        return boundsInfo.gcBefore;
    }

    public boolean isPostBootstrapAndOwned(TxnId txnId)
    {
        return isPostBootstrapAndOwned(txnId, boundsInfo);
    }

    private static boolean isPostBootstrapAndOwned(TxnId txnId, RedundantBefore.Entry boundsInfo)
    {
        return txnId.compareTo(boundsInfo.bootstrappedAt) >= 0 && txnId.epoch() >= boundsInfo.startOwnershipEpoch;
    }

    public boolean isPreBootstrap(TxnId txnId)
    {
        return isPreBootstrap(txnId, boundsInfo);
    }

    private static boolean isPreBootstrap(TxnId txnId, RedundantBefore.Entry boundsInfo)
    {
        return txnId.compareTo(boundsInfo.bootstrappedAt) < 0 || txnId.epoch() < boundsInfo.startOwnershipEpoch;
    }

    private TxnId nextWaitingToApply(Kinds kinds, @Nullable Timestamp untilExecuteAt)
    {
        int i = maxAppliedWriteByExecuteAt + 1;
        while (i < committedByExecuteAt.length && (committedByExecuteAt[i].is(APPLIED) || !committedByExecuteAt[i].is(kinds)))
        {
            if (untilExecuteAt != null && committedByExecuteAt[i].executeAt.compareTo(untilExecuteAt) >= 0)
                return null;

            ++i;
        }

        if (i >= committedByExecuteAt.length)
            return null;

        TxnInfo result = committedByExecuteAt[i];
        if (untilExecuteAt != null && result.executeAt.compareTo(untilExecuteAt) >= 0)
            return null;
        return result;
    }

    @VisibleForTesting
    public TxnId nextWaitingToApply()
    {
        return nextWaitingToApply(AnyGloballyVisible, Timestamp.MAX);
    }

    public TxnId blockedOnTxnId(TxnId txnId, @Nullable Timestamp executeAt)
    {
        TxnInfo minUndecided = minUndecided();
        if (minUndecided != null && minUndecided.compareTo(txnId) < 0)
            return minUndecided.plainTxnId();

        Kinds kinds = txnId.witnesses();
        return nextWaitingToApply(kinds, executeAt);
    }

    /**
     * All commands before/after (exclusive of) the given timestamp, excluding those that are redundant,
     * or have locally applied prior to some other command that is stable, will be returned by the collection.
     *
     * Note that if the command has already applied locally, this method may erroneously treat the command as
     * being unwitnessed by some following/dependent command.
     * <p>
     * Note that {@code testDep} applies only to commands that MAY have the command in their deps; if specified any
     * commands that do not know any deps will be ignored, as will any with an executeAt prior to the txnId.
     * <p>
     */
    public <P1, T> T mapReduceFull(TxnId testTxnId,
                                   Kinds testKind,
                                   TestStartedAt testStartedAt,
                                   TestDep testDep,
                                   TestStatus testStatus,
                                   CommandFunction<P1, T, T> map, P1 p1, T initialValue)
    {
        int start, end, loadingIndex = 0;
        // if this is null the TxnId is known in byId
        // otherwise, it must be non-null and represents the transactions (if any) that have requested it be loaded due to being pruned
        TxnId prunedBefore = prunedBefore();
        TxnId[] loadingFor = null;
        {
            int insertPos = Arrays.binarySearch(byId, testTxnId);
            if (insertPos < 0)
            {
                loadingFor = NO_TXNIDS;
                insertPos = -1 - insertPos;
                switch (testDep)
                {
                    default: throw new AssertionError("Unhandled TestDep: " + testDep);
                    case ANY_DEPS:
                        break;

                    case WITH:
                        if (testTxnId.compareTo(prunedBefore) >= 0)
                            return initialValue;

                        loadingFor = loadingPrunedFor(loadingPruned, testTxnId, NO_TXNIDS);
                        break;

                    case WITHOUT:
                        if (testTxnId.compareTo(prunedBefore) < 0)
                            loadingFor = loadingPrunedFor(loadingPruned, testTxnId, NO_TXNIDS);
                }
            }

            switch (testStartedAt)
            {
                default: throw new AssertionError("Unhandled TestStartedAt: " + testTxnId);
                case STARTED_BEFORE: start = 0; end = insertPos; break;
                case STARTED_AFTER: start = insertPos; end = byId.length; break;
                case ANY: start = 0; end = byId.length;
            }
        }

        for (int i = start; i < end ; ++i)
        {
            TxnInfo txn = byId[i];
            if (!txn.is(testKind)) continue;
            if (!txn.is(testStatus)) continue;

            Timestamp executeAt = txn.executeAt;
            if (testDep != ANY_DEPS)
            {
                if (!txn.hasDeps())
                    continue;

                if (executeAt.compareTo(testTxnId) <= 0)
                    continue;

                boolean hasAsDep;
                if (loadingFor == null)
                {
                    TxnId[] missing = txn.missing();
                    hasAsDep = missing == NO_TXNIDS || Arrays.binarySearch(txn.missing(), testTxnId) < 0;
                }
                else if (loadingFor == NO_TXNIDS)
                {
                    hasAsDep = false;
                }
                else
                {
                    // we could use expontentialSearch and moving index for improved algorithmic complexity,
                    // but since should be rarely taken path probably not worth code complexity
                    loadingIndex = SortedArrays.exponentialSearch(loadingFor, loadingIndex, loadingFor.length, txn);
                    if (hasAsDep = (loadingIndex >= 0)) ++loadingIndex;
                    else loadingIndex = -1 - loadingIndex;
                }

                if (hasAsDep != (testDep == WITH))
                    continue;
            }

            initialValue = map.apply(p1, key, txn.plainTxnId(), executeAt, initialValue);
        }
        return initialValue;
    }

    public <P1, T> T mapReduceActive(Timestamp startedBefore,
                                     Kinds testKind,
                                     CommandFunction<P1, T, T> map, P1 p1, T initialValue)
    {
        TxnId prunedBefore = prunedBefore();
        int end = insertPos(startedBefore);
        Timestamp maxCommittedWriteBefore;
        {
            int from = 0, to = committedByExecuteAt.length;
            if (maxAppliedWriteByExecuteAt >= 0)
            {
                if (committedByExecuteAt[maxAppliedWriteByExecuteAt].executeAt.compareTo(startedBefore) <= 0) from = maxAppliedWriteByExecuteAt;
                else to = maxAppliedWriteByExecuteAt;
            }
            int i = SortedArrays.binarySearch(committedByExecuteAt, from, to, startedBefore, (f, v) -> f.compareTo(v.executeAt), FAST);
            if (i < 0) i = -2 - i;
            else --i;
            while (i >= 0 && !committedByExecuteAt[i].is(Write)) --i;
            maxCommittedWriteBefore = i < 0 ? null : committedByExecuteAt[i].executeAt;
        }

        for (int i = 0; i < end ; ++i)
        {
            TxnInfo txn = byId[i];
            if (!txn.is(testKind)) continue;
            if (!txn.isManaged()) continue;

            switch (txn.status())
            {
                case COMMITTED:
                case STABLE:
                case APPLIED:
                    if (!ELIDE_TRANSITIVE_DEPENDENCIES || maxCommittedWriteBefore == null || txn.executeAt.compareTo(maxCommittedWriteBefore) >= 0 || !Write.witnesses(txn))
                        break;
                case TRANSITIVELY_KNOWN:
                case INVALID_OR_TRUNCATED_OR_PRUNED:
                    continue;
            }

            initialValue = map.apply(p1, key, txn.plainTxnId(), txn.executeAt, initialValue);
        }

        if (startedBefore.compareTo(prunedBefore) <= 0)
        {
            // in the event we have pruned transactions that may execute before us, we take the earliest future dependency we can in their place.
            // in practice this only has an effect on ExclusiveSyncPoints because they do not agree an execution time
            // and only take dependencies on TxnId lower than them. Other transactions will propose a timestamp that
            // occurs after any dependencies witnessed here, or will be invalidated, or have already been agreed
            // and their dependencies are known.
            int i = SortedArrays.binarySearch(committedByExecuteAt, 0, maxAppliedWriteByExecuteAt, startedBefore, (f, v) -> f.compareTo(v.executeAt), FAST);
            if (i < 0) i = -1 - i;
            while (i < committedByExecuteAt.length)
            {
                if (committedByExecuteAt[i].epoch() > startedBefore.epoch())
                    break;

                if (committedByExecuteAt[i].is(Write))
                {
                    initialValue = map.apply(p1, key, committedByExecuteAt[i].plainTxnId(), committedByExecuteAt[i].executeAt, initialValue);
                    break;
                }
                ++i;
            }

        }

        return initialValue;
    }

    // NOTE: prev MAY NOT be the version that last updated us due to various possible race conditions
    @VisibleForTesting
    public CommandsForKeyUpdate update(Command next, boolean isOutOfRange)
    {
        Invariants.checkState(manages(next.txnId()));
        InternalStatus newStatus = InternalStatus.from(next.saveStatus());
        if (newStatus == null)
            return this;

        return update(newStatus, next, isOutOfRange, false);
    }

    public CommandsForKeyUpdate update(Command next)
    {
        return update(next, !next.participants().touches(key));
    }

    CommandsForKeyUpdate updatePruned(Command next)
    {
        InternalStatus newStatus = InternalStatus.from(next.saveStatus());
        if (newStatus == null)
            newStatus = TRANSITIVELY_KNOWN;
        if (!manages(next.txnId()))
            newStatus = newStatus.compareTo(InternalStatus.COMMITTED) < 0 ? TRANSITIVELY_KNOWN : INVALID_OR_TRUNCATED_OR_PRUNED;
        return update(newStatus, next, false, true);
    }

    private CommandsForKeyUpdate update(InternalStatus newStatus, Command next, boolean isOutOfRange, boolean wasPruned)
    {
        TxnId txnId = next.txnId();
        Invariants.checkArgument(wasPruned || manages(txnId));

        if (txnId.compareTo(redundantBefore()) < 0)
            return this;

        boolean mayExecute = mayExecute(txnId, newStatus, next);
        if (isOutOfRange && newStatus == INVALID_OR_TRUNCATED_OR_PRUNED) isOutOfRange = false; // invalidated is safe to use anywhere, and erases deps

        TxnId[] loadingAsPrunedFor = loadingPrunedFor(loadingPruned, txnId, null); // we default to null to distinguish between no match, and a match with NO_TXNIDS
        wasPruned |= loadingAsPrunedFor != null;

        int pos = Arrays.binarySearch(byId, txnId);
        CommandsForKeyUpdate result;
        if (pos < 0)
        {
            if (isOutOfRange && loadingAsPrunedFor == null)
                return this; // if outOfRange we only need to maintain any existing records; if none, don't update

            pos = -1 - pos;
            if (isOutOfRange) result = insertOrUpdateOutOfRange(pos, txnId, null, newStatus, mayExecute, wasPruned, loadingAsPrunedFor);
            else if (newStatus.hasExecuteAtOrDeps && !wasPruned) result = insert(pos, txnId, newStatus, mayExecute, next);
            else result = insert(pos, txnId, TxnInfo.create(txnId, newStatus, mayExecute, next), wasPruned, loadingAsPrunedFor);
        }
        else
        {
            // update
            TxnInfo cur = byId[pos];

            if (cur != null)
            {
                int c = newStatus.compareTo(cur.status());
                if (c <= 0)
                {
                    if (c < 0)
                    {
                        if (!(newStatus == PREACCEPTED_OR_ACCEPTED_INVALIDATE && cur.status() == ACCEPTED && next.acceptedOrCommitted().compareTo(cur.ballot()) > 0))
                            return this;
                    }
                    else
                    {
                        if (!newStatus.hasInfo)
                            return this;

                        if (next.acceptedOrCommitted().compareTo(cur.ballot()) <= 0)
                            return this;
                    }
                }
            }

            if (isOutOfRange) result = insertOrUpdateOutOfRange(pos, txnId, cur, newStatus, mayExecute, wasPruned, loadingAsPrunedFor);
            else if (newStatus.hasExecuteAtOrDeps && !wasPruned) result = update(pos, txnId, cur, newStatus, mayExecute, next);
            else result = update(pos, txnId, cur, TxnInfo.create(txnId, newStatus, mayExecute, next), wasPruned, loadingAsPrunedFor);
        }

        return result;
    }

    private CommandsForKeyUpdate insert(int insertPos, TxnId plainTxnId, InternalStatus newStatus, boolean mayExecute, Command command)
    {
        return insertOrUpdate(this, insertPos, -1, plainTxnId, null, newStatus, mayExecute, command);
    }

    private CommandsForKeyUpdate update(int updatePos, TxnId plainTxnId, TxnInfo curInfo, InternalStatus newStatus, boolean mayExecute, Command command)
    {
        return insertOrUpdate(this, updatePos, updatePos, plainTxnId, curInfo, newStatus, mayExecute, command);
    }

    CommandsForKeyUpdate update(int pos, TxnId plainTxnId, TxnInfo curInfo, TxnInfo newInfo, boolean wasPruned, TxnId[] loadingAsPrunedFor)
    {
        return insertOrUpdate(this, pos, plainTxnId, curInfo, newInfo, wasPruned, loadingAsPrunedFor);
    }

    /**
     * Insert a new txnId and info
     */
    CommandsForKeyUpdate insert(int pos, TxnId plainTxnId, TxnInfo newInfo, boolean wasPruned, TxnId[] loadingAsPrunedFor)
    {
        return insertOrUpdate(this, pos, plainTxnId, null, newInfo, wasPruned, loadingAsPrunedFor);
    }

    private CommandsForKeyUpdate insertOrUpdateOutOfRange(int updatePos, TxnId plainTxnId, @Nullable TxnInfo curInfo, InternalStatus newStatus, boolean mayExecute, boolean wasPruned, TxnId[] loadingAsPrunedFor)
    {
        Invariants.checkArgument(!mayExecute);
        TxnInfo baseInfo = curInfo == null ? NO_INFO : curInfo;
        boolean hasDeps = baseInfo.hasDeps();
        boolean depsKnownUntilExecuteAt = baseInfo.depsKnownUntilExecuteAt();
        TxnInfo newInfo = baseInfo.withEncodedStatus(TxnInfo.encode(plainTxnId, newStatus, false, hasDeps, depsKnownUntilExecuteAt));
        return insertOrUpdate(this, updatePos, plainTxnId, curInfo, newInfo, wasPruned, loadingAsPrunedFor);
    }

    // TODO (required): additional linearizability violation detection, based on expectation of presence in missing set

    CommandsForKeyUpdate update(TxnInfo[] newById, int newMinUndecidedById, TxnInfo[] newCommittedByExecuteAt, int newMaxAppliedWriteByExecuteAt, Object[] newLoadingPruned, int newPrunedBeforeById, @Nullable TxnInfo curInfo, @Nonnull TxnInfo newInfo)
    {
        Invariants.checkState(prunedBeforeById < 0 || newById[newPrunedBeforeById].equals(byId[prunedBeforeById]));
        return updateAndNotifyUnmanageds(key, boundsInfo,
                                         newById, newCommittedByExecuteAt, newMinUndecidedById, newMaxAppliedWriteByExecuteAt,
                                         newLoadingPruned, newPrunedBeforeById, unmanageds, curInfo, newInfo);
    }

    static CommandsForKey reconstruct(RoutingKey key, RedundantBefore.Entry boundsInfo, TxnInfo[] byId, TxnId prunedBefore, Unmanaged[] unmanageds)
    {
        int prunedBeforeById = Arrays.binarySearch(byId, prunedBefore);
        Invariants.checkState(prunedBeforeById >= 0 || prunedBefore.equals(TxnId.NONE));
        return reconstruct(key, boundsInfo, byId, BTree.empty(), prunedBeforeById, unmanageds);
    }

    static CommandsForKey reconstruct(RoutingKey key, RedundantBefore.Entry newBoundsInfo, TxnInfo[] byId, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds)
    {
        return reconstruct(key, newBoundsInfo, byId, loadingPruned, newPrunedBeforeById, unmanageds, CommandsForKey::new);
    }

    static <O> O reconstruct(RoutingKey key, RedundantBefore.Entry newBoundsInfo, TxnInfo[] byId, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds, Updater<O> updater)
    {
        int countCommitted = 0;
        int minUndecidedById = -1;
        for (int i = 0; i < byId.length ; ++i)
        {
            TxnInfo txn = byId[i];
            if (txn.status() == INVALID_OR_TRUNCATED_OR_PRUNED) continue;
            if (txn.isCommittedAndExecutes()) ++countCommitted;
            else if (minUndecidedById == -1 && !txn.isCommittedToExecute() && txn.mayExecute())
                minUndecidedById = i;
        }
        TxnInfo[] committedByExecuteAt = new TxnInfo[countCommitted];
        countCommitted = 0;
        for (TxnInfo txn : byId)
        {
            if (txn.isCommittedAndExecutes())
                committedByExecuteAt[countCommitted++] = txn;
        }
        Arrays.sort(committedByExecuteAt, TxnInfo::compareExecuteAt);
        int maxAppliedWriteByExecuteAt = committedByExecuteAt.length;
        while (--maxAppliedWriteByExecuteAt >= 0)
        {
            TxnInfo txn = committedByExecuteAt[maxAppliedWriteByExecuteAt];
            if (txn.kind() == Write && (txn.status() == APPLIED || isPreBootstrap(txn, newBoundsInfo)))
                break;
        }

        return updater.update(key, newBoundsInfo, byId, committedByExecuteAt, minUndecidedById, maxAppliedWriteByExecuteAt, loadingPruned, newPrunedBeforeById, unmanageds);
    }

    static CommandsForKeyUpdate reconstructAndUpdateUnmanaged(RoutingKey key, RedundantBefore.Entry boundsInfo, TxnInfo[] byId, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds)
    {
        return reconstruct(key, boundsInfo, byId, loadingPruned, newPrunedBeforeById, unmanageds, CommandsForKey::updateAndNotifyUnmanageds);
    }

    static CommandsForKeyUpdate updateAndNotifyUnmanageds(RoutingKey key, RedundantBefore.Entry boundsInfo, TxnInfo[] byId, TxnInfo[] committedByExecuteAt, int minUndecidedById, int maxAppliedWriteByExecuteAt, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds)
    {
        return updateAndNotifyUnmanageds(key, boundsInfo, byId, committedByExecuteAt, minUndecidedById, maxAppliedWriteByExecuteAt, loadingPruned, newPrunedBeforeById, unmanageds, null, null);
    }

    static CommandsForKeyUpdate updateAndNotifyUnmanageds(RoutingKey key, RedundantBefore.Entry boundsInfo, TxnInfo[] byId, TxnInfo[] committedByExecuteAt, int minUndecidedById, int maxAppliedWriteByExecuteAt, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds, @Nullable TxnInfo curInfo, @Nullable TxnInfo newInfo)
    {
        NotifyUnmanagedResult notifyUnmanaged = PostProcess.notifyUnmanaged(unmanageds, byId, minUndecidedById, committedByExecuteAt, maxAppliedWriteByExecuteAt, loadingPruned, boundsInfo, curInfo, newInfo);
        if (notifyUnmanaged != null)
            unmanageds = notifyUnmanaged.newUnmanaged;
        CommandsForKey result = new CommandsForKey(key, boundsInfo, byId, committedByExecuteAt, minUndecidedById, maxAppliedWriteByExecuteAt, loadingPruned, newPrunedBeforeById, unmanageds);
        if (notifyUnmanaged == null)
            return result;
        return new CommandsForKeyUpdateWithPostProcess(result, notifyUnmanaged.postProcess);
    }

    CommandsForKey update(Unmanaged[] newUnmanageds)
    {
        return new CommandsForKey(key, boundsInfo, byId, committedByExecuteAt, minUndecidedById, maxAppliedWriteByExecuteAt, loadingPruned, prunedBeforeById, newUnmanageds);
    }

    CommandsForKey update(Object[] newLoadingPruned)
    {
        return new CommandsForKey(key, boundsInfo, byId, committedByExecuteAt, minUndecidedById, maxAppliedWriteByExecuteAt, newLoadingPruned, prunedBeforeById, unmanageds);
    }

    CommandsForKeyUpdate registerUnmanaged(SafeCommand safeCommand, UpdateUnmanagedMode mode)
    {
        Invariants.checkState(mode != UPDATE);
        return Updating.updateUnmanaged(this, safeCommand, mode, null);
    }

    void postProcess(SafeCommandStore safeStore, CommandsForKey prevCfk, @Nullable Command command, NotifySink notifySink)
    {
        TxnInfo minUndecided = minUndecided();
        if (minUndecided != null && !minUndecided.equals(prevCfk.minUndecided()))
            notifySink.waitingOn(safeStore, minUndecided, key, SaveStatus.Stable, HasStableDeps, true);

        if (command == null)
        {
            notifyManaged(safeStore, AnyGloballyVisible, 0, committedByExecuteAt.length, -1, notifySink);
            return;
        }

        if (!command.hasBeen(Status.Committed))
            return;

        /*
         * Make sure transactions are notified they can execute, without repeatedly notifying.
         *
         * Basic logic:
         *  - If there is any uncommitted transaction, notify the minimum txnId that we are expecting it to commit
         *  - If a nextWrite is known, then:
         *    - if it is next to execute we notify it;
         *    - otherwise we try to notify any transactions that execute before it, but none after it
         *  - If no nextWrite is known, then we attempt to notify any transaction as it is decided
         *
         * Note: If we have any uncommitted transactions that were declared before the next decided transaction,
         *       we do not set next or nextWrite, and so do not notify them
         */

        TxnId updatedTxnId = command.txnId();
        TxnInfo newInfo = get(updatedTxnId);
        if (newInfo == null)
            return;

        InternalStatus newStatus = newInfo.status();
        {
            TxnInfo maxAppliedWrite = maxAppliedWrite();
            if (newStatus == STABLE && newInfo.executeAt.compareTo(maxAppliedWrite.executeAt) < 0)
            {
                // We have a read or write that has been made stable before our latest write.
                // This is either a linearizability violation, or it is pre-bootstrap.
                checkBehindCommitForLinearizabilityViolation(newInfo, maxAppliedWrite);
                notifySink.notWaiting(safeStore, updatedTxnId, key);
                return;
            }
        }

        TxnInfo prevInfo = prevCfk.get(updatedTxnId);
        InternalStatus prevStatus = prevInfo == null ? TRANSITIVELY_KNOWN : prevInfo.status();

        int mayExecuteToIndex;
        int mayExecuteAnyAtIndex = -1;
        if (newStatus.compareTo(APPLIED) >= 0)
        {
            mayExecuteToIndex = committedByExecuteAt.length;
        }
        else
        {
            int byExecuteAtIndex = Arrays.binarySearch(committedByExecuteAt, newInfo, TxnInfo::compareExecuteAt);
            if (byExecuteAtIndex >= 0)
            {
                if (newInfo.is(STABLE)) mayExecuteAnyAtIndex = byExecuteAtIndex;
                mayExecuteToIndex = byExecuteAtIndex + 1;
            }
            else
            {
                mayExecuteToIndex = -1 - byExecuteAtIndex;
            }

            if (prevStatus.compareTo(InternalStatus.COMMITTED) >= 0 && newStatus != STABLE)
                return;
        }

        notifyManaged(safeStore, updatedTxnId.witnessedBy(), 0, mayExecuteToIndex, mayExecuteAnyAtIndex, notifySink);
    }

    private void notifyManaged(SafeCommandStore safeStore, Kinds kinds, int mayNotExecuteBeforeIndex, int mayExecuteToIndex, int mayExecuteAny, NotifySink notifySink)
    {
        int undecidedIndex = minUndecidedById < 0 ? byId.length : minUndecidedById;
        long unappliedCounters = 0L;
        TxnId minUndecided = minUndecided();
        if (minUndecided == null)
            minUndecided = bootstrappedAt(boundsInfo); // we don't count txns before this as waiting to execute

        for (int i = maxAppliedWriteByExecuteAt + 1; i < mayExecuteToIndex ; ++i)
        {
            TxnInfo txn = committedByExecuteAt[i];
            if (txn.is(APPLIED))
                continue;

            Kind kind = txn.kind();
            if (txn.mayExecute() && !txn.hasNotifiedReady())
            {
                if (i >= mayNotExecuteBeforeIndex && (kinds.test(kind) || i == mayExecuteAny) && !isWaitingOnPruned(loadingPruned, txn, txn.executeAt))
                {
                    switch (txn.status())
                    {
                        case COMMITTED:
                        {
                            if (txn.hasNotifiedWaiting())
                                break;

                            // cannot execute as dependencies not stable, so notify progress log to get or decide stable deps
                            notifySink.waitingOn(safeStore, txn, key, SaveStatus.Stable, HasStableDeps, true);
                            updateCommittedByExecuteAtInSitu(i, txn.asNotifiedWaiting());
                            break;
                        }

                        case STABLE:
                        {
                            if (txn.hasNotifiedReady())
                                break;

                            if (undecidedIndex < byId.length)
                            {
                                int nextUndecidedIndex = SortedArrays.exponentialSearch(byId, undecidedIndex, byId.length, txn.executeAt, Timestamp::compareTo, FAST);
                                if (nextUndecidedIndex < 0) nextUndecidedIndex = -1 -nextUndecidedIndex;
                                while (undecidedIndex < nextUndecidedIndex)
                                {
                                    TxnInfo backfillTxn = byId[undecidedIndex++];
                                    if (backfillTxn.status().compareTo(InternalStatus.COMMITTED) >= 0 || !mayExecute(backfillTxn)) continue;
                                    unappliedCounters += unappliedCountersDelta(backfillTxn.kind());
                                }
                            }

                            int expectMissingCount = unappliedCount(unappliedCounters, kind);

                            // We remove committed transactions from the missing set, since they no longer need them there
                            // So the missing collection represents only those uncommitted transaction ids that a transaction
                            // witnesses/conflicts with. So we may simply count all of those we know of with a lower TxnId,
                            // and if the count is the same then we are not awaiting any of them for execution and can remove
                            // this command's dependency on this key for execution.
                            TxnId[] missing = txn.missing();
                            int missingCount = missing.length;
                            if (missingCount > 0)
                            {
                                int missingFrom = 0;
                                if (minUndecided != null)
                                {
                                    missingFrom = SortedArrays.binarySearch(missing, 0, missing.length, minUndecided, TxnId::compareTo, FAST);
                                    if (missingFrom < 0) missingFrom = -1 - missingFrom;
                                    missingCount -= missingFrom;
                                }
                                for (int j = missingFrom ; j < missing.length ; ++j)
                                {
                                    if (!managesExecution(missing[j]))
                                        --missingCount;
                                }
                            }
                            if (expectMissingCount == missingCount)
                            {
                                TxnId txnId = txn.plainTxnId();
                                notifySink.notWaiting(safeStore, txnId, key);
                                // TODO (required): avoid invoking this here; we may do redundant work if we have local dependencies we're already waiting on
                                notifySink.waitingOn(safeStore, txn, key, SaveStatus.PreApplied, CanApply, false);
                                updateCommittedByExecuteAtInSitu(i, txn.asNotifiedReady());
                            }
                        }
                    }
                }
            }

            unappliedCounters += unappliedCountersDelta(kind);
            if (kind == Kind.Write)
                return; // the minimum execute index occurs after the next write, so nothing to do yet
        }
    }

    private void updateCommittedByExecuteAtInSitu(int committedIndex, TxnInfo newInfo)
    {
        committedByExecuteAt[committedIndex] = newInfo;
        byId[Arrays.binarySearch(byId, newInfo)] = newInfo;
    }

    private static long unappliedCountersDelta(Kind kind)
    {
        switch (kind)
        {
            default: throw new AssertionError("Unhandled Txn.Kind: " + kind);
            case LocalOnly:
            case EphemeralRead:
                throw illegalState("Invalid Txn.Kind for CommandsForKey: " + kind);

            case ExclusiveSyncPoint:
            case SyncPoint:
                return 0L;

            case Write:
                return (1L << 32) + 1L;

            case Read:
                return 1L;
        }
    }

    private static int unappliedCount(long unappliedCounters, Kind kind)
    {
        switch (kind)
        {
            default: throw new AssertionError("Unhandled Txn.Kind: " + kind);
            case LocalOnly:
            case EphemeralRead:
            case ExclusiveSyncPoint:
                throw illegalState("Invalid Txn.Kind for CommandsForKey: " + kind);

            case SyncPoint:
            case Write:
                return (int)unappliedCounters;

            case Read:
                return (int) (unappliedCounters >>> 32);
        }
    }

    public CommandsForKeyUpdate withRedundantBeforeAtLeast(RedundantBefore.Entry newRedundantBeforeEntry)
    {
        return withRedundantBeforeAtLeast(newRedundantBeforeEntry, false);
    }

    /**
     * Use force=true on load from disk to ensure any notifications that may be needed after an out-of-band truncation are run.
     */
    public CommandsForKeyUpdate withRedundantBeforeAtLeast(RedundantBefore.Entry newBoundsInfo, boolean force)
    {
        // TODO (required): handle receiving an entry from the past, e.g. on reload (OR expunge all CFK on restart)
        if (!force && newBoundsInfo.gcBefore.equals(boundsInfo.gcBefore)
            && newBoundsInfo.bootstrappedAt.equals(boundsInfo.bootstrappedAt)
            && newBoundsInfo.locallyDecidedAndAppliedOrInvalidatedBefore.equals(boundsInfo.locallyDecidedAndAppliedOrInvalidatedBefore)
            && newBoundsInfo.endOwnershipEpoch == boundsInfo.endOwnershipEpoch)
            return this;

        if (newBoundsInfo.gcBefore.epoch() >= newBoundsInfo.endOwnershipEpoch)
        {
            // we should be completely finished; notify every unmanaged and return an empty CFK
            // we special case this to handle the case of future dependencies supplied to us by other CommandsForKey that had pruned their dependencies;
            // we could have an ExclusiveSyncPoint waiting on this command's dependencies to be filled in, which will never happen
            TxnId[] notify = new TxnId[unmanageds.length];
            for (int i = 0 ; i < notify.length ; ++i)
                notify[i] = unmanageds[i].txnId;
            PostProcess newPostProcess = new PostProcess.NotifyNotWaiting(null, notify);
            CommandsForKey newCfk = new CommandsForKey(key, newBoundsInfo, NO_INFOS, NO_INFOS, -1, -1, BTree.empty(), -1, NO_PENDING_UNMANAGED);
            return new CommandsForKeyUpdateWithPostProcess(newCfk, newPostProcess);
        }

        TxnInfo[] newById = pruneById(byId, boundsInfo, newBoundsInfo);
        int newPrunedBeforeById = prunedBeforeId(newById, prunedBefore(), redundantBefore(newBoundsInfo));
        Object[] newLoadingPruned = Pruning.removeRedundantLoadingPruned(loadingPruned, redundantBefore(newBoundsInfo));

        return notifyManagedPreBootstrap(this, newBoundsInfo, reconstructAndUpdateUnmanaged(key, newBoundsInfo, newById, newLoadingPruned, newPrunedBeforeById, unmanageds));
    }

    /**
     * Permits out-of-band truncation of CommandsForKey (i.e. on another thread touching only storage) so that we do not
     * notify any commands. Correctness largely relies on the fact that the full withRedundantBeforeAtLeast will be invoked
     * on load and not no-op due to e.g. newSafelyPrunedBefore or newBootstrappedAt being non-null.
     */
    @VisibleForImplementation
    public CommandsForKey withRedundantBeforeAtLeast(TxnId newRedundantBefore)
    {
        RedundantBefore.Entry newBoundsInfo = boundsInfo.withGcBeforeBeforeAtLeast(newRedundantBefore);

        TxnInfo[] newById = pruneById(byId, boundsInfo, newBoundsInfo);
        int newPrunedBeforeById = prunedBeforeId(newById, prunedBefore(), newRedundantBefore);
        Object[] newLoadingPruned = Pruning.removeRedundantLoadingPruned(loadingPruned, newRedundantBefore);

        return reconstruct(key, newBoundsInfo, newById, newLoadingPruned, newPrunedBeforeById, unmanageds);
    }

    /**
     * Remove transitively redundant applied or invalidated commands
     * @param pruneInterval the number of committed commands we must have prior to the first prune point candidate to trigger a prune attempt
     * @param minHlcDelta do not prune any commands with an HLC within this distance of the prune point candidate
     */
    public CommandsForKey maybePrune(int pruneInterval, long minHlcDelta)
    {
        return Pruning.maybePrune(this, pruneInterval, minHlcDelta);
    }

    public CommandsForKey maximalPrune()
    {
        return Pruning.maybePrune(this, 0, 0);
    }

    int insertPos(Timestamp timestamp)
    {
        return insertPos(byId, timestamp);
    }

    static int insertPos(TxnInfo[] byId, Timestamp timestamp)
    {
        int i = Arrays.binarySearch(byId, 0, byId.length, timestamp);
        if (i < 0) i = -1 -i;
        return i;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return Objects.equals(key, that.key)
               && Arrays.equals(byId, that.byId)
               && Arrays.equals(unmanageds, that.unmanageds);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CommandsForKey cfk()
    {
        return this;
    }

    @Override
    PostProcess postProcess()
    {
        return null;
    }

    public TxnInfo minUndecided()
    {
        return minUndecidedById < 0 ? null : byId[minUndecidedById];
    }

    public TxnId minUndecidedTxnId()
    {
        return minUndecidedById < 0 ? null : byId[minUndecidedById].plainTxnId();
    }

    TxnInfo maxAppliedWrite()
    {
        return maxAppliedWriteByExecuteAt < 0 ? NO_INFO : committedByExecuteAt[maxAppliedWriteByExecuteAt];
    }

    static int maxContiguousManagedAppliedIndex(TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, TxnId bootstrappedAt)
    {
        int i = maxAppliedWriteByExecuteAt + 1;
        while (i < committedByExecuteAt.length)
        {
            TxnInfo txn = committedByExecuteAt[i];
            // TODO (expected): should we count any final run of !managesExecution()? i.e. if we have Y(es)N(o)YNYNNN, should we not stop after only YNYNY?
            if (txn.status() != APPLIED && managesExecution(txn) && (bootstrappedAt == null || bootstrappedAt.compareTo(txn) <= 0))
                break;
            ++i;
        }
        return i - 1;
    }

    static TxnInfo maxContiguousManagedApplied(TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, TxnId bootstrappedAt)
    {
        int i = maxContiguousManagedAppliedIndex(committedByExecuteAt, maxAppliedWriteByExecuteAt, bootstrappedAt);
        return i < 0 ? null : committedByExecuteAt[i];
    }

    /**
     * Treat anything pre bootstrap or redundant as applied. i.e.,
     *
     * max(max(bootstrappedAt, redundantBefore), maxContiguousManagedApplied().executeAt)
     */
    static Timestamp maxContiguousManagedAppliedExecuteAt(TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, TxnId bootstrappedAt, TxnId redundantBefore)
    {
        Timestamp maxBound = TxnId.nonNullOrMax(redundantBefore, bootstrappedAt);
        TxnInfo maxInfo = maxContiguousManagedApplied(committedByExecuteAt, maxAppliedWriteByExecuteAt, bootstrappedAt);
        if (maxInfo == null || maxInfo.executeAt.compareTo(maxBound) < 0)
            return maxBound;
        return maxInfo.executeAt;
    }

    private void checkBehindCommitForLinearizabilityViolation(TxnInfo newInfo, TxnInfo maxAppliedWrite)
    {
        if (newInfo.mayExecute())
        {
            for (int i = maxAppliedWriteByExecuteAt ; i >= 0 ; --i)
            {
                TxnInfo txn = committedByExecuteAt[i];
                if (newInfo == txn && CommandsForKey.reportLinearizabilityViolations())
                {
                    // we haven't found anything pre-bootstrap that follows this command, so log a linearizability violation
                    // TODO (expected): this should be a rate-limited logger; need to integrate with Cassandra
                    logger.error("Linearizability violation on key {}: {} is committed to execute (at {}) before {} that should witness it but has already applied (at {})", key, newInfo.plainTxnId(), newInfo.plainExecuteAt(), maxAppliedWrite.plainTxnId(), maxAppliedWrite.plainExecuteAt());
                    break;
                }

                if (isPreBootstrap(txn))
                    break;
            }
        }
    }

    private void checkIntegrity()
    {
        if (isParanoid())
        {
            Invariants.checkState(byId.length == 0 || byId[0].compareTo(redundantBefore()) >= 0);
            Invariants.checkState(prunedBeforeById == -1 || (prunedBefore().status() == APPLIED && prunedBefore().is(Write)));
            Invariants.checkState(minUndecidedById < 0 || (byId[minUndecidedById].status().compareTo(InternalStatus.COMMITTED) < 0 && mayExecute(byId[minUndecidedById])));

            if (maxAppliedWriteByExecuteAt >= 0)
            {
                Invariants.checkState(committedByExecuteAt[maxAppliedWriteByExecuteAt].kind() == Write);
                Invariants.checkState(committedByExecuteAt[maxAppliedWriteByExecuteAt].status() == APPLIED);
            }

            if (testParanoia(LINEAR, NONE, LOW))
            {
                Invariants.checkArgument(SortedArrays.isSortedUnique(byId));
                Invariants.checkArgument(SortedArrays.isSortedUnique(committedByExecuteAt, TxnInfo::compareExecuteAt));

                for (TxnInfo txn : byId) Invariants.checkState(mayExecute(txn) == txn.mayExecute());
                for (TxnInfo txn : committedByExecuteAt) Invariants.checkState(txn.mayExecute());

                if (minUndecidedById >= 0) for (int i = 0 ; i < minUndecidedById ; ++i) Invariants.checkState(byId[i].status().compareTo(InternalStatus.COMMITTED) >= 0 || !mayExecute(byId[i]) || isPreBootstrap(byId[i]));
                else for (TxnInfo txn : byId) Invariants.checkState(txn.status().compareTo(InternalStatus.COMMITTED) >= 0 || !mayExecute(txn) || isPreBootstrap(txn));

                if (maxAppliedWriteByExecuteAt >= 0)
                {
                    for (int i = maxAppliedWriteByExecuteAt + 1; i < committedByExecuteAt.length ; ++i)
                        Invariants.checkState(committedByExecuteAt[i].kind() != Kind.Write || committedByExecuteAt[i].status().compareTo(APPLIED) < 0);
                }
                else
                {
                    for (TxnInfo txn : committedByExecuteAt)
                        Invariants.checkState(txn.kind() != Kind.Write || txn.status().compareTo(APPLIED) < 0 && mayExecute(txn));
                }
                Invariants.checkState(BTree.size(loadingPruned) == 0 || redundantBefore().compareTo(BTree.findByIndex(loadingPruned, 0)) <= 0);
            }
            if (testParanoia(SUPERLINEAR, NONE, LOW))
            {
                for (TxnInfo txn : committedByExecuteAt)
                {
                    Invariants.checkState(txn == get(txn, byId));
                }
                for (TxnInfo txn : byId)
                {
                    for (TxnId missingId : txn.missing())
                    {
                        Invariants.checkState(txn.kind().witnesses(missingId));
                        TxnInfo missingInfo = get(missingId, byId);
                        Invariants.checkState(missingInfo.status().compareTo(InternalStatus.COMMITTED) < 0);
                        Invariants.checkState(txn.depsKnownBefore().compareTo(missingId) >= 0);
                    }
                    if (txn.isCommittedAndExecutes())
                        Invariants.checkState(txn == committedByExecuteAt[Arrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, txn, TxnInfo::compareExecuteAt)]);
                }
                for (LoadingPruned txn : BTree.<LoadingPruned>iterable(loadingPruned))
                {
                    Invariants.checkState(indexOf(txn) < 0);
                }
                int decidedBefore = minUndecidedById < 0 ? byId.length : minUndecidedById;
                if (!BTree.isEmpty(loadingPruned))
                {
                    int maxDecidedBefore = Arrays.binarySearch(byId, BTree.findByIndex(loadingPruned, 0));
                    if (maxDecidedBefore < 0)
                        maxDecidedBefore = -2 - maxDecidedBefore;
                    if (maxDecidedBefore < decidedBefore)
                        decidedBefore = maxDecidedBefore;
                }
                int appliedBefore = 1 + maxContiguousManagedAppliedIndex(committedByExecuteAt, maxAppliedWriteByExecuteAt, bootstrappedAt());
                if (bootstrappedAt() != null)
                {
                    for (int i = 0 ; i < appliedBefore ; ++i)
                    {
                        if (committedByExecuteAt[i].compareTo(bootstrappedAt()) < 0) continue;
                        if (committedByExecuteAt[i].status() != APPLIED)
                        {
                            appliedBefore = i;
                            break;
                        }
                    }
                }
                for (Unmanaged unmanaged : unmanageds)
                {
                    switch (unmanaged.pending)
                    {
                        case COMMIT:
                        {
                            int byIdIndex = Arrays.binarySearch(byId, unmanaged.waitingUntil);
                            if (byIdIndex < 0)
                                byIdIndex = -1 - byIdIndex;
                            Invariants.checkState(byIdIndex >= decidedBefore || unmanaged.waitingUntil.epoch() >= boundsInfo.endOwnershipEpoch || isAnyPredecessorWaitingOnPruned(loadingPruned, unmanaged.txnId));
                            break;
                        }
                        case APPLY:
                        {
                            int byExecuteAtIndex = SortedArrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, unmanaged.waitingUntil, (f, i) -> f.compareTo(i.executeAt), FAST);
                            Invariants.checkState(byExecuteAtIndex >= 0 && (byExecuteAtIndex >= appliedBefore || unmanaged.waitingUntil.epoch() >= boundsInfo.endOwnershipEpoch));
                            break;
                        }
                    }
                }
            }
        }
    }

    public boolean isEmpty()
    {
        return byId.length == 0 && unmanageds.length == 0;
    }

    public static boolean reportLinearizabilityViolations()
    {
        return reportLinearizabilityViolations;
    }

    public static void enableLinearizabilityViolationsReporting()
    {
        reportLinearizabilityViolations = true;
    }

    public static void disableLinearizabilityViolationsReporting()
    {
        reportLinearizabilityViolations = false;
    }
}
