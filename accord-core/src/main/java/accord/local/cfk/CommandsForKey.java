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
import java.util.Iterator;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.RoutingKey;
import accord.api.ViolationHandler;
import accord.api.ViolationHandler.ViolationHandlerHolder;
import accord.api.VisibleForImplementation;
import accord.local.Command;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.CommandSummaries.ActiveCommandVisitor;
import accord.local.CommandSummaries.SupersedingCommandVisitor;
import accord.local.CommandSummaries.IsDep;
import accord.local.CommandSummaries.SummaryStatus;
import accord.primitives.Deps;
import accord.primitives.Deps.DepRelationList;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.local.cfk.PostProcess.NotifyUnmanagedResult;
import accord.local.cfk.Pruning.LoadingPruned;
import accord.primitives.Ballot;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.btree.BTree;

import static accord.api.ProgressLog.BlockedUntil.CanApply;
import static accord.api.ProgressLog.BlockedUntil.HasStableDeps;
import static accord.api.ProtocolModifiers.dependencyElision;
import static accord.api.ProtocolModifiers.isTransitiveDependencyVisible;
import static accord.api.ProtocolModifiers.shouldVisitMaybePruned;
import static accord.local.CommandSummaries.IsDep.IS_COORD_DEP;
import static accord.local.CommandSummaries.IsDep.IS_PROPOSED_OR_STABLE_DEP;
import static accord.local.CommandSummaries.IsDep.IS_NOT_COORD_DEP;
import static accord.local.CommandSummaries.IsDep.IS_NOT_PROPOSED_OR_STABLE_DEP;
import static accord.local.CommandSummaries.IsDep.NOT_ELIGIBLE;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.CommandSummaries.SummaryStatus.NOT_DIRECTLY_WITNESSED;
import static accord.local.CommandSummaries.SummaryStatus.PREACCEPTED;
import static accord.local.RedundantBefore.*;
import static accord.local.cfk.CommandsForKey.InternalStatus.ACCEPTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.APPLIED_DURABLE;
import static accord.local.cfk.CommandsForKey.InternalStatus.APPLIED_NOT_DURABLE;
import static accord.local.cfk.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.cfk.CommandsForKey.InternalStatus.ERASED;
import static accord.local.cfk.CommandsForKey.InternalStatus.PRUNED;
import static accord.local.cfk.CommandsForKey.InternalStatus.STABLE;
import static accord.local.cfk.CommandsForKey.InternalStatus.TO_SUMMARY_STATUS;
import static accord.local.cfk.CommandsForKey.InternalStatus.INVALIDATED;
import static accord.local.cfk.CommandsForKey.InternalStatus.TRANSITIVE;
import static accord.local.cfk.CommandsForKey.InternalStatus.TRANSITIVE_VISIBLE;
import static accord.local.cfk.CommandsForKey.InternalStatus.from;
import static accord.local.cfk.CommandsForKey.InternalStatus.prunedFrom;
import static accord.local.cfk.PostProcess.notifyManagedUnready;
import static accord.local.cfk.Pruning.find;
import static accord.local.cfk.Pruning.isAnyPredecessorWaitingOnPruned;
import static accord.local.cfk.Pruning.isWaitingOnPruned;
import static accord.local.cfk.Pruning.loadingPrunedFor;
import static accord.local.cfk.Pruning.removeRedundantById;
import static accord.local.cfk.Pruning.prunedBeforeId;
import static accord.local.cfk.UpdateUnmanagedMode.UPDATE;
import static accord.local.cfk.Updating.insertOrUpdate;
import static accord.local.cfk.Updating.maybeUpdateMaxAppliedUnreadyWriteById;
import static accord.primitives.Known.KnownExecuteAt.ApplyAtKnown;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Status.Durability.AllQuorums;
import static accord.primitives.Status.Durability.DurablyCommitted;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Timestamp.Flag.HLC_BOUND;
import static accord.primitives.Timestamp.Flag.UNSTABLE;
import static accord.primitives.Txn.Kind.All;
import static accord.primitives.Txn.Kind.AnyVisible;
import static accord.primitives.Txn.Kind.EphemeralRead;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.primitives.TxnId.FastPath.PrivilegedCoordinatorWithDeps;
import static accord.primitives.TxnId.NO_TXNIDS;
import static accord.utils.Invariants.Paranoia.LINEAR;
import static accord.utils.Invariants.Paranoia.NONE;
import static accord.utils.Invariants.Paranoia.SUPERLINEAR;
import static accord.utils.Invariants.ParanoiaCostFactor.LOW;
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
 * TODO (formalise): tighten semantics around transactions that are not owned by this CFK, which occur in two to three cases:
 *      1) when a dependency calculation must report dependencies imported from a prior epoch.
 *         in this case we do not need to manage dependencies for the transaction, only report the transaction itself.
 *      2) when a transaction proposed in a future epoch visits an earlier epoch, it is registered here for recovery
 *         decisions, so that recovery does not need to contact future epochs to find any superseding transactions
 *      3) when an accept round visits a later epoch than the one in which it is agreed.
 * TODO (desired):  by waiting for a prefix to commit before executing we can in theory have old ids proposed that delay execution.
 * TODO (desired):  track whether a TxnId is a write on this key only for execution (rather than globally)
 * TODO (desired):  save space by encoding InternalStatus in TxnId.flags(), so that when executeAt==txnId we can save 8 bytes per entry
 * TODO (required): better linearizability violation detection
 * TODO (expected): cleanup unmanaged transitively known transactions
 * TODO (desired):  introduce a new status or other fast and simple mechanism for filtering treatment of range or unmanaged transactions
 * TODO (desired):  store missing transactions against the highest known transaction only (this should also permit us to prune better by ignoring the missing collection contents)
 */
public class CommandsForKey extends CommandsForKeyUpdate
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);

    private static boolean reportLinearizabilityViolations = true;

    public static final QuickBounds NO_BOUNDS_INFO = new QuickBounds(0, Long.MAX_VALUE, TxnId.NONE, TxnId.NONE, TxnId.NONE, TxnId.NONE);
    public static final TxnInfo NO_INFO = TxnInfo.create(TxnId.NONE, TRANSITIVE, false, TxnId.NONE, Ballot.ZERO);
    public static final TxnInfo[] NO_INFOS = new TxnInfo[0];
    static final TxnId[] NOT_LOADING_PRUNED = new TxnId[0];
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
        return executes(bounds, txnId, executeAt);
    }

    public boolean mayExecute(TxnId txnId)
    {
        return mayExecute(bounds, txnId);
    }

    public boolean mayExecute(TxnInfo txn)
    {
        return mayExecute(txn, txn.isCommittedToExecute() ? txn.executeAt : null);
    }

    public boolean mayExecute(TxnId txnId, InternalStatus status, Command command)
    {
        return mayExecute(txnId, status.isCommittedToExecute() ? command.executeAt() : null);
    }

    public static boolean mayExecute(QuickBounds bounds, TxnInfo txn)
    {
        return executes(bounds, txn, txn.isCommittedToExecute() ? txn.executeAt : txn);
    }

    public static boolean mayExecute(QuickBounds bounds, TxnId txnId)
    {
        return executes(bounds, txnId, txnId);
    }

    public boolean mayExecute(TxnId txnId, @Nullable Timestamp committedToExecuteAt)
    {
        return executes(bounds, txnId, committedToExecuteAt != null ? committedToExecuteAt : txnId);
    }

    public static boolean executes(QuickBounds bounds, TxnId txnId, Timestamp executeAt)
    {
        return executesIgnoringBootstrap(bounds, txnId, executeAt)
               && bounds.readyAt.compareTo(txnId) < 0;
    }

    public static boolean executesIgnoringBootstrap(QuickBounds bounds, TxnId txnId, Timestamp executeAt)
    {
        return managesExecution(txnId)
               && bounds.endEpoch > executeAt.epoch()
               && bounds.startEpoch <= txnId.epoch(); // if we don't own from txnId.epoch() we will learn of this transaction via bootstrap
    }

    public static boolean needsUpdate(SafeCommandStore safeStore, Command prev, Command updated)
    {
        InternalStatus newStatus = from(safeStore, updated, false);
        if (newStatus == null)
            return updated.known().is(ApplyAtKnown) && updated.executeAt().hasDistinctHlcAndUniqueHlc();

        InternalStatus prevStatus = from(safeStore, prev, false);
        if (prevStatus != newStatus)
            return true;

        if (!newStatus.hasBallot)
            return false;

        Ballot prevAcceptedOrCommitted = prev.acceptedOrCommitted();
        Ballot newAcceptedOrCommitted = updated.acceptedOrCommitted();

        return newAcceptedOrCommitted.compareTo(prevAcceptedOrCommitted) > 0;
    }

    public static class SerializerSupport
    {
        public static CommandsForKey create(RoutingKey key, TxnInfo[] byId, long maxUniqueHlc, Unmanaged[] unmanageds, TxnId prunedBefore, QuickBounds bounds, boolean expectUpToDate)
        {
            return reconstruct(key, bounds, true, byId, maxUniqueHlc, prunedBefore, unmanageds, expectUpToDate);
        }
    }

    interface Updater<O>
    {
        O update(RoutingKey key, QuickBounds bounds, boolean isNewBoundsInfo,
                 TxnInfo[] byId, int minUndecidedManagedById, int minUndecidedById, int maxAppliedUnreadyWriteById,
                 TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt,
                 long maxUniqueHlc, Object[] loadingPruned, int newPrunedBeforeById,
                 Unmanaged[] unmanageds, boolean expectUpToDate);
    }

    /**
     * An object representing the basic CommandsForKey state, extending TxnId to save memory and improve locality.
     */
    public static class TxnInfo extends TxnId
    {
        // TODO (desired): consider saving in TxnId flag bits (plenty of room); can then store just a TxnId in most cases
        static final int INTERNAL_STATUS_ORDINAL_MASK = 0xF;
        static final int INTERNAL_STATUS_ORDINAL_SHIFT = 0;
        static final int MANAGED = 0x10;
        static final int MAY_EXECUTE = 0x20;
        static final int COMMITTED_TO_EXECUTE = 0x40;
        static final int COMMITTED_AND_EXECUTES = MAY_EXECUTE | COMMITTED_TO_EXECUTE;
        static final int INTERNAL_STATUS_FLAGS_SHIFT = 7;
        static final int HAS_DEPS = 0x80; // deps and executeAt (maybe not committed)
        static final int HAS_EXECUTE_AT = 0x100;
        static final int DEPS_KNOWN_UNTIL_EXECUTE_AT = 0x400;
        static final int HAS_BALLOT = 0x800;
        static final int SUMMARY_STATUS_ORDINAL_MASK = 0xF;
        static final int SUMMARY_STATUS_ORDINAL_SHIFT = 12;
        static final int NOTIFIED_READY = 0x10000;
        static final int NOTIFIED_WAITING = 0x20000;
        static final int DURABLY_COMMITTED = 0x40000;
        static final int DURABLY_COMMITTED_SHIFT = Integer.numberOfTrailingZeros(DURABLY_COMMITTED);

        int encodedStatus;
        public final Timestamp executeAt;

        private TxnInfo(TxnId txnId, int encodedStatus, Timestamp executeAt)
        {
            super(txnId);
            Invariants.requireArgument(executeAt == txnId || !executeAt.equals(txnId));
            Invariants.requireArgument(hasOnlyIdentityFlags());
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

        public static TxnInfo createTransitive(@Nonnull TxnId txnId, QuickBounds bounds, @Nonnull TxnId witnessedBy)
        {
            return create(txnId, isTransitiveDependencyVisible(witnessedBy) ? TRANSITIVE_VISIBLE : TRANSITIVE, CommandsForKey.mayExecute(bounds, txnId), txnId, Ballot.ZERO);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, @Nonnull Timestamp executeAt, @Nonnull Ballot ballot)
        {
            return create(txnId, status, mayExecute, 0, executeAt, ballot);
        }
        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, int statusOverrideXor, @Nonnull Timestamp executeAt, @Nonnull Ballot ballot)
        {
            return create(txnId, status, mayExecute, statusOverrideXor, false, executeAt, NO_TXNIDS, ballot);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing, @Nonnull Ballot ballot)
        {
            return create(txnId, status, mayExecute, 0, false, executeAt, missing, ballot);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, boolean mayExecute, int statusOverrideXor, boolean durablyCommitted, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing, @Nonnull Ballot ballot)
        {
            Invariants.require(executeAt == txnId || !executeAt.equals(txnId));
            Invariants.require(status.hasExecuteAt || executeAt == txnId);
            Invariants.require(status.hasDeps || missing == NO_TXNIDS);
            Invariants.expect(status.hasBallot || ballot == Ballot.ZERO);
            int encodedStatus = encode(txnId, status, mayExecute, statusOverrideXor, durablyCommitted);
            if (missing == NO_TXNIDS && (!status.hasBallot || ballot == Ballot.ZERO))
                return new TxnInfo(txnId, encodedStatus, executeAt);
            Invariants.require(missing.length > 0 || missing == NO_TXNIDS);
            return new TxnInfoExtra(txnId, encodedStatus, executeAt, missing, ballot);
        }

        int summaryStatusOrdinal()
        {
            return ((encodedStatus >>> SUMMARY_STATUS_ORDINAL_SHIFT) & SUMMARY_STATUS_ORDINAL_MASK);
        }

        int internalStatusOrdinal()
        {
            return ((encodedStatus >>> INTERNAL_STATUS_ORDINAL_SHIFT) & INTERNAL_STATUS_ORDINAL_MASK);
        }

        public boolean is(SummaryStatus status)
        {
            return summaryStatusOrdinal() == status.ordinal();
        }

        public boolean is(InternalStatus status)
        {
            return internalStatusOrdinal() == status.ordinal();
        }

        boolean isNot(SummaryStatus status)
        {
            return summaryStatusOrdinal() != status.ordinal();
        }

        boolean isNot(InternalStatus status)
        {
            return internalStatusOrdinal() != status.ordinal();
        }

        boolean isAtLeast(SummaryStatus status)
        {
            return summaryStatusOrdinal() >= status.ordinal();
        }

        boolean isAtLeast(InternalStatus status)
        {
            return internalStatusOrdinal() >= status.ordinal();
        }

        int compareTo(SummaryStatus status)
        {
            return summaryStatusOrdinal() - status.ordinal();
        }

        int compareTo(InternalStatus status)
        {
            return internalStatusOrdinal() - status.ordinal();
        }

        // TODO (expected): document the reason for this, and the justification for why it is safe
        //      At least, ensure and explain how we guarantee that we do not break the calculation of the W set from the protocol.
        //      (Since we only compute W on the coordination epoch, and we should never have a statusOverride for that epoch, this should be safe)
        // bit 0 only may be set
        public int statusOverrides()
        {
            int overrides = status().flags ^ statusFlags();
            Invariants.require(overrides <= 1); // only currently permitted to override hasDeps
            return overrides;
        }

        public int statusFlags()
        {
            return (encodedStatus >>> INTERNAL_STATUS_FLAGS_SHIFT) & 0x7;
        }

        public boolean isDurablyCommitted()
        {
            return 0 != (encodedStatus & DURABLY_COMMITTED);
        }

        public int durablyCommittedBit()
        {
            return (encodedStatus >>> DURABLY_COMMITTED_SHIFT) & 1;
        }

        public Durability durability()
        {
            if (is(APPLIED_DURABLE))
                return AllQuorums;

            if (isDurablyCommitted())
                return DurablyCommitted;

            return NotDurable;
        }

        // This field may differ from status().hasDeps(), in the event that the key is out of range for the transaction when committed.
        // this might happen if we are contacted in a future epoch than the one it commits in, or if we are contacted for a vote
        // from a future epoch that has yet to fully take ownership.
        // This is required at least for computing rejected fast path decisions in retired epochs.
        // TODO (testing): formalise and test this more completely, especially wrt interplay with status (e.g. if the deps are from accepted but then treated as committed).
        //
        public boolean hasDeps()
        {
            return 0 != (encodedStatus & HAS_DEPS);
        }

        public boolean hasExecuteAt()
        {
            return 0 != (encodedStatus & HAS_EXECUTE_AT);
        }

        public boolean hasBallot()
        {
            return 0 != (encodedStatus & HAS_BALLOT);
        }

        public boolean isManaged()
        {
            return 0 != (encodedStatus & MANAGED);
        }

        /**
         * Indicates if we may (or will, once committed) execute this command locally.
         * This excludes pre-bootstrap commands, which for consistency includes commands that were initiated in
         * an epoch we don't own, even if the command executes in an epoch we do own. This is because we will have
         * to join the epoch, and any data we receive when joining must include this command.
         */
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
            return InternalStatus.get(internalStatusOrdinal());
        }

        public SummaryStatus summaryStatus()
        {
            return TO_SUMMARY_STATUS[summaryStatusOrdinal()];
        }

        Timestamp depsKnownBefore()
        {
            return depsKnownUntilExecuteAt() ? executeAt : this;
        }

        public TxnInfo withMissing(TxnId[] newMissing)
        {
            Invariants.require(hasDeps());
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
            return super.equals(info)
                   && status() == info.status()
                   && (executeAt == this ? info.executeAt == info : Objects.equals(executeAt, info.executeAt))
                   && isDurablyCommitted() == info.isDurablyCommitted()
                   && TxnId.equalsStrict(missing(), info.missing());
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
                   (isDurablyCommitted() && !is(APPLIED_DURABLE) ? "*" : "") +
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

        public int compareExecuteAt(Timestamp that)
        {
            return this.executeAt.compareTo(that);
        }

        public int compareExecuteAtEpoch(TxnInfo that)
        {
            return Long.compare(this.executeAt.epoch(), that.executeAt.epoch());
        }

        private static int preencode(InternalStatus v, boolean isDurablyCommitted)
        {
            int encoded = v.ordinal();
            encoded |= (v.summaryStatus == null ? SummaryStatus.values().length : v.summaryStatus.ordinal()) << SUMMARY_STATUS_ORDINAL_SHIFT;
            if (v.hasExecuteAt) encoded |= HAS_EXECUTE_AT;
            if (v.hasDeps) encoded |= HAS_DEPS;
            if (v.isCommittedToExecute) encoded |= COMMITTED_TO_EXECUTE;
            if (v.depsKnownUntilExecuteAt()) encoded |= DEPS_KNOWN_UNTIL_EXECUTE_AT;
            if (v.hasBallot) encoded |= HAS_BALLOT;
            if (isDurablyCommitted) encoded |= DURABLY_COMMITTED;
            return encoded;
        }

        static int encode(TxnId txnId, InternalStatus internalStatus, boolean mayExecute)
        {
            int encoded = internalStatus.txnInfoEncoded | (mayExecute ? MAY_EXECUTE : 0);
            if (txnId.is(Key)) encoded |= MANAGED;
            return encoded;
        }

        // statusOverrides is the bitmask we xor, not the new values of the flag (so providing zero has no effect, and providing the base value sets to zero)
        private static int encode(TxnId txnId, InternalStatus internalStatus, boolean mayExecute, int statusOverrideXor, boolean durablyCommitted)
        {
            Invariants.requireArgument(statusOverrideXor <= 1);
            int encoded = internalStatus.txnInfoEncoded | (mayExecute ? MAY_EXECUTE : 0);
            if (txnId.is(Key)) encoded |= MANAGED;
            encoded ^= statusOverrideXor << INTERNAL_STATUS_FLAGS_SHIFT;
            encoded |= (durablyCommitted ? DURABLY_COMMITTED : 0);
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

        void setAppliedDurableInPlace()
        {
            Invariants.require(is(APPLIED_NOT_DURABLE));
            encodedStatus &= ~((SUMMARY_STATUS_ORDINAL_MASK << SUMMARY_STATUS_ORDINAL_SHIFT) | (INTERNAL_STATUS_ORDINAL_MASK << INTERNAL_STATUS_ORDINAL_SHIFT));
            encodedStatus |= APPLIED_DURABLE.ordinal() << INTERNAL_STATUS_ORDINAL_SHIFT;
            encodedStatus |= APPLIED_DURABLE.summaryStatus.ordinal() << SUMMARY_STATUS_ORDINAL_SHIFT;
            encodedStatus |= DURABLY_COMMITTED;
        }

        void setDurablyCommittedInPlace()
        {
            encodedStatus |= DURABLY_COMMITTED;
        }

        void setNotifiedReadyInPlace()
        {
            encodedStatus |= NOTIFIED_READY;
        }

        void setNotifiedWaitingInPlace()
        {
            encodedStatus |= NOTIFIED_WAITING;
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
            TxnId[] missing = this.missing;
            if ((encodedStatus & HAS_DEPS) == 0)
                missing = NO_TXNIDS;
            Ballot ballot = this.ballot;
            if ((encodedStatus & HAS_BALLOT) == 0)
                ballot = Ballot.ZERO;
            if (missing == NO_TXNIDS && ballot == Ballot.ZERO)
                return new TxnInfo(this, encodedStatus, executeAt);
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
            this.waitingUntil = Invariants.requireArgument(waitingUntil, waitingUntil != null);
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
        TRANSITIVE                             (SummaryStatus.NOT_DIRECTLY_WITNESSED, false, false, false, false),
        TRANSITIVE_VISIBLE                     (SummaryStatus.NOT_DIRECTLY_WITNESSED, false, false, false, false),
        PREACCEPTED_WITHOUT_DEPS               (SummaryStatus.PREACCEPTED,            false, false, true,  false),
        PREACCEPTED_WITH_DEPS                  (SummaryStatus.PREACCEPTED,            false, true,  true,  false),
        PREACCEPTED_COORD_NO_FAST_COMMIT       (SummaryStatus.PREACCEPTED,            false, false, true,  false),
        NOTACCEPTED                            (SummaryStatus.NOTACCEPTED,            false, false, true,  false),
        // note that while ACCEPTED does not require executeAt for dependencies, it does require executeAt for computing the earlierWait collection for recovery
        ACCEPTED                               (SummaryStatus.ACCEPTED,               true,  true,  true,  false),
        COMMITTED                              (SummaryStatus.COMMITTED,              true,  true,  true,  true),
        STABLE                                 (SummaryStatus.STABLE,                 true,  true,  false, true),
        // TODO (expected): do not encode missing collection for APPLIED transactions,
        //  as anything they should have witnessed can be treated as supersedingRejected
        //  NOTE: this only applies for transactions we execute. We need a new state for transactions
        //  that have been APPLIED but we don't execute as we want to retain the missing collection there.
        APPLIED_NOT_DURABLE                    (APPLIED,                              true, true, false, true),
        APPLIED_DURABLE                        (APPLIED,                              true, true, false, true, true),
        APPLIED_NOT_EXECUTED /*(reserved)*/    (APPLIED,                              true, true, false, true),
        INVALIDATED                            (SummaryStatus.INVALIDATED,            false,false,false, false),
        ERASED                                 (SummaryStatus.NONE,                   false,false,false, false),
        PRUNED                                 (SummaryStatus.NONE,                   false,false,false, false),
        ;

        static final InternalStatus[] FROM_SAVE_STATUS = new InternalStatus[SaveStatus.values().length];
        static final InternalStatus[] FROM_SAVE_STATUS_FILTERED = new InternalStatus[SaveStatus.values().length];
        static final InternalStatus[] VALUES = values();
        static final SummaryStatus[] TO_SUMMARY_STATUS;

        private static void putFromSaveStatus(SaveStatus from, InternalStatus to)
        {
            putFromSaveStatus(from, to, false);
        }
        
        private static void putFromSaveStatus(SaveStatus from, InternalStatus to, boolean intermediate)
        {
            FROM_SAVE_STATUS[from.ordinal()] = to;
            if (!intermediate)
                FROM_SAVE_STATUS_FILTERED[from.ordinal()] = to;
        }
        
        static
        {
            putFromSaveStatus(SaveStatus.PreAccepted, PREACCEPTED_WITHOUT_DEPS);
            putFromSaveStatus(SaveStatus.PreAcceptedWithVote, PREACCEPTED_WITHOUT_DEPS);
            putFromSaveStatus(SaveStatus.PreAcceptedWithDeps, PREACCEPTED_WITH_DEPS);
            putFromSaveStatus(SaveStatus.AcceptedInvalidate, NOTACCEPTED);
            putFromSaveStatus(SaveStatus.AcceptedInvalidateWithDefinition, NOTACCEPTED);
            putFromSaveStatus(SaveStatus.AcceptedMedium, ACCEPTED);
            putFromSaveStatus(SaveStatus.AcceptedMediumWithDefinition, ACCEPTED);
            putFromSaveStatus(SaveStatus.AcceptedMediumWithDefAndVote, ACCEPTED);
            putFromSaveStatus(SaveStatus.AcceptedSlow, ACCEPTED);
            putFromSaveStatus(SaveStatus.AcceptedSlowWithDefinition, ACCEPTED);
            putFromSaveStatus(SaveStatus.AcceptedSlowWithDefAndVote, ACCEPTED);
            putFromSaveStatus(SaveStatus.PreCommitted, PREACCEPTED_WITHOUT_DEPS);
            putFromSaveStatus(SaveStatus.PreCommittedWithDefinition, PREACCEPTED_WITHOUT_DEPS);
            putFromSaveStatus(SaveStatus.PreCommittedWithDeps, ACCEPTED);
            putFromSaveStatus(SaveStatus.PreCommittedWithFixedDeps, ACCEPTED);
            putFromSaveStatus(SaveStatus.PreCommittedWithDefAndDeps, ACCEPTED);
            putFromSaveStatus(SaveStatus.PreCommittedWithDefAndFixedDeps, ACCEPTED);
            putFromSaveStatus(SaveStatus.Committed, COMMITTED);
            putFromSaveStatus(SaveStatus.Stable, STABLE);
            putFromSaveStatus(SaveStatus.ReadyToExecute, STABLE);
            putFromSaveStatus(SaveStatus.PreApplied, STABLE);
            putFromSaveStatus(SaveStatus.Applying, STABLE);
            putFromSaveStatus(SaveStatus.Applied, APPLIED_NOT_DURABLE);
            putFromSaveStatus(SaveStatus.TruncatedApplyWithOutcome, APPLIED_DURABLE, true);
            putFromSaveStatus(SaveStatus.TruncatedApply, APPLIED_DURABLE, true);
            putFromSaveStatus(SaveStatus.TruncatedUnapplied, APPLIED_DURABLE, true);
            putFromSaveStatus(SaveStatus.Erased, ERASED, true);
            // esp. to support pruning where we expect the prunedBefore entr*ies* to be APPLIED
            // Note importantly that we have multiple logical pruned befores - the last APPLIED
            // write per epoch is retained to cleanly support
            // TODO (desired): can we improve our semantics here to at least PRUNE truncated commands if there's a superseding APPLIED command?
            // TODO (desired): move all truncated to ERASED, but we have to handle the case we erase our prunedBefore boundary
            putFromSaveStatus(SaveStatus.Vestigial, ERASED);
            putFromSaveStatus(SaveStatus.Invalidated, INVALIDATED);
            for (SaveStatus saveStatus : SaveStatus.values())
                Invariants.require(FROM_SAVE_STATUS[saveStatus.ordinal()] != null || saveStatus.is(Status.NotDefined));

            SummaryStatus[] summaryStatuses = SummaryStatus.values();
            TO_SUMMARY_STATUS = Arrays.copyOf(summaryStatuses, summaryStatuses.length + 1);
        }

        public static final int HAS_DEPS = 1;
        public static final int HAS_EXECUTE_AT = 2;

        public final SummaryStatus summaryStatus;
        public final boolean hasExecuteAt;
        public final boolean hasDeps;
        public final boolean hasBallot;
        public final boolean isCommittedToExecute;
        public final int flags;
        final int txnInfoEncoded;

        InternalStatus(SummaryStatus summaryStatus, boolean hasExecuteAt, boolean hasDeps, boolean hasBallot, boolean isCommittedToExecute)
        {
            this(summaryStatus, hasExecuteAt, hasDeps, hasBallot, isCommittedToExecute, false);
        }
        InternalStatus(SummaryStatus summaryStatus, boolean hasExecuteAt, boolean hasDeps, boolean hasBallot, boolean isCommittedToExecute, boolean isDurablyCommitted)
        {
            this.summaryStatus = summaryStatus;
            this.hasBallot = hasBallot;
            this.hasExecuteAt = hasExecuteAt;
            this.hasDeps = hasDeps;
            this.isCommittedToExecute = isCommittedToExecute;
            this.flags = (hasExecuteAt ? HAS_EXECUTE_AT : 0) | (hasDeps ? HAS_DEPS : 0);
            this.txnInfoEncoded = TxnInfo.preencode(this, isDurablyCommitted);
        }

        public boolean hasExecuteAtOrDeps()
        {
            return flags != 0;
        }

        public boolean hasExecuteAt()
        {
            return hasExecuteAt;
        }

        public boolean hasDeps()
        {
            return hasDeps;
        }

        public boolean isCommittedToExecute()
        {
            return isCommittedToExecute;
        }

        public Timestamp depsKnownBefore(TxnId txnId, Timestamp executeAt)
        {
            return depsKnownUntilExecuteAt() ? executeAt : txnId;
        }

        boolean depsKnownUntilExecuteAt()
        {
            return isCommittedToExecute();
        }

        int compareTo(SummaryStatus summaryStatus)
        {
            return this.summaryStatus == null ? 1 : this.summaryStatus.compareTo(summaryStatus);
        }

        @VisibleForTesting
        public static InternalStatus from(SaveStatus status)
        {
            return FROM_SAVE_STATUS[status.ordinal()];
        }

        @VisibleForTesting
        public static InternalStatus prunedFrom(SaveStatus saveStatus)
        {
            InternalStatus result = from(saveStatus);
            if (result == null)
            {
                if (saveStatus.compareTo(SaveStatus.PreAccepted) < 0)
                    return TRANSITIVE_VISIBLE;
                return PRUNED;
            }

            if (result.compareTo(APPLIED) == 0)
                return PRUNED;

            return result;
        }

        public static InternalStatus from(SafeCommandStore safeStore, Command command, boolean includeIntermediate)
        {
            SaveStatus saveStatus = command.saveStatus();
            InternalStatus status = (includeIntermediate ? FROM_SAVE_STATUS : FROM_SAVE_STATUS_FILTERED)[saveStatus.ordinal()];
            if (status == APPLIED_NOT_DURABLE && command.durability().isDurable())
                status = APPLIED_DURABLE;
            if (status == PREACCEPTED_WITH_DEPS && command.txnId().node.equals(safeStore.node().id()) && !Ballot.ZERO.equals(command.promised()))
                status = PREACCEPTED_COORD_NO_FAST_COMMIT;
            return status;
        }

        public static InternalStatus get(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    final RoutingKey key;
    final QuickBounds bounds;

    // all transactions, sorted by TxnId
    final TxnInfo[] byId;
    final int minUndecidedManagedById;
    final int minUndecidedUnmanagedById;
    final int maxAppliedUnreadyWriteById;

    // managed commands that are committed, stable or applied; sorted by executeAt
    // TODO (desired): filter transactions whose execution we don't manage
    final TxnInfo[] committedByExecuteAt;
    final int maxAppliedWriteByExecuteAt; // applied OR unready

    // a btree keyed by TxnId we have encountered since pruning that occur before prunedBefore;
    // mapping to those TxnId that had witnessed this potentially-pruned TxnId.
    // note that this may include TxnId < readyAt
    final Object[] loadingPruned;
    // this points to the primary prunedBefore APPLIED write, but note that if this CFK spans multiple epochs
    // we retain the latest APPLIED write per epoch so that any dependencies we compute that might include
    // a future transaction due to pruning do not include transactions in a future *epoch*
    final int prunedBeforeById;

    final Unmanaged[] unmanageds;
    final long maxUniqueHlc;

    CommandsForKey(RoutingKey key, QuickBounds bounds, boolean isNewBoundsInfo, TxnInfo[] byId, int minUndecidedManagedById, int minUndecidedUnmanagedById, int maxAppliedUnreadyWriteById, TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, long maxUniqueHlc, Object[] loadingPruned, int prunedBeforeById, Unmanaged[] unmanageds, boolean expectUpToDate)
    {
        this(key, bounds, byId, minUndecidedManagedById, minUndecidedUnmanagedById, maxAppliedUnreadyWriteById, committedByExecuteAt, maxAppliedWriteByExecuteAt, maxUniqueHlc, loadingPruned, prunedBeforeById, unmanageds, expectUpToDate);
    }

    CommandsForKey(RoutingKey key, QuickBounds bounds, TxnInfo[] byId, int minUndecidedManagedById, int minUndecidedUnmanagedById, int maxAppliedUnreadyWriteById, TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, long maxUniqueHlc, Object[] loadingPruned, int prunedBeforeById, Unmanaged[] unmanageds, boolean expectUpToDate)
    {
        this.key = key;
        this.bounds = bounds;
        this.byId = byId;
        this.minUndecidedManagedById = minUndecidedManagedById;
        this.minUndecidedUnmanagedById = minUndecidedUnmanagedById;
        this.maxAppliedUnreadyWriteById = maxAppliedUnreadyWriteById;
        this.committedByExecuteAt = committedByExecuteAt;
        this.maxAppliedWriteByExecuteAt = maxAppliedWriteByExecuteAt;
        this.maxUniqueHlc = maxUniqueHlc;
        this.loadingPruned = loadingPruned;
        this.prunedBeforeById = prunedBeforeById;
        this.unmanageds = unmanageds;
        checkIntegrity(expectUpToDate);
    }

    CommandsForKey(CommandsForKey copy, Object[] loadingPruned, Unmanaged[] unmanageds)
    {
        this.key = copy.key;
        this.bounds = copy.bounds;
        this.byId = copy.byId;
        this.minUndecidedManagedById = copy.minUndecidedManagedById;
        this.minUndecidedUnmanagedById = copy.minUndecidedUnmanagedById;
        this.maxAppliedUnreadyWriteById = copy.maxAppliedUnreadyWriteById;
        this.committedByExecuteAt = copy.committedByExecuteAt;
        this.maxAppliedWriteByExecuteAt = copy.maxAppliedWriteByExecuteAt;
        this.maxUniqueHlc = copy.maxUniqueHlc;
        this.loadingPruned = loadingPruned;
        this.prunedBeforeById = copy.prunedBeforeById;
        this.unmanageds = unmanageds;
        checkIntegrity(true);
    }

    public CommandsForKey(RoutingKey key)
    {
        this(key, 0);
    }

    public CommandsForKey(RoutingKey key, long maxUniqueHlc)
    {
        this.key = key;
        this.bounds = NO_BOUNDS_INFO;
        this.byId = NO_INFOS;
        this.committedByExecuteAt = NO_INFOS;
        this.minUndecidedManagedById = this.minUndecidedUnmanagedById = this.maxAppliedUnreadyWriteById = this.maxAppliedWriteByExecuteAt = -1;
        this.maxUniqueHlc = maxUniqueHlc;
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

    public int committedIndexOf(Timestamp executeAt)
    {
        return committedIndexOf(executeAt, 0, committedByExecuteAt.length);
    }

    public int unappliedCommittedIndexOf(Timestamp executeAt)
    {
        return committedIndexOf(executeAt, Math.max(0, maxAppliedWriteByExecuteAt), committedByExecuteAt.length);
    }

    public int committedIndexOf(Timestamp executeAt, int from, int to)
    {
        return SortedArrays.binarySearch(committedByExecuteAt, from, to, executeAt, (f, v) -> f.compareTo(v.executeAt), FAST);
    }

    public TxnId txnId(int i)
    {
        return byId[i];
    }

    public TxnInfo get(int i)
    {
        return byId[i];
    }

    public int committedSize()
    {
        return committedByExecuteAt.length;
    }

    public TxnInfo committedByExecuteAt(int i)
    {
        return committedByExecuteAt[i];
    }

    @VisibleForImplementation
    public Unmanaged getUnmanaged(int i)
    {
        return unmanageds[i];
    }

    public QuickBounds bounds()
    {
        return bounds;
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

    public TxnId redundantOrBootstrappedBefore()
    {
        return TxnId.nonNullOrMax(redundantBefore(), bounds.readyAt);
    }

    public TxnId readyAt()
    {
        return readyAt(bounds);
    }

    static TxnId readyAt(QuickBounds bounds)
    {
        TxnId readyAt = bounds.readyAt;
        if (readyAt.compareTo(bounds.cleanCfkBefore()) <= 0)
            readyAt = null;
        return readyAt;
    }

    public TxnId redundantBefore()
    {
        return redundantBefore(bounds);
    }

    static TxnId redundantBefore(QuickBounds bounds)
    {
        // TODO (expected): this can be weakened to shardAppliedOrInvalidatedBefore
        return bounds.cleanCfkBefore();
    }

    public TxnId appliedBefore()
    {
        return appliedBefore(bounds);
    }

    static TxnId appliedBefore(QuickBounds bounds)
    {
        // TODO (expected): this can be weakened to shardAppliedOrInvalidatedBefore
        return bounds.locallyAppliedBefore;
    }

    public boolean isReadyAndOwned(TxnId txnId)
    {
        return isReadyAndOwned(txnId, bounds);
    }

    private static boolean isReadyAndOwned(TxnId txnId, QuickBounds bounds)
    {
        return txnId.compareTo(bounds.readyAt) >= 0 && txnId.epoch() >= bounds.startEpoch;
    }

    public boolean isUnready(TxnId txnId)
    {
        return isUnready(txnId, bounds);
    }

    static boolean isUnready(TxnId txnId, QuickBounds bounds)
    {
        return txnId.compareTo(bounds.readyAt) < 0 || txnId.epoch() < bounds.startEpoch;
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
        return result.plainTxnId();
    }

    @VisibleForTesting
    public TxnId nextWaitingToApply()
    {
        return nextWaitingToApply(AnyVisible, Timestamp.MAX);
    }

    public TxnId blockedOnTxnId(TxnId txnId, @Nullable Timestamp executeAt)
    {
        TxnInfo minUndecided = minUndecidedManaged();
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
     * commands that do not know any deps will be ignored, as will any whose deps do not cover the txnId
     * (either executeAt is before, or txnId is before and command is not committed so deps do not extend to executeAt)
     * <p>
     */
    public boolean visit(TxnId testTxnId, Kinds testKind, SupersedingCommandVisitor visitor)
    {
        int loadingIndex = 0;
        // if this is null the TxnId is known in byId
        // otherwise, it must be non-null and represents the transactions (if any) that have requested it be loaded due to being pruned
        TxnId prunedBefore = prunedBefore();
        TxnId[] loadingFor = null;
        if (Arrays.binarySearch(byId, testTxnId) < 0)
        {
            loadingFor = NOT_LOADING_PRUNED;
            if (testTxnId.compareTo(prunedBefore) < 0)
                loadingFor = loadingPrunedFor(loadingPruned, testTxnId, NOT_LOADING_PRUNED);
        }

        for (int i = 0; i < byId.length ; ++i)
        {
            TxnInfo txn = byId[i];
            if (!txn.is(testKind)) continue;
            SummaryStatus summaryStatus = txn.summaryStatus();
            if (summaryStatus == null) continue;

            Timestamp executeAt = txn.executeAt;
            IsDep dep;
            if (!txn.hasDeps() || (txn.depsKnownUntilExecuteAt() ? executeAt : txn).compareTo(testTxnId) <= 0)
            {
                dep = NOT_ELIGIBLE;
            }
            else
            {
                boolean hasAsDep;
                if (loadingFor == null)
                {
                    TxnId[] missing = txn.missing();
                    hasAsDep = missing == NO_TXNIDS || Arrays.binarySearch(txn.missing(), testTxnId) < 0;
                }
                else if (loadingFor == NOT_LOADING_PRUNED)
                {
                    hasAsDep = false;
                }
                else
                {
                    // we could use expontentialSearch and moving index for improved algorithmic complexity,
                    // but since should be rarely taken path probably not worth code complexity
                    loadingIndex = SortedArrays.exponentialSearch(loadingFor, loadingIndex, loadingFor.length, txn);
                    if (loadingIndex >= 0)
                    {
                        hasAsDep = !loadingFor[loadingIndex].is(UNSTABLE);
                        ++loadingIndex;
                    }
                    else
                    {
                        hasAsDep = false;
                        loadingIndex = -1 - loadingIndex;
                    }
                }

                if (txn.compareTo(ACCEPTED) >= 0) dep = hasAsDep ? IS_PROPOSED_OR_STABLE_DEP : IS_NOT_PROPOSED_OR_STABLE_DEP;
                else if (txn.is(PrivilegedCoordinatorWithDeps)) dep = hasAsDep ? IS_COORD_DEP : IS_NOT_COORD_DEP;
                else dep = NOT_ELIGIBLE;
            }
            if (!visitor.visit(key, txn.plainTxnId(), executeAt, summaryStatus, dep, txn.durability()))
                return false;
        }

        return true;
    }

    public <P1, P2> void visit(Timestamp startedBefore,
                               Kinds testKind,
                               ActiveCommandVisitor<P1, P2> visitor,
                               P1 p1, P2 p2)
    {
        visitor.visitMaxAppliedHlc(maxUniqueHlc);

        int end = insertPos(startedBefore);
        // TODO (expected): revisit this behaviour to allow us to improve performance by skipping to minUndecidedId when
        //  we have a durablyDecidedId
        // We only filter durable transactions less than BOTH the txnId and executeAt of our max preceding write.
        // This is to avoid the following pre-bootstrap edge case, so this filtering can be made stricter in future:
        // A replica is bootstrapping so that includes all transactions before the bootstrap point, but our latest
        // transaction occurs before the bootstrap by TxnId, and some of its dependencies occur after.
        // Because the replica is pre-bootstrap, it happily participates in an RX including these keys without
        // actually witnessing any of the dependencies that have higher TxnId than the RX. The RX applies and
        // the range is marked as durable. If now the replica then witnesses one of these transactions it will
        // consider that they must be invalidated, as it had not witnessed them, they were not pre-bootstrap and
        // they were pre-RX.
        // NOTE: One approach we could adopt for BOOTSTRAP ONLY would be to treat pre-bootstrap within a period
        //       the replica was participating in consensus differently to the period for which they weren't.
        //       That is, pre-bootstrap-or-stale becomes no-consensus-log, and pre-bootstrap only means no-data
        //       (and really can be represented entirely by safeToRead).
        // HOWEVER: this would not help solve the actual stale+rebootstrap case, though perhaps this could be handled
        //       differently with its own special casing.
        TxnInfo maxCommittedWriteBefore = NO_INFO;
        TxnInfo maxCommittedWriteForEpoch = NO_INFO;
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
            if (i >= 0)
            {
                maxCommittedWriteBefore = committedByExecuteAt[i];
                if (maxCommittedWriteBefore.executeAt == maxCommittedWriteBefore)
                    maxCommittedWriteForEpoch = maxCommittedWriteBefore;
            }
        }

        // TODO (expected): consider whether this is strictly needed - if we've applied a transaction after the pruned
        //  point can we guarantee we have witnessed all earlier TxnId? If no RX has yet run, but the new epoch is
        //  applying transactions then maybe not...? But seems edge casey and may not exist.
        //  Reason this case out more fully, but in the meantime we'll make sure to return them.
        Iterator<LoadingPruned> loadingPruned = null;
        LoadingPruned nextPruned = null;
        if (shouldVisitMaybePruned() && !BTree.isEmpty(this.loadingPruned))
        {
            loadingPruned = BTree.iterator(this.loadingPruned);
            nextPruned = loadingPruned.next();
        }

        for (int i = 0; i < end ; ++i)
        {
            TxnInfo txn = byId[i];
            if (!txn.is(testKind)) continue;
            if (!txn.isManaged()) continue;

            while (nextPruned != null && nextPruned.compareTo(txn) < 0)
            {
                if (nextPruned.isVisible && nextPruned.is(testKind))
                    visitor.visit(p1, p2, NOT_DIRECTLY_WITNESSED, NotDurable, key, nextPruned.plainTxnId());

                if (loadingPruned.hasNext()) nextPruned = loadingPruned.next();
                else nextPruned = null;
            }

            switch (txn.status())
            {
                case TRANSITIVE:
                case INVALIDATED:
                case ERASED:
                case PRUNED:
                    continue;

                case COMMITTED:
                case STABLE:
                case APPLIED_NOT_DURABLE:
                case APPLIED_DURABLE:
                case APPLIED_NOT_EXECUTED:
                    if (txn.compareTo(maxCommittedWriteBefore) >= 0
                        || txn.executeAt.compareTo(maxCommittedWriteBefore.executeAt) >= 0
                        || !Write.witnesses(txn))
                        break;

                    switch (dependencyElision())
                    {
                        case IF_DURABLY_COMMITTED:
                            if (!txn.isDurablyCommitted())
                                break;

                        case ALWAYS:
                            if (testKind != AnyVisible)
                                continue;

                            // cannot be the max known write for the epoch, so no need to return it
                            if (!txn.isWrite())
                                break;

                            // if we witness everything we assume we're coordinating an ExclusiveSyncPoint
                            // which must execute in all epochs, so for ease of processing we return a dependency
                            // from all epochs we know

                            long epoch = txn.epoch();
                            if (epoch != maxCommittedWriteForEpoch.epoch())
                            {
                                maxCommittedWriteForEpoch = NO_INFO;
                                for (int j = i ; j < end ; ++j)
                                {
                                    TxnInfo t = byId[j];
                                    if (t.epoch() != epoch)
                                        break;

                                    if (!t.isWrite() || t.executeAt != t || !t.isCommittedToExecute())
                                        continue;

                                    if (t.compareExecuteAt(maxCommittedWriteForEpoch) > 0)
                                        maxCommittedWriteForEpoch = t;
                                }
                                if (maxCommittedWriteForEpoch == NO_INFO)
                                    maxCommittedWriteForEpoch = txn;
                            }

                            if (txn.compareTo(maxCommittedWriteForEpoch) < 0
                                && txn.executeAt.compareTo(maxCommittedWriteForEpoch.executeAt) < 0)
                                continue;
                    }
            }

            visitor.visit(p1, p2, txn.summaryStatus(), txn.durability(), key, txn.plainTxnId());
        }

        if (end <= prunedBeforeById)
        {
            // In the event we have pruned transactions that may execute before us, we take the earliest future dependency we can in their place.
            // This may occur for any transaction that isn't witnessed by a Write (so, sync points and exclusive sync points).
            // In the former case, this can only happen on recovery, as the original coordinator would propose a later executeAt than any dependency it may witness.

            // Note: we do not use committedByExecuteAt because it might exclude transactions that are locally pre-bootstrap, which might lead to an invalid future transaction being included
            TxnInfo best = byId[prunedBeforeById];
            for (int i = end; i < prunedBeforeById ; ++i)
            {
                TxnInfo txn = byId[i];
                if (!txn.is(Write) || !txn.isCommittedToExecute()) continue;
                if (txn.executeAt.compareTo(best.executeAt) < 0)
                    best = txn;

                if (txn.executeAt == txn || txn.epoch() > startedBefore.epoch())
                    break;
            }
            if (best.executeAt.epoch() <= startedBefore.epoch() && !best.equals(startedBefore))
                visitor.visit(p1, p2, best.summaryStatus(), best.durability(), key, best.plainTxnId());
        }
    }

    public boolean hasUniqueHlcAndIsReadyToExecute(TxnId txnId, Timestamp executeAt, Deps deps)
    {
        if (txnId.isWrite() && executeAt.hlc() <= maxUniqueHlc)
            return false;

        if (txnId.awaitsOnlyDeps())
            executeAt = Timestamp.MAX;

        DepRelationList list = deps.keyDeps.txnIdsWithFlags(key);
        int i = list.size() - 1, j = byId.length - 1;
        i: while (i >= 0)
        {
            TxnId d = list.get(i);
            TxnInfo e;
            while (true)
            {
                if (--j < 0)
                    return d.compareTo(redundantBefore()) < 0;
                e = byId[j];
                int c = e.compareTo(d);
                if (c == 0)
                {
                    if (!e.isAtLeast(APPLIED) && (!e.isAtLeast(COMMITTED) || e.executeAt.compareTo(executeAt) < 0))
                        return false;
                    --i;
                    continue i;
                }
                else if (c > 0)
                {
                    if (!e.isAtLeast(APPLIED) && (!e.isAtLeast(COMMITTED) || e.executeAt.compareTo(executeAt) < 0) && txnId.witnesses(e) && e.isManaged())
                        return false;
                }
                else return d.compareTo(redundantBefore()) < 0;
            }
        }
        return true;
    }

    public CommandsForKeyUpdate callback(SafeCommandStore safeStore, Command update)
    {
        LoadingPruned loading = find(loadingPruned, update.txnId());
        if (maybePruned(update))
            return maybePrunedCallback(safeStore, update, loading);
        return update(safeStore, update, loading);
    }

    public CommandsForKeyUpdate update(SafeCommandStore safeStore, Command update)
    {
        return update(safeStore, update, find(loadingPruned, update.txnId()));
    }

    private CommandsForKeyUpdate update(SafeCommandStore safeStore, Command update, @Nullable LoadingPruned loading)
    {
        Invariants.require(manages(update.txnId()));
        InternalStatus newStatus = from(safeStore, update, true);
        if (newStatus == null)
        {
            if (loading == null)
            {
                // handle replay of TruncatedApply with uniqueHlc
                // note: this is no longer needed because we will replay TruncatedApply entries,
                // but we keep the logic in case we modify that behaviour later
                if (update.known().is(ApplyAtKnown))
                    return updateUniqueHlc(update.executeAt().uniqueHlc());
                return this;
            }
            newStatus = loading.isVisible ? TRANSITIVE_VISIBLE : TRANSITIVE;
        }

        return update(newStatus, update, loading);
    }

    public CommandsForKey updateUniqueHlc(long minUniqueHlc)
    {
        if (maxUniqueHlc >= minUniqueHlc)
            return this;

        if (maxUniqueHlc <= bounds.cleanCfkBefore().hlc() && bounds.cleanCfkBefore().is(HLC_BOUND))
            return this;

        return new CommandsForKey(key, bounds, byId, minUndecidedManagedById, minUndecidedUnmanagedById, maxAppliedUnreadyWriteById, committedByExecuteAt, maxAppliedWriteByExecuteAt, minUniqueHlc, loadingPruned, prunedBeforeById, unmanageds, true);
    }

    public CommandsForKey setDurable(TxnId txnId, Durability durability)
    {
        TxnInfo txn = get(txnId);
        if (txn == null || !durability.isDurablyCommitted())
            return this;

        if (durability.isDurable() && txn.is(APPLIED_NOT_DURABLE)) txn.setAppliedDurableInPlace();
        else if (!txn.isDurablyCommitted()) txn.setDurablyCommittedInPlace();
        else return this;

        // return the exact same data as we have updated in place, but change detection relies on identity
        return new CommandsForKey(key, bounds, byId, minUndecidedManagedById, minUndecidedUnmanagedById, maxAppliedUnreadyWriteById, committedByExecuteAt, maxAppliedWriteByExecuteAt, maxUniqueHlc, loadingPruned, prunedBeforeById, unmanageds, true);
    }

    CommandsForKeyUpdate maybePrunedCallback(SafeCommandStore safeStore, Command update, @Nullable LoadingPruned loading)
    {
        TxnId txnId = update.txnId();
        InternalStatus ifPrunedStatus = prunedFrom(update.saveStatus());
        if (loading == null)
        {
            TxnInfo cur = get(txnId);
            if (cur != null && !cur.is(PRUNED))
                return update(safeStore, update, loading); // if we're not pruned, this should be treated as a regular callback

            if (!manages(txnId))
                ifPrunedStatus = PRUNED;
        }

        return update(ifPrunedStatus, update, loading);
    }

    private boolean maybePruned(Command command)
    {
        TxnId txnId = command.txnId();
        TxnInfo prunedBefore = prunedBefore();
        if (prunedBefore.compareTo(txnId) < 0)
            return false;
        if (!manages(txnId))
            return true;
        return command.hasBeen(Status.Truncated) || (command.hasBeen(Status.Applied) && prunedBefore.executeAt.compareTo(command.executeAt()) > 0);
    }

    private CommandsForKeyUpdate update(InternalStatus newStatus, Command updated, @Nullable LoadingPruned loading)
    {
        TxnId txnId = updated.txnId();
        Invariants.requireArgument(updated.participants().hasTouched(key) || !manages(txnId) || newStatus.compareTo(ERASED) >= 0);

        if (txnId.compareTo(redundantBefore()) < 0)
            return this;

        boolean mayExecute = mayExecute(txnId, newStatus, updated);
        boolean isOutOfRange = !mayExecute && newStatus.compareTo(APPLIED) <= 0 && manages(txnId)
                               && !updated.participants().stillTouches(key);

        TxnId[] witnessedBy = loading != null ? loading.witnessedBy : NOT_LOADING_PRUNED;
        if (loading == null && isOutOfRange && newStatus.compareTo(COMMITTED) < 0)
            return this;

        int pos = Arrays.binarySearch(byId, txnId);
        CommandsForKeyUpdate result;
        if (pos < 0)
        {
            if (isOutOfRange && loading == null)
                return this; // if outOfRange we only need to maintain any existing records; if none, don't update

            pos = -1 - pos;
            if (isOutOfRange) result = insertOrUpdateOutOfRange(pos, txnId, null, newStatus, mayExecute, updated, witnessedBy);
            else if (newStatus.hasDeps()) result = insert(pos, txnId, newStatus, mayExecute, updated, witnessedBy);
            else result = insert(pos, txnId, TxnInfo.create(txnId, newStatus, mayExecute, updated), updated, witnessedBy);
        }
        else
        {
            // update
            TxnInfo cur = byId[pos];
            int c = cur.compareTo(newStatus);
            if (c > 0)
            {
                // newStatus moves us backwards; we only permit this for (Pre)?(Not)?Accepted states
                if (cur.compareTo(COMMITTED) >= 0 || newStatus.compareTo(PREACCEPTED) <= 0)
                    return this;

                // and only when the new ballot is strictly greater
                if (updated.acceptedOrCommitted().compareTo(cur.ballot()) <= 0)
                    return this;
            }
            else if (c == 0)
            {
                // we're updating to the same state; we only do this with a strictly greater ballot;
                // even so, if we have no executeAt or deps there's nothing to record
                if (updated.acceptedOrCommitted().compareTo(cur.ballot()) <= 0 || !newStatus.hasExecuteAtOrDeps())
                    return this;
            }
            else
            {
                // we're advancing to a higher status, but this is only permitted either if the new state is stable or the ballot is higher
                if (cur.compareTo(STABLE) < 0 && updated.acceptedOrCommitted().compareTo(cur.ballot()) < 0)
                    return this;
            }

            if (isOutOfRange) result = insertOrUpdateOutOfRange(pos, txnId, cur, newStatus, mayExecute, updated, witnessedBy);
            else if (cur.compareTo(STABLE) >= 0) result = update(pos, txnId, cur, cur.withEncodedStatus(TxnInfo.encode(txnId, newStatus, cur.mayExecute(), cur.statusOverrides(), cur.isDurablyCommitted())), updated, witnessedBy);
            else if (newStatus.hasDeps()) result = update(pos, txnId, cur, newStatus, mayExecute, updated, witnessedBy);
            else result = update(pos, txnId, cur, TxnInfo.create(txnId, newStatus, mayExecute, updated), updated, witnessedBy);
        }

        return result;
    }

    private CommandsForKeyUpdate insert(int insertPos, TxnId plainTxnId, InternalStatus newStatus, boolean mayExecute, Command command, TxnId[] witnessedBy)
    {
        return insertOrUpdate(this, insertPos, -1, plainTxnId, null, newStatus, mayExecute, command, witnessedBy);
    }

    private CommandsForKeyUpdate update(int updatePos, TxnId plainTxnId, TxnInfo curInfo, InternalStatus newStatus, boolean mayExecute, Command command, TxnId[] witnessedBy)
    {
        return insertOrUpdate(this, updatePos, updatePos, plainTxnId, curInfo, newStatus, mayExecute, command, witnessedBy);
    }

    CommandsForKeyUpdate update(int pos, TxnId plainTxnId, TxnInfo curInfo, TxnInfo newInfo, Command command, TxnId[] witnessedBy)
    {
        return insertOrUpdate(this, pos, plainTxnId, curInfo, newInfo, command, witnessedBy);
    }

    /**
     * Insert a new txnId and info
     */
    CommandsForKeyUpdate insert(int pos, TxnId plainTxnId, TxnInfo newInfo, Command updated, TxnId[] witnessedBy)
    {
        return insertOrUpdate(this, pos, plainTxnId, null, newInfo, updated, witnessedBy);
    }

    private CommandsForKeyUpdate insertOrUpdateOutOfRange(int updatePos, TxnId plainTxnId, @Nullable TxnInfo curInfo, InternalStatus newStatus, boolean mayExecute, Command updated, TxnId[] witnessedBy)
    {
        Invariants.requireArgument(!mayExecute);
        int statusOverridesXor = newStatus.flags & 1;
        TxnInfo newInfo;
        if (curInfo == null) newInfo = TxnInfo.create(plainTxnId, newStatus, false, statusOverridesXor, plainTxnId, newStatus.hasBallot ? updated.acceptedOrCommitted() : Ballot.ZERO);
        else newInfo = curInfo.withEncodedStatus(TxnInfo.encode(plainTxnId, newStatus, false, statusOverridesXor, updated.durability().isDurablyCommitted()));
        // out of range means we have no deps, we're just marking committed, so we set HAS_DEPS to 0
        return insertOrUpdate(this, updatePos, plainTxnId, curInfo, newInfo, updated, witnessedBy);
    }

    // TODO (required): additional linearizability violation detection, based on expectation of presence in missing set

    CommandsForKeyUpdate update(TxnInfo[] newById, int newMinUndecidedManagedById, int newMinUndecidedById, int newMaxAppliedUnreadyWriteById, TxnInfo[] newCommittedByExecuteAt, int newMaxAppliedWriteByExecuteAt, long maxUniqueHlc, Object[] newLoadingPruned, int newPrunedBeforeById, @Nullable TxnInfo curInfo, @Nonnull TxnInfo newInfo)
    {
        Invariants.require(prunedBeforeById < 0 || newById[newPrunedBeforeById].equals(byId[prunedBeforeById]));
        return updateAndNotifyUnmanageds(key, bounds, false,
                                         newById, newMinUndecidedManagedById, newMinUndecidedById, newMaxAppliedUnreadyWriteById,
                                         newCommittedByExecuteAt, newMaxAppliedWriteByExecuteAt,
                                         maxUniqueHlc, newLoadingPruned, newPrunedBeforeById,
                                         unmanageds, true, curInfo, newInfo);
    }

    static CommandsForKey reconstruct(RoutingKey key, QuickBounds bounds, boolean isNewBoundsInfo, TxnInfo[] byId, long maxUniqueHlc, TxnId prunedBefore, Unmanaged[] unmanageds, boolean expectUpToDate)
    {
        int prunedBeforeById = Arrays.binarySearch(byId, prunedBefore);
        Invariants.require(prunedBeforeById >= 0 || prunedBefore.equals(TxnId.NONE));
        return reconstruct(key, bounds, isNewBoundsInfo, byId, maxUniqueHlc, BTree.empty(), prunedBeforeById, unmanageds, expectUpToDate);
    }

    static CommandsForKey reconstruct(RoutingKey key, QuickBounds bounds, boolean isNewBoundsInfo, TxnInfo[] byId, long maxUniqueHlc, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds, boolean expectUpToDate)
    {
        return reconstruct(key, bounds, isNewBoundsInfo, byId, maxUniqueHlc, loadingPruned, newPrunedBeforeById, unmanageds, expectUpToDate, CommandsForKey::new);
    }

    static <O> O reconstruct(RoutingKey key, QuickBounds bounds, boolean isNewBoundsInfo, TxnInfo[] byId, long maxUniqueHlc, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds, boolean expectUpToDate, Updater<O> updater)
    {
        int countCommitted = 0;
        int minUndecidedManagedById = -1;
        int minUndecidedById = -1;
        int maxAppliedUnreadyWriteById = -1;
        for (int i = 0; i < byId.length ; ++i)
        {
            TxnInfo txn = byId[i];
            if (txn.isAtLeast(INVALIDATED)) continue;
            if (txn.mayExecute())
            {
                if (txn.isCommittedAndExecutes()) ++countCommitted;
                else if (minUndecidedManagedById == -1 && !txn.isCommittedToExecute())
                    minUndecidedManagedById = i;
            }
            else
            {
                maxAppliedUnreadyWriteById = maybeUpdateMaxAppliedUnreadyWriteById(bounds, byId, maxAppliedUnreadyWriteById, i);
                if (minUndecidedById == -1 && txn.compareTo(COMMITTED) < 0)
                    minUndecidedById = i;
            }
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
            if (txn.is(Write) && (txn.is(APPLIED) || isUnready(txn, bounds)))
                break;
        }
        if (maxAppliedWriteByExecuteAt >= 0) maxUniqueHlc = Math.max(maxUniqueHlc, committedByExecuteAt[maxAppliedWriteByExecuteAt].executeAt.hlc());
        else maxUniqueHlc = Math.max(maxUniqueHlc, bounds.cleanCfkBefore().hlc() - 1); // only guaranteed to witness those with strictly lower hlc; might be some txn agreed with higher flag bits

        return updater.update(key, bounds, isNewBoundsInfo, byId, minUndecidedManagedById, minUndecidedById, maxAppliedUnreadyWriteById, committedByExecuteAt, maxAppliedWriteByExecuteAt, maxUniqueHlc, loadingPruned, newPrunedBeforeById, unmanageds, expectUpToDate);
    }

    static CommandsForKeyUpdate reconstructAndUpdateUnmanaged(RoutingKey key, QuickBounds bounds, boolean isNewBoundsInfo, TxnInfo[] byId, long maxUniqueHlc, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds, boolean expectUpToDate)
    {
        return reconstruct(key, bounds, isNewBoundsInfo, byId, maxUniqueHlc, loadingPruned, newPrunedBeforeById, unmanageds, expectUpToDate, CommandsForKey::updateAndNotifyUnmanageds);
    }

    static CommandsForKeyUpdate updateAndNotifyUnmanageds(RoutingKey key, QuickBounds bounds, boolean isNewBoundsInfo, TxnInfo[] byId, int minUndecidedManagedById, int minUndecidedById, int maxAppliedUnreadyWriteById, TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, long maxUniqueHlc, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds, boolean expectUpToDate)
    {
        return updateAndNotifyUnmanageds(key, bounds, isNewBoundsInfo, byId, minUndecidedManagedById, minUndecidedById, maxAppliedUnreadyWriteById, committedByExecuteAt, maxAppliedWriteByExecuteAt, maxUniqueHlc, loadingPruned, newPrunedBeforeById, unmanageds, expectUpToDate, null, null);
    }

    static CommandsForKeyUpdate updateAndNotifyUnmanageds(RoutingKey key, QuickBounds bounds, boolean isNewBoundsInfo, TxnInfo[] byId, int minUndecidedManagedById, int minUndecidedById, int maxAppliedUnreadyWriteById, TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, long maxUniqueHlc, Object[] loadingPruned, int newPrunedBeforeById, Unmanaged[] unmanageds, boolean expectUpToDate, @Nullable TxnInfo curInfo, @Nullable TxnInfo newInfo)
    {
        NotifyUnmanagedResult notifyUnmanaged = PostProcess.notifyUnmanaged(unmanageds, byId, minUndecidedManagedById, committedByExecuteAt, maxAppliedWriteByExecuteAt, loadingPruned, bounds, isNewBoundsInfo, curInfo, newInfo);
        if (notifyUnmanaged != null)
            unmanageds = notifyUnmanaged.newUnmanaged;
        CommandsForKey newCfk = new CommandsForKey(key, bounds, byId, minUndecidedManagedById, minUndecidedById, maxAppliedUnreadyWriteById, committedByExecuteAt, maxAppliedWriteByExecuteAt, maxUniqueHlc, loadingPruned, newPrunedBeforeById, unmanageds, expectUpToDate);
        CommandsForKeyUpdate result = newCfk;
        if (notifyUnmanaged != null)
            result = new CommandsForKeyUpdateWithPostProcess(newCfk, notifyUnmanaged.postProcess);
        if (newInfo != null && newInfo.is(STABLE) && !newInfo.mayExecute() && managesExecution(newInfo))
            result = new CommandsForKeyUpdateWithPostProcess(newCfk, new PostProcess.NotifyNotWaiting(result.postProcess(), new TxnId[] { newInfo.plainTxnId() }));
        return result;
    }

    CommandsForKey update(Unmanaged[] newUnmanageds)
    {
        return new CommandsForKey(key, bounds, byId, minUndecidedManagedById, minUndecidedUnmanagedById, maxAppliedUnreadyWriteById, committedByExecuteAt, maxAppliedWriteByExecuteAt, maxUniqueHlc, loadingPruned, prunedBeforeById, newUnmanageds, true);
    }

    CommandsForKey update(Object[] newLoadingPruned)
    {
        return new CommandsForKey(key, bounds, byId, minUndecidedManagedById, minUndecidedUnmanagedById, maxAppliedUnreadyWriteById, committedByExecuteAt, maxAppliedWriteByExecuteAt, maxUniqueHlc, newLoadingPruned, prunedBeforeById, unmanageds, true);
    }

    @VisibleForTesting
    public CommandsForKeyUpdate registerUnmanaged(SafeCommandStore safeStore, SafeCommand safeCommand, UpdateUnmanagedMode mode)
    {
        Invariants.require(mode != UPDATE);
        return Updating.updateUnmanaged(this, safeStore, safeCommand, mode, null);
    }

    void postProcess(SafeCommandStore safeStore, CommandsForKey prevCfk, @Nullable Command updated, NotifySink notifySink, boolean forceNotify)
    {
        postProcessInternal(safeStore, prevCfk, updated, notifySink, forceNotify);
        TxnInfo minUndecided = minUndecidedManaged();
        if (minUndecided != null && (forceNotify || !minUndecided.equals(prevCfk.minUndecidedManaged())))
            notifySink.waitingOn(safeStore, minUndecided, key, SaveStatus.Stable, HasStableDeps, true);
    }
    private void postProcessInternal(SafeCommandStore safeStore, CommandsForKey prevCfk, @Nullable Command updated, NotifySink notifySink, boolean forceNotify)
    {
        if (updated == null || forceNotify)
        {
            notifyManaged(safeStore, AnyVisible, 0, committedByExecuteAt.length, -1, notifySink);
            return;
        }

        if (!updated.hasBeen(Status.Committed))
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

        TxnId updatedTxnId = updated.txnId();
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
                notifySink.notWaiting(safeStore, updatedTxnId, key, 0);
                return;
            }
        }

        TxnInfo prevInfo = prevCfk.get(updatedTxnId);
        InternalStatus prevStatus = prevInfo == null ? TRANSITIVE : prevInfo.status();

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

        notifyManaged(safeStore, All, 0, mayExecuteToIndex, mayExecuteAnyAtIndex, notifySink);
        if (Invariants.isParanoid() && safeStore.get(key).current() == this)
        {
            int unappliedWrites = 0, unappliedReads = 0;
            // we limit ourselves to >= maxAppliedWrite to handle the replay case where we may backfill in any order
            // already applied transactions and find their predecessors are not notified (because we only notify from maxAppliedWriteByExecuteAt onwards)
            // TODO (expected): consider modifying notification logic instead, so we can have more robust validation
            for (int i = Math.max(maxAppliedWriteByExecuteAt, 0) ; i < committedByExecuteAt.length ; i++)
            {
                TxnInfo txn = committedByExecuteAt[i];
                int j = indexOf(txn);
                Invariants.require(j >= 0 && txn == byId[j]);
                if (txn.compareTo(APPLIED) >= 0)
                    continue;

                if (txn.is(STABLE) && !txn.hasNotifiedReady())
                {
                    int m = Arrays.binarySearch(byId, txn.executeAt);
                    if (m < 0) m = -1 - m;
                    if ((minUndecidedManagedById < 0 || m < minUndecidedManagedById) && !isWaitingOnPruned(loadingPruned, txn, txn.executeAt, bounds))
                    {
                        int missingCount = 0;
                        for (TxnId missing : txn.missing())
                        {
                            if (!missing.is(UNSTABLE) && managesExecution(missing) && !isUnready(missing))
                                ++missingCount;
                        }
                        if (unappliedWrites + (txn.is(Write) ? unappliedReads : 0) == missingCount)
                        {
                            if (updated.txnId().equals(txn) && !updated.waitingOn().isWaitingOnKey(key))
                                txn.setNotifiedReadyInPlace();
                            else
                                Invariants.require(false);
                        }
                    }
                }
                if (txn.is(Write)) ++unappliedWrites;
                else if (txn.is(Read)) ++unappliedReads;
            }
        }
    }

    private void notifyManaged(SafeCommandStore safeStore, Kinds kinds, int mayNotExecuteBeforeIndex, int mayExecuteToIndex, int mayExecuteAny, NotifySink notifySink)
    {
        int undecidedIndex = minUndecidedManagedById < 0 ? byId.length : minUndecidedManagedById;
        long unappliedCounters = 0L;
        TxnId minUndecided = minUndecidedManaged();
        if (minUndecided == null)
            minUndecided = readyAt(bounds); // we don't count txns before this as waiting to execute

        for (int i = maxAppliedWriteByExecuteAt + 1; i < mayExecuteToIndex ; ++i)
        {
            TxnInfo txn = committedByExecuteAt[i];
            if (txn.is(APPLIED))
                continue;

            if (!txn.hasNotifiedReady())
            {
                if (i >= mayNotExecuteBeforeIndex && (kinds.test(txn) || i == mayExecuteAny))
                {
                    switch (txn.status())
                    {
                        case COMMITTED:
                        {
                            if (txn.hasNotifiedWaiting() || isWaitingOnPruned(loadingPruned, txn, txn.executeAt, bounds))
                                break;

                            // cannot execute as dependencies not stable, so notify progress log to get or decide stable deps
                            notifySink.waitingOn(safeStore, txn, key, SaveStatus.Stable, HasStableDeps, true);
                            txn.setNotifiedWaitingInPlace();
                            break;
                        }

                        case STABLE:
                        {
                            if (isWaitingOnPruned(loadingPruned, txn, txn.executeAt, bounds))
                                break;

                            if (undecidedIndex < byId.length)
                            {
                                int nextUndecidedIndex = SortedArrays.exponentialSearch(byId, undecidedIndex, byId.length, txn.executeAt, Timestamp::compareTo, FAST);
                                if (nextUndecidedIndex < 0) nextUndecidedIndex = -1 -nextUndecidedIndex;
                                while (undecidedIndex < nextUndecidedIndex)
                                {
                                    TxnInfo backfillTxn = byId[undecidedIndex++];
                                    if (backfillTxn.compareTo(InternalStatus.COMMITTED) >= 0 || !mayExecute(backfillTxn)) continue;
                                    unappliedCounters += unappliedCountersDelta(backfillTxn.kindOrdinal());
                                }
                            }

                            int expectMissingCount = unappliedCount(unappliedCounters, txn.kindOrdinal());

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
                                    if (!managesExecution(missing[j]) || missing[j].is(UNSTABLE))
                                        --missingCount;
                                }
                            }
                            if (expectMissingCount == missingCount)
                            {
                                TxnId txnId = txn.plainTxnId();
                                long minUniqueHlc = 0;
                                if (txnId.isWrite() && (maxAppliedUnreadyWriteById < 0 || byId[maxAppliedUnreadyWriteById].executeAt.compareTo(txn.executeAt) < 0))
                                    minUniqueHlc = maxUniqueHlc + 1;
                                notifySink.notWaiting(safeStore, txnId, key, minUniqueHlc);
                                // TODO (expected): avoid invoking this here; we may do redundant work if we have local dependencies we're already waiting on
                                notifySink.waitingOn(safeStore, txn, key, SaveStatus.PreApplied, CanApply, false);
                                txn.setNotifiedReadyInPlace();
                            }
                        }
                    }
                }
            }

            unappliedCounters += unappliedCountersDelta(txn.kindOrdinal());
            if (txn.is(Kind.Write))
                return; // the minimum execute index occurs after the next write, so nothing to do yet
        }
    }

    static final long DELTA_MASK = 0x3;
    static final long HIGH_BIT = 1L << 32;

    static
    {
        // check if we need to update unappliedCount / unappliedCountersDelta
        Invariants.require(Kind.values().length == 5);
        Invariants.require(Write.ordinal() == 1);
        Invariants.require((Read.ordinal() & 1) == 0);
        Invariants.require((EphemeralRead.ordinal() & 1) == 0);
        Invariants.require(unappliedCountersDelta(EphemeralRead.ordinal()) == 0);
    }
    private static long unappliedCountersDelta(int kindOrdinal)
    {
        // lowBit -> reads and writes
        long lowBit = (DELTA_MASK >>> kindOrdinal) & 1;
        // highBit -> writes only
        long highBit = HIGH_BIT & ((kindOrdinal ^ 1) - 1);
        return lowBit | highBit;
    }

    private static int unappliedCount(long unappliedCounters, int kindOrdinal)
    {
        Invariants.require(kindOrdinal <= 2);
        return (int)(unappliedCounters >>> ((~kindOrdinal & 1) << 5));
    }

    public CommandsForKeyUpdate withBoundsAtLeast(QuickBounds withBounds, boolean expectUpToDate)
    {
        Invariants.expect(withBounds.cleanCfkBefore().compareTo(bounds.cleanCfkBefore()) >= 0, "cleanCfkBefore %s may have moved backwards from %s for key %s", withBounds.cleanCfkBefore(), bounds, key);

        QuickBounds newBounds = bounds.withEpochs(bounds.startEpoch, withBounds.endEpoch)
                                      .withCleanCfkBeforeAtLeast(withBounds.cleanCfkBefore())
                                      .withReadyAtLeast(withBounds.readyAt)
                                      .withLocallyAppliedAtLeast(withBounds.locallyAppliedBefore);

        if (newBounds == bounds)
            return this;

        if (newBounds.cleanCfkBefore().epoch() >= newBounds.endEpoch)
        {
            // we should be completely finished; notify every unmanaged and return an empty CFK
            // we special case this to handle the case of future dependencies supplied to us by other CommandsForKey that had pruned their dependencies;
            // we could have an ExclusiveSyncPoint waiting on this command's dependencies to be filled in, which will never happen
            TxnId[] notify = new TxnId[unmanageds.length];
            for (int i = 0 ; i < notify.length ; ++i)
                notify[i] = unmanageds[i].txnId;
            PostProcess newPostProcess = new PostProcess.NotifyNotWaiting(null, notify);
            CommandsForKey newCfk = new CommandsForKey(key, newBounds, NO_INFOS, -1, -1, -1, NO_INFOS, -1, maxUniqueHlc, BTree.empty(), -1, NO_PENDING_UNMANAGED, expectUpToDate);
            return new CommandsForKeyUpdateWithPostProcess(newCfk, newPostProcess);
        }

        Object[] newLoadingPruned = Pruning.removeRedundantLoadingPruned(loadingPruned, redundantBefore(newBounds));
        TxnInfo[] newById = removeRedundantById(byId, newLoadingPruned != loadingPruned, bounds, newBounds, expectUpToDate);
        int newPrunedBeforeById = prunedBeforeId(newById, prunedBefore(), redundantBefore(newBounds));
        Invariants.paranoid(!expectUpToDate || (newPrunedBeforeById < 0 ? prunedBeforeById < 0 || byId[prunedBeforeById].compareTo(newBounds.cleanCfkBefore()) < 0 : newById[newPrunedBeforeById].equals(byId[prunedBeforeById])));

        return notifyManagedUnready(this, newBounds, reconstructAndUpdateUnmanaged(key, newBounds, true, newById, maxUniqueHlc, newLoadingPruned, newPrunedBeforeById, unmanageds, expectUpToDate));
    }

    /**
     * Permits out-of-band truncation of CommandsForKey (i.e. on another thread touching only storage) so that we do not
     * notify any commands. Correctness largely relies on the fact that the full withRedundantBeforeAtLeast will be invoked
     * on load and not no-op due to e.g. newSafelyPrunedBefore or newReadyAt being non-null.
     */
    @VisibleForImplementation
    public CommandsForKey withCleanCfkBeforeAtLeast(TxnId newCleanCfkBefore, boolean expectUpToDate)
    {
        QuickBounds newBounds = bounds.withCleanCfkBeforeAtLeast(newCleanCfkBefore);
        if (newBounds == bounds)
        {
            Invariants.expect(newCleanCfkBefore.compareTo(bounds.cleanCfkBefore()) >= 0, "gcBefore %s may have moved backwards from %s for key %s", newCleanCfkBefore, bounds, key);
            return this;
        }

        Object[] newLoadingPruned = Pruning.removeRedundantLoadingPruned(loadingPruned, newCleanCfkBefore);
        TxnInfo[] newById = removeRedundantById(byId, newLoadingPruned != loadingPruned, bounds, newBounds, expectUpToDate);
        int newPrunedBeforeById = prunedBeforeId(newById, prunedBefore(), newCleanCfkBefore);
        Invariants.paranoid(!expectUpToDate || (newPrunedBeforeById < 0 ? prunedBeforeById < 0 || byId[prunedBeforeById].compareTo(newCleanCfkBefore) < 0 : newById[newPrunedBeforeById].equals(byId[prunedBeforeById])));

        return reconstruct(key, newBounds, true, newById, maxUniqueHlc, newLoadingPruned, newPrunedBeforeById, unmanageds, expectUpToDate);
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
        return Pruning.maximalPrune(this);
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
        return Objects.equals(key, that.key) && equalContents(that);
    }

    /**
     * A quick check to see if anything we need to save may have been changed;
     * ignores in-place updates that would not be serialized, and changes to QuickBounds
     */
    public boolean hasChanges(CommandsForKey cfk)
    {
        return byId != cfk.byId || unmanageds != cfk.unmanageds;
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

    public TxnInfo minUndecidedManaged()
    {
        return minUndecidedManagedById < 0 ? null : byId[minUndecidedManagedById];
    }

    public TxnInfo minUndecidedUnmanaged()
    {
        return minUndecidedUnmanagedById < 0 ? null : byId[minUndecidedUnmanagedById];
    }

    public TxnId minUndecidedManagedTxnId()
    {
        return minUndecidedManagedById < 0 ? null : byId[minUndecidedManagedById].plainTxnId();
    }

    final int minUndecidedById()
    {
        return minUndecidedManagedById <= minUndecidedUnmanagedById
               ? minUndecidedManagedById   >= 0 ? minUndecidedManagedById   : minUndecidedUnmanagedById
               : minUndecidedUnmanagedById >= 0 ? minUndecidedUnmanagedById : minUndecidedManagedById;
    }

    TxnInfo maxAppliedWrite()
    {
        return maxAppliedWriteByExecuteAt < 0 ? NO_INFO : committedByExecuteAt[maxAppliedWriteByExecuteAt];
    }

    static int maxContiguousManagedAppliedIndex(TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, TxnId readyAt)
    {
        int i = maxAppliedWriteByExecuteAt + 1;
        while (i < committedByExecuteAt.length)
        {
            TxnInfo txn = committedByExecuteAt[i];
            // TODO (expected): should we count any final run of !managesExecution()? i.e. if we have Y(es)N(o)YNYNNN, should we not stop after only YNYNY?
            if (!txn.is(APPLIED) && managesExecution(txn) && (readyAt == null || readyAt.compareTo(txn) <= 0))
                break;
            ++i;
        }
        return i - 1;
    }

    static TxnInfo maxContiguousManagedApplied(TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, TxnId readyAt)
    {
        int i = maxContiguousManagedAppliedIndex(committedByExecuteAt, maxAppliedWriteByExecuteAt, readyAt);
        return i < 0 ? null : committedByExecuteAt[i];
    }

    /**
     * Treat anything pre bootstrap or redundant as applied. i.e.,
     *
     * max(max(readyAt, redundantBefore), maxContiguousManagedApplied().executeAt)
     */
    static Timestamp maxContiguousManagedAppliedExecuteAt(TxnInfo[] committedByExecuteAt, int maxAppliedWriteByExecuteAt, TxnId readyAt, TxnId redundantBefore)
    {
        Timestamp maxBound = TxnId.nonNullOrMax(redundantBefore, readyAt);
        TxnInfo maxInfo = maxContiguousManagedApplied(committedByExecuteAt, maxAppliedWriteByExecuteAt, readyAt);
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
                if (newInfo == txn)
                {
                    reportLinearizabilityViolation(key, newInfo.plainTxnId(), newInfo.plainExecuteAt(), maxAppliedWrite.plainTxnId(), maxAppliedWrite.plainExecuteAt());
                    break;
                }

                if (isUnready(txn))
                    break;
            }
        }
    }

    private void checkIntegrity(boolean expectUpToDate)
    {
        if (isParanoid())
        {
            Invariants.require(byId.length == 0 || byId[0].compareTo(redundantBefore()) >= 0);
            Invariants.require(prunedBeforeById == -1 || (prunedBefore().is(APPLIED) && prunedBefore().is(Write)));
            Invariants.require(minUndecidedManagedById < 0 || (byId[minUndecidedManagedById].status().compareTo(InternalStatus.COMMITTED) < 0 && mayExecute(byId[minUndecidedManagedById])));
            Invariants.require(minUndecidedUnmanagedById < 0 || (byId[minUndecidedUnmanagedById].status().compareTo(InternalStatus.COMMITTED) < 0 && !mayExecute(byId[minUndecidedUnmanagedById])));
            Invariants.require(maxAppliedWrite() == null || maxUniqueHlc >= maxAppliedWrite().executeAt.hlc());
            Invariants.require(maxAppliedUnreadyWriteById < 0 || (maxAppliedUnreadyWriteById < byId.length && byId[maxAppliedUnreadyWriteById].isWrite() && !byId[maxAppliedUnreadyWriteById].mayExecute()));

            if (maxAppliedWriteByExecuteAt >= 0)
            {
                Invariants.require(committedByExecuteAt[maxAppliedWriteByExecuteAt].kind() == Write);
                Invariants.require(committedByExecuteAt[maxAppliedWriteByExecuteAt].is(APPLIED));
            }

            if (testParanoia(LINEAR, NONE, LOW))
            {
                Invariants.requireArgument(SortedArrays.isSortedUnique(byId));
                Invariants.requireArgument(SortedArrays.isSortedUnique(committedByExecuteAt, TxnInfo::compareExecuteAt));

                for (int i = 0 ; i < byId.length ; ++i)
                {
                    TxnInfo txn = byId[i];
                    Invariants.require(mayExecute(txn) == txn.mayExecute());
                    Invariants.require(txn.hasDeps() || txn.missing() == NO_TXNIDS);
                }
                for (TxnInfo txn : committedByExecuteAt) Invariants.require(txn.mayExecute());

                if (minUndecidedManagedById >= 0) for (int i = 0; i < minUndecidedManagedById; ++i) Invariants.require(byId[i].status().compareTo(InternalStatus.COMMITTED) >= 0 || !mayExecute(byId[i]) || isUnready(byId[i]));
                else for (TxnInfo txn : byId) Invariants.require(txn.status().compareTo(InternalStatus.COMMITTED) >= 0 || !mayExecute(txn) || isUnready(txn));

                if (maxAppliedWriteByExecuteAt >= 0)
                {
                    for (int i = 0 ; i < maxAppliedWriteByExecuteAt ; ++i)
                        Invariants.require(committedByExecuteAt[i].compareTo(APPLIED) >= 0 || committedByExecuteAt[i].isSyncPoint() || !reportLinearizabilityViolations());
                    for (int i = maxAppliedWriteByExecuteAt + 1; i < committedByExecuteAt.length ; ++i)
                        Invariants.require(committedByExecuteAt[i].kind() != Kind.Write || committedByExecuteAt[i].status().compareTo(APPLIED) < 0);
                }
                else
                {
                    for (TxnInfo txn : committedByExecuteAt)
                        Invariants.require(txn.kind() != Kind.Write || txn.status().compareTo(APPLIED) < 0 && mayExecute(txn));
                }
                LoadingPruned minLoadingPruned = BTree.isEmpty(loadingPruned) ? null : BTree.findByIndex(loadingPruned, 0);
                Invariants.require(minLoadingPruned == null|| redundantBefore().compareTo(BTree.findByIndex(loadingPruned, 0)) <= 0);
                for (Unmanaged unmanaged : unmanageds)
                    Invariants.require(unmanaged.waitingUntil.epoch() < bounds.endEpoch);
            }
            if (testParanoia(SUPERLINEAR, NONE, LOW))
            {
                for (TxnInfo txn : committedByExecuteAt)
                {
                    Invariants.require(txn == get(txn));
                }
                for (TxnInfo txn : byId)
                {
                    if (txn.isCommittedAndExecutes())
                        Invariants.require(Arrays.binarySearch(committedByExecuteAt, txn, TxnInfo::compareExecuteAt) >= 0);

                    for (TxnId missingId : txn.missing())
                    {
                        Invariants.require(txn.witnesses(missingId));
                        TxnInfo missingInfo = get(missingId, byId);
                        Invariants.require(missingInfo == null ? missingId.is(UNSTABLE) && find(loadingPruned, missingId) != null : missingInfo.status().compareTo(InternalStatus.COMMITTED) < 0);
                        Invariants.require(txn.depsKnownBefore().compareTo(missingId) >= 0);
                    }
                    if (txn.isCommittedAndExecutes())
                        Invariants.require(txn == committedByExecuteAt[Arrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, txn, TxnInfo::compareExecuteAt)]);
                }
                for (LoadingPruned txn : BTree.<LoadingPruned>iterable(loadingPruned))
                {
                    Invariants.require(indexOf(txn) < 0);
                }
                int decidedBefore = minUndecidedManagedById < 0 ? byId.length : minUndecidedManagedById;
                if (!BTree.isEmpty(loadingPruned))
                {
                    int maxDecidedBefore = Arrays.binarySearch(byId, BTree.findByIndex(loadingPruned, 0));
                    if (maxDecidedBefore < 0)
                        maxDecidedBefore = -2 - maxDecidedBefore;
                    if (maxDecidedBefore < decidedBefore)
                        decidedBefore = maxDecidedBefore;
                }
                int appliedBefore = 1 + maxContiguousManagedAppliedIndex(committedByExecuteAt, maxAppliedWriteByExecuteAt, readyAt());
                if (readyAt() != null)
                {
                    for (int i = 0 ; i < appliedBefore ; ++i)
                    {
                        if (committedByExecuteAt[i].compareTo(readyAt()) < 0) continue;
                        if (!committedByExecuteAt[i].is(APPLIED))
                        {
                            appliedBefore = i;
                            break;
                        }
                    }
                }
                if (expectUpToDate)
                {
                    for (Unmanaged unmanaged : unmanageds)
                    {
                        switch (unmanaged.pending)
                        {
                            case COMMIT:
                            {
                                int byIdIndex = Arrays.binarySearch(byId, unmanaged.waitingUntil);
                                if (byIdIndex < 0)
                                    byIdIndex = -1 - byIdIndex;
                                Invariants.require(byIdIndex >= decidedBefore || isAnyPredecessorWaitingOnPruned(loadingPruned, unmanaged.txnId));
                                break;
                            }
                            case APPLY:
                            {
                                int byExecuteAtIndex = SortedArrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, unmanaged.waitingUntil, (f, i) -> f.compareTo(i.executeAt), FAST);
                                if (byExecuteAtIndex >= 0 && byExecuteAtIndex < appliedBefore)
                                {
                                    Invariants.require(!reportLinearizabilityViolations);
                                    int i = 0;
                                    for (; i < appliedBefore && committedByExecuteAt[i].compareTo(APPLIED) >= 0; ++i)
                                    {
                                    }
                                    Invariants.require(i < appliedBefore || isLoadingPruned());
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    public boolean equalContents(CommandsForKey that)
    {
        if (!Arrays.equals(this.unmanageds, that.unmanageds))
            return false;

        if (this.byId.length != that.byId.length)
            return false;

        for (int i = 0 ; i < byId.length ; ++i)
        {
            TxnInfo a = this.byId[i], b = that.byId[i];
            if (a.getClass() != b.getClass())
                return false;
            if (!a.equals(b))
                return false;
            if (a.statusOverrides() != b.statusOverrides())
                return false;
            if (a.status() != b.status())
                return false;
            if (!a.executeAt.equals(b.executeAt))
                return false;
            if (!a.ballot().equals(b.ballot()))
                return false;
            if (a.durablyCommittedBit() != b.durablyCommittedBit())
                return false;
            if (!TxnId.equalsStrict(a.missing(), b.missing()))
                return false;
        }

        return (this.hasMaxUniqueHlc() ? this.maxUniqueHlc : 0) == (that.hasMaxUniqueHlc() ? that.maxUniqueHlc : 0);
    }

    public boolean isEmpty()
    {
        return byId.length == 0 && unmanageds.length == 0 && !hasMaxUniqueHlc();
    }

    public boolean hasMaxUniqueHlc()
    {
        TxnInfo maxWrite = maxAppliedWrite();
        if (maxWrite != null && maxWrite.hlc() >= maxUniqueHlc)
            return false;
        TxnId gcBefore = redundantBefore();
        return gcBefore.hlc() <= maxUniqueHlc || !gcBefore.is(HLC_BOUND);
    }

    // TODO (required): this should be configurable per-node for tests
    public static boolean reportLinearizabilityViolations()
    {
        return reportLinearizabilityViolations;
    }

    static void reportLinearizabilityViolation(RoutingKey key, TxnId notWitnessed, Timestamp notWitnessedExecuteAt, TxnId by, Timestamp byExecuteAt)
    {
        if (!reportLinearizabilityViolations)
            return;

        ViolationHandler agent = ViolationHandlerHolder.get();
        agent.onDependencyViolation(RoutingKeys.of(key), notWitnessed, notWitnessedExecuteAt, by, byExecuteAt);
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
