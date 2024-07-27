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

package accord.local;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Key;

import accord.api.VisibleForImplementation;
import accord.impl.CommandsSummary;
import accord.local.SafeCommandStore.CommandFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.local.SafeCommandStore.TestStatus;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.Invariants;
import accord.utils.RelationMultiMap.SortedRelationList;
import accord.utils.SortedArrays;
import accord.utils.SortedList;
import accord.utils.btree.BTree;
import accord.utils.btree.BTreeRemoval;
import accord.utils.btree.UpdateFunction;
import net.nicoulaj.compilecommand.annotations.Inline;

import static accord.local.CommandsForKey.InternalStatus.ACCEPTED;
import static accord.local.CommandsForKey.InternalStatus.APPLIED;
import static accord.local.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.CommandsForKey.InternalStatus.PREACCEPTED_OR_ACCEPTED_INVALIDATE;
import static accord.local.CommandsForKey.InternalStatus.STABLE;
import static accord.local.CommandsForKey.InternalStatus.HISTORICAL;
import static accord.local.CommandsForKey.InternalStatus.INVALID_OR_TRUNCATED;
import static accord.local.CommandsForKey.InternalStatus.TRANSITIVELY_KNOWN;
import static accord.local.CommandsForKey.Unmanaged.Pending.APPLY;
import static accord.local.CommandsForKey.Unmanaged.Pending.COMMIT;
import static accord.local.KeyHistory.COMMANDS;
import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SaveStatus.LocalExecution.WaitingToApply;
import static accord.local.SaveStatus.LocalExecution.WaitingToExecute;
import static accord.primitives.Txn.Kind.Kinds.AnyGloballyVisible;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.Invariants.checkNonNegative;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.isParanoid;
import static accord.utils.Invariants.paranoia;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * <h2>Introduction</h2>
 * A specialised collection for efficiently representing and querying everything we need for making coordination
 * and recovery decisions about a key's command conflicts.
 *
 * Every command we know about that is not shard-redundant is listed in the {@code byId} collection, which is sorted by {@code TxnId}.
 * This includes all transitive dependencies we have witnessed via other transactions, but not witnessed directly.
 *
 * <h2>Dependency Encoding</h2>
 * The byId list implies the contents of the deps of all commands in the collection - that is, it is assumed that in
 * the normal course of events every transaction will include the full set of {@code TxnId} we know that would be
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
 * 1) redundantBefore represents the global lower bound covering the key for transactions all being applied or invalidated,
 * so we know that nothing with a lower TxnId should ever need to be computed, taken as a dependency or recovered. So
 * we may simply erase these.
 *
 * 2) prunedBefore represents a local bound that permits us to optimistically remove data from the CommandsForKey
 * that may need to be loaded again later. Specifically, we pick an applied {@code TxnId} that we will retain, and we
 * remove from the {@code CommandsForKey} any transaction with a lower {@code TxnId} and {@code executeAt} that is also
 * applied or invalidated.
 *
 * The trick here is that, by virtue of being a local point, we cannot guarantee that no coordinator will contact us
 * with either a new TxnId that is lower than this, or a dependency collection containing a TxnId we have already
 * processed.
 *
 * [We pick a TxnId stale by some time bound so that we can expect that any earlier already-applied TxnId will
 * not be included in a future set of dependencies - we expect that "transitive dependency elision" will ordinarily filter
 * it; but it might not on all replicas.]
 *
 * The difficulty is that we cannot immediately distinguish these two cases, and so on encountering a TxnId that is
 * less than our prunedBefore we must load the local command state for the TxnId. If we have not witnessed the TxnId
 * then we know it is a new transitive dependency. If we have witnessed it, and it is applied, then we load it
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
 * If the later command is recovered, this replica will report its Stable deps thereby recovering them.
 * If this replica is not contacted, some other replica must participate that either has taken the same action as this replica,
 * or else does not know the later command is Stable, and so will report the earlier command as a dependency again.
 * If the earlier command is recovered, this replica will report that it is {@code Committed}, and so will not consult
 * this replica's collection to decipher any fast path decision. Any other replica must either do the same, or else
 * will correctly record this transaction as present in any relevant deps of later transactions.
 *
 * TODO (expected): maintain separate redundantBefore and closedBefore timestamps, latter implied by any exclusivesyncpoint;
 *                  advance former based on Applied status of all TxnId before the latter
 * TODO (desired):  track whether a TxnId is a write on this key only for execution (rather than globally)
 * TODO (expected): merge with TimestampsForKey
 * TODO (desired):  save space by encoding InternalStatus in TxnId.flags(), so that when executeAt==txnId we can save 8 bytes per entry
 * TODO (expected): remove a command that is committed to not intersect with the key for this store (i.e. if accepted in a later epoch than committed on, so ownership changes)
 * TODO (expected): avoid updating transactions we don't manage the execution of - perhaps have a dedicated InternalStatus
 * TODO (expected): minimise repeated notification, either by logic or marking a command as notified once ready-to-execute
 * TODO (required): linearizability violation detection
 */
public class CommandsForKey extends CommandsForKeyUpdate implements CommandsSummary
{
    private static final Logger logger = LoggerFactory.getLogger(CommandsForKey.class);
    private static final boolean ELIDE_TRANSITIVE_DEPENDENCIES = true;

    public static final RedundantBefore.Entry NO_REDUNDANT_BEFORE = new RedundantBefore.Entry(null, Long.MIN_VALUE, Long.MAX_VALUE, TxnId.NONE, TxnId.NONE, TxnId.NONE, null);
    public static final TxnId[] NO_TXNIDS = new TxnId[0];
    public static final TxnInfo NO_INFO = new TxnInfo(TxnId.NONE, HISTORICAL, TxnId.NONE);
    public static final TxnInfo[] NO_INFOS = new TxnInfo[0];
    public static final Unmanaged[] NO_PENDING_UNMANAGED = new Unmanaged[0];

    /**
     * Transactions that are witnessed by {@code CommandsForKey} for dependency management
     * (essentially all globally visible key transactions).
     */
    public static boolean manages(TxnId txnId)
    {
        return txnId.domain().isKey() && txnId.kind().isGloballyVisible();
    }

    /**
     * Transactions whose execution will be wholly managed by {@code CommandsForKey} (essentially reads and writes).
     *
     * Other transactions that depend on these transactions need only adopt a dependency on the {@code Key} to represent
     * all of these transactions; the {@code CommandsForKey} will then notify when they have executed.
     */
    public static boolean managesExecution(TxnId txnId)
    {
        return Write.witnesses(txnId.kind()) && txnId.domain().isKey();
    }

    /**
     * managesExecution when we already know the TxnId covers keys
     */
    public static boolean managesKeyExecution(TxnId txnId)
    {
        return Write.witnesses(txnId.kind());
    }

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

    public static class SerializerSupport
    {
        public static CommandsForKey create(Key key, TxnInfo[] txns, Unmanaged[] unmanageds, TxnId prunedBefore)
        {
            return new CommandsForKey(key, NO_REDUNDANT_BEFORE, prunedBefore, LoadingPruned.empty(), txns, unmanageds);
        }
    }

    public enum InternalStatus
    {
        TRANSITIVELY_KNOWN(false, false), // (unwitnessed; no need for mapReduce to witness)
        HISTORICAL(false, false),
        PREACCEPTED_OR_ACCEPTED_INVALIDATE(false, true),
        ACCEPTED(true, true),
        COMMITTED(true, true),
        STABLE(true, false),
        APPLIED(true, false),
        INVALID_OR_TRUNCATED(false, false);

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
            convert.put(SaveStatus.TruncatedApplyWithDeps, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.TruncatedApplyWithOutcome, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.TruncatedApply, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.ErasedOrInvalidated, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.Erased, INVALID_OR_TRUNCATED);
            convert.put(SaveStatus.Invalidated, INVALID_OR_TRUNCATED);
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

        boolean hasDeps()
        {
            return hasExecuteAtOrDeps;
        }

        boolean hasStableDeps()
        {
            return this == STABLE || this == APPLIED;
        }

        public boolean isCommitted()
        {
            return this == COMMITTED | this == STABLE | this == APPLIED;
        }

        public Timestamp depsKnownBefore(TxnId txnId, Timestamp executeAt)
        {
            switch (this)
            {
                default: throw new AssertionError("Unhandled InternalStatus: " + this);
                case TRANSITIVELY_KNOWN:
                case INVALID_OR_TRUNCATED:
                case HISTORICAL:
                    throw new AssertionError("Invalid InternalStatus to know deps");

                case PREACCEPTED_OR_ACCEPTED_INVALIDATE:
                case ACCEPTED:
                    return txnId;

                case APPLIED:
                case STABLE:
                case COMMITTED:
                    return executeAt;
            }
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

    /**
     * A TxnId that we have witnessed as a dependency that predates {@link #prunedBefore}, so we must load its
     * Command state to determine if this is a new transaction to track, or if it is an already-applied transaction
     * we have pruned.
     */
    static class LoadingPruned extends TxnId
    {
        static final UpdateFunction.Simple<LoadingPruned> LOADINGF = UpdateFunction.Simple.of(LoadingPruned::merge);

        /**
         * Transactions that had witnessed this pre-pruned TxnId and are therefore waiting for the load to complete
         */
        final TxnId[] witnessedBy;

        public LoadingPruned(TxnId copy, TxnId[] witnessedBy)
        {
            super(copy);
            this.witnessedBy = witnessedBy;
        }

        LoadingPruned merge(LoadingPruned that)
        {
            return new LoadingPruned(this, SortedArrays.linearUnion(witnessedBy, that.witnessedBy, cachedTxnIds()));
        }

        static Object[] empty()
        {
            return BTree.empty();
        }

        /**
         * Updating {@code loadingPruned} to register that each element of {@code toLoad} is being loaded for {@code loadingFor}
         */
        static Object[] load(Object[] loadingPruned, TxnId[] toLoad, TxnId loadingFor)
        {
            return load(loadingPruned, toLoad, new TxnId[] { loadingFor });
        }

        static Object[] load(Object[] loadingPruned, TxnId[] toLoad, TxnId[] loadingForAsList)
        {
            Object[] toLoadAsTree;
            try (BTree.FastBuilder<LoadingPruned> fastBuilder = BTree.fastBuilder())
            {
                for (TxnId txnId : toLoad)
                    fastBuilder.add(new LoadingPruned(txnId, loadingForAsList));
                toLoadAsTree = fastBuilder.build();
            }
            return BTree.update(loadingPruned, toLoadAsTree, LoadingPruned::compareTo, LOADINGF);
        }

        /**
         * Find the list of TxnId that are waiting for {@code find} to load
         */
        static TxnId[] get(Object[] loadingPruned, TxnId find, TxnId[] ifNoMatch)
        {
            LoadingPruned obj = (LoadingPruned) BTree.find(loadingPruned, TxnId::compareTo, find);
            if (obj == null)
                return ifNoMatch;

            return obj.witnessedBy;
        }

        /**
         * Updating {@code loadingPruned} to remove {@code find}, as it has been loaded
         */
        static Object[] remove(Object[] loadingPruned, TxnId find)
        {
            return BTreeRemoval.remove(loadingPruned, TxnId::compareTo, find);
        }

        /**
         * Return true if {@code waitingId} is waiting for any transaction with a lower TxnId than waitingExecuteAt
         */
        static boolean isWaiting(Object[] loadingPruned, TxnId waitingId, Timestamp waitingExecuteAt)
        {
            if (BTree.isEmpty(loadingPruned))
                return false;

            int ceilIndex = BTree.ceilIndex(loadingPruned, Timestamp::compareTo, waitingExecuteAt);
            // TODO (desired): this is O(n.lg n), whereas we could import the accumulate function and perform in O(max(m, lg n))
            for (int i = 0 ; i < ceilIndex ; ++i)
            {
                TxnId[] loadingFor = ((LoadingPruned)BTree.findByIndex(loadingPruned, i)).witnessedBy;
                if (Arrays.binarySearch(loadingFor, waitingId) >= 0)
                    return true;
            }

            return false;
        }
    }

    /**
     * An object representing the basic CommandsForKey state, extending TxnId to save memory and improve locality.
     */
    public static class TxnInfo extends TxnId
    {
        public final InternalStatus status;
        public final Timestamp executeAt;

        TxnInfo(TxnId txnId, InternalStatus status, Timestamp executeAt)
        {
            super(txnId);
            this.status = status;
            this.executeAt = executeAt == txnId ? this : executeAt;
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status)
        {
            return new TxnInfo(txnId, status, txnId);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, Command command)
        {
            Timestamp executeAt = txnId;
            if (status.hasExecuteAt()) executeAt = command.executeAt();
            Ballot ballot;
            if (!status.hasBallot || (ballot = command.acceptedOrCommitted()).equals(Ballot.ZERO))
                return new TxnInfo(txnId, status, executeAt);
            return new TxnInfoExtra(txnId, status, executeAt, NO_TXNIDS, ballot);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, @Nonnull Timestamp executeAt, @Nonnull Ballot ballot)
        {
            Invariants.checkState(executeAt == txnId || !executeAt.equals(txnId));
            Invariants.checkState(status.hasExecuteAtOrDeps || executeAt == txnId);
            Invariants.checkState(status.hasBallot || ballot == Ballot.ZERO);
            if (!status.hasBallot || ballot.equals(Ballot.ZERO)) return new TxnInfo(txnId, status, executeAt);
            return new TxnInfoExtra(txnId, status, executeAt, NO_TXNIDS, ballot);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing, @Nonnull Ballot ballot)
        {
            Invariants.checkState(executeAt == txnId || !executeAt.equals(txnId));
            Invariants.checkState(status.hasExecuteAtOrDeps || executeAt == txnId);
            Invariants.checkState(status.hasBallot || ballot == Ballot.ZERO);
            Invariants.checkState(status.hasExecuteAtOrDeps || missing == NO_TXNIDS);
            if (missing == NO_TXNIDS && (!status.hasBallot || ballot == Ballot.ZERO)) return new TxnInfo(txnId, status, executeAt);
            Invariants.checkState(missing.length > 0 || missing == NO_TXNIDS);
            return new TxnInfoExtra(txnId, status, executeAt, missing, ballot);
        }

        public static TxnInfo createMock(TxnId txnId, InternalStatus status, @Nullable Timestamp executeAt, @Nullable TxnId[] missing, @Nullable Ballot ballot)
        {
            Invariants.checkState(executeAt == null || executeAt == txnId || !executeAt.equals(txnId));
            Invariants.checkArgument(missing == null || missing == NO_TXNIDS);
            if (missing == NO_TXNIDS && ballot == Ballot.ZERO) return new TxnInfo(txnId, status, executeAt);
            return new TxnInfoExtra(txnId, status, executeAt, missing, ballot);
        }

        Timestamp depsKnownBefore()
        {
            return status.depsKnownBefore(this, executeAt);
        }

        public TxnInfo update(TxnId[] newMissing)
        {
            Invariants.checkState(status.hasExecuteAtOrDeps);
            return newMissing == NO_TXNIDS
                   ? new TxnInfo(this, status, executeAt)
                   : new TxnInfoExtra(this, status, executeAt, newMissing, Ballot.ZERO);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TxnInfo info = (TxnInfo) o;
            return status == info.status
                   && (executeAt == this ? info.executeAt == info : Objects.equals(executeAt, info.executeAt))
                   && Arrays.equals(missing(), info.missing());
        }

        TxnId plainTxnId()
        {
            return new TxnId(this);
        }

        Timestamp plainExecuteAt()
        {
            return executeAt == this ? plainTxnId() : executeAt;
        }

        /**
         * Any uncommitted transactions the owning CommandsForKey is aware of, that could have been included in our
         * dependencies but weren't.
         *
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
                   ", status=" + status +
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

        Timestamp executeAtIfKnownElseTxnId()
        {
            return status == INVALID_OR_TRUNCATED ? this : executeAt;
        }
    }

    public static class TxnInfoExtra extends TxnInfo
    {
        /**
         * {@link TxnInfo#missing()}
         */
        public final TxnId[] missing;
        public final Ballot ballot;

        TxnInfoExtra(TxnId txnId, InternalStatus status, Timestamp executeAt, TxnId[] missing, Ballot ballot)
        {
            super(txnId, status, executeAt);
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

        public TxnInfo update(TxnId[] newMissing)
        {
            if (newMissing == missing)
                return this;

            return newMissing == NO_TXNIDS && ballot == Ballot.ZERO
                   ? new TxnInfo(this, status, executeAt)
                   : new TxnInfoExtra(this, status, executeAt, newMissing, ballot);
        }

        @Override
        public String toString()
        {
            return "Info{" +
                   "txnId=" + toPlainString() +
                   ", status=" + status +
                   ", executeAt=" + plainExecuteAt() +
                   (ballot != Ballot.ZERO ? ", ballot=" + ballot : "") +
                   ", missing=" + Arrays.toString(missing) +
                   '}';
        }
    }

    private final Key key;
    private final RedundantBefore.Entry redundantBefore;
    private final TxnInfo prunedBefore;
    // a btree keyed by TxnId we have encountered since pruning that occur before prunedBefore;
    // mapping to those TxnId that had witnessed this potentially-pruned TxnId.
    private final Object[] loadingPruned;
    // all transactions, sorted by TxnId
    private final TxnInfo[] byId;
    // reads and writes ONLY that are committed or stable or applied, keyed by executeAt
    // TODO (required): validate that it is always a prefix that is Applied (i.e. never a gap)
    private final TxnInfo[] committedByExecuteAt;
    private final int minUndecidedById, maxAppliedWriteByExecuteAt;
    private final Unmanaged[] unmanageds;

    CommandsForKey(Key key, RedundantBefore.Entry redundantBefore, TxnInfo prunedBefore, Object[] loadingPruned, TxnInfo[] byId, TxnInfo[] committedByExecuteAt, int minUndecidedById, int maxAppliedWriteByExecuteAt, Unmanaged[] unmanageds)
    {
        this.key = key;
        this.redundantBefore = Invariants.nonNull(redundantBefore);
        this.prunedBefore = Invariants.nonNull(prunedBefore);
        this.loadingPruned = loadingPruned;
        this.byId = byId;
        this.committedByExecuteAt = committedByExecuteAt;
        this.minUndecidedById = minUndecidedById;
        this.maxAppliedWriteByExecuteAt = maxAppliedWriteByExecuteAt;
        this.unmanageds = unmanageds;
        checkIntegrity();
    }

    private void checkIntegrity()
    {
        Invariants.checkState(prunedBefore == NO_INFO || (prunedBefore.status == APPLIED && prunedBefore.kind().isWrite()));
        Invariants.checkState(minUndecidedById < 0 || byId[minUndecidedById].status.compareTo(COMMITTED) < 0);
        Invariants.checkState(maxAppliedWriteByExecuteAt < 0 || committedByExecuteAt[maxAppliedWriteByExecuteAt].status == APPLIED);
        if (isParanoid())
        {
            Invariants.checkArgument(SortedArrays.isSortedUnique(byId));
            Invariants.checkArgument(SortedArrays.isSortedUnique(committedByExecuteAt, TxnInfo::compareExecuteAt));

            if (paranoia() >= 2)
            {
                if (minUndecidedById >= 0) for (int i = 0 ; i < minUndecidedById ; ++i) Invariants.checkState(byId[i].status.compareTo(COMMITTED) >= 0);
                else for (TxnInfo txn : byId) Invariants.checkState(txn.status.compareTo(COMMITTED) >= 0);

                if (maxAppliedWriteByExecuteAt >= 0)
                {
                    Invariants.checkState(committedByExecuteAt[maxAppliedWriteByExecuteAt].kind() == Write);
                    Invariants.checkState(committedByExecuteAt[maxAppliedWriteByExecuteAt].status == APPLIED);
                    for (int i = maxAppliedWriteByExecuteAt + 1; i < committedByExecuteAt.length ; ++i) Invariants.checkState(committedByExecuteAt[i].kind() != Kind.Write || committedByExecuteAt[i].status.compareTo(APPLIED) < 0);
                }
                else
                {
                    for (TxnInfo txn : committedByExecuteAt) Invariants.checkState(txn.kind() != Kind.Write || txn.status.compareTo(APPLIED) < 0);
                }

                for (TxnInfo txn : committedByExecuteAt)
                {
                    Invariants.checkState(txn == get(txn, byId));
                }
                for (TxnInfo txn : byId)
                {
                    Invariants.checkState(manages(txn));
                    for (TxnId missingId : txn.missing())
                    {
                        Invariants.checkState(txn.kind().witnesses(missingId));
                        Invariants.checkState(get(missingId, byId).status.compareTo(COMMITTED) < 0);
                    }
                }
                for (LoadingPruned txn : BTree.<LoadingPruned>iterable(loadingPruned))
                {
                    Invariants.checkState(indexOf(txn) < 0);
                }
            }
        }
    }

    CommandsForKey(Key key, RedundantBefore.Entry redundantBefore, TxnId prunedBefore, Object[] loadingPruned, TxnInfo[] byId, Unmanaged[] unmanageds)
    {
        this.key = key;
        this.redundantBefore = Invariants.nonNull(redundantBefore);
        this.prunedBefore = redundantBefore.shardRedundantBefore().compareTo(prunedBefore) >= 0
                            ? NO_INFO : prunedBefore.equals(TxnId.NONE)
                                        ? NO_INFO : Invariants.nonNull(get(prunedBefore, byId));
        this.loadingPruned = loadingPruned;
        this.byId = byId;
        this.unmanageds = unmanageds;

        int countCommitted = 0;
        int minUndecided = -1;
        for (int i = 0; i < byId.length ; ++i)
        {
            TxnInfo txn = byId[i];
            if (txn.status == INVALID_OR_TRUNCATED) continue;
            if (txn.status.compareTo(COMMITTED) >= 0 && managesExecution(txn)) ++countCommitted;
            else if (txn.status.compareTo(COMMITTED) < 0 && minUndecided == -1) minUndecided = i;
        }
        this.minUndecidedById = minUndecided;
        this.committedByExecuteAt = new TxnInfo[countCommitted];
        countCommitted = 0;
        for (TxnInfo txn : byId)
        {
            if (txn.status.compareTo(COMMITTED) >= 0 && txn.status != INVALID_OR_TRUNCATED && managesExecution(txn))
                committedByExecuteAt[countCommitted++] = txn;
        }
        Arrays.sort(committedByExecuteAt, TxnInfo::compareExecuteAt);
        int maxAppliedByExecuteAt = committedByExecuteAt.length;
        while (--maxAppliedByExecuteAt >= 0)
        {
            TxnInfo txn = committedByExecuteAt[maxAppliedByExecuteAt];
            if (txn.status == APPLIED && txn.kind() == Write)
                break;
        }
        this.maxAppliedWriteByExecuteAt = maxAppliedByExecuteAt;

        checkIntegrity();
    }

    CommandsForKey(CommandsForKey copy, Object[] loadingPruned, Unmanaged[] unmanageds)
    {
        this.key = copy.key;
        this.redundantBefore = copy.redundantBefore;
        this.prunedBefore = copy.prunedBefore;
        this.loadingPruned = loadingPruned;
        this.byId = copy.byId;
        this.committedByExecuteAt = copy.committedByExecuteAt;
        this.minUndecidedById = copy.minUndecidedById;
        this.maxAppliedWriteByExecuteAt = copy.maxAppliedWriteByExecuteAt;
        this.unmanageds = unmanageds;

        checkIntegrity();
    }

    public CommandsForKey(Key key)
    {
        this.key = key;
        this.redundantBefore = NO_REDUNDANT_BEFORE;
        this.prunedBefore = NO_INFO;
        this.loadingPruned = LoadingPruned.empty();
        this.byId = NO_INFOS;
        this.committedByExecuteAt = NO_INFOS;
        this.minUndecidedById = this.maxAppliedWriteByExecuteAt = -1;
        this.unmanageds = NO_PENDING_UNMANAGED;
    }

    @Override
    public String toString()
    {
        return "CommandsForKey@" + System.identityHashCode(this) + '{' + key + '}';
    }

    public Key key()
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

    public RedundantBefore.Entry redundantBefore()
    {
        return redundantBefore;
    }

    public TxnId prunedBefore()
    {
        return prunedBefore;
    }

    public TxnId locallyRedundantBefore()
    {
        return redundantBefore.locallyRedundantBefore();
    }

    public TxnId shardRedundantBefore()
    {
        return redundantBefore.shardRedundantBefore();
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
        TxnId[] loadingFor = null;
        {
            int insertPos = Arrays.binarySearch(byId, testTxnId);
            if (insertPos < 0)
            {
                insertPos = -1 - insertPos;
                switch (testDep)
                {
                    default: throw new AssertionError("Unhandled TestDep: " + testDep);
                    case ANY_DEPS:
                        break;

                    case WITH:
                        if (testTxnId.compareTo(prunedBefore) >= 0)
                            return initialValue;

                        loadingFor = LoadingPruned.get(loadingPruned, testTxnId, NO_TXNIDS);
                        break;

                    case WITHOUT:
                        if (testTxnId.compareTo(prunedBefore) < 0)
                            loadingFor = LoadingPruned.get(loadingPruned, testTxnId, NO_TXNIDS);
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
            if (!testKind.test(txn.kind())) continue;
            InternalStatus status = txn.status;
            switch (testStatus)
            {
                default: throw new AssertionError("Unhandled TestStatus: " + testStatus);
                case IS_PROPOSED:
                    if (status == ACCEPTED || status == COMMITTED) break;
                    else continue;
                case IS_STABLE:
                    if (status.compareTo(STABLE) >= 0 && status.compareTo(INVALID_OR_TRUNCATED) < 0) break;
                    else continue;
                case ANY_STATUS:
                    if (status == TRANSITIVELY_KNOWN)
                        continue;
            }

            Timestamp executeAt = txn.executeAt;
            if (testDep != ANY_DEPS)
            {
                if (!status.hasExecuteAtOrDeps)
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
        Timestamp maxCommittedBefore;
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
            while (i >= 0 && !committedByExecuteAt[i].kind().isWrite()) --i;
            maxCommittedBefore = i < 0 ? null : committedByExecuteAt[i].executeAt;
        }
        int start = 0, end = insertPos(start, startedBefore);

        for (int i = start; i < end ; ++i)
        {
            TxnInfo txn = byId[i];
            if (!testKind.test(txn.kind()))
                continue;

            switch (txn.status)
            {
                case COMMITTED:
                case STABLE:
                case APPLIED:
                    // TODO (expected): prove the correctness of this approach
                    if (!ELIDE_TRANSITIVE_DEPENDENCIES || maxCommittedBefore == null || txn.executeAt.compareTo(maxCommittedBefore) >= 0 || !Write.witnesses(txn))
                        break;
                case TRANSITIVELY_KNOWN:
                case INVALID_OR_TRUNCATED:
                    continue;
            }

            initialValue = map.apply(p1, key, txn.plainTxnId(), txn.executeAt, initialValue);
        }
        return initialValue;
    }

    // NOTE: prev MAY NOT be the version that last updated us due to various possible race conditions
    @VisibleForTesting
    public CommandsForKeyUpdate update(Command next)
    {
        InternalStatus newStatus = InternalStatus.from(next.saveStatus());
        if (newStatus == null)
            return this;

        return update(newStatus, next, false);
    }

    CommandsForKeyUpdate updatePruned(Command next)
    {
        InternalStatus newStatus = InternalStatus.from(next.saveStatus());
        if (newStatus == null)
            newStatus = TRANSITIVELY_KNOWN;

        return update(newStatus, next, true);
    }

    private CommandsForKeyUpdate update(InternalStatus newStatus, Command next, boolean wasPruned)
    {
        TxnId txnId = next.txnId();
        Invariants.checkArgument(manages(txnId));
        int pos = Arrays.binarySearch(byId, txnId);
        CommandsForKeyUpdate result;
        if (pos < 0)
        {
            pos = -1 - pos;
            if (newStatus.hasExecuteAtOrDeps && !wasPruned) result = insert(pos, txnId, newStatus, next);
            else result = insert(pos, txnId, TxnInfo.create(txnId, newStatus, next));
        }
        else
        {
            // update
            TxnInfo cur = byId[pos];

            if (cur != null)
            {
                int c = newStatus.compareTo(cur.status);
                if (c <= 0)
                {
                    if (c < 0)
                    {
                        if (!(newStatus == PREACCEPTED_OR_ACCEPTED_INVALIDATE && cur.status == ACCEPTED && next.acceptedOrCommitted().compareTo(cur.ballot()) > 0))
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

            if (newStatus.hasExecuteAtOrDeps && !wasPruned) result = update(pos, txnId, cur, newStatus, next);
            else result = update(pos, txnId, cur, TxnInfo.create(txnId, newStatus, next));
        }

        return result;
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

    private CommandsForKeyUpdate insert(int insertPos, TxnId plainTxnId, InternalStatus newStatus, Command command)
    {
        return insertOrUpdate(insertPos, -1, plainTxnId, null, newStatus, command);
    }

    private CommandsForKeyUpdate update(int updatePos, TxnId plainTxnId, TxnInfo curInfo, InternalStatus newStatus, Command command)
    {
        return insertOrUpdate(updatePos, updatePos, plainTxnId, curInfo, newStatus, command);
    }

    private CommandsForKeyUpdate insertOrUpdate(int insertPos, int updatePos, TxnId plainTxnId, TxnInfo curInfo, InternalStatus newStatus, Command command)
    {
        // TODO (now): do not calculate any deps or additions if we're transitioning from Stable to Applied; wasted effort and might trigger LoadPruned
        Object newInfoObj = computeInfoAndAdditions(insertPos, updatePos, plainTxnId, newStatus, command);
        if (newInfoObj.getClass() != InfoWithAdditions.class)
            return insertOrUpdate(insertPos, plainTxnId, curInfo, (TxnInfo)newInfoObj);

        InfoWithAdditions newInfoWithAdditions = (InfoWithAdditions) newInfoObj;
        TxnId[] additions = newInfoWithAdditions.additions;
        int additionCount = newInfoWithAdditions.additionCount;
        TxnInfo newInfo = newInfoWithAdditions.info;

        TxnId[] prunedIds = removePruned(additions, additionCount, prunedBefore);
        Object[] newLoadingPruned = loadingPruned;
        if (prunedIds != NO_TXNIDS)
        {
            additionCount -= prunedIds.length;
            newLoadingPruned = LoadingPruned.load(loadingPruned, prunedIds, plainTxnId);
        }

        TxnInfo[] newById = new TxnInfo[byId.length + additionCount + (updatePos < 0 ? 1 : 0)];
        TxnId[] loadingAsPrunedFor = LoadingPruned.get(newLoadingPruned, plainTxnId, null); // we default to null to distinguish between no match, and a match with NO_TXNIDS
        if (loadingAsPrunedFor != null) newLoadingPruned = LoadingPruned.remove(newLoadingPruned, plainTxnId);
        else loadingAsPrunedFor = NO_TXNIDS;

        insertOrUpdateWithAdditions(insertPos, updatePos, plainTxnId, newInfo, additions, additionCount, newById, loadingAsPrunedFor);
        if (paranoia() >= 2)
            validateMissing(newById, additions, additionCount, curInfo, newInfo, loadingAsPrunedFor);

        int newMinUndecidedById = minUndecidedById;
        {
            TxnId updatedIfUndecided = newInfo.status.compareTo(COMMITTED) < 0 ? newInfo : null;
            TxnId minNewUndecided = additionCount == 0 ? updatedIfUndecided : TxnId.nonNullOrMin(updatedIfUndecided, additions[0]);
            TxnId prevMinUndecided = minUndecided();
            if (minNewUndecided == null)
            {
                // ==> additionCount == 0
                if (insertPos <= minUndecidedById)
                {
                    if (insertPos == minUndecidedById)
                        newMinUndecidedById = nextUndecided(newById, newMinUndecidedById + 1);
                    else if (updatePos < 0)
                        ++newMinUndecidedById;
                }
            }
            else
            {
                // TODO (desired): can probably be more efficient in some cases by narrowing search range
                if (prevMinUndecided == null || prevMinUndecided.compareTo(minNewUndecided) >= 0)
                    newMinUndecidedById = additionCount == 0 ? insertPos : Arrays.binarySearch(newById, 0, newById.length, minNewUndecided);
                else if (insertPos < minUndecidedById && updatePos < 0)
                    ++newMinUndecidedById;
                else if (insertPos == minUndecidedById && (updatePos < 0 || updatedIfUndecided == null))
                    newMinUndecidedById = nextUndecided(newById, newMinUndecidedById + 1);
            }
        }

        cachedTxnIds().forceDiscard(additions, additionCount + prunedIds.length);
        return LoadPruned.load(prunedIds, update(newById, newMinUndecidedById, newLoadingPruned, plainTxnId, curInfo, newInfo, false));
    }

    private static TxnId[] removePruned(TxnId[] additions, int additionCount, TxnId prunedBefore)
    {
        if (additions[0].compareTo(prunedBefore) >= 0)
            return NO_TXNIDS;

        int prunedIndex = Arrays.binarySearch(additions, 1, additionCount, prunedBefore);
        if (prunedIndex < 0) prunedIndex = -1 - prunedIndex;
        if (prunedIndex == 0)
            return NO_TXNIDS;

        TxnId[] prunedIds = new TxnId[prunedIndex];
        System.arraycopy(additions, 0, prunedIds, 0, prunedIndex);
        System.arraycopy(additions, prunedIndex, additions, 0, additionCount - prunedIndex);
        return prunedIds;
    }

    private static int nextUndecided(TxnInfo[] infos, int pos)
    {
        while (true)
        {
            if (pos == infos.length)
                return -1;

            if (infos[pos].status.compareTo(COMMITTED) < 0)
                return pos;

            ++pos;
        }
    }

    private void insertOrUpdateWithAdditions(int sourceInsertPos, int sourceUpdatePos, TxnId updatePlainTxnId, TxnInfo info, TxnId[] additions, int additionCount, TxnInfo[] newInfos, TxnId[] withAsDep)
    {
        int additionInsertPos = Arrays.binarySearch(additions, 0, additionCount, updatePlainTxnId);
        additionInsertPos = Invariants.checkArgument(-1 - additionInsertPos, additionInsertPos < 0);
        int targetInsertPos = sourceInsertPos + additionInsertPos;

        // additions plus the updateTxnId when necessary
        TxnId[] missingSource = additions;
        boolean insertSelfMissing = sourceUpdatePos < 0 && info.status.compareTo(COMMITTED) < 0;
        boolean removeSelfMissing = sourceUpdatePos >= 0 && info.status.compareTo(COMMITTED) >= 0 && byId[sourceUpdatePos].status.compareTo(COMMITTED) < 0;

        // the most recently constructed pure insert missing array, so that it may be reused if possible
        int i = 0, j = 0, missingCount = 0, missingLimit = additionCount, count = 0;
        while (i < byId.length)
        {
            if (count == targetInsertPos)
            {
                newInfos[count] = info;
                if (i == sourceUpdatePos) ++i;
                else if (insertSelfMissing) ++missingCount;
                ++count;
                continue;
            }

            int c = j == additionCount ? -1 : byId[i].compareTo(additions[j]);
            if (c < 0)
            {
                TxnInfo txn = byId[i];
                if (i == sourceUpdatePos)
                {
                    txn = info;
                }
                else if (txn.status.hasDeps())
                {
                    Timestamp depsKnownBefore = txn.depsKnownBefore();
                    if (insertSelfMissing && missingSource == additions && (missingCount != j || (depsKnownBefore != txn && depsKnownBefore.compareTo(updatePlainTxnId) > 0)))
                    {
                        missingSource = insertMissing(additions, additionCount, updatePlainTxnId, additionInsertPos);
                        ++missingLimit;
                    }

                    int to = to(txn, depsKnownBefore, missingSource, missingCount, missingLimit);
                    if (to > 0 || removeSelfMissing)
                    {
                        TxnId[] prevMissing = txn.missing();
                        TxnId[] newMissing = prevMissing;
                        if (to > 0)
                        {
                            TxnId skipInsertMissing = null;
                            if (withAsDep != null && Arrays.binarySearch(withAsDep, updatePlainTxnId) >= 0)
                                skipInsertMissing = updatePlainTxnId;

                            newMissing = mergeAndFilterMissing(txn, prevMissing, missingSource, to, skipInsertMissing);
                        }

                        if (removeSelfMissing)
                            newMissing = removeOneMissing(newMissing, updatePlainTxnId);

                        if (newMissing != prevMissing)
                            txn = txn.update(newMissing);
                    }
                }
                newInfos[count] = txn;
                i++;
            }
            else if (c > 0)
            {
                TxnId txnId = additions[j++];
                newInfos[count] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId, Ballot.ZERO);
                ++missingCount;
            }
            else
            {
                throw illegalState(byId[i] + " should be an insertion, but found match when merging with origin");
            }
            count++;
        }

        if (j < additionCount)
        {
            if (count <= targetInsertPos)
            {
                while (count < targetInsertPos)
                {
                    TxnId txnId = additions[j++];
                    newInfos[count++] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId, Ballot.ZERO);
                }
                newInfos[targetInsertPos] = info;
                count = targetInsertPos + 1;
            }
            while (j < additionCount)
            {
                TxnId txnId = additions[j++];
                newInfos[count++] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId, Ballot.ZERO);
            }
        }
        else if (count == targetInsertPos)
        {
            newInfos[targetInsertPos] = info;
        }
    }

    private static void validateMissing(TxnInfo[] byId, TxnId[] additions, int additionCount, TxnInfo curInfo, TxnInfo newInfo, TxnId[] shouldNotHaveMissing)
    {
        for (TxnInfo txn : byId)
        {
            if (txn == newInfo) continue;
            if (!txn.status.hasDeps()) continue;
            int additionIndex = Arrays.binarySearch(additions, 0, additionCount, txn.depsKnownBefore());
            if (additionIndex < 0) additionIndex = -1 - additionIndex;
            TxnId[] missing = txn.missing();
            int j = 0;
            for (int i = 0 ; i < additionIndex ; ++i)
            {
                if (!txn.kind().witnesses(additions[i])) continue;
                j = SortedArrays.exponentialSearch(missing, j, missing.length, additions[i]);
                if (shouldNotHaveMissing != NO_TXNIDS && Arrays.binarySearch(shouldNotHaveMissing, txn) >= 0) Invariants.checkState(j < 0);
                else Invariants.checkState(j >= 0);
            }
            if (curInfo == null && newInfo.status.compareTo(COMMITTED) < 0 && txn.kind().witnesses(newInfo) && txn.depsKnownBefore().compareTo(newInfo) > 0 && (shouldNotHaveMissing == NO_TXNIDS || Arrays.binarySearch(shouldNotHaveMissing, txn) < 0))
                Invariants.checkState(Arrays.binarySearch(missing, newInfo) >= 0);
        }
    }

    private CommandsForKeyUpdate insertOrUpdate(int pos, TxnId plainTxnId, TxnInfo curInfo, TxnInfo newInfo)
    {
        if (curInfo == newInfo)
            return this;

        int newMinUndecidedById = minUndecidedById;
        TxnInfo[] newInfos;
        if (curInfo == null)
        {
            newInfos = new TxnInfo[byId.length + 1];
            System.arraycopy(byId, 0, newInfos, 0, pos);
            newInfos[pos] = newInfo;
            System.arraycopy(byId, pos, newInfos, pos + 1, byId.length - pos);
            if (newInfo.status.compareTo(COMMITTED) >= 0)
            {
                if (newMinUndecidedById < 0 || pos <= newMinUndecidedById)
                {
                    if (pos < newMinUndecidedById) ++newMinUndecidedById;
                    else newMinUndecidedById = nextUndecided(newInfos, pos + 1);
                }
            }
            else
            {
                if (newMinUndecidedById < 0 || pos < newMinUndecidedById)
                    newMinUndecidedById = pos;
            }
        }
        else
        {
            newInfos = byId.clone();
            newInfos[pos] = newInfo;
            if (pos == newMinUndecidedById && curInfo.status.compareTo(COMMITTED) < 0 && newInfo.status.compareTo(COMMITTED) >= 0)
                newMinUndecidedById = nextUndecided(newInfos, pos + 1);
        }

        return update(newInfos, newMinUndecidedById, loadingPruned, plainTxnId, curInfo, newInfo, true);
    }

    private CommandsForKeyUpdate update(int pos, TxnId plainTxnId, TxnInfo curInfo, TxnInfo newInfo)
    {
        return insertOrUpdate(pos, plainTxnId, curInfo, newInfo);
    }

    /**
     * Insert a new txnId and info
     */
    private CommandsForKeyUpdate insert(int pos, TxnId plainTxnId, TxnInfo newInfo)
    {
        return insertOrUpdate(pos, plainTxnId, null, newInfo);
    }

    // TODO (required): additional linearizability violation detection, based on expectation of presence in missing set

    /**
     * {@code removeTxnId} no longer needs to be tracked in missing arrays;
     * remove it from byId and committedByExecuteAt, ensuring both arrays still reference the same TxnInfo where updated
     */
    private static void removeFromMissingArrays(TxnInfo[] byId, TxnInfo[] committedByExecuteAt, TxnId removeTxnId)
    {
        int startIndex = SortedArrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, removeTxnId, (id, info) -> id.compareTo(info.executeAt), FAST);
        if (startIndex < 0) startIndex = -1 - startIndex;
        else ++startIndex;

        int minSearchIndex = Arrays.binarySearch(byId, removeTxnId) + 1;
        for (int i = startIndex ; i < committedByExecuteAt.length ; ++i)
        {
            int newMinSearchIndex;
            {
                TxnInfo txn = committedByExecuteAt[i];
                if (txn.getClass() == TxnInfo.class) continue;
                if (!txn.kind().witnesses(removeTxnId)) continue;

                TxnId[] missing = txn.missing();
                TxnId[] newMissing = removeOneMissing(missing, removeTxnId);
                if (missing == newMissing) continue;

                newMinSearchIndex = updateInfoArrays(i, txn, txn.update(newMissing), minSearchIndex, byId, committedByExecuteAt);
            }

            minSearchIndex = removeFromMissingArraysById(byId, minSearchIndex, newMinSearchIndex, removeTxnId);
        }

        removeFromMissingArraysById(byId, minSearchIndex, byId.length, removeTxnId);
    }

    /**
     * {@code removeTxnId} no longer needs to be tracked in missing arrays;
     * remove it from a range of byId ACCEPTED status entries only, that could not be tracked via committedByExecuteAt
     */
    private static int removeFromMissingArraysById(TxnInfo[] byId, int from, int to, TxnId removeTxnId)
    {
        for (int i = from ; i < to ; ++i)
        {
            TxnInfo txn = byId[i];
            if (txn.getClass() == TxnInfo.class) continue;
            if (!txn.status.hasExecuteAtOrDeps) continue;
            if (!txn.kind().witnesses(removeTxnId)) continue;
            if (txn.status != ACCEPTED) continue;

            TxnId[] missing = txn.missing();
            TxnId[] newMissing = removeOneMissing(missing, removeTxnId);
            if (missing == newMissing) continue;
            byId[i] = txn.update(newMissing);
        }
        return to;
    }

    /**
     * {@code insertTxnId} needs to be tracked in missing arrays;
     * add it to byId and committedByExecuteAt, ensuring both arrays still reference the same TxnInfo where updated
     * Do not insert it into any members of {@code doNotInsert} as these are known to have witnessed {@code insertTxnId}
     */
    private static void addToMissingArrays(TxnInfo[] byId, TxnInfo[] committedByExecuteAt, TxnInfo newInfo, TxnId insertTxnId, @Nonnull TxnId[] doNotInsert)
    {
        TxnId[] oneMissing = null;

        int startIndex = SortedArrays.binarySearch(committedByExecuteAt, 0, committedByExecuteAt.length, insertTxnId, (id, info) -> id.compareTo(info.executeAt), FAST);
        if (startIndex < 0) startIndex = -1 - startIndex;
        else ++startIndex;

        int minByIdSearchIndex = Arrays.binarySearch(byId, insertTxnId) + 1;
        int minDoNotInsertSearchIndex = 0;
        for (int i = startIndex ; i < committedByExecuteAt.length ; ++i)
        {
            int newMinSearchIndex;
            {
                TxnInfo txn = committedByExecuteAt[i];
                if (txn == newInfo) continue;
                if (!txn.kind().witnesses(insertTxnId)) continue;

                if (doNotInsert != NO_TXNIDS)
                {
                    if (txn.executeAt == txn)
                    {
                        int j = SortedArrays.exponentialSearch(doNotInsert, 0, doNotInsert.length, txn);
                        if (j >= 0)
                        {
                            minDoNotInsertSearchIndex = j;
                            continue;
                        }
                        minDoNotInsertSearchIndex = -1 -j;
                    }
                    else
                    {
                        if (Arrays.binarySearch(doNotInsert, txn) >= 0)
                            continue;
                    }
                }

                TxnId[] missing = txn.missing();
                if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
                else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);

                newMinSearchIndex = updateInfoArrays(i, txn, txn.update(missing), minByIdSearchIndex, byId, committedByExecuteAt);
            }

            for (; minByIdSearchIndex < newMinSearchIndex ; ++minByIdSearchIndex)
            {
                TxnInfo txn = byId[minByIdSearchIndex];
                if (txn == newInfo) continue;
                if (!txn.status.hasExecuteAtOrDeps) continue;
                if (!txn.kind().witnesses(insertTxnId)) continue;
                if (txn.status != ACCEPTED) continue;
                if (minDoNotInsertSearchIndex < doNotInsert.length && doNotInsert[minDoNotInsertSearchIndex].equals(txn))
                {
                    ++minDoNotInsertSearchIndex;
                    continue;
                }

                TxnId[] missing = txn.missing();
                if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
                else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);
                byId[minByIdSearchIndex] = txn.update(missing);
            }
        }

        for (; minByIdSearchIndex < byId.length ; ++minByIdSearchIndex)
        {
            TxnInfo txn = byId[minByIdSearchIndex];
            if (txn == newInfo) continue;
            if (!txn.status.hasExecuteAtOrDeps) continue;
            if (!txn.kind().witnesses(insertTxnId)) continue;
            if (txn.status != ACCEPTED) continue;
            if (minDoNotInsertSearchIndex < doNotInsert.length && doNotInsert[minDoNotInsertSearchIndex].equals(txn))
            {
                ++minDoNotInsertSearchIndex;
                continue;
            }

            TxnId[] missing = txn.missing();
            if (missing == NO_TXNIDS) missing = oneMissing = ensureOneMissing(insertTxnId, oneMissing);
            else missing = SortedArrays.insert(missing, insertTxnId, TxnId[]::new);
            byId[minByIdSearchIndex] = txn.update(missing);
        }
    }

    /**
     * Take an index in {@code committedByExecuteAt}, find the companion entry in {@code byId}, and update both of them.
     * Return the updated minSearchIndex used for querying {@code byId} - this will be updated only if txnId==executeAt
     */
    @Inline
    private static int updateInfoArrays(int i, TxnInfo prevTxn, TxnInfo newTxn, int minSearchIndex, TxnInfo[] byId, TxnInfo[] committedByExecuteAt)
    {
        int j;
        if (prevTxn.executeAt == prevTxn)
        {
            j = SortedArrays.exponentialSearch(byId, minSearchIndex, byId.length, prevTxn, TxnInfo::compareTo, FAST);
            minSearchIndex = 1 + j;
        }
        else
        {
            j = Arrays.binarySearch(byId, prevTxn);
        }
        Invariants.checkState(byId[j] == prevTxn);
        byId[j] = committedByExecuteAt[i] = newTxn;
        return minSearchIndex;
    }

    /**
     * If a {@code missing} contains {@code removeTxnId}, return a new array without it (or NO_TXNIDS if the only entry)
     */
    private static TxnId[] removeOneMissing(TxnId[] missing, TxnId removeTxnId)
    {
        if (missing == NO_TXNIDS) return NO_TXNIDS;

        int j = Arrays.binarySearch(missing, removeTxnId);
        if (j < 0) return missing;

        if (missing.length == 1)
            return NO_TXNIDS;

        int length = missing.length;
        TxnId[] newMissing = new TxnId[length - 1];
        System.arraycopy(missing, 0, newMissing, 0, j);
        System.arraycopy(missing, j + 1, newMissing, j, length - (1 + j));
        return newMissing;
    }

    private static TxnId[] ensureOneMissing(TxnId txnId, TxnId[] oneMissing)
    {
        return oneMissing != null ? oneMissing : new TxnId[] { txnId };
    }

    private static TxnId[] insertMissing(TxnId[] additions, int additionCount, TxnId updateTxnId, int additionInsertPos)
    {
        TxnId[] result = new TxnId[additionCount + 1];
        System.arraycopy(additions, 0, result, 0, additionInsertPos);
        System.arraycopy(additions, additionInsertPos, result, additionInsertPos + 1, additionCount - additionInsertPos);
        result[additionInsertPos] = updateTxnId;
        return result;
    }

    /**
     * Insert the contents of {@code additions} up to {@code additionCount} into {@code current}, ignoring {@code skipAddition} if not null.
     * Only insert entries that would be witnessed by {@code owner}.
     */
    private static TxnId[] mergeAndFilterMissing(TxnId owner, TxnId[] current, TxnId[] additions, int additionCount, @Nullable TxnId skipAddition)
    {
        Kinds kinds = owner.kind().witnesses();
        int additionLength = additionCount;
        for (int i = additionCount - 1 ; i >= 0 ; --i)
        {
            if (!kinds.test(additions[i].kind()))
                --additionCount;
        }

        if (additionCount == (skipAddition == null ? 0 : 1))
            return current;

        TxnId[] buffer = cachedTxnIds().get(current.length + (skipAddition == null ? additionCount : additionCount - 1));
        int i = 0, j = 0, count = 0;
        while (i < additionLength && j < current.length)
        {
            if (kinds.test(additions[i].kind()))
            {
                int c = additions[i].compareTo(current[j]);
                if (c < 0)
                {
                    TxnId addition = additions[i++];
                    if (addition != skipAddition)
                        buffer[count++] = addition;
                }
                else
                {
                    buffer[count++] = current[j++];
                }
            }
            else i++;
        }
        while (i < additionLength)
        {
            if (kinds.test(additions[i].kind()))
            {
                TxnId addition = additions[i];
                if (addition != skipAddition)
                    buffer[count++] = addition;
            }
            i++;
        }
        while (j < current.length)
        {
            buffer[count++] = current[j++];
        }
        Invariants.checkState(count == additionCount + current.length);
        return cachedTxnIds().completeAndDiscard(buffer, count);
    }

    private static int to(TxnId txnId, Timestamp depsKnownBefore, TxnId[] missingSource, int missingCount, int missingLimit)
    {
        if (depsKnownBefore == txnId) return missingCount;
        int to = Arrays.binarySearch(missingSource, 0, missingLimit, depsKnownBefore);
        if (to < 0) to = -1 - to;
        return to;
    }

    /**
     * A TxnInfo to insert alongside any dependencies we did not already know about
     */
    static class InfoWithAdditions
    {
        final TxnInfo info;
        // a cachedTxnIds() array that should be returned once done
        final TxnId[] additions;
        final int additionCount;

        InfoWithAdditions(TxnInfo info, TxnId[] additions, int additionCount)
        {
            this.info = info;
            this.additions = additions;
            this.additionCount = additionCount;
        }
    }

    private Object computeInfoAndAdditions(int insertPos, int updatePos, TxnId txnId, InternalStatus newStatus, Command command)
    {
        Invariants.checkState(newStatus.hasExecuteAtOrDeps);
        Timestamp executeAt = command.executeAt();
        if (executeAt.equals(txnId)) executeAt = txnId;
        Ballot ballot = Ballot.ZERO;
        if (newStatus.hasBallot)
            ballot = command.acceptedOrCommitted();

        Timestamp depsKnownBefore = newStatus.depsKnownBefore(txnId, executeAt);
        return computeInfoAndAdditions(insertPos, updatePos, txnId, newStatus, ballot, executeAt, depsKnownBefore, command.partialDeps().keyDeps.txnIds(key));
    }

    /**
     * We return an Object here to avoid wasting allocations; most of the time we expect a new TxnInfo to be returned,
     * but if we have transitive dependencies to insert we return an InfoWithAdditions
     */
    private Object computeInfoAndAdditions(int insertPos, int updatePos, TxnId plainTxnId, InternalStatus newStatus, Ballot ballot, Timestamp executeAt, Timestamp depsKnownBefore, SortedList<TxnId> deps)
    {
        int depsKnownBeforePos;
        if (depsKnownBefore == plainTxnId)
        {
            depsKnownBeforePos = insertPos;
        }
        else
        {
            depsKnownBeforePos = Arrays.binarySearch(byId, insertPos, byId.length, depsKnownBefore);
            Invariants.checkState(depsKnownBeforePos < 0);
            depsKnownBeforePos = -1 - depsKnownBeforePos;
        }

        TxnId[] additions = NO_TXNIDS, missing = NO_TXNIDS;
        int additionCount = 0, missingCount = 0;

        int depsIndex = deps.find(redundantBefore.shardRedundantBefore());
        if (depsIndex < 0) depsIndex = -1 - depsIndex;
        int txnIdsIndex = 0;
        while (txnIdsIndex < depsKnownBeforePos && depsIndex < deps.size())
        {
            TxnInfo t = byId[txnIdsIndex];
            TxnId d = deps.get(depsIndex);
            int c = t.compareTo(d);
            if (c == 0)
            {
                ++txnIdsIndex;
                ++depsIndex;
            }
            else if (c < 0)
            {
                // we expect to be missing ourselves
                // we also permit any transaction we have recorded as COMMITTED or later to be missing, as recovery will not need to consult our information
                if (txnIdsIndex != updatePos && t.status.compareTo(COMMITTED) < 0 && plainTxnId.kind().witnesses(t))
                {
                    if (missingCount == missing.length)
                        missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount * 2));
                    missing[missingCount++] = t.plainTxnId();
                }
                txnIdsIndex++;
            }
            else
            {
                if (additionCount >= additions.length)
                    additions = cachedTxnIds().resize(additions, additionCount, Math.max(8, additionCount * 2));

                additions[additionCount++] = d;
                depsIndex++;
            }
        }

        while (txnIdsIndex < depsKnownBeforePos)
        {
            if (txnIdsIndex != updatePos && byId[txnIdsIndex].status.compareTo(COMMITTED) < 0)
            {
                TxnId txnId = byId[txnIdsIndex].plainTxnId();
                if ((plainTxnId.kind().witnesses(txnId)))
                {
                    if (missingCount == missing.length)
                        missing = cachedTxnIds().resize(missing, missingCount, Math.max(8, missingCount * 2));
                    missing[missingCount++] = txnId;
                }
            }
            txnIdsIndex++;
        }

        while (depsIndex < deps.size())
        {
            if (additionCount >= additions.length)
                additions = cachedTxnIds().resize(additions, additionCount, Math.max(8, additionCount * 2));
            additions[additionCount++] = deps.get(depsIndex++);
        }

        TxnInfo info = TxnInfo.create(plainTxnId, newStatus, executeAt, cachedTxnIds().completeAndDiscard(missing, missingCount), ballot);
        if (additionCount == 0)
            return info;

        return new InfoWithAdditions(info, additions, additionCount);
    }

    private CommandsForKeyUpdate update(TxnInfo[] newById, int newMinUndecidedById, Object[] newLoadingPruned, TxnId plainTxnId, @Nullable TxnInfo curInfo, @Nonnull TxnInfo newInfo, boolean maybeInsertMissing)
    {
        TxnInfo updatedCommitted = newInfo.status.isCommitted() ? newInfo : null;
        TxnInfo[] newCommittedByExecuteAt = committedByExecuteAt;

        TxnId[] loadingAsPrunedFor = LoadingPruned.get(newLoadingPruned, plainTxnId, null); // we default to null to distinguish between no match, and a match with NO_TXNIDS
        if (loadingAsPrunedFor != null) newLoadingPruned = LoadingPruned.remove(newLoadingPruned, plainTxnId);
        else loadingAsPrunedFor = NO_TXNIDS;

        if (!maybeInsertMissing)
        {
            // we may have inserted some missing to the newById entries; in this case, update the copies in committedByExecuteAt
            // TODO (desired): we can make this more efficient than scanning the whole list
            int i = 0, j = 0, minSearchIndex = 0;
            while (i < byId.length)
            {
                TxnInfo cur = byId[i], next = newById[j];
                if (cur == next) { ++j; ++i; }
                else if (cur.equalsStrict(next))
                {
                    if (cur != curInfo && cur.status.isCommitted())
                    {
                        int k = SortedArrays.exponentialSearch(newCommittedByExecuteAt, minSearchIndex, newCommittedByExecuteAt.length, cur, TxnInfo::compareExecuteAt, FAST);
                        if (cur.executeAt == cur) minSearchIndex = k;
                        Invariants.checkState(newCommittedByExecuteAt[k] == cur);
                        if (committedByExecuteAt == newCommittedByExecuteAt)
                            newCommittedByExecuteAt = newCommittedByExecuteAt.clone();
                        newCommittedByExecuteAt[k] = newById[j];
                    }
                    ++i; ++j;
                }
                else ++j;
            }
        }

        int newMaxAppliedWriteByExecuteAt = maxAppliedWriteByExecuteAt;
        if (updatedCommitted != null)
        {
            Kind updatedKind = updatedCommitted.kind();
            int pos = Arrays.binarySearch(newCommittedByExecuteAt, 0, newCommittedByExecuteAt.length, updatedCommitted, TxnInfo::compareExecuteAt);
            if (pos >= 0 && newCommittedByExecuteAt[pos].equals(updatedCommitted))
            {
                if (newCommittedByExecuteAt == committedByExecuteAt)
                {
                    newCommittedByExecuteAt = newCommittedByExecuteAt.clone();
                    newCommittedByExecuteAt[pos] = updatedCommitted;
                }
                if (pos > newMaxAppliedWriteByExecuteAt && updatedCommitted.status == APPLIED)
                    newMaxAppliedWriteByExecuteAt = maybeUpdateMaxAppliedAndCheckForLinearizabilityViolations(pos, updatedKind, updatedCommitted, loadingAsPrunedFor != null);
            }
            else
            {
                if (pos >= 0) logger.error("Execution timestamp clash on key {}: {} and {} both have executeAt {}", key, updatedCommitted.plainTxnId(), newCommittedByExecuteAt[pos].plainTxnId(), updatedCommitted.executeAt);
                else pos = -1 - pos;

                TxnInfo[] newInfos = new TxnInfo[newCommittedByExecuteAt.length + 1];
                System.arraycopy(newCommittedByExecuteAt, 0, newInfos, 0, pos);
                newInfos[pos] = updatedCommitted;
                System.arraycopy(newCommittedByExecuteAt, pos, newInfos, pos + 1, newCommittedByExecuteAt.length - pos);
                newCommittedByExecuteAt = newInfos;

                if (pos <= maxAppliedWriteByExecuteAt)
                {
                    if (pos < maxAppliedWriteByExecuteAt && loadingAsPrunedFor == null)
                    {
                        for (int i = pos; i <= maxAppliedWriteByExecuteAt; ++i)
                        {
                            if (committedByExecuteAt[pos].kind().witnesses(updatedCommitted))
                                logger.error("Linearizability violation on key {}: {} is committed to execute (at {}) before {} that should witness it but has already applied (at {})", key, updatedCommitted.plainTxnId(), updatedCommitted.plainExecuteAt(), committedByExecuteAt[i].plainTxnId(), committedByExecuteAt[i].plainExecuteAt());
                        }
                    }
                    ++newMaxAppliedWriteByExecuteAt;
                }
                else if (updatedCommitted.status == APPLIED)
                {
                    newMaxAppliedWriteByExecuteAt = maybeUpdateMaxAppliedAndCheckForLinearizabilityViolations(pos, updatedKind, updatedCommitted, loadingAsPrunedFor != null);
                }

                removeFromMissingArrays(newById, newCommittedByExecuteAt, plainTxnId);
            }
        }
        else if (curInfo != null && newInfo.status == INVALID_OR_TRUNCATED)
        {
            if (curInfo.status.compareTo(COMMITTED) < 0) removeFromMissingArrays(newById, newCommittedByExecuteAt, plainTxnId);
            else if (curInfo.status != INVALID_OR_TRUNCATED)
            {
                // we can transition from COMMITTED, STABLE or APPLIED to INVALID_OR_TRUNCATED if the local command store ERASES a command
                // in this case, we not only need to update committedByExecuteAt, we also need to update any unmanaged transactions that
                // might have been waiting for this command's execution, to either wait for the preceding committed command or else to stop waiting
                int pos = Arrays.binarySearch(newCommittedByExecuteAt, 0, newCommittedByExecuteAt.length, curInfo, TxnInfo::compareExecuteAt);
                TxnInfo[] newInfos = new TxnInfo[newCommittedByExecuteAt.length - 1];
                System.arraycopy(newCommittedByExecuteAt, 0, newInfos, 0, pos);
                System.arraycopy(newCommittedByExecuteAt, pos + 1, newInfos, pos, newInfos.length - pos);
                newCommittedByExecuteAt = newInfos;
                if (pos <= newMaxAppliedWriteByExecuteAt)
                {
                    --newMaxAppliedWriteByExecuteAt;
                    while (newMaxAppliedWriteByExecuteAt >= 0 && newCommittedByExecuteAt[newMaxAppliedWriteByExecuteAt].kind() != Write)
                        --newMaxAppliedWriteByExecuteAt;
                }
            }
        }
        else if (curInfo == null && maybeInsertMissing && newInfo.status != INVALID_OR_TRUNCATED)
        {
            // TODO (desired): for consistency, move this to insertOrUpdate (without additions), while maintaining the efficiency
            addToMissingArrays(newById, newCommittedByExecuteAt, newInfo, plainTxnId, loadingAsPrunedFor);
        }

        if (paranoia() >= 2 && curInfo == null && newInfo.status.compareTo(COMMITTED) < 0)
            validateMissing(newById, NO_TXNIDS, 0, curInfo, newInfo, loadingAsPrunedFor);

        return new CommandsForKey(key, redundantBefore, prunedBefore, newLoadingPruned, newById, newCommittedByExecuteAt, newMinUndecidedById, newMaxAppliedWriteByExecuteAt, unmanageds)
               .notifyUnmanaged(curInfo, newInfo);
    }

    private int maybeUpdateMaxAppliedAndCheckForLinearizabilityViolations(int appliedPos, Kind appliedKind, TxnInfo applied, boolean wasPruned)
    {
        if (!wasPruned)
        {
            for (int i = maxAppliedWriteByExecuteAt + 1; i < appliedPos ; ++i)
            {
                if (committedByExecuteAt[i].status != APPLIED && appliedKind.witnesses(committedByExecuteAt[i]))
                    logger.error("Linearizability violation on key {}: {} is committed to execute (at {}) before {} that should witness it but has already applied (at {})", key, committedByExecuteAt[i].plainTxnId(), committedByExecuteAt[i].plainExecuteAt(), applied.plainTxnId(), applied.plainExecuteAt());
            }
        }

        return appliedKind == Kind.Write ? appliedPos : maxAppliedWriteByExecuteAt;
    }

    private CommandsForKey update(Unmanaged[] newUnmanageds)
    {
        return new CommandsForKey(key, redundantBefore, prunedBefore, loadingPruned, byId, committedByExecuteAt, minUndecidedById, maxAppliedWriteByExecuteAt, newUnmanageds);
    }

    private CommandsForKey update(Object[] newLoadingPruned)
    {
        return new CommandsForKey(key, redundantBefore, prunedBefore, newLoadingPruned, byId, committedByExecuteAt, minUndecidedById, maxAppliedWriteByExecuteAt, unmanageds);
    }

    private CommandsForKeyUpdate notifyUnmanaged(@Nullable TxnInfo curInfo, TxnInfo newInfo)
    {
        ExtraNotify notifier = null;
        Unmanaged[] unmanageds = this.unmanageds;
        {
            // notify commit uses exclusive bounds, as we use minUndecided
            Timestamp minUndecided = minUndecidedById < 0 ? Timestamp.MAX : byId[minUndecidedById];
            if (!BTree.isEmpty(loadingPruned)) minUndecided = Timestamp.min(minUndecided, BTree.<LoadingPruned>findByIndex(loadingPruned, 0));
            int end = findCommit(unmanageds, minUndecided);
            if (end > 0)
            {
                TxnId[] notifyUnmanaged = new TxnId[end];
                for (int i = 0 ; i < end ; ++i)
                    notifyUnmanaged[i] = unmanageds[i].txnId;

                unmanageds = Arrays.copyOfRange(unmanageds, end, unmanageds.length);
                notifier = new NotifyUnmanagedOfCommit(null, notifyUnmanaged);
            }
        }

        if (newInfo.status == APPLIED)
        {
            TxnInfo maxContiguousApplied = maxContiguousManagedApplied();
            if (maxContiguousApplied != null && maxContiguousApplied.compareExecuteAt(newInfo) < 0)
                maxContiguousApplied = null;

            if (maxContiguousApplied != null)
            {
                int start = findFirstApply(unmanageds);
                int end = findApply(unmanageds, start, maxContiguousApplied.executeAt);
                if (start != end)
                {
                    TxnId[] notifyNotWaiting = notifyUnmanaged(unmanageds, start, end);
                    unmanageds = removeUnmanaged(unmanageds, start, end);
                    notifier = new NotifyNotWaiting(notifier, notifyNotWaiting);
                }
            }
        }
        else if (newInfo.status == INVALID_OR_TRUNCATED && curInfo != null && curInfo.status.isCommitted())
        {
            // this is a rare edge case, but we might have unmanaged transactions waiting on this command we must re-schedule or notify
            int start = findFirstApply(unmanageds);
            int end = start;
            while (end < unmanageds.length && unmanageds[end].waitingUntil.equals(curInfo.executeAt))
                ++end;

            if (start != end)
            {
                // find committed predecessor, if any
                int predecessor = -2 - Arrays.binarySearch(committedByExecuteAt, curInfo, TxnInfo::compareExecuteAt);

                if (predecessor >= 0)
                {
                    int maxContiguousApplied = maxContiguousManagedAppliedIndex();
                    if (maxContiguousApplied >= predecessor)
                        predecessor = -1;
                }

                if (predecessor >= 0)
                {
                    Timestamp waitingUntil = committedByExecuteAt[predecessor].plainExecuteAt();
                    unmanageds = unmanageds.clone();
                    for (int i = start ; i < end ; ++i)
                        unmanageds[i] = new Unmanaged(APPLY, unmanageds[i].txnId, waitingUntil);
                }
                else
                {
                    TxnId[] notifyNotWaiting = notifyUnmanaged(unmanageds, start, end);
                    unmanageds = removeUnmanaged(unmanageds, start, end);
                    notifier = new NotifyNotWaiting(notifier, notifyNotWaiting);
                }
            }
        }

        if (notifier == null)
            return this;

        return new CommandsForKeyUpdateWithNotifier(new CommandsForKey(this, loadingPruned, unmanageds), notifier);
    }

    private static TxnId[] notifyUnmanaged(Unmanaged[] unmanageds, int start, int end)
    {
        TxnId[] notifyNotWaiting = new TxnId[end - start];
        for (int i = start ; i < end ; ++i)
        {
            Unmanaged unmanaged = unmanageds[i];
            TxnId txnId = unmanaged.txnId;
            notifyNotWaiting[i - start] = txnId;
        }
        return notifyNotWaiting;
    }

    private static Unmanaged[] removeUnmanaged(Unmanaged[] unmanageds, int start, int end)
    {
        Unmanaged[] newUnmanageds = new Unmanaged[unmanageds.length - (end - start)];
        System.arraycopy(unmanageds, 0, newUnmanageds, 0, start);
        System.arraycopy(unmanageds, end, newUnmanageds, start, unmanageds.length - end);
        return newUnmanageds;
    }

    private void notifyWaitingOnCommit(SafeCommandStore safeStore, TxnInfo uncommitted, NotifySink notifySink)
    {
        if (redundantBefore.endEpoch > uncommitted.epoch())
            notifySink.waitingOnCommit(safeStore, uncommitted, key);
    }

    private static void updateUnmanagedAsync(SafeCommandStore safeStore, TxnId txnId, Key key, NotifySink notifySink)
    {
        PreLoadContext context = PreLoadContext.contextFor(txnId, Keys.of(key), COMMANDS);
        safeStore.commandStore().execute(context, safeStore0 -> {
            SafeCommandsForKey safeCommandsForKey = safeStore0.get(key);
            CommandsForKey prev = safeCommandsForKey.current();
            Unmanaged addPending = updateUnmanaged(safeStore0, safeStore0.get(txnId), prev, notifySink);
            if (addPending != null)
            {
                Unmanaged[] newPending = SortedArrays.insert(prev.unmanageds, addPending, Unmanaged[]::new);
                safeCommandsForKey.set(new CommandsForKey(prev, prev.loadingPruned, newPending));
            }
        }).begin(safeStore.agent());
    }

    private static Unmanaged updateUnmanaged(SafeCommandStore safeStore, SafeCommand safeCommand, CommandsForKey cfk, NotifySink notifySink)
    {
        if (safeCommand.current().hasBeen(Status.Truncated))
            return null;

        Command.Committed command = safeCommand.current().asCommitted();
        TxnId waitingTxnId = command.txnId();
        Timestamp waitingExecuteAt = command.executeAt();

        SortedRelationList<TxnId> txnIds = command.partialDeps().keyDeps.txnIds(cfk.key);
        int i = txnIds.find(cfk.shardRedundantBefore());
        if (i < 0) i = -1 - i;

        if (i < txnIds.size())
        {
            boolean readyToExecute = true;
            Timestamp executesAt = null;
            int j = SortedArrays.binarySearch(cfk.byId, 0, cfk.byId.length, txnIds.get(i), Timestamp::compareTo, FAST);
            if (j < 0) j = -1 -j;
            while (i < txnIds.size() && j < cfk.byId.length)
            {
                int c = txnIds.get(i).compareTo(cfk.byId[j]);
                if (c > 0)
                {
                    ++j;
                }
                else if (c < 0)
                {
                    TxnId txnId = txnIds.get(i);
                    if (txnId.compareTo(cfk.prunedBefore) < 0 || !managesExecution(txnId)) ++i;
                    else throw illegalState("Transaction not found: " + cfk.byId[j]);
                }
                else
                {
                    TxnInfo txn = cfk.byId[j];
                    Invariants.checkState(txn.status.compareTo(COMMITTED) >= 0);
                    if (txn.status != INVALID_OR_TRUNCATED && (waitingTxnId.kind().awaitsOnlyDeps() || txn.executeAt.compareTo(waitingExecuteAt) < 0))
                    {
                        readyToExecute &= txn.status == APPLIED;
                        executesAt = Timestamp.nonNullOrMax(executesAt, txn.executeAt);
                    }
                    ++i;
                    ++j;
                }
            }

            if (!readyToExecute)
            {
                if (executesAt instanceof TxnInfo)
                    executesAt = ((TxnInfo) executesAt).plainExecuteAt();

                if (waitingTxnId.kind().awaitsOnlyDeps() && executesAt != null)
                {
                    if (executesAt.compareTo(command.waitingOn.executeAtLeast(Timestamp.NONE)) > 0)
                    {
                        Command.WaitingOn.Update waitingOn = new Command.WaitingOn.Update(command.waitingOn);
                        waitingOn.updateExecuteAtLeast(executesAt);
                        safeCommand.updateWaitingOn(waitingOn);
                    }
                }
                return new Unmanaged(APPLY, command.txnId(), executesAt);
            }
        }

        notifySink.notWaiting(safeStore, safeCommand, cfk.key);
        return null;
    }

    private static int findCommit(Unmanaged[] unmanageds, Timestamp exclusive)
    {
        return -1 - SortedArrays.exponentialSearch(unmanageds, 0, unmanageds.length, exclusive, (f, v) -> {
            if (v.pending != COMMIT) return -1;
            return f.compareTo(v.waitingUntil) > 0 ? 1 : -1;
        }, FAST);
    }

    private static int findFirstApply(Unmanaged[] unmanageds)
    {
        return -1 - SortedArrays.binarySearch(unmanageds, 0, unmanageds.length, null, (f, v) -> v.pending == COMMIT ? 1 : -1, FAST);
    }

    private static int findApply(Unmanaged[] unmanageds, int start, Timestamp inclusive)
    {
        return -1 - SortedArrays.binarySearch(unmanageds, start, unmanageds.length, inclusive, (f, v) -> {
            return f.compareTo(v.waitingUntil) >= 0 ? 1 : -1;
        }, FAST);
    }

    CommandsForKeyUpdate registerUnmanaged(SafeCommandStore safeStore, SafeCommand safeCommand)
    {
        return registerUnmanaged(safeStore, safeCommand, DefaultNotifySink.INSTANCE);
    }

    CommandsForKeyUpdate registerUnmanaged(SafeCommandStore safeStore, SafeCommand safeCommand, NotifySink notifySink)
    {
        if (safeCommand.current().hasBeen(Status.Truncated))
            return this;

        Command.Committed command = safeCommand.current().asCommitted();
        TxnId waitingTxnId = command.txnId();
        Timestamp waitingExecuteAt = command.executeAt();

        SortedRelationList<TxnId> txnIds = command.partialDeps().keyDeps.txnIds(key);
        TxnId[] missing = cachedTxnIds().get(8);
        int missingCount = 0;
        int i = txnIds.find(shardRedundantBefore());
        if (i < 0) i = -1 - i;
        if (i < txnIds.size())
        {
            boolean readyToExecute = true;
            boolean waitingToApply = true;
            Timestamp executesAt = null;
            int j = SortedArrays.binarySearch(byId, 0, byId.length, txnIds.get(i), Timestamp::compareTo, FAST);
            if (j < 0) j = -1 -j;
            while (i < txnIds.size())
            {
                int c = j == byId.length ? -1 : txnIds.get(i).compareTo(byId[j]);
                if (c == 0)
                {
                    TxnInfo txn = byId[j];
                    if (txn.status.compareTo(COMMITTED) < 0) readyToExecute = waitingToApply = false;
                    else if (txn.status != INVALID_OR_TRUNCATED && (waitingTxnId.kind().awaitsOnlyDeps() || txn.executeAt.compareTo(waitingExecuteAt) < 0))
                    {
                        readyToExecute &= txn.status == APPLIED;
                        executesAt = Timestamp.nonNullOrMax(executesAt, txn.executeAt);
                    }
                    ++i;
                    ++j;
                }
                else if (c > 0) ++j;
                else if (!managesExecution(txnIds.get(i))) ++i;
                else
                {
                    readyToExecute = waitingToApply = false;
                    if (missingCount == missing.length)
                        missing = cachedTxnIds().resize(missing, missingCount, missingCount * 2);
                    missing[missingCount++] = txnIds.get(i++);
                }
            }

            if (!readyToExecute)
            {
                TxnInfo[] newInfos = byId;
                int newMinUndecidedById = minUndecidedById;
                Object[] newLoadingPruned = loadingPruned;
                TxnId[] loadPruned = NO_TXNIDS;
                if (missingCount > 0)
                {
                    int prunedIndex = Arrays.binarySearch(missing, 0, missingCount, prunedBefore);
                    if (prunedIndex < 0) prunedIndex = -1 - prunedIndex;
                    if (prunedIndex > 0)
                    {
                        loadPruned = Arrays.copyOf(missing, prunedIndex);
                        newLoadingPruned = LoadingPruned.load(loadingPruned, loadPruned, waitingTxnId);
                    }

                    if (prunedIndex != missingCount)
                    {
                        newInfos = new TxnInfo[(missingCount - prunedIndex) + byId.length];
                        i = prunedIndex;
                        j = 0;
                        int count = 0;
                        while (i < missingCount && j < byId.length)
                        {
                            int c = missing[i].compareTo(byId[j]);
                            if (c < 0) newInfos[count++] = TxnInfo.create(missing[i++], TRANSITIVELY_KNOWN);
                            else newInfos[count++] = byId[j++];
                        }
                        while (i < missingCount)
                            newInfos[count++] = TxnInfo.create(missing[i++], TRANSITIVELY_KNOWN);
                        while (j < byId.length)
                            newInfos[count++] = byId[j++];

                        newMinUndecidedById = Arrays.binarySearch(newInfos, TxnId.nonNullOrMin(missing[prunedIndex], minUndecided()));
                    }
                }
                cachedTxnIds().discard(missing, missingCount);

                Unmanaged newPendingRecord;
                if (waitingToApply)
                {
                    if (executesAt instanceof TxnInfo)
                        executesAt = ((TxnInfo) executesAt).plainExecuteAt();

                    if (waitingTxnId.kind().awaitsOnlyDeps() && executesAt != null)
                    {
                        if (executesAt.compareTo(command.waitingOn.executeAtLeast(Timestamp.NONE)) > 0)
                        {
                            Command.WaitingOn.Update waitingOn = new Command.WaitingOn.Update(command.waitingOn);
                            waitingOn.updateExecuteAtLeast(executesAt);
                            safeCommand.updateWaitingOn(waitingOn);
                        }
                    }

                    newPendingRecord = new Unmanaged(APPLY, command.txnId(), executesAt);
                }
                else newPendingRecord = new Unmanaged(COMMIT, command.txnId(), txnIds.get(txnIds.size() - 1));
                Unmanaged[] newUnmanaged = SortedArrays.insert(unmanageds, newPendingRecord, Unmanaged[]::new);

                CommandsForKey result;
                if (newInfos == byId) result = new CommandsForKey(this, newLoadingPruned, newUnmanaged);
                else result = new CommandsForKey(key, redundantBefore, prunedBefore, newLoadingPruned, newInfos, committedByExecuteAt, newMinUndecidedById, maxAppliedWriteByExecuteAt, newUnmanaged);

                if (loadPruned == NO_TXNIDS)
                    return result;

                return new CommandsForKeyUpdateWithNotifier(result, new LoadPruned(null, loadPruned));
            }
        }

        notifySink.notWaiting(safeStore, safeCommand, key);
        return this;
    }

    void notify(SafeCommandStore safeStore, CommandsForKey prevCfk, @Nullable Command command, NotifySink notifySink)
    {
        TxnInfo minUndecided = minUndecided();
        if (minUndecided != null && !minUndecided.equals(prevCfk.minUndecided()))
            notifyWaitingOnCommit(safeStore, minUndecided, notifySink);

        if (command == null || !command.hasBeen(Status.Committed) || !managesExecution(command.txnId()))
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
        InternalStatus newStatus = newInfo.status;

        TxnInfo prevInfo = prevCfk.get(updatedTxnId);
        InternalStatus prevStatus = prevInfo == null ? TRANSITIVELY_KNOWN : prevInfo.status;

        int byExecuteAtIndex = newStatus == INVALID_OR_TRUNCATED ? -1 : checkNonNegative(Arrays.binarySearch(committedByExecuteAt, newInfo, TxnInfo::compareExecuteAt));

        Kinds mayExecuteKinds;
        int mayExecuteToIndex, mayNotExecuteBeforeIndex = 0;
        int mayExecuteAnyAtIndex = -1;
        if (prevStatus.compareTo(COMMITTED) < 0)
        {
            mayExecuteKinds = updatedTxnId.kind().witnessedBy();
            switch (newInfo.status)
            {
                default: throw new AssertionError("Unhandled InternalStatus: " + newInfo.status);
                case ACCEPTED:
                case HISTORICAL:
                case PREACCEPTED_OR_ACCEPTED_INVALIDATE:
                case TRANSITIVELY_KNOWN:
                    throw illegalState("Invalid status: command has been committed but we have InternalStatus " + newInfo.status);

                case INVALID_OR_TRUNCATED:
                case APPLIED:
                    mayExecuteToIndex = committedByExecuteAt.length;
                    break;

                case COMMITTED:
                    mayExecuteToIndex = byExecuteAtIndex;
                    break;

                case STABLE:
                    mayExecuteToIndex = byExecuteAtIndex + 1;
                    mayExecuteAnyAtIndex = byExecuteAtIndex;
                    break;
            }
        }
        else if (newStatus == APPLIED)
        {
            mayExecuteKinds = updatedTxnId.kind().witnessedBy();
            mayExecuteToIndex = committedByExecuteAt.length;
        }
        else if (newStatus == INVALID_OR_TRUNCATED && prevStatus != INVALID_OR_TRUNCATED)
        {
            // rare case of us erasing a command that has been committed, so simply try to execute the next executable thing
            mayExecuteKinds = AnyGloballyVisible;
            mayExecuteToIndex = committedByExecuteAt.length;
        }
        else
        {
            // we only permit to execute the updated transaction itself in this case, as there's no new information for the execution of other transactions
            if (newStatus != STABLE)
                return;

            mayExecuteAnyAtIndex = byExecuteAtIndex;
            mayExecuteKinds = AnyGloballyVisible;
            mayExecuteToIndex = byExecuteAtIndex + 1;
        }

        notifyManaged(safeStore, mayExecuteKinds, mayNotExecuteBeforeIndex, mayExecuteToIndex, mayExecuteAnyAtIndex, notifySink);
    }

    private void notifyManaged(SafeCommandStore safeStore, Kinds kinds, int mayNotExecuteBeforeIndex, int mayExecuteToIndex, int mayExecuteAny, NotifySink notifySink)
    {
        // TODO (now): handle loadingPruned
        Participants<?> asParticipants = null;
        int undecidedIndex = minUndecidedById < 0 ? byId.length : minUndecidedById;
        long unappliedCounters = 0L;
        TxnInfo minUndecided = minUndecided();

        for (int i = maxAppliedWriteByExecuteAt + 1; i < mayExecuteToIndex ; ++i)
        {
            TxnInfo txn = committedByExecuteAt[i];
            if (txn.status == APPLIED || !managesKeyExecution(txn))
                continue;

            Kind kind = txn.kind();
            if (i >= mayNotExecuteBeforeIndex && (kinds.test(kind) || i == mayExecuteAny) && !LoadingPruned.isWaiting(loadingPruned, txn, txn.executeAt))
            {
                switch (txn.status)
                {
                    case COMMITTED:
                    {
                        // cannot execute as dependencies not stable, so notify progress log to get or decide stable deps
                        if (asParticipants == null)
                            asParticipants = Keys.of(key).toParticipants();
                        safeStore.progressLog().waiting(txn.plainTxnId(), WaitingToExecute, null, asParticipants);
                        break;
                    }

                    case STABLE:
                    {
                        if (undecidedIndex < byId.length)
                        {
                            int nextUndecidedIndex = SortedArrays.exponentialSearch(byId, undecidedIndex, byId.length, txn.executeAt, Timestamp::compareTo, FAST);
                            if (nextUndecidedIndex < 0) nextUndecidedIndex = -1 -nextUndecidedIndex;
                            while (undecidedIndex < nextUndecidedIndex)
                            {
                                TxnInfo backfillTxn = byId[undecidedIndex++];
                                if (backfillTxn.status.compareTo(COMMITTED) >= 0) continue;
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
                        if (missingCount > 0 && minUndecided != null)
                        {
                            int missingFrom = SortedArrays.binarySearch(missing, 0, missing.length, minUndecided, TxnId::compareTo, FAST);
                            if (missingFrom < 0) missingFrom = -1 - missingFrom;
                            missingCount -= missingFrom;
                        }
                        if (expectMissingCount == missingCount)
                        {
                            TxnId txnId = txn.plainTxnId();
                            notifySink.notWaiting(safeStore, txnId, key);
                            if (asParticipants == null)
                                asParticipants = Keys.of(key).toParticipants();
                            safeStore.progressLog().waiting(txnId, WaitingToApply, null, asParticipants);
                        }
                    }
                }
            }

            unappliedCounters += unappliedCountersDelta(kind);
            if (kind == Kind.Write)
                return; // the minimum execute index occurs after the next write, so nothing to do yet
        }
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

    public CommandsForKey withRedundantBeforeAtLeast(RedundantBefore.Entry newRedundantBefore)
    {
        Invariants.checkArgument(newRedundantBefore.shardRedundantBefore().compareTo(shardRedundantBefore()) >= 0, "Expect new RedundantBefore.Entry shardAppliedOrInvalidatedBefore to be ahead of existing one");

        if (newRedundantBefore.equals(redundantBefore))
            return this;

        TxnInfo[] newInfos = byId;
        int pos = insertPos(0, newRedundantBefore.shardRedundantBefore());
        if (pos != 0)
        {
            newInfos = Arrays.copyOfRange(byId, pos, byId.length);
            for (int i = 0 ; i < newInfos.length ; ++i)
            {
                TxnInfo txn = newInfos[i];
                TxnId[] missing = txn.missing();
                if (missing == NO_TXNIDS) continue;
                missing = removeRedundantMissing(missing, newRedundantBefore.shardRedundantBefore());
                newInfos[i] = txn.update(missing);
            }
        }

        // TODO (expected): filter pending unmanageds
        return new CommandsForKey(key, newRedundantBefore, prunedBefore, loadingPruned, newInfos, unmanageds);
    }

    /**
     * Remove transitively redundant applied or invalidated commands
     * @param pruneInterval the number of committed commands we must have prior to the first prune point candidate to trigger a prune attempt
     * @param minHlcDelta do not prune any commands with an HLC within this distance of the prune point candidate
     */
    public CommandsForKey maybePrune(int pruneInterval, long minHlcDelta)
    {
        TxnInfo newPrunedBefore;
        {
            if (maxAppliedWriteByExecuteAt < pruneInterval)
                return this;

            int i = maxAppliedWriteByExecuteAt;
            long maxPruneHlc = committedByExecuteAt[i].executeAt.hlc() - minHlcDelta;
            while (--i >= 0)
            {
                TxnInfo txn = committedByExecuteAt[i];
                if (txn.kind().isWrite() && txn.executeAt.hlc() <= maxPruneHlc && txn.status == APPLIED)
                    break;
            }

            if (i < 0)
                return this;

            newPrunedBefore = committedByExecuteAt[i];
            if (newPrunedBefore.compareTo(prunedBefore) <= 0)
                return this;
        }

        int pos = insertPos(0, newPrunedBefore);
        if (pos == 0)
            return this;

        return pruneBefore(newPrunedBefore, pos);
    }

    private CommandsForKey pruneBefore(TxnInfo newPrunedBefore, int pos)
    {
        Invariants.checkArgument(newPrunedBefore.compareTo(prunedBefore) >= 0, "Expect new prunedBefore to be ahead of existing one");

        int minUndecidedById = -1;
        int retainCount = 0, removedCommittedCount = 0;
        TxnInfo[] newInfos;
        {
            for (int i = 0 ; i < pos ; ++i)
            {
                TxnInfo txn = byId[i];
                switch (txn.status)
                {
                    default: throw new AssertionError("Unhandled status: " + txn.status);
                    case COMMITTED:
                    case STABLE:
                        ++retainCount;
                        break;

                    case HISTORICAL:
                    case TRANSITIVELY_KNOWN:
                    case PREACCEPTED_OR_ACCEPTED_INVALIDATE:
                    case ACCEPTED:
                        if (minUndecidedById < 0)
                            minUndecidedById = retainCount;
                        ++retainCount;
                        break;

                    case APPLIED:
                        if (txn.executeAt.compareTo(newPrunedBefore.executeAt) < 0) ++removedCommittedCount;
                        else ++retainCount;

                    case INVALID_OR_TRUNCATED:
                        break;
                }
            }

            if (pos == retainCount)
                return this;

            int removedByIdCount = pos - retainCount;
            newInfos = new TxnInfo[byId.length - removedByIdCount];
            if (minUndecidedById < 0 && this.minUndecidedById >= 0) // must occur later, so deduct all removals
                minUndecidedById = this.minUndecidedById - removedByIdCount;
        }

        {   // copy to new byTxnId array, and update missing()
            int count = 0;
            for (int i = 0; i < pos ; ++i)
            {
                TxnInfo txn = byId[i];
                if (txn.status == INVALID_OR_TRUNCATED || (txn.status == APPLIED && txn.executeAt.compareTo(newPrunedBefore.executeAt) < 0))
                    continue;

                newInfos[count++] = txn;
            }
            System.arraycopy(byId, pos, newInfos, count, byId.length - pos);

            for (int i = 0; i < newInfos.length ; ++i)
            {
                TxnInfo txn = newInfos[i];
                TxnId[] missing = txn.missing();
                TxnId[] newMissing = pruneMissing(missing, prunedBefore, newInfos, retainCount);
                if (missing != newMissing)
                    txn = txn.update(newMissing);

                newInfos[i] = txn;
            }
        }

        int newMaxAppliedWriteByExecuteAt;
        TxnInfo[] newCommittedByExecuteAt;
        {
            newCommittedByExecuteAt = new TxnInfo[committedByExecuteAt.length - removedCommittedCount];

            int count = 0;
            int minByIdIndex = 0;
            for (TxnInfo txn : committedByExecuteAt)
            {
                if (txn.status == APPLIED && txn.compareTo(newPrunedBefore) < 0 && txn.executeAt.compareTo(newPrunedBefore.executeAt) < 0)
                    continue;

                TxnId[] missing = txn.missing();
                if (missing != NO_TXNIDS)
                {
                    // TODO (desired): can probably make this more efficient esp. for second case
                    int j = txn.executeAt == txn ? minByIdIndex = SortedArrays.exponentialSearch(newInfos, minByIdIndex, newInfos.length, txn)
                                                 : Arrays.binarySearch(newInfos, 0, newInfos.length, txn);
                    txn = newInfos[j];
                }
                else if (txn.executeAt == txn) ++minByIdIndex;

                newCommittedByExecuteAt[count++] = txn;
            }

        }
        newMaxAppliedWriteByExecuteAt = maxAppliedWriteByExecuteAt - removedCommittedCount;

        return new CommandsForKey(key, redundantBefore, newPrunedBefore, loadingPruned, newInfos, newCommittedByExecuteAt, minUndecidedById, newMaxAppliedWriteByExecuteAt, unmanageds);
    }

    private TxnId[] removeRedundantMissing(TxnId[] missing, TxnId removeBefore)
    {
        if (missing == NO_TXNIDS)
            return NO_TXNIDS;

        int j = Arrays.binarySearch(missing, removeBefore);
        if (j < 0) j = -1 - j;
        if (j <= 0) return missing;
        if (j == missing.length) return NO_TXNIDS;
        return Arrays.copyOfRange(missing, j, missing.length);
    }

    private TxnId[] pruneMissing(TxnId[] prune, TxnId removeBefore, TxnInfo[] newInfos, int maxPos)
    {
        if (prune == NO_TXNIDS)
            return NO_TXNIDS;

        int j = Arrays.binarySearch(prune, removeBefore);
        if (j < 0) j = -1 - j;
        if (j <= 0) return prune;

        TxnId[] retain = cachedTxnIds().get(j);
        int retainCount = 0;
        int minSearchIndex = 0;
        for (int i = 0 ; i < j ; ++i)
        {
            int infoIndex = Arrays.binarySearch(newInfos, minSearchIndex, maxPos, prune[i]);
            if (infoIndex >= 0) retain[retainCount++] = prune[i];
            else infoIndex = -1 - infoIndex;
            minSearchIndex = infoIndex;
        }

        TxnId[] result;
        if (retainCount == 0 && j == prune.length) result = NO_TXNIDS;
        else
        {
            result = new TxnId[retainCount + prune.length - j];
            System.arraycopy(retain, 0, result, 0, retainCount);
            System.arraycopy(prune, j, result, retainCount, prune.length - j);
        }
        cachedTxnIds().discard(retain, retainCount);
        return result;
    }

    public CommandsForKeyUpdate registerHistorical(TxnId txnId)
    {
        if (txnId.compareTo(shardRedundantBefore()) < 0)
            return this;

        int i = Arrays.binarySearch(byId, txnId);
        if (i >= 0)
        {
            if (byId[i].status.compareTo(HISTORICAL) >= 0)
                return this;
            return update(i, txnId, byId[i], TxnInfo.create(txnId, HISTORICAL, txnId, Ballot.ZERO));
        }
        else if (txnId.compareTo(prunedBefore) >= 0)
        {
            return insert(-1 - i, txnId, TxnInfo.create(txnId, HISTORICAL, txnId, Ballot.ZERO));
        }
        else
        {
            TxnId[] txnIdArray = new TxnId[] { txnId };
            Object[] newLoadingPruned = LoadingPruned.load(loadingPruned, txnIdArray, NO_TXNIDS);
            return LoadPruned.load(new TxnId[] { txnId }, update(newLoadingPruned));
        }
    }

    private int insertPos(int min, Timestamp timestamp)
    {
        int i = Arrays.binarySearch(byId, min, byId.length, timestamp);
        if (i < 0) i = -1 -i;
        return i;
    }

    public TxnId findFirst()
    {
        return byId.length > 0 ? byId[0] : null;
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
    ExtraNotify notifier()
    {
        return null;
    }

    private TxnInfo minUndecided()
    {
        return minUndecidedById < 0 ? null : byId[minUndecidedById];
    }

    private int maxContiguousManagedAppliedIndex()
    {
        int i = maxAppliedWriteByExecuteAt + 1;
        while (i < committedByExecuteAt.length)
        {
            TxnInfo txn = committedByExecuteAt[i];
            if (txn.status != APPLIED && managesKeyExecution(txn))
                break;
            ++i;
        }
        return i - 1;
    }

    private TxnInfo maxContiguousManagedApplied()
    {
        int i = maxContiguousManagedAppliedIndex();
        return i < 0 ? null : committedByExecuteAt[i];
    }

    private static class NotifyNotWaiting extends ExtraNotify
    {
        final TxnId[] notify;
        private NotifyNotWaiting(ExtraNotify prev, TxnId[] notify)
        {
            super(prev);
            this.notify = notify;
        }

        void doNotify(SafeCommandStore safeStore, Key key, NotifySink notifySink)
        {
            for (TxnId txnId : notify)
                notifySink.notWaiting(safeStore, txnId, key);
        }
    }

    private static class LoadPruned extends ExtraNotify
    {
        final TxnId[] load;
        private LoadPruned(ExtraNotify prev, TxnId[] load)
        {
            super(prev);
            this.load = load;
        }

        void doNotify(SafeCommandStore safeStore, Key key, NotifySink notifySink)
        {
            SafeCommandsForKey safeCfk = safeStore.get(key);
            for (TxnId txnId : load)
            {
                safeStore = safeStore; // make it unsafe for use in lambda
                SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
                if (safeCommand != null) load(safeStore, safeCommand, safeCfk, notifySink);
                else safeStore.commandStore().execute(PreLoadContext.contextFor(txnId, Keys.of(key), COMMANDS), safeStore0 -> {
                    load(safeStore0, safeStore0.get(txnId), safeStore0.get(key), notifySink);
                }).begin(safeStore.agent());
            }
        }

        private static void load(SafeCommandStore safeStore, SafeCommand safeCommand, SafeCommandsForKey safeCfk, NotifySink notifySink)
        {
            safeCfk.updatePruned(safeStore, safeCommand.current(), notifySink);
        }

        private static CommandsForKeyUpdate load(TxnId[] txnIds, CommandsForKeyUpdate result)
        {
            if (txnIds.length == 0)
                return result;
            return new CommandsForKeyUpdateWithNotifier(result.cfk(), new LoadPruned(result.notifier(), txnIds));
        }
    }

    private static class NotifyUnmanagedOfCommit extends ExtraNotify
    {
        final TxnId[] notify;
        private NotifyUnmanagedOfCommit(ExtraNotify prev, TxnId[] notify)
        {
            super(prev);
            this.notify = notify;
        }

        void doNotify(SafeCommandStore safeStore, Key key, NotifySink notifySink)
        {
            SafeCommandsForKey safeCfk = safeStore.get(key);
            Unmanaged[] addUnmanageds = new Unmanaged[notify.length];
            int addCount = 0;
            for (TxnId txnId : notify)
            {
                SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
                if (safeCommand != null)
                {
                    Unmanaged addUnmanaged = updateUnmanaged(safeStore, safeCommand, safeCfk.current(), notifySink);
                    if (addUnmanaged != null)
                        addUnmanageds[addCount++] = addUnmanaged;
                }
                else
                {
                    updateUnmanagedAsync(safeStore, txnId, key, notifySink);
                }
            }

            if (addCount > 0)
            {
                CommandsForKey cur = safeCfk.current();
                Arrays.sort(addUnmanageds, 0, addCount, Unmanaged::compareTo);
                Unmanaged[] newUnmanageds = SortedArrays.linearUnion(cur.unmanageds, 0, cur.unmanageds.length, addUnmanageds, 0, addCount, Unmanaged::compareTo, ArrayBuffers.uncached(Unmanaged[]::new));
                safeCfk.set(cur.update(newUnmanageds));
            }
        }
    }

}
