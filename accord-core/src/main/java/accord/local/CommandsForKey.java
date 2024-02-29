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
import java.util.Comparator;
import java.util.EnumMap;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;

import accord.impl.CommandsSummary;
import accord.local.SafeCommandStore.CommandFunction;
import accord.local.SafeCommandStore.TestDep;
import accord.local.SafeCommandStore.TestStartedAt;
import accord.local.SafeCommandStore.TestStatus;
import accord.primitives.Ballot;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn.Kind;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.utils.ArrayBuffers;
import accord.utils.Invariants;
import accord.utils.RelationMultiMap.SortedRelationList;
import accord.utils.SortedArrays;
import accord.utils.SortedList;

import static accord.local.CommandsForKey.InternalStatus.ACCEPTED;
import static accord.local.CommandsForKey.InternalStatus.APPLIED;
import static accord.local.CommandsForKey.InternalStatus.COMMITTED;
import static accord.local.CommandsForKey.InternalStatus.PREACCEPTED;
import static accord.local.CommandsForKey.InternalStatus.STABLE;
import static accord.local.CommandsForKey.InternalStatus.HISTORICAL;
import static accord.local.CommandsForKey.InternalStatus.INVALID_OR_TRUNCATED;
import static accord.local.CommandsForKey.InternalStatus.TRANSITIVELY_KNOWN;
import static accord.local.CommandsForKey.InternalStatus.from;
import static accord.local.CommandsForKey.Unmanaged.Pending.APPLY;
import static accord.local.CommandsForKey.Unmanaged.Pending.COMMIT;
import static accord.local.SafeCommandStore.TestDep.ANY_DEPS;
import static accord.local.SafeCommandStore.TestDep.WITH;
import static accord.local.SaveStatus.LocalExecution.WaitingToApply;
import static accord.local.SaveStatus.LocalExecution.WaitingToExecute;
import static accord.primitives.Txn.Kind.Kinds.AnyGloballyVisible;
import static accord.utils.ArrayBuffers.cachedTxnIds;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.isParanoid;
import static accord.utils.SortedArrays.Search.CEIL;
import static accord.utils.SortedArrays.Search.FAST;

/**
 * A specialised collection for efficiently representing and querying everything we need for making coordination
 * and recovery decisions about a key's command conflicts.
 *
 * Every command we know about that is not shard-redundant is listed in the TxnId[] collection, which is sorted by TxnId.
 * This list implies the contents of the deps of all commands in the collection - it is assumed that in the normal course
 * of events every transaction will include the full set of TxnId we know. We only encode divergences from this, stored
 * in each command's {@code missing} collection.
 *
 * We then go one step further, exploiting the fact that the missing collection exists solely to implement recovery,
 * and so we elide from this missing collection any TxnId we have recorded as Committed or higher.
 * Any recovery coordinator that contacts this replica will report that the command has been agreed to execute,
 * and so will not need to decipher any fast-path decisions. So the missing collection is redundant, as no command's deps
 * will not be queried for this TxnId's presence/absence.
 * TODO (expected) this logic applies equally well to Accepted
 *
 * The goal with these behaviours is that this missing collection will ordinarily be empty, and represented by the exact
 * same NO_TXNIDS array instance as every other command.
 *
 * We also exploit the property that most commands will also agree to execute at their proposed TxnId. If we have
 * no missing collection to encode, and no modified executeAt, we store a global NoInfo object that takes up no
 * space on heap. These NoInfo objects permit further efficiencies, as we may perform class-pointer comparisons
 * before querying any contents to skip uninteresting contents, permitting fast iteration of the collection's contents.
 *
 * We also impose the condition that every TxnId is uniquely represented in the collection, so any executeAt and missing
 * collection that represents the same value as the TxnId must be the same object present in the main TxnId[].
 *
 * This collection also implements transitive dependency elision.
 * When evaluating mapReduceActive, we first establish the last-executing Stable write command (i.e. those whose deps
 * are considered durably decided, and so must wait for all commands Committed with a lower executeAt).
 * We then elide any Committed command that has a lower executeAt than this command.
 *
 * Both commands must be known at a majority, but neither might be Committed at any other replica.
 * Either command may therefore be recovered.
 * If the later command is recovered, this replica will report its Stable deps thereby recovering them.
 * If this replica is not contacted, some other replica must participate that either has taken the same action as this replica,
 * or else does not know the later command is Stable, and so will report the earlier command as a dependency again.
 * If the earlier command is recovered, this replica will report that it is Committed, and so will not consult
 * this replica's collection to decipher any fast path decision. Any other replica must either do the same, or else
 * will correctly record this transaction as present in any relevant deps of later transactions.
 *
 * TODO (expected): optimisations:
 *    3) consider storing a prefix of TxnId that are all NoInfo PreApplied encoded as a BitStream as only required for computing missing collection
 *    4) consider storing (or caching) an int[] of records with an executeAt that occurs out of order, sorted by executeAt
 *
 * TODO (expected): maintain separate redundantBefore and closedBefore timestamps, latter implied by any exclusivesyncpoint;
 *                  advance former based on Applied status of all TxnId before the latter
 * TODO (expected): track whether a TxnId is a write on this key only for execution (rather than globally)
 * TODO (expected): merge with TimestampsForKey
 * TODO (expected): save bytes by encoding InternalStatus in TxnId.flags()
 * TODO (expected): migrate to BTree
 * TODO (expected): remove a command that is committed to not intersect with the key for this store (i.e. if accepted in a later epoch than committed on, so ownership changes)
 * TODO (expected): mark a command as notified once ready-to-execute or applying
 * TODO (required): randomised testing
 * TODO (required): linearizability violation detection
 * TODO (required): account for whether transactions should witness each other for determining the missing collection
 * TODO (required): enforce that a transaction is not added with a key dependency on a SyncPoint or ExclusiveSyncPoint;
 *                  also impose this invariant elsewhere, and create a compile-time dependency between these code locations
 */
public class CommandsForKey implements CommandsSummary
{
    private static final boolean PRUNE_TRANSITIVE_DEPENDENCIES = true;
    public static final RedundantBefore.Entry NO_REDUNDANT_BEFORE = new RedundantBefore.Entry(null, Long.MIN_VALUE, Long.MAX_VALUE, TxnId.NONE, TxnId.NONE, TxnId.NONE, null);
    public static final TxnId[] NO_TXNIDS = new TxnId[0];
    public static final TxnInfo[] NO_INFOS = new TxnInfo[0];
    public static final Unmanaged[] NO_PENDING_UNMANAGED = new Unmanaged[0];

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
            if (this.pending != that.pending) return this.pending == COMMIT ? -1 : 1;
            int c = this.waitingUntil.compareTo(that.waitingUntil);
            if (c == 0) c = this.txnId.compareTo(that.txnId);
            return c;
        }

        @Override
        public String toString()
        {
            return "Pending{" + txnId + " until:" + waitingUntil + " " + pending + "}";
        }
    }

    public static class SerializerSupport
    {
        public static CommandsForKey create(Key key, TxnInfo[] txns, Unmanaged[] unmanageds)
        {
            return new CommandsForKey(key, NO_REDUNDANT_BEFORE, txns, unmanageds);
        }
    }

    public enum InternalStatus
    {
        TRANSITIVELY_KNOWN(false, false), // (unwitnessed; no need for mapReduce to witness)
        HISTORICAL(false, false),
        PREACCEPTED(false),
        ACCEPTED(true),
        COMMITTED(true),
        STABLE(true),
        APPLIED(true),
        INVALID_OR_TRUNCATED(false);

        static final EnumMap<SaveStatus, InternalStatus> convert = new EnumMap<>(SaveStatus.class);
        static final InternalStatus[] VALUES = values();
        static
        {
            convert.put(SaveStatus.PreAccepted, PREACCEPTED);
            convert.put(SaveStatus.AcceptedInvalidateWithDefinition, PREACCEPTED);
            convert.put(SaveStatus.Accepted, ACCEPTED);
            convert.put(SaveStatus.AcceptedWithDefinition, ACCEPTED);
            convert.put(SaveStatus.PreCommittedWithDefinition, PREACCEPTED);
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
        // TODO (desired): cleanup expectation logic re: accepted invalidate
        final InternalStatus expectMatch;

        InternalStatus(boolean hasInfo)
        {
            this(hasInfo, true);
        }

        InternalStatus(boolean hasInfo, boolean expectMatch)
        {
            this.hasInfo = hasInfo;
            this.expectMatch = expectMatch ? this : null;
        }

        boolean hasExecuteAt()
        {
            return hasInfo;
        }

        boolean hasDeps()
        {
            return hasInfo;
        }

        boolean hasStableDeps()
        {
            return this == STABLE || this == APPLIED;
        }

        public Timestamp depsKnownBefore(TxnId txnId, @Nullable Timestamp executeAt)
        {
            switch (this)
            {
                default: throw new AssertionError("Unhandled InternalStatus: " + this);
                case TRANSITIVELY_KNOWN:
                case INVALID_OR_TRUNCATED:
                case HISTORICAL:
                    throw new AssertionError("Invalid InternalStatus to know deps");

                case PREACCEPTED:
                case ACCEPTED:
                    return txnId;

                case APPLIED:
                case STABLE:
                case COMMITTED:
                    return executeAt == null ? txnId : executeAt;
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

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, @Nonnull Timestamp executeAt)
        {
            Invariants.checkState(executeAt == txnId || !executeAt.equals(txnId));
            return new TxnInfo(txnId, status, executeAt);
        }

        public static TxnInfo create(@Nonnull TxnId txnId, InternalStatus status, @Nonnull Timestamp executeAt, @Nonnull TxnId[] missing)
        {
            Invariants.checkState(executeAt == txnId || !executeAt.equals(txnId));
            if (missing == NO_TXNIDS) return new TxnInfo(txnId, status, executeAt);
            return new TxnInfoWithMissing(txnId, status, executeAt, missing);
        }

        public static TxnInfo createMock(TxnId txnId, InternalStatus status, @Nullable Timestamp executeAt, @Nullable TxnId[] missing)
        {
            Invariants.checkState(executeAt == null || executeAt == txnId || !executeAt.equals(txnId));
            Invariants.checkArgument(missing == null || missing == NO_TXNIDS);
            if (missing == NO_TXNIDS) return new TxnInfo(txnId, status, executeAt);
            return new TxnInfoWithMissing(txnId, status, executeAt, missing);
        }

        Timestamp depsKnownBefore()
        {
            return status.depsKnownBefore(this, executeAt);
        }

        public TxnInfo update(TxnId[] newMissing)
        {
            return newMissing == NO_TXNIDS ? new TxnInfo(this, status, executeAt)
                                           : new TxnInfoWithMissing(this, status, executeAt, newMissing);
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

        TxnId asPlainTxnId()
        {
            return new TxnId(this);
        }

        public TxnId[] missing()
        {
            return NO_TXNIDS;
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
                   ", executeAt=" + (executeAt == this ? toPlainString() : executeAt.toString()) +
                   '}';
        }

        public String toPlainString()
        {
            return super.toString();
        }
    }

    public static class TxnInfoWithMissing extends TxnInfo
    {
        public final TxnId[] missing;

        TxnInfoWithMissing(TxnId txnId, InternalStatus status, Timestamp executeAt, TxnId[] missing)
        {
            super(txnId, status, executeAt);
            this.missing = missing;
        }

        public TxnId[] missing()
        {
            return missing;
        }

        @Override
        public String toString()
        {
            return "Info{" +
                   "txnId=" + toPlainString() +
                   ", status=" + status +
                   ", executeAt=" + (this == executeAt ? toPlainString() : executeAt) +
                   ", missing=" + Arrays.toString(missing) +
                   '}';
        }
    }

    private final Key key;
    private final RedundantBefore.Entry redundantBefore;
    // all transactions, sorted by TxnId
    private final TxnInfo[] txns;
    // next and nextWrite are only non-null if they and their dependencies are all committed
    private final TxnInfo minUncommitted, next, nextWrite;
    // transactions that are committed or stable plus MAYBE Applied, keyed by executeAt
    private final TxnInfo[] committed;
    private final Unmanaged[] unmanageds;

    CommandsForKey(Key key, RedundantBefore.Entry redundantBefore, TxnInfo[] txns, Unmanaged[] unmanageds)
    {
        this.key = key;
        this.redundantBefore = Invariants.nonNull(redundantBefore);
        this.txns = txns;
        this.unmanageds = unmanageds;
        if (isParanoid()) Invariants.checkArgument(SortedArrays.isSortedUnique(txns));

        // TODO (required): update these efficiently
        int countCommitted = 0;
        TxnInfo minUncommitted = null, nextWrite = null, next = null;
        for (int i = 0 ; i < txns.length ; ++i)
        {
            TxnInfo txn = txns[i];
            if (txn.status == INVALID_OR_TRUNCATED) continue;

            if (txn.status.compareTo(COMMITTED) >= 0)
            {
                if (txn.status.compareTo(APPLIED) < 0)
                {
                    if (txn.kind().isWrite() && (nextWrite == null || nextWrite.executeAt.compareTo(txn.executeAt) > 0))
                        nextWrite = txn;
                    if (next == null || next.executeAt.compareTo(txn.executeAt) > 0)
                        next = txn;
                }
                ++countCommitted;
            }
            else if (minUncommitted == null) minUncommitted = txn;
        }
        if (minUncommitted != null)
        {
            // TODO (expected): discount minUncommitted if not a dependency of next/nextWrite
            if (next != null && minUncommitted.compareTo(next.executeAt) < 0)
                next = null;
            if (nextWrite != null && minUncommitted.compareTo(nextWrite.executeAt) < 0)
                nextWrite = null;
        }
        this.minUncommitted = minUncommitted;
        this.nextWrite = nextWrite;
        this.next = next;
        this.committed = new TxnInfo[countCommitted];
        countCommitted = 0;
        for (int i = 0 ; i < txns.length ; ++i)
        {
            if (txns[i].status.compareTo(COMMITTED) >= 0 && txns[i].status != INVALID_OR_TRUNCATED)
                committed[countCommitted++] = txns[i];
        }
        Arrays.sort(committed, Comparator.comparing(c -> c.executeAt));
    }

    public CommandsForKey(Key key)
    {
        this.key = key;
        this.redundantBefore = NO_REDUNDANT_BEFORE;
        this.txns = NO_INFOS;
        this.committed = NO_INFOS;
        this.minUncommitted = null;
        this.nextWrite = null;
        this.next = null;
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
        return txns.length;
    }

    public int unmanagedCount()
    {
        return unmanageds.length;
    }

    public int indexOf(TxnId txnId)
    {
        return Arrays.binarySearch(txns, txnId);
    }

    public TxnId txnId(int i)
    {
        return txns[i];
    }

    public TxnInfo get(int i)
    {
        return txns[i];
    }

    public Unmanaged getUnmanaged(int i)
    {
        return unmanageds[i];
    }

    public TxnInfo get(TxnId txnId)
    {
        int i = indexOf(txnId);
        return i < 0 ? null : txns[i];
    }

    public RedundantBefore.Entry redundantBefore()
    {
        return redundantBefore;
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
     * All commands before/after (exclusive of) the given timestamp
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
        int start, end;
        boolean isKnown;
        {
            int insertPos = Arrays.binarySearch(txns, testTxnId);
            isKnown = insertPos >= 0;
            if (!isKnown && testDep == WITH) return initialValue;
            if (!isKnown) insertPos = -1 - insertPos;
            switch (testStartedAt)
            {
                default: throw new AssertionError("Unhandled TestStartedAt: " + testTxnId);
                case STARTED_BEFORE: start = 0; end = insertPos; break;
                case STARTED_AFTER: start = insertPos; end = txns.length; break;
                case ANY: start = 0; end = txns.length;
            }
        }

        for (int i = start; i < end ; ++i)
        {
            TxnInfo txn = txns[i];
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
                if (!status.hasInfo)
                    continue;

                if (executeAt.compareTo(testTxnId) <= 0)
                    continue;

                boolean hasAsDep = Arrays.binarySearch(txn.missing(), testTxnId) < 0;
                if (hasAsDep != (testDep == WITH))
                    continue;
            }

            initialValue = map.apply(p1, key, txn.asPlainTxnId(), executeAt, initialValue);
        }
        return initialValue;
    }

    public <P1, T> T mapReduceActive(Timestamp startedBefore,
                                     Kinds testKind,
                                     CommandFunction<P1, T, T> map, P1 p1, T initialValue)
    {
        Timestamp maxCommittedBefore;
        {
            int i = SortedArrays.binarySearch(committed, 0, committed.length, startedBefore, (f, v) -> f.compareTo(v.executeAt), FAST);
            if (i < 0) i = -2 - i;
            else --i;
            while (i >= 0 && !committed[i].kind().isWrite()) --i;
            maxCommittedBefore = i < 0 ? null : committed[i].executeAt;
        }
        int start = 0, end = insertPos(start, startedBefore);

        for (int i = start; i < end ; ++i)
        {
            TxnInfo txn = txns[i];
            if (!testKind.test(txn.kind()))
                continue;

            switch (txn.status)
            {
                case COMMITTED:
                case STABLE:
                case APPLIED:
                    // TODO (expected): prove the correctness of this approach
                    if (!PRUNE_TRANSITIVE_DEPENDENCIES || maxCommittedBefore == null || txn.executeAt.compareTo(maxCommittedBefore) >= 0)
                        break;
                case TRANSITIVELY_KNOWN:
                case INVALID_OR_TRUNCATED:
                    continue;
            }

            initialValue = map.apply(p1, key, txn.asPlainTxnId(), txn.executeAt, initialValue);
        }
        return initialValue;
    }

    public CommandsForKey update(Command prev, Command next)
    {
        return update(prev, next, true);
    }

    CommandsForKey update(Command prev, Command next, boolean hasPrev)
    {
        InternalStatus newStatus = InternalStatus.from(next.saveStatus());
        if (newStatus == null)
            return this;

        TxnId txnId = next.txnId();
        int pos = Arrays.binarySearch(txns, txnId);
        CommandsForKey result;
        if (pos < 0)
        {
            pos = -1 - pos;
            if (newStatus.hasInfo) result = insert(pos, txnId, newStatus, next);
            else result = insert(pos, txnId, TxnInfo.create(txnId, newStatus, txnId));
        }
        else
        {
            // update
            TxnInfo cur = txns[pos];
            if (hasPrev)
            {
                if (newStatus.compareTo(cur.status) <= 0)
                {
                    // we can redundantly update the same transaction via notifyWaitingOnCommit since updates to CFK may be asynchronous
                    // (particularly for invalidations). So we should expect that we might already represent the latest information for this transaction.
                    // TODO (desired): consider only accepting this for Invalidation
                    // TODO (desired): also clean-up special casing for AcceptedInvalidate, which exists because it currently has no effect on the CFK state
                    //    so it could be any of Transitively Known, Historic, PreAccept or Accept
                    Invariants.checkState(cur.status == newStatus || next.status() == Status.AcceptedInvalidate);
                    if (!newStatus.hasInfo || next.acceptedOrCommitted().equals(prev.acceptedOrCommitted()))
                        return this;
                }

                InternalStatus prevStatus = prev == null ? null : InternalStatus.from(prev.saveStatus());
                Invariants.checkState(cur.status.expectMatch == prevStatus || cur.status.expectMatch == null && !prev.keysOrRanges().contains(key) || (prev != null && prev.status() == Status.AcceptedInvalidate));
            }
            else if (newStatus.compareTo(cur.status) <= 0)
            {
                Invariants.checkState(cur.status == newStatus || next.status() == Status.AcceptedInvalidate);
                if (!newStatus.hasInfo)
                    return this;
            }

            if (newStatus.hasInfo) result = update(pos, txnId, cur, newStatus, next);
            else result = update(pos, txnId, cur, TxnInfo.create(txnId, newStatus, txnId));
        }

        return result;
    }

    public static boolean needsUpdate(Command prev, Command updated)
    {
        if (!updated.txnId().kind().isGloballyVisible())
            return false;

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
        return updated != prev || (updated != null && updated.hasInfo && !prevAcceptedOrCommitted.equals(updatedAcceptedOrCommitted));
    }

    private CommandsForKey insert(int insertPos, TxnId insertTxnId, InternalStatus newStatus, Command command)
    {
        Object newInfo = computeInsert(insertPos, insertTxnId, newStatus, command);
        if (newInfo.getClass() != InfoWithAdditions.class)
            return insert(insertPos, insertTxnId, (TxnInfo)newInfo);

        InfoWithAdditions newInfoWithAdditions = (InfoWithAdditions) newInfo;
        TxnInfo[] newTxns = new TxnInfo[txns.length + newInfoWithAdditions.additionCount + 1];
        insertWithAdditions(insertPos, insertTxnId, newInfoWithAdditions, newTxns);
        return update(newTxns);
    }

    private CommandsForKey update(int updatePos, TxnId plainTxnId, TxnInfo prevInfo, InternalStatus newStatus, Command command)
    {
        Object newInfo = computeUpdate(updatePos, plainTxnId, newStatus, command);
        if (newInfo.getClass() != InfoWithAdditions.class)
            return update(updatePos, plainTxnId, prevInfo, (TxnInfo)newInfo);

        InfoWithAdditions newInfoWithAdditions = (InfoWithAdditions) newInfo;
        TxnInfo[] newTxns = new TxnInfo[txns.length + newInfoWithAdditions.additionCount];
        updateWithAdditions(updatePos, plainTxnId, newInfoWithAdditions, newTxns);
        if (prevInfo.status.compareTo(COMMITTED) < 0 && newStatus.compareTo(COMMITTED) >= 0)
            removeMissing(newTxns, plainTxnId);
        return update(newTxns);
    }

    private void updateWithAdditions(int updatePos, TxnId updateTxnId, InfoWithAdditions withInfo, TxnInfo[] newInfos)
    {
        updateOrInsertWithAdditions(updatePos, updatePos, updateTxnId, withInfo, newInfos);
    }

    private void insertWithAdditions(int pos, TxnId updateTxnId, InfoWithAdditions withInfo, TxnInfo[] newInfos)
    {
        updateOrInsertWithAdditions(pos, -1, updateTxnId, withInfo, newInfos);
    }

    private void updateOrInsertWithAdditions(int sourceInsertPos, int sourceUpdatePos, TxnId updatePlainTxnId, InfoWithAdditions withInfo, TxnInfo[] newTxns)
    {
        TxnId[] additions = withInfo.additions;
        int additionCount = withInfo.additionCount;
        int additionInsertPos = Arrays.binarySearch(additions, 0, additionCount, updatePlainTxnId);
        additionInsertPos = Invariants.checkArgument(-1 - additionInsertPos, additionInsertPos < 0);
        int targetInsertPos = sourceInsertPos + additionInsertPos;

        // additions plus the updateTxnId when necessary
        TxnId[] missingSource = additions;
        boolean insertSelfMissing = sourceUpdatePos < 0 && withInfo.info.status.compareTo(COMMITTED) < 0;

        // the most recently constructed pure insert missing array, so that it may be reused if possible
        int i = 0, j = 0, missingCount = 0, missingLimit = additionCount, count = 0;
        while (i < txns.length)
        {
            if (count == targetInsertPos)
            {
                newTxns[count] = withInfo.info;
                if (i == sourceUpdatePos) ++i;
                else if (insertSelfMissing) ++missingCount;
                ++count;
                continue;
            }

            int c = j == additionCount ? -1 : txns[i].compareTo(additions[j]);
            if (c < 0)
            {
                TxnInfo txn = txns[i];
                if (i == sourceUpdatePos)
                {
                    txn = withInfo.info;
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
                    if (to > 0)
                    {
                        TxnId[] prevMissing = txn.missing();
                        TxnId[] newMissing = mergeAndFilterMissing(txn, prevMissing, missingSource, to);
                        if (newMissing != prevMissing)
                            txn = txn.update(newMissing);
                    }
                }
                newTxns[count] = txn;
                i++;
            }
            else if (c > 0)
            {
                TxnId txnId = additions[j++];
                newTxns[count] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId);
                ++missingCount;
            }
            else
            {
                throw illegalState(txns[i] + " should be an insertion, but found match when merging with origin");
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
                    newTxns[count++] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId);
                }
                newTxns[targetInsertPos] = withInfo.info;
                count = targetInsertPos + 1;
            }
            while (j < additionCount)
            {
                TxnId txnId = additions[j++];
                newTxns[count++] = TxnInfo.create(txnId, TRANSITIVELY_KNOWN, txnId);
            }
        }
        else if (count == targetInsertPos)
        {
            newTxns[targetInsertPos] = withInfo.info;
        }

        cachedTxnIds().forceDiscard(additions, additionCount);
    }

    private CommandsForKey update(int pos, TxnId plainTxnId, TxnInfo curInfo, TxnInfo newInfo)
    {
        if (curInfo == newInfo)
            return this;

        TxnInfo[] newTxns = txns.clone();
        newTxns[pos] = newInfo;
        if (curInfo.status.compareTo(COMMITTED) < 0 && newInfo.status.compareTo(COMMITTED) >= 0)
            removeMissing(newTxns, plainTxnId);
        return update(newTxns);
    }

    /**
     * Insert a new txnId and info
     */
    private CommandsForKey insert(int pos, TxnId insertPlainTxnId, TxnInfo insert)
    {
        TxnInfo[] newTxns = new TxnInfo[txns.length + 1];
        if (insert.status.compareTo(COMMITTED) >= 0)
        {
            System.arraycopy(txns, 0, newTxns, 0, pos);
            newTxns[pos] = insert;
            System.arraycopy(txns, pos, newTxns, pos + 1, txns.length - pos);
        }
        else
        {
            insertInfoAndOneMissing(pos, insertPlainTxnId, insert, txns, newTxns, 1);
        }
        return update(newTxns);
    }

    /**
     * Insert a new txnId and info, then insert the txnId into the missing collection of any command that should have already caused us to witness it
     */
    private static void insertInfoAndOneMissing(int insertPos, TxnId insertedPlainTxnId, TxnInfo inserted, TxnInfo[] oldTxns, TxnInfo[] newTxns, int offsetAfterInsertPos)
    {
        TxnId[] oneMissing = null;
        for (int i = 0 ; i < insertPos ; ++i)
        {
            TxnInfo txn = oldTxns[i];
            if (txn.status.hasDeps())
            {
                Timestamp depsKnownBefore = txn.depsKnownBefore();
                if (depsKnownBefore != null && depsKnownBefore.compareTo(inserted) > 0 && txn.kind().witnesses(inserted))
                {
                    TxnId[] missing = txn.missing();
                    if (missing == NO_TXNIDS)
                        missing = oneMissing = ensureOneMissing(insertedPlainTxnId, oneMissing);
                    else
                        missing = SortedArrays.insert(missing, insertedPlainTxnId, TxnId[]::new);

                    txn = txn.update(missing);
                }
            }
            newTxns[i] = txn;
        }

        newTxns[insertPos] = inserted;

        for (int i = insertPos; i < oldTxns.length ; ++i)
        {
            int newIndex = i + offsetAfterInsertPos;
            TxnInfo txn = oldTxns[i];
            if (txn.status.hasDeps())
            {
                if (txn.kind().witnesses(inserted))
                {
                    TxnId[] missing = txn.missing();
                    if (missing == NO_TXNIDS)
                        missing = oneMissing = ensureOneMissing(insertedPlainTxnId, oneMissing);
                    else
                        missing = SortedArrays.insert(missing, insertedPlainTxnId, TxnId[]::new);

                    txn = txn.update(missing);
                }
            }

            newTxns[newIndex] = txn;
        }
    }

    private static void removeMissing(TxnInfo[] txns, TxnId removeTxnId)
    {
        for (int i = 0 ; i < txns.length ; ++i)
        {
            TxnInfo txn = txns[i];
            TxnId[] missing = txn.missing();
            if (missing == NO_TXNIDS) continue;

            int j = Arrays.binarySearch(missing, removeTxnId);
            if (j < 0) continue;

            if (missing.length == 1)
            {
                missing = NO_TXNIDS;
            }
            else
            {
                int length = missing.length;
                TxnId[] newMissing = new TxnId[length - 1];
                System.arraycopy(missing, 0, newMissing, 0, j);
                System.arraycopy(missing, j + 1, newMissing, j, length - (1 + j));
                missing = newMissing;
            }

            txns[i] = txn.update(missing);
        }
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

    private static TxnId[] mergeAndFilterMissing(TxnId owner, TxnId[] current, TxnId[] additions, int additionCount)
    {
        Kinds kinds = owner.kind().witnesses();
        int additionLength = additionCount;
        for (int i = additionCount - 1 ; i >= 0 ; --i)
        {
            if (!kinds.test(additions[i].kind()))
                --additionCount;
        }

        if (additionCount == 0)
            return current;

        TxnId[] buffer = cachedTxnIds().get(current.length + additionCount);
        int i = 0, j = 0, count = 0;
        while (i < additionLength && j < current.length)
        {
            if (kinds.test(additions[i].kind()))
            {
                int c = additions[i].compareTo(current[j]);
                if (c < 0) buffer[count++] = additions[i++];
                else buffer[count++] = current[j++];
            }
            else i++;
        }
        while (i < additionLength)
        {
            if (kinds.test(additions[i].kind()))
                buffer[count++] = additions[i];
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

    static class InfoWithAdditions
    {
        final TxnInfo info;
        final TxnId[] additions;
        final int additionCount;

        InfoWithAdditions(TxnInfo info, TxnId[] additions, int additionCount)
        {
            this.info = info;
            this.additions = additions;
            this.additionCount = additionCount;
        }
    }

    private Object computeInsert(int insertPos, TxnId txnId, InternalStatus newStatus, Command command)
    {
        return computeInfoAndAdditions(insertPos, -1, txnId, newStatus, command);
    }

    private Object computeUpdate(int updatePos, TxnId txnId, InternalStatus newStatus, Command command)
    {
        return computeInfoAndAdditions(updatePos, updatePos, txnId, newStatus, command);
    }

    private Object computeInfoAndAdditions(int insertPos, int updatePos, TxnId txnId, InternalStatus newStatus, Command command)
    {
        Timestamp executeAt = txnId;
        if (newStatus.hasInfo)
        {
            executeAt = command.executeAt();
            if (executeAt.equals(txnId)) executeAt = txnId;
        }
        Timestamp depsKnownBefore = newStatus.depsKnownBefore(txnId, executeAt);
        return computeInfoAndAdditions(insertPos, updatePos, txnId, newStatus, executeAt, depsKnownBefore, command.partialDeps().keyDeps.txnIds(key));
    }

    private Object computeInfoAndAdditions(int insertPos, int updatePos, TxnId plainTxnId, InternalStatus newStatus, Timestamp executeAt, Timestamp depsKnownBefore, SortedList<TxnId> deps)
    {
        int depsKnownBeforePos;
        if (depsKnownBefore == plainTxnId)
        {
            depsKnownBeforePos = insertPos;
        }
        else
        {
            depsKnownBeforePos = Arrays.binarySearch(txns, insertPos, txns.length, depsKnownBefore);
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
            TxnInfo t = txns[txnIdsIndex];
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
                    missing[missingCount++] = t.asPlainTxnId();
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
            if (txnIdsIndex != updatePos && txns[txnIdsIndex].status.compareTo(COMMITTED) < 0)
            {
                TxnId txnId = txns[txnIdsIndex].asPlainTxnId();
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

        TxnInfo info = TxnInfo.create(plainTxnId, newStatus, executeAt, cachedTxnIds().completeAndDiscard(missing, missingCount));
        if (additionCount == 0)
            return info;

        return new InfoWithAdditions(info, additions, additionCount);
    }

    private CommandsForKey update(TxnInfo[] newTxns)
    {
        return new CommandsForKey(key, redundantBefore, newTxns, unmanageds);
    }

    // directly notify the progress log of any command we're now awaiting the coordination of (as the lowest TxnId not committed)
    public CommandsForKey notifyAndUpdatePending(SafeCommandStore safeStore, Command command, CommandsForKey prevCfk)
    {
        return notifyAndUpdatePending(safeStore, command.txnId(), from(command.saveStatus()), command.executeAt(), prevCfk);
    }

    public CommandsForKey notifyAndUpdatePending(SafeCommandStore safeStore, TxnId updatedTxnId, InternalStatus newStatus, Timestamp newExecuteAt, CommandsForKey prevCfk)
    {
        TxnInfo prevInfo = prevCfk.get(updatedTxnId);

        switch (newStatus)
        {
            default: throw new AssertionError("Unhandled InternalStatus: " + newStatus);
            case TRANSITIVELY_KNOWN:
            case PREACCEPTED:
            case HISTORICAL:
            case ACCEPTED:
                break;

            case STABLE:
            case COMMITTED:
                int compareWithNext = nextWrite == null ? -1 : newExecuteAt.compareTo(nextWrite.executeAt);
                if (compareWithNext <= 0)
                {
                    if (newStatus == COMMITTED)
                        break;

                    // the updated transaction itself might be executable
                    notify(safeStore, AnyGloballyVisible, next == null ? Timestamp.NONE : next.executeAt, newExecuteAt);
                }
                else
                {
                    // somebody waiting on us might now be ready to execute, but only if we execute after them and were not previously committed, but WERE previously known
                    // if the next command executes after us, or executes before we were started
                    // then our change in status cannot have triggered any change in immediate execution dependencies
                    if (prevInfo == null || prevInfo.status == COMMITTED || nextWrite.executeAt.compareTo(updatedTxnId) < 0 || newExecuteAt.equals(updatedTxnId))
                        break;

                    notify(safeStore, updatedTxnId.kind().witnessedBy(), next.executeAt, nextWrite.executeAt);
                }
                break;

            case APPLIED:
            case INVALID_OR_TRUNCATED:
                if (next != null)
                    notify(safeStore, updatedTxnId.kind().witnessedBy(), next.executeAt, nextWrite != null ? nextWrite.executeAt : Timestamp.MAX);
                break;
        }

        Unmanaged[] pending = this.unmanageds;
        if (minUncommitted != null && (prevCfk.minUncommitted == null || !prevCfk.minUncommitted.equals(minUncommitted)))
            notifyWaitingOnCommit(safeStore, minUncommitted);
        if (newStatus.compareTo(COMMITTED) >= 0 && (prevInfo == null || prevInfo.status.compareTo(COMMITTED) < 0))
            pending = notifyUnmanaged(safeStore, COMMIT, pending, minUncommitted == null ? Timestamp.MAX : minUncommitted, this);
        if (minUncommitted == null || next != null)
            pending = notifyUnmanaged(safeStore, APPLY, pending, next == null ? Timestamp.MAX : next.executeAt, this);

        return new CommandsForKey(key, redundantBefore, txns, pending);
    }

    public CommandsForKey notifyAndUpdatePending(SafeCommandStore safeStore, CommandsForKey prevCfk)
    {
        Unmanaged[] pending = this.unmanageds;
        if (minUncommitted != null && (prevCfk.minUncommitted == null || !prevCfk.minUncommitted.equals(minUncommitted)))
            notifyWaitingOnCommit(safeStore, minUncommitted);
        if (minUncommitted == null || next != null)
            pending = notifyUnmanaged(safeStore, APPLY, pending, next == null ? Timestamp.MAX : next.executeAt, this);

        return new CommandsForKey(key, redundantBefore, txns, pending);
    }

    private void notifyWaitingOnCommit(SafeCommandStore safeStore, TxnInfo uncommitted)
    {
        if (redundantBefore.endEpoch <= uncommitted.epoch())
            return;

        TxnId txnId = uncommitted.asPlainTxnId();
        if (uncommitted.status.compareTo(PREACCEPTED) < 0)
        {
            Key key = this.key;
            Keys keys = Keys.of(key);
            safeStore = safeStore; // make unsafe for compiler to permit in lambda
            safeStore.commandStore().execute(PreLoadContext.contextFor(txnId, keys), safeStore0 -> {
                SafeCommand safeCommand0 = safeStore0.get(txnId);
                safeCommand0.initialise();
                Command command = safeCommand0.current();
                Seekables<?, ?> keysOrRanges = command.keysOrRanges();
                if (keysOrRanges == null || !keysOrRanges.contains(key))
                {
                    CommonAttributes.Mutable attrs = command.mutable();
                    if (command.additionalKeysOrRanges() == null) attrs.additionalKeysOrRanges(keys);
                    else attrs.additionalKeysOrRanges(keys.with((Keys)command.additionalKeysOrRanges()));
                    safeCommand0.update(safeStore0, command.updateAttributes(attrs));
                }
                if (command.hasBeen(Status.Committed))
                {
                    // if we're committed but not invalidated, that means EITHER we have raced with a commit+
                    // OR we adopted as a dependency a
                    safeStore0.get(key).update(safeStore0, safeCommand0.current());
                }
                else
                {
                    // TODO (desired): we could complicate our state machine to replicate PreCommitted here, so we can simply wait for ReadyToExclude
                    safeStore0.progressLog().waiting(txnId, WaitingToExecute, null, keys.toParticipants());
                }
             }).begin(safeStore.agent());
        }
        else
        {
            safeStore.progressLog().waiting(txnId, WaitingToExecute, null, Keys.of(key).toParticipants());
        }
    }

    private static Unmanaged[] notifyUnmanaged(SafeCommandStore safeStore, Unmanaged.Pending pendingStatus, Unmanaged[] pending, Timestamp timestamp, CommandsForKey cfk)
    {
        switch (pendingStatus)
        {
            default: throw new AssertionError("Unhandled PendingRangeTransaction.Pending: " + pendingStatus);
            case COMMIT:
            {
                // notify commit uses exclusive bounds, as we use minUncommitted
                int end = findPendingCommit(pending, timestamp);
                if (end < 0) end = -1 - end;

                Unmanaged[] addPending = new Unmanaged[end];
                int addPendingCount = 0;
                for (int i = 0 ; i < end ; ++i)
                {
                    TxnId txnId = pending[i].txnId;
                    SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
                    if (safeCommand != null)
                    {
                        addPending[addPendingCount] = updatePending(safeStore, safeCommand, cfk);
                        if (addPending[addPendingCount] != null) ++addPendingCount;
                    }
                    else updatePendingAsync(safeStore, txnId, cfk.key);
                }

                Arrays.sort(addPending, 0, addPendingCount);
                return SortedArrays.linearUnion(pending, end, pending.length, addPending, 0, addPendingCount, Unmanaged::compareTo, ArrayBuffers.uncached(Unmanaged[]::new));
            }
            case APPLY:
            {
                int start = -1 - SortedArrays.binarySearch(pending, 0, pending.length, timestamp, (f, v) -> v.pending == APPLY ? -1 : 1, FAST);
                int end = findPendingApply(pending, timestamp);
                if (end < 0) end = -1 - end;

                if (start == end)
                    return pending;

                for (int i = start; i < end ; ++i)
                    removeWaitingOn(safeStore, pending[i].txnId, cfk.key);

                Unmanaged[] newPending = new Unmanaged[pending.length - (end - start)];
                System.arraycopy(pending, 0, newPending, 0, start);
                System.arraycopy(pending, end, newPending, start, newPending.length - start);
                return newPending;
            }
        }
    }

    private static void updatePendingAsync(SafeCommandStore safeStore, TxnId txnId, Key key)
    {
        PreLoadContext context = PreLoadContext.contextFor(txnId, Keys.of(key), KeyHistory.COMMANDS);
        safeStore.commandStore().execute(context, safeStore0 -> {
            SafeCommandsForKey safeCommandsForKey = safeStore0.get(key);
            CommandsForKey prev = safeCommandsForKey.current();
            Unmanaged addPending = updatePending(safeStore0, safeStore0.get(txnId), prev);
            if (addPending != null)
            {
                Unmanaged[] newPending = SortedArrays.insert(prev.unmanageds, addPending, Unmanaged[]::new);
                safeCommandsForKey.set(new CommandsForKey(prev.key, prev.redundantBefore, prev.txns, newPending));
            }
        }).begin(safeStore.agent());
    }

    private static Unmanaged updatePending(SafeCommandStore safeStore, SafeCommand safeCommand, CommandsForKey cfk)
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
            int j = SortedArrays.binarySearch(cfk.txns, 0, cfk.txns.length, txnIds.get(i), Timestamp::compareTo, FAST);
            if (j < 0) j = -1 -j;
            while (i < txnIds.size() && j < cfk.txns.length)
            {
                int c = txnIds.get(i).compareTo(cfk.txns[j]);
                if (c > 0) ++j;
                else if (c < 0) throw illegalState("Transaction not found: " + cfk.txns[j]);
                else
                {
                    TxnInfo txn = cfk.txns[j];
                    if (waitingTxnId.kind().awaitsOnlyDeps() || txn.executeAt.compareTo(waitingExecuteAt) < 0)
                    {
                        readyToExecute &= txn.status.compareTo(APPLIED) >= 0;
                        executesAt = Timestamp.nonNullOrMax(executesAt, txn.executeAt);
                    }
                    ++i;
                    ++j;
                }
            }

            if (!readyToExecute)
            {
                if (waitingTxnId.kind().awaitsOnlyDeps() && executesAt != null)
                {
                    if (executesAt instanceof TxnInfo) executesAt = ((TxnInfo) executesAt).asPlainTxnId();
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

        removeWaitingOn(safeStore, safeCommand, cfk.key, true);
        return null;
    }

    private static int findPendingCommit(Unmanaged[] pending, Timestamp txnId)
    {
        return SortedArrays.binarySearch(pending, 0, pending.length, txnId, (f, v) -> {
            if (v.pending == APPLY) return -1;
            return f.compareTo(v.waitingUntil);
        }, CEIL);
    }

    private static int findPendingApply(Unmanaged[] pending, Timestamp txnId)
    {
        return SortedArrays.binarySearch(pending, 0, pending.length, txnId, (f, v) -> {
            if (v.pending == COMMIT) return 1;
            return f.compareTo(v.waitingUntil);
        }, CEIL);
    }

    public CommandsForKey registerUnmanaged(SafeCommandStore safeStore, SafeCommand safeCommand)
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
            int j = SortedArrays.binarySearch(txns, 0, txns.length, txnIds.get(i), Timestamp::compareTo, FAST);
            if (j < 0) j = -1 -j;
            while (i < txnIds.size())
            {
                int c = j == txns.length ? -1 : txnIds.get(i).compareTo(txns[j]);
                if (c == 0)
                {
                    TxnInfo txn = txns[j];
                    if (txn.status.compareTo(COMMITTED) < 0) readyToExecute = waitingToApply = false;
                    else if (waitingTxnId.kind().awaitsOnlyDeps() || txn.executeAt.compareTo(waitingExecuteAt) < 0)
                    {
                        readyToExecute &= txn.status.compareTo(APPLIED) >= 0;
                        executesAt = Timestamp.nonNullOrMax(executesAt, txn.executeAt);
                    }
                    ++i;
                    ++j;
                }
                else if (c > 0) ++j;
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
                TxnInfo[] newTxns = txns;
                if (missingCount > 0)
                {
                    newTxns = new TxnInfo[missingCount + txns.length];
                    i = 0;
                    j = 0;
                    int count = 0;
                    while (i < missingCount && j < txns.length)
                    {
                        int c = missing[i].compareTo(txns[j]);
                        if (c < 0) newTxns[count++] = TxnInfo.create(missing[i++], TRANSITIVELY_KNOWN);
                        else newTxns[count++] = txns[j++];
                    }
                    while (i < missingCount)
                        newTxns[count++] = TxnInfo.create(missing[i++], TRANSITIVELY_KNOWN);
                    while (j < txns.length)
                        newTxns[count++] = txns[j++];
                }

                Unmanaged newPendingRecord;
                if (waitingToApply)
                {
                    if (waitingTxnId.kind().awaitsOnlyDeps() && executesAt != null)
                    {
                        if (executesAt.compareTo(command.waitingOn.executeAtLeast(Timestamp.NONE)) > 0)
                        {
                            Command.WaitingOn.Update waitingOn = new Command.WaitingOn.Update(command.waitingOn);
                            waitingOn.updateExecuteAtLeast(executesAt);
                            safeCommand.updateWaitingOn(waitingOn);
                        }
                    }
                    if (executesAt != null)
                        if (executesAt instanceof TxnInfo) executesAt = ((TxnInfo) executesAt).asPlainTxnId();

                    newPendingRecord = new Unmanaged(APPLY, command.txnId(), executesAt);
                }
                else newPendingRecord = new Unmanaged(COMMIT, command.txnId(), txnIds.get(txnIds.size() - 1));
                Unmanaged[] newPending = SortedArrays.insert(unmanageds, newPendingRecord, Unmanaged[]::new);
                return new CommandsForKey(key, redundantBefore, newTxns, newPending);
            }
        }

        removeWaitingOn(safeStore, safeCommand, key, true);
        return this;
    }

    // minVisitIndex is the index we guarantee to visit from contiguously, i.e. ignoring any jumps backwards due to executeAt
    private void notify(SafeCommandStore safeStore, Kinds kinds, Timestamp from, Timestamp to)
    {
        int start = SortedArrays.binarySearch(committed, 0, committed.length, from, (f, v) -> f.compareTo(v.executeAt), FAST);
        if (start < 0) start = -1 - start;
        int end = SortedArrays.binarySearch(committed, start, committed.length, to, (f, v) -> f.compareTo(v.executeAt), FAST);
        if (end < 0) end = -1 - end;
        else ++end;

        notify(safeStore, kinds, start, end);
    }

    private void notify(SafeCommandStore safeStore, Kinds kinds, int start, int end)
    {
        Participants<?> asParticipants = null;
        int uncommittedIndex = minUncommitted == null ? txns.length : -1;
        int unappliedReadCount = 0, unappliedWriteCount = 0, unappliedSyncPointCount = 0;

        for (int i = start ; i < end ; ++i)
        {
            TxnInfo txn = committed[i];
            Kind kind = txn.kind();
            switch (txn.status)
            {
                case APPLIED:
                case INVALID_OR_TRUNCATED:
                    continue;

                case COMMITTED:
                    // cannot execute as dependencies not stable, so notify progress log to get or decide stable deps
                    if (kinds.test(kind))
                    {
                        if (asParticipants == null)
                            asParticipants = Keys.of(key).toParticipants();
                        TxnId txnId = new TxnId(txn);
                        safeStore.progressLog().waiting(txnId, WaitingToExecute, null, asParticipants);
                    }
                    break;

                case STABLE:
                    if (uncommittedIndex < 0)
                        uncommittedIndex = SortedArrays.binarySearch(txns, 0, txns.length, minUncommitted, Timestamp::compareTo, FAST);

                    int nextUncommittedIndex = SortedArrays.exponentialSearch(txns, uncommittedIndex, txns.length, txn.executeAt, Timestamp::compareTo, FAST);
                    if (nextUncommittedIndex < 0) nextUncommittedIndex = -1 -nextUncommittedIndex;
                    while (uncommittedIndex < nextUncommittedIndex)
                    {
                        TxnInfo backfillTxn = txns[uncommittedIndex++];
                        if (backfillTxn.status.compareTo(COMMITTED) >= 0) continue;
                        Kind backfillKind = backfillTxn.kind();
                        switch (backfillKind)
                        {
                            default: throw new AssertionError("Unhandled Txn.Kind: " + backfillKind);
                            case LocalOnly:
                            case EphemeralRead:
                                throw illegalState("Invalid Txn.Kind for CommandsForKey: " + backfillKind);

                            case Write:
                                ++unappliedWriteCount;
                                break;

                            case Read:
                                ++unappliedReadCount;
                                break;

                            case SyncPoint:
                            case ExclusiveSyncPoint:
                                ++unappliedSyncPointCount;
                                break;
                        }
                    }

                    int expectMissingCount = 0;
                    switch (txn.kind())
                    {
                        default: throw new AssertionError("Unhandled Txn.Kind: " + kind);
                        case LocalOnly:
                        case EphemeralRead:
                            throw illegalState("Invalid Txn.Kind for CommandsForKey: " + kind);

                        case SyncPoint:
                        case ExclusiveSyncPoint:
                            expectMissingCount += unappliedSyncPointCount;

                        case Write:
                            expectMissingCount += unappliedReadCount;

                        case Read:
                            expectMissingCount += unappliedWriteCount;
                    }

                    // We remove committed transactions from the missing set, since they no longer need them there
                    // So the missing collection represents only those uncommitted transaction ids that a transaction
                    // witnesses/conflicts with. So we may simply count all of those we know of with a lower TxnId,
                    // and if the count is the same then we are not awaiting any of them for execution and can remove
                    // this command's dependency on this key for execution.
                    TxnId[] missing = txn.missing();
                    int missingCount = missing.length;
                    if (missingCount > 0 && minUncommitted != null)
                    {
                        int missingFrom = SortedArrays.binarySearch(missing, 0, missing.length, minUncommitted, TxnId::compareTo, FAST);
                        if (missingFrom < 0) missingFrom = -1 - missingFrom;
                        missingCount -= missingFrom;
                    }
                    if (expectMissingCount == missingCount)
                    {
                        TxnId txnId = new TxnId(txn);
                        removeWaitingOn(safeStore, txnId, key);
                        if (asParticipants == null)
                            asParticipants = Keys.of(key).toParticipants();
                        safeStore.progressLog().waiting(txnId, WaitingToApply, null, asParticipants);
                    }
            }

            switch (kind)
            {
                default: throw new AssertionError("Unhandled Txn.Kind: " + kind);
                case LocalOnly:
                case EphemeralRead:
                    throw illegalState("Invalid Txn.Kind for CommandsForKey: " + kind);

                case Write:
                    ++unappliedWriteCount;
                    break;

                case Read:
                    ++unappliedReadCount;
                    break;

                case SyncPoint:
                case ExclusiveSyncPoint:
                    ++unappliedSyncPointCount;
                    break;
            }
        }
    }

    private static void removeWaitingOn(SafeCommandStore safeStore, TxnId txnId, Key key)
    {
        SafeCommand safeCommand = safeStore.ifLoadedAndInitialised(txnId);
        if (safeCommand != null) removeWaitingOn(safeStore, safeCommand, key, true);
        else
        {
            PreLoadContext context = PreLoadContext.contextFor(txnId);
            safeStore.commandStore().execute(context, safeStore0 -> removeWaitingOn(safeStore0, safeStore0.get(txnId), key, false))
                     .begin(safeStore.agent());
        }
    }

    private static void removeWaitingOn(SafeCommandStore safeStore, SafeCommand safeCommand, Key key, boolean executeAsync)
    {
        Commands.removeWaitingOnKeyAndMaybeExecute(safeStore, safeCommand, key, executeAsync);
    }

    public CommandsForKey withRedundantBefore(RedundantBefore.Entry newRedundantBefore)
    {
        Invariants.checkArgument(newRedundantBefore.shardRedundantBefore().compareTo(shardRedundantBefore()) >= 0, "Expect new RedundantBefore.Entry shardAppliedOrInvalidatedBefore to be ahead of existing one");

        if (newRedundantBefore.equals(redundantBefore))
            return this;

        TxnInfo[] newTxns = txns;
        if (newRedundantBefore.shardRedundantBefore().compareTo(shardRedundantBefore()) >= 0)
        {
            int pos = insertPos(0, newRedundantBefore.shardRedundantBefore());
            if (pos != 0)
            {
                newTxns = Arrays.copyOfRange(txns, pos, txns.length);
                for (int i = 0 ; i < newTxns.length ; ++i)
                {
                    TxnInfo txn = newTxns[i];
                    TxnId[] missing = txn.missing();
                    if (missing == NO_TXNIDS) continue;
                    int j = Arrays.binarySearch(missing, newRedundantBefore.shardRedundantBefore());
                    if (j < 0) j = -1 - j;
                    if (j <= 0) continue;
                    missing = j == missing.length ? NO_TXNIDS : Arrays.copyOfRange(missing, j, missing.length);
                    newTxns[i] = txn.update(missing);
                }
            }
        }

        // TODO (expected): filter pending unmanageds
        return new CommandsForKey(key, newRedundantBefore, newTxns, unmanageds);
    }

    public CommandsForKey registerHistorical(TxnId txnId)
    {
        if (txnId.compareTo(shardRedundantBefore()) < 0)
            return this;

        int i = Arrays.binarySearch(txns, txnId);
        if (i >= 0)
            return txns[i].status.compareTo(HISTORICAL) >= 0 ? this : update(i, txnId, txns[i], TxnInfo.create(txnId, HISTORICAL, txnId));

        return insert(-1 - i, txnId, TxnInfo.create(txnId, HISTORICAL, txnId));
    }

    private int insertPos(int min, Timestamp timestamp)
    {
        int i = Arrays.binarySearch(txns, min, txns.length, timestamp);
        if (i < 0) i = -1 -i;
        return i;
    }

    public TxnId findFirst()
    {
        return txns.length > 0 ? txns[0] : null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CommandsForKey that = (CommandsForKey) o;
        return Objects.equals(key, that.key)
               && Objects.equals(redundantBefore, that.redundantBefore)
               && Arrays.equals(txns, that.txns);
    }

    @Override
    public int hashCode()
    {
        throw new UnsupportedOperationException();
    }
}
