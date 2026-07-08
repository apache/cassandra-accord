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

import java.util.NavigableMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.RoutingKey;
import accord.local.MaxDecidedRX.DecidedRX;
import accord.local.RedundantBefore.Bounds;
import accord.local.cfk.CommandsForKey;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind.Kinds;
import accord.primitives.TxnId;
import accord.primitives.Unseekable;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.TriPredicate;
import accord.utils.UnhandledEnum;

import static accord.local.CommandSummaries.Relevance.ACTIVE;
import static accord.local.CommandSummaries.Relevance.IRRELEVANT;
import static accord.local.CommandSummaries.Relevance.MAYBE_SUPERSEDING;
import static accord.local.CommandSummaries.Relevance.SUPERSEDING;
import static accord.local.CommandSummaries.SummaryStatus.ACCEPTED;
import static accord.local.CommandSummaries.SummaryStatus.APPLIED;
import static accord.local.CommandSummaries.SummaryStatus.NOTACCEPTED;
import static accord.local.CommandSummaries.SummaryStatus.PREACCEPTED;
import static accord.local.LoadKeysFor.RECOVERY;
import static accord.local.LoadKeysFor.WRITE;
import static accord.local.MaxDecidedRX.forDeps;
import static accord.primitives.Known.KnownDeps.NoDeps;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Txn.Kind.Nothing;

public interface CommandSummaries
{
    enum SummaryStatus
    {
        NOT_DIRECTLY_WITNESSED,
        PREACCEPTED,
        NOTACCEPTED,
        ACCEPTED,
        COMMITTED,
        STABLE,
        APPLIED,
        INVALIDATED;

        public static final SummaryStatus NONE = null;

        private static final SummaryStatus[] SUMMARY_STATUSES = values();
    }

    enum IsDep
    {
        IS_COORD_DEP, IS_NOT_COORD_DEP, NOT_ELIGIBLE, IS_PROPOSED_OR_STABLE_DEP, IS_NOT_PROPOSED_OR_STABLE_DEP;
        private static final IsDep[] IS_DEPS = values();
    }

    enum Relevance
    {
        /* No need to visit this command */
        IRRELEVANT(0),
        /* May need to visit this command, but can make do with TxnId, SaveStatus and Durability */
        ACTIVE(1),
        /* May need to visit this command, and requires SupersedingVisitor information */
        MAYBE_SUPERSEDING(2),
        MAYBE_BOTH(3),
        /* May need to visit this command, and requires SupersedingVisitor information */
        SUPERSEDING(6),
        BOTH(7),
        ;


        private static final Relevance[] lookup = values();
        public static final int ENCODED_MASK = 7;
        public static final int ENCODED_BITS = 3;
        private final int encoded;

        Relevance(int encoded)
        {
            this.encoded = encoded;
        }

        public boolean is(Relevance relevance)
        {
            return (relevance.encoded & encoded) == relevance.encoded;
        }

        public Relevance or(Relevance that)
        {
            return forBits(this.encoded | that.encoded);
        }

        public static Relevance forOrdinal(int ordinal)
        {
            return lookup[ordinal];
        }

        public static Relevance forBits(int bits)
        {
            return forOrdinal(bits < 4 ? bits : 4 + (bits & 1));
        }
    }

    class Summary extends TxnId
    {
        private static final int SUMMARY_STATUS_MASK = 0x7;
        private static final int IS_DEP_SHIFT = 3;
        private static final int IS_DEP_MASK = 0x7;
        private static final int DURABILITY_SHIFT = 6;
        private static final int RELEVANCE_SHIFT = DURABILITY_SHIFT + Durability.ENCODING_BITS;

        final @Nullable Timestamp executeAt;
        final int encoded;
        final Unseekables<?> participants;

        public Summary slice(Ranges ranges)
        {
            return new Summary(this, this.executeAt, encoded, participants.slice(ranges, Minimal));
        }

        @VisibleForTesting
        public Summary(@Nonnull TxnId txnId, @Nullable Timestamp executeAt, @Nonnull SummaryStatus status, @Nonnull Durability durability, IsDep dep, Relevance relevance, Unseekables<?> participants)
        {
            super(txnId);
            this.participants = participants;
            this.executeAt = txnId.equals(executeAt) ? this : executeAt;
            this.encoded = Invariants.nonNull(status).ordinal()
                           | (dep == null ? Integer.MIN_VALUE : (dep.ordinal() << IS_DEP_SHIFT))
                           | (Invariants.nonNull(durability).encoded() << DURABILITY_SHIFT)
                           | (relevance.encoded << RELEVANCE_SHIFT);
        }

        private Summary(@Nonnull TxnId txnId, @Nullable Timestamp executeAt, int encoded, Unseekables<?> participants)
        {
            super(txnId);
            this.participants = participants;
            this.executeAt = executeAt == txnId || txnId.equals(executeAt) ? this : executeAt;
            this.encoded = encoded;
        }

        public boolean is(IsDep isDep)
        {
            return (encoded >> IS_DEP_SHIFT) == isDep.ordinal();
        }

        public boolean is(Relevance relevance)
        {
            return ((encoded >> RELEVANCE_SHIFT) & relevance.encoded) == relevance.encoded;
        }

        public IsDep isDep()
        {
            if (encoded < 0)
                return null;
            return IsDep.IS_DEPS[(encoded >>> IS_DEP_SHIFT) & IS_DEP_MASK];
        }

        public Durability durability()
        {
            return Durability.forEncoded((encoded >>> DURABILITY_SHIFT) & Durability.ENCODING_MASK);
        }

        public Relevance relevance()
        {
            return Relevance.forOrdinal((encoded >>> RELEVANCE_SHIFT) & Relevance.ENCODED_MASK);
        }

        public boolean is(SummaryStatus summaryStatus)
        {
            return (encoded & SUMMARY_STATUS_MASK) == summaryStatus.ordinal();
        }

        public SummaryStatus status()
        {
            int ordinal = encoded & SUMMARY_STATUS_MASK;
            return SummaryStatus.SUMMARY_STATUSES[ordinal];
        }

        public Unseekables<?> participants()
        {
            return participants;
        }

        public TxnId plainTxnId()
        {
            return new TxnId(this);
        }

        public Timestamp plainExecuteAt()
        {
            return executeAt == this ? new Timestamp(this) : executeAt;
        }

        @Override
        public String toString()
        {
            return "Summary{" +
                   "txnId=" + plainTxnId() +
                   ", executeAt=" + plainExecuteAt() +
                   ", saveStatus=" + status() +
                   ", isDep=" + isDep() +
                   ", relevance=" + relevance() +
                   '}';
        }
    }

    class SummaryLoader
    {
        public interface Factory<L extends SummaryLoader>
        {
            L create(RedundantBefore redundantBefore, @Nullable MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Unseekables<?> searchKeysOrRanges, Kinds testKind, TxnId minTxnId, Timestamp executeAt, LoadKeysFor loadKeysFor);
        }

        private static final ReducingRangeMap<TxnId> NO_RX = new ReducingRangeMap<>();
        
        protected final RedundantBefore redundantBefore;
        protected final MaxDecidedRX maxDecidedRX;
        protected final Unseekables<?> searchFor;
        // TODO (expected): separate out Kinds we need before/after primaryTxnId/executeAt
        protected final Kinds testKind;
        protected final LoadKeysFor loadKeysFor;
        protected final TxnId primaryTxnId, minTxnId;
        protected final DecidedRX decidedRx;
        protected final Timestamp primaryExecuteAt;

        private ReducingRangeMap<TxnId> minVisitedFutureRX = NO_RX;
        private TxnId maxRx = TxnId.MAX; // a cached summary of minVisitedFutureRX to avoid consulting the full collection

        // TODO (expected): provide executeAt to PreLoadContext so we can more aggressively filter what we load, esp. by Kind
        public static SummaryLoader loader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, ExecutionContext context)
        {
            return loader(redundantBefore, maxDecidedRX, context.primaryTxnId(), context.executeAt(), context.loadKeysFor(), context.keys());
        }

        public static SummaryLoader loader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Timestamp executeAt, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges)
        {
            return loader(redundantBefore, maxDecidedRX, primaryTxnId, executeAt, loadKeysFor, keysOrRanges, SummaryLoader::new);
        }

        public static <L extends SummaryLoader> L loader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, ExecutionContext context, Factory<L> factory)
        {
            return loader(redundantBefore, maxDecidedRX, context.primaryTxnId(), context.executeAt(), context.loadKeysFor(), context.keys(), factory);
        }

        public static <L extends SummaryLoader> L loader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Timestamp executeAt, LoadKeysFor loadKeysFor, Unseekables<?> keysOrRanges, Factory<L> factory)
        {
            Invariants.require(primaryTxnId != null);
            TxnId minTxnId = redundantBefore.min(keysOrRanges, Bounds::gcBefore);
            Kinds kinds = primaryTxnId.witnesses().or(loadKeysFor == RECOVERY ? primaryTxnId.witnessedBy() : Nothing);
            if (!primaryTxnId.is(Txn.Kind.ExclusiveSyncPoint)) // the main distinction between RX and RV is that RV doesn't filter out decided transactions
                maxDecidedRX = null;
            return factory.create(redundantBefore, maxDecidedRX, primaryTxnId, keysOrRanges, kinds, minTxnId, executeAt, loadKeysFor);
        }

        public SummaryLoader(RedundantBefore redundantBefore, MaxDecidedRX maxDecidedRX, TxnId primaryTxnId, Unseekables<?> searchFor, Kinds testKind, TxnId minTxnId, Timestamp primaryExecuteAt, LoadKeysFor loadKeysFor)
        {
            this.redundantBefore = redundantBefore;
            this.maxDecidedRX = maxDecidedRX;
            this.primaryTxnId = primaryTxnId;
            this.searchFor = searchFor;
            this.testKind = testKind;
            this.minTxnId = minTxnId;
            this.primaryExecuteAt = primaryExecuteAt;
            this.loadKeysFor = loadKeysFor;
            this.decidedRx = forDeps(maxDecidedRX, searchFor, primaryTxnId);
        }

        public Unseekables<?> participants()
        {
            return searchFor;
        }

        public LoadKeysFor loadKeysFor()
        {
            return loadKeysFor;
        }

        public TxnId primaryTxnId()
        {
            return primaryTxnId;
        }

        public boolean isRelevant(CommandsForKey cfk)
        {
            //noinspection SizeReplaceableByIsEmpty (not equivalent)
            if (cfk == null || cfk.size() == 0)
                return false;

            return isRelevant(cfk.key(), cfk.get(cfk.size() - 1), cfk.minUndecidedManaged());
        }

        public boolean isRelevant(RoutingKey key, TxnId last, TxnId minUndecided)
        {
            // NOTE: we CANNOT safely filter on first element, as we may have pruned dependencies we need to witness
            //  and that will be populated on the receiving replicas as necessary - that is,
            //  we must permit adopting future dependencies
            if (last.compareTo(minTxnId) < 0)
                return false;

            if (maxDecidedRX == null)
                return true;

            if (minUndecided != null)
                return true;

            // TODO (expected): can we improve our inferences to avoid looping?
            if (decidedRx != null && decidedRx.excludeDecided(last))
                return false;

            DecidedRX decidedRx = maxDecidedRX.forDeps(key, primaryTxnId);
            return decidedRx == null || decidedRx.includeDecided(last);
        }

        public boolean shouldRecordFutureRx(TxnId txnId, SummaryStatus status)
        {
            return txnId.isSyncPoint() && txnId.compareTo(primaryTxnId) > 0 && (status == SummaryStatus.STABLE || status == APPLIED);
        }

        // the caller must manage mutual exclusion for this method, but not to any others
        public void recordFutureRx(TxnId txnId, Unseekables<?> participants)
        {
            minVisitedFutureRX = ReducingRangeMap.merge(minVisitedFutureRX, ReducingRangeMap.create(participants.toRanges(), txnId), TxnId::min);
            maxRx = minVisitedFutureRX.foldlWithDefault(searchFor, TxnId::max, TxnId.MAX, TxnId.NONE);
        }

        public final Summary ifRelevant(Command cmd)
        {
            return ifRelevant(cmd.txnId(), cmd.executeAtOrTxnId(), cmd.saveStatus(), cmd.durability(), cmd.participants(), cmd.partialDeps());
        }

        public final Summary ifRelevant(MinimalCommand cmd)
        {
            return ifRelevant(cmd.txnId, cmd.executeAt == null ? cmd.txnId : cmd.executeAt, cmd.saveStatus, cmd.durability, cmd.participants, null);
        }

        public final Summary ifRelevant(MinimalCommand.MinimalWithDeps cmd)
        {
            if (cmd.participants == null)
                return null;

            return ifRelevant(cmd.txnId, cmd.executeAt == null ? cmd.txnId : cmd.executeAt, cmd.saveStatus, cmd.durability, cmd.participants.touches(), cmd, (c, find, intersecting) -> isDep(c.partialDeps(), find, intersecting));
        }

        public final boolean isMaybeRelevant(TxnId txnId)
        {
            return maxRelevance(txnId, null, null, null, null) != IRRELEVANT;
        }

        public final Relevance maxRelevance(TxnId txnId, @Nullable SaveStatus saveStatus, @Nullable Durability durability, @Nullable Timestamp executeAt, @Nullable Unseekables<?> participants)
        {
            return relevanceInternal(txnId, saveStatus, durability, executeAt, participants, true);
        }

        public final Relevance relevance(Command cmd)
        {
            return relevance(cmd.txnId(), cmd.saveStatus(), cmd.durability(), cmd.executeAt(), cmd.participants().stillTouches());
        }

        public final Relevance relevance(TxnId txnId, @Nonnull SaveStatus saveStatus, @Nonnull Durability durability, @Nonnull Timestamp executeAt, @Nonnull Unseekables<?> participants)
        {
            return relevanceInternal(txnId, saveStatus, durability, executeAt, participants, false);
        }

        private Relevance relevanceInternal(TxnId txnId, @Nullable SaveStatus saveStatus, @Nullable Durability durability, @Nullable Timestamp executeAt, @Nullable Unseekables<?> participants, boolean permitNull)
        {
            if (loadKeysFor == WRITE || !txnId.is(testKind) || (saveStatus != null && saveStatus.compareTo(SaveStatus.TruncatedUnapplied) >= 0))
                return IRRELEVANT;

            if (!permitNull)
            {
                Invariants.requireArgument(durability != null, "durability was expected to be non-null");
                Invariants.requireArgument(saveStatus != null, "saveStatus was expected to be non-null");
                Invariants.requireArgument(participants != null, "participants was expected to be non-null");
                if (executeAt == null)
                {
                    if (saveStatus == SaveStatus.NotDefined || saveStatus.status == Status.AcceptedInvalidate)
                        return IRRELEVANT;

                    Invariants.refuseArgument("executeAt was expected to be non-null");
                }
            }

            if (txnId.compareTo(minTxnId) < 0)
                return IRRELEVANT;

            Relevance atLeast = IRRELEVANT;
            if (loadKeysFor == RECOVERY && txnId.witnesses(primaryTxnId))
            {
                if (isIgnorableFutureRx(txnId, participants))
                    return IRRELEVANT;

                atLeast = supersedingRelevance(txnId, saveStatus, executeAt, participants);
            }

            if (primaryExecuteAt.compareTo(txnId) < 0 || !primaryTxnId.witnesses(txnId))
                return atLeast;

            boolean mayFilterAsDecided = maxDecidedRX != null && (txnId.isSyncPoint() || (durability != null && durability.isDurablyCommitted()));
            if (!mayFilterAsDecided)
                return atLeast.or(ACTIVE);

            if (decidedRx != null && decidedRx.excludeDecided(txnId))
                return atLeast;

            if (participants == null)
                return atLeast.or(ACTIVE);

            DecidedRX decidedRx = forDeps(maxDecidedRX, participants, primaryTxnId);
            return atLeast.or(decidedRx == null || decidedRx.includeDecided(txnId) ? ACTIVE : IRRELEVANT);
        }

        private Relevance supersedingRelevance(TxnId txnId, @Nullable SaveStatus saveStatus, @Nullable Timestamp executeAt, @Nullable Unseekables<?> participants)
        {
            if (txnId.isSyncPoint())
                executeAt = txnId;

            Relevance ifRelevant = SUPERSEDING;
            if (saveStatus != null)
            {
                switch (saveStatus.known.deps())
                {
                    default: throw UnhandledEnum.unknown(saveStatus.known.deps());
                    case NoDeps: throw UnhandledEnum.invalid(NoDeps);
                    case DepsUnknown:
                        // SyncPoint do not (by default) collect additional dependencies in their Accept phase; to support this we must
                        // wait for any undecided future sync point (prior to the latest decided one after us) to decide itself
                        // and record its dependencies here. However, we ignore any we have not directly witnessed.
                        // This does not risk deadlock as there is no fast path to recover for sync points
                        if (txnId.isSyncPoint() && !saveStatus.is(Status.NotDefined))
                            break;
                    case DepsErased:
                        return IRRELEVANT;
                    case DepsFromCoordinator:
                    case DepsProposedFixed:
                        executeAt = txnId;
                        break;
                    case DepsProposed:
                        if (txnId.compareTo(primaryTxnId) < 0)
                            ifRelevant = MAYBE_SUPERSEDING;
                        // cannot assume executeAt is txnId for DepsProposed when not sync point,
                        // as if the transaction executes after us we may need to wait for the transaction to stabilise its dependencies
                    case DepsCommitted:
                    case DepsKnown:
                }
            }

            if ((executeAt == null || executeAt.compareTo(primaryTxnId) > 0) && (participants == null || participants.intersects(searchFor)))
            {
                // TODO (desired): for pre-filter we can terminate here; only need to continue on construction
                return ifRelevant;
            }
            return IRRELEVANT;
        }

        public final boolean isIgnorableFutureRx(TxnId txnId, Unseekables<?> participants)
        {
            return txnId.isSyncPoint() &&
                   (txnId.compareTo(maxRx) > 0 ||
                    (participants != null
                     && txnId.compareTo(primaryTxnId) > 0
                     && txnId.compareTo(minVisitedFutureRX.foldlWithDefault(participants, TxnId::max, TxnId.MAX, TxnId.NONE)) > 0));
        }

        public final Summary ifRelevant(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, StoreParticipants participants, @Nullable PartialDeps partialDeps)
        {
            if (participants == null)
                return null;

            return ifRelevant(txnId, executeAt, saveStatus, durability, participants.touches(), partialDeps);
        }

        public final Summary ifRelevant(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, Participants<?> touches, @Nullable PartialDeps partialDeps)
        {
            return ifRelevant(txnId, executeAt, saveStatus, durability, touches, partialDeps, SummaryLoader::isDep);
        }

        public final <P> Summary ifRelevant(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, StoreParticipants participants, @Nullable P deps, TriPredicate<P, TxnId, Unseekables<?>> depTester)
        {
            if (participants == null)
                return null;

            return ifRelevant(txnId, executeAt, saveStatus, durability, participants.touches(), deps, depTester);
        }

        public final <P> Summary ifRelevant(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, Participants<?> touches, @Nullable P deps, TriPredicate<P, TxnId, Unseekables<?>> depTester)
        {
            Relevance relevance = relevance(txnId, saveStatus, durability, executeAt, touches);
            return get(relevance, txnId, executeAt, saveStatus, durability, touches, deps, depTester);
        }

        public final <P> Summary get(Relevance relevance, Command cmd)
        {
            return get(relevance, cmd.txnId(), cmd.executeAtOrTxnId(), cmd.saveStatus(), cmd.durability(), cmd.participants().stillTouches(), cmd.partialDeps(), SummaryLoader::isDep);
        }

        public final <P> Summary get(Relevance relevance, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, Participants<?> touches, @Nullable PartialDeps partialDeps)
        {
            return get(relevance, txnId, executeAt, saveStatus, durability, touches, partialDeps, SummaryLoader::isDep);
        }

        public final <P> Summary get(Relevance relevance, TxnId txnId, Timestamp executeAt, SaveStatus saveStatus, Durability durability, Participants<?> touches, @Nullable P deps, TriPredicate<P, TxnId, Unseekables<?>> depTester)
        {
            touches = touches.intersecting(searchFor, Minimal);
            IsDep isDep = loadKeysFor == RECOVERY ? IsDep.NOT_ELIGIBLE : null;
            switch (relevance)
            {
                default: throw new UnhandledEnum(relevance);
                case IRRELEVANT: return null;
                case BOTH:
                case SUPERSEDING:
                    if (deps != null)
                    {
                        boolean isCoordDeps = saveStatus.summary.compareTo(ACCEPTED) < 0;
                        boolean isAnyDep = depTester.test(deps, primaryTxnId, touches);
                        isDep = isAnyDep ? (isCoordDeps ? IsDep.IS_COORD_DEP     : IsDep.IS_PROPOSED_OR_STABLE_DEP)
                                         : (isCoordDeps ? IsDep.IS_NOT_COORD_DEP : IsDep.IS_NOT_PROPOSED_OR_STABLE_DEP);
                    }
                    else Invariants.require(txnId.isSyncPoint() && (saveStatus.summary == PREACCEPTED || saveStatus.summary == NOTACCEPTED));
                case MAYBE_BOTH:
                case MAYBE_SUPERSEDING:
                case ACTIVE:
            }
            return construct(txnId, executeAt, saveStatus.summary, durability, isDep, relevance, touches);

        }

        private static boolean isDep(PartialDeps deps, TxnId find, Unseekables<?> intersecting)
        {
            int index = deps.indexOf(find);
            // TODO (desired): don't construct participants, pass intersecting as parameter
            return index >= 0 && deps.isStable(index)
                   && deps.participants(index).containsAll(intersecting);

        }

        protected Summary construct(TxnId txnId, Timestamp executeAt, SummaryStatus summaryStatus, Durability durability, IsDep isDep, Relevance relevance, Unseekables<?> participants)
        {
            return new Summary(txnId, executeAt, summaryStatus, durability, isDep, relevance, participants);
        }
    }
    
    enum ComputeIsDep
    {
        // don't test deps
        IGNORE,

        // calculate but don't filter
        EITHER
    }

    interface ActiveCommandVisitor<P1, P2>
    {
        void visit(P1 p1, P2 p2, SummaryStatus status, Durability durability, Unseekable keyOrRange, TxnId txnId);
        default void visitMaxAppliedHlc(long maxAppliedHlc) {}
    }

    interface SupersedingCommandVisitor
    {
        /**
         * Note: Durability is not guaranteed to return anything besides NotDurable; implementation is free to return more information if easily available.
         */
        boolean visit(Unseekable keyOrRange, TxnId txnId, Timestamp executeAt, SummaryStatus status, @Nullable IsDep dep, Durability minDurability);
    }

    boolean visit(Unseekables<?> keysOrRanges, TxnId testTxnId, Kinds testKind, SupersedingCommandVisitor visit);

    /**
     * Visits keys first in ascending order, with equal keys visiting TxnId is ascending order.
     * Visits range transactions in ascending order by TxnId, then visiting each Range in ascending order
     */
    <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visit, P1 p1, P2 p2);

    // TODO (expected): ByRangeSnapshot based on IntervalBTree so we can elide superseded dependencies as we do with CommandsForKey
    interface ByTxnIdSnapshot extends CommandSummaries
    {
        NavigableMap<Timestamp, Summary> byTxnId();

        default boolean visit(Unseekables<?> keysOrRanges,
                              TxnId testTxnId,
                              Kinds testKind,
                              SupersedingCommandVisitor visit)
        {
            NavigableMap<Timestamp, Summary> map = byTxnId();

            for (Summary value : map.values())
            {
                if (!testKind.test(value))
                    continue;

                if (!value.is(MAYBE_SUPERSEDING))
                    continue;

                Unseekables<?> participants = value.participants;
                Unseekables<?> intersecting = participants.overlapping(keysOrRanges);
                if (!intersecting.isEmpty())
                {
                    Timestamp executeAt = value.plainExecuteAt();
                    Invariants.require(executeAt != null);
                    SummaryStatus status = value.status();
                    IsDep dep = value.isDep();
                    for (Unseekable participant : intersecting)
                    {
                        if (!visit.visit(participant, value.plainTxnId(), executeAt, status, dep, NotDurable))
                            return false;
                    }
                }
            }

            return true;
        }

        @Override
        default <P1, P2> void visit(Unseekables<?> keysOrRanges, Timestamp startedBefore, Kinds testKind, ActiveCommandVisitor<P1, P2> visit, P1 p1, P2 p2)
        {
            NavigableMap<Timestamp, Summary> map = byTxnId();
            for (Summary value : map.headMap(startedBefore, false).values())
            {
                if (!testKind.test(value))
                    continue;

                if (value.is(SummaryStatus.INVALIDATED))
                    continue;

                if (!value.is(ACTIVE))
                    continue;

                for (Unseekable keyOrRange : value.participants.intersecting(keysOrRanges, Minimal))
                    visit.visit(p1, p2, value.status(), value.durability(), keyOrRange, value.plainTxnId());
            }
        }
    }
}
