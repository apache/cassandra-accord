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

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.primitives.AbstractRanges;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

import static accord.local.RedundantBefore.PreBootstrapOrStale.FULLY;
import static accord.local.RedundantBefore.PreBootstrapOrStale.POST_BOOTSTRAP;
import static accord.local.RedundantBefore.PreBootstrapOrStale.PARTIALLY;
import static accord.local.RedundantStatus.LIVE;
import static accord.local.RedundantStatus.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.NOT_OWNED;
import static accord.local.RedundantStatus.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.SHARD_REDUNDANT;
import static accord.local.RedundantStatus.WAS_OWNED;
import static accord.utils.Invariants.illegalState;

public class RedundantBefore extends ReducingRangeMap<RedundantBefore.Entry>
{
    public static class SerializerSupport
    {
        public static RedundantBefore create(boolean inclusiveEnds, RoutingKey[] ends, Entry[] values)
        {
            return new RedundantBefore(inclusiveEnds, ends, values);
        }
    }

    public enum PreBootstrapOrStale { NOT_OWNED, FULLY, PARTIALLY, POST_BOOTSTRAP }

    // TODO (required): rationalise the various bounds we maintain; make merge idempotent and apply any filtering by superseding bounds on access
    public static class Entry
    {
        // TODO (desired): we don't need to maintain this now, can migrate to ReducingRangeMap.foldWithBounds
        public final Range range;
        // start inclusive, end exclusive
        public final long startOwnershipEpoch, endOwnershipEpoch;

        /**
         * Represents the maximum TxnId we know to have fully executed until locally for the range in question.
         * Unless we are stale or pre-bootstrap, in which case no such guarantees can be made.
         *
         * We maintain locallyAppliedOrInvalidatedBefore that were reached prior to a new bootstrap exceeding them,
         * as these were reached correctly.
         */
        public final @Nonnull TxnId locallyAppliedOrInvalidatedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until locally for the range in question,
         * and for which we guarantee that any distributed decision that might need to be consulted is also recorded
         * locally (i.e. it is known to be Stable locally, or else it did not execute)
         *
         * We maintain locallyDecidedAndExecutedBefore that were reached prior to a new bootstrap exceeding them,
         * as these were reached correctly and can be used for GC.
         *
         * However, once a bootstrap has begun we cannot safely advance until shardAppliedOrInvalidatedBefore goes
         * ahead of the bootstrappedAt, because we cannot guarantee to have any intervening decision recorded locally.
         */
        public final @Nonnull TxnId locallyDecidedAndAppliedOrInvalidatedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until across all healthy non-bootstrapping replicas
         * for the range in question, including ourselves.
         */
        public final @Nonnull TxnId shardAppliedOrInvalidatedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until across all healthy replicas for the range in question.
         * Unless we are stale or pre-bootstrap, in which case no such guarantees can be made.
         */
        public final @Nonnull TxnId gcBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until across all healthy non-bootstrapping replicas
         * for the range in question.
         */
        public final @Nonnull TxnId shardOnlyAppliedOrInvalidatedBefore;

        /**
         * bootstrappedAt defines the txnId bounds we expect to maintain data for locally.
         *
         * We can bootstrap ranges at different times, and have a transaction that participates in both ranges -
         * in this case one of the portions of the transaction may be totally unordered with respect to other transactions
         * in that range because both occur prior to the bootstrappedAt point, so their dependencies are entirely erased.
         * We can also re-bootstrap the same range because bootstrap failed, and leave dangling transactions to execute
         * which then execute in an unordered fashion.
         *
         * See also {@link CommandStore#safeToRead}.
         */
        public final @Nonnull TxnId bootstrappedAt;

        /**
         * staleUntilAtLeast provides a minimum TxnId until which we know we will be unable to completely execute
         * transactions locally for the impacted range.
         *
         * See also {@link CommandStore#safeToRead}.
         */
        public final @Nullable Timestamp staleUntilAtLeast;

        public Entry(Range range, long startOwnershipEpoch, long endOwnershipEpoch, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId locallyDecidedAndAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId shardOnlyAppliedOrInvalidatedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
        {
            this.range = range;
            this.startOwnershipEpoch = startOwnershipEpoch;
            this.endOwnershipEpoch = endOwnershipEpoch;
            this.locallyAppliedOrInvalidatedBefore = locallyAppliedOrInvalidatedBefore;
            this.locallyDecidedAndAppliedOrInvalidatedBefore = locallyDecidedAndAppliedOrInvalidatedBefore;
            this.shardAppliedOrInvalidatedBefore = shardAppliedOrInvalidatedBefore;
            this.gcBefore = gcBefore;
            this.shardOnlyAppliedOrInvalidatedBefore = shardOnlyAppliedOrInvalidatedBefore;
            this.bootstrappedAt = bootstrappedAt;
            this.staleUntilAtLeast = staleUntilAtLeast;
            Invariants.checkArgument(locallyAppliedOrInvalidatedBefore.equals(TxnId.NONE) || locallyAppliedOrInvalidatedBefore.domain().isRange());
            Invariants.checkArgument(locallyDecidedAndAppliedOrInvalidatedBefore.equals(TxnId.NONE) || locallyDecidedAndAppliedOrInvalidatedBefore.domain().isRange());
            Invariants.checkArgument(shardAppliedOrInvalidatedBefore.equals(TxnId.NONE) || shardAppliedOrInvalidatedBefore.domain().isRange());
            Invariants.checkArgument(gcBefore.equals(TxnId.NONE) || gcBefore.domain().isRange());
            Invariants.checkArgument(gcBefore.compareTo(shardAppliedOrInvalidatedBefore) <= 0);
        }

        public static Entry reduce(Entry a, Entry b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Entry merge(Range range, Entry cur, Entry add)
        {
            // TODO (required): we shouldn't be trying to merge non-intersecting epochs
            if (cur.startOwnershipEpoch > add.endOwnershipEpoch)
                return cur;

            if (add.startOwnershipEpoch > cur.endOwnershipEpoch)
                return add;

            long startEpoch = Long.max(cur.startOwnershipEpoch, add.startOwnershipEpoch);
            long endEpoch = Long.min(cur.endOwnershipEpoch, add.endOwnershipEpoch);
            int cl = cur.locallyAppliedOrInvalidatedBefore.compareTo(add.locallyAppliedOrInvalidatedBefore);
            int cd = cur.locallyDecidedAndAppliedOrInvalidatedBefore.compareTo(add.locallyDecidedAndAppliedOrInvalidatedBefore);
            int cs = cur.shardOnlyAppliedOrInvalidatedBefore.compareTo(add.shardOnlyAppliedOrInvalidatedBefore);
            int cg = cur.gcBefore.compareTo(add.gcBefore);
            int cb = cur.bootstrappedAt.compareTo(add.bootstrappedAt);
            int csu = compareStaleUntilAtLeast(cur.staleUntilAtLeast, add.staleUntilAtLeast);

            if (range.equals(cur.range) && startEpoch == cur.startOwnershipEpoch && endEpoch == cur.endOwnershipEpoch && cl >= 0 && cb >= 0 && cs >= 0 && csu >= 0)
                return cur;
            if (range.equals(add.range) && startEpoch == add.startOwnershipEpoch && endEpoch == add.endOwnershipEpoch && cl <= 0 && cb <= 0 && cs <= 0 && csu <= 0)
                return add;

            TxnId locallyAppliedOrInvalidatedBefore = cl >= 0 ? cur.locallyAppliedOrInvalidatedBefore : add.locallyAppliedOrInvalidatedBefore;
            TxnId locallyDecidedAndAppliedOrInvalidatedBefore = cd >= 0 ? cur.locallyDecidedAndAppliedOrInvalidatedBefore : add.locallyDecidedAndAppliedOrInvalidatedBefore;
            TxnId shardOnlyAppliedOrInvalidatedBefore = cs >= 0 ? cur.shardOnlyAppliedOrInvalidatedBefore : add.shardOnlyAppliedOrInvalidatedBefore;
            TxnId gcBefore = cg >= 0 ? cur.gcBefore : add.gcBefore;
            TxnId bootstrappedAt = cb >= 0 ? cur.bootstrappedAt : add.bootstrappedAt;
            Timestamp staleUntilAtLeast = csu >= 0 ? cur.staleUntilAtLeast : add.staleUntilAtLeast;
            TxnId shardAppliedOrInvalidatedBefore = TxnId.min(shardOnlyAppliedOrInvalidatedBefore, locallyAppliedOrInvalidatedBefore);

            // if a NEW redundantBefore predates our current bootstrappedAt, we should not update it to avoid erroneously
            // treating transactions prior as locally redundant when they may simply have not applied yet, since we may
            // permit the sync point that defines redundancy to apply locally without waiting for these earlier
            // transactions, since we now consider them to be bootstrapping.
            // however, any locallyAppliedOrInvalidatedBefore that was set before bootstrap can be safely maintained,
            // and should not ideally go backwards (as CommandsForKey utilises it for GC)
            // TODO (desired): revisit later as semantics here evolve
            if (bootstrappedAt.compareTo(locallyAppliedOrInvalidatedBefore) >= 0)
                locallyAppliedOrInvalidatedBefore = cur.locallyAppliedOrInvalidatedBefore;
            if (bootstrappedAt.compareTo(shardAppliedOrInvalidatedBefore) >= 0)
                locallyDecidedAndAppliedOrInvalidatedBefore = cur.locallyDecidedAndAppliedOrInvalidatedBefore;
            if (staleUntilAtLeast != null && bootstrappedAt.compareTo(staleUntilAtLeast) >= 0)
                staleUntilAtLeast = null;

            return new Entry(range, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, locallyDecidedAndAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, shardOnlyAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public Entry withGcBeforeBeforeAtLeast(TxnId newGcBefore)
        {
            if (newGcBefore.compareTo(gcBefore) <= 0)
                return this;

            TxnId locallyAppliedOrInvalidatedBefore = TxnId.nonNullOrMax(this.locallyAppliedOrInvalidatedBefore, newGcBefore);
            TxnId shardAppliedOrInvalidatedBefore = TxnId.nonNullOrMax(this.shardAppliedOrInvalidatedBefore, newGcBefore);
            return new Entry(range, startOwnershipEpoch, endOwnershipEpoch, locallyAppliedOrInvalidatedBefore, locallyDecidedAndAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, shardOnlyAppliedOrInvalidatedBefore, newGcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        @VisibleForImplementation
        public Entry withEpochs(long start, long end)
        {
            return new Entry(range, start, end, locallyAppliedOrInvalidatedBefore, locallyDecidedAndAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, shardOnlyAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        static @Nonnull RedundantStatus getAndMerge(Entry entry, @Nonnull RedundantStatus prev, TxnId txnId, Object ignore)
        {
            if (entry == null)
                return prev;
            return prev.merge(entry.get(txnId));
        }

        static @Nonnull Boolean isAnyOnCoordinationEpoch(Entry entry, @Nonnull Boolean prev, TxnId txnId, RedundantStatus status)
        {
            if (entry == null || prev)
                return prev;

            long epoch = txnId.epoch();
            if (entry.startOwnershipEpoch > epoch || entry.endOwnershipEpoch <= epoch)
                return false;

            return entry.getIgnoringOwnership(txnId) == status;
        }

        static @Nonnull Boolean isAnyOnAnyEpoch(Entry entry, @Nonnull Boolean prev, TxnId txnId, RedundantStatus status)
        {
            if (entry == null || prev)
                return prev;

            long epoch = txnId.epoch();
            if (entry.startOwnershipEpoch > epoch || entry.endOwnershipEpoch <= epoch)
                return false;

            return entry.getIgnoringOwnership(txnId) == status;
        }

        static RedundantStatus get(Entry entry, TxnId txnId, EpochSupplier executeAt)
        {
            if (entry == null)
                return NOT_OWNED;

            return entry.get(txnId);
        }

        static PreBootstrapOrStale getAndMerge(Entry entry, @Nonnull PreBootstrapOrStale prev, TxnId txnId, Object ignore)
        {
            if (prev == PARTIALLY || entry == null)
                return prev;

            // TODO (required): consider all call-sites and confirm the answers when wasOwned and willBeOwned are reasonable
            if (entry.wasOwned(txnId) && entry.isComplete())
                return prev;

            boolean isPreBootstrapOrStale = entry.staleUntilAtLeast != null || entry.bootstrappedAt.compareTo(txnId) > 0;
            return isPreBootstrapOrStale ? prev == POST_BOOTSTRAP ? PARTIALLY : FULLY
                                         : prev == FULLY          ? PARTIALLY : POST_BOOTSTRAP;
        }

        static <T extends Deps> Deps.AbstractBuilder<T> collectDep(Entry entry, @Nonnull Deps.AbstractBuilder<T> prev, @Nonnull EpochSupplier minEpoch, @Nonnull EpochSupplier executeAt)
        {
            if (entry == null)
                return prev;

            if (entry.gcBefore.compareTo(Timestamp.NONE) > 0)
                prev.add(entry.range, entry.gcBefore);

            return prev;
        }

        static Ranges validateSafeToRead(Entry entry, @Nonnull Ranges safeToRead, Timestamp bootstrapAt, Object ignore)
        {
            if (entry == null)
                return safeToRead;

            if (bootstrapAt.compareTo(entry.bootstrappedAt) < 0 || (entry.staleUntilAtLeast != null && bootstrapAt.compareTo(entry.staleUntilAtLeast) < 0))
                return safeToRead.without(Ranges.of(entry.range));

            return safeToRead;
        }

        static Ranges expectToExecute(Entry entry, @Nonnull Ranges executeRanges, TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (entry == null || (executeAt == null ? entry.outOfBounds(txnId) : entry.outOfBounds(txnId, executeAt)))
                return executeRanges;

            if (txnId.compareTo(entry.bootstrappedAt) < 0 || entry.staleUntilAtLeast != null)
                return executeRanges.without(Ranges.of(entry.range));

            return executeRanges;
        }

        static Ranges removeShardRedundant(Entry entry, @Nonnull Ranges notRedundant, TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (entry == null || (executeAt == null ? entry.outOfBounds(txnId) : entry.outOfBounds(txnId, executeAt)))
                return notRedundant;

            if (txnId.compareTo(entry.gcBefore) < 0)
                return notRedundant.without(Ranges.of(entry.range));

            return notRedundant;
        }

        static Ranges removePreBootstrap(Entry entry, @Nonnull Ranges notPreBootstrap, TxnId txnId, Object ignore)
        {
            if (entry == null)
                return notPreBootstrap;

            if (txnId.compareTo(entry.bootstrappedAt) < 0)
                return notPreBootstrap.without(Ranges.of(entry.range));

            return notPreBootstrap;
        }

        RedundantStatus get(TxnId txnId)
        {
            if (wasOwned(txnId) && isComplete())
                return WAS_OWNED;
            return getIgnoringOwnership(txnId);
        }

        RedundantStatus getIgnoringOwnership(TxnId txnId)
        {
            // we have to first check bootstrappedAt, since we are not locally redundant for the covered range
            // if the txnId is partially pre-bootstrap (since we may not have applied it for this range)
            if (staleUntilAtLeast != null || bootstrappedAt.compareTo(txnId) > 0)
                return PRE_BOOTSTRAP_OR_STALE;

            if (locallyAppliedOrInvalidatedBefore.compareTo(txnId) > 0)
            {
                if (gcBefore.compareTo(txnId) > 0)
                    return SHARD_REDUNDANT;
                return LOCALLY_REDUNDANT;
            }

            if (txnId.epoch() < startOwnershipEpoch)
                return PRE_BOOTSTRAP_OR_STALE;

            return LIVE;
        }

        private static int compareStaleUntilAtLeast(@Nullable Timestamp a, @Nullable Timestamp b)
        {
            boolean aIsNull = a == null, bIsNull = b == null;
            if (aIsNull != bIsNull) return aIsNull ? -1 : 1;
            return aIsNull ? 0 : a.compareTo(b);
        }

        public final TxnId shardRedundantBefore()
        {
            return gcBefore;
        }

        public final TxnId locallyRedundantBefore()
        {
            return locallyAppliedOrInvalidatedBefore;
        }

        public final TxnId locallyRedundantOrBootstrappedBefore()
        {
            return TxnId.max(locallyAppliedOrInvalidatedBefore, bootstrappedAt);
        }

        private boolean outOfBounds(EpochSupplier lb, EpochSupplier ub)
        {
            return ub.epoch() < startOwnershipEpoch || lb.epoch() >= endOwnershipEpoch;
        }

        private boolean wasOwned(EpochSupplier lb)
        {
            return lb.epoch() >= endOwnershipEpoch;
        }

        // TODO (required): do we still need this, or can we stick to just the explicit endOwnershipEpoch
        private boolean isComplete()
        {
            // TODO (required): carefully consider whether we should ALSO expect some local property to be met here
            return endOwnershipEpoch <= gcBefore.epoch();
        }

        private boolean outOfBounds(Timestamp lb)
        {
            return lb.epoch() >= endOwnershipEpoch;
        }

        Entry withEpochs(int startEpoch, int endEpoch)
        {
            return new Entry(range, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, locallyDecidedAndAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, shardOnlyAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public Entry withRange(Range range)
        {
            return new Entry(range, startOwnershipEpoch, endOwnershipEpoch, locallyAppliedOrInvalidatedBefore, locallyDecidedAndAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, shardOnlyAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public boolean equals(Object that)
        {
            return that instanceof Entry && equals((Entry) that);
        }

        public boolean equals(Entry that)
        {
            return this.range.equals(that.range) && equalsIgnoreRange(that);
        }

        public boolean equalsIgnoreRange(Entry that)
        {
            return this.startOwnershipEpoch == that.startOwnershipEpoch
                   && this.endOwnershipEpoch == that.endOwnershipEpoch
                   && this.locallyAppliedOrInvalidatedBefore.equals(that.locallyAppliedOrInvalidatedBefore)
                   && this.locallyDecidedAndAppliedOrInvalidatedBefore.equals(that.locallyDecidedAndAppliedOrInvalidatedBefore)
                   && this.shardAppliedOrInvalidatedBefore.equals(that.shardAppliedOrInvalidatedBefore)
                   && this.shardOnlyAppliedOrInvalidatedBefore.equals(that.shardOnlyAppliedOrInvalidatedBefore)
                   && this.gcBefore.equals(that.gcBefore)
                   && this.bootstrappedAt.equals(that.bootstrappedAt)
                   && Objects.equals(this.staleUntilAtLeast, that.staleUntilAtLeast);
        }

        @Override
        public String toString()
        {
            return "("
                   + (startOwnershipEpoch == Long.MIN_VALUE ? "-\u221E" : Long.toString(startOwnershipEpoch)) + ","
                   + (endOwnershipEpoch == Long.MAX_VALUE ? "\u221E" : Long.toString(endOwnershipEpoch)) + ","
                   + (locallyAppliedOrInvalidatedBefore.compareTo(bootstrappedAt) >= 0 ? locallyAppliedOrInvalidatedBefore + ")" : bootstrappedAt + "*)");
        }
    }

    public static RedundantBefore EMPTY = new RedundantBefore();

    private final Ranges staleRanges;

    private RedundantBefore()
    {
        staleRanges = Ranges.EMPTY;
    }

    RedundantBefore(boolean inclusiveEnds, RoutingKey[] starts, Entry[] values)
    {
        super(inclusiveEnds, starts, values);
        staleRanges = extractStaleRanges(values);
        checkParanoid(starts, values);
    }

    private static Ranges extractStaleRanges(Entry[] values)
    {
        int countStaleRanges = 0;
        for (Entry entry : values)
        {
            if (entry != null && entry.staleUntilAtLeast != null)
                ++countStaleRanges;
        }

        if (countStaleRanges == 0)
            return Ranges.EMPTY;

        Range[] staleRanges = new Range[countStaleRanges];
        countStaleRanges = 0;
        for (Entry entry : values)
        {
            if (entry != null && entry.staleUntilAtLeast != null)
                staleRanges[countStaleRanges++] = entry.range;
        }
        return Ranges.ofSortedAndDeoverlapped(staleRanges).mergeTouching();
    }

    public static RedundantBefore create(AbstractRanges ranges, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt)
    {
        return create(ranges, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, null);
    }

    public static RedundantBefore create(AbstractRanges ranges, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
    {
        return create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
    }

    public static RedundantBefore create(AbstractRanges ranges, long startEpoch, long endEpoch, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt)
    {
        return create(ranges, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, null);
    }

    public static RedundantBefore create(AbstractRanges ranges, long startEpoch, long endEpoch, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardOnlyAppliedOrInvalidatedBefore, @Nonnull TxnId gcBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
    {
        if (ranges.isEmpty())
            return new RedundantBefore();

        TxnId locallyDecidedAndAppliedOrInvalidatedBefore = locallyAppliedOrInvalidatedBefore;
        TxnId shardAppliedOrInvalidatedBefore = TxnId.min(locallyAppliedOrInvalidatedBefore, shardOnlyAppliedOrInvalidatedBefore);
        Entry entry = new Entry(null, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, locallyDecidedAndAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, shardOnlyAppliedOrInvalidatedBefore, gcBefore, bootstrappedAt, staleUntilAtLeast);
        Builder builder = new Builder(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (int i = 0 ; i < ranges.size() ; ++i)
        {
            Range cur = ranges.get(i);
            builder.append(cur.start(), cur.end(), entry.withRange(cur));
        }
        return builder.build();
    }

    public static RedundantBefore merge(RedundantBefore a, RedundantBefore b)
    {
        return ReducingIntervalMap.mergeIntervals(a, b, Builder::new);
    }

    public RedundantStatus get(TxnId txnId, @Nullable EpochSupplier executeAt, RoutingKey participant)
    {
        if (executeAt == null) executeAt = txnId;
        Entry entry = get(participant);
        return Entry.get(entry, txnId, executeAt);
    }

    public RedundantStatus status(TxnId txnId, Participants<?> participants)
    {   // TODO (required): consider how the use of txnId for executeAt affects exclusive sync points for cleanup
        //    may want to issue synthetic sync points for local evaluation in later epochs
        return foldl(participants, Entry::getAndMerge, NOT_OWNED, txnId, null, ignore -> false);
    }

    public boolean isAnyOnCoordinationEpoch(TxnId txnId, Unseekables<?> participants, RedundantStatus status)
    {
        return foldl(participants, Entry::isAnyOnCoordinationEpoch, false, txnId, status, isDone -> isDone);
    }

    public boolean isAnyOnAnyEpoch(TxnId txnId, Unseekables<?> participants, RedundantStatus status)
    {
        return foldl(participants, Entry::isAnyOnAnyEpoch, false, txnId, status, isDone -> isDone);
    }

    /**
     * RedundantStatus.REDUNDANT overrides PRE_BOOTSTRAP; to avoid complicating that state machine,
     * for cases where we care independently about the overall pre-bootstrap state we have a separate mechanism
     */
    public PreBootstrapOrStale preBootstrapOrStale(TxnId txnId, Participants<?> participants)
    {
        return foldl(participants, Entry::getAndMerge, PreBootstrapOrStale.NOT_OWNED, txnId, null, r -> r == PARTIALLY);
    }

    public <T extends Deps> Deps.AbstractBuilder<T> collectDeps(Routables<?> participants, Deps.AbstractBuilder<T> builder, EpochSupplier minEpoch, EpochSupplier executeAt)
    {
        return foldl(participants, Entry::collectDep, builder, minEpoch, executeAt, ignore -> false);
    }

    public Ranges validateSafeToRead(Timestamp forBootstrapAt, Ranges ranges)
    {
        return foldl(ranges, Entry::validateSafeToRead, ranges, forBootstrapAt, null, r -> false);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges removeShardRedundant(TxnId txnId, @Nonnull Timestamp executeAt, Ranges ranges)
    {
        Invariants.checkArgument(executeAt != null, "executeAt must not be null");
        return foldl(ranges, Entry::removeShardRedundant, ranges, txnId, executeAt, r -> false);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges removePreBootstrap(TxnId txnId, Ranges ranges)
    {
        return foldl(ranges, Entry::removePreBootstrap, ranges, txnId, null, r -> false);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap
     */
    public Ranges expectToExecute(TxnId txnId, @Nonnull Timestamp executeAt, Ranges ranges)
    {
        Invariants.checkArgument(executeAt != null, "executeAt must not be null");
        return foldl(ranges, Entry::expectToExecute, ranges, txnId, executeAt, r -> false);
    }

    /**
     * Subtract any ranges we consider stale or pre-bootstrap at any point
     */
    public Ranges everExpectToExecute(TxnId txnId, Ranges ranges)
    {
        return foldl(ranges, Entry::expectToExecute, ranges, txnId, null, r -> false);
    }

    /**
     * RedundantStatus.REDUNDANT overrides PRE_BOOTSTRAP; to avoid complicating that state machine,
     * for cases where we care independently about the overall pre-bootstrap state we have a separate mechanism
     */
    public Ranges staleRanges()
    {
        return staleRanges;
    }

    public static class Builder extends AbstractIntervalBuilder<RoutingKey, Entry, RedundantBefore>
    {
        public Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected Entry slice(RoutingKey start, RoutingKey end, Entry v)
        {
            if (v.range.start().equals(start) && v.range.end().equals(end))
                return v;

            return new Entry(v.range.newRange(start, end), v.startOwnershipEpoch, v.endOwnershipEpoch, v.locallyAppliedOrInvalidatedBefore, v.locallyDecidedAndAppliedOrInvalidatedBefore, v.shardAppliedOrInvalidatedBefore, v.shardOnlyAppliedOrInvalidatedBefore, v.gcBefore, v.bootstrappedAt, v.staleUntilAtLeast);
        }

        @Override
        protected Entry reduce(Entry a, Entry b)
        {
            return Entry.reduce(a, b);
        }

        @Override
        protected Entry tryMergeEqual(Entry a, Entry b)
        {
            if (!a.equalsIgnoreRange(b))
                return null;

            Invariants.checkState(a.range.compareIntersecting(b.range) == 0 || a.range.end().equals(b.range.start()) || a.range.start().equals(b.range.end()));
            return new Entry(a.range.newRange(
                a.range.start().compareTo(b.range.start()) <= 0 ? a.range.start() : b.range.start(),
                a.range.end().compareTo(b.range.end()) >= 0 ? a.range.end() : b.range.end()
            ), a.startOwnershipEpoch, a.endOwnershipEpoch, a.locallyDecidedAndAppliedOrInvalidatedBefore, a.locallyAppliedOrInvalidatedBefore, a.shardAppliedOrInvalidatedBefore, a.shardOnlyAppliedOrInvalidatedBefore, a.gcBefore, a.bootstrappedAt, a.staleUntilAtLeast);
        }

        @Override
        public void append(RoutingKey start, RoutingKey end, @Nonnull Entry value)
        {
            if (value.range.start().compareTo(start) != 0 || value.range.end().compareTo(end) != 0)
                throw illegalState();
            super.append(start, end, value);
        }

        @Override
        protected RedundantBefore buildInternal()
        {
            return new RedundantBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }

    private static void checkParanoid(RoutingKey[] starts, Entry[] values)
    {
        if (!Invariants.isParanoid())
            return;

        for (int i = 0 ; i < values.length ; ++i)
        {
            if (values[i] != null)
            {
                Invariants.checkArgument(starts[i].equals(values[i].range.start()));
                Invariants.checkArgument(starts[i + 1].equals(values[i].range.end()));
            }
        }
    }
}
