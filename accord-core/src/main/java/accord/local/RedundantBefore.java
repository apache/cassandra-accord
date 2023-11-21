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
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
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

    public static class Entry
    {
        // TODO (desired): we don't need to maintain this now, and can simplify our builder, by migrating to ReducingRangeMap.foldWithBounds
        public final Range range;
        public final long startEpoch, endEpoch;

        /**
         * Represents the maximum TxnId we know to have fully executed until locally for the range in question.
         * Unless we are stale or pre-bootstrap, in which case no such guarantees can be made.
         */
        public final @Nonnull TxnId locallyAppliedOrInvalidatedBefore;

        /**
         * Represents the maximum TxnId we know to have fully executed until across all healthy replicas for the range in question.
         * Unless we are stale or pre-bootstrap, in which case no such guarantees can be made.
         */
        public final @Nonnull TxnId shardAppliedOrInvalidatedBefore;

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
         * The maximum of each of locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore and bootstrappedAt.
         */
        public final @Nonnull TxnId minConflict;

        /**
         * staleUntilAtLeast provides a minimum TxnId until which we know we will be unable to completely execute
         * transactions locally for the impacted range.
         *
         * See also {@link CommandStore#safeToRead}.
         */
        public final @Nullable Timestamp staleUntilAtLeast;

        public Entry(Range range, long startEpoch, long endEpoch, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
        {
            this.range = range;
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
            this.locallyAppliedOrInvalidatedBefore = locallyAppliedOrInvalidatedBefore;
            this.shardAppliedOrInvalidatedBefore = shardAppliedOrInvalidatedBefore;
            this.bootstrappedAt = bootstrappedAt;
            this.minConflict = Timestamp.max(Timestamp.max(locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore), bootstrappedAt);
            this.staleUntilAtLeast = staleUntilAtLeast;
        }

        public static Entry reduce(Entry a, Entry b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Entry merge(Range range, Entry a, Entry b)
        {
            if (a.startEpoch > b.endEpoch)
                return a;

            if (b.startEpoch > a.endEpoch)
                return b;

            long startEpoch = Long.max(a.startEpoch, b.startEpoch);
            long endEpoch = Long.min(a.endEpoch, b.endEpoch);
            int cl = a.locallyAppliedOrInvalidatedBefore.compareTo(b.locallyAppliedOrInvalidatedBefore);
            int cs = a.shardAppliedOrInvalidatedBefore.compareTo(b.shardAppliedOrInvalidatedBefore);
            int cb = a.bootstrappedAt.compareTo(b.bootstrappedAt);
            int csu = compareStaleUntilAtLeast(a.staleUntilAtLeast, b.staleUntilAtLeast);

            if (range.equals(a.range) && startEpoch == a.startEpoch && endEpoch == a.endEpoch && cl >= 0 && cb >= 0 && cs >= 0 && csu >= 0)
                return a;
            if (range.equals(b.range) && startEpoch == b.startEpoch && endEpoch == b.endEpoch && cl <= 0 && cb <= 0 && cs <= 0 && csu <= 0)
                return b;

            TxnId locallyAppliedOrInvalidatedBefore = cl >= 0 ? a.locallyAppliedOrInvalidatedBefore : b.locallyAppliedOrInvalidatedBefore;
            TxnId shardAppliedOrInvalidatedBefore = cs >= 0 ? a.shardAppliedOrInvalidatedBefore : b.shardAppliedOrInvalidatedBefore;
            TxnId bootstrappedAt = cb >= 0 ? a.bootstrappedAt : b.bootstrappedAt;
            Timestamp staleUntilAtLeast = csu >= 0 ? a.staleUntilAtLeast : b.staleUntilAtLeast;

            // if our redundantBefore predates bootstrappedAt, we should clear it to avoid erroneously treating
            // transactions prior as locally redundant when they may simply have not applied yet, since we may
            // permit the sync point that defines redundancy to apply locally without waiting for these earlier
            // transactions, since we now consider them to be bootstrapping
            // TODO (desired): revisit later as semantics here evolve
            if (bootstrappedAt.compareTo(locallyAppliedOrInvalidatedBefore) >= 0)
                locallyAppliedOrInvalidatedBefore = TxnId.NONE;
            if (bootstrappedAt.compareTo(shardAppliedOrInvalidatedBefore) >= 0)
                shardAppliedOrInvalidatedBefore = TxnId.NONE;
            if (staleUntilAtLeast != null && bootstrappedAt.compareTo(staleUntilAtLeast) >= 0)
                staleUntilAtLeast = null;

            return new Entry(range, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast);
        }

        static @Nonnull RedundantStatus getAndMerge(Entry entry, @Nonnull RedundantStatus prev, TxnId txnId, EpochSupplier executeAt)
        {
            if (entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;
            return prev.merge(entry.get(txnId));
        }

        static RedundantStatus get(Entry entry, TxnId txnId, EpochSupplier executeAt)
        {
            if (entry == null || entry.outOfBounds(txnId, executeAt))
                return NOT_OWNED;
            return entry.get(txnId);
        }

        static PreBootstrapOrStale getAndMerge(Entry entry, @Nonnull PreBootstrapOrStale prev, TxnId txnId, EpochSupplier executeAt)
        {
            if (prev == PARTIALLY || entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;

            boolean isPreBootstrapOrStale = entry.staleUntilAtLeast != null || entry.bootstrappedAt.compareTo(txnId) > 0;
            return isPreBootstrapOrStale ? prev == POST_BOOTSTRAP ? PARTIALLY : FULLY
                                         : prev == FULLY          ? PARTIALLY : POST_BOOTSTRAP;
        }

        static Timestamp getAndMergeMinConflict(Entry entry, @Nonnull Timestamp prev)
        {
            if (entry == null)
                return prev;
            return Timestamp.max(prev, entry.minConflict);
        }

        static <T extends Deps> Deps.AbstractBuilder<T> collectDep(Entry entry, @Nonnull Deps.AbstractBuilder<T> prev, @Nonnull EpochSupplier minEpoch, @Nonnull EpochSupplier executeAt)
        {
            if (entry == null || entry.outOfBounds(minEpoch, executeAt))
                return prev;

            if (entry.shardAppliedOrInvalidatedBefore.compareTo(Timestamp.NONE) > 0)
                prev.add(entry.range, entry.shardAppliedOrInvalidatedBefore);

            return prev;
        }

        static Ranges validateSafeToRead(Entry entry, @Nonnull Ranges safeToRead, Timestamp bootstrapAt, Object ignore)
        {
            if (entry == null)
                return safeToRead;

            if (bootstrapAt.compareTo(entry.bootstrappedAt) < 0 || (entry.staleUntilAtLeast != null && bootstrapAt.compareTo(entry.staleUntilAtLeast) < 0))
                return safeToRead.subtract(Ranges.of(entry.range));

            return safeToRead;
        }

        static Ranges expectToExecute(Entry entry, @Nonnull Ranges executeRanges, TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (entry == null || (executeAt == null ? entry.outOfBounds(txnId) : entry.outOfBounds(txnId, executeAt)))
                return executeRanges;

            if (txnId.compareTo(entry.bootstrappedAt) < 0 || entry.staleUntilAtLeast != null)
                return executeRanges.subtract(Ranges.of(entry.range));

            return executeRanges;
        }

        RedundantStatus get(TxnId txnId)
        {
            // we have to first check bootstrappedAt, since we are not locally redundant for the covered range
            // if the txnId is partially pre-bootstrap (since we may not have applied it for this range)
            if (staleUntilAtLeast != null || bootstrappedAt.compareTo(txnId) > 0)
                return PRE_BOOTSTRAP_OR_STALE;
            if (locallyAppliedOrInvalidatedBefore.compareTo(txnId) > 0)
            {
                if (shardAppliedOrInvalidatedBefore.compareTo(txnId) > 0)
                    return SHARD_REDUNDANT;
                return LOCALLY_REDUNDANT;
            }
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
            return shardAppliedOrInvalidatedBefore;
        }

        // TODO (required, consider): this admits the range of epochs that cross the two timestamps, which matches our
        //   behaviour elsewhere but we probably want to only interact with the two point epochs in which we participate,
        //   but note hasRedundantDependencies which really does want to scan a range
        private boolean outOfBounds(EpochSupplier lb, EpochSupplier ub)
        {
            return ub.epoch() < startEpoch || lb.epoch() >= endEpoch;
        }

        private boolean outOfBounds(Timestamp lb)
        {
            return lb.epoch() >= endEpoch;
        }

        Entry withRange(Range range)
        {
            return new Entry(range, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast);
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
            return this.startEpoch == that.startEpoch
                   && this.endEpoch == that.endEpoch
                   && this.locallyAppliedOrInvalidatedBefore.equals(that.locallyAppliedOrInvalidatedBefore)
                   && this.shardAppliedOrInvalidatedBefore.equals(that.shardAppliedOrInvalidatedBefore)
                   && this.bootstrappedAt.equals(that.bootstrappedAt)
                   && Objects.equals(this.staleUntilAtLeast, that.staleUntilAtLeast);
        }

        @Override
        public String toString()
        {
            return "("
                   + (startEpoch == Long.MIN_VALUE ? "-\u221E" : Long.toString(startEpoch)) + ","
                   + (endEpoch == Long.MAX_VALUE ? "\u221E" : Long.toString(endEpoch)) + ","
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

    public static RedundantBefore create(Ranges ranges, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId bootstrappedAt)
    {
        return create(ranges, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, null);
    }

    public static RedundantBefore create(Ranges ranges, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
    {
        return create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast);
    }

    public static RedundantBefore create(Ranges ranges, long startEpoch, long endEpoch, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId bootstrappedAt)
    {
        return create(ranges, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, null);
    }

    public static RedundantBefore create(Ranges ranges, long startEpoch, long endEpoch, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
    {
        if (ranges.isEmpty())
            return new RedundantBefore();

        Entry entry = new Entry(null, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast);
        Builder builder = new Builder(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (int i = 0 ; i < ranges.size() ; ++i)
        {
            Range cur = ranges.get(i);
            builder.append(cur.start(), entry.withRange(cur), (a, b) -> { throw new IllegalStateException(); });
            builder.append(cur.end(), null, (a, b) -> a); // if we are equal to prev end, take the prev value not zero
        }
        return builder.build();
    }

    public static RedundantBefore merge(RedundantBefore a, RedundantBefore b)
    {
        return ReducingIntervalMap.merge(a, b, RedundantBefore.Entry::reduce, Builder::new);
    }

    public RedundantStatus get(TxnId txnId, @Nullable EpochSupplier executeAt, RoutingKey participant)
    {
        if (executeAt == null) executeAt = txnId;
        Entry entry = get(participant);
        return Entry.get(entry, txnId, executeAt);
    }

    public RedundantStatus status(TxnId txnId, EpochSupplier executeAt, Participants<?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        return foldl(participants, Entry::getAndMerge, NOT_OWNED, txnId, executeAt, ignore -> false);
    }

    /**
     * RedundantStatus.REDUNDANT overrides PRE_BOOTSTRAP; to avoid complicating that state machine,
     * for cases where we care independently about the overall pre-bootstrap state we have a separate mechanism
     */
    public PreBootstrapOrStale preBootstrapOrStale(TxnId txnId, @Nullable EpochSupplier executeAt, Participants<?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        return foldl(participants, Entry::getAndMerge, PreBootstrapOrStale.NOT_OWNED, txnId, executeAt, r -> r == PARTIALLY);
    }

    public Timestamp minConflict(Seekables<?, ?> participants)
    {
        return foldl(participants, Entry::getAndMergeMinConflict, Timestamp.NONE, ignore -> false);
    }

    public <T extends Deps> Deps.AbstractBuilder<T> collectDeps(Seekables<?, ?> participants, Deps.AbstractBuilder<T> builder, EpochSupplier minEpoch, EpochSupplier executeAt)
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

    static class Builder extends ReducingIntervalMap.Builder<RoutingKey, Entry, RedundantBefore>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected boolean equals(Entry a, Entry b)
        {
            return a.equalsIgnoreRange(b);
        }

        // we maintain the range that an Entry applies to for ease of some other functionality - this logic is applied here in append and above by overriding equals.
        // entries may be null for ranges we do not have any knowledge for; the final "entry" we append is expected to be null by the ReducingIntervalMap.Builder
        // TODO (desired): consider migrating to foldlWithBounds
        @Override
        public void append(RoutingKey start, @Nullable Entry value, BiFunction<Entry, Entry, Entry> reduce)
        {
            if (value != null && value.range.start().compareTo(start) < 0)
                value = value.withRange(value.range.newRange(start, value.range.end()));

            int tailIdx = values.size() - 1;
            super.append(start, value, reduce);

            Entry tailValue; // TODO (desired): clean up maintenance of accurate range bounds
            if (values.size() - 2 == tailIdx && tailIdx >= 0 && (tailValue = values.get(tailIdx)) != null && tailValue.range.end().compareTo(start) > 0)
                values.set(tailIdx, tailValue.withRange(tailValue.range.newRange(tailValue.range.start(), start)));
        }

        @Override
        protected Entry mergeEqual(Entry a, Entry b)
        {
            Invariants.checkState(a.range.compareIntersecting(b.range) == 0 || a.range.end().equals(b.range.start()) || a.range.start().equals(b.range.end()));
            return new Entry(a.range.newRange(
                a.range.start().compareTo(b.range.start()) <= 0 ? a.range.start() : b.range.start(),
                a.range.end().compareTo(b.range.end()) >= 0 ? a.range.end() : b.range.end()
            ), a.startEpoch, a.endEpoch, a.locallyAppliedOrInvalidatedBefore, a.shardAppliedOrInvalidatedBefore, a.bootstrappedAt, a.staleUntilAtLeast);
        }

        @Override
        protected RedundantBefore buildInternal()
        {
            return new RedundantBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }
}
