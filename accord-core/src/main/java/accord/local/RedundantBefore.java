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
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
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
import static accord.utils.Invariants.illegalState;

public class RedundantBefore extends ReducingRangeMap<RedundantBefore.Entry>
{
    public static final EpochSupplier NO_UPPER_BOUND = () -> Long.MAX_VALUE;

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
        // TODO (desired): we don't need to maintain this now, can migrate to ReducingRangeMap.foldWithBounds
        public final Range range;
        // start inclusive, end exclusive
        public final long startEpoch, endEpoch;

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
         * staleUntilAtLeast provides a minimum TxnId until which we know we will be unable to completely execute
         * transactions locally for the impacted range.
         *
         * See also {@link CommandStore#safeToRead}.
         */
        public final @Nullable Timestamp staleUntilAtLeast;

        public Entry(Range range, long startEpoch, long endEpoch, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
        {
            this(range, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast);
        }

        public Entry(Range range, long startEpoch, long endEpoch, @Nonnull TxnId locallyAppliedOrInvalidatedBefore, @Nonnull TxnId locallyDecidedAndAppliedOrInvalidatedBefore, @Nonnull TxnId shardAppliedOrInvalidatedBefore, @Nonnull TxnId bootstrappedAt, @Nullable Timestamp staleUntilAtLeast)
        {
            this.range = range;
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
            this.locallyAppliedOrInvalidatedBefore = locallyAppliedOrInvalidatedBefore;
            this.locallyDecidedAndAppliedOrInvalidatedBefore = locallyDecidedAndAppliedOrInvalidatedBefore;
            this.shardAppliedOrInvalidatedBefore = shardAppliedOrInvalidatedBefore;
            this.bootstrappedAt = bootstrappedAt;
            this.staleUntilAtLeast = staleUntilAtLeast;
            Invariants.checkArgument(locallyAppliedOrInvalidatedBefore.equals(TxnId.NONE) || locallyAppliedOrInvalidatedBefore.domain().isRange());
            Invariants.checkArgument(locallyDecidedAndAppliedOrInvalidatedBefore.equals(TxnId.NONE) || locallyDecidedAndAppliedOrInvalidatedBefore.domain().isRange());
            Invariants.checkArgument(shardAppliedOrInvalidatedBefore.equals(TxnId.NONE) || shardAppliedOrInvalidatedBefore.domain().isRange());
        }

        public static Entry reduce(Entry a, Entry b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Entry merge(Range range, Entry cur, Entry add)
        {
            if (cur.startEpoch > add.endEpoch)
                return cur;

            if (add.startEpoch > cur.endEpoch)
                return add;

            long startEpoch = Long.max(cur.startEpoch, add.startEpoch);
            long endEpoch = Long.min(cur.endEpoch, add.endEpoch);
            int cl = cur.locallyAppliedOrInvalidatedBefore.compareTo(add.locallyAppliedOrInvalidatedBefore);
            int cd = cur.locallyDecidedAndAppliedOrInvalidatedBefore.compareTo(add.locallyDecidedAndAppliedOrInvalidatedBefore);
            int cs = cur.shardAppliedOrInvalidatedBefore.compareTo(add.shardAppliedOrInvalidatedBefore);
            int cb = cur.bootstrappedAt.compareTo(add.bootstrappedAt);
            int csu = compareStaleUntilAtLeast(cur.staleUntilAtLeast, add.staleUntilAtLeast);

            if (range.equals(cur.range) && startEpoch == cur.startEpoch && endEpoch == cur.endEpoch && cl >= 0 && cb >= 0 && cs >= 0 && csu >= 0)
                return cur;
            if (range.equals(add.range) && startEpoch == add.startEpoch && endEpoch == add.endEpoch && cl <= 0 && cb <= 0 && cs <= 0 && csu <= 0)
                return add;

            TxnId locallyAppliedOrInvalidatedBefore = cl >= 0 ? cur.locallyAppliedOrInvalidatedBefore : add.locallyAppliedOrInvalidatedBefore;
            TxnId locallyDecidedAndAppliedOrInvalidatedBefore = cd >= 0 ? cur.locallyDecidedAndAppliedOrInvalidatedBefore : add.locallyDecidedAndAppliedOrInvalidatedBefore;
            TxnId shardAppliedOrInvalidatedBefore = cs >= 0 ? cur.shardAppliedOrInvalidatedBefore : add.shardAppliedOrInvalidatedBefore;
            TxnId bootstrappedAt = cb >= 0 ? cur.bootstrappedAt : add.bootstrappedAt;
            Timestamp staleUntilAtLeast = csu >= 0 ? cur.staleUntilAtLeast : add.staleUntilAtLeast;

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

            return new Entry(range, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, locallyDecidedAndAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast);
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

            if (txnId.compareTo(entry.shardAppliedOrInvalidatedBefore) < 0)
                return notRedundant.without(Ranges.of(entry.range));

            return notRedundant;
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

        public final TxnId locallyRedundantBefore()
        {
            return locallyAppliedOrInvalidatedBefore;
        }

        public final TxnId locallyRedundantOrBootstrappedBefore()
        {
            return TxnId.max(locallyAppliedOrInvalidatedBefore, bootstrappedAt);
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

    public TxnId shardRedundantBefore(RoutableKey key)
    {
        Entry entry = get(key);
        if (entry == null)
            return TxnId.NONE;
        return entry.shardAppliedOrInvalidatedBefore;
    }

    public TxnId locallyRedundantBefore(RoutableKey key)
    {
        Entry entry = get(key);
        if (entry == null)
            return TxnId.NONE;
        return entry.locallyAppliedOrInvalidatedBefore;
    }

    public RedundantStatus status(TxnId txnId, EpochSupplier executeAt, Participants<?> participants)
    {   // TODO (required): consider how the use of txnId for executeAt affects exclusive sync points for cleanup
        //    may want to issue synthetic sync points for local evaluation in later epochs
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
    public Ranges removeShardRedundant(TxnId txnId, @Nonnull Timestamp executeAt, Ranges ranges)
    {
        Invariants.checkArgument(executeAt != null, "executeAt must not be null");
        return foldl(ranges, Entry::removeShardRedundant, ranges, txnId, executeAt, r -> false);
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

            return new Entry(v.range.newRange(start, end), v.startEpoch, v.endEpoch, v.locallyAppliedOrInvalidatedBefore, v.shardAppliedOrInvalidatedBefore, v.bootstrappedAt, v.staleUntilAtLeast);
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
            ), a.startEpoch, a.endEpoch, a.locallyAppliedOrInvalidatedBefore, a.shardAppliedOrInvalidatedBefore, a.bootstrappedAt, a.staleUntilAtLeast);
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
