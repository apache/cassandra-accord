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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.ProtocolModifiers;
import accord.api.RoutingKey;
import accord.api.VisibleForImplementation;
import accord.local.RedundantStatus.Coverage;
import accord.local.RedundantStatus.SomeStatus;
import accord.local.RedundantStatus.Property;
import accord.primitives.AbstractRanges;
import accord.primitives.Deps;
import accord.primitives.EpochSupplier;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Functions;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;
import accord.utils.UnhandledEnum;

import static accord.api.ProtocolModifiers.dataStoreRequiresUniqueHlcs;
import static accord.local.RedundantStatus.Coverage.SOME;
import static accord.local.RedundantStatus.ONLY_LE_MASK;
import static accord.local.RedundantStatus.NOT_OWNED_ONLY;
import static accord.local.RedundantStatus.Property.GC_BEFORE;
import static accord.local.RedundantStatus.Property.LOCALLY_APPLIED;
import static accord.local.RedundantStatus.Property.LOCALLY_DEFUNCT;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_COMMAND_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_DURABLE_TO_DATA_STORE;
import static accord.local.RedundantStatus.Property.LOCALLY_REDUNDANT;
import static accord.local.RedundantStatus.Property.LOCALLY_SYNCED;
import static accord.local.RedundantStatus.Property.LOCALLY_WITNESSED;
import static accord.local.RedundantStatus.Property.QUORUM_APPLIED;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED_HLC_BOUND;
import static accord.local.RedundantStatus.Property.UNREADY;
import static accord.local.RedundantStatus.Property.SHARD_APPLIED;
import static accord.local.RedundantStatus.WAS_OWNED_SYNCED;
import static accord.local.RedundantStatus.WAS_OWNED_ONLY;
import static accord.local.RedundantStatus.WAS_OWNED_RETIRED;
import static accord.local.RedundantStatus.addHistory;
import static accord.local.RedundantStatus.any;
import static accord.local.RedundantStatus.shiftedMask;
import static accord.local.RedundantStatus.matchesMask;
import static accord.local.RedundantStatus.toAll;
import static accord.primitives.Routables.Slice.Minimal;
import static accord.primitives.Timestamp.Flag.SHARD_BOUND;
import static accord.utils.ArrayBuffers.cachedAny;
import static accord.utils.ArrayBuffers.cachedInts;
import static accord.utils.Functions.alwaysFalse;
import static accord.utils.Functions.alwaysTrue;
import static accord.utils.Invariants.illegalState;
import static accord.utils.Invariants.require;
import static accord.utils.Invariants.requireStrictlyOrdered;

public class RedundantBefore extends ReducingRangeMap<RedundantBefore.Bounds>
{
    public interface RedundantBeforeSupplier
    {
        RedundantBefore redundantBefore();
    }

    public static class SerializerSupport
    {
        public static RedundantBefore create(RoutingKey[] ends, Bounds[] values)
        {
            return new RedundantBefore(ends, values);
        }
    }

    public static class QuickBounds
    {
        // start inclusive, end exclusive
        public final long startEpoch, endEpoch;
        public final TxnId readyAt;
        public final TxnId gcBefore;
        public final TxnId shardAppliedHlcBoundBefore;
        public final TxnId locallyAppliedBefore;

        public QuickBounds(long startEpoch, long endEpoch, TxnId readyAt, TxnId gcBefore, TxnId shardAppliedHlcBoundBefore, TxnId locallyAppliedBefore)
        {
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
            this.readyAt = readyAt;
            this.gcBefore = gcBefore;
            this.shardAppliedHlcBoundBefore = TxnId.max(gcBefore, shardAppliedHlcBoundBefore); // SHARD_APPLIED_HLC_BOUND introduced later, not guaranteed to be set; this can be removed some time in the future
            this.locallyAppliedBefore = locallyAppliedBefore;
        }

        public QuickBounds withEpochs(long startEpoch, long endEpoch)
        {
            if (startEpoch == this.startEpoch && endEpoch == this.endEpoch)
                return this;
            return new QuickBounds(startEpoch, endEpoch, readyAt, gcBefore, shardAppliedHlcBoundBefore, locallyAppliedBefore);
        }

        public QuickBounds withReadyAtLeast(TxnId newReadyAt)
        {
            if (newReadyAt.compareTo(readyAt) <= 0)
                return this;
            return new QuickBounds(startEpoch, endEpoch, newReadyAt, gcBefore, shardAppliedHlcBoundBefore, locallyAppliedBefore);
        }

        public QuickBounds withLocallyAppliedAtLeast(TxnId newLocallyAppliedBefore)
        {
            if (newLocallyAppliedBefore.compareTo(locallyAppliedBefore) <= 0)
                return this;
            return new QuickBounds(startEpoch, endEpoch, readyAt, gcBefore, shardAppliedHlcBoundBefore, newLocallyAppliedBefore);
        }

        public QuickBounds withShardAppliedHlcBoundBeforeAtLeast(TxnId newShardAppliedHlcBoundBefore)
        {
            if (newShardAppliedHlcBoundBefore.compareTo(this.shardAppliedHlcBoundBefore) <= 0)
                return this;
            // we can't let HLC epoch go backwards as this breaks assumptions around maxUniqueHlc tracking
            if (newShardAppliedHlcBoundBefore.hlc() < this.shardAppliedHlcBoundBefore.hlc())
                return this;
            return new QuickBounds(startEpoch, endEpoch, readyAt, gcBefore, newShardAppliedHlcBoundBefore, locallyAppliedBefore);
        }

        public QuickBounds withGcBeforeAtLeast(TxnId newGcBefore)
        {
            if (newGcBefore.compareTo(this.gcBefore) <= 0)
                return this;
            // we can't let HLC epoch go backwards as this breaks assumptions around maxUniqueHlc tracking
            if (newGcBefore.hlc() < this.gcBefore.hlc())
                return this;
            return new QuickBounds(startEpoch, endEpoch, readyAt, newGcBefore, shardAppliedHlcBoundBefore, locallyAppliedBefore);
        }

        public TxnId cleanCfkBefore()
        {
            switch (ProtocolModifiers.cleanCfkBefore())
            {
                default: throw new UnhandledEnum(ProtocolModifiers.cleanCfkBefore());
                case SHARD_APPLIED: return shardAppliedHlcBoundBefore;
                case GC: return gcBefore;
            }
        }

        public QuickBounds withCleanCfkBeforeAtLeast(TxnId atLeast)
        {
            switch (ProtocolModifiers.cleanCfkBefore())
            {
                default: throw new UnhandledEnum(ProtocolModifiers.cleanCfkBefore());
                case SHARD_APPLIED: return withShardAppliedHlcBoundBeforeAtLeast(atLeast);
                case GC: return withGcBeforeAtLeast(atLeast);
            }
        }

        @Override
        public String toString()
        {
            TreeMap<TxnId, Set<Property>> build = new TreeMap<>(Comparator.reverseOrder());
            build.computeIfAbsent(gcBefore, ignore -> new TreeSet<>(Comparator.reverseOrder())).add(GC_BEFORE);
            build.computeIfAbsent(readyAt, ignore -> new TreeSet<>(Comparator.reverseOrder())).add(UNREADY);
            build.computeIfAbsent(locallyAppliedBefore, ignore -> new TreeSet<>(Comparator.reverseOrder())).add(LOCALLY_APPLIED);
            return build.toString();
        }
    }

    public static class Bounds extends QuickBounds
    {
        public static final Bounds NONE = new Bounds(null, Long.MIN_VALUE, Long.MAX_VALUE, TxnId.NO_TXNIDS, new int[0], null);

        // TODO (desired): we don't need to maintain this now, can migrate to ReducingRangeMap.foldWithBounds
        public final Range range;

        // TODO (expected): we need to eventually support GCing UNREADY bounds
        //  once we know storage layer has fully expunged earlier TxnId
        //  OR we may be able to safely overwrite them with some better invariants and adequate testing
        public final TxnId[] bounds;
        // two entries per bound, first for equality (LE) matches, second for inequality (LT) matches
        private final int[] statuses;

        private transient final long maxBoundEpoch, maxBoundHlc;
        public transient final TxnId depBound;

        /**
         * staleUntilAtLeast provides a minimum TxnId until which we know we will be unable to completely execute
         * transactions locally for the impacted range.
         *
         * See also {@link SafeCommandStore#safeToReadAt()}.
         */
        public final @Nullable Timestamp staleUntilAtLeast;
        private transient RedundantStatus last = RedundantStatus.NONE;

        public Bounds(Range range, long startEpoch, long endEpoch, TxnId[] bounds, int[] statuses, @Nullable Timestamp staleUntilAtLeast)
        {
            super(startEpoch, endEpoch,
                  maxBound(bounds, statuses, UNREADY),
                  maxBound(bounds, statuses, GC_BEFORE),
                  maxBound(bounds, statuses, SHARD_APPLIED_HLC_BOUND),
                  maxBound(bounds, statuses, LOCALLY_APPLIED));
            Invariants.require(statuses.length == bounds.length * 2);
            this.range = range;
            this.bounds = bounds;
            this.statuses = statuses;
            this.staleUntilAtLeast = staleUntilAtLeast;
            this.maxBoundEpoch = bounds.length == 0 ? 0 : bounds[0].epoch();
            this.maxBoundHlc = bounds.length == 0 ? 0 : bounds[0].hlc();
            this.depBound = depBound(bounds, statuses);
            checkMinBoundOrSyncPoint(bounds);
            requireStrictlyOrdered(Comparator.reverseOrder(), bounds);
            require(isShardBound(gcBefore) || isMinBound(gcBefore));
        }

        public final long status(int index)
        {
            return statuses[index] & 0xFFFFFFFFL;
        }

        public static Bounds create(Range range, TxnId bound, SomeStatus status, @Nullable Timestamp staleUntilAtLeast)
        {
            return create(range, Long.MIN_VALUE, Long.MAX_VALUE, bound, status, staleUntilAtLeast);
        }

        public static Bounds create(Range range, long startEpoch, long endEpoch, TxnId bound, SomeStatus status, @Nullable Timestamp staleUntilAtLeast)
        {
            return new Bounds(range, startEpoch, endEpoch, new TxnId[] { bound }, new int[] { (int) (status.encoded & ONLY_LE_MASK), (int)status.encoded }, staleUntilAtLeast);
        }

        private static TxnId depBound(TxnId[] bounds, int[] statuses)
        {
            TxnId depBound = maxBound(bounds, statuses, shiftedMask(SHARD_APPLIED, SOME) | shiftedMask(LOCALLY_APPLIED, SOME));
            if (depBound.equals(TxnId.NONE))
                return null;
            return depBound.addFlag(SHARD_BOUND);
        }

        private static void checkMinBoundOrSyncPoint(TxnId ... txnIds)
        {
            for (TxnId txnId : txnIds)
                checkMinBoundOrSyncPoint(txnId);
        }

        private static void checkMinBoundOrSyncPoint(TxnId txnId)
        {
            Invariants.requireArgument(txnId.domain().isRange() && txnId.isSyncPoint() || isMinBound(txnId));
        }

        private static boolean isMinBound(TxnId txnId)
        {
            return txnId.hlc() == 0 && txnId.flags() == 0 && txnId.node.id == 0;
        }

        private static boolean isShardBound(TxnId txnId)
        {
            return txnId.domain().isRange() && txnId.isSyncPoint() && txnId.is(SHARD_BOUND);
        }

        public static Bounds reduce(Bounds a, Bounds b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Bounds merge(Range range, Bounds cur, Bounds add)
        {
            // TODO (required): we shouldn't be trying to merge non-intersecting epochs
            if (cur.startEpoch > add.endEpoch)
                return cur;

            if (add.startEpoch > cur.endEpoch)
                return add;

            int csu = compareStaleUntilAtLeast(cur.staleUntilAtLeast, add.staleUntilAtLeast);
            Timestamp staleUntilAtLeast = csu >= 0 ? cur.staleUntilAtLeast : add.staleUntilAtLeast;
            staleUntilAtLeast = maybeClearStaleUntilAtLeast(staleUntilAtLeast, cur.bounds, cur.statuses);
            staleUntilAtLeast = maybeClearStaleUntilAtLeast(staleUntilAtLeast, add.bounds, add.statuses);

            long startEpoch = Long.max(cur.startEpoch, add.startEpoch);
            long endEpoch = Long.min(cur.endEpoch, add.endEpoch);
            TxnId[] mergedBounds;
            int[] mergedStatuses;
            {
                Object[] boundBuf = null;
                int[] statusBuf = null;
                int mergedCount = 0;
                long prevLtStatus = 0;
                long prevExistingLtStatus = prevLtStatus; // we don't apply UNREADY_MERGE_MASK as this already applied
                int i = 0, j = 0;
                while (i < cur.bounds.length || j < add.bounds.length)
                {
                    int c = i == cur.bounds.length ? -1 : j == add.bounds.length ? 1 : cur.bounds[i].compareTo(add.bounds[j]);
                    TxnId nextBound;
                    long leStatus, ltStatus;
                    if (c > 0)
                    {
                        nextBound = cur.bounds[i];
                        leStatus = addHistory(prevLtStatus, cur.status(i*2));
                        ltStatus = addHistory(prevLtStatus, prevExistingLtStatus = cur.status(i*2+1));
                        ++i;
                    }
                    else if (c < 0)
                    {
                        nextBound = add.bounds[j];
                        leStatus = addHistory(prevLtStatus | add.status(j * 2), prevExistingLtStatus);
                        ltStatus = addHistory(prevLtStatus | add.status(j * 2 + 1), prevExistingLtStatus);
                        ++j;
                    }
                    else
                    {
                        nextBound = cur.bounds[i].addFlags(add.bounds[j]);
                        leStatus = addHistory(prevLtStatus | add.status(j * 2), cur.status(i * 2));
                        ltStatus = addHistory(prevLtStatus | add.status(j * 2 + 1), prevExistingLtStatus = cur.status(i * 2 + 1));
                        ++i;
                        ++j;
                    }

                    // we keep the start/end bound of an equal pre-bootstrap run, so that we correctly apply Property.mergeWithUnready
                    if (leStatus == ltStatus && ltStatus == prevLtStatus)
                    {
                        if (!any(prevLtStatus, UNREADY))
                            continue;

                        if (mergedCount >= 2)
                        {
                            int[] prev = statusBuf != null ? statusBuf : cur.statuses;
                            long prev2LtStatus = 0xFFFFFFFFL & prev[mergedCount*2 - 3];
                            long prevLeStatus = 0xFFFFFFFFL & prev[mergedCount*2 - 2];
                            Invariants.require(prevLtStatus == (0xFFFFFFFFL & prev[mergedCount*2 - 1]));
                            if (prevLtStatus == prev2LtStatus && prevLeStatus == prev2LtStatus)
                                --mergedCount;
                        }
                    }

                    if (boundBuf == null)
                    {
                        if (mergedCount < cur.bounds.length && cur.bounds[mergedCount].equalsStrict(nextBound)
                            && cur.status(mergedCount*2) == leStatus && cur.status(mergedCount*2+1) == ltStatus)
                        {
                            prevLtStatus = ltStatus;
                            ++mergedCount;
                            continue;
                        }

                        boundBuf = cachedAny().get(cur.bounds.length + add.bounds.length);
                        statusBuf = cachedInts().getInts((cur.bounds.length + add.bounds.length) * 2);
                        System.arraycopy(cur.bounds, 0, boundBuf, 0, mergedCount);
                        System.arraycopy(cur.statuses, 0, statusBuf, 0, mergedCount*2);
                    }
                    boundBuf[mergedCount] = nextBound;
                    statusBuf[mergedCount*2] = (int) leStatus;
                    statusBuf[mergedCount*2 + 1] = (int) ltStatus;
                    ++mergedCount;
                    prevLtStatus = ltStatus;
                }

                if (boundBuf == null)
                {
                    mergedBounds = mergedCount == cur.bounds.length ? cur.bounds : Arrays.copyOf(cur.bounds, mergedCount);
                    mergedStatuses = mergedCount == cur.statuses.length ? cur.statuses : Arrays.copyOf(cur.statuses, mergedCount * 2);
                }
                else
                {
                    mergedBounds = new TxnId[mergedCount];
                    mergedStatuses = new int[mergedCount*2];
                    System.arraycopy(boundBuf, 0, mergedBounds, 0, mergedCount);
                    System.arraycopy(statusBuf, 0, mergedStatuses, 0, mergedCount*2);
                    cachedAny().forceDiscard(boundBuf, mergedCount);
                    cachedInts().forceDiscard(statusBuf);
                }
            }

            return new Bounds(range, startEpoch, endEpoch, mergedBounds, mergedStatuses, staleUntilAtLeast);
        }

        private static Timestamp maybeClearStaleUntilAtLeast(Timestamp staleUntilAtLeast, TxnId[] bounds, int[] statuses)
        {
            for (int i = 0 ; staleUntilAtLeast != null && i < bounds.length && bounds[i].compareTo(staleUntilAtLeast) > 0 ; ++i)
            {
                if (any(statuses[i], UNREADY))
                    staleUntilAtLeast = null;
            }
            return staleUntilAtLeast;
        }

        public Bounds with(TxnId newBound, SomeStatus addStatus)
        {
            // TODO (desired): introduce special-cased faster merge for adding a single value
            return merge(range, this, new Bounds(range, Long.MIN_VALUE, Long.MAX_VALUE, new TxnId[] { newBound }, new int[] { (int)addStatus.encoded }, null));
        }

        @VisibleForImplementation
        public Bounds withEpochs(long start, long end)
        {
            return new Bounds(range, start, end, bounds, statuses, staleUntilAtLeast);
        }

        static @Nonnull Boolean isShardOnlyApplied(Bounds bounds, @Nonnull Boolean prev, TxnId txnId)
        {
            return is(bounds, prev, txnId, SHARD_APPLIED);
        }

        static @Nonnull Boolean is(Bounds bounds, @Nonnull Boolean prev, TxnId txnId, Property property)
        {
            return bounds == null ? prev : bounds.is(txnId, property);
        }

        boolean is(TxnId txnId, Property a, Property b)
        {
            Invariants.require(a != GC_BEFORE && b != GC_BEFORE);
            if (a.compareLessEqual != b.compareLessEqual)
                return is(txnId, a) && is(txnId, b);

            long propertyMask = shiftedMask(a, SOME) | shiftedMask(b, SOME);
            if (noBoundMatches(txnId))
                return false;

            int i = findStatusIndexInternal(txnId);
            return i >= 0 && matchesMask(statuses[i], propertyMask);
        }

        boolean is(TxnId txnId, Property test)
        {
            Invariants.require(test != GC_BEFORE);
            if (noBoundMatches(txnId))
                return false;

            if (test.mergeWithUnready)
                return txnId.compareTo(bound(test)) <= test.compareLessEqual;

            int i = findStatusIndexInternal(txnId);
            return i >= 0 && any(statuses[i], test);
        }

        TxnId bound(Property property)
        {
            Invariants.require(property != GC_BEFORE);
            Invariants.require(property.mergeWithUnready);
            return maxBound(property);
        }

        TxnId bound(Property a, Property b)
        {
            Invariants.require(a != GC_BEFORE);
            Invariants.require(a.mergeWithUnready);
            Invariants.require(b != GC_BEFORE);
            Invariants.require(b.mergeWithUnready);
            return maxBoundBoth(a, b);
        }

        boolean noBoundMatches(TxnId txnId)
        {
            long epoch = txnId.epoch();
            return epoch > maxBoundEpoch || (epoch == maxBoundEpoch && txnId.hlc() > maxBoundHlc);
        }

        int findStatusIndex(TxnId txnId)
        {
            if (noBoundMatches(txnId))
                return -1;
            return findStatusIndexInternal(txnId);
        }

        int findStatusIndexInternal(TxnId txnId)
        {
            int i = 0;
            while (i < bounds.length)
            {
                int c = txnId.compareTo(bounds[i]);
                if (c >= 0)
                    return i * 2 - (c == 0 ? 0 : 1);
                ++i;
            }
            return statuses.length - 1;
        }

        public TxnId maxBound(Property property)
        {
            return maxBound(bounds, statuses, property);
        }

        public TxnId maxBoundBoth(Property a, Property b)
        {
            return maxBound(bounds, statuses, shiftedMask(a, SOME) | shiftedMask(b, SOME));
        }

        static TxnId maxBound(TxnId[] bounds, int[] statuses, Property property)
        {
            return maxBound(bounds, statuses, shiftedMask(property, SOME));
        }

        static TxnId maxBound(TxnId[] bounds, int[] statuses, long propertyMask)
        {
            // we ignore LT/LE, as this should be applied by the caller as part of comparing maxBound
            for (int i = 1; i < statuses.length ; i += 2)
            {
                if ((statuses[i] & propertyMask) == propertyMask)
                    return bounds[i/2];
            }
            return TxnId.NONE;
        }

        static @Nullable RedundantStatus getAndMerge(Bounds bounds, @Nullable RedundantStatus prev, TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            if (bounds == null)
                return prev;
            RedundantStatus next = bounds.get(txnId, executeAtIfKnown);
            return prev == null ? next : prev.mergeShards(next);
        }

        static RangeDeps.BuilderByRange collectDep(Bounds bounds, @Nonnull RangeDeps.BuilderByRange prev, @Nonnull EpochSupplier minEpoch, @Nonnull EpochSupplier executeAt)
        {
            // we report an RX that represents a point on or after our GC bound, so that we never report an incomplete
            // transitive dependency history. If we consistently only GC'd at gcBefore we could report this bound,
            // but since it is likely safe to use this bound in cases that don't have lagged durability,
            // we conservatively report this bound since it is expected to be applied already at all non-stale shards
            if (bounds != null && bounds.depBound != null)
                prev.add(bounds.range, bounds.depBound);

            return prev;
        }

        static Ranges validateSafeToRead(Bounds bounds, @Nonnull Ranges safeToRead, Timestamp bootstrapAt, Object ignore)
        {
            if (bounds == null)
                return safeToRead;

            if (bootstrapAt.compareTo(bounds.maxReadyAt()) < 0 || (bounds.staleUntilAtLeast != null && bootstrapAt.compareTo(bounds.staleUntilAtLeast) < 0))
                return safeToRead.without(Ranges.of(bounds.range));

            return safeToRead;
        }

        static TxnId min(Bounds bounds, @Nullable TxnId min, Function<Bounds, TxnId> get)
        {
            if (bounds == null)
                return min;

            return TxnId.nonNullOrMin(min, get.apply(bounds));
        }

        static TxnId max(Bounds bounds, @Nullable TxnId max, Function<Bounds, TxnId> get)
        {
            if (bounds == null)
                return max;

            return TxnId.nonNullOrMax(max, get.apply(bounds));
        }

        static Participants<?> withoutLostAtExecutionOrUnready(Bounds bounds, @Nonnull Participants<?> execute, TxnId txnId, EpochSupplier executeAt)
        {
            if (bounds == null)
                return execute;

            if (bounds.endEpoch <= executeAt.epoch() || bounds.is(txnId, UNREADY))
                return execute.without(Ranges.of(bounds.range));

            return execute;
        }

        static Participants<?> withoutUnreadyOrLocallyRetired(Bounds bounds, @Nonnull Participants<?> execute, TxnId txnId)
        {
            if (bounds == null)
                return execute;

            if (bounds.is(txnId, UNREADY) || bounds.isLocallyRetired())
                return execute.without(Ranges.of(bounds.range));

            return execute;
        }

        static Participants<?> withoutRedundantAnd_Unready(Bounds bounds, @Nonnull Participants<?> execute, TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (bounds == null)
                return execute;

            boolean outOfBounds = executeAt == null ? bounds.outOfBounds(txnId) : bounds.outOfBounds(txnId, executeAt);
            Invariants.expect(!outOfBounds, "Trying to apply withoutRedundantAnd_Unready to %s for a range we don't own (%s), suggesting we computed ownership without an up-to-date epoch", txnId, bounds);
            if (outOfBounds || bounds.is(txnId, SHARD_APPLIED, UNREADY))
                return execute.without(Ranges.of(bounds.range));

            return execute;
        }

        static Participants<?> withoutRedundantAnd_UnreadyOrRetiredOrNotOwned(Bounds bounds, @Nonnull Participants<?> execute, TxnId txnId, @Nullable Timestamp executeAtIfKnown)
        {
            if (bounds == null)
                return execute;

            // TODO (required): audit each of these methods
            if (bounds.is(txnId, SHARD_APPLIED)
                && ((bounds.endEpoch <= txnId.epoch() && bounds.isLocallyRetired())
                    || (executeAtIfKnown != null && executeAtIfKnown.epoch() < bounds.startEpoch)
                    || bounds.is(txnId, UNREADY)))
                return execute.without(Ranges.of(bounds.range));

            return execute;
        }

        static Participants<?> withoutShardApplied(Bounds bounds, @Nonnull Participants<?> notShardApplied, TxnId txnId)
        {
            if (bounds == null)
                return notShardApplied;

            if (bounds.is(txnId, SHARD_APPLIED))
                return notShardApplied.without(Ranges.of(bounds.range));

            return notShardApplied;
        }

        static Participants<?> withoutShardAppliedLocallySynced(Bounds bounds, @Nonnull Participants<?> notShardAppliedLocallySynced, TxnId txnId)
        {
            if (bounds == null)
                return notShardAppliedLocallySynced;

            if (bounds.isRetired() || bounds.is(txnId, SHARD_APPLIED, LOCALLY_SYNCED))
                return notShardAppliedLocallySynced.without(Ranges.of(bounds.range));

            return notShardAppliedLocallySynced;
        }

        static Ranges withoutBeforeGc(Bounds entry, @Nonnull Ranges notGarbage, TxnId txnId, @Nullable Timestamp executeAt)
        {
            if (entry == null || (executeAt == null ? entry.outOfBounds(txnId) : entry.outOfBounds(txnId, executeAt)))
                return notGarbage;

            if (txnId.compareTo(entry.gcBefore) < 0)
                return notGarbage.without(Ranges.of(entry.range));

            return notGarbage;
        }

        static Ranges withoutWitnessed(Bounds entry, @Nonnull Ranges notWitnessed, TxnId txnId)
        {
            if (entry == null || entry.outOfBounds(txnId))
                return notWitnessed;

            if (txnId.compareTo(entry.locallyWitnessedBefore()) < 0)
                return notWitnessed.without(Ranges.of(entry.range));

            return notWitnessed;
        }

        static Ranges withoutRetired(Bounds bounds, @Nonnull Ranges notRetired)
        {
            return withoutRetired(bounds, notRetired, b -> b.maxBound(SHARD_APPLIED));
        }

        static Ranges withoutLocallyRetired(Bounds bounds, @Nonnull Ranges notRetired)
        {
            return withoutRetired(bounds, notRetired, b -> b.maxBound(LOCALLY_APPLIED));
        }

        static Ranges withoutQuorumAndLocallyRetired(Bounds bounds, @Nonnull Ranges notRetired)
        {
            return withoutRetired(bounds, notRetired, b -> b.maxBoundBoth(QUORUM_APPLIED, LOCALLY_APPLIED));
        }

        private static Ranges withoutRetired(Bounds bounds, @Nonnull Ranges notRetired, Function<Bounds, TxnId> getBound)
        {
            if (bounds == null || bounds.endEpoch > getBound.apply(bounds).epoch())
                return notRetired;

            return notRetired.without(Ranges.of(bounds.range));
        }

        public RedundantStatus get(TxnId txnId, @Nullable Timestamp applyAtIfKnown)
        {
            if (wasOwned(txnId))
            {
                if (isRetired()) return WAS_OWNED_RETIRED;
                if (isLocallyRetired()) return WAS_OWNED_SYNCED;
                return WAS_OWNED_ONLY;
            }
            return getIgnoringOwnership(txnId, applyAtIfKnown);
        }

        RedundantStatus getIgnoringOwnership(TxnId txnId, @Nullable Timestamp applyAtIfKnown)
        {
            int i = findStatusIndex(txnId);
            if (i < 0)
                return RedundantStatus.NONE;

            long status = status(i);
            if (any(status, GC_BEFORE))
            {
                if (dataStoreRequiresUniqueHlcs() && txnId.isWrite())
                {
                    if (applyAtIfKnown == null)
                    {
                        // if this is partial input (e.g. compaction) we don't want to infer ERASE
                        // we anyway only EXPUNGE on summary information before this point
                        status &= ~(1L << GC_BEFORE.shift());
                    }
                    else if (bounds[i/2].hlc() <= applyAtIfKnown.uniqueHlc())
                    {
                        long uniqueHlc = applyAtIfKnown.uniqueHlc();
                        i/= 2;
                        while (--i >= 0 && bounds[i].hlc() <= uniqueHlc) {}
                        if (i < 0 || !any(statuses[i*2+1], GC_BEFORE))
                            status &= ~(1L << GC_BEFORE.shift());
                    }
                }
            }

            RedundantStatus reuse = last;
            if (status == reuse.encoded)
                return reuse;
            return last = new RedundantStatus(toAll(status));
        }

        private static int compareStaleUntilAtLeast(@Nullable Timestamp a, @Nullable Timestamp b)
        {
            boolean aIsNull = a == null, bIsNull = b == null;
            if (aIsNull != bIsNull) return aIsNull ? -1 : 1;
            return aIsNull ? 0 : a.compareTo(b);
        }

        public final TxnId gcBefore()
        {
            return gcBefore;
        }

        public final TxnId shardAndLocallyRedundantBefore()
        {
            return bound(SHARD_APPLIED, LOCALLY_REDUNDANT);
        }

        public final TxnId shardAppliedBefore()
        {
            return bound(SHARD_APPLIED);
        }

        public final TxnId shardAppliedHlcBoundBefore()
        {
            return bound(SHARD_APPLIED_HLC_BOUND);
        }

        public final TxnId locallyWitnessedBefore()
        {
            return bound(LOCALLY_WITNESSED);
        }

        public final TxnId locallyRedundantBefore()
        {
            return bound(LOCALLY_REDUNDANT);
        }

        // we may not have actually applied all earlier TxnId
        // TODO (expected): use LOCALLY_SYNCED?
        public final TxnId maxLocallyAppliedBefore()
        {
            return maxBound(LOCALLY_APPLIED);
        }

        public final TxnId maxReadyAt()
        {
            return readyAt;
        }

        private boolean outOfBounds(EpochSupplier lb, EpochSupplier ub)
        {
            return ub.epoch() < startEpoch || lb.epoch() >= endEpoch;
        }

        // TODO (expected): should we consider executeAt here?
        //  Anyone that didn't own txnId will have to bootstrap to include this transaction anyway, so it's safe
        private boolean wasOwned(EpochSupplier lb)
        {
            return lb.epoch() >= endEpoch;
        }

        public boolean isRetired()
        {
            return endEpoch <= maxBoundBoth(SHARD_APPLIED, LOCALLY_SYNCED).epoch();
        }

        public boolean isLocallyRetired()
        {
            return endEpoch <= maxBound(LOCALLY_SYNCED).epoch();
        }

        public boolean hasLostOwnership()
        {
            return endEpoch < Long.MAX_VALUE;
        }

        public boolean isLocallyRetiredOrUnready(TxnId txnId)
        {
            return isLocallyRetired() || is(txnId, UNREADY);
        }

        private boolean outOfBounds(Timestamp lb)
        {
            return lb.epoch() >= endEpoch;
        }

        public Bounds withRange(Range range)
        {
            return new Bounds(range, startEpoch, endEpoch, bounds, statuses, staleUntilAtLeast);
        }

        public boolean equals(Object that)
        {
            return that instanceof Bounds && equals((Bounds) that);
        }

        public boolean equals(Bounds that)
        {
            return this.range.equals(that.range) && equalsIgnoreRange(that);
        }

        public boolean equalsIgnoreRange(Bounds that)
        {
            return this.startEpoch == that.startEpoch
                   && this.endEpoch == that.endEpoch
                   && Arrays.equals(this.bounds, that.bounds)
                   && Arrays.equals(this.statuses, that.statuses)
                   && Objects.equals(this.staleUntilAtLeast, that.staleUntilAtLeast);
        }

        private static final Property[] PROPERTIES = Property.values();

        @Override
        public String toString()
        {
            TreeMap<TxnId, Set<Property>> build = new TreeMap<>(Comparator.reverseOrder());
            for (Property property : PROPERTIES)
                build.computeIfAbsent(maxBound(property), ignore -> new TreeSet<>(Comparator.reverseOrder()))
                     .add(property);
            return build.toString();
        }
    }

    public static RedundantBefore EMPTY = new RedundantBefore();

    private final Ranges staleRanges, lostRanges;
    private final TxnId maxStale, maxShardAppliedBefore, maxGcBefore;
    private final TxnId minShardAndLocallyAppliedBefore, minGcBefore;
    private final long minGcHlcBefore;
    private final long maxStartEpoch, minEndEpoch;

    private RedundantBefore()
    {
        staleRanges = lostRanges = Ranges.EMPTY;
        maxStale = maxShardAppliedBefore = maxGcBefore = TxnId.NONE;
        minShardAndLocallyAppliedBefore = minGcBefore = TxnId.NONE;
        minGcHlcBefore = 0L;
        maxStartEpoch = 0;
        minEndEpoch = Long.MAX_VALUE;
    }

    RedundantBefore(RoutingKey[] starts, Bounds[] values)
    {
        super(starts, values);
        staleRanges = extractRanges(values, b -> b.staleUntilAtLeast != null);
        lostRanges = extractRanges(values, Bounds::hasLostOwnership);
        TxnId maxUnready = TxnId.NONE, maxGcBefore = TxnId.NONE, maxShardAppliedBefore = TxnId.NONE;
        TxnId minShardAndLocallyRedundantBefore = TxnId.MAX, minGcBefore = TxnId.MAX;
        long minGcHlcBefore = Long.MAX_VALUE;
        long maxStartEpoch = 0, minEndEpoch = Long.MAX_VALUE;
        for (Bounds bounds : values)
        {
            if (bounds == null)
                continue;

            {
                TxnId unready = bounds.maxBound(UNREADY);
                if (unready.compareTo(maxUnready) > 0)
                    maxUnready = unready;
            }
            {
                TxnId gcBefore = bounds.maxBound(GC_BEFORE);
                if (gcBefore.compareTo(maxGcBefore) > 0)
                    maxGcBefore = gcBefore;
                if (gcBefore.compareTo(minGcBefore) < 0)
                    minGcBefore = gcBefore;
                minGcHlcBefore = Math.min(gcBefore.hlc(), minGcHlcBefore);
            }
            {
                TxnId shardAndLocallyRedundantBefore = bounds.shardAndLocallyRedundantBefore();
                TxnId shardAppliedBefore = bounds.shardAppliedBefore();
                if (shardAndLocallyRedundantBefore.compareTo(minShardAndLocallyRedundantBefore) < 0)
                    minShardAndLocallyRedundantBefore = shardAndLocallyRedundantBefore;
                if (shardAppliedBefore.compareTo(maxShardAppliedBefore) > 0)
                    maxShardAppliedBefore = shardAppliedBefore;
            }
            if (bounds.startEpoch > maxStartEpoch)
                maxStartEpoch = bounds.startEpoch;
            if (bounds.endEpoch < minEndEpoch)
                minEndEpoch = bounds.endEpoch;
        }

        Invariants.require(minGcHlcBefore < Long.MAX_VALUE);
        this.maxStale = maxUnready;
        this.maxShardAppliedBefore = maxShardAppliedBefore;
        this.maxGcBefore = maxGcBefore;
        this.minShardAndLocallyAppliedBefore = minShardAndLocallyRedundantBefore;
        this.minGcBefore = minGcBefore;
        this.minGcHlcBefore = minGcHlcBefore;
        this.maxStartEpoch = maxStartEpoch;
        this.minEndEpoch = minEndEpoch;
        checkParanoid(starts, values);
    }

    private static Ranges extractRanges(Bounds[] values, Predicate<Bounds> include)
    {
        int count = 0;
        for (Bounds bounds : values)
        {
            if (bounds != null && include.test(bounds))
                ++count;
        }

        if (count == 0)
            return Ranges.EMPTY;

        Range[] result = new Range[count];
        count = 0;
        for (Bounds bounds : values)
        {
            if (bounds != null && include.test(bounds))
                result[count++] = bounds.range;
        }
        return Ranges.ofSortedAndDeoverlapped(result).mergeTouching();
    }

    public static RedundantBefore create(AbstractRanges ranges, TxnId bound, SomeStatus status)
    {
        return create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, bound, status);
    }

    public static RedundantBefore createStale(AbstractRanges ranges, Timestamp staleUntilAtLeast)
    {
        return create(ranges, Long.MIN_VALUE, Long.MAX_VALUE, TxnId.NONE, SomeStatus.NONE, staleUntilAtLeast);
    }

    public static RedundantBefore create(AbstractRanges ranges, long startEpoch, long endEpoch, TxnId bound, SomeStatus status)
    {
        return create(ranges, startEpoch, endEpoch, bound, status, null);
    }

    public static RedundantBefore create(AbstractRanges ranges, long startEpoch, long endEpoch, TxnId bound, SomeStatus status, @Nullable Timestamp staleUntilAtLeast)
    {
        if (ranges.isEmpty())
            return EMPTY;

        Bounds bounds = new Bounds(null, startEpoch, endEpoch, new TxnId[] { bound }, new int[] { (int) (status.encoded & ONLY_LE_MASK), (int)status.encoded }, staleUntilAtLeast);
        Builder builder = new Builder(ranges.size() * 2);
        for (int i = 0 ; i < ranges.size() ; ++i)
        {
            Range cur = ranges.get(i);
            builder.append(cur.start(), cur.end(), bounds.withRange(cur));
        }
        return builder.build();
    }

    public static RedundantBefore merge(RedundantBefore a, RedundantBefore b)
    {
        return ReducingIntervalMap.mergeIntervals(a, b, Builder::new);
    }

    public RedundantStatus status(TxnId txnId, @Nullable Timestamp applyAtIfKnown, Participants<?> participants)
    {
        RedundantStatus result = foldl(participants, Bounds::getAndMerge, null, txnId, applyAtIfKnown);
        return result == null ? NOT_OWNED_ONLY : result;
    }

    public RedundantStatus status(TxnId txnId, @Nullable Timestamp applyAtIfKnown, RoutingKey key)
    {
        Bounds bounds = get(key);
        return bounds == null ? NOT_OWNED_ONLY : bounds.get(txnId, applyAtIfKnown);
    }

    public boolean isShardOnlyApplied(TxnId txnId, Unseekables<?> participants)
    {
        return foldl(participants, Bounds::isShardOnlyApplied, false, txnId);
    }

    /**
     * RedundantStatus.REDUNDANT overrides PRE_BOOTSTRAP; to avoid complicating that state machine,
     * for cases where we care independently about the overall pre-bootstrap state we have a separate mechanism
     */
    public boolean isLocallyDefunct(TxnId txnId, Participants<?> participants, Coverage coverage)
    {
        return status(txnId, null, participants).is(LOCALLY_DEFUNCT, coverage);
    }

    public <T extends Deps> RangeDeps.BuilderByRange collectDeps(Routables<?> participants, RangeDeps.BuilderByRange builder, EpochSupplier minEpoch, EpochSupplier executeAt)
    {
        return foldl(participants, Bounds::collectDep, builder, minEpoch, executeAt);
    }

    public Ranges validateSafeToRead(Timestamp forBootstrapAt, Ranges ranges)
    {
        return foldl(ranges, Bounds::validateSafeToRead, ranges, forBootstrapAt, null);
    }

    public TxnId min(Routables<?> participants, Function<Bounds, TxnId> get)
    {
        return TxnId.nonNullOrMax(TxnId.NONE, foldl(participants, Bounds::min, null, get));
    }

    public TxnId max(Routables<?> participants, Function<Bounds, TxnId> get)
    {
        return foldl(participants, Bounds::max, TxnId.NONE, get);
    }

    /**
     * Subtract any ranges that are before a GC point
     */
    @VisibleForImplementation
    public Ranges removeGcBefore(TxnId txnId, @Nonnull Timestamp executeAt, Ranges ranges)
    {
        Invariants.requireArgument(executeAt != null, "executeAt must not be null");
        if (txnId.compareTo(maxGcBefore) >= 0)
            return ranges;
        return foldl(ranges, Bounds::withoutBeforeGc, ranges, txnId, executeAt);
    }

    /**
     * Subtract any ranges that are before a GC point
     */
    @VisibleForImplementation
    public Ranges removeWitnessed(TxnId txnId, Ranges ranges)
    {
        return foldl(ranges, Bounds::withoutWitnessed, ranges, txnId);
    }

    public Ranges removeRetired(Ranges ranges)
    {
        return removeRetired(ranges, Bounds::withoutRetired);
    }

    public Ranges removeLocallyRetired(Ranges ranges)
    {
        return removeRetired(ranges, Bounds::withoutLocallyRetired);
    }

    public Ranges removeQuorumAndLocallyRetired(Ranges ranges)
    {
        return removeRetired(ranges, Bounds::withoutQuorumAndLocallyRetired);
    }

    private Ranges removeRetired(Ranges ranges, BiFunction<Bounds, Ranges, Ranges> fold)
    {
        if (!lostRanges.intersects(ranges))
            return ranges;

        return foldl(ranges, fold, ranges, alwaysFalse());
    }

    public Ranges removeLostOrStale(Ranges ranges)
    {
        return ranges.without(lostRanges).without(staleRanges);
    }

    public TxnId minShardAndLocallyAppliedBefore()
    {
        return minShardAndLocallyAppliedBefore;
    }

    public TxnId minGcBefore()
    {
        return minGcBefore;
    }

    public long minGcHlcBefore()
    {
        return minGcHlcBefore;
    }

    public TxnId maxGcBefore()
    {
        return maxGcBefore;
    }

    /**
     * Subtract anything we don't need to coordinate (because they are known to be shard durable),
     * and we don't execute locally, i.e. are pre-bootstrap or stale (or for RX are on ranges that are already retired)
     *
     * TODO (required): document and justify all of this, in terms of invariants we're maintaining
     */
    public Participants<?> expectToOwn(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
    {
        if (txnId.isSyncPoint())
        {
            if (!mayFilterUnreadyOrNotOwned(txnId, executeAt, participants))
                return participants;

            return foldl(participants, Bounds::withoutRedundantAnd_UnreadyOrRetiredOrNotOwned, participants, txnId, executeAt);
        }
        else
        {
            if (!mayFilterStale(txnId, participants))
                return participants;

            return foldl(participants, Bounds::withoutRedundantAnd_Unready, participants, txnId, executeAt);
        }
    }

    public boolean isAtLeast(RedundantBefore atLeast)
    {
        if (!ranges().containsAll(atLeast.ranges()))
            return false;

        return foldl((b, v, al, e) -> {
            return al.foldl(Ranges.of(b.range), (lb, v2, ub) -> {
                if (!v2 || ub.endEpoch > lb.endEpoch || ub.startEpoch != lb.startEpoch)
                    return false;

                int j = 0;
                for (int i = 0 ; i < lb.bounds.length ; ++i)
                {
                    TxnId bound = lb.bounds[i];
                    j = Arrays.binarySearch(ub.bounds, j, ub.bounds.length, bound, Comparator.reverseOrder());
                    if (j < 0) j = -2 - j;
                    if (j < 0
                        || (lb.status(i*2) & ~ub.status(j*2)) != 0
                        || (lb.status(i*2+1) & ~ub.status(j*2+1)) != 0)
                        return false;
                }
                return true;
            }, v, b);
        }, true, atLeast, null, i -> !i);
    }

    /**
     * Subtract anything we won't execute locally, i.e. are pre-bootstrap or stale (or for RX are on ranges that are already retired)
     * but also anything that executes in an epoch we don't own
     */
    public Participants<?> expectToExecute(TxnId txnId, @Nonnull Timestamp executeAt, Participants<?> participants)
    {
        Invariants.require(executeAt != null);
        return foldl(participants, Bounds::withoutLostAtExecutionOrUnready, (Participants)participants, txnId, executeAt);
    }

    /**
     * Subtract anything we won't execute locally, i.e. are pre-bootstrap or stale or retired
     */
    public Participants<?> expectExclusiveSyncPointToWaitOn(TxnId txnId, @Nonnull Timestamp executeAt, Participants<?> participants)
    {
        Invariants.require(executeAt != null);
        Invariants.require(txnId.isSyncPoint());
        return foldl(participants, Bounds::withoutUnreadyOrLocallyRetired, participants, txnId);
    }

    public boolean mayFilter(TxnId txnId, @Nullable Timestamp executeAtIfKnown, Participants<?> participants)
    {
        return mayFilterUnreadyOrNotOwned(txnId, executeAtIfKnown, participants);
    }

    private boolean mayFilterUnreadyOrNotOwned(TxnId txnId, @Nullable Timestamp executeAt, Participants<?> participants)
    {
        long maxEpoch = (executeAt == null ? txnId : executeAt).epoch();
        return (minEndEpoch <= maxEpoch && lostRanges.intersects(participants))
               || (executeAt != null && executeAt.epoch() < maxStartEpoch)
               || mayFilterStale(txnId, participants);
    }

    private boolean mayFilterStale(TxnId txnId, Participants<?> participants)
    {
        return maxStale.compareTo(txnId) > 0 || (staleRanges != null && staleRanges.intersects(participants));
    }

    /**
     * Subtract any ranges we consider stale, pre-bootstrap, or that were previously owned and have been retired
     */
    public Participants<?> withoutShardApplied(TxnId txnId, Participants<?> participants)
    {
        if (participants.isEmpty() || maxShardAppliedBefore.compareTo(txnId) < 0)
            return participants;
        return foldl(participants, Bounds::withoutShardApplied, participants, txnId);
    }

    /**
     * Subtract any ranges we consider stale, pre-bootstrap, or that were previously owned and have been retired
     */
    public Participants<?> withoutShardAppliedLocallySynced(TxnId txnId, Participants<?> participants)
    {
        if (participants.isEmpty() || maxShardAppliedBefore.compareTo(txnId) < 0)
            return participants;
        return foldl(participants, Bounds::withoutShardAppliedLocallySynced, participants, txnId);
    }

    public static class Builder extends AbstractIntervalBuilder<RoutingKey, Bounds, RedundantBefore>
    {
        public Builder(int capacity)
        {
            super(capacity);
        }

        @Override
        protected Bounds slice(RoutingKey start, RoutingKey end, Bounds v)
        {
            if (v.range.start().equals(start) && v.range.end().equals(end))
                return v;

            return new Bounds(v.range.newRange(start, end), v.startEpoch, v.endEpoch, v.bounds, v.statuses, v.staleUntilAtLeast);
        }

        @Override
        protected Bounds reduce(Bounds a, Bounds b)
        {
            return Bounds.reduce(a, b);
        }

        @Override
        protected Bounds tryMergeEqual(Bounds a, Bounds b)
        {
            if (!a.equalsIgnoreRange(b))
                return null;

            Invariants.require(a.range.compareIntersecting(b.range) == 0 || a.range.end().equals(b.range.start()) || a.range.start().equals(b.range.end()));
            return new Bounds(a.range.newRange(
                a.range.start().compareTo(b.range.start()) <= 0 ? a.range.start() : b.range.start(),
                a.range.end().compareTo(b.range.end()) >= 0 ? a.range.end() : b.range.end()
            ), a.startEpoch, a.endEpoch, a.bounds, a.statuses, a.staleUntilAtLeast);
        }

        @Override
        public void append(RoutingKey start, RoutingKey end, @Nonnull Bounds value)
        {
            if (value.range.start().compareTo(start) != 0 || value.range.end().compareTo(end) != 0)
                throw illegalState();
            super.append(start, end, value);
        }

        @Override
        protected RedundantBefore buildInternal()
        {
            return new RedundantBefore(starts.toArray(new RoutingKey[0]), values.toArray(new Bounds[0]));
        }
    }

    private static void checkParanoid(RoutingKey[] starts, Bounds[] values)
    {
        if (!Invariants.isParanoid())
            return;

        for (int i = 0 ; i < values.length ; ++i)
        {
            if (values[i] != null)
            {
                Invariants.requireArgument(starts[i].equals(values[i].range.start()));
                Invariants.requireArgument(starts[i + 1].equals(values[i].range.end()));
            }
        }
    }

    public final void removeRedundantDependencies(Unseekables<?> participants, Command.WaitingOn.Update builder)
    {
        // Note: we do not need to track the bootstraps we implicitly depend upon, because we will not serve any read requests until this has completed
        //  and since we are a timestamp store, and we write only this will sort itself out naturally
        // TODO (required): make sure we have no races on HLC around SyncPoint else this resolution may not work (we need to know the micros equivalent timestamp of the snapshot)
        /**
         * If we have to handle bootstrapping ranges for range transactions, these may only partially cover the
         * transaction, in which case we should not remove the transaction as a dependency. But if it is fully
         * covered by bootstrapping ranges then we *must* remove it as a dependency.
         */
        class RangeState
        {
            final Unseekables<?> participants;
            Range range;
            int bootstrapIdx, appliedIdx;
            Map<Integer, Ranges> partiallyBootstrapping;

            RangeState(Unseekables<?> participants)
            {
                this.participants = participants;
            }

            /**
             * Are the participating ranges for the txn fully covered by bootstrapping ranges for this command store
             */
            boolean isFullyBootstrapping(int rangeTxnIdx)
            {
                // if all deps for the txnIdx are contained in the range, don't inflate any shared object state
                if (builder.directRangeDeps.foldEachRange(rangeTxnIdx, range, true, (r1, r2, p) -> p && r1.contains(r2)))
                    return true;

                if (partiallyBootstrapping == null)
                    partiallyBootstrapping = new HashMap<>();
                Ranges prev = partiallyBootstrapping.get(rangeTxnIdx);
                Ranges remaining = prev;
                if (remaining == null) remaining = builder.directRangeDeps.ranges(rangeTxnIdx).intersecting(participants, Minimal);
                else Invariants.require(!remaining.isEmpty());
                remaining = remaining.without(Ranges.of(range));
                if (prev == null && remaining.isEmpty())
                    return true;
                partiallyBootstrapping.put(rangeTxnIdx, remaining);
                return remaining.isEmpty();
            }
        }

        RangeDeps rangeDeps = builder.directRangeDeps;
        foldl(participants, (e, s, d, b) -> {
            int bootstrapIdx = d.txnIdsWithFlags().find(e.maxReadyAt());
            if (bootstrapIdx < 0) bootstrapIdx = -1 - bootstrapIdx;
            s.bootstrapIdx = bootstrapIdx;

            TxnId locallyAppliedBefore = e.maxLocallyAppliedBefore();
            int appliedIdx = d.txnIdsWithFlags().find(locallyAppliedBefore);
            if (appliedIdx < 0) appliedIdx = -1 - appliedIdx;
            if (locallyAppliedBefore.epoch() >= e.endEpoch)
            {
                // for range transactions, we should not infer that a still-owned range is redundant because a not-owned range that overlaps is redundant
                int altAppliedIdx = d.txnIdsWithFlags().find(TxnId.minForEpoch(e.endEpoch));
                if (altAppliedIdx < 0) altAppliedIdx = -1 - altAppliedIdx;
                if (altAppliedIdx < appliedIdx) appliedIdx = altAppliedIdx;
            }
            s.appliedIdx = appliedIdx;

            // remove intersecting transactions with known redundant txnId
            if (appliedIdx > bootstrapIdx)
            {
                // TODO (desired):
                // TODO (desired): move the bounds check into forEach, matching structure used for keys
                d.forEach(e.range, b, s, (b0, s0, txnIdx) -> {
                    if (txnIdx >= s0.bootstrapIdx && txnIdx < s0.appliedIdx)
                        b0.removeWaitingOnDirectRangeTxnId(txnIdx);
                });
            }

            if (bootstrapIdx > 0)
            {
                // if we have any ranges where bootstrap is involved, we have to do a more complicated dance since
                // this may imply only partial redundancy (we may still depend on the transaction for some other range)
                s.range = e.range;
                // TODO (desired): move the bounds check into forEach, matching structure used for keys
                d.forEach(e.range, b, s, (b0, s0, txnIdx) -> {
                    if (txnIdx < s0.bootstrapIdx && b0.isWaitingOnTxnIndex(txnIdx) && s0.isFullyBootstrapping(txnIdx))
                        b0.removeWaitingOnDirectRangeTxnId(txnIdx);
                });
            }
            return s;
        }, new RangeState(participants), rangeDeps, builder);
    }

    public final boolean hasLocallyRedundantDependencies(TxnId minimumDependencyId, Timestamp executeAt, Participants<?> participantsOfWaitingTxn)
    {
        // TODO (required): consider race conditions when bootstrapping into an active command store, that may have seen a higher txnId than this?
        //   might benefit from maintaining a per-CommandStore largest TxnId register to ensure we allocate a higher TxnId for our ExclSync,
        //   or from using whatever summary records we have for the range, once we maintain them
        return status(minimumDependencyId, executeAt, participantsOfWaitingTxn).any(LOCALLY_REDUNDANT);
    }

    @Override
    public String toString()
    {
        return toString(", ");
    }

    public String toString(String delimiter)
    {
        StringBuilder sb = new StringBuilder();
        append(sb, delimiter, "gc:", GC_BEFORE);
        append(sb, delimiter, "applied:", LOCALLY_APPLIED);
        append(sb, delimiter, "command_store:", LOCALLY_DURABLE_TO_COMMAND_STORE);
        append(sb, delimiter, "data_store:", LOCALLY_DURABLE_TO_DATA_STORE);
        append(sb, delimiter, "unready:", UNREADY);
        return sb.toString();
    }

    private void append(StringBuilder builder, String delimiter, String prefix, Property p1)
    {
        append(builder, delimiter, prefix, p1, null);
    }

    private void append(StringBuilder builder, String delimiter, String prefix, Property p1, Property p2)
    {
        TreeMap<TxnId, List<Range>> map = new TreeMap<>();
        foldl((e, m, pp1, pp2) -> {
            TxnId bound = pp2 == null ? e.maxBound(pp1) : e.maxBoundBoth(pp1, pp2);
            m.computeIfAbsent(bound, ignore -> new ArrayList<>())
             .add(e.range);
            return m;
        }, map, p1, p2);

        if (map.size() == 0 || map.size() == 1 && map.firstKey().equals(TxnId.NONE))
            return;

        if (builder.length() > 0)
            builder.append(delimiter);
        builder.append(prefix);
        builder.append(map.descendingMap().entrySet().stream()
                  .map(e -> (e.getKey().equals(TxnId.NONE) ? "none" : e.getKey().toString()) + ':' + Ranges.ofSorted(e.getValue().toArray(new Range[0])).mergeTouching())
                  .collect(Collectors.joining(", ", "{", "}")));
    }

    public RedundantBefore map(Function<Bounds, Bounds> map)
    {
        return map(map, Bounds[]::new, EMPTY, RedundantBefore::new);
    }
}
