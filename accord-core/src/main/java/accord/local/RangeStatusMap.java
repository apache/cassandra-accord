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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.EpochSupplier;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

import static accord.local.RangeStatus.DURABLE;
import static accord.local.RangeStatus.LIVE;
import static accord.local.RangeStatus.NOT_OWNED;
import static accord.local.RangeStatus.REDUNDANT;
import static accord.local.RangeStatus.TRUNCATED;

public class RangeStatusMap extends ReducingRangeMap<RangeStatusMap.Entry>
{
    public static class Entry
    {
        final Range range;
        final long startEpoch, endEpoch;
        @Nonnull final TxnId redundantBefore, truncatedBefore;
        // we compress redundant and durable into a single register, and simply ignore any durableBefore < redundantBefore,
        // i.e. we always prefer the most recent of the two, and simply set the boolean to whichever that is.
        // what this means in practice is that
        final boolean isRedundantDurable;

        public Entry(Range range, long startEpoch, long endEpoch, @Nonnull TxnId redundantBefore, boolean isRedundantDurable, @Nonnull TxnId truncatedBefore)
        {
            this.range = range;
            this.startEpoch = startEpoch;
            this.endEpoch = endEpoch;
            this.redundantBefore = redundantBefore;
            this.truncatedBefore = truncatedBefore;
            this.isRedundantDurable = isRedundantDurable;
        }

        public static Entry reduce(Entry a, Entry b)
        {
            return merge(a.range.slice(b.range), a, b);
        }

        private static Entry merge(Range range, Entry a, Entry b)
        {
            long startEpoch = Long.max(a.startEpoch, b.startEpoch);
            long endEpoch = Long.min(a.endEpoch, b.endEpoch);
            int cr = a.redundantBefore.compareTo(b.redundantBefore);

            TxnId redundantBefore = cr >= 0 ? a.redundantBefore : b.redundantBefore;
            boolean isRedundantDurable = cr > 0 ? a.isRedundantDurable : cr < 0 ? b.isRedundantDurable : a.isRedundantDurable | b.isRedundantDurable;

            int ct = a.truncatedBefore.compareTo(b.truncatedBefore);
            TxnId truncatedBefore = TxnId.max(a.truncatedBefore, b.truncatedBefore);

            if (range.equals(a.range) && startEpoch == a.startEpoch && endEpoch == a.endEpoch && cr >= 0 && ct >= 0 && isRedundantDurable == a.isRedundantDurable)
                return a;
            if (range.equals(b.range) && startEpoch == b.startEpoch && endEpoch == b.endEpoch && cr <= 0 && ct <= 0 && isRedundantDurable == b.isRedundantDurable)
                return b;

            return new Entry(range, startEpoch, endEpoch, redundantBefore, isRedundantDurable, truncatedBefore);
        }

        static RangeStatus mergeMin(Entry entry, RangeStatus prev, TxnId txnId, EpochSupplier executeAt)
        {
            if (entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;
            if (entry.truncatedBefore.compareTo(txnId) > 0)
                return prev == null ? TRUNCATED : prev;
            if (entry.redundantBefore.compareTo(txnId) > 0)
                return entry.isRedundantDurable ? RangeStatus.nonNullOrMin(prev, DURABLE) : prev == LIVE ? LIVE : REDUNDANT;
            return LIVE;
        }

        static RangeStatus mergeMax(Entry entry, RangeStatus prev, TxnId txnId, EpochSupplier executeAt)
        {
            if (entry == null || entry.outOfBounds(txnId, executeAt))
                return prev;
            if (entry.truncatedBefore.compareTo(txnId) > 0)
                return TRUNCATED;
            if (entry.redundantBefore.compareTo(txnId) > 0)
                return entry.isRedundantDurable ? prev == TRUNCATED ? TRUNCATED : DURABLE : RangeStatus.nonNullOrMax(prev, REDUNDANT);
            return prev == null ? LIVE : prev;
        }

        // TODO (required, consider): this admits the range of epochs that cross the two timestamps, which matches our
        //   behaviour elsewhere but we probably want to only interact with the two point epochs in which we participate,
        //   but note hasRedundantDependencies which really does want to scan a range
        private boolean outOfBounds(Timestamp lb, EpochSupplier ub)
        {
            return ub.epoch() < startEpoch || lb.epoch() >= endEpoch;
        }

        Entry withRange(Range range)
        {
            return new Entry(range, startEpoch, endEpoch, redundantBefore, isRedundantDurable, truncatedBefore);
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
                   && this.redundantBefore.equals(that.redundantBefore)
                   && this.truncatedBefore.equals(that.truncatedBefore);
        }

        @Override
        public String toString()
        {
            return "("
                   + (startEpoch == Long.MIN_VALUE ? "-\u221E" : Long.toString(startEpoch)) + ","
                   + (endEpoch == Long.MAX_VALUE ? "\u221E" : Long.toString(endEpoch)) + ","
                   + redundantBefore + ","
                   + truncatedBefore + ")";
        }
    }

//    final Ranges ranges;
    public RangeStatusMap()
    {
//        this.ranges = Ranges.EMPTY;
    }

    RangeStatusMap(boolean inclusiveEnds, RoutingKey[] starts, Entry[] values)
//    RangeStatusMap(boolean inclusiveEnds, RoutingKey[] starts, Entry[] values, BiFunction<RoutingKey, RoutingKey, Range> rangeFactory)
    {
        super(inclusiveEnds, starts, values);
//        Range[] ranges = new Range[starts.length - 1];
//        for (int i = 0 ; i < ranges.length ; ++i)
//            ranges[i] = rangeFactory.apply(starts[i], starts[i + 1]);
    }

    public static RangeStatusMap create(Ranges ranges, long startEpoch, long endEpoch, @Nullable TxnId redundantBefore, boolean isRedundantDurable, @Nullable TxnId truncatedBefore)
    {
        if (ranges.isEmpty())
            return new RangeStatusMap();

        if (redundantBefore == null)
            redundantBefore = TxnId.NONE;
        if (truncatedBefore == null)
            truncatedBefore = TxnId.NONE;

        Entry entry = new Entry(null, startEpoch, endEpoch, redundantBefore, isRedundantDurable, truncatedBefore);
        Builder builder = new Builder(ranges.get(0).endInclusive(), ranges.size() * 2);
        for (int i = 0 ; i < ranges.size() ; ++i)
        {
            Range cur = ranges.get(i);
            builder.append(cur.start(), entry.withRange(cur), (a, b) -> { throw new IllegalStateException(); });
            builder.append(cur.end(), null, (a, b) -> a); // if we are equal to prev end, take the prev value not zero
        }
        return builder.build();
    }

    public static RangeStatusMap merge(RangeStatusMap a, RangeStatusMap b)
    {
        return ReducingIntervalMap.merge(a, b, RangeStatusMap.Entry::reduce, Builder::new);
    }

    public RangeStatus min(TxnId txnId, @Nullable EpochSupplier executeAt, Routables<?, ?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        return notOwnedIfNull(foldl(participants, Entry::mergeMin, null, txnId, executeAt, test -> test == LIVE));
    }

    public RangeStatus get(TxnId txnId, @Nullable EpochSupplier executeAt, RoutingKey participant)
    {
        if (executeAt == null) executeAt = txnId;
        Entry entry = get(participant);
        return entry == null ? NOT_OWNED : notOwnedIfNull(Entry.mergeMax(entry,null, txnId, executeAt));
    }

    public RangeStatus max(TxnId txnId, @Nullable EpochSupplier executeAt, Routables<?, ?> participants)
    {
        if (executeAt == null) executeAt = txnId;
        return notOwnedIfNull(foldl(participants, Entry::mergeMax, null, txnId, executeAt, test -> test == TRUNCATED));
    }

    public boolean isTruncated(TxnId txnId, @Nullable EpochSupplier executeAt, Routables<?, ?> participants)
    {
        return min(txnId, executeAt, participants) == TRUNCATED;
    }

    public boolean isTruncated(TxnId txnId, @Nullable EpochSupplier executeAt, RoutingKey participant)
    {
        return get(txnId, executeAt, participant) == TRUNCATED;
    }

    public boolean isSomeShardDurable(TxnId txnId, Timestamp executeAt, Routables<?, ?> participants)
    {
        return max(txnId, executeAt, participants).compareTo(DURABLE) >= 0;
    }

    public boolean isRedundant(TxnId txnId, Timestamp executeAt, Routables<?, ?> participants)
    {
        return max(txnId, executeAt, participants).compareTo(REDUNDANT) >= 0;
    }

    private static RangeStatus notOwnedIfNull(RangeStatus status)
    {
        return status == null ? NOT_OWNED : status;
    }

    static class Builder extends ReducingIntervalMap.Builder<RoutingKey, Entry, RangeStatusMap>
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

        @Override
        protected Entry mergeEqual(Entry a, Entry b)
        {
            Invariants.checkState(a.range.compareIntersecting(b.range) == 0 || a.range.end().equals(b.range.start()) || a.range.start().equals(b.range.end()));
            return new Entry(a.range.newRange(
                a.range.start().compareTo(b.range.start()) <= 0 ? a.range.start() : b.range.start(),
                a.range.end().compareTo(b.range.end()) >= 0 ? a.range.end() : b.range.end()
            ), a.startEpoch, a.endEpoch, a.redundantBefore, a.isRedundantDurable, a.truncatedBefore);
        }

        @Override
        protected RangeStatusMap buildInternal()
        {
            return new RangeStatusMap(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }
}
