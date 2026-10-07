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

import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.RoutingKey;
import accord.primitives.AbstractRanges;
import accord.primitives.Routables;
import accord.primitives.Status.Durability.HasOutcome;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.BTreeReducingRangeMap;
import accord.utils.Invariants;
import accord.utils.PersistentField;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import accord.utils.btree.BTree;
import accord.utils.btree.ReducingBTree;

import static accord.primitives.Status.Durability.HasOutcome.None;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Durability.HasOutcome.Universal;

public class DurableBefore extends BTreeReducingRangeMap<DurableBefore.Entry>
{
    public static class Entry extends ReducingBTree.Entry<Entry>
    {
        public static final Entry MAX = new Entry(TxnId.MAX, TxnId.MAX);
        public static final Entry NONE = new Entry(TxnId.NONE, TxnId.NONE);

        public final @Nonnull TxnId quorum, universal;

        public Entry(RoutingKey start, RoutingKey end, @Nonnull TxnId quorum, @Nonnull TxnId universal)
        {
            super(start, end);
            Invariants.requireArgument(quorum.compareTo(universal) >= 0, "quorum %s < universal %s", quorum, universal);
            this.quorum = quorum;
            this.universal = universal;
        }

        Entry(@Nonnull TxnId quorum, @Nonnull TxnId universal)
        {
            super(null, null, UnsafeMarker.NULLS);
            Invariants.requireArgument(quorum.compareTo(universal) >= 0, "quorum %s < universal %s", quorum, universal);
            this.quorum = quorum;
            this.universal = universal;
        }

        public static Entry constructWithoutRange(@Nonnull TxnId quorum, @Nonnull TxnId universal)
        {
            return new Entry(quorum, universal);
        }

        public static Entry reduceMax(RoutingKey withStart, RoutingKey withEnd, Entry a, Entry b)
        {
            return reduce(withStart, withEnd, a, b, TxnId::max);
        }

        public static Entry reduceMin(RoutingKey withStart, RoutingKey withEnd, Entry a, Entry b)
        {
            return reduce(withStart, withEnd, a, b, TxnId::min);
        }

        public static Entry max(Entry a, Entry b)
        {
            return reduceWithoutRange(a, b, TxnId::max);
        }

        public static Entry min(Entry a, Entry b)
        {
            return reduceWithoutRange(a, b, TxnId::min);
        }

        private static Entry reduce(RoutingKey withStart, RoutingKey withEnd, Entry a, Entry b, BiFunction<TxnId, TxnId, TxnId> reduce)
        {
            TxnId majority = reduce.apply(a.quorum, b.quorum);
            TxnId universal = reduce.apply(a.universal, b.universal);

            if (majority == a.quorum && universal == a.universal && a.equalsRange(withStart, withEnd))
                return a;
            if (majority.equals(b.quorum) && universal.equals(b.universal) && b.equalsRange(withStart, withEnd))
                return b;

            return new Entry(withStart, withEnd, majority, universal);
        }

        private static Entry reduceWithoutRange(Entry a, Entry b, BiFunction<TxnId, TxnId, TxnId> reduce)
        {
            TxnId majority = reduce.apply(a.quorum, b.quorum);
            TxnId universal = reduce.apply(a.universal, b.universal);

            if (majority == a.quorum && universal == a.universal)
                return a;
            if (majority.equals(b.quorum) && universal.equals(b.universal))
                return b;

            return new Entry(majority, universal);
        }

        public HasOutcome get(TxnId txnId)
        {
            if (txnId.compareTo(quorum) < 0)
                return txnId.compareTo(universal) < 0 ? Universal : Quorum;
            return None;
        }

        static HasOutcome mergeMin(Entry entry, @Nullable HasOutcome prev, TxnId txnId)
        {
            HasOutcome next = entry.get(txnId);
            return prev != null && prev.compareTo(next) <= 0 ? prev : next;
        }

        static HasOutcome mergeMax(Entry entry, HasOutcome prev, TxnId txnId)
        {
            HasOutcome next = entry.get(txnId);
            return prev != null && prev.compareTo(next) >= 0 ? prev : next;
        }

        public boolean equals(Object that)
        {
            return that instanceof Entry && equals((Entry) that);
        }

        public boolean equals(Entry that)
        {
            return    this.quorum.equals(that.quorum)
                   && this.universal.equals(that.universal);
        }

        @Override
        public String toString()
        {
            return (start() == null ? "" : super.toString() + "=") + "(quorum=" + quorum + ",universal=" + universal + ")";
        }

        @Override
        public boolean equalsIgnoreRange(Entry that)
        {
            return this.quorum.equals(that.quorum)
                   && this.universal.equals(that.universal);
        }

        @Override
        public Entry with(RoutingKey start, RoutingKey end)
        {
            return new Entry(start, end, quorum, universal);
        }
    }

    public static final DurableBefore EMPTY = new DurableBefore();
    final DurableBefore.Entry min;
    DurableBefore()
    {
        this.min = new DurableBefore.Entry(TxnId.NONE, TxnId.NONE);
    }

    private static DurableBefore construct(Object[] tree)
    {
        if (BTree.isEmpty(tree))
            return EMPTY;

        return new DurableBefore(tree);
    }

    private DurableBefore(Object[] tree)
    {
        super(tree);
        Invariants.require(!isEmpty());
        DurableBefore.Entry min = null;
        for (DurableBefore.Entry e : BTree.<DurableBefore.Entry>iterable(tree))
        {
            if (min == null) min = e;
            else min = DurableBefore.Entry.min(min, e);
        }
        this.min = min;
    }

    public static DurableBefore create(AbstractRanges ranges, @Nonnull TxnId majority, @Nonnull TxnId universal)
    {
        return create(ranges, new DurableBefore.Entry(majority, universal));
    }

    public static DurableBefore create(AbstractRanges ranges, DurableBefore.Entry entry)
    {
        if (ranges.isEmpty())
            return DurableBefore.EMPTY;

        return create(ranges, entry, DurableBefore::construct);
    }

    public DurableBefore update(Routables<?> keysOrRanges, TxnId quorum, TxnId universal)
    {
        return update(keysOrRanges, new DurableBefore.Entry(quorum, universal));
    }

    public DurableBefore update(Routables<?> keysOrRanges, DurableBefore.Entry entry)
    {
        return add(this, keysOrRanges, entry, DurableBefore.Entry::reduceMax, DurableBefore::construct);
    }

    public static DurableBefore merge(DurableBefore a, DurableBefore b)
    {
        return BTreeReducingRangeMap.merge(a, b, DurableBefore.Entry::reduceMax, DurableBefore::construct);
    }

    public static DurableBefore mergeIfDifferent(DurableBefore prev, DurableBefore input)
    {
        DurableBefore next = DurableBefore.merge(prev, input);
        return next.equals(prev) ? prev : next;
    }

    public HasOutcome min(TxnId txnId, Unseekables<?> unseekables)
    {
        return notDurableIfNull(foldlWithDefault(unseekables, DurableBefore.Entry::mergeMin, DurableBefore.Entry.NONE, null, txnId));
    }

    public DurableBefore.Entry minEntry(Unseekables<?> unseekables)
    {
        return foldlWithDefault(unseekables, DurableBefore.Entry::min, DurableBefore.Entry.NONE, DurableBefore.Entry.MAX);
    }

    public HasOutcome max(TxnId txnId, Unseekables<?> unseekables)
    {
        return notDurableIfNull(foldl(unseekables, DurableBefore.Entry::mergeMax, null, txnId));
    }

    public HasOutcome get(TxnId txnId, RoutingKey participant)
    {
        DurableBefore.Entry entry = get(participant);
        return entry == null ? None : entry.get(txnId);
    }

    public boolean isUniversal(TxnId txnId, RoutingKey participant)
    {
        return get(txnId, participant) == Universal;
    }

    public HasOutcome min(TxnId txnId)
    {
        if (min.universal.compareTo(txnId) > 0)
            return Universal;
        if (min.quorum.compareTo(txnId) > 0)
            return Quorum;
        return None;
    }

    public long maxEpoch()
    {
        return foldl((e, v) -> TxnId.max(v, TxnId.max(e.quorum, e.universal)), TxnId.NONE).epoch();
    }

    private static HasOutcome notDurableIfNull(HasOutcome status)
    {
        return status == null ? None : status;
    }

    public static final PersistentField.Persister<DurableBefore, DurableBefore> NOOP_PERSISTER = new PersistentField.Persister<>()
    {
        @Override public AsyncResult<?> persist(DurableBefore addValue, DurableBefore newValue) { return AsyncResults.success(null); }
        @Override public AsyncResult<?> persist(DurableBefore newValue) { return AsyncResults.success(null); }
        @Override public DurableBefore load() { return DurableBefore.EMPTY; }
    };

    /**
     * A non-validating builder that expects all entries to be in correct order. For implementations' ser/de logic.
     */
    public static class Builder extends BTreeReducingRangeMap.Builder<DurableBefore.Entry, DurableBefore>
    {
        public DurableBefore build()
        {
            return build(DurableBefore::construct);
        }
    }

}
