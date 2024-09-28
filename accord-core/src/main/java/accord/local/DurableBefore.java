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
import accord.local.Status.Durability;
import accord.primitives.AbstractRanges;
import accord.primitives.Participants;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.Invariants;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

import static accord.local.Status.Durability.MajorityOrInvalidated;
import static accord.local.Status.Durability.NotDurable;
import static accord.local.Status.Durability.UniversalOrInvalidated;

public class DurableBefore extends ReducingRangeMap<DurableBefore.Entry>
{
    public static class SerializerSupport
    {
        public static DurableBefore create(boolean inclusiveEnds, RoutingKey[] ends, Entry[] values)
        {
            if (values.length == 0)
            {
                Invariants.checkState(ends.length == 1 && ends[0] == null);
                return DurableBefore.EMPTY;
            }
            return new DurableBefore(inclusiveEnds, ends, values);
        }
    }

    public static class Entry
    {
        public final @Nonnull TxnId majorityBefore, universalBefore;

        public Entry(@Nonnull TxnId majority, @Nonnull TxnId universalBefore)
        {
            Invariants.checkArgument(majority.compareTo(universalBefore) >= 0, "majority %s < universal %s", majority, universalBefore);
            this.majorityBefore = majority;
            this.universalBefore = universalBefore;
        }

        private static Entry max(Entry a, Entry b)
        {
            return reduce(a, b, TxnId::max);
        }

        private static Entry min(Entry a, Entry b)
        {
            return reduce(a, b, TxnId::min);
        }

        private static Entry reduce(Entry a, Entry b, BiFunction<TxnId, TxnId, TxnId> reduce)
        {
            TxnId majority = reduce.apply(a.majorityBefore, b.majorityBefore);
            TxnId universal = reduce.apply(a.universalBefore, b.universalBefore);

            if (majority == a.majorityBefore && universal == a.universalBefore)
                return a;
            if (majority.equals(b.majorityBefore) && universal.equals(b.universalBefore))
                return b;

            return new Entry(majority, universal);
        }

        public Durability get(TxnId txnId)
        {
            if (txnId.compareTo(majorityBefore) < 0)
                return txnId.compareTo(universalBefore) < 0 ? UniversalOrInvalidated : MajorityOrInvalidated;
            return NotDurable;
        }

        static Durability mergeMin(Entry entry, @Nullable Durability prev, TxnId txnId)
        {
            Durability next = entry.get(txnId);
            return prev != null && prev.compareTo(next) <= 0 ? prev : next;
        }

        static Durability mergeMax(Entry entry, Durability prev, TxnId txnId)
        {
            Durability next = entry.get(txnId);
            return prev != null && prev.compareTo(next) >= 0 ? prev : next;
        }

        public boolean equals(Object that)
        {
            return that instanceof Entry && equals((Entry) that);
        }

        public boolean equals(Entry that)
        {
            return    this.majorityBefore.equals(that.majorityBefore)
                   && this.universalBefore.equals(that.universalBefore);
        }

        @Override
        public String toString()
        {
            return "(" + majorityBefore + "," + universalBefore + ")";
        }
    }

    public static final DurableBefore EMPTY = new DurableBefore();

    final Entry min;
    private DurableBefore()
    {
        this.min = new Entry(TxnId.NONE, TxnId.NONE);
    }

    DurableBefore(boolean inclusiveEnds, RoutingKey[] starts, Entry[] values)
    {
        super(inclusiveEnds, starts, values);
        if (values.length == 0)
        {
            min = new Entry(TxnId.NONE, TxnId.NONE);
        }
        else
        {
            Entry min = null;
            for (Entry value : values)
            {
                if (value == null)
                    continue;

                if (min == null) min = value;
                else min = Entry.min(min, value);
            }
            this.min = min;
        }
    }

    public static DurableBefore create(AbstractRanges ranges, @Nonnull TxnId majority, @Nonnull TxnId universal)
    {
        if (ranges.isEmpty())
            return DurableBefore.EMPTY;

        Entry entry = new Entry(majority, universal);
        return create(ranges, entry, Builder::new);
    }

    public static DurableBefore merge(DurableBefore a, DurableBefore b)
    {
        return ReducingIntervalMap.merge(a, b, DurableBefore.Entry::max, Builder::new);
    }

    public Durability min(TxnId txnId, Unseekables<?> unseekables)
    {
        return notDurableIfNull(foldl(unseekables, Entry::mergeMin, null, txnId, test -> test == NotDurable));
    }

    public Durability max(TxnId txnId, Unseekables<?> unseekables)
    {
        return notDurableIfNull(foldl(unseekables, Entry::mergeMax, null, txnId, test -> test == UniversalOrInvalidated));
    }

    public Durability get(TxnId txnId, RoutingKey participant)
    {
        DurableBefore.Entry entry = get(participant);
        return entry == null ? NotDurable : entry.get(txnId);
    }

    public boolean isUniversal(TxnId txnId, RoutingKey participant)
    {
        return get(txnId, participant) == UniversalOrInvalidated;
    }

    public boolean isSomeShardDurable(TxnId txnId, Participants<?> participants, Durability durability)
    {
        return max(txnId, participants).compareTo(durability) >= 0;
    }

    public Durability min(TxnId txnId)
    {
        if (min.universalBefore.compareTo(txnId) > 0)
            return UniversalOrInvalidated;
        if (min.majorityBefore.compareTo(txnId) > 0)
            return MajorityOrInvalidated;
        return NotDurable;
    }

    private static Durability notDurableIfNull(Durability status)
    {
        return status == null ? NotDurable : status;
    }

    static class Builder extends AbstractBoundariesBuilder<RoutingKey, Entry, DurableBefore>
    {
        protected Builder(boolean inclusiveEnds, int capacity)
        {
            super(inclusiveEnds, capacity);
        }

        @Override
        protected DurableBefore buildInternal()
        {
            return new DurableBefore(inclusiveEnds, starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }
}
