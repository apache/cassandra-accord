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

package accord.local.durability;

import java.util.NavigableMap;
import java.util.TreeMap;

import javax.annotation.Nullable;

import com.google.common.collect.Maps;

import accord.api.RoutingKey;
import accord.local.Node;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.ReducingRangeMap;
import accord.utils.SortedArrays.SortedArrayList;

public class DurabilityResults extends ReducingRangeMap<DurabilityResults.Entry>
{
    public static class Entry
    {
        public final TxnId txnId;
        public final @Nullable SortedArrayList<Node.Id> readable;

        public Entry(TxnId txnId, SortedArrayList<Node.Id> readable)
        {
            this.txnId = txnId;
            this.readable = readable;
        }

        private Entry merge(Entry that)
        {
            int c = this.txnId.compareTo(that.txnId);
            if (c < 0) return this;
            else if (c > 0) return that;

            SortedArrayList<Node.Id> readable = SortedArrayList.union(this.readable, that.readable);
            if (readable == this.readable)
                return this;
            if (readable == that.readable)
                return that;
            return new Entry(txnId, readable);
        }

        @Override
        public String toString()
        {
            return txnId.toString() + " @" + readable;
        }
    }

    public static class ByIdEntry
    {
        public final Ranges ranges;
        public final @Nullable SortedArrayList<Node.Id> readable;

        public ByIdEntry(Ranges ranges, SortedArrayList<Node.Id> readable)
        {
            this.ranges = ranges;
            this.readable = readable;
        }

        ByIdEntry merge(ByIdEntry that)
        {
            return new ByIdEntry(this.ranges.with(that.ranges), SortedArrayList.intersection(this.readable, that.readable));
        }

        @Override
        public String toString()
        {
            return ranges.toString() + " @" + readable;
        }
    }

    static final DurabilityResults EMPTY = new DurabilityResults();
    private volatile NavigableMap<TxnId, ByIdEntry> byTxnId;

    private DurabilityResults()
    {
    }

    private DurabilityResults(RoutingKey[] starts, Entry[] values)
    {
        super(starts, values);
    }

    public NavigableMap<TxnId, Ranges> rangesByTxnId()
    {
        return Maps.transformValues(byTxnId(), e -> e.ranges);
    }

    public NavigableMap<TxnId, ByIdEntry> byTxnId()
    {
        if (byTxnId == null)
        {
            byTxnId = foldlWithBounds((e, map, start, end) -> {
                map.merge(e.txnId, new ByIdEntry(Ranges.of(Range.of(start, end)), e.readable), ByIdEntry::merge);
                return map;
            }, new TreeMap<>());
        }
        return byTxnId;
    }

    public DurabilityResults merge(DurabilityResults that)
    {
        return ReducingRangeMap.merge(this, that, Entry::merge, Builder::new);
    }

    public static DurabilityResults of(Ranges ranges, TxnId txnId, @Nullable SortedArrayList<Node.Id> readable)
    {
        return ReducingRangeMap.create((Unseekables<?>) ranges, new Entry(txnId, readable), Builder::new);
    }

    public static class Builder extends AbstractBoundariesBuilder<RoutingKey, Entry, DurabilityResults>
    {
        public Builder(int capacity)
        {
            super(capacity);
        }

        @Override
        protected DurabilityResults buildInternal()
        {
            if (values.isEmpty())
                return EMPTY;

            return new DurabilityResults(starts.toArray(new RoutingKey[0]), values.toArray(new Entry[0]));
        }
    }
}
