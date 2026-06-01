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

import accord.api.RoutingKey;
import accord.primitives.AbstractRanges;
import accord.primitives.Routables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.BTreeReducingRangeMap;
import accord.utils.Invariants;
import accord.utils.btree.ReducingBTree;

import static accord.primitives.Timestamp.Flag.REJECTED;

// TODO (expected): track read/write conflicts separately
public class MaxConflicts extends BTreeReducingRangeMap<MaxConflicts.Entry>
{
    public static class Entry extends ReducingBTree.Entry<Entry>
    {
        public final Timestamp any, write, reject;

        private Entry(RoutingKey start, RoutingKey end, Timestamp any, Timestamp write, Timestamp reject)
        {
            super(start, end);
            if (Invariants.isParanoid())
            {
                Invariants.require(!any.hasNonIdentityFlags());
                Invariants.require(!write.hasNonIdentityFlags());
            }
            this.any = any;
            this.write = write;
            this.reject = reject;
        }

        private Entry(Timestamp any, Timestamp write, Timestamp reject)
        {
            super(null, null, UnsafeMarker.NULLS);
            if (Invariants.isParanoid())
            {
                Invariants.require(!any.hasNonIdentityFlags());
                Invariants.require(!write.hasNonIdentityFlags());
            }
            this.any = any;
            this.write = write;
            this.reject = reject;
        }

        public static Entry create(Timestamp any, Timestamp write, Timestamp reject)
        {
            if (any == write)
            {
                any = write = ensureNoFlags(any);
            }
            else
            {
                any = ensureNoFlags(any);
                write = ensureNoFlags(write);
            }
            return new Entry(any, write, reject);
        }

        public static Entry create(RoutingKey start, RoutingKey end, Timestamp any, Timestamp write, Timestamp reject)
        {
            if (any == write)
            {
                any = write = ensureNoFlags(any);
            }
            else
            {
                any = ensureNoFlags(any);
                write = ensureNoFlags(write);
            }
            return new Entry(start, end, any, write, reject);
        }

        private static Timestamp ensureNoFlags(Timestamp ts)
        {
            if (ts.hasNonIdentityFlags())
                return ts.withoutNonIdentityFlags().next();
            return ts;
        }

        @Override
        public boolean equalsIgnoreRange(Entry that)
        {
            return this.any.equals(that.any) && this.write.equals(that.write) && this.reject.equals(that.reject);
        }

        @Override
        public Entry with(RoutingKey start, RoutingKey end)
        {
            return new Entry(start, end, any, write, reject);
        }

        public Timestamp get(Timestamp atLeast, TxnId txnId)
        {
            return get(reject, any, atLeast, txnId);
        }

        public Timestamp getWrite(Timestamp atLeast, TxnId txnId)
        {
            return get(reject, write, atLeast, txnId);
        }

        private static Timestamp get(Timestamp reject, Timestamp conflict, Timestamp atLeast, TxnId txnId)
        {
            if (rejects(reject, txnId))
                atLeast = atLeast.addFlag(REJECTED);

            if (atLeast.compareSimultaneousEpochAndHlc(conflict) >= 0)
                return atLeast;

            Timestamp result = Timestamp.mergeMax(atLeast, conflict);
            if (atLeast.is(REJECTED))
                result = result.addFlag(REJECTED);
            return result;
        }

        private static boolean rejects(Timestamp rejectIfBefore, TxnId test)
        {
            return rejectIfBefore.compareSimultaneousEpochAndHlc(test) >= 0;
        }

        public static Entry reduce(RoutingKey start, RoutingKey end, Entry a, Entry b)
        {
            Timestamp any = Timestamp.mergeMax(a.any, b.any);
            Timestamp write = Timestamp.mergeMax(a.write, b.write);
            Timestamp reject = Timestamp.mergeMax(a.reject, b.reject);
            if (any == a.any && write == a.write && reject == a.reject && a.equalsRange(start, end))
                return a;
            if (any.equals(b.any) && write.equals(b.write) && reject.equals(b.reject) && b.equalsRange(start, end))
                return b;
            return new Entry(start, end, any, write, reject);
        }
    }
    public static final MaxConflicts EMPTY = new MaxConflicts();

    private MaxConflicts()
    {
        super();
    }

    private MaxConflicts(Object[] tree)
    {
        super(tree);
    }

    Timestamp get(TxnId txnId, Routables<?> keysOrRanges)
    {
        return txnId.isSomeRead() ? getWrite(txnId, keysOrRanges) : getAny(txnId, keysOrRanges);
    }

    Timestamp getWrite(TxnId txnId, Routables<?> keysOrRanges)
    {
        return foldl(keysOrRanges, Entry::getWrite, Timestamp.NONE, txnId);
    }

    Timestamp getAny(TxnId txnId, Routables<?> keysOrRanges)
    {
        return foldl(keysOrRanges, Entry::get, Timestamp.NONE, txnId);
    }

    public MaxConflicts update(TxnId txnId, Routables<?> keysOrRanges, Timestamp executeAt)
    {
        return update(keysOrRanges, executeAt, txnId.isSomeRead() ? Timestamp.NONE : executeAt, Timestamp.NONE);
    }

    public MaxConflicts update(Routables<?> keysOrRanges, Timestamp any, Timestamp write)
    {
        return update(keysOrRanges, any, write, Timestamp.NONE);
    }

    public MaxConflicts update(Routables<?> keysOrRanges, Timestamp any, Timestamp write, Timestamp reject)
    {
        // note: we use mergeMax to ensure we take the maximum epoch and hlc independently from any conflict
        //  this is particularly essential for propagating unique HLCs, so that bootstrap recipients don't
        //  begin serving reads too early
        return add(this, keysOrRanges, Entry.create(any, write, reject), Entry::reduce, MaxConflicts::new);
    }

    public MaxConflicts update(MaxConflicts that)
    {
        return merge(this, that, Entry::reduce, MaxConflicts::new);
    }

    public static MaxConflicts create(AbstractRanges ranges, MaxConflicts.Entry entry)
    {
        return create(ranges, entry, MaxConflicts::new);
    }

    public static class Builder extends BTreeReducingRangeMap.Builder<Entry, MaxConflicts>
    {
        public MaxConflicts build()
        {
            return build(MaxConflicts::new);
        }
    }
}
