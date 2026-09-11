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
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import accord.api.RoutingKey;
import accord.impl.PrefixedIntHashKey;
import accord.primitives.AbstractRanges;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Status;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;
import accord.utils.ReducingIntervalMap;
import accord.utils.ReducingRangeMap;

import static accord.primitives.Status.Durability.HasOutcome.None;
import static accord.primitives.Status.Durability.HasOutcome.Quorum;
import static accord.primitives.Status.Durability.HasOutcome.Universal;
import static accord.utils.BTreeReducingRangeMap.isWellFormed;
import static accord.utils.Functions.alwaysFalse;

public abstract class AbstractDurableBeforeTest
{
    enum TestAction
    {
        NEW, UPDATE, MERGE
    }

    public void test()
    {
        test(1L, 100000, 5);
        for (int i = 0 ; i < 10 ; ++i)
            test(ThreadLocalRandom.current().nextLong(), 100000, 5);
    }

    protected abstract void assertEquals(Object a, Object b);
    protected abstract void assertTrue(boolean isTrue);

    protected RoutingKey key(int prefix, int hash)
    {
        return PrefixedIntHashKey.forHash(prefix, hash);
    }

    private void test(long seed, int actionCount, int maxDurableBefores)
    {
        System.out.println("Seed " + seed);
        int MAX_HASH = 1 << 20;
        RandomTestRunner.test().withSeed(seed).check(rs -> {
            int maxHashRange = 1 + rs.nextInt(MAX_HASH - 1);
            int prefixCount = rs.nextInt(1, 3);
            Gen<TxnId> genSyncIds = AccordGens.txnIds(Gens.pick(Txn.Kind.VisibilitySyncPoint, Txn.Kind.ExclusiveSyncPoint));
            Gen<Ranges> genRanges = rs2 -> {
                int size = 1 + rs.nextInt(2);
                int prefix = rs.nextInt(prefixCount);
                Range[] ranges = new Range[size];
                for (int i = 0 ; i < size ; ++i)
                {
                    int startHash = rs.nextInt(MAX_HASH - (1 + maxHashRange));
                    int endHash = startHash + 1 + rs.nextInt(maxHashRange);
                    ranges[i] = Range.of(key(prefix, startHash), key(prefix, endHash));
                }
                return Ranges.of(ranges);
            };

            List<DurableBeforeLinear> as = new ArrayList<>();
            List<DurableBefore> bs = new ArrayList<>();
            as.add(DurableBeforeLinear.EMPTY);
            bs.add(DurableBefore.EMPTY);

            Supplier<TestAction> nextAction = rs.weightedPicker(TestAction.values(), new float[]{ 1f, rs.nextInt(10, 100), 1f });
            for (int actionCounter = 0 ; actionCounter < actionCount ; ++actionCounter)
            {
                switch (nextAction.get())
                {
                    case NEW:
                        if (as.size() >= maxDurableBefores)
                        {
                            --actionCounter;
                        }
                        else
                        {
                            as.add(DurableBeforeLinear.EMPTY);
                            bs.add(DurableBefore.EMPTY);
                        }
                        break;
                    case MERGE:
                        if (as.size() == 1)
                        {
                            --actionCounter;
                        }
                        else
                        {
                            int i = rs.nextInt(as.size());
                            int j = rs.nextInt(as.size());
                            while (i == j) j = rs.nextInt(as.size());

                            DurableBeforeLinear old1 = as.get(i);
                            DurableBeforeLinear old2 = as.get(j);
                            DurableBefore new1 = bs.get(i);
                            DurableBefore new2 = bs.get(j);
                            DurableBeforeLinear nextold = DurableBeforeLinear.merge(old1, old2);
                            DurableBefore nextnew = DurableBefore.merge(new1, new2);
                            check(rs, nextnew, nextold, genRanges);
                            as.set(i, nextold);
                            bs.set(i, nextnew);
                            as.set(j, as.get(as.size() - 1));
                            bs.set(j, bs.get(bs.size() - 1));
                            as.remove(as.size() - 1);
                            bs.remove(bs.size() - 1);
                        }
                        break;
                    case UPDATE:
                        int i = rs.nextInt(as.size());
                        DurableBeforeLinear prevold = as.get(i);
                        DurableBefore prevnew = bs.get(i);
                        TxnId syncId1 = genSyncIds.next(rs);
                        TxnId syncId2 = genSyncIds.next(rs);
                        TxnId q = syncId1.compareTo(syncId2) > 0 ? syncId1 : syncId2;
                        TxnId u = q == syncId1 ? syncId2 : syncId1;
                        Ranges ranges = genRanges.next(rs);

                        DurableBeforeLinear nextold = DurableBeforeLinear.merge(prevold, DurableBeforeLinear.create(ranges, q, u));
                        DurableBefore nextnew = prevnew.update(ranges, q, u);
                        check(rs, nextnew, nextold, genRanges);
                        as.set(i, nextold);
                        bs.set(i, nextnew);
                }
            }
        });
    }

    void spotCheck(int count, Gen<Ranges> rangesGen, RandomSource rs, DurableBefore test, DurableBeforeLinear against)
    {
        if (test.size() == 0)
            return;

        while (count-- > 0)
        {
            if (rs.decide(0.5f))
            {
                Ranges ranges = rangesGen.next(rs);
                List<DurableBefore.Entry> vs = test.foldl(ranges, (v, l) -> { if (!l.contains(v)) l.add(v); return l; }, new ArrayList<>());
                List<DurableBefore.Entry> vs2 = against.foldl(ranges, (v, l) -> { if (!l.contains(v)) l.add(v); return l; }, new ArrayList<>());
                assertEquals(vs.size(), vs2.size());
                for (int i = 0 ; i < vs.size() ; ++i)
                    assertTrue(vs.get(i).equalsIgnoreRange(vs2.get(i)));
            }
            else
            {
                int i = rs.nextInt(test.size());
                assertTrue(equalsIgnoreRange(against.valueAt(i), test.get(against.startAt(i + 1))));
            }
        }
    }

    static boolean equalsIgnoreRange(DurableBefore.Entry a, DurableBefore.Entry b)
    {
        return a == null || b == null ? a == b : a.equalsIgnoreRange(b);
    }

    protected void check(RandomSource rs, DurableBefore tree, DurableBeforeLinear linear, Gen<Ranges> genRanges)
    {
        assertTrue(isWellFormed(tree));
        assertTrue(linear.isEqualTo(tree));
        if (linear.size() > 0)
            spotCheck(rs.nextInt(Math.min(linear.size() / 2, 20), Math.min(100, linear.size())), genRanges, rs, tree, linear);
    }

    public static class DurableBeforeLinear extends ReducingRangeMap<DurableBefore.Entry>
    {
        public static final DurableBeforeLinear EMPTY = new DurableBeforeLinear();

        final DurableBefore.Entry min;
        DurableBeforeLinear()
        {
            this.min = new DurableBefore.Entry(TxnId.NONE, TxnId.NONE);
        }

        DurableBeforeLinear(RoutingKey[] starts, DurableBefore.Entry[] values)
        {
            super(starts, values);
            if (values.length == 0)
            {
                min = new DurableBefore.Entry(TxnId.NONE, TxnId.NONE);
            }
            else
            {
                DurableBefore.Entry min = null;
                for (DurableBefore.Entry value : values)
                {
                    if (value == null)
                        continue;

                    if (min == null) min = value;
                    else min = DurableBefore.Entry.min(min, value);
                }
                this.min = min;
            }
        }

        public static DurableBeforeLinear create(AbstractRanges ranges, @Nonnull TxnId majority, @Nonnull TxnId universal)
        {
            return create(ranges, new DurableBefore.Entry(majority, universal));
        }

        public static DurableBeforeLinear create(AbstractRanges ranges, DurableBefore.Entry entry)
        {
            if (ranges.isEmpty())
                return DurableBeforeLinear.EMPTY;

            return create(ranges, entry, Builder::new);
        }

        public static DurableBeforeLinear merge(DurableBeforeLinear a, DurableBeforeLinear b)
        {
            return ReducingIntervalMap.merge(a, b, DurableBefore.Entry::max, Builder::new);
        }

        public Status.Durability.HasOutcome min(TxnId txnId, Unseekables<?> unseekables)
        {
            return notDurableIfNull(foldlWithDefault(unseekables, DurableBefore.Entry::mergeMin, DurableBefore.Entry.NONE, null, txnId, test -> test == None));
        }

        public DurableBefore.Entry minEntry(Unseekables<?> unseekables)
        {
            return foldlWithDefault(unseekables, DurableBefore.Entry::min, DurableBefore.Entry.NONE, DurableBefore.Entry.MAX);
        }

        public Status.Durability.HasOutcome max(TxnId txnId, Unseekables<?> unseekables)
        {
            return notDurableIfNull(foldl(unseekables, DurableBefore.Entry::mergeMax, null, txnId, test -> test == Universal));
        }

        public Status.Durability.HasOutcome get(TxnId txnId, RoutingKey participant)
        {
            DurableBefore.Entry entry = get(participant);
            return entry == null ? None : entry.get(txnId);
        }

        public boolean isUniversal(TxnId txnId, RoutingKey participant)
        {
            return get(txnId, participant) == Universal;
        }

        public Status.Durability.HasOutcome min(TxnId txnId)
        {
            if (min.universal.compareTo(txnId) > 0)
                return Universal;
            if (min.quorum.compareTo(txnId) > 0)
                return Quorum;
            return None;
        }

        public long maxEpoch()
        {
            return foldl((e, v) -> TxnId.max(v, TxnId.max(e.quorum, e.universal)), TxnId.NONE, alwaysFalse()).epoch();
        }

        private static Status.Durability.HasOutcome notDurableIfNull(Status.Durability.HasOutcome status)
        {
            return status == null ? None : status;
        }

        public static DurableBeforeLinear from(DurableBefore copy)
        {
            IntervalBuilder builder = new IntervalBuilder(copy.size());
            for (DurableBefore.Entry e : copy)
                builder.append(e.start(), e.end(), e);
            return builder.build();
        }

        public boolean isEqualTo(DurableBefore that)
        {
            int i = 0;
            for (DurableBefore.Entry e : that)
            {
                if (i >= this.size() || this.valueAt(i) == null && ++i == this.size())
                    return false;
                if (!e.equalsRange(this.startAt(i), this.startAt(i + 1)))
                    return false;
                if (!e.equalsIgnoreRange(this.valueAt(i)))
                    return false;
                ++i;
            }
            return i == this.size();
        }

        static class IntervalBuilder extends AbstractIntervalBuilder<RoutingKey, DurableBefore.Entry, DurableBeforeLinear>
        {
            protected IntervalBuilder(int capacity)
            {
                super(capacity);
            }

            @Override
            protected DurableBeforeLinear buildInternal()
            {
                return new DurableBeforeLinear(starts.toArray(new RoutingKey[0]), values.toArray(new DurableBefore.Entry[0]));
            }

            @Override
            protected DurableBefore.Entry slice(RoutingKey start, RoutingKey end, DurableBefore.Entry value)
            {
                return new DurableBefore.Entry(start, end, value.quorum, value.universal);
            }

            @Override
            protected DurableBefore.Entry reduce(DurableBefore.Entry a, DurableBefore.Entry b)
            {
                return DurableBefore.Entry.max(a, b);
            }
        }

        static class Builder extends AbstractBoundariesBuilder<RoutingKey, DurableBefore.Entry, DurableBeforeLinear>
        {
            protected Builder(int capacity)
            {
                super(capacity);
            }

            @Override
            protected DurableBeforeLinear buildInternal()
            {
                if (values.isEmpty())
                    return EMPTY;

                return new DurableBeforeLinear(starts.toArray(new RoutingKey[0]), values.toArray(new DurableBefore.Entry[0]));
            }
        }
    }
}
