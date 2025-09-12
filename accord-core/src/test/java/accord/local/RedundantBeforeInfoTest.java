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
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import accord.api.RoutingKey;
import accord.local.RedundantStatus.SomeStatus;
import accord.local.RedundantStatus.Property;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;
import org.agrona.collections.Object2ObjectHashMap;
import org.assertj.core.api.Assertions;

import static accord.impl.IntKey.routing;
import static accord.local.RedundantStatus.ONLY_LE_MASK;
import static accord.local.RedundantStatus.PRE_BOOTSTRAP_MERGE_MASK;
import static accord.local.RedundantStatus.Property.PRE_BOOTSTRAP_OR_STALE;
import static accord.local.RedundantStatus.selectOrCreate;
import static accord.local.RedundantStatus.toAll;

public class RedundantBeforeInfoTest
{
    @Test
    public void test()
    {
        for (int i = 0; i < 100 ; ++i)
        {
            RandomTestRunner.test().check(rs -> {
                test(rs, 1000, 10, rs.nextInt(2, 10), 1024, rs.nextInt(2, 4), 128 * rs.nextInt(1, 16));
            });
        }
    }

    static class Unmerged extends RedundantStatus
    {
        final int insertCounter;
        Unmerged(int encoded, int insertCounter)
        {
            super(encoded);
            this.insertCounter = insertCounter;
        }

        @Override
        public String toString()
        {
            return insertCounter + ":" + super.toString();
        }
    }

    static class CanonStatus
    {
        final List<Unmerged> unmerged;
        SomeStatus mergedLt, mergedLe;

        CanonStatus(List<Unmerged> unmerged, SomeStatus mergedLt, SomeStatus mergedLe)
        {
            this.unmerged = unmerged;
            this.mergedLt = mergedLt;
            this.mergedLe = mergedLe;
        }

        CanonStatus add(CanonStatus add)
        {
            List<Unmerged> unmerged = new ArrayList<>();
            unmerged.addAll(this.unmerged);
            unmerged.addAll(add.unmerged);
            SomeStatus lt = merge(filter(add.mergedLt, mergedLt), mergedLt);
            SomeStatus le = merge(filter(add.mergedLe, mergedLe), mergedLe);
            return new CanonStatus(unmerged, lt, le);
        }

        @Override
        public String toString()
        {
            return unmerged.toString();
        }
    }

    private static void test(RandomSource rs, int outerLoop, int innerLoop, int keyCount, int keyDomain, int epochDomain, int hlcDomain)
    {
        RoutingKey[] keys = new RoutingKey[keyCount];
        {
            BitSet used = new BitSet();
            for (int i = 0 ; i < keys.length ; ++i)
            {
                int v = rs.nextInt(1, keyDomain);
                while (used.get(v))
                    v = rs.nextInt(1, keyDomain);
                keys[i] = routing(v);
            }
        }

        IntSupplier epochSupplier = rs.uniformInts(1, 1 + epochDomain);
        IntSupplier hlcSupplier = rs.uniformInts(1, 1 + hlcDomain);
        Supplier<Property> picker = rs.randomWeightedPicker(Property.values());
        Supplier<TxnId> txnIdSupplier = () -> new TxnId(epochSupplier.getAsInt(), hlcSupplier.getAsInt(), Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, Node.Id.NONE);

        Object2ObjectHashMap<RoutingKey, TreeMap<TxnId, CanonStatus>> canon = new Object2ObjectHashMap<>();
        for (RoutingKey key : keys)
            canon.put(key, new TreeMap<>());

        RedundantBefore redundantBefore = RedundantBefore.EMPTY;
        for (int i = 0 ; i < outerLoop ; ++i)
        {
            int ignore = 0;
            for (int j = 0 ; j < innerLoop ; ++j)
            {
                SomeStatus status = RedundantStatus.oneSlow(picker.get());
                TxnId txnId = txnIdSupplier.get();
                if (status.is(Property.GC_BEFORE))
                    txnId = txnId.addFlag(Timestamp.Flag.SHARD_BOUND);

                RoutingKey key = keys[rs.nextInt(keys.length)];
                addCanon(txnId, status, canon.get(key));
                redundantBefore = RedundantBefore.merge(redundantBefore, RedundantBefore.create(Ranges.of(key.asRange()), txnId, status));
                ++ignore;
            }

            // query
            for (int j = 0; j < innerLoop * 10 ; ++j)
            {
                TxnId txnId = txnIdSupplier.get();
                RoutingKey key = keys[rs.nextInt(keys.length)];

                RedundantStatus canonStatus = toAll(getCanon(txnId, canon.get(key)));
                RedundantBefore.Bounds bounds = redundantBefore.get(key);
                RedundantStatus status = bounds == null ? RedundantStatus.NONE : bounds.getIgnoringOwnership(txnId, null);
                Assertions.assertThat(status).isEqualTo(canonStatus);
            }
        }
    }

    static void addCanon(TxnId txnId, SomeStatus status, TreeMap<TxnId, CanonStatus> map)
    {
        SomeStatus add;
        {
            Map.Entry<TxnId, CanonStatus> e = map.ceilingEntry(txnId);
            SomeStatus mergele, mergelt;
            if (e == null) mergele = mergelt = SomeStatus.NONE;
            else if (e.getKey().equals(txnId)) { mergele = e.getValue().mergedLe; mergelt = e.getValue().mergedLt; }
            else mergele = mergelt = e.getValue().mergedLt;
            SomeStatus le = merge(filter(le(status), mergele), mergele);
            SomeStatus lt = merge(filter(status, mergelt), mergelt);
            add = map.merge(txnId, new CanonStatus(Collections.singletonList(new Unmerged(status.encoded, map.size())), lt, le), CanonStatus::add).mergedLt;
        }

        for (Map.Entry<TxnId, CanonStatus> e : map.headMap(txnId, false).descendingMap().entrySet())
        {
            CanonStatus cur = e.getValue();
            if ((cur.mergedLe.encoded & add.encoded) == add.encoded)
                break;
            cur.mergedLe = merge(filter(add, cur.mergedLe), cur.mergedLe);
            if ((cur.mergedLt.encoded & add.encoded) == add.encoded)
                break;
            cur.mergedLt = merge(filter(add, cur.mergedLt), cur.mergedLt);
        }
    }

    private static SomeStatus getCanon(TxnId txnId, TreeMap<TxnId, CanonStatus> canon)
    {
        Map.Entry<TxnId, CanonStatus> e = canon.ceilingEntry(txnId);
        if (e == null) return SomeStatus.NONE;
        else if (e.getKey().equals(txnId)) return e.getValue().mergedLe;
        else return e.getValue().mergedLt;
    }

    private static int merge(int a, int b)
    {
        return a | b;
    }

    private static SomeStatus merge(SomeStatus a, SomeStatus b)
    {
        return RedundantStatus.selectOrCreate(merge(a.encoded, b.encoded), a, b);
    }

    private static SomeStatus filter(SomeStatus a, SomeStatus history)
    {
        if (history.is(PRE_BOOTSTRAP_OR_STALE))
            return selectOrCreate((short) (a.encoded & PRE_BOOTSTRAP_MERGE_MASK), a, history);
        return a;
    }

    private static SomeStatus le(SomeStatus le)
    {
        short masked = (short) (le.encoded & ONLY_LE_MASK);
        return masked == le.encoded ? le : new SomeStatus(masked);
    }
}
