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

package accord.primitives;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

import accord.api.Key;
import accord.impl.IntKey;
import accord.utils.AccordGens;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

class DepsTest
{
    @Test
    void test()
    {
        qt().forAll(AccordGens.depsFromKey(AccordGens.intKeys(), AccordGens.ranges(AccordGens.intRoutingKey(), (ignore, a, b) -> IntKey.range(a, b)))).check(deps -> {
            validateEquals(deps);
            validateContains(deps);
            validateSelfWith(deps);
            validateIndexes(deps);
            validateMaxTxnId(deps);
            validateIntersects(deps);
        });
    }

    private static void validateEquals(Deps deps)
    {
        assertThat(deps).isNotEqualTo(null);
        Deps clone = new Deps(deps.keyDeps, deps.rangeDeps, deps.directKeyDeps);
        assertThat(clone).isEqualTo(deps);
        assertThat(deps).isEqualTo(clone);
    }

    private static void validateContains(Deps deps)
    {
        for (TxnId id : deps.keyDeps.txnIds())
            assertThat(deps.contains(id)).isTrue();
        for (TxnId id : deps.directKeyDeps.txnIds())
            assertThat(deps.contains(id)).isTrue();
        for (TxnId id : deps.rangeDeps.txnIds())
            assertThat(deps.contains(id)).isTrue();
    }

    private static void validateSelfWith(Deps deps)
    {
        assertThat(deps.with(deps)).isEqualTo(deps);
    }

    private static void validateIndexes(Deps deps)
    {
        int index = 0;
        List<TxnId> ids = new ArrayList<>(deps.keyDeps.txnIdCount() + deps.directKeyDeps.txnIdCount() + deps.rangeDeps.txnIdCount());
        for (TxnId id : deps.keyDeps.txnIds())
        {
            assertThat(deps.txnId(index)).describedAs("Expected key deps txn at index %d", index).isEqualTo(id);
            ids.add(id);
            index++;
        }
        for (TxnId id : deps.directKeyDeps.txnIds())
        {
            assertThat(deps.txnId(index)).describedAs("Expected direct key deps txn at index %d", index).isEqualTo(id);
            ids.add(id);
            index++;
        }
        for (TxnId id : deps.rangeDeps.txnIds())
        {
            assertThat(deps.txnId(index)).describedAs("Expected range deps txn at index %d", index).isEqualTo(id);
            ids.add(id);
            index++;
        }
        assertThat(deps.txnIds()).isEqualTo(ids);
    }

    private static void validateMaxTxnId(Deps deps)
    {
        assertThat(deps.maxTxnId()).isEqualTo(deps.txnIds().stream().max(Comparator.naturalOrder()).orElse(null));
    }

    private static void validateIntersects(Deps deps)
    {
        Ranges covering = ranges(deps);
        deps.txnIds().forEach(id -> assertThat(deps.intersects(id, covering)));
    }

    private static Ranges ranges(Deps deps)
    {
        List<Range> ranges = new ArrayList<>();
        for (Key key : deps.keyDeps.keys)
            ranges.add(key.asRange());
        for (Key key : deps.directKeyDeps.keys)
            ranges.add(key.asRange());
        for (Range range : deps.rangeDeps.ranges)
            ranges.add(range);
        return Ranges.of(ranges.toArray(Range[]::new));
    }
}