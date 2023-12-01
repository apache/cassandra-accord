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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.ProgressLog;
import accord.impl.DepsBuilder;
import accord.impl.DepsBuilder.Builder;
import accord.impl.IntKey;
import accord.local.*;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Predicate;

public class DepsBuilderTest
{
    private static final Key KEY = IntKey.key(1);
    private static class InstrumentedSafeStore extends SafeCommandStore
    {
        public static class Removed
        {
            public final Set<TxnId> commands = new HashSet<>();

            void add(TxnId txnId, Timestamp executeAt)
            {
                commands.add(txnId);
            }
        }

        Map<Seekable, Removed> removed = new HashMap<>();

        @Override
        public void removeCommandFromSeekableDeps(Seekable seekable, TxnId txnId, Timestamp executeAt, Status status)
        {
            removed.computeIfAbsent(seekable, s -> new Removed()).add(txnId, executeAt);
        }

        @Override protected SafeCommand getInternal(TxnId txnId) { throw new UnsupportedOperationException(); }
        @Override protected SafeCommand getInternalIfLoadedAndInitialised(TxnId txnId) { throw new UnsupportedOperationException(); }
        @Override public boolean canExecuteWith(PreLoadContext context) { throw new UnsupportedOperationException(); }
        @Override public <P1, T> T mapReduce(Seekables<?, ?> keys, Ranges slice, KeyHistory keyHistory, Txn.Kind.Kinds testKind, TestTimestamp testTimestamp, Timestamp timestamp, TestDep testDep, @Nullable TxnId depId, @Nullable Status minStatus, @Nullable Status maxStatus, CommandFunction<P1, T, T> map, P1 p1, T initialValue, Predicate<? super T> terminate) { throw new UnsupportedOperationException(); }
        @Override protected void register(Seekables<?, ?> keysOrRanges, Ranges slice, Command command) { throw new UnsupportedOperationException(); }
        @Override protected void register(Seekable keyOrRange, Ranges slice, Command command) { throw new UnsupportedOperationException(); }
        @Override public CommandStore commandStore() { throw new UnsupportedOperationException(); }
        @Override public DataStore dataStore() { throw new UnsupportedOperationException(); }
        @Override public Agent agent() { throw new UnsupportedOperationException(); }
        @Override public ProgressLog progressLog() { throw new UnsupportedOperationException(); }
        @Override public NodeTimeService time() { throw new UnsupportedOperationException(); }
        @Override public CommandStores.RangesForEpoch ranges() { throw new UnsupportedOperationException(); }
        @Override public Timestamp maxConflict(Seekables<?, ?> keys, Ranges slice) { throw new UnsupportedOperationException(); }
        @Override public void registerHistoricalTransactions(Deps deps) { throw new UnsupportedOperationException(); }
        @Override public void erase(SafeCommand safeCommand) { throw new UnsupportedOperationException(); }
    }
    private static final Node.Id NODE = new Node.Id(0);

    private static TxnId txnId(int v)
    {
        return TxnId.fromValues(1, v, 0, 0);
    }

    private static List<TxnId> txnIds(int... vs)
    {
        ImmutableList.Builder<TxnId> builder = ImmutableList.builder();
        Arrays.sort(vs);
        for (int v : vs)
            builder.add(txnId(v));
        return builder.build();
    }

    private static Timestamp timestamp(int v)
    {
        return Timestamp.fromValues(1, v, NODE);
    }

    private static List<TxnId> build(SafeCommandStore safeStore, Seekable seekable, Builder builder)
    {
        List<TxnId> result = new ArrayList<>();
        builder.build(safeStore, seekable, result::add);
        result.sort(Comparator.naturalOrder());
        return result;
    }

    private static List<TxnId> build(SafeCommandStore safeStore, Builder builder)
    {
        return build(safeStore, KEY, builder);
    }

    @BeforeAll
    static void setUpClass()
    {
        DepsBuilder.setCanPruneUnsafe(true);
    }

    @AfterAll
    static void tearDownClass()
    {
        DepsBuilder.setCanPruneUnsafe(false);
    }

    @Test
    void basicTest()
    {
        InstrumentedSafeStore safeStore = new InstrumentedSafeStore();
        Builder builder = new Builder();
        builder.add(txnId(0), Status.Applied, timestamp(0), txnIds());
        builder.add(txnId(1), Status.Applied, timestamp(1), txnIds(0));
        Assertions.assertEquals(txnIds(1), build(safeStore, builder));
    }

    /**
     * Committed deps should be included if they point to an uncommitted command
     */
    @Test
    void uncommittedTest()
    {
        InstrumentedSafeStore safeStore = new InstrumentedSafeStore();
        Builder builder = new Builder();
        builder.add(txnId(0), Status.Accepted, timestamp(0), txnIds());
        builder.add(txnId(1), Status.Applied, timestamp(1), txnIds(0));
        builder.add(txnId(2), Status.Applied, timestamp(2), txnIds(1));
        builder.add(txnId(3), Status.Applied, timestamp(3), txnIds(2));
        Assertions.assertEquals(txnIds(0, 1, 2, 3), build(safeStore, builder));
    }

    /**
     * Committed txns should be included if they point to a command with a later execution time
     */
    @Test
    void executionOrderTest()
    {
        InstrumentedSafeStore safeStore = new InstrumentedSafeStore();
        Builder builder = new Builder();
        builder.add(txnId(0), Status.Applied, timestamp(2), txnIds());
        builder.add(txnId(1), Status.Applied, timestamp(1), txnIds(0));
        Assertions.assertEquals(txnIds(0, 1), build(safeStore, builder));
    }
}
