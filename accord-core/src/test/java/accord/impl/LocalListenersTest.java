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

package accord.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.LocalListeners;
import accord.api.LocalListeners.ComplexListener;
import accord.api.RemoteListeners.NoOpRemoteListeners;
import accord.local.Command;
import accord.local.CommonAttributes;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;

import static accord.primitives.Status.Durability.NotDurable;

public class LocalListenersTest
{
    @Test
    public void test()
    {
        Random random = new Random();
        for (int i = 0 ; i < 100 ; ++i)
            testOne(random.nextLong(), 1000 + random.nextInt(9000));
    }

    @Test
    public void testOne()
    {
        testOne(-905746076555856133L, 10000);
    }

    private void testOne(long seed, int ops)
    {
        System.out.println(seed);
        RandomTestRunner.test().withSeed(seed).check(rnd -> new TestCase(rnd).run(ops));
    }

    static class TestCase
    {
        final RandomSource rnd;
        final Function<RandomSource, TxnId> txnIdFunction;
        final Supplier<TxnId> txnIds;
        final Supplier<SaveStatus> awaits;
        final float reentrantRemoveChance, reentrantInsertChance, updateExistingChance;
        final int maxReentrantInsertSize;
        boolean permitReentrantInsert = true;

        final SavingNotifySink canonicalSink = new SavingNotifySink(this, true);
        final TreeMap<TxnId, TxnState> state = new TreeMap<>();
        final SavingNotifySink testSink = new SavingNotifySink(this, false);
        final LocalListeners test = new DefaultLocalListeners(new NoOpRemoteListeners(), testSink);
        int nextCanonicalId = 0, nextTestId;

        TestCase(RandomSource rnd)
        {
            this.rnd = rnd;
            Function<RandomSource, TxnId> txnIdFunction = AccordGens.txnIds()::next;
            if (rnd.decide(0.5f)) this.txnIdFunction = txnIdFunction;
            else
            {
                int count = 8 << rnd.nextInt(1, 6);
                TxnId[] txnIds = Stream.generate(() -> txnIdFunction.apply(rnd)).limit(count).toArray(TxnId[]::new);
                this.txnIdFunction = rs -> txnIds[rs.nextInt(txnIds.length)];
            }
            this.txnIds = () -> txnIdFunction.apply(rnd);
            this.awaits = rnd.randomWeightedPicker(SaveStatus.values());

            reentrantInsertChance = rnd.nextFloat() * 0.5f;
            reentrantRemoveChance = rnd.nextFloat() * 0.5f;
            updateExistingChance = rnd.nextFloat() * 0.5f;
            maxReentrantInsertSize = rnd.nextInt(16, 128);
        }

        void run(int ops)
        {
            float notifyChance = 0.5f * rnd.nextFloat();
            float objRatio = rnd.nextFloat();
            for (int op = 0; op < ops; ++op)
            {
                if (state.isEmpty() || !rnd.decide(notifyChance)) registerOne(objRatio);
                else notifyOne(awaits.get());
            }
            permitReentrantInsert = false;
            while (!state.isEmpty())
                notifyOne(SaveStatus.Invalidated);
        }

        void registerOne(float objRatio)
        {
            TxnId txnId = rnd.decide(updateExistingChance) ? tryPickExisting() : txnIds.get();
            TxnState state = this.state.computeIfAbsent(txnId, ignore -> new TxnState());
            int registerCount = rnd.nextInt(1, 10);
            while (registerCount-- > 0)
            {
                if (rnd.decide(objRatio)) addObjectListeners(txnId, state);
                else addStatusListeners(txnId, state);
            }
        }

        TxnId tryPickExisting()
        {
            TxnId txnId = txnIds.get();
            if (state.isEmpty())
                return txnId;

            txnId = state.floorKey(txnId);
            if (txnId == null) txnId = state.firstKey();
            return txnId;
        }

        void notifyOne(SaveStatus newStatus)
        {
            TxnId txnId = tryPickExisting();
            TxnState state = this.state.get(txnId);
            TestSafeCommand safeCommand = new TestSafeCommand(txnId, newStatus, NotDurable);

            {
                List<TestListener> snapshot = new ArrayList<>(state.canonObjs);
                for (TestListener listener : snapshot)
                {
                    if (!state.canonObjs.contains(listener)) continue;
                    if (!canonicalSink.notify(null, safeCommand, listener))
                        state.canonObjs.remove(listener);
                }
            }

            {
                Map<SaveStatus, TreeSet<TxnId>> subMap = state.canonTxns.headMap(newStatus, true);
                List<TreeSet<TxnId>> snapshot = new ArrayList<>(subMap.values());
                snapshot.forEach(ids -> ids.forEach(id -> canonicalSink.notify(null, safeCommand, id)));
                snapshot.forEach(ids -> state.canonTxnCount -= ids.size()); // done separately for consistency with testTxns
                subMap.clear();
            }
            test.notify(null, safeCommand, null);
            {
                Map<SaveStatus, TreeSet<TxnId>> subMap = state.testTxns.headMap(newStatus, true);
                subMap.values().forEach(ids -> {
                    state.testTxnCount -= ids.size();
                });
                subMap.clear();
            }
            Assertions.assertEquals(canonicalSink.objs, testSink.objs);
            Assertions.assertEquals(canonicalSink.objResults, testSink.objResults);
            Assertions.assertEquals(canonicalSink.txns, testSink.txns);
            Assertions.assertEquals(state.testTxns, state.canonTxns);
            if (state.canonTxns.isEmpty() && state.canonObjs.isEmpty())
            {
                Invariants.checkState(state.testObjs.isEmpty());
                Invariants.checkState(state.testTxns.isEmpty());
                this.state.remove(txnId);
            }
            canonicalSink.clear();
            testSink.clear();
        }

        void onNotify(TestListener listener)
        {
            TxnState state = this.state.get(listener.txnId);
            Random rnd = new Random(listener.txnId.hashCode() ^ listener.count);
            boolean insert = permitReentrantInsert
                             && (listener.canonical ? state.canonObjs.size() : state.testObjs.size()) < maxReentrantInsertSize
                             && rnd.nextFloat() < reentrantInsertChance;
            boolean remove = rnd.nextFloat() < reentrantRemoveChance;

            if (insert && remove)
            {
                if (rnd.nextBoolean())
                {
                    addObjectListener(listener.txnId, state, listener.canonical, rnd);
                    reentrantRemove(listener, rnd);
                }
                else
                {
                    reentrantRemove(listener, rnd);
                    addObjectListener(listener.txnId, state, listener.canonical, rnd);
                }
            }
            else if (insert)
            {
                addObjectListener(listener.txnId, state, listener.canonical, rnd);
            }
            else if (remove)
            {
                reentrantRemove(listener, rnd);
            }
        }

        private void addObjectListeners(TxnId txnId, TxnState state)
        {
            int invokeCount = rnd.nextInt(1, 4);
            TestListener testListener = new TestListener(this, txnId, nextTestId++, false, invokeCount);
            state.testObjs.put(testListener, test.register(txnId, testListener));
            state.canonObjs.add(new TestListener(this, txnId, nextCanonicalId++, true, invokeCount));
        }

        private void addObjectListener(TxnId txnId, TxnState state, boolean canonical, Random rnd)
        {
            int invokeCount = 1 + rnd.nextInt(3);
            if (canonical) state.canonObjs.add(new TestListener(this, txnId, nextCanonicalId++, true, invokeCount));
            else
            {
                TestListener listener = new TestListener(this, txnId, nextTestId++, false, invokeCount);
                state.testObjs.put(listener, test.register(txnId, listener));
            }
        }

        private void reentrantRemove(TestListener listener, Random rnd)
        {
            TxnState state = this.state.get(listener.txnId);
            int pick = rnd.nextInt(listener.canonical ? state.canonObjs.size() : state.testObjs.size());
            if (listener.canonical)
            {
                TestListener remove = Iterables.get(state.canonObjs, pick);
                state.canonObjs.remove(remove);
            }
            else
            {
                Map.Entry<TestListener, LocalListeners.Registered> remove = Iterables.get(state.testObjs.entrySet(), pick);
                state.testObjs.remove(remove.getKey());
                remove.getValue().cancel();
            }
        }

        void onNotify(TxnId txnId, SaveStatus saveStatus, TxnId listenerId, boolean canonical)
        {
            TxnState state = this.state.get(txnId);
            Random rnd = new Random(listenerId.hashCode() ^ saveStatus.ordinal());
            boolean insert =    permitReentrantInsert
                             && saveStatus != SaveStatus.Invalidated
                             && (canonical ? state.canonTxnCount : state.testTxnCount) < maxReentrantInsertSize
                             && rnd.nextFloat() < reentrantInsertChance;

            if (insert)
            {
                int range = SaveStatus.Invalidated.ordinal() - (1 + saveStatus.ordinal());
                int offset = range == 0 ? 0 : rnd.nextInt(range);
                SaveStatus await = SaveStatus.forOrdinal(1 + offset + saveStatus.ordinal());
                addStatusListener(txnId, state, await, canonical, rnd);
            }
        }

        private void addStatusListeners(TxnId txnId, TxnState state)
        {
            SaveStatus await = awaits.get();
            TxnId listenerId = txnIds.get();
            if (state.canonTxns.computeIfAbsent(await, ignore -> new TreeSet<>()).add(listenerId))
                ++state.canonTxnCount;
            if (state.testTxns.computeIfAbsent(await, ignore -> new TreeSet<>()).add(listenerId))
                ++state.testTxnCount;
            test.register(txnId, await, listenerId);
        }

        private void addStatusListener(TxnId txnId, TxnState state, SaveStatus await, boolean canonical, Random rnd)
        {
            TxnId listenerId = txnIdFunction.apply(RandomSource.wrap(rnd));
            if (canonical && state.canonTxns.computeIfAbsent(await, ignore -> new TreeSet<>()).add(listenerId))
                ++state.canonTxnCount;
            if (!canonical)
            {
                if (state.testTxns.computeIfAbsent(await, ignore -> new TreeSet<>()).add(listenerId))
                    ++state.testTxnCount;
                test.register(txnId, await, listenerId);
            }
        }
    }

    static class TxnState
    {
        final TreeMap<SaveStatus, TreeSet<TxnId>> canonTxns = new TreeMap<>();
        int canonTxnCount;
        final Set<TestListener> canonObjs = new LinkedHashSet<>();
        final TreeMap<SaveStatus, TreeSet<TxnId>> testTxns = new TreeMap<>();
        int testTxnCount;
        final Map<TestListener, LocalListeners.Registered> testObjs = new LinkedHashMap<>();
    }

    static class SavingNotifySink implements DefaultLocalListeners.NotifySink
    {
        final TestCase owner;
        final boolean canonical;
        final List<TxnId> txns = new ArrayList<>();
        final List<ComplexListener> objs = new ArrayList<>();
        final List<Boolean> objResults = new ArrayList<>();

        SavingNotifySink(TestCase owner, boolean canonical)
        {
            this.owner = owner;
            this.canonical = canonical;
        }

        @Override
        public void notify(SafeCommandStore safeStore, SafeCommand safeCommand, TxnId listener)
        {
            owner.onNotify(safeCommand.txnId(), safeCommand.current().saveStatus(), listener, canonical);
            txns.add(listener);
        }

        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand, ComplexListener listener)
        {
            objs.add(listener);
            boolean result = listener.notify(safeStore, safeCommand);
            objResults.add(result);
            return result;
        }

        void clear()
        {
            txns.clear();
            objs.clear();
            objResults.clear();
        }
    }

    static class TestListener implements ComplexListener
    {
        final TestCase owner;
        final TxnId txnId;
        final int id;
        final boolean canonical;
        int count;

        public TestListener(TestCase owner, TxnId txnId, int id, boolean canonical, int count)
        {
            this.owner = owner;
            this.txnId = txnId;
            this.id = id;
            this.canonical = canonical;
            this.count = count;
        }

        @Override
        public boolean notify(SafeCommandStore safeStore, SafeCommand safeCommand)
        {
            Invariants.checkState(count > 0);
            owner.onNotify(this);
            if (--count > 0)
                return true;
            if (!canonical)
                owner.state.get(txnId).testObjs.remove(this);
            return false;
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj.getClass() == TestListener.class && ((TestListener) obj).id == id;
        }

        @Override
        public String toString()
        {
            return Integer.toString(id);
        }
    }

    static class TestSafeCommand extends SafeCommand
    {
        Command current;
        public TestSafeCommand(TxnId txnId, SaveStatus saveStatus, final Durability durability)
        {
            super(txnId);
            current = new TestCommand(txnId, saveStatus, durability);
        }

        @Override
        public Command current() { return current; }

        @Override
        public void invalidate() {}

        @Override
        public boolean invalidated() { return false; }

        @Override
        protected void set(Command command)
        {
            current = command;
        }
    }

    static class TestCommand extends Command
    {
        final TxnId txnId;
        final SaveStatus saveStatus;
        final Durability durability;
        final StoreParticipants participants;

        TestCommand(TxnId txnId, SaveStatus saveStatus, Durability durability)
        {
            this.txnId = txnId;
            this.saveStatus = saveStatus;
            this.durability = durability;
            this.participants = StoreParticipants.empty(txnId);
        }

        @Override
        public StoreParticipants participants()
        {
            return participants;
        }

        @Override
        public TxnId txnId()
        {
            return txnId;
        }

        @Override
        public Ballot promised()
        {
            return null;
        }

        @Override
        public Durability durability()
        {
            return durability;
        }

        @Override
        public SaveStatus saveStatus()
        {
            return saveStatus;
        }

        @Override
        public Timestamp executeAt()
        {
            return null;
        }

        @Override
        public Ballot acceptedOrCommitted()
        {
            return null;
        }

        @Override
        public PartialTxn partialTxn()
        {
            return null;
        }

        @Nullable
        @Override
        public PartialDeps partialDeps()
        {
            return null;
        }

        @Override
        public Command updateAttributes(CommonAttributes attrs, Ballot promised)
        {
            return null;
        }
    }



}
