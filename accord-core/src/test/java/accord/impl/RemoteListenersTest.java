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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.Journal;
import accord.api.ProgressLog;
import accord.api.RemoteListeners;
import accord.api.RemoteListeners.Registration;
import accord.api.RoutingKey;
import accord.impl.LocalListenersTest.TestSafeCommand;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.PreLoadContext;
import accord.local.RedundantBefore;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.RangeDeps;
import accord.primitives.SaveStatus;
import accord.primitives.Status.Durability;
import accord.local.cfk.SafeCommandsForKey;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.utils.AccordGens;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;
import accord.utils.async.AsyncChain;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.ObjectHashSet;

import static accord.primitives.SaveStatus.Uninitialised;
import static accord.primitives.Status.Durability.NotDurable;
import static accord.primitives.Known.KnownRoute.Full;

public class RemoteListenersTest
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
        testOne(-2459738250850982405L, 10000);
    }

    private void testOne(long seed, int ops)
    {
        System.out.println(seed);
        RandomTestRunner.test().withSeed(seed).check(rnd -> new TestCase(rnd).run(ops));
    }

    static class StateKey implements Comparable<StateKey>
    {
        final SaveStatus saveStatus;
        final Durability durability;

        StateKey(SaveStatus saveStatus, Durability durability)
        {
            this.saveStatus = saveStatus;
            this.durability = durability;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StateKey stateKey = (StateKey) o;
            return saveStatus == stateKey.saveStatus && durability == stateKey.durability;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(saveStatus, durability);
        }

        @Override
        public int compareTo(StateKey that)
        {
            int c = this.saveStatus.compareTo(that.saveStatus);
            if (c == 0) c = this.durability.compareTo(that.durability);
            return c;
        }

        @Override
        public String toString()
        {
            return saveStatus + "+" + durability;
        }
    }

    static class State
    {
        final IntHashSet waitingOn = new IntHashSet();
        final ObjectHashSet<RemoteCallback> listeners = new ObjectHashSet<>();

        @Override
        public String toString()
        {
            return listeners.toString();
        }
    }

    static class RemoteCallback implements Comparable<RemoteCallback>
    {
        final int nodeId;
        final int callbackId;

        RemoteCallback(Node.Id nodeId, int callbackId)
        {
            this.nodeId = nodeId.id;
            this.callbackId = callbackId;
        }

        @Override
        public int compareTo(RemoteCallback that)
        {
            int c = Integer.compare(this.nodeId, that.nodeId);
            if (c == 0) c = Integer.compare(this.callbackId, that.callbackId);
            return c;
        }

        @Override
        public String toString()
        {
            return nodeId + "/" + callbackId;
        }
    }

    static class TestCase
    {
        final RandomSource rnd;
        final Supplier<TxnId> txnIds;
        final Supplier<Node.Id> nodeIds;
        final Supplier<SaveStatus> awaits;
        final Supplier<Durability> durabilities;
        final IntSupplier storeIds;
        final IntSupplier storeIdCount;

        final SavingNotifySink canonicalSink = new SavingNotifySink();
        final TreeMap<TxnId, TreeMap<StateKey, State>> canonical = new TreeMap<>();

        final SavingNotifySink testSink = new SavingNotifySink();
        final RemoteListeners test = new DefaultRemoteListeners(testSink);

        final IntHashSet tmpUniqueStoreIds = new IntHashSet();

        TestCase(RandomSource rnd)
        {
            this.rnd = rnd;
            Supplier<TxnId> txnIdSupplier = AccordGens.txnIds().asSupplier(rnd);
            if (rnd.decide(0.5f)) this.txnIds = txnIdSupplier;
            else
            {
                int count = 8 << rnd.nextInt(1, 6);
                TxnId[] txnIds = Stream.generate(txnIdSupplier).limit(count).toArray(TxnId[]::new);
                this.txnIds = () -> txnIds[rnd.nextInt(txnIds.length)];
            }

            int nodeCount = rnd.nextBoolean() ? rnd.nextInt(10, 1000) : Integer.MAX_VALUE;
            this.nodeIds = () -> new Node.Id(rnd.nextInt(0, nodeCount));
            this.awaits = rnd.randomWeightedPicker(Arrays.stream(SaveStatus.values())
                                                         .filter(s -> s.known.route == Full)
                                                         .toArray(SaveStatus[]::new));
            this.durabilities = rnd.randomWeightedPicker(Durability.values());

            int maxStoreId = rnd.nextInt(16, 128);
            int maxStoreIdCount = rnd.nextInt(2, maxStoreId / 4);
            this.storeIdCount = () -> rnd.nextInt(1, maxStoreIdCount);
            this.storeIds = () -> rnd.nextInt(maxStoreId);
        }

        void run(int ops)
        {
            float notifyChance = 0.5f + (0.4f * rnd.nextFloat());
            float notifyRatio = 0.5f + (0.4f * rnd.nextFloat());
            for (int op = 0; op < ops; ++op)
            {
                if (canonical.isEmpty() || !rnd.decide(notifyChance)) registerOne();
                else notifyOne(awaits.get(), durabilities.get(), notifyRatio);
            }
            while (!canonical.isEmpty())
                notifyOne(SaveStatus.Invalidated, Durability.Universal, notifyRatio);
        }

        void registerOne()
        {
            TxnId txnId = txnIds.get();
            TreeMap<StateKey, State> stateMap = canonical.computeIfAbsent(txnId, ignore -> new TreeMap<>());
            int registerCount = rnd.nextInt(1, 10);
            while (registerCount-- > 0)
            {
                SaveStatus awaitSaveStatus = awaits.get();
                Durability awaitDurability = durabilities.get();
                Node.Id nodeId = nodeIds.get();
                int callbackId = rnd.nextInt(0, Integer.MAX_VALUE);

                StateKey key = new StateKey(awaitSaveStatus, awaitDurability);
                State state = stateMap.computeIfAbsent(key, ignore -> new State());
                state.listeners.add(new RemoteCallback(nodeId, callbackId));

                int waitingCount = storeIdCount.getAsInt();

                Registration registration = test.register(txnId, awaitSaveStatus, awaitDurability, nodeId, callbackId);

                for (int i = 0 ; i < waitingCount ; ++i)
                {
                    int storeId = storeIds.getAsInt();
                    state.waitingOn.add(storeId);
                    tmpUniqueStoreIds.add(storeId);
                    SafeCommandStore safeStore = new TestSafeCommandStore(storeId);
                    registration.add(safeStore, new TestSafeCommand(txnId, Uninitialised, NotDurable));
                }

                int[] uniqueStoreIds = tmpUniqueStoreIds.stream().mapToInt(i -> i).toArray();
                Arrays.sort(uniqueStoreIds);
                Assertions.assertEquals(tmpUniqueStoreIds.size(), registration.done());
                tmpUniqueStoreIds.clear();
            }
        }

        void notifyOne(SaveStatus newStatus, Durability newDurability, float notifyRatio)
        {
            TxnId txnId;
            {
                TxnId tmp = canonical.floorKey(txnIds.get());
                txnId = tmp == null ? canonical.firstKey() : tmp;
            }
            TreeMap<StateKey, State> stateMap = canonical.get(txnId);
            TestSafeCommand safeCommand = new TestSafeCommand(txnId, newStatus, newDurability);

            Map<StateKey, State> subMap = stateMap.headMap(new StateKey(newStatus, newDurability), true);
            subMap.forEach((key, state) -> {
                if (key.durability.compareTo(newDurability) > 0)
                    return;

                IntHashSet.IntIterator iterator = state.waitingOn.iterator();
                while (iterator.hasNext())
                {
                    int storeId = iterator.nextValue();
                    if (rnd.decide(notifyRatio))
                        tmpUniqueStoreIds.add(storeId);
                }
            });
            tmpUniqueStoreIds.forEach(storeId -> {
                test.notify(new TestSafeCommandStore(storeId), safeCommand, null);
            });
            subMap.entrySet().removeIf((entry) -> {
                if (entry.getKey().durability.compareTo(newDurability) > 0)
                    return false;

                State state = entry.getValue();
                state.waitingOn.removeAll(tmpUniqueStoreIds);
                if (!state.waitingOn.isEmpty())
                    return false;

                long[] listeners = state.listeners.stream().mapToLong(l -> DefaultRemoteListeners.encodeListener(l.nodeId, l.callbackId)).toArray();
                Arrays.sort(listeners);
                canonicalSink.notify(txnId, newStatus, null, listeners, listeners.length);
                return true;
            });
            tmpUniqueStoreIds.clear();
            canonicalSink.saved.sort(SavingNotifySink.Saved::compareTo);
            testSink.saved.sort(SavingNotifySink.Saved::compareTo);
            Assertions.assertEquals(canonicalSink.saved, testSink.saved);
            if (stateMap.isEmpty())
                canonical.remove(txnId);
            canonicalSink.clear();
            testSink.clear();
        }
    }

    static class SavingNotifySink implements DefaultRemoteListeners.NotifySink
    {
        static class Saved implements Comparable<Saved>
        {
            final TxnId txnId;
            final SaveStatus saveStatus;
            final long[] listeners;

            Saved(TxnId txnId, SaveStatus saveStatus, long[] listeners)
            {
                this.txnId = txnId;
                this.saveStatus = saveStatus;
                this.listeners = listeners;
            }

            @Override
            public boolean equals(Object that)
            {
                return that instanceof Saved && equals((Saved) that);
            }

            public boolean equals(Saved that)
            {
                return txnId.equals(that.txnId) && saveStatus.equals(that.saveStatus) && Arrays.equals(listeners, that.listeners);
            }

            @Override
            public int compareTo(Saved that)
            {
                int c = txnId.compareTo(that.txnId);
                if (c == 0) c = saveStatus.compareTo(that.saveStatus);
                if (c == 0)
                {
                    int i = 0, j = 0;
                    while (c == 0 && i < this.listeners.length && j < that.listeners.length)
                        c = Long.compare(this.listeners[i++], that.listeners[j++]);
                    if (c == 0) c = this.listeners.length - that.listeners.length;
                }
                return c;
            }

            @Override
            public String toString()
            {
                return txnId + "@" + saveStatus + ": " + Arrays.toString(listeners);
            }
        }

        final List<Saved> saved = new ArrayList<>();

        @Override
        public void notify(TxnId txnId, SaveStatus saveStatus, Route<?> route, long[] listeners, int listenerCount)
        {
            saved.add(new Saved(txnId, saveStatus, Arrays.copyOf(listeners, listenerCount)));
        }

        void clear()
        {
            saved.clear();
        }
    }

    static class TestCommandStore extends CommandStore
    {
        final int storeId;

        protected TestCommandStore(int id)
        {
            super(id,
                  null,
                  null,
                  null,
                  ignore -> new ProgressLog.NoOpProgressLog(),
                  ignore -> new DefaultLocalListeners(new DefaultRemoteListeners((a, b, c, d, e)->{}),
                                                      DefaultLocalListeners.DefaultNotifySink.INSTANCE),
                  new EpochUpdateHolder());
            this.storeId = id;
        }

        @Override
        public Journal.Loader loader()
        {
            throw new UnsupportedOperationException();
        }

        @Override public boolean inStore() { return false; }
        @Override public AsyncChain<Void> execute(PreLoadContext context, Consumer<? super SafeCommandStore> consumer) { return null; }
        @Override public <T> AsyncChain<T> submit(PreLoadContext context, Function<? super SafeCommandStore, T> apply) { return null; }
        @Override public void shutdown() {}
        @Override protected void registerTransitive(SafeCommandStore safeStore, RangeDeps deps) { }
        @Override public <T> AsyncChain<T> submit(Callable<T> task) { return null; }
    }

    static class TestSafeCommandStore extends SafeCommandStore
    {
        final CommandStore commandStore;

        TestSafeCommandStore(int storeId)
        {
            this.commandStore = new TestCommandStore(storeId);
        }

        @Override public CommandStore commandStore() { return commandStore; }

        @Override protected SafeCommand getInternal(TxnId txnId) { return null; }
        @Override protected SafeCommand ifLoadedInternal(TxnId txnId) { return null; }
        @Override protected SafeCommandsForKey getInternal(RoutingKey key) { return null;}
        @Override protected SafeCommandsForKey ifLoadedInternal(RoutingKey key) { return null;}
        @Override public SafeTimestampsForKey timestampsForKey(RoutingKey key) { return null; }

        @Override public PreLoadContext canExecute(PreLoadContext context) { return null;}
        @Override public PreLoadContext context() { return null; }

        @Override
        public void upsertRedundantBefore(RedundantBefore addRedundantBefore)
        {
            unsafeUpsertRedundantBefore(addRedundantBefore);
        }

        @Override public <P1, T> T mapReduceActive(Unseekables<?> keys, @Nullable Timestamp withLowerTxnId, Txn.Kind.Kinds kinds, CommandFunction<P1, T, T> map, P1 p1, T initialValue) { return null; }
        @Override public <P1, T> T mapReduceFull(Unseekables<?> keys, TxnId testTxnId, Txn.Kind.Kinds testKind, TestStartedAt testStartedAt, TestDep testDep, TestStatus testStatus, CommandFunction<P1, T, T> map, P1 p1, T initialValue) { return null; }
        @Override public DataStore dataStore() { return null; }
        @Override public Agent agent() { return null; }
        @Override public ProgressLog progressLog() { return null; }
        @Override public NodeCommandStoreService node() { return null; }
        @Override public CommandStores.RangesForEpoch ranges() { return null; }
    }
}
