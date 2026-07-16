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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.Agent;
import accord.api.ProgressLog.NoOpProgressLog;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.coordinate.CoordinationAdapter;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.impl.DefaultTimeouts;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStores;
import accord.impl.IntKey;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.impl.TopologyFactory;
import accord.impl.basic.InMemoryJournal;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockStore;
import accord.impl.mock.MockTopologyService;
import accord.local.Node.Id;
import accord.local.UniqueTimeService.AtomicUniqueTime;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullKeyRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.DefaultRandom;
import accord.utils.ImmutableBitSet;
import accord.utils.LargeBitSet;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChainUtils;

import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.primitives.Status.Durability.NotDurable;

/**
 * Locks down the {@link Commands.MaybeExecuteAdapter} contract:
 *
 * <em>Every invocation of
 * {@link Commands#maybeExecute(SafeCommandStore, SafeCommand, Command, boolean, boolean, Commands.MaybeExecuteAdapter)}
 * must invoke exactly one of {@code adapter.notifyWaiting} or {@code adapter.notWaiting} before
 * returning.</em>
 *
 * <p>Some adapters (notably {@code NotifyWaitingOnPlus.adapter(continuation,...)} used by
 * {@link CommandStore#tryToExecuteListeningTxns(boolean)}) chain a continuation off of
 * {@code notWaiting}; a return path that fires neither callback silently abandons the
 * continuation and can deadlock the {@code AsyncResult} returned by
 * {@code tryToExecuteListeningTxns}.  That bug used to manifest on startup of nodes with a large
 * backlog of registered listeners whose target txns were {@code Stable}/{@code PreApplied} with
 * only outstanding *key* waits (no command waits), because
 * {@code waitingOn.isWaitingOnCommand()} was the only condition under which the adapter was
 * contacted on that return path.
 *
 * <p>This test intentionally bypasses the
 * {@link SafeCommand#update(SafeCommandStore, Command, boolean)} machinery (and the
 * {@code CommandsForKey}, progress log, etc. side effects it triggers) by installing the
 * synthetic command directly into the underlying {@link InMemoryCommandStore.GlobalCommand}.
 * That keeps the test scoped to the branching inside {@code Commands.maybeExecute}.
 */
public class MaybeExecuteAdapterTest
{
    // Topology / route / key plumbing.
    private static final Id ID1 = id(1);
    private static final Id ID2 = id(2);
    private static final Id ID3 = id(3);
    private static final List<Id> IDS = Lists.newArrayList(ID1, ID2, ID3);
    private static final Range FULL_RANGE = IntKey.range(0, 100);
    private static final Ranges FULL_RANGES = Ranges.single(FULL_RANGE);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, FULL_RANGE);
    private static final IntKey.Raw KEY = IntKey.key(10);
    private static final RoutingKey HOME_KEY = KEY.toUnseekable();
    private static final FullKeyRoute ROUTE = RoutingKeys.of(HOME_KEY).toRoute(HOME_KEY);
    private static final Keys KEYS = Keys.of(KEY);

    /** Counts adapter callbacks, ignoring their inputs.  We are only verifying the contract. */
    private static class Recorder implements Commands.MaybeExecuteAdapter
    {
        int notifyWaiting;
        int notWaiting;

        @Override public void notifyWaiting(SafeCommandStore safeStore, SafeCommand safeCommand) { ++notifyWaiting; }
        @Override public void notWaiting(SafeCommandStore safeStore)                              { ++notWaiting;    }
    }


    private static class CommandStoreSupport
    {
        final AtomicReference<Topology> local = new AtomicReference<>(TOPOLOGY);
        final MockStore data = new MockStore();
    }

    private static InMemoryCommandStore.Synchronized createStore(CommandStoreSupport support)
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        Agent agent = new TestAgent(clock);
        RandomSource random = new DefaultRandom();
        Node node = new Node(ID1, null, new MockTopologyService(ignore -> null, support.local.get()),
                             clock, new AtomicUniqueTime(clock),
                             () -> support.data,
                             new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter()),
                             agent, random.fork(), Scheduler.NEVER_RUN_SCHEDULED,
                             SizeOfIntersectionSorter.SUPPLIER, DefaultRemoteListeners::new,
                             time -> new DefaultTimeouts(time, Runnable::run),
                             ignore -> ignore2 -> new NoOpProgressLog(),
                             DefaultLocalListeners.Factory::new,
                             InMemoryCommandStores.Synchronized::new,
                             new CoordinationAdapter.DefaultFactory(),
                             DurableBefore.NOOP_PERSISTER,
                             new InMemoryJournal(ID1, random.fork()));
        AsyncChainUtils.awaitUninterruptibly(node.unsafeStart().chain());
        return (InMemoryCommandStore.Synchronized) node.unsafeByIndex(0);
    }

    /**
     * Build a {@code Stable} {@link Command} with the supplied {@link Command.WaitingOn}.  The
     * {@code waitingOn} can be any shape (key-only, command-only, mixed, empty) regardless of
     * whether it is semantically consistent with the rest of the command: {@link Command#validate}
     * only requires {@code waitingOn != null} for stable/committed statuses, and we are testing
     * pure {@code maybeExecute} branching here.
     */
    private static Command buildStable(TxnId txnId, Command.WaitingOn waitingOn)
    {
        Txn txn = writeTxn(KEYS);
        return new CommandBuilder(txnId)
                       .durability(NotDurable)
                       .participants(StoreParticipants.all(ROUTE))
                       .promised(Ballot.ZERO)
                       .acceptedOrCommitted(Ballot.ZERO)
                       .partialTxn(txn.slice(FULL_RANGES, true))
                       .partialDeps(Deps.NONE.intersecting(ROUTE))
                       .executeAt(txnId)
                       .waitingOn(waitingOn)
                       .build(SaveStatus.Stable);
    }

    private static Command buildPreAccepted(TxnId txnId)
    {
        Txn txn = writeTxn(KEYS);
        return new CommandBuilder(txnId)
                       .durability(NotDurable)
                       .participants(StoreParticipants.all(ROUTE))
                       .promised(Ballot.ZERO)
                       .acceptedOrCommitted(Ballot.ZERO)
                       .partialTxn(txn.slice(FULL_RANGES, true))
                       .executeAt(txnId)
                       .build(SaveStatus.PreAccepted);
    }

    private static Command.WaitingOn waitingOnKeysOnly(int numKeyBits)
    {
        // 0 txn waits + N key waits => bitset of size numKeyBits; all bits live in the key region
        // (because txnIdCount() = directRangeDeps.txnIdCount() = 0).  This is the data shape that
        // tripped the old maybeExecute key-only branch.
        RoutingKey[] keys = new RoutingKey[numKeyBits];
        for (int i = 0; i < numKeyBits; ++i)
            keys[i] = IntKey.routing(20 + i);
        RoutingKeys waitKeys = RoutingKeys.of(keys);
        LargeBitSet bits = new LargeBitSet(waitKeys.size());
        bits.setRange(0, waitKeys.size());
        return new Command.WaitingOn(waitKeys, RangeDeps.NONE,
                                     new ImmutableBitSet(bits),
                                     new ImmutableBitSet(0));
    }

    private static Command.WaitingOn waitingOnCommandsOnly(TxnId... depTxnIds)
    {
        Range[] ranges = new Range[]{ IntKey.range(0, 5) };
        // SerializerSupport's create() takes a flat int[] of [range0_end, range1_end, ..., dep0_idx, dep1_idx, ...]
        // (per the existing AccordCommandStoreTryExecuteListeningTest helper).
        int[] rangesToTxnIds = new int[ranges.length + depTxnIds.length];
        rangesToTxnIds[0] = rangesToTxnIds.length; // a single range covers all dep ids
        for (int i = 0; i < depTxnIds.length; ++i)
            rangesToTxnIds[ranges.length + i] = i;
        RangeDeps directRangeDeps = RangeDeps.SerializerSupport.create(ranges, depTxnIds, rangesToTxnIds, null);

        // bitset size = directRangeDeps.txnIdCount() + 0 keys = depTxnIds.length; set all command bits
        LargeBitSet bits = new LargeBitSet(depTxnIds.length);
        bits.setRange(0, depTxnIds.length);
        return new Command.WaitingOn(RoutingKeys.EMPTY, directRangeDeps,
                                     new ImmutableBitSet(bits),
                                     new ImmutableBitSet(0));
    }

    private static Command.WaitingOn waitingOnMixed(TxnId depTxnId, RoutingKey... keys)
    {
        Range[] ranges = new Range[]{ IntKey.range(0, 5) };
        TxnId[] depTxnIds = new TxnId[]{ depTxnId };
        int[] rangesToTxnIds = new int[]{ 2, 0 };
        RangeDeps directRangeDeps = RangeDeps.SerializerSupport.create(ranges, depTxnIds, rangesToTxnIds, null);

        RoutingKeys waitKeys = RoutingKeys.of(keys);
        int size = directRangeDeps.txnIdCount() + waitKeys.size();
        LargeBitSet bits = new LargeBitSet(size);
        bits.setRange(0, size);
        return new Command.WaitingOn(waitKeys, directRangeDeps,
                                     new ImmutableBitSet(bits),
                                     new ImmutableBitSet(0));
    }

    private static TxnId nextTxnId(MockCluster.Clock clock, int n)
    {
        return clock.idForNode(1, n);
    }

    /** Directly inject {@code command} into the store as the current state of {@code txnId}. */
    private static void installCommand(InMemoryCommandStore.Synchronized commands, TxnId txnId, Command command)
    {
        commands.command(txnId).value(command);
    }

    /**
     * Open a SafeCommandStore against {@code txnId} and invoke
     * {@link Commands#maybeExecute(SafeCommandStore, SafeCommand, Command, boolean, boolean, Commands.MaybeExecuteAdapter)}
     * with the supplied command and recorder.
     */
    private static void runMaybeExecute(InMemoryCommandStore.Synchronized commands,
                                        TxnId txnId,
                                        Command command,
                                        boolean alwaysNotifyListeners,
                                        boolean notifyWaitingOn,
                                        Recorder rec)
    {
        commands.execute(() -> {
            SafeCommandStore safeStore = commands.beginOperation(PreLoadContext.contextFor(txnId, "Test"), null);
            try
            {
                SafeCommand safeCommand = safeStore.unsafeGet(txnId);
                Commands.maybeExecute(safeStore, safeCommand, command, alwaysNotifyListeners, notifyWaitingOn, rec);
            }
            finally
            {
                commands.completeOperation(safeStore);
            }
        });
    }

    // -------- The tests --------

    /**
     * Sanity: confirm the helpers produce {@code WaitingOn} instances with the bit-shape we
     * intend to drive {@code maybeExecute} into.  If this ever stops holding (e.g. because of an
     * encoding change to {@code WaitingOn}), the dependent tests below would silently fail to
     * exercise the buggy branch.
     */
    @Test
    void sanityHelpersProduceExpectedShapes()
    {
        Command.WaitingOn keyOnly = waitingOnKeysOnly(2);
        Assertions.assertTrue(keyOnly.isWaiting(),         "keyOnly should be waiting");
        Assertions.assertFalse(keyOnly.isWaitingOnCommand(),"keyOnly should NOT be waiting on a command");
        Assertions.assertTrue(keyOnly.isWaitingOnKey(),    "keyOnly should be waiting on a key");

        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId dep = nextTxnId(clock, 2);
        Command.WaitingOn cmdOnly = waitingOnCommandsOnly(dep);
        Assertions.assertTrue(cmdOnly.isWaiting());
        Assertions.assertTrue(cmdOnly.isWaitingOnCommand());
        Assertions.assertFalse(cmdOnly.isWaitingOnKey());

        Command.WaitingOn mixed = waitingOnMixed(dep, IntKey.routing(20));
        Assertions.assertTrue(mixed.isWaiting());
        Assertions.assertTrue(mixed.isWaitingOnCommand());
        Assertions.assertTrue(mixed.isWaitingOnKey());
    }

    /**
     * Regression test for the deadlock: a {@code Stable} target with only key waits used to fire
     * neither adapter callback, silently dropping any continuation chained off of {@code notWaiting}.
     */
    @Test
    void notWaitingFiresForStableWithOnlyKeyWaits()
    {
        InMemoryCommandStore.Synchronized commands = createStore(new CommandStoreSupport());
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = nextTxnId(clock, 1);

        Command stable = buildStable(txnId, waitingOnKeysOnly(1));
        installCommand(commands, txnId, stable);

        Recorder rec = new Recorder();
        runMaybeExecute(commands, txnId, stable, false, true, rec);

        Assertions.assertEquals(0, rec.notifyWaiting,
                                "notifyWaiting must NOT be called when waitingOn has no command bits");
        Assertions.assertEquals(1, rec.notWaiting,
                                "notWaiting must be called exactly once when waitingOn has no command bits");
    }

    /** {@code Stable} with only command waits should report via {@code notifyWaiting}. */
    @Test
    void notifyWaitingFiresForStableWithOnlyCommandWaits()
    {
        InMemoryCommandStore.Synchronized commands = createStore(new CommandStoreSupport());
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = nextTxnId(clock, 1);
        TxnId depTxnId = nextTxnId(clock, 2);

        Command stable = buildStable(txnId, waitingOnCommandsOnly(depTxnId));
        installCommand(commands, txnId, stable);

        Recorder rec = new Recorder();
        runMaybeExecute(commands, txnId, stable, false, true, rec);

        Assertions.assertEquals(1, rec.notifyWaiting,
                                "notifyWaiting must be called exactly once when waitingOn still has command bits");
        Assertions.assertEquals(0, rec.notWaiting,
                                "notWaiting must NOT be called when we are still waiting on a command");
    }

    /** {@code Stable} with both command and key waits is still "waiting on a command" overall. */
    @Test
    void notifyWaitingFiresForStableWithMixedWaits()
    {
        InMemoryCommandStore.Synchronized commands = createStore(new CommandStoreSupport());
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = nextTxnId(clock, 1);
        TxnId depTxnId = nextTxnId(clock, 2);

        Command stable = buildStable(txnId, waitingOnMixed(depTxnId, IntKey.routing(20)));
        installCommand(commands, txnId, stable);

        Recorder rec = new Recorder();
        runMaybeExecute(commands, txnId, stable, false, true, rec);

        Assertions.assertEquals(1, rec.notifyWaiting);
        Assertions.assertEquals(0, rec.notWaiting);
    }

    /**
     * When the caller asks for "do not propagate to dependencies" ({@code notifyWaitingOn = false}),
     * we still owe the adapter a {@code notWaiting} signal on the waiting path; otherwise any
     * adapter that chains a continuation off of {@code notWaiting} would silently lose it.
     */
    @Test
    void notWaitingFiresWhenNotifyWaitingOnIsFalse()
    {
        InMemoryCommandStore.Synchronized commands = createStore(new CommandStoreSupport());
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = nextTxnId(clock, 1);
        TxnId depTxnId = nextTxnId(clock, 2);

        // notifyWaitingOn = false suppresses notifyWaiting; the symmetric notWaiting still must fire.
        Command stable = buildStable(txnId, waitingOnCommandsOnly(depTxnId));
        installCommand(commands, txnId, stable);

        Recorder rec = new Recorder();
        runMaybeExecute(commands, txnId, stable, false, false, rec);

        Assertions.assertEquals(0, rec.notifyWaiting);
        Assertions.assertEquals(1, rec.notWaiting);
    }

    /**
     * A non-{@code Stable}/{@code PreApplied} target hits the early-return branch; that branch
     * must also fire {@code notWaiting} (existing behaviour; protect against regression).
     */
    @Test
    void notWaitingFiresForNonExecutingSaveStatus()
    {
        InMemoryCommandStore.Synchronized commands = createStore(new CommandStoreSupport());
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = nextTxnId(clock, 1);

        Command preaccepted = buildPreAccepted(txnId);
        installCommand(commands, txnId, preaccepted);

        Recorder rec = new Recorder();
        runMaybeExecute(commands, txnId, preaccepted, false, true, rec);

        Assertions.assertEquals(0, rec.notifyWaiting);
        Assertions.assertEquals(1, rec.notWaiting,
                                "notWaiting must fire from the non-Stable/PreApplied early-return branch");
    }
}