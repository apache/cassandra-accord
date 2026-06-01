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
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import accord.api.Agent;
import accord.api.ProgressLog.NoOpProgressLog;
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
import accord.local.RedundantStatus.SomeStatus;
import accord.local.UniqueTimeService.AtomicUniqueTime;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRangeRoute;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.SaveStatus;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.utils.DefaultRandom;
import accord.utils.ImmutableBitSet;
import accord.utils.LargeBitSet;
import accord.utils.RandomSource;
import accord.utils.async.AsyncChainUtils;
import org.assertj.core.api.Assertions;

import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.local.RedundantStatus.SomeStatus.LOG_INCOMPLETE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.LOG_UNAVAILABLE_ONLY;
import static accord.local.RedundantStatus.SomeStatus.UNREADY_ONLY;
import static accord.primitives.Status.Durability.NotDurable;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This file was authored by LLM
 * 
 * A {@link Command.WaitingOn} is filtered against {@link RedundantBefore} when it is <i>initialised</i> (at
 * Commit/Stable, see {@code Commands.initialiseWaitingOn}) and thereafter only when one of its dependencies notifies
 * us. A rebootstrap installs its UNREADY / LOG_INCOMPLETE / LOG_UNAVAILABLE bound only once its bound sync point is
 * durable - i.e. after that sync point has been stabilised locally with a {@code WaitingOn} computed under the old
 * bounds - so the bound arrives too late for the filter, and the dependencies it makes redundant must be cleared some
 * other way.
 * <p>
 * That matters because such a dependency may be a proposal whose agreement was abandoned (timed out, preempted,
 * invalidated): it will never apply anywhere, so no notification will ever arrive, and any local waiter stays
 * {@code PreApplied} for the life of the process - which is how a rebootstrap's bound sync point can fail to achieve
 * {@code MinorityQuorumAndWaitedForAll} indefinitely.
 * <p>
 * Advancing the bound is therefore required to re-filter the waiters it affects. The registrations we are about to
 * drop ({@code LocalListeners.clearBefore}) are precisely the index of who those waiters are - and, critically, that
 * work must not be abandoned by a {@link LogFaultException}: a fault forbids us from <i>concluding</i> anything from an
 * absent or undecided record, and must not additionally silence the only thing that will ever unblock a waiter on it.
 */
public class RedundantBoundWaiterTest
{
    private static final Id ID1 = id(1);
    private static final List<Id> IDS = Lists.newArrayList(ID1, id(2), id(3));
    private static final Range FULL_RANGE = IntKey.range(0, 100);
    private static final Ranges FULL_RANGES = Ranges.single(FULL_RANGE);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, FULL_RANGE);
    private static final FullRangeRoute ROUTE = FULL_RANGES.toRoute(IntKey.routing(50));

    /** the abandoned proposal: witnessed (so it is a dependency) but never decided */
    private static final TxnId DEP = syncPoint(1, 100, Txn.Kind.VisibilitySyncPoint);
    /** the rebootstrap's bound sync point, which took DEP as a dependency and is waiting for it locally */
    private static final TxnId WAITER = syncPoint(1, 200, Txn.Kind.ExclusiveSyncPoint);

    private static TxnId syncPoint(long epoch, long hlc, Txn.Kind kind)
    {
        return new TxnId(epoch, hlc, kind, Domain.Range, ID1);
    }

    @Test
    public void logIncompleteBoundUnblocksWaiterOnAbandonedDependency()
    {
        assertBoundUnblocksWaiter(LOG_INCOMPLETE_ONLY);
    }

    @Test
    public void logUnavailableBoundUnblocksWaiterOnAbandonedDependency()
    {
        assertBoundUnblocksWaiter(LOG_UNAVAILABLE_ONLY);
    }

    /**
     * A plain bootstrap (GAIN_OWNERSHIP/CATCHUP) throws no fault, but is equally stuck without a re-filter:
     * {@code Commands.listenerUpdate} ignores pre-commit statuses, so notifying the waiter about an undecided
     * dependency does nothing at all.
     */
    @Test
    public void unreadyBoundUnblocksWaiterOnAbandonedDependency()
    {
        assertBoundUnblocksWaiter(UNREADY_ONLY);
    }

    private void assertBoundUnblocksWaiter(SomeStatus status)
    {
        InMemoryCommandStore.Synchronized store = createStore();

        install(store, DEP, preAccepted(DEP));
        install(store, WAITER, stableWaitingOn(WAITER, DEP));

        // as Commands does for a dependency in LocalExecution.NotReady
        inStore(store, ExecutionContext.unsequenced(DEP, "Register"), safeStore -> {
            safeStore.registerListener(safeStore.unsafeTryGetNoCleanup(DEP), SaveStatus.Applied, WAITER);
        });
        assertTrue(isWaitingOnDep(store), "expected " + WAITER + " to be waiting on " + DEP);

        // rebootstrap: the bound is the sync point we are waiting on, so everything strictly before it - including the
        // abandoned dependency - is now unready, and (for the log faults) unusable
        inStore(store, ExecutionContext.unsequenced(WAITER, "Bound"), safeStore -> {
            safeStore.upsertRedundantBefore(RedundantBefore.create(FULL_RANGES, WAITER, status));
        });

        assertFalse(isWaitingOnDep(store),
                    "advancing the bound past " + DEP + " must clear it from " + WAITER + "'s WaitingOn; nothing else "
                    + "ever will, as it was never decided");
    }

    /**
     * Pins the reason {@code clearBefore} and the other internal bookkeeping loads must use
     * {@link SafeCommandStore#unsafeTryGetNoLogFault(TxnId)}: the FULL load of an undecided record below a log-fault
     * bound throws, so any bookkeeping that used it would be silently abandoned.
     */
    @Test
    public void bookkeepingLoadOfAFaultedRecordDoesNotThrow()
    {
        InMemoryCommandStore.Synchronized store = createStore();
        install(store, DEP, preAccepted(DEP));

        inStore(store, ExecutionContext.unsequenced(DEP, "Bound"), safeStore -> {
            safeStore.upsertRedundantBefore(RedundantBefore.create(FULL_RANGES, WAITER, LOG_INCOMPLETE_ONLY));
        });

        inStore(store, ExecutionContext.unsequenced(DEP, "Load"), safeStore -> {
            Assertions.assertThatThrownBy(() -> safeStore.unsafeTryGet(DEP))
                      .describedAs("a FULL load of an undecided record below a log-fault bound must refuse")
                      .isInstanceOf(LogFaultException.class);
            Assertions.assertThatCode(() -> safeStore.unsafeTryGetNoLogFault(DEP))
                      .describedAs("... but bookkeeping over the same record must not be abandoned")
                      .doesNotThrowAnyException();
        });
    }

    // -------- plumbing --------

    private static boolean isWaitingOnDep(InMemoryCommandStore.Synchronized store)
    {
        Command command = store.commandIfPresent(WAITER).value();
        if (command.hasBeen(accord.primitives.Status.Applied))
            return false;
        return command.asCommitted().waitingOn().isWaitingOn(DEP);
    }

    private static void install(InMemoryCommandStore.Synchronized store, TxnId txnId, Command command)
    {
        store.execute(() -> store.command(txnId).value(command));
    }

    private static void inStore(InMemoryCommandStore.Synchronized store, ExecutionContext context, Consumer<SafeCommandStore> consumer)
    {
        store.execute(() -> {
            SafeCommandStore safeStore = store.beginOperation(context, null);
            try { consumer.accept(safeStore); }
            finally { store.completeOperation(safeStore); }
        });
    }

    /** an undecided record: witnessed, so it is reported as a dependency, but its agreement was abandoned */
    private static Command preAccepted(TxnId txnId)
    {
        return new CommandBuilder(txnId)
               .durability(NotDurable)
               .participants(StoreParticipants.all(ROUTE, SaveStatus.PreAccepted))
               .promised(Ballot.ZERO)
               .acceptedOrCommitted(Ballot.ZERO)
               .partialTxn(writeTxn(FULL_RANGES).slice(FULL_RANGES, true))
               .executeAt(txnId)
               .build(SaveStatus.PreAccepted);
    }

    /** a stable sync point whose WaitingOn still holds {@code dep}, as a bound sync point's does */
    private static Command stableWaitingOn(TxnId txnId, TxnId dep)
    {
        return new CommandBuilder(txnId)
               .durability(NotDurable)
               .participants(StoreParticipants.all(ROUTE, SaveStatus.Stable))
               .promised(Ballot.ZERO)
               .acceptedOrCommitted(Ballot.ZERO)
               .partialTxn(writeTxn(FULL_RANGES).slice(FULL_RANGES, true))
               .partialDeps(Deps.builder(true).add(FULL_RANGE, dep).build().intersecting(ROUTE))
               .executeAt(txnId)
               .waitingOn(waitingOnCommand(dep))
               .build(SaveStatus.Stable);
    }

    private static Command.WaitingOn waitingOnCommand(TxnId dep)
    {
        RangeDeps.BuilderByRange builder = RangeDeps.builderByRange();
        builder.add(FULL_RANGE, dep);
        RangeDeps directRangeDeps = builder.build();
        LargeBitSet bits = new LargeBitSet(directRangeDeps.txnIdCount());
        bits.setRange(0, directRangeDeps.txnIdCount());
        return new Command.WaitingOn(accord.primitives.RoutingKeys.EMPTY, directRangeDeps,
                                     new ImmutableBitSet(bits), new ImmutableBitSet(0));
    }

    private static InMemoryCommandStore.Synchronized createStore()
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        Agent agent = new TestAgent(clock);
        RandomSource random = new DefaultRandom();
        MockStore data = new MockStore();
        Node node = new Node(ID1, null, new MockTopologyService(ignore -> null, TOPOLOGY),
                             clock, new AtomicUniqueTime(clock),
                             () -> data,
                             new ShardDistributor.EvenSplit(1, ignore -> new IntKey.Splitter()),
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
}
