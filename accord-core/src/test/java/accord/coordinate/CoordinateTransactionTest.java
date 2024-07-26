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

package accord.coordinate;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.BarrierType;
import accord.api.LocalListeners;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockStore;
import accord.local.Command;
import accord.local.Commands;
import accord.local.Commands.AcceptOutcome;
import accord.local.Commands.ApplyOutcome;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.primitives.SaveStatus;
import accord.local.StoreParticipants;
import accord.messages.ReadData.ReadOk;
import accord.primitives.Ballot;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;

import static accord.Utils.id;
import static accord.Utils.ids;
import static accord.Utils.ranges;
import static accord.Utils.spinUntilSuccess;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.key;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.primitives.Status.Applied;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Routable.Domain.Range;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Invariants.checkState;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static com.google.common.base.Predicates.alwaysTrue;
import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CoordinateTransactionTest
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinateTransactionTest.class);

    @AfterEach
    public void tearDown()
    {
        DefaultProgressLogs.unsafePauseForTesting(false);
    }

    @Test
    void simpleTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Key);
            Keys keys = keys(10);
            Txn txn = writeTxn(keys);
            FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
            Result result = getUninterruptibly(CoordinateTransaction.coordinate(node, route, txnId, txn));
            assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void simpleRangeTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Range);
            Ranges keys = ranges(range(1, 2));
            Txn txn = writeTxn(keys);
            FullRangeRoute route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));
            Result result = getUninterruptibly(CoordinateTransaction.coordinate(node, route, txnId, txn));
            assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void exclusiveSyncTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            assertNotNull(node);

            TxnId oldId1 = node.nextTxnId(Write, Key);
            TxnId oldId2 = node.nextTxnId(Write, Key);

            getUninterruptibly(CoordinateSyncPoint.exclusiveSyncPoint(node, ranges(range(0, 1))));
            try
            {
                Keys keys = keys(1);
                Txn txn = writeTxn(keys);
                FullKeyRoute route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));
                getUninterruptibly(CoordinateTransaction.coordinate(node, route, oldId1, txn));
                fail();
            }
            catch (ExecutionException e)
            {
                assertEquals(Invalidated.class, e.getCause().getClass());
            }

            Keys keys = keys(2);
            Txn txn = writeTxn(keys);
            FullKeyRoute route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));
            getUninterruptibly(CoordinateTransaction.coordinate(node, route, oldId2, txn));
        }
    }

    @Test
    void barrierTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            Agent agent = node.agent();
            assertNotNull(node);
            long epoch = node.epoch();

            // This is checking for a local barrier so it should succeed even if we drop the completion messages from the other nodes
            cluster.networkFilter.addFilter(id -> ImmutableSet.of(cluster.get(2).id(), cluster.get(3).id()).contains(id), alwaysTrue(), message -> message instanceof ReadOk);
            // Should create a sync transaction since no pre-existing one can be used and return as soon as it is locally applied
            Barrier localInitiatingBarrier = Barrier.barrier(node, Keys.of(key(3)), toRoute(Keys.of(key(3))), node.epoch(), BarrierType.local);
            // Sync transaction won't be created until callbacks for existing transaction check runs
            Semaphore existingTransactionCheckCompleted = new Semaphore(0);
            localInitiatingBarrier.existingTransactionCheck.addCallback((ignored1, ignored2) -> existingTransactionCheckCompleted.release());
            assertTrue(existingTransactionCheckCompleted.tryAcquire(5, TimeUnit.SECONDS));
            // It's possible for the callback to run before the sync point is created in a different callback
            // because multiple callbacks can run concurrently. `addCallback` might see the finished result
            // and run immediately in this thread
            spinUntilSuccess(() -> checkState(localInitiatingBarrier.coordinateSyncPoint != null));
            // Should be able to find the txnid now and wait for local application
            TxnId initiatingBarrierSyncTxnId = AsyncChains.getBlocking(localInitiatingBarrier.coordinateSyncPoint).syncId;
            Semaphore barrierAppliedLocally = new Semaphore(0);
            node.ifLocal(PreLoadContext.contextFor(initiatingBarrierSyncTxnId), key(3).toUnseekable(), epoch, (safeStore) ->
                safeStore.registerAndInvoke(initiatingBarrierSyncTxnId, key(3).toUnseekable(),
                    commandListener((safeStore2, command) -> {
                        if (command.current().is(Applied))
                            barrierAppliedLocally.release();
                    }))
            ).begin(agent);
            assertTrue(barrierAppliedLocally.tryAcquire(5, TimeUnit.SECONDS));
            // If the command is locally applied the future for the barrier should be completed as well and not waiting on messages from other nodes
            getUninterruptibly(localInitiatingBarrier, 1, TimeUnit.SECONDS);
            cluster.networkFilter.clear();

            Keys globalSyncBarrierKeys = keys(2, 3);
            // At least one other should have completed by the time it is locally applied, a down node should be fine since it is quorum
            cluster.networkFilter.isolate(cluster.get(2).id());
            Barrier globalInitiatingBarrier = Barrier.barrier(node, globalSyncBarrierKeys, toRoute(globalSyncBarrierKeys), node.epoch(), BarrierType.global_sync);
            Timestamp globalBarrierTimestamp = getUninterruptibly(globalInitiatingBarrier);

            assertNotNull(globalInitiatingBarrier.coordinateSyncPoint);
            cluster.networkFilter.clear();

            // The existing barrier should suffice here
            Barrier nonInitiatingLocalBarrier = Barrier.barrier(node, Keys.of(key(2)), toRoute(Keys.of(key(2))), node.epoch(), BarrierType.local);
            Timestamp previousBarrierTimestamp = getUninterruptibly(nonInitiatingLocalBarrier);
            assertNull(nonInitiatingLocalBarrier.coordinateSyncPoint);
            assertEquals(previousBarrierTimestamp, getUninterruptibly(nonInitiatingLocalBarrier));
            assertEquals(previousBarrierTimestamp, globalBarrierTimestamp);

            // Sync over nothing should work
            SyncPoint<Range> syncPoint = getUninterruptibly(CoordinateSyncPoint.inclusiveAndAwaitQuorum(node, ranges(range(99, 100))));
            assertEquals(node.epoch(), syncPoint.syncId.epoch());

            // Keys and so on for the upcoming transaction pair
            Keys keys = keys(1);
            RoutingKey key = keys.get(0).toUnseekable();
            Ranges ranges = ranges(key.asRange());
            FullKeyRoute route = keys.toRoute(key);
            TxnId txnId = node.nextTxnId(Write, Key);

            // Create a txn to block the one we are about to create after this
            DefaultProgressLogs.unsafePauseForTesting(true);
            TxnId blockingTxnId = new TxnId(txnId.epoch(), 1, Read, Key, new Id(1));
            Txn blockingTxn = new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY, null);
            PreLoadContext blockingTxnContext = PreLoadContext.contextFor(blockingTxnId, route);
            for (Node n : cluster)
                assertEquals(AcceptOutcome.Success, getUninterruptibly(n.unsafeForKey(key).submit(blockingTxnContext, safeStore -> {
                    StoreParticipants participants = StoreParticipants.update(safeStore, route, blockingTxnId.epoch(), blockingTxnId, blockingTxnId.epoch());
                    return Commands.preaccept(safeStore, safeStore.get(blockingTxnId, participants), participants, blockingTxnId, blockingTxnId.epoch(), blockingTxn.slice(safeStore.ranges().allAt(blockingTxnId), true), route);
                })));

            // Now create the transaction that should be blocked by the previous one
            Txn txn = new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY, null);
            PreLoadContext context = PreLoadContext.contextFor(txnId, route);
            for (Node n : cluster)
                assertEquals(AcceptOutcome.Success, getUninterruptibly(n.unsafeForKey(key).submit(context, safeStore -> {
                    StoreParticipants participants = StoreParticipants.update(safeStore, route, txnId.epoch(), txnId, txnId.epoch());
                    return Commands.preaccept(safeStore, safeStore.get(txnId, participants), participants, txnId, txnId.epoch(), txn.slice(safeStore.ranges().allAt(txnId.epoch()), true), route);
                })));


            AsyncResult<SyncPoint<Range>> syncInclusiveSyncFuture = CoordinateSyncPoint.inclusiveAndAwaitQuorum(node, ranges);
            // Shouldn't complete because it is blocked waiting for the dependency just created to apply
            sleep(500);
            assertFalse(syncInclusiveSyncFuture.isDone());

            // Async sync should return a result immediately since we are going to wait on the sync point transaction that was created by the sync point
            AsyncResult<SyncPoint<Range>> asyncInclusiveSyncFuture = CoordinateSyncPoint.inclusive(node, ranges);
            SyncPoint<Range> localSyncPoint = getUninterruptibly(asyncInclusiveSyncFuture);
            Semaphore localSyncOccurred = new Semaphore(0);
            node.commandStores().ifLocal(PreLoadContext.contextFor(localSyncPoint.syncId), key, epoch, epoch, safeStore ->
                safeStore.registerAndInvoke(localSyncPoint.syncId, key.toUnseekable(),
                    commandListener((safeStore2, command) -> {
                        if (command.current().hasBeen(Applied))
                            localSyncOccurred.release();
                    })
                )
            ).begin(agent);

            // Move to preapplied in order to test that Barrier will find the transaction and add a listener
            for (Node n : cluster)
                getUninterruptibly(n.unsafeForKey(key).execute(context, safeStore ->  {
                    StoreParticipants participants = StoreParticipants.update(safeStore, route, txnId.epoch(), txnId, txnId.epoch());
                    SafeCommand safeCommand = safeStore.get(txnId, participants);
                    Command command = safeCommand.current();
                    PartialDeps.Builder depsBuilder = PartialDeps.builder(safeStore.ranges().currentRanges());
                    depsBuilder.add(key, blockingTxnId);
                    PartialDeps partialDeps = depsBuilder.build();
                    Commands.commit(safeStore, safeCommand, participants, SaveStatus.Stable, Ballot.ZERO, txnId, route, command.partialTxn(), txnId, partialDeps);
                    Commands.apply(safeStore, safeCommand, participants, txnId, route, txnId, partialDeps, command.partialTxn(), txn.execute(txnId, txnId, null), txn.query().compute(txnId, txnId, keys, null, null, null));
                }));

            Barrier listeningLocalBarrier = Barrier.barrier(node, keys, toRoute(keys), node.epoch(), BarrierType.local);
            // Wait and make sure the existing transaction check worked and there is no coordinate sync point created
            Thread.sleep(500);
            assertNull(listeningLocalBarrier.coordinateSyncPoint);
            assertNotNull(listeningLocalBarrier.existingTransactionCheck);
            assertEquals(txnId, getUninterruptibly(listeningLocalBarrier.existingTransactionCheck).executeAt);
            assertFalse(listeningLocalBarrier.isDone());

            // Apply the blockingTxn to unblock the rest
            for (Node n : cluster)
                assertEquals(ApplyOutcome.Success, getUninterruptibly(n.unsafeForKey(key).submit(blockingTxnContext, safeStore -> {
                    StoreParticipants participants = StoreParticipants.update(safeStore, route, blockingTxnId.epoch(), blockingTxnId, blockingTxnId.epoch());
                    return Commands.apply(safeStore, safeStore.get(blockingTxnId, participants), participants, blockingTxnId, route, blockingTxnId, PartialDeps.builder(route).build(), blockingTxn.slice(safeStore.ranges().allAt(blockingTxnId.epoch()), true), blockingTxn.execute(blockingTxnId, blockingTxnId, null), blockingTxn.query().compute(blockingTxnId, blockingTxnId, keys, null, null, null));
                })));
            // Global sync should be unblocked
            syncPoint = getUninterruptibly(syncInclusiveSyncFuture);
            assertEquals(node.epoch(), syncPoint.syncId.epoch());
            // Command listener for local sync transaction should get notified
            assertTrue(localSyncOccurred.tryAcquire(5, TimeUnit.SECONDS));
            // Listening local barrier should have succeeded in waiting on the local transaction that just applied
            assertEquals(getUninterruptibly(listeningLocalBarrier), getUninterruptibly(listeningLocalBarrier.existingTransactionCheck).executeAt);
            assertEquals(txnId, getUninterruptibly(listeningLocalBarrier));
        }
        finally
        {
            DefaultProgressLogs.unsafePauseForTesting(false);
        }
    }

    @Test
    void slowPathTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(7).replication(7).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            assertNotNull(node);

            Txn txn = writeTxn(keys(10));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            assertEquals(MockStore.RESULT, result);
        }
    }

    private TxnId coordinate(Node node, long clock, Keys keys) throws Throwable
    {
        TxnId txnId = node.nextTxnId(Write, Key);
        txnId = new TxnId(txnId.epoch(), txnId.hlc() + clock, Write, Key, txnId.node);
        Txn txn = writeTxn(keys);
        Result result = getUninterruptibly(CoordinateTransaction.coordinate(node, node.computeRoute(txnId, txn.keys()), txnId, txn));
        assertEquals(MockStore.RESULT, result);
        return txnId;
    }

    @Test
    void multiKeyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(6).maxKey(600).build())
        {
            Node node = cluster.get(1);
            assertNotNull(node);

            TxnId txnId1 = coordinate(node, 100, keys(50, 350, 550));
            TxnId txnId2 = coordinate(node, 150, keys(250, 350, 450));
            TxnId txnId3 = coordinate(node, 125, keys(50, 60, 70, 80, 350, 550));
        }
    }

    @Test
    void writeOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            assertNotNull(node);

            Keys keys = keys(10);
            Txn txn = new Txn.InMemory(keys, MockStore.read(Keys.EMPTY), MockStore.QUERY, MockStore.update(keys));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void readOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(3).replication(3).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            assertNotNull(node);

            Keys keys = keys(10);
            Txn txn = new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY, MockStore.update(Keys.EMPTY));
            Result result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void simpleTxnThenReadOnlyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Key);
            Keys oneKey = keys(10);
            Keys twoKeys = keys(10, 20);
            Txn txn = new Txn.InMemory(oneKey, MockStore.read(oneKey), MockStore.QUERY, MockStore.update(twoKeys));
            Result result = getUninterruptibly(CoordinateTransaction.coordinate(node, txn.keys().toRoute(oneKey.get(0).toUnseekable()), txnId, txn));
            assertEquals(MockStore.RESULT, result);

            txn = new Txn.InMemory(oneKey, MockStore.read(oneKey), MockStore.QUERY, MockStore.update(Keys.EMPTY));
            result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            assertEquals(MockStore.RESULT, result);
        }
    }

    private static LocalListeners.ComplexListener commandListener(BiConsumer<SafeCommandStore, SafeCommand> listener)
    {
        return (safeStore, command) -> {
            listener.accept(safeStore, command);
            return true;
        };
    }

    private static FullRoute<RoutingKey> toRoute(Keys keys)
    {
        return keys.toRoute(keys.get(0).toUnseekable());
    }
}
