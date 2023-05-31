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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.BarrierType;
import accord.api.Key;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.impl.SimpleProgressLog;
import accord.impl.TestAgent;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockStore;
import accord.impl.mock.MockStore.MockData;
import accord.impl.mock.MockStore.MockFollowupRead;
import accord.impl.mock.MockStore.MockRepairWrites;
import accord.impl.mock.MockStore.MockResolver;
import accord.impl.mock.MockStore.MockUpdate;
import accord.impl.mock.MockStore.MockWrite;
import accord.local.Command;
import accord.local.CommandListener;
import accord.local.Commands;
import accord.local.Commands.AcceptOutcome;
import accord.local.Commands.ApplyOutcome;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.PreLoadContext;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.messages.Apply.ApplyReply;
import accord.messages.WhenReadyToExecute.ExecuteOk;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.Keys;
import accord.primitives.PartialDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Seekables;
import accord.primitives.SyncPoint;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.assertj.core.util.Throwables;

import static accord.Utils.id;
import static accord.Utils.idSet;
import static accord.Utils.ids;
import static accord.Utils.ranges;
import static accord.Utils.spinUntilSuccess;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.key;
import static accord.impl.IntKey.keys;
import static accord.impl.IntKey.range;
import static accord.local.PreLoadContext.EMPTY_PRELOADCONTEXT;
import static accord.local.Status.Applied;
import static accord.primitives.Txn.Kind.Read;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.Invariants.checkState;
import static accord.utils.async.AsyncChains.getUninterruptibly;
import static com.google.common.base.Predicates.alwaysFalse;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.primitives.Ints.checkedCast;
import static java.lang.Thread.sleep;
import static java.util.Collections.synchronizedList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class CoordinateTest
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinateTest.class);

    @AfterEach
    public void tearDown()
    {
        SimpleProgressLog.PAUSE_FOR_TEST = false;
    }

    @Test
    void simpleTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Domain.Key);
            Keys keys = keys(10);
            Txn txn = writeTxn(keys);
            FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, route));
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

            TxnId txnId = node.nextTxnId(Write, Domain.Key);
            Ranges keys = ranges(range(1, 2));
            Txn txn = writeTxn(keys);
            FullRangeRoute route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, route));
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

            TxnId oldId1 = node.nextTxnId(Write, Domain.Key);
            TxnId oldId2 = node.nextTxnId(Write, Domain.Key);

            getUninterruptibly(CoordinateSyncPoint.exclusive(node, ranges(range(1, 2))));
            try
            {
                Keys keys = keys(1);
                Txn txn = writeTxn(keys);
                FullKeyRoute route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));
                getUninterruptibly(Coordinate.coordinate(node, oldId1, txn, route));
                fail();
            }
            catch (ExecutionException e)
            {
                assertEquals(Invalidated.class, e.getCause().getClass());
            }

            Keys keys = keys(2);
            Txn txn = writeTxn(keys);
            FullKeyRoute route = keys.toRoute(keys.get(0).someIntersectingRoutingKey(null));
            getUninterruptibly(Coordinate.coordinate(node, oldId2, txn, route));
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
            cluster.networkFilter.addFilter(id -> ImmutableSet.of(cluster.get(2).id(), cluster.get(3).id()).contains(id), alwaysTrue(), message -> message instanceof ExecuteOk);
            // Should create a sync transaction since no pre-existing one can be used and return as soon as it is locally applied
            Barrier localInitiatingBarrier = Barrier.barrier(node, Seekables.of(key(3)), node.epoch(), BarrierType.local);
            // Sync transaction won't be created until callbacks for existing transaction check runs
            Semaphore existingTransactionCheckCompleted = new Semaphore(0);
            localInitiatingBarrier.existingTransactionCheck.addCallback((ignored1, ignored2) -> existingTransactionCheckCompleted.release());
            assertTrue(existingTransactionCheckCompleted.tryAcquire(5, TimeUnit.SECONDS));
            // It's possible for the callback to run before the sync point is created in a different callback
            // because multiple callbacks can run concurrently. `addCallback` might see the finished result
            // and run immediately in this thread
            spinUntilSuccess(() -> checkState(localInitiatingBarrier.coordinateSyncPoint != null));
            // Should be able to find the txnid now and wait for local application
            TxnId initiatingBarrierSyncTxnId = localInitiatingBarrier.coordinateSyncPoint.txnId;
            Semaphore barrierAppliedLocally = new Semaphore(0);
            node.ifLocal(PreLoadContext.contextFor(initiatingBarrierSyncTxnId), key(3).toUnseekable(), epoch, (safeStore) ->
                safeStore.command(initiatingBarrierSyncTxnId).addAndInvokeListener(
                    safeStore,
                    commandListener((safeStore2, command) -> {
                        if (command.current().is(Applied))
                            barrierAppliedLocally.release();
                    }))
            ).begin(agent);
            assertTrue(barrierAppliedLocally.tryAcquire(5, TimeUnit.SECONDS));
            // If the command is locally applied the future for the barrier should be completed as well and not waiting on messages from other nodes
            assertTrue(localInitiatingBarrier.isDone());
            cluster.networkFilter.clear();

            Seekables globalSyncBarrierKeys = keys(2, 3);
            // At least one other should have completed by the time it is locally applied, a down node should be fine since it is quorum
            cluster.networkFilter.isolate(cluster.get(2).id());
            Barrier globalInitiatingBarrier = Barrier.barrier(node, globalSyncBarrierKeys, node.epoch(), BarrierType.global_sync);
            Timestamp globalBarrierTimestamp = getUninterruptibly(globalInitiatingBarrier);
            int localBarrierCount = ((TestAgent)agent).completedLocalBarriers.getOrDefault(globalBarrierTimestamp, new AtomicInteger(0)).get();
            assertNotNull(globalInitiatingBarrier.coordinateSyncPoint);
            assertEquals(2, localBarrierCount);
            logger.info("Local barrier count " + localBarrierCount);
            cluster.networkFilter.clear();

            // The existing barrier should suffice here
            Barrier nonInitiatingLocalBarrier = Barrier.barrier(node, Seekables.of(key(2)), node.epoch(), BarrierType.local);
            Timestamp previousBarrierTimestamp = getUninterruptibly(nonInitiatingLocalBarrier);
            assertNull(nonInitiatingLocalBarrier.coordinateSyncPoint);
            assertEquals(previousBarrierTimestamp, getUninterruptibly(nonInitiatingLocalBarrier));
            assertEquals(previousBarrierTimestamp, globalBarrierTimestamp);

            // Sync over nothing should work
            SyncPoint syncPoint = null;
            syncPoint = getUninterruptibly(CoordinateSyncPoint.inclusive(node, ranges(range(99, 100)), false));
            assertEquals(node.epoch(), syncPoint.txnId.epoch());

            // Keys and so on for the upcoming transaction pair
            Keys keys = keys(1);
            accord.api.Key key = keys.get(0);
            RoutingKey homeKey = key.toUnseekable();
            Ranges ranges = ranges(homeKey.asRange());
            FullKeyRoute route = keys.toRoute(homeKey);
            TxnId txnId = node.nextTxnId(Write, Domain.Key);

            // Create a txn to block the one we are about to create after this
            SimpleProgressLog.PAUSE_FOR_TEST = true;
            TxnId blockingTxnId = new TxnId(txnId.epoch(), 1, Read, Domain.Key, new Id(1));
            Txn blockingTxn = agent.emptyTxn(Read, keys);
            PreLoadContext blockingTxnContext = PreLoadContext.contextFor(blockingTxnId, keys);
            for (Node n : cluster)
                assertEquals(AcceptOutcome.Success, getUninterruptibly(n.unsafeForKey(key).submit(blockingTxnContext, store ->
                        Commands.preaccept(store, blockingTxnId, blockingTxn.slice(store.ranges().at(txnId.epoch()), true), route, homeKey))));

            // Now create the transaction that should be blocked by the previous one
            Txn txn = agent.emptyTxn(Write, keys);
            PreLoadContext context = PreLoadContext.contextFor(txnId, keys);
            for (Node n : cluster)
                assertEquals(AcceptOutcome.Success, getUninterruptibly(n.unsafeForKey(key).submit(context, store ->
                    Commands.preaccept(store, txnId, txn.slice(store.ranges().at(txnId.epoch()), true), route, homeKey))));


            CoordinateSyncPoint syncInclusiveSyncFuture = CoordinateSyncPoint.inclusive(node, ranges, false);
            // Shouldn't complete because it is blocked waiting for the dependency just created to apply
            sleep(500);
            assertFalse(syncInclusiveSyncFuture.isDone());

            // Async sync should return a result immediately since we are going to wait on the sync point transaction that was created by the sync point
            CoordinateSyncPoint asyncInclusiveSyncFuture = CoordinateSyncPoint.inclusive(node, ranges, true);
            SyncPoint localSyncPoint = getUninterruptibly(asyncInclusiveSyncFuture);
            Semaphore localSyncOccurred = new Semaphore(0);
            node.commandStores().ifLocal(PreLoadContext.contextFor(localSyncPoint.txnId), homeKey, epoch, epoch, safeStore ->
                safeStore.command(localSyncPoint.txnId).addAndInvokeListener(
                    safeStore,
                    commandListener((safeStore2, command) -> {
                        if (command.current().hasBeen(Applied))
                            localSyncOccurred.release();
                    })
                )
            ).begin(agent);

            // Move to preapplied in order to test that Barrier will find the transaction and add a listener
            for (Node n : cluster)
                getUninterruptibly(n.unsafeForKey(key).execute(context, store ->  {
                    SafeCommand safeCommand = store.command(txnId);
                    Command command = safeCommand.current();
                    PartialDeps.Builder depsBuilder = PartialDeps.builder(store.ranges().currentRanges());
                    depsBuilder.add(key, blockingTxnId);
                    PartialDeps partialDeps = depsBuilder.build();
                    Commands.commit(store, txnId, route, command.progressKey(), command.partialTxn(), txnId, partialDeps);
                    Commands.apply(store, txnId, txnId.epoch(), route, txnId, partialDeps, txn.execute(txnId, null, null), txn.query().compute(txnId, txnId, keys, null, null, null));
                }));

            Barrier listeningLocalBarrier = Barrier.barrier(node, Seekables.of(key), node.epoch(), BarrierType.local);
            // Wait and make sure the existing transaction check worked and there is no coordinate sync point created
            Thread.sleep(500);
            assertNull(listeningLocalBarrier.coordinateSyncPoint);
            assertNotNull(listeningLocalBarrier.existingTransactionCheck);
            assertEquals(txnId,getUninterruptibly(listeningLocalBarrier.existingTransactionCheck).executeAt);
            assertFalse(listeningLocalBarrier.isDone());

            // Apply the blockingTxn to unblock the rest
            for (Node n : cluster)
                assertEquals(ApplyOutcome.Success, getUninterruptibly(n.unsafeForKey(key).submit(blockingTxnContext, store -> {
                    return Commands.apply(store, blockingTxnId, blockingTxnId.epoch(), route, blockingTxnId, PartialDeps.builder(store.ranges().at(blockingTxnId.epoch())).build(), blockingTxn.execute(blockingTxnId, null, null), blockingTxn.query().compute(blockingTxnId, blockingTxnId, keys, null, null, null));
                })));
            // Global sync should be unblocked
            syncPoint = getUninterruptibly(syncInclusiveSyncFuture);
            assertEquals(node.epoch(), syncPoint.txnId.epoch());
            // Command listener for local sync transaction should get notified
            assertTrue(localSyncOccurred.tryAcquire(5, TimeUnit.SECONDS));
            // Listening local barrier should have succeeded in waiting on the local transaction that just applied
            assertEquals(getUninterruptibly(listeningLocalBarrier), getUninterruptibly(listeningLocalBarrier.existingTransactionCheck).executeAt);
            assertEquals(txnId, getUninterruptibly(listeningLocalBarrier));
        }
        finally
        {
            SimpleProgressLog.PAUSE_FOR_TEST = false;
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
        TxnId txnId = node.nextTxnId(Write, Domain.Key);
        txnId = new TxnId(txnId.epoch(), txnId.hlc() + clock, Write, Domain.Key, txnId.node);
        Txn txn = writeTxn(keys);
        Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, node.computeRoute(txnId, txn.keys())));
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
            Txn txn = new Txn.InMemory(keys, MockStore.read(Keys.EMPTY), MockStore.RESOLVER, MockStore.QUERY, MockStore.update(keys));
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
            Txn txn = new Txn.InMemory(keys, MockStore.read(keys), MockStore.RESOLVER, MockStore.QUERY, MockStore.update(Keys.EMPTY));
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

            TxnId txnId = node.nextTxnId(Write, Domain.Key);
            Keys oneKey = keys(10);
            Keys twoKeys = keys(10, 20);
            Txn txn = new Txn.InMemory(oneKey, MockStore.read(oneKey), MockStore.RESOLVER, MockStore.QUERY, MockStore.update(twoKeys));
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, txn.keys().toRoute(oneKey.get(0).toUnseekable())));
            assertEquals(MockStore.RESULT, result);

            txn = new Txn.InMemory(oneKey, MockStore.read(oneKey), MockStore.RESOLVER, MockStore.QUERY, MockStore.update(Keys.EMPTY));
            result = getUninterruptibly(cluster.get(id(1)).coordinate(txn));
            assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void quorumTest() throws Throwable
    {
        quorumTest(false, false, false, false, false);
    }

    @Test
    void quorumTestWithFailedNode() throws Throwable
    {
        quorumTest(true, false, false, false, false);
    }

    @Test
    void quorumTestFollowupWrongNode() throws Throwable
    {
        quorumTest(false, true, false, false, false);
    }

    @Test
    void quorumTestFollowupWrongKey() throws Throwable
    {
        quorumTest(false, false, true, false, false);
    }

    @Test
    void quorumTestFollowupKeyNotInRead() throws Throwable
    {
        quorumTest(false, false, false, true, false);
    }

    @Test
    void quorumTestReadRepair() throws Throwable
    {
        quorumTest(false, false, false, false, true);
        quorumTest(true, false, false, false, true);
    }

    private void quorumTest(boolean withFailedNode, boolean failOnFollowupWrongNode, boolean failOnFollowupWrongKey, boolean failOnKeyNotInRead, boolean includeRepairWrites) throws Throwable
    {
        quorumTest(true, withFailedNode, failOnFollowupWrongNode, failOnFollowupWrongKey, failOnKeyNotInRead, includeRepairWrites);
        quorumTest(false, withFailedNode, failOnFollowupWrongNode, failOnFollowupWrongKey, failOnKeyNotInRead, includeRepairWrites);
    }

    private void quorumTest(boolean read, boolean withFailedNode, boolean failOnFollowupWrongNode, boolean failOnFollowupWrongKey, boolean failOnKeyNotInRead, boolean includeRepairWrites) throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().recordMessages().build())
        {
            Node node = cluster.get(1);
            // Force an additional read to be sent to verify that additional reads are always data reads
            if (withFailedNode)
                cluster.networkFilter.isolate(id(2));

            // Add slight delay so the test has a chance to see if the callback is invoked
            // without waiting on a quorum of applies
            for (Node n : cluster)
                cluster.networkFilter.delay(n.id(), 20, TimeUnit.MILLISECONDS);
            assertNotNull(node);

            TxnId txnId;
            if (read)
                txnId = node.nextTxnId(Read, Domain.Key);
            else
                txnId = node.nextTxnId(Write, Domain.Key);

            Keys txnKeys;
            if (read)
                txnKeys = keys(10);
            else
                txnKeys = keys(10, 11);
            Keys readKeys = keys(10);
            Keys notReplicatedByNodeKeys = keys(-1);
            Keys writeKeys = !read ? keys(11) : Keys.EMPTY;
            Keys readAndWriteKeys = readKeys.with(writeKeys);
            Keys notInReadKeys = keys(32);
            List<Boolean> digestReads = synchronizedList(new ArrayList<>());
            accord.api.Read accordRead = MockStore.read(readKeys, digestReads::add, DataConsistencyLevel.QUORUM);

            // For forcing follow up reads that should generate errors
            MockFollowupRead mockFollowupRead = null;
            if (failOnFollowupWrongNode)
                mockFollowupRead = new MockFollowupRead(id(-42), readKeys);
            if (failOnFollowupWrongKey)
                mockFollowupRead = new MockFollowupRead(id(2), notReplicatedByNodeKeys);
            if (failOnKeyNotInRead)
                mockFollowupRead = new MockFollowupRead(id(2), notInReadKeys);

            MockWrite mockWrite = new MockWrite();
            MockRepairWrites repairWrites = includeRepairWrites ? new MockRepairWrites(readKeys, mockWrite) : null;
            // Testing writeDataCL is done in a separate test, in this test we do need to establish
            // that repair writes are persisted at the read consistency level, so provide DataConsistencyLevel.UNSPECIFIED
            MockUpdate mockUpdate = new MockUpdate(writeKeys, mockWrite, DataConsistencyLevel.UNSPECIFIED);
            MockResolver mockResolver = new MockResolver(true, repairWrites, mockFollowupRead);
            Txn txn;
            if (read)
                txn = new Txn.InMemory(txnKeys, accordRead, mockResolver, MockStore.QUERY_RETURNING_INPUT);
            else
                txn = new Txn.InMemory(txnKeys, accordRead, mockResolver, MockStore.QUERY_RETURNING_INPUT, mockUpdate);

            Result result = null;
            try
            {
                result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, txn.keys().toRoute(txnKeys.get(0).toUnseekable())));
                if (failOnFollowupWrongKey || failOnFollowupWrongNode || failOnKeyNotInRead)
                    fail("Query should have failed");
            }
            catch (Throwable t)
            {
                if (failOnFollowupWrongKey || failOnFollowupWrongNode || failOnKeyNotInRead)
                {
                    Throwable cause = Throwables.getRootCause(t);
                    if (failOnFollowupWrongNode)
                        assertEquals("java.lang.IllegalArgumentException: -42 is not a replica in executeAt epoch1", cause.toString());
                    if (failOnFollowupWrongKey)
                        assertEquals("java.lang.IllegalArgumentException: -1 is not replicated by node 2 in executeAt epoch 1", cause.toString());
                    if (failOnKeyNotInRead)
                        assertEquals("java.lang.IllegalArgumentException: 32 is not one of the read keys for this transaction", cause.toString());
                    return;
                }
                throw t;
            }

            // With the delay it should usually be observable if the apply responses were sent before the transaction was acked
            // If no repairs were generated then it is safe to ack before apply
            if (includeRepairWrites)
            {
                List<Key> appliedKeys = mockWrite.appliedKeys;
                // Expect either exactly (withFailedNode) or >= this number of applies
                int expectedApplies = 2 * readAndWriteKeys.size();
                if (withFailedNode)
                    assertEquals(expectedApplies, appliedKeys.size());
                else
                    assertTrue(appliedKeys.size() >= expectedApplies);
                readAndWriteKeys.forEach(key -> {
                    int count = checkedCast(appliedKeys.stream().filter(key::equals).count());
                    if (withFailedNode)
                        assertEquals(2, count);
                    else
                        assertTrue(count >= 2);
                });
            }

            MockData mockData = (MockData)result;
            System.out.println(digestReads);
            if (withFailedNode)
            {
                // With a failed node there will be no recorded digest reads because the node that got the digest read is failed and the added read due to
                // failure response should always be a data read
                assertTrue(digestReads.stream().allMatch(Boolean.FALSE::equals));
            }
            else
            {
                // The initial read should have a data read and a digest read, and the follow up reads (last 2) should always be data reads
                if (digestReads.stream().filter(Boolean.TRUE::equals).count() > 1)
                    System.out.println("oops");
                assertEquals(1, digestReads.stream().filter(Boolean.TRUE::equals).count());
                assertTrue(digestReads.get(0) == true || digestReads.get(1) == true);
            }

            Set<Id> expectedContacts = withFailedNode ? idSet(1, 3) : idSet(3, 2);
            // The initial reads should contact two different nodes
            assertEquals(2, ((MockData)mockResolver.unresolvedData).size());
            assertEquals(expectedContacts, ImmutableSet.copyOf((MockData)mockResolver.unresolvedData));
            // The follow up reads should contact the same two nodes just because that is what the mock chooses
            assertEquals(2, mockData.size());
            assertEquals(expectedContacts, ImmutableSet.copyOf(mockData));
        }
    }

    @Test
    void testNonQuorumCallbackInvokedBeforeApply() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            cluster.networkFilter.addFilter(alwaysTrue(), alwaysTrue(), message -> message instanceof ApplyReply);
            assertNotNull(node);

            TxnId txnId = node.nextTxnId(Write, Domain.Key);
            Keys keys = keys(10);
            Txn txn = writeTxn(keys);
            FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
            Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, route));
            assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void testQuorumAppliesAtCL() throws Throwable
    {
        for (DataConsistencyLevel writeDataCL : ImmutableList.of(DataConsistencyLevel.QUORUM, DataConsistencyLevel.ALL))
            testQuorumAppliesAtCL(writeDataCL);
    }

    private void testQuorumAppliesAtCL(DataConsistencyLevel writeDataCL) throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            for (boolean succeed : ImmutableList.of(true, false))
            {
                System.out.println("Testing " + writeDataCL + " with expected success: " + succeed);
                cluster.networkFilter.clear();
                Node node = cluster.get(1);
                Predicate<Id> blockNodes;
                if (succeed)
                    if (writeDataCL == DataConsistencyLevel.QUORUM)
                        blockNodes = id -> ImmutableSet.of(cluster.get(3).id()).contains(id);
                    else
                        blockNodes = alwaysFalse();
                else
                    if (writeDataCL == DataConsistencyLevel.QUORUM)
                        blockNodes = id -> ImmutableSet.of(cluster.get(2).id(), cluster.get(3).id()).contains(id);
                    else
                        blockNodes = id -> ImmutableSet.of(cluster.get(3).id()).contains(id);
                cluster.networkFilter.addFilter(blockNodes, alwaysTrue(), message -> message instanceof ApplyReply);
                assertNotNull(node);

                TxnId txnId = node.nextTxnId(Write, Domain.Key);
                Keys keys = keys(10);
                Txn txn = writeTxn(keys, writeDataCL);
                FullKeyRoute route = keys.toRoute(keys.get(0).toUnseekable());
                try
                {
                    Result result = getUninterruptibly(Coordinate.coordinate(node, txnId, txn, route));
                    assertEquals(MockStore.RESULT, result);
                    assertTrue(succeed);
                }
                catch (Throwable t)
                {
                    if (t instanceof ExecutionException && t.getCause() instanceof Timeout)
                        assertFalse(succeed);
                    else
                        throw t;
                }
            }
        }
    }

    private static CommandListener commandListener(BiConsumer<SafeCommandStore, SafeCommand> listener)
    {
        return new CommandListener()
        {
            @Override
            public void onChange(SafeCommandStore safeStore, SafeCommand command)
            {
                listener.accept(safeStore, command);
            }

            @Override
            public PreLoadContext listenerPreLoadContext(TxnId caller)
            {
                return EMPTY_PRELOADCONTEXT;
            }
        };
    }
}
