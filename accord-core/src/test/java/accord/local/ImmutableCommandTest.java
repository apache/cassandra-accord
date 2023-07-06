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

import accord.api.ProgressLog;
import accord.api.RoutingKey;
import accord.api.TestableConfigurationService;
import accord.impl.*;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.impl.mock.MockStore;
import accord.local.Node.Id;
import accord.local.Status.Known;
import accord.primitives.*;
import accord.topology.Topology;
import com.google.common.collect.Lists;
import accord.utils.DefaultRandom;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.InMemoryCommandStore.inMemory;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.async.AsyncChains.awaitUninterruptibly;
import static accord.utils.async.AsyncChains.getUninterruptibly;

public class ImmutableCommandTest
{
    private static final Node.Id ID1 = id(1);
    private static final Node.Id ID2 = id(2);
    private static final Node.Id ID3 = id(3);
    private static final List<Node.Id> IDS = Lists.newArrayList(ID1, ID2, ID3);
    private static final Range FULL_RANGE = IntKey.range(0, 100);
    private static final Ranges FULL_RANGES = Ranges.single(FULL_RANGE);
    private static final Topology TOPOLOGY = TopologyFactory.toTopology(IDS, 3, FULL_RANGE);
    private static final IntKey.Raw KEY = IntKey.key(10);
    private static final RoutingKey HOME_KEY = KEY.toUnseekable();
    private static final FullKeyRoute ROUTE = RoutingKeys.of(HOME_KEY).toRoute(HOME_KEY);

    private static class CommandStoreSupport
    {
        final AtomicReference<Topology> local = new AtomicReference<>(TOPOLOGY);
        final MockStore data = new MockStore();
    }

    private static void setTopologyEpoch(AtomicReference<Topology> topology, long epoch)
    {
        topology.set(topology.get().withEpoch(epoch));
    }

    private static InMemoryCommandStore createStore(CommandStoreSupport storeSupport)
    {
        return (InMemoryCommandStore) createNode(ID1, storeSupport).unsafeByIndex(0);
    }

    private static class NoOpProgressLog implements ProgressLog
    {
        @Override public void unwitnessed(TxnId txnId, ProgressShard shard) {}
        @Override public void preaccepted(Command command, ProgressShard shard) {}
        @Override public void accepted(Command command, ProgressShard shard) {}
        @Override public void committed(Command command, ProgressShard shard) {}
        @Override public void readyToExecute(Command command) {}
        @Override public void executed(Command command, ProgressShard shard) {}
        @Override public void durable(Command command) {}
        @Override public void waiting(SafeCommand blockedBy, Known blockedUntil, Route<?> blockedOnRoute, Participants<?> blockedOnParticipants) {}
        @Override public void clear(TxnId txnId) {}
    }

    private static Node createNode(Id id, CommandStoreSupport storeSupport)
    {
        Node node = new Node(id, null, new MockConfigurationService(null, (epoch, service) -> { }, storeSupport.local.get()),
                        new MockCluster.Clock(100), () -> storeSupport.data, new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter()), new TestAgent(), new DefaultRandom(), null,
                        SizeOfIntersectionSorter.SUPPLIER, ignore -> ignore2 -> new NoOpProgressLog(), InMemoryCommandStores.Synchronized::new);
        awaitUninterruptibly(node.start());
        node.onTopologyUpdate(storeSupport.local.get(), true);
        return node;
    }

    @Test
    void noConflictWitnessTest()
    {
        CommandStoreSupport support = new CommandStoreSupport();
        InMemoryCommandStore commands = createStore(support);
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = clock.idForNode(1, 1);
        Keys keys = Keys.of(KEY);
        Txn txn = writeTxn(keys);

        {
            Command command = Command.NotDefined.uninitialised(txnId);
            Assertions.assertNull(inMemory(commands).command(txnId).value());
            Assertions.assertEquals(Status.NotDefined, command.status());
            Assertions.assertNull(command.executeAt());
        }
        SafeCommandStore safeStore = commands.beginOperation(PreLoadContext.contextFor(txnId, keys));
        SafeCommand  safeCommand = safeStore.get(txnId, ROUTE);
        Commands.preaccept(safeStore, safeCommand, txnId, txnId.epoch(), txn.slice(FULL_RANGES, true), ROUTE, HOME_KEY);
        Command command = safeStore.get(txnId).current();
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(txnId, command.executeAt());
    }

    @Test
    void supersedingEpochWitnessTest() throws ExecutionException {
        CommandStoreSupport support = new CommandStoreSupport();
        Node node = createNode(ID1, support);
        CommandStore commands = node.unsafeByIndex(0);
        TxnId txnId = node.nextTxnId(Write, Key);
        ((MockCluster.Clock)node.unsafeGetNowSupplier()).increment(10);
        Keys keys = Keys.of(KEY);
        Txn txn = writeTxn(keys);

        {
            Command command = Command.NotDefined.uninitialised(txnId);
            Assertions.assertNull(inMemory(commands).command(txnId).value());
            Assertions.assertEquals(Status.NotDefined, command.status());
            Assertions.assertNull(command.executeAt());
        }
        PreLoadContext context = PreLoadContext.contextFor(txnId, keys);

        setTopologyEpoch(support.local, 2);
        ((TestableConfigurationService)node.configService()).reportTopology(support.local.get().withEpoch(2));
        Timestamp expectedTimestamp = Timestamp.fromValues(2, 110, ID1);
        getUninterruptibly(commands.execute(context, (Consumer<? super SafeCommandStore>) store -> Commands.preaccept(store, store.get(txnId, ROUTE), txnId, txnId.epoch(), txn.slice(FULL_RANGES, true), ROUTE, HOME_KEY)));
        commands.execute(PreLoadContext.contextFor(txnId, txn.keys()), safeStore -> {
            Command command = safeStore.get(txnId).current();
            Assertions.assertEquals(Status.PreAccepted, command.status());
            Assertions.assertEquals(expectedTimestamp, command.executeAt());
        });
    }
}
