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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import accord.api.LocalConfig;
import accord.api.ProgressLog.NoOpProgressLog;
import accord.api.RoutingKey;
import accord.api.Scheduler;
import accord.api.TestableConfigurationService;
import accord.coordinate.CoordinationAdapter;
import accord.impl.DefaultRequestTimeouts;
import accord.impl.InMemoryCommandStore;
import accord.impl.InMemoryCommandStores;
import accord.impl.IntKey;
import accord.impl.DefaultLocalListeners;
import accord.impl.DefaultRemoteListeners;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.impl.TestAgent.RethrowAgent;
import accord.impl.TopologyFactory;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.impl.mock.MockStore;
import accord.local.Node.Id;
import accord.primitives.FullKeyRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.topology.TopologyUtils;
import accord.utils.DefaultRandom;

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
        topology.set(TopologyUtils.withEpoch(topology.get(), epoch));
    }

    private static InMemoryCommandStore createStore(CommandStoreSupport storeSupport)
    {
        return (InMemoryCommandStore) createNode(ID1, storeSupport).unsafeByIndex(0);
    }

    private static Node createNode(Id id, CommandStoreSupport storeSupport)
    {
        MockCluster.Clock clock = new MockCluster.Clock(100);
        LocalConfig localConfig = LocalConfig.DEFAULT;
        Node node = new Node(id, null, new MockConfigurationService(null, (epoch, service) -> { }, storeSupport.local.get()),
                             clock, NodeTimeService.elapsedWrapperFromNonMonotonicSource(TimeUnit.MICROSECONDS, clock),
                             () -> storeSupport.data, new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter()), new TestAgent(), new DefaultRandom(), Scheduler.NEVER_RUN_SCHEDULED,
                             SizeOfIntersectionSorter.SUPPLIER, DefaultRemoteListeners::new, DefaultRequestTimeouts::new, ignore -> ignore2 -> new NoOpProgressLog(), DefaultLocalListeners.Factory::new,
                             InMemoryCommandStores.Synchronized::new,
                             new CoordinationAdapter.DefaultFactory(),
                             localConfig);
        awaitUninterruptibly(node.unsafeStart());
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
        SafeCommand  safeCommand = safeStore.get(txnId, txnId, ROUTE);
        Commands.preaccept(safeStore, safeCommand, txnId, txnId.epoch(), txn.slice(FULL_RANGES, true), ROUTE);
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
        ((TestableConfigurationService)node.configService()).reportTopology(TopologyUtils.withEpoch(support.local.get(), 2));
        Timestamp expectedTimestamp = Timestamp.fromValues(2, 110, ID1);
        getUninterruptibly(commands.execute(context, (Consumer<? super SafeCommandStore>) store -> Commands.preaccept(store, store.get(txnId, txnId, ROUTE), txnId, txnId.epoch(), txn.slice(FULL_RANGES, true), ROUTE)));
        commands.execute(PreLoadContext.contextFor(txnId, txn.keys()), safeStore -> {
            Command command = safeStore.get(txnId).current();
            Assertions.assertEquals(Status.PreAccepted, command.status());
            Assertions.assertEquals(expectedTimestamp, command.executeAt());
        }).begin(new RethrowAgent());
    }
}
