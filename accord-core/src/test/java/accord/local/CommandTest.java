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
import accord.impl.InMemoryCommandStores;
import accord.impl.IntKey;
import accord.impl.TestAgent;
import accord.impl.TopologyFactory;
import accord.impl.*;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.impl.mock.MockStore;
import accord.local.Node.Id;
import accord.local.Status.Known;
import accord.primitives.*;
import accord.topology.Topology;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static accord.Utils.id;
import static accord.Utils.writeTxn;
import static accord.impl.InMemoryCommandStore.inMemory;
import static accord.primitives.Routable.Domain.Key;
import static accord.primitives.Txn.Kind.Write;
import static accord.utils.async.AsyncChains.getUninterruptibly;

public class CommandTest
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

    private static CommandStore createStore(CommandStoreSupport storeSupport)
    {
        return createNode(ID1, storeSupport).unsafeByIndex(0);
    }

    private static class NoOpProgressLog implements ProgressLog
    {
        @Override
        public void unwitnessed(TxnId txnId, RoutingKey homeKey, ProgressShard shard)
        {
        }

        @Override
        public void preaccepted(Command command, ProgressShard shard)
        {
        }

        @Override
        public void accepted(Command command, ProgressShard shard)
        {
        }

        @Override
        public void committed(Command command, ProgressShard shard)
        {
        }

        @Override
        public void readyToExecute(Command command, ProgressShard shard)
        {
        }

        @Override
        public void executed(Command command, ProgressShard shard)
        {
        }

        @Override
        public void invalidated(Command command, ProgressShard shard)
        {
        }

        @Override
        public void durableLocal(TxnId txnId)
        {
        }

        @Override
        public void durable(Command command, @Nullable Set<Id> persistedOn)
        {
        }

        @Override
        public void durable(TxnId txnId, @Nullable Unseekables<?, ?> unseekables, ProgressShard shard)
        {
        }

        @Override
        public void waiting(TxnId blockedBy, Known blockedUntil, Unseekables<?, ?> blockedOn)
        {
        }
    }

    private static Node createNode(Id id, CommandStoreSupport storeSupport)
    {
        return new Node(id, null, new MockConfigurationService(null, (epoch, service) -> { }, storeSupport.local.get()),
                        new MockCluster.Clock(100), () -> storeSupport.data, new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter()), new TestAgent(), new Random(), null,
                        SizeOfIntersectionSorter.SUPPLIER, ignore -> ignore2 -> new NoOpProgressLog(), InMemoryCommandStores.Synchronized::new);
    }

    @Test
    void noConflictWitnessTest()
    {
        CommandStoreSupport support = new CommandStoreSupport();
        CommandStore commands = createStore(support);
        MockCluster.Clock clock = new MockCluster.Clock(100);
        TxnId txnId = clock.idForNode(1, 1);
        Txn txn = writeTxn(Keys.of(KEY));

        Command command = new InMemoryCommand(commands, txnId);
        Assertions.assertEquals(Status.NotWitnessed, command.status());
        Assertions.assertNull(command.executeAt());

        command.preaccept(inMemory(commands), txn.slice(FULL_RANGES, true), ROUTE, HOME_KEY);
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(txnId, command.executeAt());
    }

    @Test
    void supersedingEpochWitnessTest()
    {
        CommandStoreSupport support = new CommandStoreSupport();
        Node node = createNode(ID1, support);
        CommandStore commands = node.unsafeByIndex(0);
        TxnId txnId = node.nextTxnId(Write, Key);
        ((MockCluster.Clock)node.unsafeGetNowSupplier()).increment(10);
        Txn txn = writeTxn(Keys.of(KEY));

        Command command = new InMemoryCommand(commands, txnId);
        Assertions.assertEquals(Status.NotWitnessed, command.status());
        Assertions.assertNull(command.executeAt());

        setTopologyEpoch(support.local, 2);
        ((TestableConfigurationService)node.configService()).reportTopology(support.local.get().withEpoch(2));
        Timestamp expectedTimestamp = Timestamp.fromValues(2, 110, ID1);
        getUninterruptibly(commands.execute(null, (Consumer<? super SafeCommandStore>) store -> command.preaccept(store, txn.slice(FULL_RANGES, true), ROUTE, HOME_KEY)));
        Assertions.assertEquals(Status.PreAccepted, command.status());
        Assertions.assertEquals(expectedTimestamp, command.executeAt());
    }
}
