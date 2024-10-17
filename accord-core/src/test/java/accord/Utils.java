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

package accord;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import accord.api.Agent;
import com.google.common.collect.Sets;

import accord.api.Key;
import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.api.TopologySorter;
import accord.api.LocalConfig;
import accord.coordinate.CoordinationAdapter;
import accord.impl.DefaultTimeouts;
import accord.impl.InMemoryCommandStores;
import accord.impl.IntKey;
import accord.impl.DefaultLocalListeners;
import accord.impl.progresslog.DefaultProgressLogs;
import accord.impl.DefaultRemoteListeners;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListUpdate;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.impl.mock.MockStore;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.ShardDistributor;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.utils.DefaultRandom;
import accord.utils.EpochFunction;
import accord.utils.Invariants;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.ThreadPoolScheduler;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;

import static accord.utils.async.AsyncChains.awaitUninterruptibly;

public class Utils
{
    public static Id id(int i)
    {
        return new Id(i);
    }

    public static List<Id> ids(int num)
    {
        List<Id> rlist = new ArrayList<>(num);
        for (int i=0; i<num; i++)
        {
            rlist.add(id(i+1));
        }
        return rlist;
    }

    public static SortedArrayList<Id> ids(int first, int last)
    {
        Invariants.checkArgument(last >= first);
        Id[] rlist = new Id[last - first + 1];
        for (int i=first; i<=last; i++)
            rlist[i - first] = id(i);

        return new SortedArrayList<>(rlist);
    }

    public static SortedArrayList<Id> idList(int... ids)
    {
        Id[] list = new Id[ids.length];
        for (int i = 0 ; i < ids.length ; ++i)
            list[i] = new Id(ids[i]);
        Arrays.sort(list);
        return new SortedArrayList<>(list);
    }

    public static Set<Id> idSet(int... ids)
    {
        Set<Id> set = Sets.newHashSetWithExpectedSize(ids.length);
        for (int i : ids)
            set.add(new Id(i));
        return set;
    }

    public static Ranges ranges(Range... ranges)
    {
        return Ranges.of(ranges);
    }

    public static Txn writeTxn(Keys keys)
    {
        return new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY, MockStore.update(keys));
    }
    public static Txn writeTxn(Ranges ranges)
    {
        return new Txn.InMemory(ranges, MockStore.read(ranges), MockStore.QUERY, MockStore.update(ranges));
    }

    public static Txn listWriteTxn(Id client, Keys keys)
    {
        ListUpdate update = new ListUpdate(Function.identity());
        for (Key k : keys)
            update.put(k, 1);
        ListRead read = new ListRead(Function.identity(), false, keys, keys);
        ListQuery query = new ListQuery(client, keys.size(), false);
        return new Txn.InMemory(keys, read, query, update);
    }

    public static Txn readTxn(Keys keys)
    {
        return new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY);
    }

    public static Shard shard(Range range, SortedArrayList<Id> nodes, Set<Id> fastPath)
    {
        return new Shard(range, nodes, fastPath);
    }

    public static Shard shard(Range range, SortedArrayList<Node.Id> nodes)
    {
        return shard(range, nodes, new TreeSet<>(nodes));
    }

    public static Topology topology(long epoch, Shard... shards)
    {
        return new Topology(epoch, shards);
    }

    public static Topology topology(Shard... shards)
    {
        return topology(1, shards);
    }

    public static Topologies topologies(Topology... topologies)
    {
        return new Topologies.Multi(SizeOfIntersectionSorter.SUPPLIER, topologies);
    }

    public static Node createNode(Id nodeId, Topology topology, MessageSink messageSink, MockCluster.Clock clock)
    {
        return createNode(nodeId, topology, messageSink, clock, new TestAgent());
    }

    public static Node createNode(Node.Id nodeId, Topology topology, MessageSink messageSink, MockCluster.Clock clock, Agent agent)
    {
        MockStore store = new MockStore();
        Scheduler scheduler = new ThreadPoolScheduler();
        LocalConfig localConfig = LocalConfig.DEFAULT;
        Node node = new Node(nodeId,
                             messageSink,
                             new MockConfigurationService(messageSink, EpochFunction.noop(), topology),
                             clock,
                             () -> store,
                             new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter()),
                             agent,
                             new DefaultRandom(),
                             scheduler,
                             SizeOfIntersectionSorter.SUPPLIER,
                             DefaultRemoteListeners::new,
                             DefaultTimeouts::new,
                             DefaultProgressLogs::new,
                             DefaultLocalListeners.Factory::new,
                             InMemoryCommandStores.Synchronized::new,
                             new CoordinationAdapter.DefaultFactory(),
                             DurableBefore.NOOP_PERSISTER,
                             localConfig);
        awaitUninterruptibly(node.unsafeStart());
        return node;
    }

    public static void spinUntilSuccess(ThrowingRunnable runnable)
    {
        spinUntilSuccess(runnable, 10);
    }

    public static void spinUntilSuccess(ThrowingRunnable runnable, int timeoutInSeconds)
    {
        Awaitility.await()
                  .pollInterval(Duration.ofMillis(100))
                  .pollDelay(0, TimeUnit.MILLISECONDS)
                  .atMost(timeoutInSeconds, TimeUnit.SECONDS)
                  .ignoreExceptions()
                  .untilAsserted(runnable);
    }

    public static TopologyManager testTopologyManager(TopologySorter.Supplier sorter, Id node)
    {
        return new TopologyManager(sorter, new TestAgent.RethrowAgent(), node, Scheduler.NEVER_RUN_SCHEDULED, new MockCluster.Clock(0), LocalConfig.DEFAULT);
    }
}
