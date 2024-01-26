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
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.Sets;

import accord.api.Key;
import accord.api.MessageSink;
import accord.api.Scheduler;
import accord.config.LocalConfig;
import accord.config.MutableLocalConfig;
import accord.coordinate.ExecuteTxn;
import accord.coordinate.PersistTxn;
import accord.impl.InMemoryCommandStores;
import accord.impl.IntKey;
import accord.impl.SimpleProgressLog;
import accord.impl.SizeOfIntersectionSorter;
import accord.impl.TestAgent;
import accord.impl.list.ListQuery;
import accord.impl.list.ListRead;
import accord.impl.list.ListUpdate;
import accord.impl.mock.MockCluster;
import accord.impl.mock.MockConfigurationService;
import accord.impl.mock.MockStore;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.ShardDistributor;
import accord.messages.Apply;
import accord.messages.LocalRequest;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.DefaultRandom;
import accord.utils.EpochFunction;
import accord.utils.Invariants;
import accord.utils.ThreadPoolScheduler;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;

import static accord.utils.async.AsyncChains.awaitUninterruptibly;

public class Utils
{
    public static Node.Id id(int i)
    {
        return new Node.Id(i);
    }

    public static List<Node.Id> ids(int num)
    {
        List<Node.Id> rlist = new ArrayList<>(num);
        for (int i=0; i<num; i++)
        {
            rlist.add(id(i+1));
        }
        return rlist;
    }

    public static List<Node.Id> ids(int first, int last)
    {
        Invariants.checkArgument(last >= first);
        List<Node.Id> rlist = new ArrayList<>(last - first + 1);
        for (int i=first; i<=last; i++)
            rlist.add(id(i));

        return rlist;
    }

    public static List<Node.Id> idList(int... ids)
    {
        List<Node.Id> list = new ArrayList<>(ids.length);
        for (int i : ids)
            list.add(new Node.Id(i));
        return list;
    }

    public static Set<Node.Id> idSet(int... ids)
    {
        Set<Node.Id> set = Sets.newHashSetWithExpectedSize(ids.length);
        for (int i : ids)
            set.add(new Node.Id(i));
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

    public static Txn listWriteTxn(Node.Id client, Keys keys)
    {
        ListUpdate update = new ListUpdate(Function.identity());
        for (Key k : keys)
            update.put(k, 1);
        ListRead read = new ListRead(Function.identity(), false, keys, keys);
        ListQuery query = new ListQuery(client, keys.size());
        return new Txn.InMemory(keys, read, query, update);
    }

    public static Txn readTxn(Keys keys)
    {
        return new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY);
    }

    public static Shard shard(Range range, List<Node.Id> nodes, Set<Node.Id> fastPath)
    {
        return new Shard(range, nodes, fastPath);
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

    public static Node createNode(Node.Id nodeId, Topology topology, MessageSink messageSink, MockCluster.Clock clock)
    {
        MockStore store = new MockStore();
        Scheduler scheduler = new ThreadPoolScheduler();
        LocalConfig localConfig = new MutableLocalConfig();
        Node node = new Node(nodeId,
                             messageSink,
                             LocalRequest::process,
                             new MockConfigurationService(messageSink, EpochFunction.noop(), topology),
                             clock,
                             NodeTimeService.unixWrapper(TimeUnit.MICROSECONDS, clock),
                             () -> store,
                             new ShardDistributor.EvenSplit(8, ignore -> new IntKey.Splitter()),
                             new TestAgent(),
                             new DefaultRandom(),
                             scheduler,
                             SizeOfIntersectionSorter.SUPPLIER,
                             SimpleProgressLog::new,
                             InMemoryCommandStores.Synchronized::new, ExecuteTxn.FACTORY, PersistTxn.FACTORY, Apply.FACTORY,
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
}
