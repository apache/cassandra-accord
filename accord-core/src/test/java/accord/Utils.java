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

import accord.primitives.KeyRange;
import accord.local.Node;
import accord.impl.mock.MockStore;
import accord.primitives.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.primitives.Txn;
import accord.primitives.Keys;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
        Preconditions.checkArgument(last >= first);
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

    public static KeyRanges ranges(KeyRange... ranges)
    {
        return KeyRanges.of(ranges);
    }

    public static Txn writeTxn(Keys keys)
    {
        return new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY, MockStore.update(keys));
    }

    public static Txn readTxn(Keys keys)
    {
        return new Txn.InMemory(keys, MockStore.read(keys), MockStore.QUERY);
    }

    public static Shard shard(KeyRange range, List<Node.Id> nodes, Set<Node.Id> fastPath)
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
        return new Topologies.Multi(topologies);
    }
}
