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

package accord.impl;

import accord.api.Key;
import accord.primitives.KeyRange;
import accord.local.Node;
import accord.primitives.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.WrapAroundList;
import accord.utils.WrapAroundSet;

import java.util.*;

public class TopologyUtils
{
    public static KeyRanges initialRanges(int num, int maxKey)
    {
        int rangeSize = maxKey / num;
        KeyRange[] ranges = new KeyRange[num];
        int end = 0;
        for (int i=0; i<num; i++)
        {
            int start = end;
            end = i == num - 1 ? maxKey : start + rangeSize;
            ranges[i] = IntKey.range(start, end);
        }
        return KeyRanges.of(ranges);
    }

    public static Topology initialTopology(Node.Id[] cluster, KeyRanges ranges, int rf)
    {
        final Map<Node.Id, Integer> lookup = new HashMap<>();
        for (int i = 0 ; i < cluster.length ; ++i)
            lookup.put(cluster[i], i);

        List<WrapAroundList<Node.Id>> electorates = new ArrayList<>();
        List<Set<Node.Id>> fastPathElectorates = new ArrayList<>();

        for (int i = 0 ; i < cluster.length + rf - 1 ; ++i)
        {
            WrapAroundList<Node.Id> electorate = new WrapAroundList<>(cluster, i % cluster.length, (i + rf) % cluster.length);
            Set<Node.Id> fastPathElectorate = new WrapAroundSet<>(lookup, electorate);
            electorates.add(electorate);
            fastPathElectorates.add(fastPathElectorate);
        }

        final List<Shard> shards = new ArrayList<>();
        for (int i = 0 ; i < ranges.size() ; ++i)
            shards.add(new Shard(ranges.get(i), electorates.get(i % electorates.size()), fastPathElectorates.get(i % fastPathElectorates.size())));
        return new Topology(1, shards.toArray(Shard[]::new));
    }

    public static Topology initialTopology(List<Node.Id> cluster, KeyRanges ranges, int rf)
    {
        return initialTopology(cluster.toArray(Node.Id[]::new), ranges, rf);
    }
}
