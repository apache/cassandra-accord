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

package accord.maelstrom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import accord.primitives.Range;
import accord.local.Node.Id;
import accord.maelstrom.Datum.Kind;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.utils.WrapAroundList;
import accord.utils.WrapAroundSet;

import static accord.utils.SortedArrays.SortedArrayList.copyUnsorted;
import static accord.utils.Utils.toArray;

public class TopologyFactory
{
    final int shards;
    final int rf;
    final Kind[] kinds;
    final Range[][] ranges;

    public TopologyFactory(int shards, int rf)
    {
        this.shards = shards;
        this.rf = rf;
        this.kinds = Datum.COMPARE_BY_HASH ? new Kind[] { Kind.HASH } : new Kind[] { Kind.STRING, Kind.LONG, Kind.DOUBLE };
        this.ranges = new MaelstromKey.Range[kinds.length][shards];
        for (int i = 0 ; i < kinds.length ; ++i)
        {
            Kind kind = kinds[i];
            MaelstromKey.Routing[] starts = kind.split(shards);
            MaelstromKey.Routing[] ends = new MaelstromKey.Routing[shards];
            System.arraycopy(starts, 1, ends, 0, shards - 1);
            ends[shards - 1] = new MaelstromKey.Routing(kind, null);
            this.ranges[i] = new MaelstromKey.Range[shards];
            for (int j=0; j<shards; j++)
                ranges[i][j] = new MaelstromKey.Range(starts[j], ends[j]);
        }
    }


    public Topology toTopology(Id[] cluster)
    {
        final Map<Id, Integer> lookup = new HashMap<>();
        for (int i = 0 ; i < cluster.length ; ++i)
            lookup.put(cluster[i], i);

        List<WrapAroundList<Id>> electorates = new ArrayList<>();
        List<Set<Id>> fastPathElectorates = new ArrayList<>();

        for (int i = 0 ; i < cluster.length + rf - 1 ; ++i)
        {
            WrapAroundList<Id> electorate = new WrapAroundList<>(cluster, i % cluster.length, (i + rf) % cluster.length);
            Set<Id> fastPathElectorate = new WrapAroundSet<>(lookup, electorate);
            electorates.add(electorate);
            fastPathElectorates.add(fastPathElectorate);
        }

        final List<Shard> shards = new ArrayList<>();
        for (int j = 0 ; j < kinds.length ; ++j)
        {
            for (int i = 0 ; i < this.shards ; ++i)
                shards.add(new Shard(ranges[j][i], copyUnsorted(electorates.get(i % electorates.size()), Id[]::new), fastPathElectorates.get(i % fastPathElectorates.size())));
        }
        return new Topology(1, toArray(shards, Shard[]::new));
    }
}
