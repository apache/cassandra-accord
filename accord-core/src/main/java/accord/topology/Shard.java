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

package accord.topology;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import accord.local.Node.Id;
import accord.primitives.Range;
import accord.primitives.RoutableKey;
import accord.utils.SortedArrays.SortedArrayList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import static accord.utils.Invariants.checkArgument;

// TODO (expected, efficiency): concept of region/locality
public class Shard
{
    public final Range range;
    public final SortedArrayList<Id> nodes;
    public final Set<Id> fastPathElectorate;
    public final Set<Id> joining;
    public final int maxFailures;
    public final int recoveryFastPathSize;
    public final int fastPathQuorumSize;
    public final int fastPathRejectSize;
    public final int slowPathQuorumSize;
    public final boolean pendingRemoval;

    public Shard(Range range, SortedArrayList<Id> nodes, Set<Id> fastPathElectorate, Set<Id> joining, boolean pendingRemoval)
    {
        this.range = range;
        this.nodes = nodes;
        this.maxFailures = maxToleratedFailures(nodes.size());
        this.fastPathElectorate = ImmutableSet.copyOf(fastPathElectorate);
        this.joining = checkArgument(ImmutableSet.copyOf(joining), Iterables.all(joining, nodes::contains),
                "joining nodes must also be present in nodes; joining=%s, nodes=%s", joining, nodes);
        int e = fastPathElectorate.size();
        this.recoveryFastPathSize = (maxFailures+1)/2;
        this.slowPathQuorumSize = slowPathQuorumSize(nodes.size());
        this.fastPathQuorumSize = fastPathQuorumSize(nodes.size(), e, maxFailures);
        this.fastPathRejectSize = 1 + fastPathElectorate.size() - fastPathQuorumSize;
        this.pendingRemoval = pendingRemoval;
    }

    public Shard(Range range, SortedArrayList<Id> nodes, Set<Id> fastPathElectorate, Set<Id> joining)
    {
        this(range, nodes, fastPathElectorate, joining, false);
    }

    public Shard(Range range, SortedArrayList<Id> nodes, Set<Id> fastPathElectorate)
    {
        this(range, nodes, fastPathElectorate, Collections.emptySet());
    }

    @VisibleForTesting
    public static int maxToleratedFailures(int replicas)
    {
        return (replicas - 1) / 2;
    }

    @VisibleForTesting
    static int fastPathQuorumSize(int replicas, int electorate, int f)
    {
        checkArgument(electorate >= replicas - f);
        return (f + electorate)/2 + 1;
    }

    public static int slowPathQuorumSize(int replicas)
    {
        return replicas - maxToleratedFailures(replicas);
    }

    public int rf()
    {
        return nodes.size();
    }

    public boolean contains(RoutableKey key)
    {
        return range.contains(key);
    }

    public String toString(boolean extendedInfo)
    {
        String s = "Shard[" + range.start() + ',' + range.end() + ']';

        if (extendedInfo)
        {
            StringBuilder sb = new StringBuilder(s);
            sb.append(":(");
            for (int i=0, mi=nodes.size(); i<mi; i++)
            {
                if (i > 0)
                    sb.append(", ");

                Id node = nodes.get(i);
                sb.append(node);
                if (fastPathElectorate.contains(node))
                    sb.append('f');
            }
            sb.append(')');
            if (!joining.isEmpty())
                sb.append(":joining=").append(joining);
            s = sb.toString();
        }
        return s;
    }

    public boolean contains(Id id)
    {
        return nodes.find(id) >= 0;
    }

    public boolean containsAll(List<Id> ids)
    {
        for (int i = 0, max = ids.size() ; i < max ; ++i)
        {
            if (!contains(ids.get(i)))
                return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return toString(true);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shard shard = (Shard) o;
        return recoveryFastPathSize == shard.recoveryFastPathSize
            && fastPathQuorumSize == shard.fastPathQuorumSize
            && slowPathQuorumSize == shard.slowPathQuorumSize
            && range.equals(shard.range)
            && nodes.equals(shard.nodes)
            && fastPathElectorate.equals(shard.fastPathElectorate)
            && joining.equals(shard.joining);
    }

    @Override
    public int hashCode()
    {
        return range.hashCode();
    }
}
