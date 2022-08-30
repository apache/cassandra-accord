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

import accord.local.Node;
import accord.local.Node.Id;
import accord.utils.IndexedConsumer;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.function.Consumer;

public interface Topologies
{
    Topology current();

    Topology forEpoch(long epoch);

    long oldestEpoch();

    default long currentEpoch()
    {
        return current().epoch;
    }

    boolean fastPathPermitted();

    // topologies are stored in reverse epoch order, with the highest epoch at idx 0
    Topology get(int i);

    int size();

    int totalShards();

    Set<Node.Id> nodes();

    Set<Node.Id> copyOfNodes();

    default void forEach(IndexedConsumer<Topology> consumer)
    {
        for (int i=0, mi=size(); i<mi; i++)
            consumer.accept(i, get(i));
    }

    default void forEachShard(Consumer<Shard> consumer)
    {
        for (int i=0, mi=size(); i<mi; i++)
        {
            Topology topology = get(i);
            for (int j=0, mj=topology.size(); j<mj; j++)
            {
                consumer.accept(topology.get(j));
            }
        }
    }

    private static boolean equals(Topologies t, Object o)
    {
        if (o == t)
            return true;

        if (!(o instanceof Topologies))
            return false;

        Topologies that = (Topologies) o;
        if (t.size() != that.size())
            return false;

        for (int i=0, mi=t.size(); i<mi; i++)
        {
            if (!t.get(i).equals(that.get(i)))
                return false;
        }
        return true;
    }

    private static int hashCode(Topologies t)
    {
        int hashCode = 1;
        for (int i=0, mi=t.size(); i<mi; i++) {
            hashCode = 31 * hashCode + t.get(i).hashCode();
        }
        return hashCode;
    }

    private static String toString(Topologies t)
    {
        StringBuilder sb = new StringBuilder("[");
        for (int i=0, mi=t.size(); i<mi; i++)
        {
            if (i > 0)
                sb.append(", ");

            sb.append(t.get(i).toString());
        }
        sb.append("]");
        return sb.toString();
    }

    class Single implements Topologies
    {
        private final Topology topology;
        private final boolean fastPathPermitted;

        public Single(Topology topology, boolean fastPathPermitted)
        {
            this.topology = topology;
            this.fastPathPermitted = fastPathPermitted;
        }

        @Override
        public Topology current()
        {
            return topology;
        }

        @Override
        public Topology forEpoch(long epoch)
        {
            if (topology.epoch != epoch)
                throw new IndexOutOfBoundsException();
            return topology;
        }

        @Override
        public long oldestEpoch()
        {
            return currentEpoch();
        }

        @Override
        public boolean fastPathPermitted()
        {
            return fastPathPermitted;
        }

        @Override
        public Topology get(int i)
        {
            if (i != 0)
                throw new IndexOutOfBoundsException(i);
            return topology;
        }

        @Override
        public int size()
        {
            return 1;
        }

        @Override
        public int totalShards()
        {
            return topology.size();
        }

        @Override
        public Set<Node.Id> nodes()
        {
            return topology.nodes();
        }

        @Override
        public Set<Id> copyOfNodes()
        {
            return new HashSet<>(nodes());
        }

        @Override
        public boolean equals(Object obj)
        {
            return Topologies.equals(this, obj);
        }

        @Override
        public int hashCode()
        {
            return Topologies.hashCode(this);
        }

        @Override
        public String toString()
        {
            return Topologies.toString(this);
        }
    }

    class Multi implements Topologies
    {
        private final List<Topology> topologies;

        public Multi(int initialCapacity)
        {
            this.topologies = new ArrayList<>(initialCapacity);
        }

        public Multi(Topology... topologies)
        {
            this(topologies.length);
            for (Topology topology : topologies)
                add(topology);
        }

        @Override
        public Topology current()
        {
            return get(0);
        }

        @Override
        public Topology forEpoch(long epoch)
        {
            long index = get(0).epoch - epoch;
            if (index < 0 || index > size())
                throw new IndexOutOfBoundsException();
            return get((int)index);
        }

        @Override
        public long oldestEpoch()
        {
            return get(size() - 1).epoch;
        }

        @Override
        public boolean fastPathPermitted()
        {
            // TODO (soon): this is overly restrictive: we can still take the fast-path during topology movements,
            //              just not for transactions started across the initiation of a topology movement (i.e.
            //              where the epoch changes while the transaction is being pre-accepted)
            return false;
        }

        @Override
        public Topology get(int i)
        {
            return topologies.get(i);
        }

        @Override
        public int size()
        {
            return topologies.size();
        }

        @Override
        public int totalShards()
        {
            int count = 0;
            for (int i=0, mi= topologies.size(); i<mi; i++)
                count += topologies.get(i).size();
            return count;
        }

        @Override
        public Set<Node.Id> nodes()
        {
            Set<Node.Id> result = new HashSet<>();
            for (int i=0,mi=size(); i<mi; i++)
                result.addAll(get(i).nodes());
            return result;
        }

        @Override
        public Set<Id> copyOfNodes()
        {
            return nodes();
        }

        public void add(Topology topology)
        {
            Preconditions.checkArgument(topologies.isEmpty() || topology.epoch == topologies.get(topologies.size() - 1).epoch - 1);
            topologies.add(topology);
        }

        @Override
        public boolean equals(Object obj)
        {
            return Topologies.equals(this, obj);
        }

        @Override
        public int hashCode()
        {
            return Topologies.hashCode(this);
        }

        @Override
        public String toString()
        {
            return Topologies.toString(this);
        }
    }
}
