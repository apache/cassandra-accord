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

import java.util.Arrays;

import accord.api.TopologySorter;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.utils.ArrayBuffers;
import accord.utils.ArrayBuffers.RecursiveObjectBuffers;
import accord.utils.IndexedConsumer;
import accord.utils.Invariants;
import accord.utils.SortedArrays;
import accord.utils.SortedArrays.SortedArrayList;

import static accord.utils.Invariants.illegalState;
import static accord.utils.SortedArrays.isSortedUnique;

// TODO (desired, efficiency/clarity): since Topologies are rarely needed, should optimise API for single topology case
//  (e.g. at least implementing Topologies by Topology)
public interface Topologies extends TopologySorter
{
    enum SelectNodeOwnership
    {
        /**
         * Slice node ownership information to cover only those ranges we have queried.
         */
        SLICE,

        /**
         * Use the node information from the topology we are selecting from. This means nodes may report
         * ranges that do not intersect the ranges we are selecting.
         */
        SHARE
    }

    Topology current();

    default boolean containsEpoch(long epoch) { return epoch >= oldestEpoch() && epoch <= currentEpoch(); }
    int indexForEpoch(long epoch);

    Topology getEpoch(long epoch);
    Topologies forEpoch(long epoch);
    Topologies forEpochs(long minEpochInclusive, long maxEpochInclusive);

    long oldestEpoch();

    default long currentEpoch()
    {
        return current().epoch;
    }

    // topologies are stored in reverse epoch order, with the highest epoch at idx 0
    Topology get(int i);

    int size();

    default boolean isEmpty()
    {
        return size() == 0;
    }

    int totalShards();

    boolean contains(Id to);

    /**
     * This should be cheap to evaluate
     */
    SortedArrayList<Id> nodes();

    default SortedArrayList<Id> staleOrRemovedIds()
    {
        SortedArrayList<Id> cur = current().stale;
        for (int i = size() - 1 ; i >= 1 ; --i)
        {
            Topology topology = get(i);
            if (!topology.removed.isEmpty())
                cur = cur.with(topology.removed);
        }
        return cur;
    }

    Ranges computeRangesForNode(Id node);

    int maxShardsPerEpoch();

    Topologies select(SortedArrayList<Node.Id> nodes);

    Topologies selectSince(Participants<?> participants, long sinceEpoch, SelectNodeOwnership selectNodeOwnership);

    Topologies selectEpoch(Participants<?> participants, long epoch, SelectNodeOwnership selectNodeOwnership);

    default void forEach(IndexedConsumer<Topology> consumer)
    {
        for (int i=0, mi=size(); i<mi; i++)
            consumer.accept(get(i), i);
    }

    static boolean equals(Topologies t, Object o)
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

    static int hashCode(Topologies t)
    {
        int hashCode = 1;
        for (int i=0, mi=t.size(); i<mi; i++) {
            hashCode = 31 * hashCode + t.get(i).hashCode();
        }
        return hashCode;
    }

    static String toString(Topologies t)
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
        private final TopologySorter sorter;
        private final Topology topology;

        public Single(TopologySorter.Supplier sorter, Topology topology)
        {
            this.topology = topology;
            this.sorter = sorter.get(this);
        }

        public Single(TopologySorter sorter, Topology topology)
        {
            this.topology = topology;
            this.sorter = sorter;
        }

        @Override
        public Topology current()
        {
            return topology;
        }

        @Override
        public int indexForEpoch(long epoch)
        {
            if (topology.epoch != epoch)
                throw new IndexOutOfBoundsException();
            return 0;
        }

        @Override
        public Topology getEpoch(long epoch)
        {
            if (topology.epoch != epoch)
                throw new IndexOutOfBoundsException();
            return topology;
        }

        @Override
        public Topologies forEpoch(long epoch)
        {
            if (epoch != topology.epoch)
                throw new IndexOutOfBoundsException();
            return this;
        }

        @Override
        public Topologies forEpochs(long minEpochInclusive, long maxEpochInclusive)
        {
            if (minEpochInclusive != topology.epoch || maxEpochInclusive != topology.epoch)
                throw new IndexOutOfBoundsException();
            return this;
        }

        @Override
        public long oldestEpoch()
        {
            return currentEpoch();
        }

        @Override
        public Topology get(int i)
        {
            if (i != 0)
                throw new IndexOutOfBoundsException(Integer.toString(i));
            return topology;
        }

        @Override
        public Topologies selectSince(Participants<?> participants, long sinceEpoch, SelectNodeOwnership selectNodeOwnership)
        {
            Invariants.require(sinceEpoch <= currentEpoch());
            Topology subset = topology.select(participants, selectNodeOwnership);
            return subset == topology ? this : new Single(sorter, subset);
        }

        @Override
        public Topologies selectEpoch(Participants<?> participants, long epoch, SelectNodeOwnership selectNodeOwnership)
        {
            Invariants.require(epoch == currentEpoch());
            Topology subset = topology.select(participants, selectNodeOwnership);
            return subset == topology ? this : new Single(sorter, subset);
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
        public boolean contains(Id to)
        {
            return topology.contains(to);
        }

        @Override
        public SortedArrayList<Node.Id> nodes()
        {
            return topology.nodes();
        }

        @Override
        public Ranges computeRangesForNode(Id node)
        {
            return topology.rangesForNode(node);
        }

        @Override
        public int maxShardsPerEpoch()
        {
            return topology.size();
        }

        @Override
        public Topologies select(SortedArrayList<Id> nodes)
        {
            Topology subset = topology.select(nodes);
            return subset == topology ? this : new Single(sorter, topology);
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

        @Override
        public int compare(Id node1, Id node2, ShardSelection shards)
        {
            return sorter.compare(node1, node2, shards);
        }

        @Override
        public boolean isFaulty(Id node)
        {
            return sorter.isFaulty(node);
        }
    }

    class Multi implements Topologies
    {
        private final TopologySorter.Supplier supplier;
        private final TopologySorter sorter;
        private final Topology[] topologies;
        private final int maxShardsPerEpoch;
        private final SortedArrayList<Id> nodes;

        public Multi(TopologySorter.Supplier sorter, Topology... topologies)
        {
            this.topologies = Invariants.requireArgument(topologies, isSortedUnique(topologies, (a, b) -> Long.compare(b.epoch, a.epoch)));
            int maxShardsPerEpoch = 0;
            for (Topology topology : topologies)
                maxShardsPerEpoch = Math.max(maxShardsPerEpoch, topology.size());
            this.maxShardsPerEpoch = maxShardsPerEpoch;
            this.supplier = sorter;
            this.nodes = nodes(topologies);
            this.sorter = sorter.get(this);
        }

        @Override
        public Topology current()
        {
            return get(0);
        }

        @Override
        public int indexForEpoch(long epoch)
        {
            long index = get(0).epoch - epoch;
            if (index < 0 || index > size())
                throw new IndexOutOfBoundsException();
            return (int)index;
        }

        @Override
        public Topology getEpoch(long epoch)
        {
            return get(indexForEpoch(epoch));
        }

        @Override
        public Topologies forEpoch(long epoch)
        {
            if (epoch < oldestEpoch() || epoch > currentEpoch())
                throw new IndexOutOfBoundsException();
            if (size() == 1)
                return this;
            return new Single(supplier, getEpoch(epoch));
        }

        @Override
        public Topologies forEpochs(long minEpochInclusive, long maxEpochInclusive)
        {
            if (minEpochInclusive < oldestEpoch() || maxEpochInclusive > currentEpoch())
                throw new IndexOutOfBoundsException();
            if (minEpochInclusive == oldestEpoch() && maxEpochInclusive == currentEpoch())
                return this;
            if (minEpochInclusive == maxEpochInclusive)
                return new Single(supplier, getEpoch(minEpochInclusive));
            // TODO (desired): copy if underlying list is small, or delta is large (or just copy)
            return new Multi(supplier, Arrays.copyOfRange(topologies, indexForEpoch(maxEpochInclusive), 1 + indexForEpoch(minEpochInclusive)));
        }

        @Override
        public long oldestEpoch()
        {
            return get(size() - 1).epoch;
        }

        @Override
        public Topology get(int i)
        {
            return topologies[i];
        }

        @Override
        public int size()
        {
            return topologies.length;
        }

        @Override
        public int totalShards()
        {
            int count = 0;
            for (Topology topology : topologies)
                count += topology.size();
            return count;
        }

        @Override
        public boolean contains(Id to)
        {
            for (Topology topology : topologies)
            {
                if (topology.contains(to))
                    return true;
            }
            return false;
        }

        @Override
        public SortedArrayList<Id> nodes()
        {
            return nodes;
        }

        private static SortedArrayList<Id> nodes(Topology[] topologies)
        {
            if (topologies.length == 1)
                return topologies[0].nodes();

            if (topologies.length == 0)
                return Topology.NO_IDS;

            RecursiveObjectBuffers<Object> merging = new RecursiveObjectBuffers<>(ArrayBuffers.cachedAny());

            SortedArrayList<Id> exactMatch = topologies[0].nodes();
            int bufferSize = exactMatch.size();
            Object[] buffer = exactMatch.backingArrayUnsafe();

            for (int i = 1; i < topologies.length ; ++i)
            {
                Topology topology = topologies[i];
                Node.Id[] input = topology.nodes().backingArrayUnsafe();

                Object[] newBuffer = SortedArrays.linearUnion(buffer, 0, bufferSize, input, 0, input.length, (a, b) -> ((Id)a).compareTo((Id)b), merging);
                bufferSize = merging.sizeOfLast(buffer);
                if (buffer == input) exactMatch = topology.nodes();
                else if (newBuffer != buffer) exactMatch = null;
                buffer = newBuffer;
            }

            SortedArrayList<Id> result = exactMatch;
            if (exactMatch == null)
            {
                Id[] array = new Id[bufferSize];
                System.arraycopy(buffer, 0, array, 0, bufferSize);
                result = new SortedArrayList<>(array);
            }
            merging.discardBuffers();
            return result;
        }

        @Override
        public Ranges computeRangesForNode(Id node)
        {
            Ranges ranges = Ranges.EMPTY;
            for (int i = 0, mi = size() ; i < mi ; i++)
                ranges = ranges.with(get(i).rangesForNode(node));
            return ranges;
        }

        @Override
        public int maxShardsPerEpoch()
        {
            return maxShardsPerEpoch;
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

        @Override
        public int compare(Id node1, Id node2, ShardSelection shards)
        {
            return sorter.compare(node1, node2, shards);
        }

        @Override
        public boolean isFaulty(Id node)
        {
            return sorter.isFaulty(node);
        }

        @Override
        public Topologies select(SortedArrayList<Id> nodes)
        {
            Topology[] subsets = null;
            for (int i = 0 ; i < size() ; ++i)
            {
                Topology superset = topologies[i];
                Topology subset = superset.select(nodes);
                if (subset != superset && subsets == null)
                {
                    subsets = new Topology[size()];
                    System.arraycopy(topologies, 0, subsets, 0, i);
                }
                if (subsets != null)
                    subsets[i] = subset;
            }

            if (subsets == null)
                return this;
            return new Multi(supplier, subsets);
        }

        @Override
        public Topologies selectSince(Participants<?> participants, long sinceEpoch, SelectNodeOwnership selectNodeOwnership)
        {
            Topology[] subsets = null;
            int limit = topologies.length;
            if (sinceEpoch > oldestEpoch())
                limit = indexForEpoch(sinceEpoch) + 1;
            for (int i = 0 ; i < limit ; ++i)
            {
                Topology superset = topologies[i];
                Topology subset = superset.select(participants, selectNodeOwnership);
                if (subset != superset && subsets == null)
                {
                    subsets = new Topology[limit];
                    System.arraycopy(topologies, 0, subsets, 0, i);
                }
                if (subsets != null)
                    subsets[i] = subset;
            }

            if (subsets == null)
            {
                if (limit == topologies.length)
                    return this;
                if (limit == 1)
                    return new Single(supplier, topologies[0]);
                return new Multi(supplier, Arrays.copyOf(topologies, limit));
            }
            return new Multi(supplier, subsets);
        }

        @Override
        public Topologies selectEpoch(Participants<?> participants, long epoch, SelectNodeOwnership selectNodeOwnership)
        {
            if (!containsEpoch(epoch))
                throw new IndexOutOfBoundsException();

            Topology superset = getEpoch(epoch);
            Topology subset = superset.select(participants, selectNodeOwnership);
            return new Single(sorter, subset);
        }
    }

    class Builder
    {
        private Object[] buffer;
        private int size;

        public Builder(int initialCapacity)
        {
            buffer = ArrayBuffers.cachedAny().get(initialCapacity);
        }

        public void add(Topology topology)
        {
            if (size == buffer.length)
                buffer = ArrayBuffers.cachedAny().resize(buffer, size, size * 2);
            buffer[size++] = topology;
        }

        public boolean isEmpty()
        {
            return size == 0;
        }

        public Topologies build(TopologySorter.Supplier sorter)
        {
            switch (size)
            {
                case 0:
                    throw illegalState("Unable to build an empty Topologies");

                case 1:
                    return new Single(sorter, (Topology) buffer[0]);

                default:
                    Topology[] topologies = new Topology[size];
                    System.arraycopy(buffer, 0, topologies, 0, size);
                    return new Multi(sorter, topologies);
            }
        }
    }
}
