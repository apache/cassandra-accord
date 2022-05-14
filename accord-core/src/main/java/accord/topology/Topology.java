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

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import accord.api.RoutingKey;
import accord.local.Node.Id;
import accord.api.Key;
import accord.primitives.AbstractKeys;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.utils.IndexedConsumer;
import accord.utils.IndexedBiFunction;
import accord.utils.IndexedIntFunction;
import accord.utils.IndexedPredicate;

import static accord.utils.SortedArrays.exponentialSearch;

public class Topology
{
    public static final Topology EMPTY = new Topology(0, new Shard[0], KeyRanges.EMPTY, Collections.emptyMap(), KeyRanges.EMPTY, new int[0]);
    final long epoch;
    final Shard[] shards;
    final KeyRanges ranges;
    final Map<Id, NodeInfo> nodeLookup;
    final KeyRanges subsetOfRanges;
    final int[] supersetIndexes;

    static class NodeInfo
    {
        final KeyRanges ranges;
        final int[] supersetIndexes;

        NodeInfo(KeyRanges ranges, int[] supersetIndexes)
        {
            this.ranges = ranges;
            this.supersetIndexes = supersetIndexes;
        }

        @Override
        public String toString()
        {
            return ranges.toString();
        }
    }

    public Topology(long epoch, Shard... shards)
    {
        this.epoch = epoch;
        this.ranges = KeyRanges.ofSortedAndDeoverlapped(Arrays.stream(shards).map(shard -> shard.range).toArray(KeyRange[]::new));
        this.shards = shards;
        this.subsetOfRanges = ranges;
        this.supersetIndexes = IntStream.range(0, shards.length).toArray();
        this.nodeLookup = new HashMap<>();
        Map<Id, List<Integer>> build = new HashMap<>();
        for (int i = 0 ; i < shards.length ; ++i)
        {
            for (Id node : shards[i].nodes)
                build.computeIfAbsent(node, ignore -> new ArrayList<>()).add(i);
        }
        for (Map.Entry<Id, List<Integer>> e : build.entrySet())
        {
            int[] supersetIndexes = e.getValue().stream().mapToInt(i -> i).toArray();
            KeyRanges ranges = this.ranges.select(supersetIndexes);
            nodeLookup.put(e.getKey(), new NodeInfo(ranges, supersetIndexes));
        }
    }

    public Topology(long epoch, Shard[] shards, KeyRanges ranges, Map<Id, NodeInfo> nodeLookup, KeyRanges subsetOfRanges, int[] supersetIndexes)
    {
        this.epoch = epoch;
        this.shards = shards;
        this.ranges = ranges;
        this.nodeLookup = nodeLookup;
        this.subsetOfRanges = subsetOfRanges;
        this.supersetIndexes = supersetIndexes;
    }

    @Override
    public String toString()
    {
        return "Topology{" + "epoch=" + epoch + ", " + super.toString() + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Topology that = (Topology) o;
        if (this.epoch != that.epoch || this.size() != that.size() || !this.subsetOfRanges.equals(that.subsetOfRanges))
            return false;

        for (int i=0, mi=this.size(); i<mi; i++)
        {
            if (!this.get(i).equals(that.get(i)))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(epoch, ranges, subsetOfRanges);
        result = 31 * result + Arrays.hashCode(shards);
        result = 31 * result + Arrays.hashCode(supersetIndexes);
        return result;
    }

    public static Topology select(long epoch, Shard[] shards, int[] indexes)
    {
        Shard[] subset = new Shard[indexes.length];
        for (int i=0; i<indexes.length; i++)
            subset[i] = shards[indexes[i]];
        return new Topology(epoch, subset);
    }

    public boolean isSubset()
    {
        return supersetIndexes.length < shards.length;
    }

    public Topology withEpoch(long epoch)
    {
        return new Topology(epoch, shards, ranges, nodeLookup, subsetOfRanges, supersetIndexes);
    }

    public long epoch()
    {
        return epoch;
    }

    public Topology forNode(Id node)
    {
        NodeInfo info = nodeLookup.get(node);
        if (info == null)
            return Topology.EMPTY;

        Map<Id, NodeInfo> lookup = new HashMap<>();
        lookup.put(node, info);
        return new Topology(epoch, shards, ranges, lookup, info.ranges, info.supersetIndexes);
    }

    public Topology trim()
    {
        return select(epoch, shards, this.supersetIndexes);
    }

    public KeyRanges rangesForNode(Id node)
    {
        NodeInfo info = nodeLookup.get(node);
        return info != null ? info.ranges : KeyRanges.EMPTY;
    }

    // TODO: optimised HomeKey concept containing the Key, Shard and Topology to avoid lookups when topology hasn't changed
    public Shard forKey(RoutingKey key)
    {
        int i = ranges.rangeIndexForKey(key);
        if (i < 0 || i >= ranges.size())
            throw new IllegalArgumentException("Range not found for " + key);
        return shards[i];
    }

    public Topology forKeys(AbstractKeys<?, ?> select)
    {
        return forKeys(select, (i, shard) -> true);
    }

    public Topology forKeys(AbstractKeys<?, ?> select, IndexedPredicate<Shard> predicate)
    {
        int[] newSubset = subsetForKeys(select, predicate);
        KeyRanges rangeSubset = ranges.select(newSubset);

        // TODO: more efficient sharing of nodeLookup state
        Map<Id, NodeInfo> nodeLookup = new HashMap<>();
        for (int shardIndex : newSubset)
        {
            Shard shard = shards[shardIndex];
            for (Id id : shard.nodes)
                nodeLookup.putIfAbsent(id, this.nodeLookup.get(id));
        }
        return new Topology(epoch, shards, ranges, nodeLookup, rangeSubset, newSubset);
    }

    public Topology forKeys(AbstractKeys<?, ?> select, Collection<Id> nodes)
    {
        return forKeys(select, nodes, (i, shard) -> true);
    }

    public Topology forKeys(AbstractKeys<?, ?> select, Collection<Id> nodes, IndexedPredicate<Shard> predicate)
    {
        int[] newSubset = subsetForKeys(select, predicate);
        KeyRanges rangeSubset = ranges.select(newSubset);

        // TODO: more efficient sharing of nodeLookup state
        Map<Id, NodeInfo> nodeLookup = new HashMap<>();
        for (Id id : nodes)
            nodeLookup.put(id, this.nodeLookup.get(id));
        return new Topology(epoch, shards, ranges, nodeLookup, rangeSubset, newSubset);
    }

    private int[] subsetForKeys(AbstractKeys<?, ?> select, IndexedPredicate<Shard> predicate)
    {
        int subsetIndex = 0;
        int count = 0;
        int[] newSubset = new int[Math.min(select.size(), subsetOfRanges.size())];
        for (int i = 0 ; i < select.size() ; )
        {
            // find the range containing the key at i
            subsetIndex = subsetOfRanges.rangeIndexForKey(subsetIndex, subsetOfRanges.size(), select.get(i));
            if (subsetIndex < 0 || subsetIndex >= subsetOfRanges.size())
                throw new IllegalArgumentException("Range not found for " + select.get(i));
            int supersetIndex = supersetIndexes[subsetIndex];
            Shard shard = shards[supersetIndex];
            if (predicate.test(subsetIndex, shard))
                newSubset[count++] = supersetIndex;
            // find the first key outside this range
            i = shard.range.nextHigherKeyIndex(select, i);
        }
        if (count != newSubset.length)
            newSubset = Arrays.copyOf(newSubset, count);
        return newSubset;
    }

    public void visitNodeForKeysOnceOrMore(AbstractKeys<?, ?> select, IndexedPredicate<Shard> predicate, Consumer<Id> nodes)
    {
        for (int shardIndex : subsetForKeys(select, predicate))
        {
            Shard shard = shards[shardIndex];
            for (Id id : shard.nodes)
                nodes.accept(id);
        }
    }

    public <T> T foldl(AbstractKeys<?, ?> select, IndexedBiFunction<Shard, T, T> function, T accumulator)
    {
        int subsetIndex = 0;
        // TODO: use SortedArrays.findNextIntersection
        for (int i = 0 ; i < select.size() ; )
        {
            // find the range containing the key at i
            subsetIndex = subsetOfRanges.rangeIndexForKey(subsetIndex, subsetOfRanges.size(), select.get(i));
            if (subsetIndex < 0 || subsetIndex >= subsetOfRanges.size())
                throw new IllegalArgumentException("Range not found for " + select.get(i));
            int supersetIndex = supersetIndexes[subsetIndex];
            Shard shard = shards[supersetIndex];
            accumulator = function.apply(subsetIndex, shard, accumulator);
            // find the first key outside this range
            i = shard.range.nextHigherKeyIndex(select, i);
        }
        return accumulator;
    }

    /**
     * @param on the node to limit our selection to
     * @param select may be a superSet of the keys owned by {@code on} but not of this {@code Topology}
     */
    public void forEachOn(Id on, Keys select, IndexedConsumer<Shard> consumer)
    {
        NodeInfo info = nodeLookup.get(on);
        for (int i = 0, j = 0, k = 0 ; i < select.size() && j < supersetIndexes.length && k < info.supersetIndexes.length ;)
        {
            Key key = select.get(i);
            Shard shard = shards[supersetIndexes[j]];
            int c = supersetIndexes[j] - info.supersetIndexes[k];
            if (c < 0) ++j;
            else if (c > 0) ++k;
            else
            {
                int rcmp = shard.range.compareKey(key);
                if (rcmp < 0) ++i;
                else if (rcmp == 0) { consumer.accept(j, shard); i++; j++; k++; }
                else { j++; k++; }
            }
        }
    }

    public void forEachOn(Id on, IndexedConsumer<Shard> consumer)
    {
        NodeInfo info = nodeLookup.get(on);
        if (info == null)
            return;
        int[] a = supersetIndexes, b = info.supersetIndexes;
        int ai = 0, bi = 0;
        while (ai < a.length && bi < b.length)
        {
            if (a[ai] == b[bi])
            {
                consumer.accept(ai, shards[a[ai]]);
                ++ai; ++bi;
            }
            else if (a[ai] < b[bi])
            {
                ai = exponentialSearch(a, ai + 1, a.length, b[bi]);
                if (ai < 0) ai = -1 -ai;
            }
            else
            {
                bi = exponentialSearch(b, bi + 1, b.length, a[ai]);
                if (bi < 0) bi = -1 -bi;
            }
        }
    }

    public int matchesOn(Id on, IndexedPredicate<Shard> consumer)
    {
        // TODO: this can be done by divide-and-conquer splitting of the lists and recursion, which should be more efficient
        int count = 0;
        NodeInfo info = nodeLookup.get(on);
        if (info == null)
            return 0;
        int[] a = supersetIndexes, b = info.supersetIndexes;
        int ai = 0, bi = 0;
        while (ai < a.length && bi < b.length)
        {
            if (a[ai] == b[bi])
            {
                if (consumer.test(ai, shards[a[ai]]))
                    ++count;
                ++ai; ++bi;
            }
            else if (a[ai] < b[bi])
            {
                ai = exponentialSearch(a, ai + 1, a.length, b[bi]);
                if (ai < 0) ai = -1 -ai;
            }
            else
            {
                bi = exponentialSearch(b, bi + 1, b.length, a[ai]);
                if (bi < 0) bi = -1 -bi;
            }
        }
        return count;
    }

    public int foldlIntOn(Id on, IndexedIntFunction<Shard> consumer, int offset, int initialValue, int terminalValue)
    {
        // TODO: this can be done by divide-and-conquer splitting of the lists and recursion, which should be more efficient
        NodeInfo info = nodeLookup.get(on);
        if (info == null)
            return initialValue;
        int[] a = supersetIndexes, b = info.supersetIndexes;
        int ai = 0, bi = 0;
        while (ai < a.length && bi < b.length)
        {
            if (a[ai] == b[bi])
            {
                initialValue = consumer.apply(offset + ai, shards[a[ai]], initialValue);
                if (terminalValue == initialValue)
                    return terminalValue;
                ++ai; ++bi;
            }
            else if (a[ai] < b[bi])
            {
                ai = exponentialSearch(a, ai + 1, a.length, b[bi]);
                if (ai < 0) ai = -1 -ai;
            }
            else
            {
                bi = exponentialSearch(b, bi + 1, b.length, a[ai]);
                if (bi < 0) bi = -1 -bi;
            }
        }
        return initialValue;
    }

    public void forEach(IndexedConsumer<Shard> consumer)
    {
        for (int i = 0; i < supersetIndexes.length ; ++i)
            consumer.accept(i, shards[supersetIndexes[i]]);
    }

    public int size()
    {
        return subsetOfRanges.size();
    }

    public int maxRf()
    {
        int rf = Integer.MIN_VALUE;
        for (int i : supersetIndexes)
            rf = Math.max(rf, shards[i].rf());
        return rf;
    }

    public Shard get(int index)
    {
        return shards[supersetIndexes[index]];
    }

    public boolean contains(Id id)
    {
        return nodeLookup.containsKey(id);
    }

    public Collection<Shard> shards()
    {
        return new AbstractCollection<Shard>()
        {
            @Override
            public Iterator<Shard> iterator()
            {
                return IntStream.of(supersetIndexes).mapToObj(i -> shards[i]).iterator();
            }

            @Override
            public int size()
            {
                return supersetIndexes.length;
            }
        };
    }

    public void forEach(Consumer<Shard> forEach)
    {
        for (int i : supersetIndexes)
            forEach.accept(shards[i]);
    }

    public Set<Id> nodes()
    {
        return nodeLookup.keySet();
    }

    public KeyRanges ranges()
    {
        return ranges;
    }

    // TODO: use SortedArrays impls
    private static boolean intersects(int[] is, int[] js)
    {
        for (int i = 0, j = 0 ; i < is.length && j < js.length ;)
        {
            int c = is[i] - js[j];
            if (c < 0) ++i;
            else if (c > 0) ++j;
            else return true;
        }
        return false;
    }

    public Shard[] unsafeGetShards()
    {
        return shards;
    }
}
