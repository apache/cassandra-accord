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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import accord.api.RoutingKey;
import accord.local.Node.Id;
import accord.primitives.*;
import accord.utils.*;
import accord.utils.ArrayBuffers.IntBuffers;

import static accord.utils.SortedArrays.Search.FLOOR;
import static accord.utils.SortedArrays.exponentialSearch;

public class Topology
{
    public static final Topology EMPTY = new Topology(0, new Shard[0], Ranges.EMPTY, Collections.emptyMap(), Ranges.EMPTY, new int[0]);
    final long epoch;
    final Shard[] shards;
    final Ranges ranges;
    /**
     * TODO (desired, efficiency): do not recompute nodeLookup for sub-topologies
     */
    final Map<Id, NodeInfo> nodeLookup;
    /**
     * This array is used to permit cheaper sharing of Topology objects between requests, as we must only specify
     * the indexes within the parent Topology that we contain. This also permits us to perform efficient merges with
     * {@code NodeInfo.supersetIndexes} to find the shards that intersect a given node without recomputing the NodeInfo.
     */
    final Ranges subsetOfRanges;
    final int[] supersetIndexes;

    static class NodeInfo
    {
        final Ranges ranges;
        final int[] supersetIndexes;

        NodeInfo(Ranges ranges, int[] supersetIndexes)
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
        this.ranges = Ranges.ofSortedAndDeoverlapped(Arrays.stream(shards).map(shard -> shard.range).toArray(Range[]::new));
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
            Ranges ranges = this.ranges.select(supersetIndexes);
            nodeLookup.put(e.getKey(), new NodeInfo(ranges, supersetIndexes));
        }
    }

    public Topology(long epoch, Shard[] shards, Ranges ranges, Map<Id, NodeInfo> nodeLookup, Ranges subsetOfRanges, int[] supersetIndexes)
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

    public Ranges rangesForNode(Id node)
    {
        NodeInfo info = nodeLookup.get(node);
        return info != null ? info.ranges : Ranges.EMPTY;
    }

    // TODO (low priority, efficiency): optimised HomeKey concept containing the Key, Shard and Topology to avoid lookups when topology hasn't changed
    public Shard forKey(RoutingKey key)
    {
        int i = ranges.indexOf(key);
        if (i < 0)
            throw new IllegalArgumentException("Range not found for " + key);
        return shards[i];
    }

    public int indexForKey(RoutingKey key)
    {
        int i = ranges.indexOf(key);
        if (i < 0) return -1;
        return Arrays.binarySearch(supersetIndexes, i);
    }

    public Topology forSelection(Unseekables<?, ?> select)
    {
        return forSelection(select, (ignore, index) -> true, null);
    }

    public <P1> Topology forSelection(Unseekables<?, ?> select, IndexedPredicate<P1> predicate, P1 param)
    {
        return forSubset(subsetFor(select, predicate, param));
    }

    public Topology forSelection(Unseekables<?, ?> select, Collection<Id> nodes)
    {
        return forSelection(select, nodes, (ignore, index) -> true, null);
    }

    public <P1> Topology forSelection(Unseekables<?, ?> select, Collection<Id> nodes, IndexedPredicate<P1> predicate, P1 param)
    {
        return forSubset(subsetFor(select, predicate, param), nodes);
    }

    private Topology forSubset(int[] newSubset)
    {
        Ranges rangeSubset = ranges.select(newSubset);

        Map<Id, NodeInfo> nodeLookup = new HashMap<>();
        for (int shardIndex : newSubset)
        {
            Shard shard = shards[shardIndex];
            for (Id id : shard.nodes)
                nodeLookup.putIfAbsent(id, this.nodeLookup.get(id));
        }
        return new Topology(epoch, shards, ranges, nodeLookup, rangeSubset, newSubset);
    }

    private Topology forSubset(int[] newSubset, Collection<Id> nodes)
    {
        Ranges rangeSubset = ranges.select(newSubset);
        Map<Id, NodeInfo> nodeLookup = new HashMap<>();
        for (Id id : nodes)
            nodeLookup.put(id, this.nodeLookup.get(id));
        return new Topology(epoch, shards, ranges, nodeLookup, rangeSubset, newSubset);
    }

    private <P1> int[] subsetFor(Unseekables<?, ?> select, IndexedPredicate<P1> predicate, P1 param)
    {
        int count = 0;
        IntBuffers cachedInts = ArrayBuffers.cachedInts();
        int[] newSubset = cachedInts.getInts(Math.min(select.size(), subsetOfRanges.size()));
        try
        {
            Routables<?, ?> as = select;
            Ranges bs = subsetOfRanges;
            int ai = 0, bi = 0;
            // ailim tracks which ai have been included; since there may be multiple matches
            // we cannot increment ai to avoid missing a match with a second bi
            int ailim = 0;

            if (subsetOfRanges == ranges)
            {
                while (true)
                {
                    long abi = as.findNextIntersection(ai, bs, bi);
                    if (abi < 0)
                    {
                        if (ailim < as.size())
                            throw new IllegalArgumentException("Range not found for " + as.get(ailim));
                        break;
                    }

                    ai = (int)abi;
                    if (ailim < ai)
                        throw new IllegalArgumentException("Range not found for " + as.get(ailim));

                    bi = (int)(abi >>> 32);
                    if (predicate.test(param, bi))
                        newSubset[count++] = bi;

                    ailim = as.findNext(ai + 1, bs.get(bi), FLOOR);
                    if (ailim < 0) ailim = -1 - ailim;
                    else ailim++;
                    ++bi;
                }
            }
            else
            {
                while (true)
                {
                    long abi = as.findNextIntersection(ai, bs, bi);
                    if (abi < 0)
                        break;

                    bi = (int)(abi >>> 32);
                    if (predicate.test(param, bi))
                        newSubset[count++] = bi;

                    ++bi;
                }
            }
        }
        catch (Throwable t)
        {
            cachedInts.forceDiscard(newSubset);
            throw t;
        }

        return cachedInts.completeAndDiscard(newSubset, count);
    }

    public <P1> void visitNodeForKeysOnceOrMore(Unseekables<?, ?> select, Consumer<Id> nodes)
    {
        visitNodeForKeysOnceOrMore(select, (i1, i2) -> true, null, nodes);
    }

    public <P1> void visitNodeForKeysOnceOrMore(Unseekables<?, ?> select, IndexedPredicate<P1> predicate, P1 param, Consumer<Id> nodes)
    {
        for (int shardIndex : subsetFor(select, predicate, param))
        {
            Shard shard = shards[shardIndex];
            for (Id id : shard.nodes)
                nodes.accept(id);
        }
    }

    public <T> T foldl(Unseekables<?, ?> select, IndexedBiFunction<Shard, T, T> function, T accumulator)
    {
        Unseekables<?, ?> as = select;
        Ranges bs = ranges;
        int ai = 0, bi = 0;

        while (true)
        {
            long abi = as.findNextIntersection(ai, bs, bi);
            if (abi < 0)
                break;

            ai = (int)(abi);
            bi = (int)(abi >>> 32);

            accumulator = function.apply(shards[bi], accumulator, bi);
            ++bi;
        }

        return accumulator;
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
                consumer.accept(shards[a[ai]], ai);
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

    public <P1, P2, P3, O> O mapReduceOn(Id on, int offset, IndexedTriFunction<? super P1, ? super P2, ? super P3, ? extends O> function, P1 p1, P2 p2, P3 p3, BiFunction<? super O, ? super O, ? extends O> reduce, O initialValue)
    {
        NodeInfo info = nodeLookup.get(on);
        if (info == null)
            return initialValue;
        int[] a = supersetIndexes, b = info.supersetIndexes;
        int ai = 0, bi = 0;
        while (ai < a.length && bi < b.length)
        {
            if (a[ai] == b[bi])
            {
                O next = function.apply(p1, p2, p3, offset + ai);
                initialValue = reduce.apply(initialValue, next);
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

    public <P> int foldlIntOn(Id on, IndexedIntFunction<P> consumer, P param, int offset, int initialValue, int terminalValue)
    {
        // TODO (low priority, efficiency/clarity): use findNextIntersection?
        NodeInfo info = nodeLookup.get(on);
        if (info == null)
            return initialValue;
        int[] a = supersetIndexes, b = info.supersetIndexes;
        int ai = 0, bi = 0;
        while (ai < a.length && bi < b.length)
        {
            if (a[ai] == b[bi])
            {
                initialValue = consumer.apply(param, initialValue, offset + ai);
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
            consumer.accept(shards[supersetIndexes[i]], i);
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

    public Ranges ranges()
    {
        return ranges;
    }

    public Shard[] unsafeGetShards()
    {
        return shards;
    }
}
