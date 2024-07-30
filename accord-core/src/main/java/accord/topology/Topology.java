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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.RoutingKey;
import accord.local.Node.Id;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routables;
import accord.primitives.Unseekables;
import accord.utils.ArrayBuffers;
import accord.utils.ArrayBuffers.IntBuffers;
import accord.utils.IndexedBiFunction;
import accord.utils.IndexedConsumer;
import accord.utils.IndexedIntFunction;
import accord.utils.IndexedTriFunction;
import accord.utils.SimpleBitSet;
import accord.utils.SortedArrays.SortedArrayList;
import accord.utils.Utils;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntArrayList;

import static accord.utils.Invariants.illegalArgument;
import static accord.utils.SortedArrays.Search.FLOOR;
import static accord.utils.SortedArrays.exponentialSearch;

public class Topology
{
    public static final long EMPTY_EPOCH = 0;
    private static final int[] EMPTY_SUBSET = new int[0];
    public static final Topology EMPTY = new Topology(null, EMPTY_EPOCH, Collections.emptySet(), new Shard[0], Ranges.EMPTY, new SortedArrayList<>(new Id[0]), new Int2ObjectHashMap<>(0, 0.9f), Ranges.EMPTY, EMPTY_SUBSET);

    final long epoch;
    final Set<Id> staleIds;
    final Shard[] shards;
    final Ranges ranges;

    final SortedArrayList<Id> nodeIds;
    final Int2ObjectHashMap<NodeInfo> nodeLookup;

    /**
     * This array is used to permit cheaper sharing of Topology objects between requests, as we must only specify
     * the indexes within the parent Topology that we contain. This also permits us to perform efficient merges with
     * {@code NodeInfo.supersetIndexes} to find the shards that intersect a given node without recomputing the NodeInfo.
     */
    final Ranges subsetOfRanges;
    final int[] supersetIndexes;
    /**
     * When the field is {@code null} use {@code this}, else use the value referenced; in most cases using {@link #global()} is best.
     */
    @Nullable
    final Topology global;

    static class NodeInfo
    {
        final Ranges ranges;
        final int[] supersetIndexes;

        NodeInfo(Ranges ranges, int[] supersetIndexes)
        {
            this.ranges = ranges;
            this.supersetIndexes = supersetIndexes;
        }

        private NodeInfo forSubset(int[] newSubset)
        {
            IntArrayList matches = new IntArrayList();
            List<Range> matchedRanges = new ArrayList<>(newSubset.length);
            for (int index : newSubset)
            {
                int idx = Arrays.binarySearch(supersetIndexes, index);
                if (idx < 0) continue;
                // found a match
                matches.add(index);
                matchedRanges.add(ranges.get(idx));
            }
            Ranges ranges = Ranges.ofSortedAndDeoverlapped(Utils.toArray(matchedRanges, Range[]::new));
            int[] supersetIndexes = matches.toIntArray();
            return new NodeInfo(ranges, supersetIndexes);
        }

        @Override
        public String toString()
        {
            return ranges.toString();
        }
    }

    @VisibleForTesting
    public Topology(long epoch, Shard... shards)
    {
        this(epoch, Collections.emptySet(), shards);
    }
    
    public Topology(long epoch, Set<Id> staleIds, Shard... shards)
    {
        this.global = null;
        this.epoch = epoch;
        this.staleIds = staleIds;
        this.ranges = Ranges.ofSortedAndDeoverlapped(Arrays.stream(shards).map(shard -> shard.range).toArray(Range[]::new));
        this.shards = shards;
        this.subsetOfRanges = ranges;
        this.supersetIndexes = IntStream.range(0, shards.length).toArray();
        this.nodeLookup = new Int2ObjectHashMap<>();
        Map<Id, IntArrayList> build = new HashMap<>();
        for (int i = 0 ; i < shards.length ; ++i)
        {
            for (Id node : shards[i].nodes)
                build.computeIfAbsent(node, ignore -> new IntArrayList()).add(i);
        }
        Id[] nodeIds = new Id[build.size()];
        int count = 0;
        for (Map.Entry<Id, IntArrayList> e : build.entrySet())
        {
            int[] supersetIndexes = e.getValue().toIntArray();
            Ranges ranges = this.ranges.select(supersetIndexes);
            nodeLookup.put(e.getKey().id, new NodeInfo(ranges, supersetIndexes));
            nodeIds[count++] = e.getKey();
        }
        Arrays.sort(nodeIds);
        this.nodeIds = new SortedArrayList<>(nodeIds);
    }

    @VisibleForTesting
    Topology(@Nullable Topology global, long epoch, Set<Id> staleIds, Shard[] shards, Ranges ranges, SortedArrayList<Id> nodeIds, Int2ObjectHashMap<NodeInfo> nodeById, Ranges subsetOfRanges, int[] supersetIndexes)
    {
        this.global = global;
        this.epoch = epoch;
        this.staleIds = staleIds;
        this.shards = shards;
        this.ranges = ranges;
        this.nodeIds = nodeIds;
        this.nodeLookup = nodeById;
        this.subsetOfRanges = subsetOfRanges;
        this.supersetIndexes = supersetIndexes;
    }

    public Topology global()
    {
        return global == null ? this : global;
    }

    @Override
    public String toString()
    {
        return "Topology{" + "epoch=" + epoch + ", " + shards() + '}';
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
        int result = Objects.hash(epoch);
        result = 31 * result + shards().hashCode();
        return result;
    }

    private static Topology select(long epoch, Set<Id> staleIds, Shard[] shards, int[] indexes)
    {
        Shard[] subset = new Shard[indexes.length];
        for (int i = 0; i < indexes.length; i++)
            subset[i] = shards[indexes[i]];
        return new Topology(epoch, staleIds, subset);
    }

    public boolean isSubset()
    {
        return supersetIndexes.length < shards.length;
    }

    public long epoch()
    {
        return epoch;
    }

    public Topology forNode(Id node)
    {
        NodeInfo info = nodeLookup.get(node.id);
        if (info == null)
            return Topology.EMPTY;

        SortedArrayList<Id> nodeIds = new SortedArrayList<>(new Id[] { node });
        return new Topology(global(), epoch, staleIds, shards, ranges, nodeIds, nodeLookup, info.ranges, info.supersetIndexes);
    }

    public Topology trim()
    {
        return select(epoch, staleIds, shards, this.supersetIndexes);
    }

    public Ranges rangesForNode(Id node)
    {
        NodeInfo info = nodeLookup.get(node.id);
        return info != null ? info.ranges : Ranges.EMPTY;
    }

    // TODO (low priority, efficiency): optimised HomeKey concept containing the Key, Shard and Topology to avoid lookups when topology hasn't changed
    public Shard forKey(RoutingKey key)
    {
        int i = subsetOfRanges.indexOf(key);
        if (i < 0)
            throw illegalArgument("Range not found for " + key);
        return shards[supersetIndexes[i]];
    }

    public int indexForKey(RoutingKey key)
    {
        return subsetOfRanges.indexOf(key);
    }

    @VisibleForTesting
    public Topology withEmptySubset()
    {
        return forSubset(EMPTY_SUBSET);
    }

    public Topology forSelection(Unseekables<?> select)
    {
        return forSubset(subsetFor(select));
    }

    public Topology forSelection(Unseekables<?> select, Collection<Id> nodes)
    {
        return forSubset(subsetFor(select), nodes);
    }

    @VisibleForTesting
    Topology forSubset(int[] newSubset)
    {
        Ranges rangeSubset = ranges.select(newSubset);
        SimpleBitSet nodes = new SimpleBitSet(nodeIds.size());
        Int2ObjectHashMap<NodeInfo> nodeLookup = new Int2ObjectHashMap<>(nodes.size(), 0.8f);
        for (int shardIndex : newSubset)
        {
            Shard shard = shards[shardIndex];
            for (Id id : shard.nodes)
            {
                nodes.set(nodeIds.find(id));
                // TODO (expected): do we need to shrink to the subset? I don't think we do anymore, and if not we can avoid copying the nodeLookup entirely
                nodeLookup.putIfAbsent(id.id, this.nodeLookup.get(id.id).forSubset(newSubset));
            }
        }
        Id[] nodeIds = new Id[nodes.getSetBitCount()];
        int count = 0;
        for (int i = nodes.firstSetBit() ; i >= 0 ; i = nodes.nextSetBit(i + 1, -1))
            nodeIds[count++] = this.nodeIds.get(i);
        return new Topology(global(), epoch, staleIds, shards, ranges, new SortedArrayList<>(nodeIds), nodeLookup, rangeSubset, newSubset);
    }

    @VisibleForTesting
    Topology forSubset(int[] newSubset, Collection<Id> nodes)
    {
        Ranges rangeSubset = ranges.select(newSubset);
        Id[] nodeIds = new Id[nodes.size()];
        Int2ObjectHashMap<NodeInfo> nodeLookup = new Int2ObjectHashMap<>(nodes.size(), 0.8f);
        for (Id id : nodes)
        {
            NodeInfo info = this.nodeLookup.get(id.id).forSubset(newSubset);
            if (info.ranges.isEmpty()) continue;
            nodeIds[nodeLookup.size()] = id;
            nodeLookup.put(id.id, info);
        }
        if (nodeLookup.size() != nodeIds.length)
            nodeIds = Arrays.copyOf(nodeIds, nodeLookup.size());
        Arrays.sort(nodeIds);
        return new Topology(global(), epoch, staleIds, shards, ranges, new SortedArrayList<>(nodeIds), nodeLookup, rangeSubset, newSubset);
    }

    private int[] subsetFor(Unseekables<?> select)
    {
        int count = 0;
        IntBuffers cachedInts = ArrayBuffers.cachedInts();
        int[] newSubset = cachedInts.getInts(Math.min(select.size(), subsetOfRanges.size()));
        try
        {
            Routables<?> as = select;
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
                    if (count == newSubset.length)
                        newSubset = cachedInts.resize(newSubset, count, count * 2);
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
                    newSubset[count++] = this.supersetIndexes[bi];

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

    public void visitNodeForKeysOnceOrMore(Unseekables<?> select, Consumer<Id> nodes)
    {
        for (int shardIndex : subsetFor(select))
        {
            Shard shard = shards[shardIndex];
            for (Id id : shard.nodes)
                nodes.accept(id);
        }
    }

    public <T> T foldl(Unseekables<?> select, IndexedBiFunction<Shard, T, T> function, T accumulator)
    {
        Unseekables<?> as = select;
        Ranges bs = subsetOfRanges;
        int ai = 0, bi = 0;

        while (true)
        {
            long abi = as.findNextIntersection(ai, bs, bi);
            if (abi < 0)
                break;

            ai = (int)(abi);
            bi = (int)(abi >>> 32);

            accumulator = function.apply(shards[supersetIndexes[bi]], accumulator, bi);
            ++bi;
        }

        return accumulator;
    }

    public void forEachOn(Id on, IndexedConsumer<Shard> consumer)
    {
        NodeInfo info = nodeLookup.get(on.id);
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
        NodeInfo info = nodeLookup.get(on.id);
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
        NodeInfo info = nodeLookup.get(on.id);
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
        return nodeLookup.containsKey(id.id);
    }

    public List<Shard> shards()
    {
        return new AbstractList<>()
        {
            @Override
            public Shard get(int i)
            {
                return shards[supersetIndexes[i]];
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

    public <A> A reduce(A zero,
                        Predicate<Shard> filter,
                        BiFunction<A, ? super Shard, A> reducer)
    {
        return Utils.reduce(zero, shards(), filter, reducer);
    }

    public SortedArrayList<Id> nodes()
    {
        return nodeIds;
    }

    public Ranges ranges()
    {
        return subsetOfRanges;
    }

    public Set<Id> staleIds()
    {
        return staleIds;
    }
    
    public Shard[] unsafeGetShards()
    {
        return shards;
    }
}
