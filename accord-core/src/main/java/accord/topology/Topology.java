package accord.topology;

import java.util.*;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import accord.api.KeyRange;
import accord.local.Node.Id;
import accord.api.Key;
import accord.txn.Keys;
import accord.utils.IndexedConsumer;
import accord.utils.IndexedBiFunction;
import accord.utils.IndexedPredicate;

public class Topology extends AbstractCollection<Shard>
{
    public static final Topology EMPTY = new Topology(0, new Shard[0], KeyRanges.EMPTY, Collections.emptyMap(), KeyRanges.EMPTY.EMPTY, new int[0]);
    final long epoch;
    final Shard[] shards;
    final KeyRanges ranges;
    final Map<Id, NodeInfo> nodeLookup;
    final KeyRanges subsetOfRanges;
    final int[] supersetRangeIndexes;

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
        this.ranges = new KeyRanges(Arrays.stream(shards).map(shard -> shard.range).toArray(KeyRange[]::new));
        this.shards = shards;
        this.subsetOfRanges = ranges;
        this.supersetRangeIndexes = IntStream.range(0, shards.length).toArray();
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
        this.supersetRangeIndexes = supersetIndexes;
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
        Topology topology = (Topology) o;
        return epoch == topology.epoch && Arrays.equals(shards, topology.shards) && ranges.equals(topology.ranges) && nodeLookup.equals(topology.nodeLookup) && subsetOfRanges.equals(topology.subsetOfRanges) && Arrays.equals(supersetRangeIndexes, topology.supersetRangeIndexes);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(epoch, ranges, nodeLookup, subsetOfRanges);
        result = 31 * result + Arrays.hashCode(shards);
        result = 31 * result + Arrays.hashCode(supersetRangeIndexes);
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
        return supersetRangeIndexes.length < shards.length;
    }

    public Topology withEpoch(long epoch)
    {
        return new Topology(epoch, shards, ranges, nodeLookup, subsetOfRanges, supersetRangeIndexes);
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
        return select(epoch, shards, info.supersetIndexes);
    }

    public Shard forKey(Key key)
    {
        int i = ranges.rangeIndexForKey(key);
        if (i < 0 || i >= ranges.size())
            throw new IllegalArgumentException("Range not found for " + key);
        return shards[i];
    }

    public Topology forKeys(Keys select)
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
            int supersetIndex = supersetRangeIndexes[subsetIndex];
            newSubset[count++] = supersetIndex;
            Shard shard = shards[supersetIndex];
            // find the first key outside this range
            i = shard.range.higherKeyIndex(select, i, select.size());
        }
        if (count != newSubset.length)
            newSubset = Arrays.copyOf(newSubset, count);
        KeyRanges rangeSubset = ranges.select(newSubset);

        // TODO: more efficient sharing of nodeLookup state
        Map<Id, NodeInfo> nodeLookup = new HashMap<>();
        for (Map.Entry<Id, NodeInfo> e : this.nodeLookup.entrySet())
        {
            if (intersects(newSubset, e.getValue().supersetIndexes))
                nodeLookup.put(e.getKey(), e.getValue());
        }
        return new Topology(epoch, shards, ranges, nodeLookup, rangeSubset, newSubset);
    }

    public <T> T accumulateForKeys(Keys select, IndexedBiFunction<Shard, T, T> function, T start)
    {
        int subsetIndex = 0;
        for (int i = 0 ; i < select.size() ; )
        {
            // find the range containing the key at i
            subsetIndex = subsetOfRanges.rangeIndexForKey(subsetIndex, subsetOfRanges.size(), select.get(i));
            if (subsetIndex < 0 || subsetIndex >= subsetOfRanges.size())
                throw new IllegalArgumentException("Range not found for " + select.get(i));
            int supersetIndex = supersetRangeIndexes[subsetIndex];
            Shard shard = shards[supersetIndex];
            start = function.apply(subsetIndex, shard, start);
            // find the first key outside this range
            i = shard.range.higherKeyIndex(select, i, select.size());
        }
        return start;
    }

    /**
     * @param on the node to limit our selection to
     * @param select may be a superSet of the keys owned by {@code on} but not of this {@code Topology}
     */
    public void forEachOn(Id on, Keys select, IndexedConsumer<Shard> consumer)
    {
        NodeInfo info = nodeLookup.get(on);
        for (int i = 0, j = 0, k = 0 ; i < select.size() && j < supersetRangeIndexes.length && k < info.supersetIndexes.length ;)
        {
            Key key = select.get(i);
            Shard shard = shards[supersetRangeIndexes[j]];
            int c = supersetRangeIndexes[j] - info.supersetIndexes[k];
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
        // TODO: this can be done by divide-and-conquer splitting of the lists and recursion, which should be more efficient
        NodeInfo info = nodeLookup.get(on);
        if (info == null)
            return;
        int[] a = supersetRangeIndexes, b = info.supersetIndexes;
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
                ai = Arrays.binarySearch(a, ai + 1, a.length, b[bi]);
                if (ai < 0) ai = -1 -ai;
            }
            else
            {
                bi = Arrays.binarySearch(b, bi + 1, b.length, a[ai]);
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
        int[] a = supersetRangeIndexes, b = info.supersetIndexes;
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
                ai = Arrays.binarySearch(a, ai + 1, a.length, b[bi]);
                if (ai < 0) ai = -1 -ai;
            }
            else
            {
                bi = Arrays.binarySearch(b, bi + 1, b.length, a[ai]);
                if (bi < 0) bi = -1 -bi;
            }
        }
        return count;
    }

    public void forEach(IndexedConsumer<Shard> consumer)
    {
        for (int i = 0; i < supersetRangeIndexes.length ; ++i)
            consumer.accept(i, shards[supersetRangeIndexes[i]]);
    }

    public <T> T[] select(Keys select, T[] indexedByShard, IntFunction<T[]> constructor)
    {
        List<T> selection = new ArrayList<>();
        for (int i = 0, j = 0 ; i < select.size() && j < supersetRangeIndexes.length ;)
        {
            Key k = select.get(i);
            Shard shard = shards[supersetRangeIndexes[j]];

            int c = shard.range.compareKey(k);
            if (c < 0) ++i;
            else if (c == 0) { selection.add(indexedByShard[j++]); i++; }
            else j++;
        }

        return selection.toArray(constructor);
    }

    @Override
    public Iterator<Shard> iterator()
    {
        return IntStream.of(supersetRangeIndexes).mapToObj(i -> shards[i]).iterator();
    }

    @Override
    public int size()
    {
        return subsetOfRanges.size();
    }

    public int maxRf()
    {
        int rf = Integer.MIN_VALUE;
        for (int i : supersetRangeIndexes)
            rf = Math.max(rf, shards[i].rf());
        return rf;
    }

    public Shard get(int index)
    {
        return shards[supersetRangeIndexes[index]];
    }

    public Set<Id> nodes()
    {
        return nodeLookup.keySet();
    }

    public KeyRanges ranges()
    {
        return ranges;
    }

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
}
