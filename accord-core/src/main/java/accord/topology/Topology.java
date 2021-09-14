package accord.topology;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import accord.api.KeyRange;
import accord.local.Node.Id;
import accord.api.Key;
import accord.txn.Keys;
import accord.utils.IndexedConsumer;

public class Topology extends AbstractCollection<Shard>
{
    final Shard[] shards;
    final KeyRanges ranges;
    final Map<Id, Shards.NodeInfo> nodeLookup;
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

    public Topology(Shard... shards)
    {
        this.ranges = new KeyRanges(Arrays.stream(shards).map(shard -> shard.range).toArray(KeyRange[]::new));
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
            nodeLookup.put(e.getKey(), new Shards.NodeInfo(ranges, supersetIndexes));
        }
    }

    public Topology(Shard[] shards, KeyRanges ranges, Map<Id, Shards.NodeInfo> nodeLookup, KeyRanges subsetOfRanges, int[] supersetIndexes)
    {
        this.shards = shards;
        this.ranges = ranges;
        this.nodeLookup = nodeLookup;
        this.subsetOfRanges = subsetOfRanges;
        this.supersetIndexes = supersetIndexes;
    }

    public Shards forNode(Id node)
    {
        NodeInfo info = nodeLookup.get(node);
        if (info == null)
            return Shards.EMPTY;
        return Shards.select(shards, info.supersetIndexes);
    }

    public Shard forKey(Key key)
    {
        int i = ranges.rangeIndexForKey(key);
        if (i < 0 || i >= ranges.size())
            throw new IllegalArgumentException("Range not found for " + key);
        return shards[i];
    }

    public Shards forKeys(Keys select)
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
            newSubset[count++] = supersetIndex;
            Shard shard = shards[supersetIndex];
            // find the first key outside this range
            i = shard.range.higherKeyIndex(select, i, select.size());
        }
        if (count != newSubset.length)
            newSubset = Arrays.copyOf(newSubset, count);
        KeyRanges rangeSubset = ranges.select(newSubset);
        return new Shards(shards, ranges, nodeLookup, rangeSubset, newSubset);
    }

    /**
     * @param on the node to limit our selection to
     * @param select may be a superSet of the keys owned by {@code on} but not of this {@code Shards}
     */
    public void forEachOn(Id on, Keys select, IndexedConsumer<Shard> consumer)
    {
        Shards.NodeInfo info = nodeLookup.get(on);
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
        // TODO: this can be done by divide-and-conquer splitting of the lists and recursion, which should be more efficient
        Shards.NodeInfo info = nodeLookup.get(on);
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

    public void forEach(IndexedConsumer<Shard> consumer)
    {
        for (int i = 0 ; i < supersetIndexes.length ; ++i)
            consumer.accept(i, shards[supersetIndexes[i]]);
    }

    public <T> T[] select(Keys select, T[] indexedByShard, IntFunction<T[]> constructor)
    {
        List<T> selection = new ArrayList<>();
        for (int i = 0, j = 0 ; i < select.size() && j < supersetIndexes.length ;)
        {
            Key k = select.get(i);
            Shard shard = shards[supersetIndexes[j]];

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
        return IntStream.of(supersetIndexes).mapToObj(i -> shards[i]).iterator();
    }

    @Override
    public int size()
    {
        return subsetOfRanges.size();
    }

    public Shard get(int index)
    {
        return shards[supersetIndexes[index]];
    }
}
