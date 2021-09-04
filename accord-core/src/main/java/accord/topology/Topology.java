package accord.topology;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import accord.local.Node.Id;
import accord.api.Key;
import accord.txn.Keys;
import accord.utils.IndexedConsumer;

public class Topology extends AbstractCollection<Shard>
{
    // TODO: introduce range version of Keys
    final Keys starts;
    final Shard[] shards;
    final Map<Id, Shards.NodeInfo> nodeLookup;
    final Keys subsetOfStarts;
    final int[] supersetIndexes;

    static class NodeInfo
    {
        final Keys starts;
        final int[] supersetIndexes;

        NodeInfo(Keys starts, int[] supersetIndexes)
        {
            this.starts = starts;
            this.supersetIndexes = supersetIndexes;
        }

        @Override
        public String toString()
        {
            return starts.toString();
        }
    }

    public Topology(Shard... shards)
    {
        this.starts = new Keys(Arrays.stream(shards).map(shard -> shard.start).sorted().collect(Collectors.toList()));
        this.shards = shards;
        this.subsetOfStarts = starts;
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
            Keys starts = this.starts.select(supersetIndexes);
            nodeLookup.put(e.getKey(), new Shards.NodeInfo(starts, supersetIndexes));
        }
    }

    public Topology(Keys starts, Shard[] shards, Map<Id, Shards.NodeInfo> nodeLookup, Keys subsetOfStarts, int[] supersetIndexes)
    {
        this.starts = starts;
        this.shards = shards;
        this.nodeLookup = nodeLookup;
        this.subsetOfStarts = subsetOfStarts;
        this.supersetIndexes = supersetIndexes;
    }

    public Shards forNode(Id node)
    {
        NodeInfo info = nodeLookup.get(node);
        if (info == null)
            return Shards.EMPTY;
        return forKeys(info.starts);
    }

    public Shard forKey(Key key)
    {
        int i = starts.floorIndex(key);
        return shards[i];
    }

    public Shards forKeys(Keys select)
    {
        int subsetIndex = 0;
        int count = 0;
        int[] newSubset = new int[Math.min(select.size(), subsetOfStarts.size())];
        for (int i = 0 ; i < select.size() ; )
        {
            subsetIndex = subsetOfStarts.floorIndex(subsetIndex, subsetOfStarts.size(), select.get(i));
            int supersetIndex = supersetIndexes[subsetIndex];
            newSubset[count++] = supersetIndex;
            Shard shard = shards[supersetIndex];
            i = select.ceilIndex(i, select.size(), shard.end);
        }
        if (count != newSubset.length)
            newSubset = Arrays.copyOf(newSubset, count);
        Keys subsetOfKeys = starts.select(newSubset);
        return new Shards(starts, shards, nodeLookup, subsetOfKeys, newSubset);
    }

    /**
     * @param on the node to limit our selection to
     * @param select may be a superSet of the keys owned by {@code on} but not of this {@code Shards}
     */
    public void forEachOn(Id on, Keys select, IndexedConsumer<Shard> consumer)
    {
        Shards.NodeInfo info = nodeLookup.get(on);
//        int nodeIndex = 0;
//        int subsetIndex = 0;
//        for (int i = select.ceilIndex(info.starts.get(0)) ; i < select.size() ; )
//        {
//            nodeIndex = info.starts.floorIndex(nodeIndex, info.starts.size(), select.get(i));
//            int supersetIndex = info.supersetIndexes[nodeIndex];
//            Shard shard = shards[supersetIndex];
//            if (shard.end.compareTo(select.get(i)) > 0)
//            {
//                subsetIndex = Arrays.binarySearch(supersetIndexes, subsetIndex, supersetIndexes.length, supersetIndex);
//                consumer.accept(subsetIndex, shard);
//            }
//            i = select.ceilIndex(i + 1, select.size(), shard.end);
//        }

        for (int i = 0, j = 0, k = 0 ; i < select.size() && j < supersetIndexes.length && k < info.supersetIndexes.length ;)
        {
            Key key = select.get(i);
            Shard shard = shards[supersetIndexes[j]];
            int c = supersetIndexes[j] - info.supersetIndexes[k];
            if (c < 0) ++j;
            else if (c > 0) ++k;
            else if (key.compareTo(shard.start) < 0) ++i;
            else if (key.compareTo(shard.end) < 0) { consumer.accept(j, shard); i++; j++; k++; }
            else { j++; k++; }
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
//        int subsetIndex = 0;
//        for (int i = select.ceilIndex(shards[supersetIndexes[0]].start) ; i < select.size() ; )
//        {
//            subsetIndex = subsetOfStarts.floorIndex(subsetIndex, subsetOfStarts.size(), select.get(i));
//            selection.add(indexedByShard[subsetIndex]);
//            Shard shard = shards[supersetIndexes[subsetIndex]];
//            i = select.ceilIndex(i + 1, select.size(), shard.end);
//        }

//        int minSubsetIndex = 0;
//        for (int i = select.ceilIndex(shards[supersetIndexes[0]].start) ; i < select.size() ; )
//        {
//            int subsetIndex = subsetOfStarts.floorIndex(minSubsetIndex, subsetOfStarts.size(), select.get(i));
//            selection.add(indexedByShard[subsetIndex]);
//            minSubsetIndex = subsetIndex + 1;
//            if (minSubsetIndex == supersetIndexes.length)
//                break;
//            Shard shard = shards[supersetIndexes[minSubsetIndex]];
//            i = select.ceilIndex(i + 1, select.size(), shard.start);
//        }

//        int minSubsetIndex = 0;
//        for (int i = select.ceilIndex(shards[supersetIndexes[0]].start) ; i < select.size() ; )
//        {
//            int subsetIndex = subsetOfStarts.floorIndex(minSubsetIndex, subsetOfStarts.size(), select.get(i));
//            Shard shard = shards[supersetIndexes[subsetIndex]];
//            if (shard.end.compareTo(select.get(i)) > 0)
//                selection.add(indexedByShard[subsetIndex]);
//            minSubsetIndex = subsetIndex + 1;
//            if (minSubsetIndex == supersetIndexes.length)
//                break;
//
//            shard = shards[supersetIndexes[minSubsetIndex]];
//            i = select.ceilIndex(i + 1, select.size(), shard.start);
//        }

        for (int i = 0, j = 0 ; i < select.size() && j < supersetIndexes.length ;)
        {
            Key k = select.get(i);
            Shard shard = shards[supersetIndexes[j]];
            int c = k.compareTo(shard.start);
            if (c < 0) ++i;
            else if (k.compareTo(shard.end) < 0) { selection.add(indexedByShard[j++]); i++; }
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
        return subsetOfStarts.size();
    }

    public Shard get(int index)
    {
        return shards[supersetIndexes[index]];
    }
}
