package accord.topology;

import java.util.Collections;
import java.util.Map;

import accord.api.KeyRange;
import accord.local.Node.Id;
import accord.txn.Keys;

public class Shards extends Topology
{
    public static final Shards EMPTY = new Shards(new Shard[0], KeyRanges.EMPTY, Collections.emptyMap(), KeyRanges.EMPTY.EMPTY, new int[0]);

    public Shards(Shard... shards)
    {
        super(shards);
    }

    public Shards(Shard[] shards, KeyRanges ranges, Map<Id, NodeInfo> nodeLookup, KeyRanges subsetOfRanges, int[] supersetIndexes)
    {
        super(shards, ranges, nodeLookup, subsetOfRanges, supersetIndexes);
    }

    public static Shards select(Shard[] shards, int[] indexes)
    {
        Shard[] subset = new Shard[indexes.length];
        for (int i=0; i<indexes.length; i++)
            subset[i] = shards[indexes[i]];

        return new Shards(subset);
    }
}
