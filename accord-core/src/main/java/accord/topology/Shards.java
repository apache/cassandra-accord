package accord.topology;

import java.util.Collections;
import java.util.Map;

import accord.api.KeyRange;
import accord.local.Node.Id;
import accord.txn.Keys;

// TODO: merge with Topology
public class Shards extends Topology
{
    public static final Shards EMPTY = new Shards(0, new Shard[0], KeyRanges.EMPTY, Collections.emptyMap(), KeyRanges.EMPTY.EMPTY, new int[0]);

    public Shards(long epoch, Shard... shards)
    {
        super(epoch, shards);
    }

    public Shards(long epoch, Shard[] shards, KeyRanges ranges, Map<Id, NodeInfo> nodeLookup, KeyRanges subsetOfRanges, int[] supersetIndexes)
    {
        super(epoch, shards, ranges, nodeLookup, subsetOfRanges, supersetIndexes);
    }

    @Override
    public Topology withEpoch(long epoch)
    {
        return new Shards(epoch, shards, ranges, nodeLookup, subsetOfRanges, supersetRangeIndexes);
    }

    public static Shards select(long epoch, Shard[] shards, int[] indexes)
    {
        Shard[] subset = new Shard[indexes.length];
        for (int i=0; i<indexes.length; i++)
            subset[i] = shards[indexes[i]];

        return new Shards(epoch, subset);
    }
}
