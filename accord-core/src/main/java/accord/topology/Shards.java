package accord.topology;

import java.util.Collections;
import java.util.Map;

import accord.local.Node.Id;
import accord.txn.Keys;

public class Shards extends Topology
{
    public static final Shards EMPTY = new Shards(Keys.EMPTY, new Shard[0], Collections.emptyMap(), Keys.EMPTY, new int[0]);

    public Shards(Shard... shards)
    {
        super(shards);
    }

    public Shards(Keys starts, Shard[] shards, Map<Id, NodeInfo> nodeLookup, Keys subsetOfStarts, int[] supersetIndexes)
    {
        super(starts, shards, nodeLookup, subsetOfStarts, supersetIndexes);
    }
}
