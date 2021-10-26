package accord.maelstrom;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import accord.api.KeyRange;
import accord.local.Node.Id;
import accord.maelstrom.Datum.Kind;
import accord.topology.Shard;
import accord.topology.Shards;
import accord.utils.WrapAroundList;
import accord.utils.WrapAroundSet;

public class TopologyFactory
{
    final int shards;
    final int rf;
    final Kind[] kinds;
    final KeyRange<MaelstromKey>[][] ranges;

    public TopologyFactory(int shards, int rf)
    {
        this.shards = shards;
        this.rf = rf;
        this.kinds = Datum.COMPARE_BY_HASH ? new Kind[] { Kind.HASH } : new Kind[] { Kind.STRING, Kind.LONG, Kind.DOUBLE };
        this.ranges = new MaelstromKey.Range[kinds.length][shards];
        for (int i = 0 ; i < kinds.length ; ++i)
        {
            Kind kind = kinds[i];
            MaelstromKey[] starts = kind.split(shards);
            MaelstromKey[] ends = new MaelstromKey[shards];
            System.arraycopy(starts, 1, ends, 0, shards - 1);
            ends[shards - 1] = new MaelstromKey(kind, null);
            this.ranges[i] = new MaelstromKey.Range[shards];
            for (int j=0; j<shards; j++)
                ranges[i][j] = new MaelstromKey.Range(starts[j], ends[j]);
        }
    }


    public Shards toShards(Id[] cluster)
    {
        final Map<Id, Integer> lookup = new HashMap<>();
        for (int i = 0 ; i < cluster.length ; ++i)
            lookup.put(cluster[i], i);

        List<WrapAroundList<Id>> electorates = new ArrayList<>();
        List<Set<Id>> fastPathElectorates = new ArrayList<>();

        for (int i = 0 ; i < cluster.length + rf - 1 ; ++i)
        {
            WrapAroundList<Id> electorate = new WrapAroundList<>(cluster, i % cluster.length, (i + rf) % cluster.length);
            Set<Id> fastPathElectorate = new WrapAroundSet<>(lookup, electorate);
            electorates.add(electorate);
            fastPathElectorates.add(fastPathElectorate);
        }

        final List<Shard> shards = new ArrayList<>();
        for (int j = 0 ; j < kinds.length ; ++j)
        {
            for (int i = 0 ; i < this.shards ; ++i)
                shards.add(new Shard(ranges[j][i], electorates.get(i % electorates.size()), fastPathElectorates.get(i % fastPathElectorates.size())));
        }
        return new Shards(1, shards.toArray(Shard[]::new));
    }
}
