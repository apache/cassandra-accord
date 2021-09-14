package accord.topology;

import java.util.List;
import java.util.Set;

import accord.api.KeyRange;
import accord.local.Node.Id;
import accord.api.Key;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class Shard
{
    public final KeyRange range;
    public final List<Id> nodes;
    public final Set<Id> fastPathElectorate;
    public final int recoveryFastPathSize;
    public final int fastPathQuorumSize;
    public final int slowPathQuorumSize;

    public Shard(KeyRange range, List<Id> nodes, Set<Id> fastPathElectorate)
    {
        this.range = range;
        this.nodes = nodes;
        int f = maxToleratedFailures(nodes.size());
        this.fastPathElectorate = fastPathElectorate;
        int e = fastPathElectorate.size();
        this.recoveryFastPathSize = (f+1)/2;
        this.slowPathQuorumSize = f + 1;
        this.fastPathQuorumSize = fastPathQuorumSize(nodes.size(), e, f);
    }

    @VisibleForTesting
    static int maxToleratedFailures(int replicas)
    {
        return (replicas - 1) / 2;
    }

    @VisibleForTesting
    static int fastPathQuorumSize(int replicas, int electorate, int f)
    {
        Preconditions.checkArgument(electorate >= replicas - f);
        return (f + electorate)/2 + 1;
    }

    public boolean contains(Key key)
    {
        return range.containsKey(key);
    }

    @Override
    public String toString()
    {
        return "Shard[" + range.start() + ',' + range.end() + ']';
    }
}
