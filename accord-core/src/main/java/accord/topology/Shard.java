package accord.topology;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import accord.api.KeyRange;
import accord.local.Node.Id;
import accord.api.Key;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class Shard
{
    public final KeyRange range;
    // TODO: use BTreeSet to combine these two (or introduce version that operates over long values)
    public final List<Id> nodes;
    public final Set<Id> nodeSet;
    public final Set<Id> fastPathElectorate;
    public final Set<Id> pending;
    public final int recoveryFastPathSize;
    public final int fastPathQuorumSize;
    public final int slowPathQuorumSize;

    public Shard(KeyRange range, List<Id> nodes, Set<Id> fastPathElectorate, Set<Id> pending)
    {
        Preconditions.checkArgument(Iterables.all(pending, nodes::contains),
                                    "pending nodes must also be present in nodes");
        this.range = range;
        this.nodes = ImmutableList.copyOf(nodes);
        this.nodeSet = ImmutableSet.copyOf(nodes);
        Preconditions.checkArgument(nodes.size() == nodeSet.size());
        int f = maxToleratedFailures(nodes.size());
        this.fastPathElectorate = ImmutableSet.copyOf(fastPathElectorate);
        this.pending = ImmutableSet.copyOf(pending);
        int e = fastPathElectorate.size();
        this.recoveryFastPathSize = (f+1)/2;
        this.slowPathQuorumSize = f + 1;
        this.fastPathQuorumSize = fastPathQuorumSize(nodes.size(), e, f);
    }

    public Shard(KeyRange range, List<Id> nodes, Set<Id> fastPathElectorate)
    {
        this(range, nodes, fastPathElectorate, Collections.emptySet());
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

    public int rf()
    {
        return nodes.size();
    }

    public boolean contains(Key key)
    {
        return range.containsKey(key);
    }

    public String toString(boolean extendedInfo)
    {
        String s = "Shard[" + range.start() + ',' + range.end() + ']';

        if (extendedInfo)
            s = s + ":(" + nodes + '/' + fastPathElectorate + ')';
        return s;
    }

    @Override
    public String toString()
    {
        return "Shard[" + range.start() + ',' + range.end() + ']';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shard shard = (Shard) o;
        return recoveryFastPathSize == shard.recoveryFastPathSize
            && fastPathQuorumSize == shard.fastPathQuorumSize
            && slowPathQuorumSize == shard.slowPathQuorumSize
            && range.equals(shard.range)
            && nodes.equals(shard.nodes)
            && nodeSet.equals(shard.nodeSet)
            && fastPathElectorate.equals(shard.fastPathElectorate)
            && pending.equals(shard.pending);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(range,
                            nodes,
                            nodeSet,
                            fastPathElectorate,
                            pending,
                            recoveryFastPathSize,
                            fastPathQuorumSize,
                            slowPathQuorumSize);
    }
}
