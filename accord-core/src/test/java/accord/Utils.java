package accord;

import accord.api.KeyRange;
import accord.local.Node;
import accord.impl.mock.MockStore;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Shards;
import accord.topology.Topology;
import accord.txn.Txn;
import accord.txn.Keys;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Utils
{
    public static Node.Id id(int i)
    {
        return new Node.Id(i);
    }

    public static List<Node.Id> ids(int num)
    {
        List<Node.Id> rlist = new ArrayList<>(num);
        for (int i=0; i<num; i++)
        {
            rlist.add(id(i+1));
        }
        return rlist;
    }

    public static List<Node.Id> ids(int first, int last)
    {
        Preconditions.checkArgument(last >= first);
        List<Node.Id> rlist = new ArrayList<>(last - first + 1);
        for (int i=first; i<=last; i++)
            rlist.add(id(i));

        return rlist;
    }

    public static List<Node.Id> idList(int... ids)
    {
        List<Node.Id> list = new ArrayList<>(ids.length);
        for (int i : ids)
            list.add(new Node.Id(i));
        return list;
    }

    public static Set<Node.Id> idSet(int... ids)
    {
        Set<Node.Id> set = Sets.newHashSetWithExpectedSize(ids.length);
        for (int i : ids)
            set.add(new Node.Id(i));
        return set;
    }

    public static KeyRanges ranges(KeyRange... ranges)
    {
        return new KeyRanges(ranges);
    }

    public static Shards shards(long epoch, Shard... shards)
    {
        return new Shards(epoch, shards);
    }

    public static Shards shards(Shard... shards)
    {
        return shards(1, shards);
    }

    public static Txn writeTxn(Keys keys)
    {
        return new Txn(keys, MockStore.READ, MockStore.QUERY, MockStore.UPDATE);
    }

    public static Txn readTxn(Keys keys)
    {
        return new Txn(keys, MockStore.READ, MockStore.QUERY);
    }

    public static Shard shard(KeyRange range, List<Node.Id> nodes, Set<Node.Id> fastPath)
    {
        return new Shard(range, nodes, fastPath);
    }

    public static Topology topology(long epoch, Shard... shards)
    {
        return new Shards(epoch, shards);
    }
}
