package accord.messages;

import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.txn.Keys;

public abstract class TxnRequest implements EpochRequest
{
    private final Keys scope;
    private final long waitForEpoch;

    public TxnRequest(Node.Id to, Topologies topologies, Keys keys)
    {
        int startIndex = findLatestRelevantEpochIndex(to, topologies, keys);
        this.scope = computeScope(to, topologies, keys, startIndex);
        this.waitForEpoch = computeWaitForEpoch(to, topologies, startIndex);
    }

    public TxnRequest(Keys scope, long waitForEpoch)
    {
        this.scope = scope;
        this.waitForEpoch = waitForEpoch;
    }

    public Keys scope()
    {
        return scope;
    }

    public long waitForEpoch()
    {
        return waitForEpoch;
    }

    private static int findLatestRelevantEpochIndex(Node.Id node, Topologies topologies, Keys keys)
    {
        KeyRanges latest = topologies.get(0).rangesForNode(node);

        if (latest != null && latest.intersects(keys))
            return 0;

        int i = 0;
        int mi = topologies.size();

        // find first non-null for node
        while (latest == null)
        {
            if (++i == mi)
                return mi;

            latest = topologies.get(i).rangesForNode(node);
        }

        if (latest.intersects(keys))
            return i;

        // find first non-empty intersection for node
        while (++i < mi)
        {
            KeyRanges next = topologies.get(i).rangesForNode(node);
            if (!next.equals(latest))
            {
                if (next.intersects(keys))
                    return i;
                latest = next;
            }
        }
        return mi;
    }

    // for now use a simple heuristic of whether the node's ownership ranges have changed,
    // on the assumption that this might also mean some local shard rearrangement
    // except if the latest epoch is empty for the keys
    public static long computeWaitForEpoch(Node.Id node, Topologies topologies, Keys keys)
    {
        return computeWaitForEpoch(node, topologies, findLatestRelevantEpochIndex(node, topologies, keys));
    }

    public static long computeWaitForEpoch(Node.Id node, Topologies topologies, int startIndex)
    {
        int i = startIndex;
        int mi = topologies.size();
        if (i == mi)
            return topologies.oldestEpoch();

        KeyRanges latest = topologies.get(i).rangesForNode(node);
        while (++i < mi)
        {
            Topology topology = topologies.get(i);
            KeyRanges ranges = topology.rangesForNode(node);
            if (ranges == null || !ranges.equals(latest))
                break;
        }
        return topologies.get(i - 1).epoch();
    }

    public static Keys computeScope(Node.Id node, Topologies topologies, Keys keys)
    {
        return computeScope(node, topologies, keys, findLatestRelevantEpochIndex(node, topologies, keys));
    }

    public static Keys computeScope(Node.Id node, Topologies topologies, Keys keys, int startIndex)
    {
        KeyRanges last = null;
        Keys scopeKeys = Keys.EMPTY;
        for (int i = startIndex, mi = topologies.size() ; i < mi ; ++i)
        {
            Topology topology = topologies.get(i);
            KeyRanges ranges = topology.rangesForNode(node);
            if (ranges != last && ranges != null && !ranges.equals(last))
                scopeKeys = scopeKeys.union(keys.intersect(ranges));

            last = ranges;
        }
        return scopeKeys;
    }
}
