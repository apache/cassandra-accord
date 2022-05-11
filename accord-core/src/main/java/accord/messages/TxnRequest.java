package accord.messages;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.primitives.KeyRanges;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.primitives.Keys;
import accord.primitives.TxnId;

import static java.lang.Long.min;

public abstract class TxnRequest implements EpochRequest
{
    public static abstract class WithUnsynced extends TxnRequest
    {
        public final TxnId txnId;
        public final long minEpoch;
        protected final boolean doNotComputeProgressKey;

        public WithUnsynced(Id to, Topologies topologies, Keys keys, TxnId txnId)
        {
            this(to, topologies, keys, txnId, latestRelevantEpochIndex(to, topologies, keys));
        }

        private WithUnsynced(Id to, Topologies topologies, Keys keys, TxnId txnId, int startIndex)
        {
            super(to, topologies, keys, startIndex);
            this.txnId = txnId;
            this.minEpoch = topologies.oldestEpoch();
            // to understand this calculation we must bear in mind the following:
            //  - startIndex is the "latest relevant" which means we skip over recent epochs where we are not owners at all,
            //    i.e. if this node does not participate in the most recent epoch, startIndex > 0
            //  - waitForEpoch gives us the most recent epoch with differing ownership information, starting from startIndex
            // So, we can have some surprising situations arise where a *prior* owner must be contacted for its vote,
            // and does not need to wait for the latest ring information because from the point of view of its contribution
            // the stale ring information is sufficient, however we do not want it to compute a progress key with this stale
            // ring information and mistakenly believe that it is a home shard for the transaction, as it will not receive
            // updates for the transaction going forward.
            // So in these cases we send a special flag indicating that the progress key should not be computed
            // (as it might be done so with stale ring information)
            this.doNotComputeProgressKey = waitForEpoch() < txnId.epoch && startIndex > 0
                                           && topologies.get(startIndex).epoch() < txnId.epoch;

            KeyRanges ranges = topologies.forEpoch(txnId.epoch).rangesForNode(to);
            if (doNotComputeProgressKey)
            {
                Preconditions.checkState(ranges == null || !ranges.intersects(keys)); // confirm dest is not a replica on txnId.epoch
            }
            else
            {
                boolean intersects = ranges != null && ranges.intersects(keys);
                long progressEpoch = Math.min(waitForEpoch(), txnId.epoch);
                KeyRanges computesRangesOn = topologies.forEpoch(progressEpoch).rangesForNode(to);
                boolean check = computesRangesOn != null && computesRangesOn.intersects(keys);
                if (check != intersects)
                    throw new IllegalStateException();
            }
        }

        Key progressKey(Node node, Key homeKey)
        {
            // if waitForEpoch < txnId.epoch, then this replica's ownership is unchanged
            long progressEpoch = min(waitForEpoch(), txnId.epoch);
            return doNotComputeProgressKey ? null : node.trySelectProgressKey(progressEpoch, scope(), homeKey);
        }

        @VisibleForTesting
        public WithUnsynced(Keys scope, long epoch, TxnId txnId)
        {
            super(scope, epoch);
            this.txnId = txnId;
            this.minEpoch = epoch;
            this.doNotComputeProgressKey = false;
        }
    }

    private final Keys scope;
    private final long waitForEpoch;

    public TxnRequest(Node.Id to, Topologies topologies, Keys keys)
    {
        this(to, topologies, keys, 0);
    }

    public TxnRequest(Node.Id to, Topologies topologies, Keys keys, int startIndex)
    {
        this(computeScope(to, topologies, keys, startIndex),
             computeWaitForEpoch(to, topologies, startIndex));
    }

    public TxnRequest(Keys scope, long waitForEpoch)
    {
        Preconditions.checkState(!scope.isEmpty());
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

    protected static int latestRelevantEpochIndex(Node.Id node, Topologies topologies, Keys keys)
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
        return computeWaitForEpoch(node, topologies, latestRelevantEpochIndex(node, topologies, keys));
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
        return computeScope(node, topologies, keys, latestRelevantEpochIndex(node, topologies, keys));
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
                scopeKeys = scopeKeys.union(keys.slice(ranges));

            last = ranges;
        }
        return scopeKeys;
    }
}
