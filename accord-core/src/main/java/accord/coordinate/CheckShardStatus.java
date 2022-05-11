package accord.coordinate;

import java.util.ArrayList;
import java.util.List;

import accord.api.Key;
import accord.coordinate.tracking.ReadTracker.ReadShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusReply;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Shard;
import accord.primitives.TxnId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public abstract class CheckShardStatus<T extends CheckStatusOk> extends AsyncFuture<T> implements Callback<CheckStatusReply>
{
    static class Tracker extends ReadShardTracker
    {
        private int successCount;
        public Tracker(Shard shard)
        {
            super(shard);
        }

        @Override
        public boolean recordReadSuccess(Id node)
        {
            if (!super.recordReadSuccess(node))
                return false;

            successCount++;
            return true;
        }

        public boolean hasReachedQuorum()
        {
            return successCount >= shard.slowPathQuorumSize;
        }

        public boolean hasInFlight()
        {
            return !inflight.isEmpty();
        }
    }

    final Node node;
    final TxnId txnId;
    final Key someKey; // not necessarily homeKey
    final Tracker tracker;
    final List<Id> candidates;
    final long epoch;
    final IncludeInfo includeInfo;

    CheckStatusOk max;
    Throwable failure;

    CheckShardStatus(Node node, TxnId txnId, Key someKey, Shard shard, long epoch, IncludeInfo includeInfo)
    {
        this.epoch = epoch;
        this.txnId = txnId;
        this.someKey = someKey;
        this.tracker = new Tracker(shard);
        this.candidates = new ArrayList<>(shard.nodes);
        this.node = node;
        this.includeInfo = includeInfo;
    }

    @Override
    public void onSuccess(Id from, CheckStatusReply response)
    {
        if (response.isOk())
        {
            onOk((CheckStatusOk) response);
            if (tracker.recordReadSuccess(from))
            {
                if (hasMetSuccessCriteria())
                {
                    onSuccessCriteriaOrExhaustion();
                }
                else if (!tracker.hasInFlight())
                {
                    if (hasMoreCandidates())
                        sendMore();
                    else
                        onSuccessCriteriaOrExhaustion();
                }
            }
        }
        else
        {
            onFailure(from, new StaleTopology());
        }
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        if (this.failure == null) this.failure = failure;
        else this.failure.addSuppressed(failure);

        // TODO: if we fail and have an incorrect topology, trigger refresh
        if (tracker.recordReadFailure(from))
        {
            if (tracker.hasFailed())
            {
                tryFailure(this.failure);
            }
            else if (!tracker.hasInFlight())
            {
                if (hasMoreCandidates())
                    sendMore();
                else
                    onSuccessCriteriaOrExhaustion();
            }
        }
    }

    @Override
    public void onSlowResponse(Id from)
    {
        if (!tracker.hasFailed() && hasMoreCandidates())
            sendMore();
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        tryFailure(failure);
    }

    private void onOk(CheckStatusOk ok)
    {
        if (max == null) max = ok;
        else max = max.merge(ok);
    }

    protected void start()
    {
        sendMore();
    }

    private void sendMore()
    {
        // TODO: send to local nodes first, and send in batches (local then remote)
        Id next = candidates.get(candidates.size() - 1);
        candidates.remove(candidates.size() - 1);
        tracker.recordInflightRead(next);
        node.send(next, new CheckStatus(txnId, someKey, epoch, includeInfo), this);
    }

    private boolean hasMoreCandidates()
    {
        return !candidates.isEmpty();
    }

    abstract boolean hasMetSuccessCriteria();
    abstract void onSuccessCriteriaOrExhaustion();
}
