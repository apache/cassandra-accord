package accord.coordinate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;

import accord.coordinate.tracking.ReadTracker;
import accord.coordinate.tracking.ReadTracker.ReadShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;

// TODO: this class needs cleaning up
//       we should also escalate the number of nodes we contact on each failure to succeed
abstract class QuorumReadCoordinator<Reply> implements Callback<Reply>
{
    protected enum Action { Abort, Continue, Reject, AcceptQuorum, Accept, Success }

    protected enum Done { Exhausted, ReachedQuorum, Success }

    static class QuorumReadShardTracker extends ReadShardTracker
    {
        private int responseCount;
        public QuorumReadShardTracker(Shard shard)
        {
            super(shard);
        }

        public boolean recordReadResponse(Id node)
        {
            Preconditions.checkArgument(shard.nodes.contains(node));
            ++responseCount;
            --inflight;
            return true;
        }

        @Override
        public boolean recordReadSuccess(Id node)
        {
            if (!super.recordReadSuccess(node))
                return false;

            ++responseCount;
            return true;
        }

        public boolean hasFailed()
        {
            return super.hasFailed() && !hasReachedQuorum();
        }

        public boolean hasReachedQuorum()
        {
            return responseCount >= shard.slowPathQuorumSize;
        }
    }

    static class Tracker extends ReadTracker<QuorumReadShardTracker>
    {
        public Tracker(Topologies topologies)
        {
            super(topologies, QuorumReadShardTracker[]::new, QuorumReadShardTracker::new);
        }

        void recordReadResponse(Id node)
        {
            if (!recordResponse(node))
                return;

            forEachTrackerForNode(node, QuorumReadShardTracker::recordReadResponse);
        }

        public boolean hasReachedQuorum()
        {
            return all(QuorumReadShardTracker::hasReachedQuorum);
        }
    }

    final Node node;
    final TxnId txnId;
    protected final Tracker tracker;
    private boolean isDone;
    private Throwable failure;
    Map<Id, Reply> debug = new HashMap<>();

    QuorumReadCoordinator(Node node, Topologies topologies, TxnId txnId)
    {
        this.node = node;
        this.txnId = txnId;
        this.tracker = new Tracker(topologies);
    }

     protected void start()
    {
        contact(tracker.computeMinimalReadSetAndMarkInflight());
    }

    protected abstract void contact(Set<Id> nodes);
    protected abstract Action process(Id from, Reply reply);

    protected abstract void onDone(Done done, Throwable failure);

    @Override
    public void onSuccess(Id from, Reply reply)
    {
        debug.put(from, reply);
        if (isDone)
            return;

        switch (process(from, reply))
        {
            default: throw new IllegalStateException();
            case Abort:
                isDone = true;
                break;

            case Continue:
                break;

            case Reject:
                tracker.recordReadFailure(from);
                tryOneMore();
                break;

            case AcceptQuorum:
                tracker.recordReadResponse(from);
                if (!finishIfQuorum())
                    tryOneMore();
                break;

            case Accept:
                tracker.recordReadSuccess(from);
                if (!tracker.hasCompletedRead())
                {
                    if (!finishIfQuorum())
                        finishIfFailure();
                    break;
                }

            case Success:
                isDone = true;
                onDone(Done.Success, null);
        }
    }

    @Override
    public void onSlowResponse(Id from)
    {
        tracker.recordSlowRead(from);
        Set<Id> readFrom = tracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null)
            contact(readFrom);
    }

    @Override
    public void onFailure(Id from, Throwable failure)
    {
        debug.put(from, null);
        if (isDone)
            return;

        if (this.failure == null) this.failure = failure;
        else this.failure.addSuppressed(failure);

        if (tracker.recordReadFailure(from))
            tryOneMore();
    }

    private void tryOneMore()
    {
        Set<Id> readFrom = tracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom != null) contact(readFrom);
        else finishIfFailure();
    }

    private boolean finishIfQuorum()
    {
        if (!tracker.hasReachedQuorum())
            return false;

        isDone = true;
        onDone(Done.ReachedQuorum, null);
        return true;
    }

    private void finishIfFailure()
    {
        if (tracker.hasFailed())
        {
            isDone = true;
            onDone(null, failure);
        }
        else if (!tracker.hasInFlight())
        {
            isDone = true;
            onDone(Done.Exhausted, null);
        }
    }

    @Override
    public void onCallbackFailure(Throwable failure)
    {
        isDone = true;
        if (this.failure != null)
            failure.addSuppressed(this.failure);
        onDone(null, failure);
    }

    protected Set<Id> nodes()
    {
        return tracker.nodes();
    }

}
